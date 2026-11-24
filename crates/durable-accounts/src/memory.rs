use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

/// Wall clock for the in-memory store — deadlines here are real, so
/// tests drive time travel through the caller-side `now_ms` params.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

use crate::{
    Account, AccountError, AccountStore, ApiKeyInfo, HandlerRegistration, KeyGrant, Mail,
    MailOutcome, Permission, SessionGrant, SignupOutcome, Workspace,
};

/// In-memory account graph. Used by dialect-free tests and fixtures; it
/// does not aim to survive restarts. `set_unavailable` flips every call
/// to `Unavailable` so outage behavior can be exercised without a real
/// Neon outage.
#[derive(Default)]
pub struct Memory {
    inner: Mutex<Inner>,
    next_workspace: AtomicU64,
    next_key: AtomicU64,
    next_account: AtomicU64,
    next_session: AtomicU64,
    next_token: AtomicU64,
    next_invite: AtomicU64,
    next_outbox: AtomicU64,
    unavailable: AtomicBool,
}

#[derive(Default)]
struct Inner {
    workspaces: HashMap<u64, Workspace>,
    keys: HashMap<u64, KeyRow>,
    handlers: HashMap<(u64, String), HandlerRow>,
    accounts: HashMap<u64, Account>,
    /// (workspace, account) -> role. ws 0 rows are never written — the
    /// ops plane is key-only.
    members: HashMap<(u64, u64), MemberRow>,
    sessions: HashMap<u64, SessionRow>,
    email_tokens: HashMap<u64, EmailTokenRow>,
    invites: HashMap<u64, InviteRow>,
    outbox: HashMap<u64, OutboxRow>,
}

struct HandlerRow {
    url: String,
    last_test_at_ms: Option<i64>,
    last_test_status: Option<String>,
    last_test_latency_ms: Option<i64>,
}

struct KeyRow {
    workspace: u64,
    public_id: uuid::Uuid,
    name: String,
    hash: [u8; 32],
    prefix: String,
    permission: Permission,
    revoked: bool,
    last_used_at_ms: Option<i64>,
    created_by: Option<u64>,
    created_at_ms: i64,
}

impl KeyRow {
    /// ApiKeyInfo.created_by is the creator's PUBLIC id — internal
    /// account ids never leave the store.
    fn info(&self, id: u64, inner: &Inner) -> ApiKeyInfo {
        ApiKeyInfo {
            id,
            public_id: self.public_id,
            name: self.name.clone(),
            prefix: self.prefix.clone(),
            permission: self.permission,
            revoked: self.revoked,
            last_used_at_ms: self.last_used_at_ms,
            created_by: self
                .created_by
                .and_then(|a| inner.accounts.get(&a).map(|acc| acc.public_id)),
            created_at_ms: self.created_at_ms,
        }
    }
}

struct SessionRow {
    account: u64,
    public_id: uuid::Uuid,
    token_hash: [u8; 32],
    csrf_seed: [u8; 32],
    expires_at_ms: i64,
    idle_expires_at_ms: i64,
    created_at_ms: i64,
    last_seen_at_ms: i64,
    user_agent: Option<String>,
    ip: Option<String>,
    /// Audit parity with pg's `replaced_by` — written, never read here.
    #[allow(dead_code)]
    replaces: Option<u64>,
    revoked: bool,
}

struct EmailTokenRow {
    account: u64,
    kind: &'static str,
    hash: [u8; 32],
    expires_at_ms: i64,
    /// The pending password hash verify installs (review #2); reset
    /// tokens carry none — the new hash arrives with the consume call.
    password_hash: Option<String>,
    consumed: bool,
    superseded: bool,
}

struct MemberRow {
    role: crate::Role,
    since_ms: i64,
}

struct InviteRow {
    workspace: u64,
    email: String,
    role: crate::Role,
    public_id: uuid::Uuid,
    /// Audit linkage for the pg dialect — written, never read in memory.
    #[allow(dead_code)]
    invited_by: u64,
    token_hash: [u8; 32],
    expires_at_ms: i64,
    created_at_ms: i64,
    accepted: bool,
    revoked: bool,
}

#[derive(Debug, Clone)]
pub struct OutboxRow {
    pub kind: &'static str,
    pub send_to: String,
    pub status: &'static str,
    pub attempts: i64,
    pub next_attempt_at_ms: i64,
    pub created_at_ms: i64,
    pub last_error: Option<String>,
    pub mail: Mail,
}

impl Memory {
    pub fn new() -> Self {
        let memory = Self::default();
        memory.seed_default();
        memory
    }

    /// Draw a fresh secret — shared by create, rotate and the seeded
    /// default workspace. A failure is a Store error, never a zero key.
    fn draw_secret() -> Result<[u8; 32], AccountError> {
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret)
            .map_err(|e| AccountError::Store(format!("cannot draw a handler secret: {e}")))?;
        Ok(secret)
    }

    /// Seed workspace 0 (`default`) so env-key fixtures look like a
    /// migrated database does. Idempotent. A broken CSPRNG is a dead
    /// machine — panic rather than seed a zero secret.
    fn seed_default(&self) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let secret = Self::draw_secret().unwrap_or_else(|e| panic!("{e}"));
        inner.workspaces.entry(0).or_insert_with(|| Workspace {
            id: 0,
            public_id: uuid::Uuid::nil(),
            name: "default".into(),
            handler_secret: secret,
            deleted_at_ms: None,
        });
        self.next_workspace.store(1, Ordering::Relaxed);
    }

    /// Simulate an account-store outage: every trait method answers
    /// `Unavailable` until `set_unavailable(false)`.
    pub fn set_unavailable(&self, down: bool) {
        self.unavailable.store(down, Ordering::Relaxed);
    }

    /// Test seam: grant `account` a member row in `workspace` (ws 0 is
    /// refused — it can never be a tenant).
    pub fn add_member(&self, workspace: u64, account: u64, role: crate::Role) {
        assert_ne!(workspace, 0, "ws 0 is the ops plane");
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.members.insert(
            (workspace, account),
            MemberRow {
                role,
                since_ms: now_ms(),
            },
        );
    }

    /// Test seam: a live invite row for the `DURABLE_INVITE_ONLY` gate
    /// (S3-3 owns the real create path).
    pub fn add_invite(
        &self,
        workspace: u64,
        email: &str,
        role: crate::Role,
        token_hash: [u8; 32],
        expires_at_ms: i64,
    ) {
        assert_ne!(workspace, 0, "ws 0 is the ops plane");
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let id = self.next_invite.fetch_add(1, Ordering::Relaxed);
        inner.invites.insert(
            id,
            InviteRow {
                workspace,
                email: email.to_string(),
                role,
                public_id: uuid::Uuid::now_v7(),
                invited_by: 0,
                token_hash,
                expires_at_ms,
                created_at_ms: now_ms(),
                accepted: false,
                revoked: false,
            },
        );
    }

    /// Test seam: the pending email-token hash of `kind` for `email` —
    /// lets a flow test follow the mailed link without a real mailbox.
    pub fn pending_token(&self, email: &str, kind: &'static str) -> Option<[u8; 32]> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner
            .email_tokens
            .values()
            .find(|t| {
                t.kind == kind
                    && !t.consumed
                    && !t.superseded
                    && inner
                        .accounts
                        .get(&t.account)
                        .is_some_and(|a| a.email == email)
            })
            .map(|t| t.hash)
    }

    /// Test seam: the outbox rows, oldest first.
    pub fn outbox(&self) -> Vec<OutboxRow> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut rows: Vec<OutboxRow> = inner.outbox.values().cloned().collect();
        rows.sort_by_key(|r| r.mail.idempotency_key.clone());
        rows
    }

    fn check_up(&self) -> Result<(), AccountError> {
        if self.unavailable.load(Ordering::Relaxed) {
            return Err(AccountError::Unavailable(
                "memory store marked unavailable".into(),
            ));
        }
        Ok(())
    }

    fn workspace_row(&self, id: u64) -> Result<Option<Workspace>, AccountError> {
        self.check_up()?;
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner.workspaces.get(&id).cloned())
    }
}

#[async_trait::async_trait]
impl AccountStore for Memory {
    async fn verify_key(&self, key_hash: &[u8; 32]) -> Result<Option<KeyGrant>, AccountError> {
        self.check_up()?;
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let grant = inner
            .keys
            .values()
            .find(|row| !row.revoked && row.hash == *key_hash)
            .map(|row| KeyGrant {
                workspace: row.workspace,
                permission: row.permission,
            });
        Ok(grant)
    }

    async fn workspace_by_id(&self, id: u64) -> Result<Option<Workspace>, AccountError> {
        self.workspace_row(id)
    }

    async fn workspace_by_public_id(
        &self,
        public_id: uuid::Uuid,
    ) -> Result<Option<Workspace>, AccountError> {
        self.check_up()?;
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner
            .workspaces
            .values()
            .find(|ws| ws.public_id == public_id)
            .cloned())
    }

    async fn create_workspace(&self, name: &str) -> Result<Workspace, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let id = self.next_workspace.fetch_add(1, Ordering::Relaxed);
        let workspace = Workspace {
            id,
            public_id: uuid::Uuid::now_v7(),
            name: name.to_string(),
            handler_secret: Self::draw_secret()?,
            deleted_at_ms: None,
        };
        inner.workspaces.insert(id, workspace.clone());
        Ok(workspace)
    }

    async fn list_workspaces(
        &self,
        limit: u32,
        after: Option<u64>,
    ) -> Result<Vec<Workspace>, AccountError> {
        self.check_up()?;
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<Workspace> = inner
            .workspaces
            .values()
            .filter(|ws| after.is_none_or(|a| ws.id > a))
            .cloned()
            .collect();
        out.sort_by_key(|ws| ws.id);
        out.truncate(limit as usize);
        Ok(out)
    }

    async fn create_key(
        &self,
        workspace: u64,
        name: &str,
        permission: Permission,
        key_hash: &[u8; 32],
        prefix: &str,
        created_by: Option<u64>,
    ) -> Result<ApiKeyInfo, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if !inner.workspaces.contains_key(&workspace) {
            return Err(AccountError::Store(format!(
                "workspace {workspace} does not exist"
            )));
        }
        let live = inner
            .keys
            .values()
            .filter(|k| k.workspace == workspace && !k.revoked)
            .count() as u64;
        if live >= crate::STOPGAP_KEYS_PER_WS {
            return Err(AccountError::PlanLimit("api_keys"));
        }
        let id = self.next_key.fetch_add(1, Ordering::Relaxed);
        let public_id = uuid::Uuid::now_v7();
        inner.keys.insert(
            id,
            KeyRow {
                workspace,
                public_id,
                name: name.to_string(),
                hash: *key_hash,
                prefix: prefix.to_string(),
                permission,
                revoked: false,
                last_used_at_ms: None,
                created_by,
                created_at_ms: now_ms(),
            },
        );
        Ok(inner
            .keys
            .get(&id)
            .map(|row| row.info(id, &inner))
            .unwrap_or_else(|| panic!("key {id}")))
    }

    async fn revoke_key(
        &self,
        workspace: u64,
        key_id: u64,
    ) -> Result<Option<[u8; 32]>, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let Some(row) = inner.keys.get_mut(&key_id) else {
            return Ok(None);
        };
        if row.workspace != workspace || row.revoked {
            return Ok(None);
        }
        row.revoked = true;
        Ok(Some(row.hash))
    }

    async fn list_keys(&self, workspace: u64) -> Result<Vec<ApiKeyInfo>, AccountError> {
        self.check_up()?;
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut keys: Vec<ApiKeyInfo> = inner
            .keys
            .iter()
            .filter(|(_, row)| row.workspace == workspace)
            .map(|(id, row)| row.info(*id, &inner))
            .collect();
        keys.sort_by_key(|info| info.id);
        Ok(keys)
    }

    async fn key_by_public_id(
        &self,
        workspace: u64,
        public_id: uuid::Uuid,
    ) -> Result<Option<ApiKeyInfo>, AccountError> {
        self.check_up()?;
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner
            .keys
            .iter()
            .find(|(_, row)| {
                row.workspace == workspace && row.public_id == public_id && !row.revoked
            })
            .map(|(id, row)| row.info(*id, &inner)))
    }

    async fn rotate_key(
        &self,
        workspace: u64,
        key_id: u64,
        new_key_hash: &[u8; 32],
        new_prefix: &str,
        created_by: Option<u64>,
    ) -> Result<Option<(ApiKeyInfo, [u8; 32])>, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let Some(old) = inner.keys.get(&key_id) else {
            return Ok(None);
        };
        if old.workspace != workspace || old.revoked {
            return Ok(None);
        }
        let (name, permission, old_hash) = (old.name.clone(), old.permission, old.hash);
        inner
            .keys
            .get_mut(&key_id)
            .unwrap_or_else(|| panic!("key {key_id}"))
            .revoked = true;
        let id = self.next_key.fetch_add(1, Ordering::Relaxed);
        let public_id = uuid::Uuid::now_v7();
        inner.keys.insert(
            id,
            KeyRow {
                workspace,
                public_id,
                name: name.clone(),
                hash: *new_key_hash,
                prefix: new_prefix.to_string(),
                permission,
                revoked: false,
                last_used_at_ms: None,
                created_by,
                created_at_ms: now_ms(),
            },
        );
        let info = inner
            .keys
            .get(&id)
            .map(|row| row.info(id, &inner))
            .unwrap_or_else(|| panic!("key {id}"));
        Ok(Some((info, old_hash)))
    }

    async fn touch_keys(&self, touches: &[crate::KeyTouch]) -> Result<(), AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        for t in touches {
            if let Some(row) = inner
                .keys
                .values_mut()
                .find(|r| !r.revoked && r.hash == t.key_hash)
            {
                row.last_used_at_ms =
                    Some(row.last_used_at_ms.map_or(t.seen_ms, |m| m.max(t.seen_ms)));
            }
        }
        Ok(())
    }

    async fn list_handlers(
        &self,
        workspace: u64,
    ) -> Result<Vec<HandlerRegistration>, AccountError> {
        self.check_up()?;
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut handlers: Vec<HandlerRegistration> = inner
            .handlers
            .iter()
            .filter(|((ws, _), _)| *ws == workspace)
            .map(|((_, name), row)| HandlerRegistration {
                name: name.clone(),
                url: row.url.clone(),
                last_test_at_ms: row.last_test_at_ms,
                last_test_status: row.last_test_status.clone(),
                last_test_latency_ms: row.last_test_latency_ms,
            })
            .collect();
        handlers.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(handlers)
    }

    async fn put_handler(&self, workspace: u64, name: &str, url: &str) -> Result<(), AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        // Same atomic bound as pg: count and insert under the one lock.
        let key = (workspace, name.to_string());
        let count = inner
            .handlers
            .keys()
            .filter(|(ws, _)| *ws == workspace)
            .count();
        if !inner.handlers.contains_key(&key) && count >= crate::MAX_HANDLERS_PER_WS {
            return Err(AccountError::RegistryFull);
        }
        inner
            .handlers
            .entry(key)
            .and_modify(|row| row.url = url.to_string())
            .or_insert(HandlerRow {
                url: url.to_string(),
                last_test_at_ms: None,
                last_test_status: None,
                last_test_latency_ms: None,
            });
        Ok(())
    }

    async fn record_handler_test(
        &self,
        workspace: u64,
        name: &str,
        at_ms: i64,
        status: &str,
        latency_ms: i64,
    ) -> Result<bool, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let Some(row) = inner.handlers.get_mut(&(workspace, name.to_string())) else {
            return Ok(false);
        };
        row.last_test_at_ms = Some(at_ms);
        row.last_test_status = Some(status.to_string());
        row.last_test_latency_ms = Some(latency_ms);
        Ok(true)
    }

    async fn delete_handler(&self, workspace: u64, name: &str) -> Result<bool, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner
            .handlers
            .remove(&(workspace, name.to_string()))
            .is_some())
    }

    async fn rotate_handler_secret(
        &self,
        workspace: u64,
    ) -> Result<Option<[u8; 32]>, AccountError> {
        self.check_up()?;
        let secret = Self::draw_secret()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let Some(ws) = inner.workspaces.get_mut(&workspace) else {
            return Ok(None);
        };
        ws.handler_secret = secret;
        Ok(Some(secret))
    }

    async fn signup(
        &self,
        email: &str,
        password_hash: &str,
        token_hash: &[u8; 32],
        token_expires_at_ms: i64,
        verify_mail: &Mail,
        notice_mail: &Mail,
    ) -> Result<SignupOutcome, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        // One mutex = the whole transaction; memory needs no advisory
        // lock to make the same interleavings impossible.
        let existing = inner.accounts.values().find(|a| a.email == email).cloned();
        match existing {
            Some(acct) if acct.email_verified => {
                let outbox = Self::enqueue(&mut inner, &self.next_outbox, notice_mail);
                Ok(SignupOutcome::Taken { outbox })
            }
            Some(acct) => {
                // Re-sign-up of an unverified address (review #2): the
                // new password replaces the pending one and every live
                // session dies — a pre-registration attacker's
                // credential and sessions lose here, not at verify.
                for row in inner.sessions.values_mut() {
                    if row.account == acct.id {
                        row.revoked = true;
                    }
                }
                if let Some(row) = inner.accounts.get_mut(&acct.id) {
                    row.password_hash = Some(password_hash.to_string());
                }
                let outbox = self.issue_verify(
                    &mut inner,
                    acct.id,
                    token_hash,
                    token_expires_at_ms,
                    Some(password_hash.to_string()),
                    verify_mail,
                );
                Ok(SignupOutcome::VerifyResent {
                    account: acct.id,
                    outbox,
                })
            }
            None => {
                let id = self.next_account.fetch_add(1, Ordering::Relaxed);
                inner.accounts.insert(
                    id,
                    Account {
                        id,
                        public_id: uuid::Uuid::now_v7(),
                        email: email.to_string(),
                        email_verified: false,
                        password_hash: Some(password_hash.to_string()),
                    },
                );
                let outbox = self.issue_verify(
                    &mut inner,
                    id,
                    token_hash,
                    token_expires_at_ms,
                    Some(password_hash.to_string()),
                    verify_mail,
                );
                Ok(SignupOutcome::Created { outbox })
            }
        }
    }

    async fn account_by_email(&self, email: &str) -> Result<Option<Account>, AccountError> {
        self.check_up()?;
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner.accounts.values().find(|a| a.email == email).cloned())
    }

    async fn session_grant(
        &self,
        token_hash: &[u8; 32],
    ) -> Result<Option<SessionGrant>, AccountError> {
        self.check_up()?;
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let Some((id, row)) = inner
            .sessions
            .iter()
            .find(|(_, s)| !s.revoked && s.token_hash == *token_hash)
        else {
            return Ok(None);
        };
        let Some(account) = inner.accounts.get(&row.account).cloned() else {
            // A session pointing at a deleted account is dead, not a
            // grant — the join in pg yields no row either.
            return Ok(None);
        };
        Ok(Some(SessionGrant {
            session_id: *id,
            public_id: row.public_id,
            csrf_seed: row.csrf_seed,
            expires_at_ms: row.expires_at_ms,
            idle_expires_at_ms: row.idle_expires_at_ms,
            account,
        }))
    }

    async fn create_session(&self, session: &crate::NewSession) -> Result<(), AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        // LRU eviction at the cap: the least-idle live sessions lose
        // their seats so this sign-in always lands (D-064).
        let mut live: Vec<u64> = inner
            .sessions
            .iter()
            .filter(|(_, s)| !s.revoked && s.account == session.account)
            .map(|(id, _)| *id)
            .collect();
        live.sort_by_key(|id| std::cmp::Reverse(inner.sessions[id].idle_expires_at_ms));
        for evict in live.iter().skip(crate::MAX_SESSIONS_PER_ACCOUNT - 1) {
            if let Some(row) = inner.sessions.get_mut(evict) {
                row.revoked = true;
            }
        }
        let id = self.next_session.fetch_add(1, Ordering::Relaxed);
        let now = now_ms();
        inner.sessions.insert(
            id,
            SessionRow {
                account: session.account,
                public_id: uuid::Uuid::now_v7(),
                token_hash: session.token_hash,
                csrf_seed: session.csrf_seed,
                expires_at_ms: session.expires_at_ms,
                idle_expires_at_ms: session.idle_expires_at_ms,
                created_at_ms: now,
                last_seen_at_ms: now,
                user_agent: session.user_agent.clone(),
                ip: session.ip.map(|a| a.to_string()),
                replaces: session.replaces,
                revoked: false,
            },
        );
        Ok(())
    }

    async fn revoke_session(&self, token_hash: &[u8; 32]) -> Result<bool, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let Some(row) = inner
            .sessions
            .values_mut()
            .find(|s| !s.revoked && s.token_hash == *token_hash)
        else {
            return Ok(false);
        };
        row.revoked = true;
        Ok(true)
    }

    async fn revoke_sessions_for_account(&self, account: u64) -> Result<(), AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        for row in inner.sessions.values_mut() {
            if row.account == account {
                row.revoked = true;
            }
        }
        Ok(())
    }

    async fn sessions_for_account(
        &self,
        account: u64,
        limit: u32,
    ) -> Result<Vec<crate::SessionInfo>, AccountError> {
        self.check_up()?;
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut rows: Vec<crate::SessionInfo> = inner
            .sessions
            .values()
            .filter(|s| !s.revoked && s.account == account)
            .map(|s| crate::SessionInfo {
                public_id: s.public_id,
                created_at_ms: s.created_at_ms,
                last_seen_at_ms: s.last_seen_at_ms,
                expires_at_ms: s.expires_at_ms,
                idle_expires_at_ms: s.idle_expires_at_ms,
                user_agent: s.user_agent.clone(),
                ip: s.ip.clone(),
            })
            .collect();
        rows.sort_by_key(|s| std::cmp::Reverse(s.created_at_ms));
        rows.truncate(limit as usize);
        Ok(rows)
    }

    async fn revoke_session_public(
        &self,
        account: u64,
        public_id: uuid::Uuid,
    ) -> Result<Option<[u8; 32]>, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let Some(row) = inner.sessions.values_mut().find(|s| {
            !s.revoked
                && s.public_id == public_id
                // ATTACK(session-idor)
                && s.account == account
        }) else {
            return Ok(None);
        };
        row.revoked = true;
        Ok(Some(row.token_hash))
    }

    async fn revoke_sessions_except(
        &self,
        account: u64,
        keep_public_id: uuid::Uuid,
    ) -> Result<u64, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut n = 0u64;
        for row in inner.sessions.values_mut() {
            if row.account == account && row.public_id != keep_public_id && !row.revoked {
                row.revoked = true;
                n += 1;
            }
        }
        Ok(n)
    }

    async fn change_password(
        &self,
        account: u64,
        new_password_hash: &str,
    ) -> Result<(), AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(acct) = inner.accounts.get_mut(&account) {
            acct.password_hash = Some(new_password_hash.to_string());
        }
        // A pending token minted before the change carries a stale
        // pending hash — it must never land over the new password.
        for row in inner.email_tokens.values_mut() {
            if row.account == account && !row.consumed && !row.superseded {
                row.superseded = true;
            }
        }
        for row in inner.sessions.values_mut() {
            if row.account == account {
                row.revoked = true;
            }
        }
        Ok(())
    }

    async fn touch_sessions(&self, touches: &[crate::SessionTouch]) -> Result<(), AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        for touch in touches {
            if let Some(row) = inner
                .sessions
                .values_mut()
                .find(|s| !s.revoked && s.token_hash == touch.token_hash)
            {
                row.last_seen_at_ms = row.last_seen_at_ms.max(touch.seen_ms);
                row.idle_expires_at_ms = row.idle_expires_at_ms.max(touch.idle_expires_at_ms);
            }
        }
        Ok(())
    }

    async fn member_role(
        &self,
        account: u64,
        workspace: u64,
    ) -> Result<Option<crate::Role>, AccountError> {
        self.check_up()?;
        if workspace == 0 {
            return Ok(None);
        }
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner.members.get(&(workspace, account)).map(|m| m.role))
    }

    async fn create_workspace_for(
        &self,
        account: u64,
        name: &str,
    ) -> Result<Workspace, AccountError> {
        // One atomic write: the row and its owner member row land or
        // neither does — an ownerless workspace can never exist.
        // The stopgap cap counts every owned row — a soft-deleted ws
        // keeps its seat until the purge (S2-2).
        {
            let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let owned = inner
                .members
                .iter()
                .filter(|((_, acc), m)| *acc == account && m.role == crate::Role::Owner)
                .count() as u64;
            if owned >= crate::STOPGAP_WS_PER_ACCOUNT {
                return Err(AccountError::PlanLimit("workspaces_per_account"));
            }
        }
        let workspace = self.create_workspace(name).await?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.members.insert(
            (workspace.id, account),
            MemberRow {
                role: crate::Role::Owner,
                since_ms: now_ms(),
            },
        );
        Ok(workspace)
    }

    async fn rename_workspace(&self, id: u64, name: &str) -> Result<bool, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let Some(ws) = inner.workspaces.get_mut(&id) else {
            return Ok(false);
        };
        ws.name = name.to_string();
        Ok(true)
    }

    async fn delete_workspace(&self, id: u64, now_ms: i64) -> Result<bool, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let Some(ws) = inner.workspaces.get_mut(&id) else {
            return Ok(false);
        };
        ws.deleted_at_ms = Some(now_ms);
        // Keys die in the same breath (D-072): the very next verify
        // finds the row revoked — no cache may outlive the delete.
        for row in inner.keys.values_mut() {
            if row.workspace == id {
                row.revoked = true;
            }
        }
        Ok(true)
    }

    async fn workspaces_for_account(
        &self,
        account: u64,
    ) -> Result<Vec<(Workspace, crate::Role)>, AccountError> {
        self.check_up()?;
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<(Workspace, crate::Role)> = inner
            .members
            .iter()
            .filter(|((_, a), _)| *a == account)
            .filter_map(|((w, _), m)| {
                inner
                    .workspaces
                    .get(w)
                    .filter(|ws| ws.deleted_at_ms.is_none())
                    .map(|ws| (ws.clone(), m.role))
            })
            .collect();
        out.sort_by_key(|(ws, _)| ws.id);
        Ok(out)
    }

    async fn list_members(&self, workspace: u64) -> Result<Vec<crate::MemberInfo>, AccountError> {
        self.check_up()?;
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<crate::MemberInfo> = inner
            .members
            .iter()
            .filter(|((w, _), _)| *w == workspace)
            .filter_map(|((_, a), m)| {
                inner.accounts.get(a).map(|acc| crate::MemberInfo {
                    account_public_id: acc.public_id,
                    email: acc.email.clone(),
                    role: m.role,
                    since_ms: m.since_ms,
                })
            })
            .collect();
        out.sort_by_key(|m| m.since_ms);
        Ok(out)
    }

    async fn set_member_role(
        &self,
        workspace: u64,
        account: u64,
        role: crate::Role,
    ) -> Result<crate::MemberChange, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let Some(m) = inner.members.get_mut(&(workspace, account)) else {
            return Ok(crate::MemberChange::NotFound);
        };
        if m.role == crate::Role::Owner {
            return Ok(crate::MemberChange::LastOwner);
        }
        m.role = role;
        Ok(crate::MemberChange::Changed)
    }

    async fn remove_member(
        &self,
        workspace: u64,
        account: u64,
    ) -> Result<crate::MemberChange, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match inner.members.get(&(workspace, account)) {
            None => return Ok(crate::MemberChange::NotFound),
            Some(m) if m.role == crate::Role::Owner => return Ok(crate::MemberChange::LastOwner),
            _ => {}
        }
        inner.members.remove(&(workspace, account));
        Ok(crate::MemberChange::Changed)
    }

    async fn account_by_public_id(
        &self,
        public_id: uuid::Uuid,
    ) -> Result<Option<Account>, AccountError> {
        self.check_up()?;
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner
            .accounts
            .values()
            .find(|a| a.public_id == public_id)
            .cloned())
    }

    async fn create_invite(
        &self,
        workspace: u64,
        email: &str,
        role: crate::Role,
        invited_by: u64,
        token_hash: &[u8; 32],
        expires_at_ms: i64,
    ) -> Result<Option<crate::WorkspaceInvite>, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        // `invites_pending`: one live (unaccepted, unrevoked) invite per
        // (workspace, email) — expiry does not free the seat.
        let dup = inner
            .invites
            .values()
            .any(|i| i.workspace == workspace && i.email == email && !i.accepted && !i.revoked);
        if dup {
            return Ok(None);
        }
        let day_ago = now_ms() - 86_400_000;
        let today = inner
            .invites
            .values()
            .filter(|i| i.workspace == workspace && i.created_at_ms > day_ago)
            .count() as u64;
        if today >= crate::STOPGAP_INVITES_PER_WS_DAY {
            return Err(AccountError::PlanLimit("invites_per_day"));
        }
        let invite = crate::WorkspaceInvite {
            public_id: uuid::Uuid::now_v7(),
            email: email.to_string(),
            role,
            expires_at_ms,
        };
        let id = self.next_invite.fetch_add(1, Ordering::Relaxed);
        inner.invites.insert(
            id,
            InviteRow {
                workspace,
                email: email.to_string(),
                role,
                public_id: invite.public_id,
                invited_by,
                token_hash: *token_hash,
                expires_at_ms,
                created_at_ms: now_ms(),
                accepted: false,
                revoked: false,
            },
        );
        Ok(Some(invite))
    }

    async fn list_invites(
        &self,
        workspace: u64,
    ) -> Result<Vec<crate::WorkspaceInvite>, AccountError> {
        self.check_up()?;
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner
            .invites
            .values()
            .filter(|i| i.workspace == workspace && !i.accepted && !i.revoked)
            .map(|i| crate::WorkspaceInvite {
                public_id: i.public_id,
                email: i.email.clone(),
                role: i.role,
                expires_at_ms: i.expires_at_ms,
            })
            .collect())
    }

    async fn revoke_invite(
        &self,
        workspace: u64,
        public_id: uuid::Uuid,
    ) -> Result<bool, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let Some(row) = inner
            .invites
            .values_mut()
            .find(|i| i.public_id == public_id && i.workspace == workspace && !i.accepted)
        else {
            return Ok(false);
        };
        row.revoked = true;
        Ok(true)
    }

    async fn accept_invite(
        &self,
        token_hash: &[u8; 32],
        account: u64,
        now_ms: i64,
    ) -> Result<Option<(u64, crate::Role)>, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let Some(row) = inner
            .invites
            .values_mut()
            .find(|i| i.token_hash == *token_hash && !i.accepted && !i.revoked)
        else {
            return Ok(None);
        };
        if row.expires_at_ms <= now_ms {
            return Ok(None);
        }
        row.accepted = true;
        let (ws, role) = (row.workspace, row.role);
        // ON CONFLICT DO NOTHING: an existing member row keeps its role.
        inner.members.entry((ws, account)).or_insert(MemberRow {
            role,
            since_ms: now_ms,
        });
        Ok(Some((ws, role)))
    }

    async fn decline_invite(
        &self,
        token_hash: &[u8; 32],
        _now_ms: i64,
    ) -> Result<bool, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let Some(row) = inner
            .invites
            .values_mut()
            .find(|i| i.token_hash == *token_hash && !i.accepted && !i.revoked)
        else {
            return Ok(false);
        };
        row.revoked = true;
        Ok(true)
    }

    async fn consume_verify_token(
        &self,
        token_hash: &[u8; 32],
    ) -> Result<Option<Account>, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = now_ms();
        // One live check covers every dead state identically — unknown,
        // consumed, superseded, expired and wrong-kind all answer None.
        let Some(token) = inner.email_tokens.values_mut().find(|t| {
            t.hash == *token_hash
                    // ATTACK(token-kind-confusion)
                    && t.kind == "verify"
                    // ATTACK(token-replay)
                    && !t.consumed
                    && !t.superseded
                    && t.expires_at_ms > now
        }) else {
            return Ok(None);
        };
        token.consumed = true;
        let account = token.account;
        let pending = token.password_hash.clone();
        if let Some(acct) = inner.accounts.get_mut(&account) {
            if let Some(phc) = pending {
                acct.password_hash = Some(phc);
            }
            acct.email_verified = true;
        }
        // The mailbox owner's proof kills every session the account
        // held — a pre-registration attacker's die here (review #2).
        // ATTACK(prereg-takeover)
        for row in inner.sessions.values_mut() {
            if row.account == account {
                row.revoked = true;
            }
        }
        Ok(inner.accounts.get(&account).cloned())
    }

    async fn consume_reset_token(
        &self,
        token_hash: &[u8; 32],
        new_password_hash: &str,
    ) -> Result<Option<Account>, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = now_ms();
        let Some(token) = inner.email_tokens.values_mut().find(|t| {
            t.hash == *token_hash
                && t.kind == "reset"
                // ATTACK(reset-token-replay)
                && !t.consumed
                && !t.superseded
                && t.expires_at_ms > now
        }) else {
            return Ok(None);
        };
        token.consumed = true;
        let account = token.account;
        if let Some(acct) = inner.accounts.get_mut(&account) {
            acct.password_hash = Some(new_password_hash.to_string());
            // Holding the mailed token proves mailbox ownership — the
            // same proof standard as the verify link.
            acct.email_verified = true;
        }
        // Every other pending token of the account is now dead weight:
        // a verify token's pending hash must never overwrite the reset.
        for row in inner.email_tokens.values_mut() {
            if row.account == account && !row.consumed && !row.superseded {
                row.superseded = true;
            }
        }
        for row in inner.sessions.values_mut() {
            if row.account == account {
                row.revoked = true;
            }
        }
        Ok(inner.accounts.get(&account).cloned())
    }

    async fn issue_reset_token(
        &self,
        email: &str,
        token_hash: &[u8; 32],
        expires_at_ms: i64,
        reset_mail: &Mail,
    ) -> Result<Option<u64>, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let Some(account) = inner
            .accounts
            .values()
            .find(|a| a.email == email)
            .map(|a| a.id)
        else {
            return Ok(None);
        };
        for row in inner.email_tokens.values_mut() {
            if row.account == account && row.kind == "reset" && !row.consumed && !row.superseded {
                row.superseded = true;
            }
        }
        let token_id = self.next_token.fetch_add(1, Ordering::Relaxed);
        inner.email_tokens.insert(
            token_id,
            EmailTokenRow {
                account,
                kind: "reset",
                hash: *token_hash,
                expires_at_ms,
                password_hash: None,
                consumed: false,
                superseded: false,
            },
        );
        Ok(Self::enqueue(&mut inner, &self.next_outbox, reset_mail))
    }

    async fn reissue_verify(
        &self,
        email: &str,
        token_hash: &[u8; 32],
        expires_at_ms: i64,
        verify_mail: &Mail,
    ) -> Result<Option<u64>, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let Some(account) = inner
            .accounts
            .values()
            .find(|a| a.email == email && !a.email_verified)
            .map(|a| (a.id, a.password_hash.clone()))
        else {
            return Ok(None);
        };
        Ok(self.issue_verify(
            &mut inner,
            account.0,
            token_hash,
            expires_at_ms,
            account.1,
            verify_mail,
        ))
    }

    async fn invite_grant(
        &self,
        token_hash: &[u8; 32],
        now_ms: i64,
    ) -> Result<Option<crate::InviteGrant>, AccountError> {
        self.check_up()?;
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner
            .invites
            .values()
            .find(|i| {
                i.token_hash == *token_hash && !i.accepted && !i.revoked && i.expires_at_ms > now_ms
            })
            .map(|i| crate::InviteGrant {
                workspace: i.workspace,
                email: i.email.clone(),
                role: i.role,
            }))
    }

    async fn enqueue_mail(&self, mail: &Mail) -> Result<Option<u64>, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(Self::enqueue(&mut inner, &self.next_outbox, mail))
    }

    async fn outbox_claim(
        &self,
        now_ms: i64,
        limit: u32,
        claim_ms: i64,
    ) -> Result<Vec<crate::OutboxMail>, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut due: Vec<u64> = inner
            .outbox
            .iter()
            .filter(|(id, r)| {
                let _ = id;
                (r.status == "pending" || r.status == "sending")
                    && r.next_attempt_at_ms <= now_ms
                    && r.attempts < crate::mail::OUTBOX_MAX_ATTEMPTS
            })
            .map(|(id, _)| *id)
            .collect();
        due.sort_unstable();
        due.truncate(limit as usize);
        let mut claimed = Vec::with_capacity(due.len());
        for id in due {
            let row = inner.outbox.get_mut(&id).unwrap_or_else(|| panic!("row"));
            if row.status == "sending" {
                row.attempts += 1; // a crash-reclaimed claim counts a lost attempt
            }
            row.status = "sending";
            row.next_attempt_at_ms = now_ms + claim_ms;
            claimed.push(crate::OutboxMail {
                id,
                attempts: row.attempts,
                mail: row.mail.clone(),
            });
        }
        Ok(claimed)
    }

    async fn outbox_sweep(&self, now_ms: i64) -> Result<u64, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut swept = 0;
        for row in inner.outbox.values_mut() {
            if (row.status == "pending" || row.status == "sending")
                && row.attempts >= crate::mail::OUTBOX_MAX_ATTEMPTS
            {
                row.status = "failed";
                if row.last_error.is_none() {
                    row.last_error = Some("attempts exhausted".into());
                }
                swept += 1;
            } else if (row.status == "pending" || row.status == "sending")
                && row.mail.expires_at_ms.is_some_and(|e| e <= now_ms)
            {
                row.status = "dropped";
                row.mail.text.clear();
                row.mail.html.clear();
                swept += 1;
            }
        }
        Ok(swept)
    }

    async fn outbox_purge(&self, now_ms: i64) -> Result<u64, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let before = inner.outbox.len();
        inner.outbox.retain(|_, r| {
            let keep_ms = if r.status == "sent" {
                24 * 3600 * 1000
            } else {
                7 * 24 * 3600 * 1000
            };
            !matches!(r.status, "sent" | "failed" | "dropped") || r.created_at_ms + keep_ms > now_ms
        });
        Ok((before - inner.outbox.len()) as u64)
    }

    async fn outbox_mark(&self, id: u64, outcome: MailOutcome) -> Result<(), AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let Some(row) = inner.outbox.get_mut(&id) else {
            return Ok(());
        };
        match outcome {
            MailOutcome::Sent => {
                if row.status == "pending" || row.status == "sending" {
                    row.status = "sent";
                }
            }
            MailOutcome::Retry {
                next_attempt_at_ms,
                error,
            } => {
                row.status = "pending";
                row.attempts += 1;
                row.next_attempt_at_ms = next_attempt_at_ms;
                row.last_error = Some(error);
            }
            MailOutcome::Failed { error } => {
                row.status = "failed";
                row.last_error = Some(error);
            }
        }
        Ok(())
    }
}

impl Memory {
    /// Token + mail for the verify flow — the same unit the pg impl
    /// runs inside its transaction. `pending_hash` is what a later
    /// `consume_verify_token` installs.
    fn issue_verify(
        &self,
        inner: &mut Inner,
        account: u64,
        token_hash: &[u8; 32],
        token_expires_at_ms: i64,
        pending_hash: Option<String>,
        verify_mail: &Mail,
    ) -> Option<u64> {
        for row in inner.email_tokens.values_mut() {
            if row.account == account && row.kind == "verify" && !row.consumed && !row.superseded {
                row.superseded = true;
            }
        }
        let token_id = self.next_token.fetch_add(1, Ordering::Relaxed);
        inner.email_tokens.insert(
            token_id,
            EmailTokenRow {
                account,
                kind: "verify",
                hash: *token_hash,
                expires_at_ms: token_expires_at_ms,
                password_hash: pending_hash,
                consumed: false,
                superseded: false,
            },
        );
        Self::enqueue(inner, &self.next_outbox, verify_mail)
    }

    /// Enqueue inside the caller's lock (the same atomicity unit the
    /// pg impl runs inside its transaction): strict single addr-spec,
    /// the per-(address,kind) hourly cap, and newest-replaces-pending
    /// do the dedupe — `None` means the mail coalesced while the fact
    /// it announced still stands.
    fn enqueue(inner: &mut Inner, next: &AtomicU64, mail: &Mail) -> Option<u64> {
        if mail.subject.len() > crate::mail::MAX_MAIL_SUBJECT
            || mail.text.len() > crate::mail::MAX_MAIL_BODY
            || mail.html.len() > crate::mail::MAX_MAIL_BODY
        {
            return None;
        }
        // ATTACK(addr-spec-injection)
        if !crate::mail::valid_addr_spec(&mail.send_to) {
            return None;
        }
        let now = now_ms();
        // Newest replaces pending: an unclaimed row for the same
        // (address, kind) carries a dead link — the fresh mail takes
        // its slot (and the hourly counter keeps the window count).
        for row in inner.outbox.values_mut() {
            if row.status == "pending" && row.send_to == mail.send_to && row.kind == mail.kind {
                row.mail = mail.clone();
                row.attempts = 0;
                row.next_attempt_at_ms = now;
                row.last_error = None;
                return None;
            }
        }
        // The hourly cap counts rows created inside the window in ANY
        // state — a spam loop cannot trade deletes for more sends.
        let in_window = inner
            .outbox
            .values()
            .filter(|r| {
                r.send_to == mail.send_to
                    && r.kind == mail.kind
                    && r.created_at_ms > now - 3_600_000
            })
            .count() as i64;
        // ATTACK(mail-budget)
        if in_window >= crate::mail::MAX_HOURLY_PER_ADDR {
            return None;
        }
        // The provider-level dedupe: one live row per idempotency key.
        if inner
            .outbox
            .values()
            .any(|r| r.mail.idempotency_key == mail.idempotency_key)
        {
            return None;
        }
        let id = next.fetch_add(1, Ordering::Relaxed);
        inner.outbox.insert(
            id,
            OutboxRow {
                kind: mail.kind,
                send_to: mail.send_to.clone(),
                status: "pending",
                attempts: 0,
                next_attempt_at_ms: now,
                created_at_ms: now,
                last_error: None,
                mail: mail.clone(),
            },
        );
        Some(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(seed: u8) -> [u8; 32] {
        [seed; 32]
    }

    #[tokio::test]
    async fn the_default_workspace_exists_at_zero() {
        let store = Memory::new();
        let ws = store.workspace_by_id(0).await;
        assert!(matches!(ws, Ok(Some(Workspace { id: 0, .. }))));
    }

    #[tokio::test]
    async fn a_created_key_verifies_and_revokes() {
        let store = Memory::new();
        let ws = store
            .create_workspace("alpha")
            .await
            .unwrap_or_else(|e| panic!("create: {e}"));
        let info = store
            .create_key(ws.id, "ci", Permission::Write, &hash(7), "dr_ci", None)
            .await
            .unwrap_or_else(|e| panic!("key: {e}"));
        let grant = store
            .verify_key(&hash(7))
            .await
            .unwrap_or_else(|e| panic!("verify: {e}"));
        assert_eq!(
            grant,
            Some(KeyGrant {
                workspace: ws.id,
                permission: Permission::Write
            })
        );
        assert!(store
            .revoke_key(ws.id, info.id)
            .await
            .unwrap_or_else(|e| panic!("revoke: {e}"))
            .is_some());
        assert_eq!(
            store
                .verify_key(&hash(7))
                .await
                .unwrap_or_else(|e| panic!("verify: {e}")),
            None
        );
    }

    /// Review S2 #6: the per-workspace handler bound lives in the store
    /// itself so a racing pair of PUTs can never both pass it — here the
    /// bound counts the workspace's rows, and updating an existing name
    /// stays legal at the bound.
    #[tokio::test]
    async fn the_handler_bound_is_enforced_atomically() {
        let store = Memory::new();
        for i in 0..crate::MAX_HANDLERS_PER_WS {
            store
                .put_handler(1, &format!("h-{i}"), "https://a.test/x")
                .await
                .unwrap_or_else(|e| panic!("put {i}: {e}"));
        }
        assert!(matches!(
            store.put_handler(1, "h-extra", "https://a.test/x").await,
            Err(AccountError::RegistryFull)
        ));
        // An update of an existing name is not a new row — still allowed.
        store
            .put_handler(1, "h-0", "https://b.test/y")
            .await
            .unwrap_or_else(|e| panic!("update at the bound: {e}"));
        // A different workspace has its own bound.
        store
            .put_handler(2, "h-x", "https://a.test/x")
            .await
            .unwrap_or_else(|e| panic!("ws 2 is unaffected: {e}"));
    }

    #[tokio::test]
    async fn an_outage_reports_unavailable_not_denied() {
        let store = Memory::new();
        store.set_unavailable(true);
        let err = store.verify_key(&hash(1)).await;
        assert!(matches!(err, Err(AccountError::Unavailable(_))));
        store.set_unavailable(false);
        assert_eq!(
            store
                .verify_key(&hash(1))
                .await
                .unwrap_or_else(|e| panic!("verify: {e}")),
            None
        );
    }

    #[tokio::test]
    async fn handlers_are_scoped_per_workspace() {
        let store = Memory::new();
        let a = store
            .create_workspace("a")
            .await
            .unwrap_or_else(|e| panic!("a: {e}"));
        let b = store
            .create_workspace("b")
            .await
            .unwrap_or_else(|e| panic!("b: {e}"));
        store
            .put_handler(a.id, "h", "https://a.example/h")
            .await
            .unwrap_or_else(|e| panic!("put: {e}"));
        store
            .put_handler(b.id, "h", "https://b.example/h")
            .await
            .unwrap_or_else(|e| panic!("put: {e}"));
        let a_handlers = store
            .list_handlers(a.id)
            .await
            .unwrap_or_else(|e| panic!("list: {e}"));
        assert_eq!(
            a_handlers,
            vec![HandlerRegistration {
                name: "h".into(),
                url: "https://a.example/h".into(),
                last_test_at_ms: None,
                last_test_status: None,
                last_test_latency_ms: None,
            }]
        );
        assert!(store
            .delete_handler(a.id, "h")
            .await
            .unwrap_or_else(|e| panic!("del: {e}")));
        assert!(store
            .list_handlers(a.id)
            .await
            .unwrap_or_else(|e| panic!("list: {e}"))
            .is_empty());
        assert_eq!(
            store
                .list_handlers(b.id)
                .await
                .unwrap_or_else(|e| panic!("list: {e}"))
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn public_ids_are_opaque_and_unique() {
        let store = Memory::new();
        let a = store
            .create_workspace("a")
            .await
            .unwrap_or_else(|e| panic!("a: {e}"));
        let b = store
            .create_workspace("b")
            .await
            .unwrap_or_else(|e| panic!("b: {e}"));
        assert_ne!(a.public_id, b.public_id);
        let found = store
            .workspace_by_public_id(a.public_id)
            .await
            .unwrap_or_else(|e| panic!("lookup: {e}"));
        assert_eq!(found.map(|w| w.id), Some(a.id));
        assert_eq!(
            store
                .workspace_by_public_id(uuid::Uuid::from_u128(0xdeadbeef))
                .await
                .unwrap_or_else(|e| panic!("none: {e}")),
            None
        );
    }
}
