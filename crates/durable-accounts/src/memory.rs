use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

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
    next_outbox: AtomicU64,
    unavailable: AtomicBool,
}

#[derive(Default)]
struct Inner {
    workspaces: HashMap<u64, Workspace>,
    keys: HashMap<u64, KeyRow>,
    handlers: HashMap<(u64, String), String>,
    accounts: HashMap<u64, Account>,
    /// (workspace, account) -> role. ws 0 rows are never written — the
    /// ops plane is key-only.
    members: HashMap<(u64, u64), crate::Role>,
    sessions: HashMap<u64, SessionRow>,
    email_tokens: HashMap<u64, EmailTokenRow>,
    outbox: HashMap<u64, OutboxRow>,
}

struct KeyRow {
    workspace: u64,
    name: String,
    hash: [u8; 32],
    prefix: String,
    permission: Permission,
    revoked: bool,
}

struct SessionRow {
    account: u64,
    public_id: uuid::Uuid,
    token_hash: [u8; 32],
    csrf_seed: [u8; 32],
    expires_at_ms: i64,
    idle_expires_at_ms: i64,
    revoked: bool,
}

struct EmailTokenRow {
    account: u64,
    kind: &'static str,
    hash: [u8; 32],
    /// Mirrors the `expires_at` column; the verify/consume path reads
    /// it when S3-1 lands token consumption.
    #[expect(dead_code, reason = "read by the verify path in S3-1")]
    expires_at_ms: i64,
    /// The pending password hash verify installs (review #2).
    #[expect(dead_code, reason = "read by the verify path in S3-1")]
    password_hash: String,
    consumed: bool,
    superseded: bool,
}

#[derive(Debug, Clone)]
pub struct OutboxRow {
    pub kind: &'static str,
    pub send_to: String,
    pub status: &'static str,
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
        inner.members.insert((workspace, account), role);
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
    ) -> Result<ApiKeyInfo, AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if !inner.workspaces.contains_key(&workspace) {
            return Err(AccountError::Store(format!(
                "workspace {workspace} does not exist"
            )));
        }
        let id = self.next_key.fetch_add(1, Ordering::Relaxed);
        inner.keys.insert(
            id,
            KeyRow {
                workspace,
                name: name.to_string(),
                hash: *key_hash,
                prefix: prefix.to_string(),
                permission,
                revoked: false,
            },
        );
        Ok(ApiKeyInfo {
            id,
            name: name.to_string(),
            prefix: prefix.to_string(),
            permission,
            revoked: false,
        })
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
            .map(|(id, row)| ApiKeyInfo {
                id: *id,
                name: row.name.clone(),
                prefix: row.prefix.clone(),
                permission: row.permission,
                revoked: row.revoked,
            })
            .collect();
        keys.sort_by_key(|info| info.id);
        Ok(keys)
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
            .map(|((_, name), url)| HandlerRegistration {
                name: name.clone(),
                url: url.clone(),
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
        inner.handlers.insert(key, url.to_string());
        Ok(())
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
                    password_hash,
                    verify_mail,
                );
                Ok(SignupOutcome::VerifyResent { outbox })
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
                    password_hash,
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
        // their seats so this sign-in always lands (D-060).
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
        inner.sessions.insert(
            id,
            SessionRow {
                account: session.account,
                public_id: uuid::Uuid::now_v7(),
                token_hash: session.token_hash,
                csrf_seed: session.csrf_seed,
                expires_at_ms: session.expires_at_ms,
                idle_expires_at_ms: session.idle_expires_at_ms,
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
        Ok(inner.members.get(&(workspace, account)).copied())
    }

    async fn outbox_mark(&self, id: u64, outcome: MailOutcome) -> Result<(), AccountError> {
        self.check_up()?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let Some(row) = inner.outbox.get_mut(&id) else {
            return Ok(());
        };
        match outcome {
            MailOutcome::Sent => {
                if row.status == "pending" {
                    row.status = "sent";
                }
            }
            MailOutcome::Retry { .. } => {
                // Memory rows never retry — the row stays pending and a
                // test inspects it directly.
            }
        }
        Ok(())
    }
}

impl Memory {
    /// Token + mail for the verify flow — the same unit the pg impl
    /// runs inside its transaction.
    fn issue_verify(
        &self,
        inner: &mut Inner,
        account: u64,
        token_hash: &[u8; 32],
        token_expires_at_ms: i64,
        password_hash: &str,
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
                password_hash: password_hash.to_string(),
                consumed: false,
                superseded: false,
            },
        );
        Self::enqueue(inner, &self.next_outbox, verify_mail)
    }

    /// The per-address pending cap: at it, the mail coalesces into the
    /// pending rows it would duplicate — `None`, and the outbox stays
    /// bounded. The fact the mail announced has already committed.
    fn enqueue(inner: &mut Inner, next: &AtomicU64, mail: &Mail) -> Option<u64> {
        let pending = inner
            .outbox
            .values()
            .filter(|r| r.status == "pending" && r.send_to == mail.send_to)
            .count() as i64;
        if pending >= crate::mail::MAX_PENDING_PER_ADDR {
            return None;
        }
        let id = next.fetch_add(1, Ordering::Relaxed);
        inner.outbox.insert(
            id,
            OutboxRow {
                kind: mail.kind,
                send_to: mail.send_to.clone(),
                status: "pending",
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
            .create_key(ws.id, "ci", Permission::Write, &hash(7), "dr_ci")
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
                url: "https://a.example/h".into()
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
