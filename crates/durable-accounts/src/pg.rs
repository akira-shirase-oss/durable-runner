use std::str::FromStr;
use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use sqlx::{PgPool, Row};

use crate::{
    Account, AccountError, AccountStore, ApiKeyInfo, HandlerRegistration, KeyGrant, Mail,
    MailOutcome, Permission, SessionGrant, SignupOutcome, Workspace, STORE_DEADLINE,
};

/// Unix-millis ↔ TIMESTAMPTZ. The trait speaks millis so the store
/// keeps the clock out of its signatures; a value outside TIMESTAMPTZ's
/// range saturates rather than erroring (deadline arithmetic overflow
/// is a config bug, not an outage).
fn ts(ms: i64) -> time::OffsetDateTime {
    time::OffsetDateTime::from_unix_timestamp_nanos(ms as i128 * 1_000_000).unwrap_or(
        time::OffsetDateTime::new_utc(time::Date::MAX, time::Time::MAX),
    )
}

fn ms(t: time::OffsetDateTime) -> i64 {
    (t.unix_timestamp_nanos() / 1_000_000) as i64
}

fn account_from(row: &sqlx::postgres::PgRow) -> Result<Account, AccountError> {
    Ok(Account {
        id: to_u64(row.try_get("id")?, "account")?,
        public_id: row.try_get("public_id")?,
        email: row.try_get("email")?,
        email_verified: row
            .try_get::<Option<time::OffsetDateTime>, _>("email_verified_at")?
            .is_some(),
        password_hash: row.try_get("password_hash")?,
    })
}

const POOL_SIZE: u32 = 5;
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);

/// Postgres-backed account store (Neon in production, any Postgres 13+
/// locally). The pool is created lazily so a store outage never blocks
/// server startup: the first failing query surfaces as `Unavailable`
/// and the pool reconnects on its own once Neon is back.
///
/// Queries use runtime `sqlx::query` (not the `query!` macros) so the
/// crate builds without a live `DATABASE_URL` at compile time.
pub struct Pg {
    pool: PgPool,
}

impl Pg {
    /// Create the store. The pool does not connect until first use; call
    /// `migrate` at startup to apply the schema (its failure should be
    /// logged and surfaced, not hidden). `Store` means the URL itself was
    /// unusable — a configuration error, distinct from an outage.
    ///
    /// TLS is part of the configuration contract (S2 review #5): a
    /// non-loopback host must use `sslmode=verify-full` — `require`
    /// encrypts but does not verify the server's certificate, leaving
    /// `handler_secret` and `verify_key` grants open to a forged
    /// endpoint. Literal loopback IPs (local development) are exempt.
    pub fn connect(url: &str) -> Result<Self, AccountError> {
        let opts = PgConnectOptions::from_str(url)
            .map_err(|e| AccountError::Store(format!("unusable DATABASE_URL: {e}")))?;
        let host = opts.get_host();
        let bare = host.trim_start_matches('[').trim_end_matches(']');
        // Only literal loopback IPs are exempt — "localhost" is a NAME
        // and resolves wherever the resolver says; a hosts-file edit or
        // split-horizon DNS would turn an exempted "localhost" URL into
        // plaintext to a remote host. Dev uses 127.0.0.1 or sets
        // sslmode explicitly.
        let loopback = bare
            .parse::<std::net::IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false);
        if !loopback && !matches!(opts.get_ssl_mode(), PgSslMode::VerifyFull) {
            return Err(AccountError::Store(format!(
                "DATABASE_URL for non-loopback host {host:?} must use \
                 sslmode=verify-full (got {:?}): the store carries secrets \
                 and grants — an unverified connection can be forged",
                opts.get_ssl_mode()
            )));
        }
        let pool = PgPoolOptions::new()
            .max_connections(POOL_SIZE)
            .acquire_timeout(ACQUIRE_TIMEOUT)
            .connect_lazy_with(opts);
        Ok(Self { pool })
    }

    /// Apply embedded migrations. Fails `Unavailable` when the store is
    /// unreachable so startup can report the real condition instead of
    /// serving an unmigrated schema silently.
    pub async fn migrate(&self) -> Result<(), AccountError> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

async fn bounded<T>(
    fut: impl std::future::Future<Output = Result<T, sqlx::Error>>,
) -> Result<T, AccountError> {
    match tokio::time::timeout(STORE_DEADLINE, fut).await {
        Ok(res) => res.map_err(AccountError::from),
        Err(_) => Err(AccountError::Unavailable(format!(
            "query exceeded {STORE_DEADLINE:?}"
        ))),
    }
}

/// Map one `api_keys` row (id, public_id, name, prefix, permission,
/// revoked, last_used_at, created_by selected) to `ApiKeyInfo`.
fn key_info_from(row: &sqlx::postgres::PgRow) -> Result<ApiKeyInfo, AccountError> {
    Ok(ApiKeyInfo {
        id: to_u64(row.try_get("id")?, "api key")?,
        public_id: row.try_get("public_id")?,
        name: row.try_get("name")?,
        prefix: row.try_get("prefix")?,
        permission: Permission::parse(row.try_get::<&str, _>("permission")?)?,
        revoked: row.try_get("revoked")?,
        last_used_at_ms: row
            .try_get::<Option<time::OffsetDateTime>, _>("last_used_at")?
            .map(ms),
        // api_keys.created_by is the internal account id; what leaves
        // the store is the account's public_id (selected as
        // created_by_pid by the caller's LEFT JOIN).
        created_by: row.try_get::<Option<uuid::Uuid>, _>("created_by_pid")?,
        created_at_ms: ms(row.try_get::<time::OffsetDateTime, _>("created_at")?),
    })
}

fn to_u64(id: i64, what: &str) -> Result<u64, AccountError> {
    u64::try_from(id).map_err(|_| AccountError::Store(format!("{what} id {id} does not fit u64")))
}

fn to_i64(id: u64, what: &str) -> Result<i64, AccountError> {
    i64::try_from(id).map_err(|_| AccountError::Store(format!("{what} id overflows i64")))
}

fn workspace_from(row: &sqlx::postgres::PgRow) -> Result<Workspace, AccountError> {
    let id: i64 = row.try_get("id")?;
    let secret: Vec<u8> = row.try_get("handler_secret")?;
    let handler_secret: [u8; 32] = secret.as_slice().try_into().map_err(|_| {
        AccountError::Store(format!(
            "workspaces.handler_secret for id {id} is {} bytes, expected 32",
            secret.len()
        ))
    })?;
    Ok(Workspace {
        id: to_u64(id, "workspace")?,
        public_id: row.try_get("public_id")?,
        name: row.try_get("name")?,
        handler_secret,
        deleted_at_ms: row
            .try_get::<Option<time::OffsetDateTime>, _>("deleted_at")?
            .map(ms),
    })
}

#[async_trait::async_trait]
impl AccountStore for Pg {
    async fn verify_key(&self, key_hash: &[u8; 32]) -> Result<Option<KeyGrant>, AccountError> {
        let row = bounded(
            sqlx::query(
                "SELECT workspace_id, permission FROM api_keys \
                 WHERE key_hash = $1 AND revoked_at IS NULL",
            )
            .bind(key_hash.as_slice())
            .fetch_optional(&self.pool),
        )
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let ws: i64 = row.try_get("workspace_id")?;
        let permission = Permission::parse(row.try_get::<&str, _>("permission")?)?;
        Ok(Some(KeyGrant {
            workspace: to_u64(ws, "workspace")?,
            permission,
        }))
    }

    async fn workspace_by_id(&self, id: u64) -> Result<Option<Workspace>, AccountError> {
        let row = bounded(
            sqlx::query("SELECT id, public_id, name, handler_secret, deleted_at FROM workspaces WHERE id = $1")
                .bind(to_i64(id, "workspace")?)
                .fetch_optional(&self.pool),
        )
        .await?;
        row.as_ref().map(workspace_from).transpose()
    }

    async fn workspace_by_public_id(
        &self,
        public_id: uuid::Uuid,
    ) -> Result<Option<Workspace>, AccountError> {
        let row = bounded(
            sqlx::query(
                "SELECT id, public_id, name, handler_secret, deleted_at FROM workspaces WHERE public_id = $1",
            )
            .bind(public_id)
            .fetch_optional(&self.pool),
        )
        .await?;
        row.as_ref().map(workspace_from).transpose()
    }

    async fn create_workspace(&self, name: &str) -> Result<Workspace, AccountError> {
        let public_id = uuid::Uuid::now_v7();
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret)
            .map_err(|e| AccountError::Store(format!("cannot draw a handler secret: {e}")))?;
        let row = bounded(
            sqlx::query(
                "INSERT INTO workspaces (public_id, name, handler_secret) \
                 VALUES ($1, $2, $3) RETURNING id",
            )
            .bind(public_id)
            .bind(name)
            .bind(secret.as_slice())
            .fetch_one(&self.pool),
        )
        .await?;
        let id: i64 = row.try_get("id")?;
        Ok(Workspace {
            id: to_u64(id, "workspace")?,
            public_id,
            name: name.to_string(),
            handler_secret: secret,
            deleted_at_ms: None,
        })
    }

    async fn list_workspaces(
        &self,
        limit: u32,
        after: Option<u64>,
    ) -> Result<Vec<Workspace>, AccountError> {
        let after = after.map(|a| to_i64(a, "workspace")).transpose()?;
        let rows = bounded(
            sqlx::query(
                "SELECT id, public_id, name, handler_secret, deleted_at FROM workspaces \
                 WHERE ($2::bigint IS NULL OR id > $2) \
                 ORDER BY id LIMIT $1",
            )
            .bind(i64::from(limit))
            .bind(after)
            .fetch_all(&self.pool),
        )
        .await?;
        rows.iter().map(workspace_from).collect()
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
        // The cap lives in the transaction with the insert, behind a
        // per-workspace advisory lock — two racing mints cannot both
        // count 9 (S2-2). Live keys only; revoked rows don't count.
        let mut tx = bounded(self.pool.begin()).await?;
        bounded(
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(to_i64(workspace, "workspace")?)
                .execute(&mut *tx),
        )
        .await?;
        let live: i64 = bounded(
            sqlx::query(
                "SELECT COUNT(*) FROM api_keys \
                 WHERE workspace_id = $1 AND revoked_at IS NULL",
            )
            .bind(to_i64(workspace, "workspace")?)
            .fetch_one(&mut *tx),
        )
        .await?
        .try_get(0)?;
        if live as u64 >= crate::STOPGAP_KEYS_PER_WS {
            return Err(AccountError::PlanLimit("api_keys"));
        }
        let row = bounded(
            sqlx::query(
                "INSERT INTO api_keys \
                 (workspace_id, name, key_hash, prefix, permission, created_by) \
                 VALUES ($1, $2, $3, $4, $5, $6) \
                 RETURNING id, public_id, created_at, \
                     (SELECT public_id FROM accounts WHERE id = $6) AS created_by_pid",
            )
            .bind(to_i64(workspace, "workspace")?)
            .bind(name)
            .bind(key_hash.as_slice())
            .bind(prefix)
            .bind(permission.as_str())
            .bind(created_by.map(|a| to_i64(a, "account")).transpose()?)
            .fetch_one(&mut *tx),
        )
        .await?;
        bounded(tx.commit()).await?;
        Ok(ApiKeyInfo {
            id: to_u64(row.try_get("id")?, "api key")?,
            public_id: row.try_get("public_id")?,
            name: name.to_string(),
            prefix: prefix.to_string(),
            permission,
            revoked: false,
            last_used_at_ms: None,
            created_by: row.try_get("created_by_pid")?,
            created_at_ms: ms(row.try_get::<time::OffsetDateTime, _>("created_at")?),
        })
    }

    async fn revoke_key(
        &self,
        workspace: u64,
        key_id: u64,
    ) -> Result<Option<[u8; 32]>, AccountError> {
        let row = bounded(
            sqlx::query(
                "UPDATE api_keys SET revoked_at = now() \
                 WHERE id = $1 AND workspace_id = $2 AND revoked_at IS NULL \
                 RETURNING key_hash",
            )
            .bind(to_i64(key_id, "api key")?)
            .bind(to_i64(workspace, "workspace")?)
            .fetch_optional(&self.pool),
        )
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let hash: Vec<u8> = row.try_get("key_hash")?;
        let hash: [u8; 32] = hash.as_slice().try_into().map_err(|_| {
            AccountError::Store(format!(
                "api_keys.key_hash for id {key_id} is {} bytes, expected 32",
                hash.len()
            ))
        })?;
        Ok(Some(hash))
    }

    async fn list_keys(&self, workspace: u64) -> Result<Vec<ApiKeyInfo>, AccountError> {
        let rows = bounded(
            sqlx::query(
                "SELECT k.id, k.public_id, k.name, k.prefix, k.permission, \
                        (k.revoked_at IS NOT NULL) AS revoked, k.last_used_at, \
                        k.created_at, a.public_id AS created_by_pid \
                 FROM api_keys k LEFT JOIN accounts a ON a.id = k.created_by \
                 WHERE k.workspace_id = $1 ORDER BY k.id",
            )
            .bind(to_i64(workspace, "workspace")?)
            .fetch_all(&self.pool),
        )
        .await?;
        rows.iter().map(key_info_from).collect()
    }

    async fn key_by_public_id(
        &self,
        workspace: u64,
        public_id: uuid::Uuid,
    ) -> Result<Option<ApiKeyInfo>, AccountError> {
        let row = bounded(
            sqlx::query(
                "SELECT k.id, k.public_id, k.name, k.prefix, k.permission, \
                        (k.revoked_at IS NOT NULL) AS revoked, k.last_used_at, \
                        k.created_at, a.public_id AS created_by_pid \
                 FROM api_keys k LEFT JOIN accounts a ON a.id = k.created_by \
                 WHERE k.workspace_id = $1 AND k.public_id = $2 AND k.revoked_at IS NULL",
            )
            .bind(to_i64(workspace, "workspace")?)
            .bind(public_id)
            .fetch_optional(&self.pool),
        )
        .await?;
        row.as_ref().map(key_info_from).transpose()
    }

    async fn rotate_key(
        &self,
        workspace: u64,
        key_id: u64,
        new_key_hash: &[u8; 32],
        new_prefix: &str,
        created_by: Option<u64>,
    ) -> Result<Option<(ApiKeyInfo, [u8; 32])>, AccountError> {
        // One tx: the old key dies and its replacement exists before any
        // other reader can look — "old 401s on the next call" is atomic.
        let mut tx = bounded(self.pool.begin()).await?;
        let old = bounded(
            sqlx::query(
                "UPDATE api_keys SET revoked_at = now() \
                 WHERE id = $1 AND workspace_id = $2 AND revoked_at IS NULL \
                 RETURNING name, permission, key_hash",
            )
            .bind(to_i64(key_id, "api key")?)
            .bind(to_i64(workspace, "workspace")?)
            .fetch_optional(&mut *tx),
        )
        .await?;
        let Some(old) = old else {
            bounded(tx.rollback()).await?;
            return Ok(None);
        };
        let old_hash: Vec<u8> = old.try_get("key_hash")?;
        let old_hash: [u8; 32] = old_hash.as_slice().try_into().map_err(|_| {
            AccountError::Store(format!(
                "api_keys.key_hash for id {key_id} is {} bytes, expected 32",
                old_hash.len()
            ))
        })?;
        let name: String = old.try_get("name")?;
        let permission = Permission::parse(old.try_get::<&str, _>("permission")?)?;
        let row = bounded(
            sqlx::query(
                "INSERT INTO api_keys \
                 (workspace_id, name, key_hash, prefix, permission, created_by) \
                 VALUES ($1, $2, $3, $4, $5, $6) \
                 RETURNING id, public_id, created_at, \
                     (SELECT public_id FROM accounts WHERE id = $6) AS created_by_pid",
            )
            .bind(to_i64(workspace, "workspace")?)
            .bind(&name)
            .bind(new_key_hash.as_slice())
            .bind(new_prefix)
            .bind(permission.as_str())
            .bind(created_by.map(|a| to_i64(a, "account")).transpose()?)
            .fetch_one(&mut *tx),
        )
        .await?;
        let info = ApiKeyInfo {
            id: to_u64(row.try_get("id")?, "api key")?,
            public_id: row.try_get("public_id")?,
            name,
            prefix: new_prefix.to_string(),
            permission,
            revoked: false,
            last_used_at_ms: None,
            created_by: row.try_get("created_by_pid")?,
            created_at_ms: ms(row.try_get::<time::OffsetDateTime, _>("created_at")?),
        };
        bounded(tx.commit()).await?;
        Ok(Some((info, old_hash)))
    }

    async fn touch_keys(&self, touches: &[crate::KeyTouch]) -> Result<(), AccountError> {
        if touches.is_empty() {
            return Ok(());
        }
        // GREATEST keeps last_used monotonic — a stale flush never
        // rewinds it.
        let mut tx = bounded(self.pool.begin()).await?;
        for t in touches {
            bounded(
                sqlx::query(
                    "UPDATE api_keys \
                     SET last_used_at = GREATEST(COALESCE(last_used_at, '-infinity'), $2) \
                     WHERE key_hash = $1 AND revoked_at IS NULL",
                )
                .bind(t.key_hash.as_slice())
                .bind(ts(t.seen_ms))
                .execute(&mut *tx),
            )
            .await?;
        }
        bounded(tx.commit()).await?;
        Ok(())
    }

    async fn list_handlers(
        &self,
        workspace: u64,
    ) -> Result<Vec<HandlerRegistration>, AccountError> {
        let rows = bounded(
            sqlx::query(
                "SELECT name, url, last_test_at, last_test_status, last_test_latency_ms \
                 FROM handler_registrations \
                 WHERE workspace_id = $1 ORDER BY name",
            )
            .bind(to_i64(workspace, "workspace")?)
            .fetch_all(&self.pool),
        )
        .await?;
        rows.iter()
            .map(|row| {
                Ok(HandlerRegistration {
                    name: row.try_get("name")?,
                    url: row.try_get("url")?,
                    last_test_at_ms: row
                        .try_get::<Option<time::OffsetDateTime>, _>("last_test_at")?
                        .map(ms),
                    last_test_status: row.try_get("last_test_status")?,
                    last_test_latency_ms: row.try_get("last_test_latency_ms")?,
                })
            })
            .collect()
    }

    async fn put_handler(&self, workspace: u64, name: &str, url: &str) -> Result<(), AccountError> {
        // The bound enforced where it cannot be raced (S2 review #6):
        // a row is written iff the name already exists (an update) or
        // the workspace is under MAX_HANDLERS_PER_WS. Under READ
        // COMMITTED each statement's count(*) sees its own snapshot —
        // a PUT burst at the boundary would all observe "under" and all
        // insert — so the check and the insert run inside a
        // transaction serialized per workspace by an advisory lock
        // (released automatically at commit/rollback).
        let mut tx = bounded(self.pool.begin()).await?;
        bounded(
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(to_i64(workspace, "workspace")?)
                .execute(&mut *tx),
        )
        .await?;
        let done = bounded(
            sqlx::query(
                "INSERT INTO handler_registrations (workspace_id, name, url) \
                 SELECT $1, $2, $3 \
                 WHERE EXISTS ( \
                     SELECT 1 FROM handler_registrations \
                     WHERE workspace_id = $1 AND name = $2 \
                 ) OR ( \
                     SELECT count(*) FROM handler_registrations \
                     WHERE workspace_id = $1 \
                 ) < $4 \
                 ON CONFLICT (workspace_id, name) \
                 DO UPDATE SET url = $3, updated_at = now()",
            )
            .bind(to_i64(workspace, "workspace")?)
            .bind(name)
            .bind(url)
            .bind(crate::MAX_HANDLERS_PER_WS as i64)
            .execute(&mut *tx),
        )
        .await?;
        if done.rows_affected() == 0 {
            return Err(AccountError::RegistryFull);
        }
        bounded(tx.commit()).await?;
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
        let n = bounded(
            sqlx::query(
                "UPDATE handler_registrations \
                 SET last_test_at = $3, last_test_status = $4, last_test_latency_ms = $5 \
                 WHERE workspace_id = $1 AND name = $2",
            )
            .bind(to_i64(workspace, "workspace")?)
            .bind(name)
            .bind(ts(at_ms))
            .bind(status)
            .bind(latency_ms)
            .execute(&self.pool),
        )
        .await?
        .rows_affected();
        Ok(n > 0)
    }

    async fn delete_handler(&self, workspace: u64, name: &str) -> Result<bool, AccountError> {
        let done = bounded(
            sqlx::query("DELETE FROM handler_registrations WHERE workspace_id = $1 AND name = $2")
                .bind(to_i64(workspace, "workspace")?)
                .bind(name)
                .execute(&self.pool),
        )
        .await?;
        Ok(done.rows_affected() > 0)
    }

    async fn rotate_handler_secret(
        &self,
        workspace: u64,
    ) -> Result<Option<[u8; 32]>, AccountError> {
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret)
            .map_err(|e| AccountError::Store(format!("cannot draw a handler secret: {e}")))?;
        let done = bounded(
            sqlx::query("UPDATE workspaces SET handler_secret = $2 WHERE id = $1")
                .bind(to_i64(workspace, "workspace")?)
                .bind(secret.as_slice())
                .execute(&self.pool),
        )
        .await?;
        Ok((done.rows_affected() > 0).then_some(secret))
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
        // One transaction = the plan's atomicity rule: an account is
        // never committed without the token and mail it needs. The
        // advisory lock serializes signups for the same address so the
        // verify-token supersede and the outbox cap cannot race.
        let mut tx = bounded(self.pool.begin()).await?;
        bounded(
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
                .bind(email)
                .execute(&mut *tx),
        )
        .await?;
        let created = bounded(
            sqlx::query(
                "INSERT INTO accounts (public_id, email, password_hash) \
                 VALUES ($2, $1, $3) ON CONFLICT (email) DO NOTHING \
                 RETURNING id",
            )
            .bind(email)
            .bind(uuid::Uuid::now_v7())
            .bind(password_hash)
            .fetch_optional(&mut *tx),
        )
        .await?;
        if let Some(row) = created {
            let account_id: i64 = row.try_get("id")?;
            let outbox = Self::issue_verify(
                &mut tx,
                account_id,
                email,
                token_hash,
                token_expires_at_ms,
                Some(password_hash.to_string()),
                verify_mail,
            )
            .await?;
            bounded(tx.commit()).await?;
            return Ok(SignupOutcome::Created { outbox });
        }
        let existing = bounded(
            sqlx::query("SELECT id, email_verified_at FROM accounts WHERE email = $1")
                .bind(email)
                .fetch_one(&mut *tx),
        )
        .await?;
        let account_id: i64 = existing.try_get("id")?;
        let verified: Option<time::OffsetDateTime> = existing.try_get("email_verified_at")?;
        let outcome = if verified.is_some() {
            SignupOutcome::Taken {
                outbox: Self::enqueue(&mut tx, notice_mail).await?,
            }
        } else {
            // Re-sign-up of an unverified address (review #2): the new
            // sign-up's password replaces the pending one and every
            // live session dies — a pre-registration attacker's
            // credential and sessions lose here, not at verify.
            bounded(
                sqlx::query("UPDATE accounts SET password_hash = $2 WHERE id = $1")
                    .bind(account_id)
                    .bind(password_hash)
                    .execute(&mut *tx),
            )
            .await?;
            bounded(
                sqlx::query(
                    "UPDATE sessions SET revoked_at = now() \
                     WHERE account_id = $1 AND revoked_at IS NULL",
                )
                .bind(account_id)
                .execute(&mut *tx),
            )
            .await?;
            SignupOutcome::VerifyResent {
                account: account_id as u64,
                outbox: Self::issue_verify(
                    &mut tx,
                    account_id,
                    email,
                    token_hash,
                    token_expires_at_ms,
                    Some(password_hash.to_string()),
                    verify_mail,
                )
                .await?,
            }
        };
        bounded(tx.commit()).await?;
        Ok(outcome)
    }

    async fn account_by_email(&self, email: &str) -> Result<Option<Account>, AccountError> {
        let row = bounded(
            sqlx::query(
                "SELECT id, public_id, email, email_verified_at, password_hash \
                 FROM accounts WHERE email = $1",
            )
            .bind(email)
            .fetch_optional(&self.pool),
        )
        .await?;
        row.as_ref().map(account_from).transpose()
    }

    async fn session_grant(
        &self,
        token_hash: &[u8; 32],
    ) -> Result<Option<SessionGrant>, AccountError> {
        let row = bounded(
            sqlx::query(
                "SELECT s.id AS session_id, s.public_id AS session_public_id, s.csrf_seed, \
                 s.expires_at, s.idle_expires_at, \
                 a.id, a.public_id, a.email, a.email_verified_at, a.password_hash \
                 FROM sessions s JOIN accounts a ON a.id = s.account_id \
                 WHERE s.token_hash = $1 AND s.revoked_at IS NULL",
            )
            .bind(token_hash.as_slice())
            .fetch_optional(&self.pool),
        )
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let seed: Vec<u8> = row.try_get("csrf_seed")?;
        let csrf_seed: [u8; 32] = seed.as_slice().try_into().map_err(|_| {
            AccountError::Store(format!(
                "sessions.csrf_seed is {} bytes, expected 32",
                seed.len()
            ))
        })?;
        Ok(Some(SessionGrant {
            session_id: to_u64(row.try_get("session_id")?, "session")?,
            public_id: row.try_get("session_public_id")?,
            csrf_seed,
            expires_at_ms: ms(row.try_get("expires_at")?),
            idle_expires_at_ms: ms(row.try_get("idle_expires_at")?),
            account: account_from(&row)?,
        }))
    }

    async fn create_session(&self, session: &crate::NewSession) -> Result<(), AccountError> {
        // One transaction per sign-in: the LRU eviction and the insert
        // are serialized per account by the advisory lock, so two
        // racing logins cannot both keep their extra session (the cap
        // would overshoot otherwise).
        let mut tx = bounded(self.pool.begin()).await?;
        bounded(
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(to_i64(session.account, "account")?)
                .execute(&mut *tx),
        )
        .await?;
        bounded(
            sqlx::query(
                "UPDATE sessions SET revoked_at = now() WHERE id IN ( \
                     SELECT id FROM sessions \
                     WHERE account_id = $1 AND revoked_at IS NULL \
                     ORDER BY idle_expires_at DESC OFFSET $2 \
                 )",
            )
            .bind(to_i64(session.account, "account")?)
            .bind((crate::MAX_SESSIONS_PER_ACCOUNT - 1) as i64)
            .execute(&mut *tx),
        )
        .await?;
        bounded(
            sqlx::query(
                "INSERT INTO sessions \
                 (account_id, token_hash, csrf_seed, expires_at, idle_expires_at, \
                  last_seen_at, user_agent, ip, replaced_by) \
                 VALUES ($1, $2, $3, $4, $5, now(), $6, $7::inet, $8)",
            )
            .bind(to_i64(session.account, "account")?)
            .bind(session.token_hash.as_slice())
            .bind(session.csrf_seed.as_slice())
            .bind(ts(session.expires_at_ms))
            .bind(ts(session.idle_expires_at_ms))
            .bind(session.user_agent.as_deref())
            .bind(session.ip.map(|a| a.to_string()))
            .bind(
                session
                    .replaces
                    .map(|id| to_i64(id, "session"))
                    .transpose()?,
            )
            .execute(&mut *tx),
        )
        .await?;
        bounded(tx.commit()).await?;
        Ok(())
    }

    async fn revoke_session(&self, token_hash: &[u8; 32]) -> Result<bool, AccountError> {
        let done = bounded(
            sqlx::query(
                "UPDATE sessions SET revoked_at = now() \
                 WHERE token_hash = $1 AND revoked_at IS NULL",
            )
            .bind(token_hash.as_slice())
            .execute(&self.pool),
        )
        .await?;
        Ok(done.rows_affected() > 0)
    }

    async fn revoke_sessions_for_account(&self, account: u64) -> Result<(), AccountError> {
        bounded(
            sqlx::query(
                "UPDATE sessions SET revoked_at = now() \
                 WHERE account_id = $1 AND revoked_at IS NULL",
            )
            .bind(to_i64(account, "account")?)
            .execute(&self.pool),
        )
        .await?;
        Ok(())
    }

    async fn sessions_for_account(
        &self,
        account: u64,
        limit: u32,
    ) -> Result<Vec<crate::SessionInfo>, AccountError> {
        let rows = bounded(
            sqlx::query(
                "SELECT public_id, created_at, last_seen_at, expires_at, \
                        idle_expires_at, user_agent, host(ip) AS ip \
                 FROM sessions \
                 WHERE account_id = $1 AND revoked_at IS NULL \
                 ORDER BY created_at DESC LIMIT $2",
            )
            .bind(to_i64(account, "account")?)
            .bind(limit as i64)
            .fetch_all(&self.pool),
        )
        .await?;
        rows.iter()
            .map(|r| {
                Ok(crate::SessionInfo {
                    public_id: r.try_get("public_id")?,
                    created_at_ms: ms(r.try_get::<time::OffsetDateTime, _>("created_at")?),
                    last_seen_at_ms: ms(r.try_get::<time::OffsetDateTime, _>("last_seen_at")?),
                    expires_at_ms: ms(r.try_get::<time::OffsetDateTime, _>("expires_at")?),
                    idle_expires_at_ms: ms(
                        r.try_get::<time::OffsetDateTime, _>("idle_expires_at")?,
                    ),
                    user_agent: r.try_get("user_agent")?,
                    ip: r.try_get("ip")?,
                })
            })
            .collect()
    }

    async fn revoke_session_public(
        &self,
        account: u64,
        public_id: uuid::Uuid,
    ) -> Result<Option<[u8; 32]>, AccountError> {
        // The account bind is inside the WHERE — a foreign public_id is
        // the same empty answer as an unknown one (no idor oracle).
        let row = bounded(
            sqlx::query(
                "UPDATE sessions SET revoked_at = now() \
                 WHERE public_id = $1 AND account_id = $2 AND revoked_at IS NULL \
                 RETURNING token_hash",
            )
            .bind(public_id)
            .bind(to_i64(account, "account")?)
            .fetch_optional(&self.pool),
        )
        .await?;
        row.map(|r| {
            let h: Vec<u8> = r.try_get("token_hash")?;
            <[u8; 32]>::try_from(h.as_slice())
                .map_err(|_| AccountError::Store("bad token_hash length".into()))
        })
        .transpose()
    }

    async fn revoke_sessions_except(
        &self,
        account: u64,
        keep_public_id: uuid::Uuid,
    ) -> Result<u64, AccountError> {
        let done = bounded(
            sqlx::query(
                "UPDATE sessions SET revoked_at = now() \
                 WHERE account_id = $1 AND public_id <> $2 AND revoked_at IS NULL",
            )
            .bind(to_i64(account, "account")?)
            .bind(keep_public_id)
            .execute(&self.pool),
        )
        .await?;
        Ok(done.rows_affected())
    }

    async fn change_password(
        &self,
        account: u64,
        new_password_hash: &str,
    ) -> Result<(), AccountError> {
        // One transaction: the new hash, dead pending tokens (a verify
        // token's stale pending hash must never revert the change) and
        // every session revoked — no partial win.
        let mut tx = bounded(self.pool.begin()).await?;
        bounded(
            sqlx::query("UPDATE accounts SET password_hash = $2 WHERE id = $1")
                .bind(to_i64(account, "account")?)
                .bind(new_password_hash)
                .execute(&mut *tx),
        )
        .await?;
        bounded(
            sqlx::query(
                "UPDATE email_tokens SET superseded_at = now() \
                 WHERE account_id = $1 AND consumed_at IS NULL AND superseded_at IS NULL",
            )
            .bind(to_i64(account, "account")?)
            .execute(&mut *tx),
        )
        .await?;
        bounded(
            sqlx::query(
                "UPDATE sessions SET revoked_at = now() \
                 WHERE account_id = $1 AND revoked_at IS NULL",
            )
            .bind(to_i64(account, "account")?)
            .execute(&mut *tx),
        )
        .await?;
        bounded(tx.commit()).await?;
        Ok(())
    }

    async fn touch_sessions(&self, touches: &[crate::SessionTouch]) -> Result<(), AccountError> {
        if touches.is_empty() {
            return Ok(());
        }
        // GREATEST keeps the slide monotonic — a stale flush never
        // pulls last_seen or idle backwards.
        let mut tx = bounded(self.pool.begin()).await?;
        for t in touches {
            bounded(
                sqlx::query(
                    "UPDATE sessions SET \
                         last_seen_at = GREATEST(last_seen_at, $2), \
                         idle_expires_at = GREATEST(idle_expires_at, $3) \
                     WHERE token_hash = $1 AND revoked_at IS NULL",
                )
                .bind(t.token_hash.as_slice())
                .bind(ts(t.seen_ms))
                .bind(ts(t.idle_expires_at_ms))
                .execute(&mut *tx),
            )
            .await?;
        }
        bounded(tx.commit()).await?;
        Ok(())
    }

    async fn member_role(
        &self,
        account: u64,
        workspace: u64,
    ) -> Result<Option<crate::Role>, AccountError> {
        if workspace == 0 {
            // ws 0 is the ops plane; the CHECK means no row anyway.
            return Ok(None);
        }
        let role: Option<String> = bounded(
            sqlx::query_scalar(
                "SELECT role FROM members WHERE workspace_id = $1 AND account_id = $2",
            )
            .bind(to_i64(workspace, "workspace")?)
            .bind(to_i64(account, "account")?)
            .fetch_optional(&self.pool),
        )
        .await?;
        role.map(|r| {
            crate::Role::parse(&r).ok_or_else(|| AccountError::Store(format!("bad role {r:?}")))
        })
        .transpose()
    }

    async fn create_workspace_for(
        &self,
        account: u64,
        name: &str,
    ) -> Result<Workspace, AccountError> {
        // One transaction: the workspace row and its owner member row
        // land or neither does — an ownerless workspace cannot exist.
        // The advisory lock + the count are in that same transaction:
        // two racing creates cannot both pass the stopgap cap (S2-2);
        // soft-deleted rows count until the purge removes them.
        let mut tx = bounded(self.pool.begin()).await?;
        bounded(
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(to_i64(account, "account")?)
                .execute(&mut *tx),
        )
        .await?;
        let owned: i64 = bounded(
            sqlx::query("SELECT COUNT(*) FROM workspaces WHERE created_by = $1")
                .bind(to_i64(account, "account")?)
                .fetch_one(&mut *tx),
        )
        .await?
        .try_get(0)?;
        if owned as u64 >= crate::STOPGAP_WS_PER_ACCOUNT {
            return Err(AccountError::PlanLimit("workspaces_per_account"));
        }
        let public_id = uuid::Uuid::now_v7();
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret)
            .map_err(|e| AccountError::Store(format!("cannot draw a handler secret: {e}")))?;
        let row = bounded(
            sqlx::query(
                "INSERT INTO workspaces (public_id, name, handler_secret, created_by) \
                 VALUES ($1, $2, $3, $4) RETURNING id",
            )
            .bind(public_id)
            .bind(name)
            .bind(secret.as_slice())
            .bind(to_i64(account, "account")?)
            .fetch_one(&mut *tx),
        )
        .await?;
        let id: i64 = row.try_get("id")?;
        bounded(
            sqlx::query(
                "INSERT INTO members (workspace_id, account_id, role) VALUES ($1, $2, 'owner')",
            )
            .bind(id)
            .bind(to_i64(account, "account")?)
            .execute(&mut *tx),
        )
        .await?;
        bounded(tx.commit()).await?;
        Ok(Workspace {
            id: to_u64(id, "workspace")?,
            public_id,
            name: name.to_string(),
            handler_secret: secret,
            deleted_at_ms: None,
        })
    }

    async fn rename_workspace(&self, id: u64, name: &str) -> Result<bool, AccountError> {
        let done = bounded(
            sqlx::query("UPDATE workspaces SET name = $2 WHERE id = $1")
                .bind(to_i64(id, "workspace")?)
                .bind(name)
                .execute(&self.pool),
        )
        .await?;
        Ok(done.rows_affected() > 0)
    }

    async fn delete_workspace(&self, id: u64, now_ms: i64) -> Result<bool, AccountError> {
        // One transaction (D-072): the row closes and every live key
        // dies in the same commit — a deleted workspace's key can never
        // outlive the delete.
        let mut tx = bounded(self.pool.begin()).await?;
        let done = bounded(
            sqlx::query(
                "UPDATE workspaces SET deleted_at = $2 WHERE id = $1 AND deleted_at IS NULL",
            )
            .bind(to_i64(id, "workspace")?)
            .bind(ts(now_ms))
            .execute(&mut *tx),
        )
        .await?;
        if done.rows_affected() == 0 {
            return Ok(false);
        }
        bounded(
            sqlx::query(
                "UPDATE api_keys SET revoked_at = $2 WHERE workspace_id = $1 AND revoked_at IS NULL",
            )
            .bind(to_i64(id, "workspace")?)
            .bind(ts(now_ms))
            .execute(&mut *tx),
        )
        .await?;
        bounded(tx.commit()).await?;
        Ok(true)
    }

    async fn workspaces_for_account(
        &self,
        account: u64,
    ) -> Result<Vec<(Workspace, crate::Role)>, AccountError> {
        let rows = bounded(
            sqlx::query(
                "SELECT w.id, w.public_id, w.name, w.handler_secret, w.deleted_at, m.role \
                 FROM members m JOIN workspaces w ON w.id = m.workspace_id \
                 WHERE m.account_id = $1 AND w.deleted_at IS NULL ORDER BY w.id",
            )
            .bind(to_i64(account, "account")?)
            .fetch_all(&self.pool),
        )
        .await?;
        rows.iter()
            .map(|r| {
                Ok((
                    workspace_from(r)?,
                    crate::Role::parse(r.try_get::<&str, _>("role")?)
                        .ok_or_else(|| AccountError::Store("bad role".into()))?,
                ))
            })
            .collect()
    }

    async fn list_members(&self, workspace: u64) -> Result<Vec<crate::MemberInfo>, AccountError> {
        let rows = bounded(
            sqlx::query(
                "SELECT a.public_id, a.email, m.role, m.created_at \
                 FROM members m JOIN accounts a ON a.id = m.account_id \
                 WHERE m.workspace_id = $1 ORDER BY m.created_at",
            )
            .bind(to_i64(workspace, "workspace")?)
            .fetch_all(&self.pool),
        )
        .await?;
        rows.iter()
            .map(|r| {
                Ok(crate::MemberInfo {
                    account_public_id: r.try_get("public_id")?,
                    email: r.try_get("email")?,
                    role: crate::Role::parse(r.try_get::<&str, _>("role")?)
                        .ok_or_else(|| AccountError::Store("bad role".into()))?,
                    since_ms: ms(r.try_get::<time::OffsetDateTime, _>("created_at")?),
                })
            })
            .collect()
    }

    async fn set_member_role(
        &self,
        workspace: u64,
        account: u64,
        role: crate::Role,
    ) -> Result<crate::MemberChange, AccountError> {
        // The owner row is untouchable: the UPDATE excludes it, so a
        // demote of the owner lands 0 rows and the read below tells the
        // two NotFound/LastOwner cases apart.
        let done = bounded(
            sqlx::query(
                "UPDATE members SET role = $3 \
                 WHERE workspace_id = $1 AND account_id = $2 AND role <> 'owner'",
            )
            .bind(to_i64(workspace, "workspace")?)
            .bind(to_i64(account, "account")?)
            .bind(role.as_str())
            .execute(&self.pool),
        )
        .await?;
        if done.rows_affected() > 0 {
            return Ok(crate::MemberChange::Changed);
        }
        Ok(match self.member_role(account, workspace).await? {
            Some(crate::Role::Owner) => crate::MemberChange::LastOwner,
            _ => crate::MemberChange::NotFound,
        })
    }

    async fn remove_member(
        &self,
        workspace: u64,
        account: u64,
    ) -> Result<crate::MemberChange, AccountError> {
        let done = bounded(
            sqlx::query(
                "DELETE FROM members \
                 WHERE workspace_id = $1 AND account_id = $2 AND role <> 'owner'",
            )
            .bind(to_i64(workspace, "workspace")?)
            .bind(to_i64(account, "account")?)
            .execute(&self.pool),
        )
        .await?;
        if done.rows_affected() > 0 {
            return Ok(crate::MemberChange::Changed);
        }
        Ok(match self.member_role(account, workspace).await? {
            Some(crate::Role::Owner) => crate::MemberChange::LastOwner,
            _ => crate::MemberChange::NotFound,
        })
    }

    async fn account_by_public_id(
        &self,
        public_id: uuid::Uuid,
    ) -> Result<Option<Account>, AccountError> {
        let row = bounded(
            sqlx::query(
                "SELECT id, public_id, email, email_verified_at, password_hash \
                 FROM accounts WHERE public_id = $1",
            )
            .bind(public_id)
            .fetch_optional(&self.pool),
        )
        .await?;
        row.map(|r| {
            Ok(Account {
                id: to_u64(r.try_get("id")?, "account")?,
                public_id: r.try_get("public_id")?,
                email: r.try_get("email")?,
                email_verified: r
                    .try_get::<Option<time::OffsetDateTime>, _>("email_verified_at")?
                    .is_some(),
                password_hash: r.try_get("password_hash")?,
            })
        })
        .transpose()
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
        // The daily cap and the dup check are one transaction behind a
        // per-workspace advisory lock — a burst cannot race past the
        // count (S2-2).
        let mut tx = bounded(self.pool.begin()).await?;
        bounded(
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(to_i64(workspace, "workspace")?)
                .execute(&mut *tx),
        )
        .await?;
        let today: i64 = bounded(
            sqlx::query(
                "SELECT COUNT(*) FROM invites \
                 WHERE workspace_id = $1 AND created_at > now() - interval '1 day'",
            )
            .bind(to_i64(workspace, "workspace")?)
            .fetch_one(&mut *tx),
        )
        .await?
        .try_get(0)?;
        if today as u64 >= crate::STOPGAP_INVITES_PER_WS_DAY {
            return Err(AccountError::PlanLimit("invites_per_day"));
        }
        let public_id = uuid::Uuid::now_v7();
        // `invites_pending` makes the live-dup refusal atomic: ON
        // CONFLICT (workspace_id, email) WHERE pending DO NOTHING.
        let done = bounded(
            sqlx::query(
                "INSERT INTO invites \
                 (public_id, workspace_id, email, role, invited_by, token_hash, expires_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7) \
                 ON CONFLICT (workspace_id, email) \
                 WHERE accepted_at IS NULL AND revoked_at IS NULL DO NOTHING",
            )
            .bind(public_id)
            .bind(to_i64(workspace, "workspace")?)
            .bind(email)
            .bind(role.as_str())
            .bind(to_i64(invited_by, "account")?)
            .bind(token_hash.as_slice())
            .bind(ts(expires_at_ms))
            .execute(&mut *tx),
        )
        .await?;
        if done.rows_affected() == 0 {
            return Ok(None);
        }
        bounded(tx.commit()).await?;
        Ok(Some(crate::WorkspaceInvite {
            public_id,
            email: email.to_string(),
            role,
            expires_at_ms,
        }))
    }

    async fn list_invites(
        &self,
        workspace: u64,
    ) -> Result<Vec<crate::WorkspaceInvite>, AccountError> {
        let rows = bounded(
            sqlx::query(
                "SELECT public_id, email, role, expires_at FROM invites \
                 WHERE workspace_id = $1 AND accepted_at IS NULL AND revoked_at IS NULL \
                 ORDER BY id",
            )
            .bind(to_i64(workspace, "workspace")?)
            .fetch_all(&self.pool),
        )
        .await?;
        rows.iter()
            .map(|r| {
                Ok(crate::WorkspaceInvite {
                    public_id: r.try_get("public_id")?,
                    email: r.try_get("email")?,
                    role: crate::Role::parse(r.try_get::<&str, _>("role")?)
                        .ok_or_else(|| AccountError::Store("bad role".into()))?,
                    expires_at_ms: ms(r.try_get::<time::OffsetDateTime, _>("expires_at")?),
                })
            })
            .collect()
    }

    async fn revoke_invite(
        &self,
        workspace: u64,
        public_id: uuid::Uuid,
    ) -> Result<bool, AccountError> {
        let done = bounded(
            sqlx::query(
                "UPDATE invites SET revoked_at = now() \
                 WHERE workspace_id = $1 AND public_id = $2 \
                   AND accepted_at IS NULL AND revoked_at IS NULL",
            )
            .bind(to_i64(workspace, "workspace")?)
            .bind(public_id)
            .execute(&self.pool),
        )
        .await?;
        Ok(done.rows_affected() > 0)
    }

    async fn accept_invite(
        &self,
        token_hash: &[u8; 32],
        account: u64,
        now_ms: i64,
    ) -> Result<Option<(u64, crate::Role)>, AccountError> {
        // One transaction: consume the invite and write the member row —
        // ON CONFLICT DO NOTHING keeps an existing row's role.
        let mut tx = bounded(self.pool.begin()).await?;
        let row = bounded(
            sqlx::query(
                "UPDATE invites SET accepted_at = $3 \
                 WHERE token_hash = $1 AND accepted_at IS NULL AND revoked_at IS NULL \
                   AND expires_at > $3 \
                 RETURNING workspace_id, role",
            )
            .bind(token_hash.as_slice())
            .bind(ts(now_ms))
            .execute(&mut *tx),
        )
        .await?;
        if row.rows_affected() == 0 {
            return Ok(None);
        }
        // Re-read the row we just consumed for its grant.
        let grant = bounded(
            sqlx::query("SELECT workspace_id, role FROM invites WHERE token_hash = $1")
                .bind(token_hash.as_slice())
                .fetch_one(&mut *tx),
        )
        .await?;
        let ws: i64 = grant.try_get("workspace_id")?;
        let role_str: &str = grant.try_get("role")?;
        let role = crate::Role::parse(role_str)
            .ok_or_else(|| AccountError::Store(format!("bad role {role_str:?}")))?;
        bounded(
            sqlx::query(
                "INSERT INTO members (workspace_id, account_id, role) \
                 VALUES ($1, $2, $3) ON CONFLICT (workspace_id, account_id) DO NOTHING",
            )
            .bind(ws)
            .bind(to_i64(account, "account")?)
            .bind(role_str)
            .execute(&mut *tx),
        )
        .await?;
        bounded(tx.commit()).await?;
        Ok(Some((to_u64(ws, "workspace")?, role)))
    }

    async fn decline_invite(
        &self,
        token_hash: &[u8; 32],
        _now_ms: i64,
    ) -> Result<bool, AccountError> {
        let done = bounded(
            sqlx::query(
                "UPDATE invites SET revoked_at = now() \
                 WHERE token_hash = $1 AND accepted_at IS NULL AND revoked_at IS NULL",
            )
            .bind(token_hash.as_slice())
            .execute(&self.pool),
        )
        .await?;
        Ok(done.rows_affected() > 0)
    }

    async fn consume_verify_token(
        &self,
        token_hash: &[u8; 32],
    ) -> Result<Option<Account>, AccountError> {
        // One transaction (review #2): consume, install the pending
        // hash, verify, revoke — a partial win is impossible. The
        // single UPDATE ... WHERE live covers unknown, consumed,
        // superseded, expired and wrong-kind identically.
        let mut tx = bounded(self.pool.begin()).await?;
        let row = bounded(
            sqlx::query(
                "UPDATE email_tokens SET consumed_at = now() \
                 WHERE token_hash = $1 AND kind = 'verify' \
                   AND consumed_at IS NULL AND superseded_at IS NULL \
                   AND expires_at > now() \
                 RETURNING account_id, password_hash",
            )
            .bind(token_hash.as_slice())
            .fetch_optional(&mut *tx),
        )
        .await?;
        let Some(row) = row else {
            bounded(tx.rollback()).await?;
            return Ok(None);
        };
        let account_id: i64 = row.try_get("account_id")?;
        let pending: Option<String> = row.try_get("password_hash")?;
        bounded(
            sqlx::query(
                "UPDATE accounts SET \
                     password_hash = COALESCE($2, password_hash), \
                     email_verified_at = COALESCE(email_verified_at, now()) \
                 WHERE id = $1",
            )
            .bind(account_id)
            .bind(pending)
            .execute(&mut *tx),
        )
        .await?;
        bounded(
            sqlx::query(
                "UPDATE sessions SET revoked_at = now() \
                 WHERE account_id = $1 AND revoked_at IS NULL",
            )
            .bind(account_id)
            .execute(&mut *tx),
        )
        .await?;
        let account = bounded(
            sqlx::query(
                "SELECT id, public_id, email, email_verified_at, password_hash \
                 FROM accounts WHERE id = $1",
            )
            .bind(account_id)
            .fetch_one(&mut *tx),
        )
        .await
        .and_then(|r| account_from(&r))?;
        bounded(tx.commit()).await?;
        Ok(Some(account))
    }

    async fn peek_verify_token(
        &self,
        token_hash: &[u8; 32],
    ) -> Result<Option<crate::VerifyPeek>, AccountError> {
        // Same live predicate as consume_verify_token — a dead token
        // peeks as nothing so the route's 410 stays honest.
        let row = bounded(
            sqlx::query(
                "SELECT COALESCE(t.password_hash, a.password_hash) AS phc                  FROM email_tokens t JOIN accounts a ON a.id = t.account_id                  WHERE t.token_hash = $1 AND t.kind = 'verify'                    AND t.consumed_at IS NULL AND t.superseded_at IS NULL                    AND t.expires_at > now()",
            )
            .bind(token_hash.as_slice())
            .fetch_optional(&self.pool),
        )
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(crate::VerifyPeek {
            password_hash: row.try_get("phc")?,
        }))
    }

    async fn consume_reset_token(
        &self,
        token_hash: &[u8; 32],
        new_password_hash: &str,
    ) -> Result<Option<Account>, AccountError> {
        let mut tx = bounded(self.pool.begin()).await?;
        let row = bounded(
            sqlx::query(
                "UPDATE email_tokens SET consumed_at = now() \
                 WHERE token_hash = $1 AND kind = 'reset' \
                   AND consumed_at IS NULL AND superseded_at IS NULL \
                   AND expires_at > now() \
                 RETURNING account_id",
            )
            .bind(token_hash.as_slice())
            .fetch_optional(&mut *tx),
        )
        .await?;
        let Some(row) = row else {
            bounded(tx.rollback()).await?;
            return Ok(None);
        };
        let account_id: i64 = row.try_get("account_id")?;
        // The reset installs the new password and proves mailbox
        // ownership — every other pending token (a verify token's
        // pending hash must never overwrite the reset) is dead.
        bounded(
            sqlx::query(
                "UPDATE email_tokens SET superseded_at = now() \
                 WHERE account_id = $1 AND consumed_at IS NULL AND superseded_at IS NULL",
            )
            .bind(account_id)
            .execute(&mut *tx),
        )
        .await?;
        bounded(
            sqlx::query(
                "UPDATE accounts SET \
                     password_hash = $2, \
                     email_verified_at = COALESCE(email_verified_at, now()) \
                 WHERE id = $1",
            )
            .bind(account_id)
            .bind(new_password_hash)
            .execute(&mut *tx),
        )
        .await?;
        bounded(
            sqlx::query(
                "UPDATE sessions SET revoked_at = now() \
                 WHERE account_id = $1 AND revoked_at IS NULL",
            )
            .bind(account_id)
            .execute(&mut *tx),
        )
        .await?;
        let account = bounded(
            sqlx::query(
                "SELECT id, public_id, email, email_verified_at, password_hash \
                 FROM accounts WHERE id = $1",
            )
            .bind(account_id)
            .fetch_one(&mut *tx),
        )
        .await
        .and_then(|r| account_from(&r))?;
        bounded(tx.commit()).await?;
        Ok(Some(account))
    }

    async fn issue_reset_token(
        &self,
        email: &str,
        token_hash: &[u8; 32],
        expires_at_ms: i64,
        reset_mail: &Mail,
    ) -> Result<Option<u64>, AccountError> {
        let mut tx = bounded(self.pool.begin()).await?;
        bounded(
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
                .bind(email)
                .execute(&mut *tx),
        )
        .await?;
        let account: Option<i64> = bounded(
            sqlx::query_scalar("SELECT id FROM accounts WHERE email = $1")
                .bind(email)
                .fetch_optional(&mut *tx),
        )
        .await?;
        let Some(account_id) = account else {
            bounded(tx.rollback()).await?;
            return Ok(None);
        };
        bounded(
            sqlx::query(
                "UPDATE email_tokens SET superseded_at = now() \
                 WHERE account_id = $1 AND kind = 'reset' \
                   AND consumed_at IS NULL AND superseded_at IS NULL",
            )
            .bind(account_id)
            .execute(&mut *tx),
        )
        .await?;
        bounded(
            sqlx::query(
                "INSERT INTO email_tokens \
                 (account_id, kind, token_hash, expires_at, sent_to) \
                 VALUES ($1, 'reset', $2, $3, $4)",
            )
            .bind(account_id)
            .bind(token_hash.as_slice())
            .bind(ts(expires_at_ms))
            .bind(email)
            .execute(&mut *tx),
        )
        .await?;
        let outbox = Self::enqueue(&mut tx, reset_mail).await?;
        bounded(tx.commit()).await?;
        Ok(outbox)
    }

    async fn reissue_verify(
        &self,
        email: &str,
        token_hash: &[u8; 32],
        expires_at_ms: i64,
        verify_mail: &Mail,
    ) -> Result<Option<u64>, AccountError> {
        let mut tx = bounded(self.pool.begin()).await?;
        bounded(
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
                .bind(email)
                .execute(&mut *tx),
        )
        .await?;
        let row = bounded(
            sqlx::query(
                "SELECT id, password_hash FROM accounts \
                 WHERE email = $1 AND email_verified_at IS NULL",
            )
            .bind(email)
            .fetch_optional(&mut *tx),
        )
        .await?;
        let Some(row) = row else {
            bounded(tx.rollback()).await?;
            return Ok(None);
        };
        let account_id: i64 = row.try_get("id")?;
        let password_hash: Option<String> = row.try_get("password_hash")?;
        let outbox = Self::issue_verify(
            &mut tx,
            account_id,
            email,
            token_hash,
            expires_at_ms,
            password_hash,
            verify_mail,
        )
        .await?;
        bounded(tx.commit()).await?;
        Ok(outbox)
    }

    async fn invite_grant(
        &self,
        token_hash: &[u8; 32],
        now_ms: i64,
    ) -> Result<Option<crate::InviteGrant>, AccountError> {
        let row = bounded(
            sqlx::query(
                "SELECT workspace_id, email, role FROM invites \
                 WHERE token_hash = $1 AND accepted_at IS NULL \
                   AND revoked_at IS NULL AND expires_at > $2",
            )
            .bind(token_hash.as_slice())
            .bind(ts(now_ms))
            .fetch_optional(&self.pool),
        )
        .await?;
        row.map(|r| {
            let role: String = r.try_get("role")?;
            Ok(crate::InviteGrant {
                workspace: to_u64(r.try_get("workspace_id")?, "workspace")?,
                email: r.try_get("email")?,
                role: crate::Role::parse(&role)
                    .ok_or_else(|| AccountError::Store(format!("bad role {role:?}")))?,
            })
        })
        .transpose()
    }

    async fn enqueue_mail(&self, mail: &Mail) -> Result<Option<u64>, AccountError> {
        let mut tx = bounded(self.pool.begin()).await?;
        let outbox = Self::enqueue(&mut tx, mail).await?;
        bounded(tx.commit()).await?;
        Ok(outbox)
    }

    async fn outbox_claim(
        &self,
        now_ms: i64,
        limit: u32,
        claim_ms: i64,
    ) -> Result<Vec<crate::OutboxMail>, AccountError> {
        // pending→sending claims due rows; a stale 'sending' row (the
        // worker died between claim and mark) is reclaimable past its
        // claim deadline and that reclaim counts a lost attempt.
        let rows = bounded(
            sqlx::query(
                "UPDATE email_outbox SET status = 'sending', \
                     attempts = attempts + CASE WHEN status = 'sending' THEN 1 ELSE 0 END, \
                     next_attempt_at = $3 \
                 WHERE id IN ( \
                     SELECT id FROM email_outbox \
                     WHERE next_attempt_at <= $1 \
                       AND status IN ('pending', 'sending') \
                       AND attempts < $4 \
                     ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED \
                 ) \
                 RETURNING id, kind, send_to, subject, text, html, \
                           idempotency_key, expires_at, attempts",
            )
            .bind(ts(now_ms))
            .bind(limit as i64)
            .bind(ts(now_ms + claim_ms))
            .bind(crate::mail::OUTBOX_MAX_ATTEMPTS)
            .fetch_all(&self.pool),
        )
        .await?;
        // An unknown kind dead-letters ITS row only — one poisoned row
        // must never burn the batch's attempts (pre-deploy S2-1: a
        // "workspace-invite" kind once failed every claim in the
        // rhythm). The CHECK in 0003 makes new bad kinds impossible;
        // this stays as the belt for any written before it landed.
        let mut dead: Vec<i64> = Vec::new();
        for r in &rows {
            let kind: String = r.try_get("kind")?;
            match kind.as_str() {
                "verify" | "reset" | "invite" | "notice" => {}
                _ => dead.push(r.try_get("id")?),
            }
        }
        if !dead.is_empty() {
            bounded(
                sqlx::query(
                    "UPDATE email_outbox SET status = 'failed',                          text = '', html = '', subject = ''                      WHERE id = ANY($1) AND status = 'sending'",
                )
                .bind(&dead)
                .execute(&self.pool),
            )
            .await?;
        }
        rows.iter()
            .filter(|r| {
                let kind: String = r.try_get("kind").unwrap_or_default();
                matches!(kind.as_str(), "verify" | "reset" | "invite" | "notice")
            })
            .map(|r| {
                let kind: String = r.try_get("kind")?;
                let kind: &'static str = match kind.as_str() {
                    "verify" => "verify",
                    "reset" => "reset",
                    "invite" => "invite",
                    _ => "notice",
                };
                Ok(crate::OutboxMail {
                    id: to_u64(r.try_get("id")?, "outbox")?,
                    attempts: i64::from(r.try_get::<i32, _>("attempts")?),
                    mail: Mail {
                        kind,
                        send_to: r.try_get("send_to")?,
                        subject: r.try_get("subject")?,
                        text: r.try_get("text")?,
                        html: r.try_get("html")?,
                        idempotency_key: r.try_get("idempotency_key")?,
                        expires_at_ms: r
                            .try_get::<Option<time::OffsetDateTime>, _>("expires_at")?
                            .map(ms),
                    },
                })
            })
            .collect()
    }

    async fn outbox_sweep(&self, now_ms: i64) -> Result<u64, AccountError> {
        let exhausted = bounded(
            sqlx::query(
                "UPDATE email_outbox SET status = 'failed', \
                     last_error = COALESCE(last_error, 'attempts exhausted') \
                 WHERE status IN ('pending', 'sending') AND attempts >= $2",
            )
            .bind(ts(now_ms))
            .bind(crate::mail::OUTBOX_MAX_ATTEMPTS)
            .execute(&self.pool),
        )
        .await?;
        let expired = bounded(
            sqlx::query(
                "UPDATE email_outbox SET status = 'dropped', text = '', html = '' \
                 WHERE status IN ('pending', 'sending') \
                   AND expires_at IS NOT NULL AND expires_at <= $1",
            )
            .bind(ts(now_ms))
            .execute(&self.pool),
        )
        .await?;
        Ok(exhausted.rows_affected() + expired.rows_affected())
    }

    async fn outbox_purge(&self, now_ms: i64) -> Result<u64, AccountError> {
        let done = bounded(
            sqlx::query(
                "DELETE FROM email_outbox \
                 WHERE (status = 'sent' AND created_at < $1) \
                    OR (status IN ('failed', 'dropped') AND created_at < $2)",
            )
            .bind(ts(now_ms - 24 * 3600 * 1000))
            .bind(ts(now_ms - 7 * 24 * 3600 * 1000))
            .execute(&self.pool),
        )
        .await?;
        Ok(done.rows_affected())
    }

    async fn outbox_mark(&self, id: u64, outcome: MailOutcome) -> Result<(), AccountError> {
        match outcome {
            MailOutcome::Sent => {
                bounded(
                    sqlx::query(
                        "UPDATE email_outbox SET status = 'sent', sent_at = now(), \
                         last_error = NULL \
                         WHERE id = $1 AND status IN ('pending', 'sending')",
                    )
                    .bind(to_i64(id, "outbox")?)
                    .execute(&self.pool),
                )
                .await?;
            }
            MailOutcome::Retry {
                next_attempt_at_ms,
                error,
            } => {
                bounded(
                    sqlx::query(
                        "UPDATE email_outbox SET status = 'pending', \
                         attempts = attempts + 1, last_error = $2, next_attempt_at = $3 \
                         WHERE id = $1",
                    )
                    .bind(to_i64(id, "outbox")?)
                    .bind(error)
                    .bind(ts(next_attempt_at_ms))
                    .execute(&self.pool),
                )
                .await?;
            }
            MailOutcome::Failed { error } => {
                bounded(
                    sqlx::query(
                        "UPDATE email_outbox SET status = 'failed', last_error = $2 \
                         WHERE id = $1 AND status IN ('pending', 'sending')",
                    )
                    .bind(to_i64(id, "outbox")?)
                    .bind(error)
                    .execute(&self.pool),
                )
                .await?;
            }
        }
        Ok(())
    }
}

impl Pg {
    /// Supersede pending verify tokens, insert the fresh one, enqueue
    /// the mail — the unit `signup`/`reissue_verify` run inside their
    /// transactions.
    async fn issue_verify(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        account_id: i64,
        email: &str,
        token_hash: &[u8; 32],
        token_expires_at_ms: i64,
        pending_hash: Option<String>,
        verify_mail: &Mail,
    ) -> Result<Option<u64>, AccountError> {
        bounded(
            sqlx::query(
                "UPDATE email_tokens SET superseded_at = now() \
                 WHERE account_id = $1 AND kind = 'verify' \
                   AND consumed_at IS NULL AND superseded_at IS NULL",
            )
            .bind(account_id)
            .execute(&mut **tx),
        )
        .await?;
        bounded(
            sqlx::query(
                "INSERT INTO email_tokens \
                 (account_id, kind, token_hash, expires_at, sent_to, password_hash) \
                 VALUES ($1, 'verify', $2, $3, $4, $5)",
            )
            .bind(account_id)
            .bind(token_hash.as_slice())
            .bind(ts(token_expires_at_ms))
            .bind(email)
            .bind(pending_hash)
            .execute(&mut **tx),
        )
        .await?;
        Self::enqueue(tx, verify_mail).await
    }

    /// Enqueue inside the caller's transaction — the same dedupe as
    /// the memory impl: strict single addr-spec, newest-replaces-
    /// pending (an unclaimed row for the pair carries a dead link),
    /// the per-(address,kind) hourly cap counted over rows created in
    /// the window in ANY state, and the idempotency key's UNIQUE
    /// constraint. `None` = the mail coalesced; the fact it announced
    /// still commits.
    async fn enqueue(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        mail: &Mail,
    ) -> Result<Option<u64>, AccountError> {
        if mail.subject.len() > crate::mail::MAX_MAIL_SUBJECT
            || mail.text.len() > crate::mail::MAX_MAIL_BODY
            || mail.html.len() > crate::mail::MAX_MAIL_BODY
        {
            return Err(AccountError::Store(
                "mail exceeds the outbox size bound".to_string(),
            ));
        }
        if !crate::mail::valid_addr_spec(&mail.send_to) {
            return Ok(None);
        }
        let replaced = bounded(
            sqlx::query(
                "UPDATE email_outbox SET subject = $3, text = $4, html = $5, \
                     idempotency_key = $6, expires_at = $7, attempts = 0, \
                     next_attempt_at = now(), last_error = NULL \
                 WHERE send_to = $2 AND kind = $1 AND status = 'pending' \
                   AND NOT EXISTS (SELECT 1 FROM email_outbox o \
                       WHERE o.idempotency_key = $6 AND o.id <> email_outbox.id) \
                 RETURNING id",
            )
            .bind(mail.kind)
            .bind(&mail.send_to)
            .bind(&mail.subject)
            .bind(&mail.text)
            .bind(&mail.html)
            .bind(&mail.idempotency_key)
            .bind(mail.expires_at_ms.map(ts))
            .fetch_optional(&mut **tx),
        )
        .await?;
        if let Some(row) = replaced {
            let id: i64 = row.try_get("id")?;
            return Ok(Some(to_u64(id, "outbox")?));
        }
        let row = bounded(
            sqlx::query(
                "INSERT INTO email_outbox \
                 (kind, send_to, subject, text, html, idempotency_key, expires_at) \
                 SELECT $1, $2, $3, $4, $5, $6, $7 \
                 WHERE (SELECT count(*) FROM email_outbox \
                        WHERE send_to = $2 AND kind = $1 \
                          AND created_at > now() - interval '1 hour') < $8 \
                 ON CONFLICT (idempotency_key) DO NOTHING \
                 RETURNING id",
            )
            .bind(mail.kind)
            .bind(&mail.send_to)
            .bind(&mail.subject)
            .bind(&mail.text)
            .bind(&mail.html)
            .bind(&mail.idempotency_key)
            .bind(mail.expires_at_ms.map(ts))
            .bind(crate::mail::MAX_HOURLY_PER_ADDR)
            .fetch_optional(&mut **tx),
        )
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let id: i64 = row.try_get("id")?;
        Ok(Some(to_u64(id, "outbox")?))
    }
}

/// Postgres-backed tests run only when `DURABLE_TEST_DATABASE_URL` points
/// at a scratch database (local Postgres in development, Neon in CI).
/// Each is `#[ignore]`d so a plain `cargo test` counts them as skipped in
/// the result line — an `ok` line can never again read as real-dialect
/// evidence. The neon CI job runs them with `--include-ignored` and sets
/// `DURABLE_REQUIRE_PG=1`, which turns a missing URL into a failure
/// instead of a skip.
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn url() -> Option<String> {
        match std::env::var("DURABLE_TEST_DATABASE_URL") {
            Ok(url) if !url.is_empty() => Some(url),
            _ if std::env::var("DURABLE_REQUIRE_PG").as_deref() == Ok("1") => {
                panic!(
                    "DURABLE_REQUIRE_PG=1 but DURABLE_TEST_DATABASE_URL is not set — \
                     the pg tests were required to run for real"
                );
            }
            _ => {
                eprintln!(
                    "DURABLE_TEST_DATABASE_URL not set — skipping Postgres-backed account tests"
                );
                None
            }
        }
    }

    async fn store() -> Option<Pg> {
        let url = url()?;
        let pg = Pg::connect(&url).unwrap_or_else(|e| panic!("parse scratch database url: {e}"));
        pg.migrate()
            .await
            .unwrap_or_else(|e| panic!("migrate scratch database: {e}"));
        Some(pg)
    }

    fn hash(seed: u8) -> [u8; 32] {
        [seed; 32]
    }

    /// S2 review #5: the store carries handler secrets and key grants —
    /// a non-loopback connection must *verify* TLS, not merely encrypt.
    /// The refusal happens at connect() (configuration), so a bad URL
    /// fails startup loudly instead of downgrading quietly later.
    #[tokio::test]
    async fn non_loopback_requires_verify_full() {
        let refused = |u: &str| {
            let e = Pg::connect(u).err().unwrap_or_else(|| {
                panic!("{u} must be refused");
            });
            assert!(
                e.to_string().contains("verify-full"),
                "{u}: refusal must name the fix: {e}"
            );
        };
        // Encrypted-but-unverified is not enough: the certificate is
        // never checked, so a forged endpoint reads everything.
        refused("postgres://u:p@db.example.com/d?sslmode=require");
        refused("postgres://u:p@db.example.com/d?sslmode=verify-ca");
        refused("postgres://u:p@db.example.com/d?sslmode=prefer");
        refused("postgres://u:p@db.example.com/d?sslmode=disable");
        // No sslmode at all defaults to prefer — downgradeable.
        refused("postgres://u:p@db.example.com/d");
        refused("postgres://u:p@[2606:4700::1]/d?sslmode=require");

        assert!(
            Pg::connect("postgres://u:p@db.example.com/d?sslmode=verify-full").is_ok(),
            "verify-full is the only accepted non-loopback mode"
        );
        // Only literal loopback IPs are exempt — any mode allowed.
        for u in [
            "postgres://u:p@127.0.0.1/d",
            "postgres://u:p@[::1]/d?sslmode=disable",
        ] {
            assert!(Pg::connect(u).is_ok(), "{u} must connect");
        }
        // "localhost" is a name, not an address: it resolves wherever
        // the resolver points, so it needs verify-full like any name.
        refused("postgres://u:p@localhost/d");
        refused("postgres://u:p@localhost/d?sslmode=require");
    }

    #[tokio::test]
    #[ignore = "needs DURABLE_TEST_DATABASE_URL (CI: neon job runs --include-ignored)"]
    async fn pg_the_default_workspace_is_seeded() {
        let Some(store) = store().await else { return };
        let ws = store
            .workspace_by_id(0)
            .await
            .unwrap_or_else(|e| panic!("lookup: {e}"));
        assert!(matches!(ws, Some(Workspace { id: 0, .. })));
    }

    #[tokio::test]
    #[ignore = "needs DURABLE_TEST_DATABASE_URL (CI: neon job runs --include-ignored)"]
    async fn pg_a_created_key_verifies_and_revokes() {
        let Some(store) = store().await else { return };
        let ws = store
            .create_workspace("pg-alpha")
            .await
            .unwrap_or_else(|e| panic!("ws: {e}"));
        let info = store
            .create_key(ws.id, "ci", Permission::Write, &hash(11), "dr_pg", None)
            .await
            .unwrap_or_else(|e| panic!("key: {e}"));
        let grant = store
            .verify_key(&hash(11))
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
                .verify_key(&hash(11))
                .await
                .unwrap_or_else(|e| panic!("verify: {e}")),
            None
        );
    }

    #[tokio::test]
    #[ignore = "needs DURABLE_TEST_DATABASE_URL (CI: neon job runs --include-ignored)"]
    async fn pg_handlers_upsert_and_scope_per_workspace() {
        let Some(store) = store().await else { return };
        let a = store
            .create_workspace("pg-a")
            .await
            .unwrap_or_else(|e| panic!("a: {e}"));
        let b = store
            .create_workspace("pg-b")
            .await
            .unwrap_or_else(|e| panic!("b: {e}"));
        store
            .put_handler(a.id, "h", "https://a.example/h")
            .await
            .unwrap_or_else(|e| panic!("put: {e}"));
        store
            .put_handler(a.id, "h", "https://a.example/h2")
            .await
            .unwrap_or_else(|e| panic!("upsert: {e}"));
        assert_eq!(
            store
                .list_handlers(a.id)
                .await
                .unwrap_or_else(|e| panic!("list: {e}")),
            vec![HandlerRegistration {
                name: "h".into(),
                url: "https://a.example/h2".into(),
                last_test_at_ms: None,
                last_test_status: None,
                last_test_latency_ms: None,
            }]
        );
        assert!(store
            .list_handlers(b.id)
            .await
            .unwrap_or_else(|e| panic!("list: {e}"))
            .is_empty());
        assert!(store
            .delete_handler(a.id, "h")
            .await
            .unwrap_or_else(|e| panic!("del: {e}")));
        assert!(!store
            .delete_handler(a.id, "h")
            .await
            .unwrap_or_else(|e| panic!("del: {e}")));
    }

    /// The re-review's race: under READ COMMITTED each PUT's count(*)
    /// saw its own snapshot, so a concurrent burst at the boundary all
    /// observed "under" and all inserted — overshooting the bound. The
    /// advisory-lock transaction makes count-and-insert one critical
    /// section: at the boundary exactly ONE concurrent put may land.
    #[tokio::test]
    #[ignore = "needs DURABLE_TEST_DATABASE_URL (CI: neon job runs --include-ignored)"]
    async fn pg_a_put_burst_at_the_bound_admits_exactly_one() {
        let Some(store) = store().await else { return };
        let store = Arc::new(store);
        let ws = store
            .create_workspace("pg-burst")
            .await
            .unwrap_or_else(|e| panic!("ws: {e}"));
        for i in 0..crate::MAX_HANDLERS_PER_WS - 1 {
            store
                .put_handler(ws.id, &format!("h{i}"), "https://h.example/x")
                .await
                .unwrap_or_else(|e| panic!("fill {i}: {e}"));
        }
        let mut set = tokio::task::JoinSet::new();
        for i in 0..8 {
            let store = Arc::clone(&store);
            set.spawn(async move {
                store
                    .put_handler(ws.id, &format!("burst{i}"), "https://h.example/y")
                    .await
            });
        }
        let mut ok = 0;
        while let Some(r) = set.join_next().await {
            match r.unwrap_or_else(|e| panic!("join: {e}")) {
                Ok(()) => ok += 1,
                Err(AccountError::RegistryFull) => {}
                Err(e) => panic!("unexpected put failure: {e}"),
            }
        }
        assert_eq!(ok, 1, "exactly one of the burst may land at the bound");
        assert_eq!(
            store
                .list_handlers(ws.id)
                .await
                .unwrap_or_else(|e| panic!("list: {e}"))
                .len(),
            crate::MAX_HANDLERS_PER_WS,
            "the bound admits exactly MAX_HANDLERS_PER_WS, never more"
        );
    }

    fn mail(kind: &'static str, to: &str) -> Mail {
        Mail {
            kind,
            send_to: to.to_string(),
            subject: format!("{kind} mail"),
            text: "body".into(),
            html: "<p>body</p>".into(),
            idempotency_key: format!("{kind}:{to}"),
            expires_at_ms: None,
        }
    }

    /// S3-0's new surface end to end on the real dialect: signup with the
    /// pending hash on the verify token, a session grant joined to its
    /// account, an unknown hash's verified negative, revoke, re-sign-up
    /// superseding the token, ws-0 membership impossible, and the outbox
    /// mark's idempotence — the same contract the Memory impl is
    /// fuzz-checked against, proved where the SQL actually runs.
    #[tokio::test]
    #[ignore = "needs DURABLE_TEST_DATABASE_URL (CI: neon job runs --include-ignored)"]
    async fn pg_signup_session_and_revoke_round_trip() {
        let Some(store) = store().await else { return };
        let email = format!("pg-s30-{}@example.com", std::process::id());
        let verify = mail("verify", &email);
        let notice = mail("notice", &email);
        let out = store
            .signup(
                &email,
                "$argon2id$v=19$m=19456,t=2,p=1$AAAAAAAAAAA$fakehashfakehashfakehashfakehashfakeh",
                &hash(1),
                1_800_000_000_000,
                &verify,
                &notice,
            )
            .await
            .unwrap_or_else(|e| panic!("signup: {e}"));
        let SignupOutcome::Created {
            outbox: Some(mail_id),
        } = out
        else {
            panic!("first signup must be Created with a queued mail: {out:?}")
        };
        let account = store
            .account_by_email(&email)
            .await
            .unwrap_or_else(|e| panic!("by email: {e}"))
            .unwrap_or_else(|| panic!("the account must exist"));
        assert!(!account.email_verified);
        store
            .create_session(&crate::NewSession {
                account: account.id,
                token_hash: hash(7),
                csrf_seed: hash(9),
                expires_at_ms: 4_000_000_000_000,
                idle_expires_at_ms: 4_000_000_000_000,
                replaces: None,
                user_agent: Some("pg-test".into()),
                ip: Some(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
            })
            .await
            .unwrap_or_else(|e| panic!("create session: {e}"));
        let grant = store
            .session_grant(&hash(7))
            .await
            .unwrap_or_else(|e| panic!("grant: {e}"))
            .unwrap_or_else(|| panic!("the live session must resolve"));
        assert_eq!(grant.account.id, account.id);
        assert_eq!(grant.csrf_seed, hash(9));
        // A verified negative is Some(None)-shaped: unknown hash → None,
        // never an error and never a different account.
        assert!(store
            .session_grant(&hash(8))
            .await
            .unwrap_or_else(|e| panic!("grant: {e}"))
            .is_none());
        // ws 0 cannot hold a member row (the CHECK), and ws ≠ 0 without a
        // row answers None — both are the ops-plane invariant, not errors.
        assert_eq!(
            store
                .member_role(account.id, 0)
                .await
                .unwrap_or_else(|e| panic!("role ws0: {e}")),
            None
        );
        assert!(store
            .revoke_session(&hash(7))
            .await
            .unwrap_or_else(|e| panic!("revoke: {e}")));
        assert!(!store
            .revoke_session(&hash(7))
            .await
            .unwrap_or_else(|e| panic!("revoke twice: {e}")));
        assert!(store
            .session_grant(&hash(7))
            .await
            .unwrap_or_else(|e| panic!("grant: {e}"))
            .is_none());
        // Re-sign-up of an unverified address supersedes the token and —
        // review #2 — revokes every live session the sign-up could have
        // minted for an attacker who registered someone else's address.
        store
            .create_session(&crate::NewSession {
                account: account.id,
                token_hash: hash(10),
                csrf_seed: hash(9),
                expires_at_ms: 4_000_000_000_000,
                idle_expires_at_ms: 4_000_000_000_000,
                replaces: None,
                user_agent: None,
                ip: None,
            })
            .await
            .unwrap_or_else(|e| panic!("create session 2: {e}"));
        let out = store
            .signup(
                &email,
                "$argon2id$v=19$m=19456,t=2,p=1$BBBBBBBBBBB$otherhashotherhashotherhashotherhash",
                &hash(2),
                1_800_000_000_000,
                &verify,
                &notice,
            )
            .await
            .unwrap_or_else(|e| panic!("re-signup: {e}"));
        assert!(
            matches!(out, SignupOutcome::VerifyResent { .. }),
            "unverified re-signup must resend: {out:?}"
        );
        assert!(
            store
                .session_grant(&hash(10))
                .await
                .unwrap_or_else(|e| panic!("grant: {e}"))
                .is_none(),
            "re-sign-up revoked the live session"
        );
        // The outbox row marks Sent once; a second mark is a no-op, never
        // a state regression (the drain may race a manual send).
        store
            .outbox_mark(mail_id, MailOutcome::Sent)
            .await
            .unwrap_or_else(|e| panic!("mark: {e}"));
        store
            .outbox_mark(mail_id, MailOutcome::Sent)
            .await
            .unwrap_or_else(|e| panic!("mark twice: {e}"));
        store
            .revoke_sessions_for_account(account.id)
            .await
            .unwrap_or_else(|e| panic!("revoke all: {e}"));
    }

    /// S3-1's consume/issue/outbox surface on the real dialect: verify
    /// consumes once and revokes sessions, a replay and a reset-kind
    /// token both answer the one `None`, reset installs the password
    /// and kills siblings, and the outbox claim/mark/sweep cycle
    /// matches the Memory contract.
    #[tokio::test]
    #[ignore = "needs DURABLE_TEST_DATABASE_URL (CI: neon job runs --include-ignored)"]
    async fn pg_verify_reset_outbox_round_trip() {
        let Some(store) = store().await else { return };
        let email = format!("pg-s31-{}@example.com", std::process::id());
        let verify = mail("verify", &email);
        let notice = mail("notice", &email);
        let pending =
            "$argon2id$v=19$m=19456,t=2,p=1$AAAAAAAAAAA$pendinghashpendinghashpendinghashpend";
        store
            .signup(
                &email,
                pending,
                &hash(1),
                4_000_000_000_000,
                &verify,
                &notice,
            )
            .await
            .unwrap_or_else(|e| panic!("signup: {e}"));
        let account = store
            .account_by_email(&email)
            .await
            .unwrap_or_else(|e| panic!("email: {e}"))
            .unwrap_or_else(|| panic!("account"));
        store
            .create_session(&crate::NewSession {
                account: account.id,
                token_hash: hash(7),
                csrf_seed: hash(9),
                expires_at_ms: 4_000_000_000_000,
                idle_expires_at_ms: 4_000_000_000_000,
                replaces: None,
                user_agent: None,
                ip: None,
            })
            .await
            .unwrap_or_else(|e| panic!("session: {e}"));
        // A reset-kind token is not a verify token — the kind column is
        // bound inside the consume's WHERE (token-kind-confusion).
        store
            .issue_reset_token(&email, &hash(50), 4_000_000_000_000, &mail("reset", &email))
            .await
            .unwrap_or_else(|e| panic!("reset issue: {e}"));
        assert!(store
            .consume_verify_token(&hash(50))
            .await
            .unwrap_or_else(|e| panic!("verify reset-kind: {e}"))
            .is_none());
        // The real verify token: consumes, installs the pending hash,
        // verifies, revokes the session — one transaction.
        let verified = store
            .consume_verify_token(&hash(1))
            .await
            .unwrap_or_else(|e| panic!("verify: {e}"))
            .unwrap_or_else(|| panic!("the live token must consume"));
        assert!(verified.email_verified);
        assert_eq!(verified.password_hash.as_deref(), Some(pending));
        assert!(store
            .session_grant(&hash(7))
            .await
            .unwrap_or_else(|e| panic!("grant: {e}"))
            .is_none());
        // Replay: the consumed token answers the same `None` as unknown.
        assert!(store
            .consume_verify_token(&hash(1))
            .await
            .unwrap_or_else(|e| panic!("replay: {e}"))
            .is_none());
        assert!(store
            .consume_verify_token(&hash(99))
            .await
            .unwrap_or_else(|e| panic!("unknown: {e}"))
            .is_none());
        // Reset: supersedes siblings, installs the hash, verifies,
        // revokes — then the verify-side pending row can never land.
        let reset = store
            .issue_reset_token(&email, &hash(60), 4_000_000_000_000, &mail("reset", &email))
            .await
            .unwrap_or_else(|e| panic!("reset issue 2: {e}"));
        assert!(reset.is_some());
        store
            .create_session(&crate::NewSession {
                account: account.id,
                token_hash: hash(8),
                csrf_seed: hash(9),
                expires_at_ms: 4_000_000_000_000,
                idle_expires_at_ms: 4_000_000_000_000,
                replaces: None,
                user_agent: None,
                ip: None,
            })
            .await
            .unwrap_or_else(|e| panic!("session 2: {e}"));
        let new_hash =
            "$argon2id$v=19$m=19456,t=2,p=1$CCCCCCCCCCC$newpasswordhashnewpasswordhashnewpa";
        let done = store
            .consume_reset_token(&hash(60), new_hash)
            .await
            .unwrap_or_else(|e| panic!("reset consume: {e}"))
            .unwrap_or_else(|| panic!("the live reset must consume"));
        assert_eq!(done.password_hash.as_deref(), Some(new_hash));
        assert!(done.email_verified, "the mailbox proof verifies");
        assert!(store
            .session_grant(&hash(8))
            .await
            .unwrap_or_else(|e| panic!("grant 2: {e}"))
            .is_none());
        assert!(store
            .consume_reset_token(&hash(60), new_hash)
            .await
            .unwrap_or_else(|e| panic!("reset replay: {e}"))
            .is_none());
        // The outbox cycle: claim → send-mark → sent rows are not
        // reclaimed; a refused mail is `failed` terminal.
        let claimed = store
            .outbox_claim(unix_ms(), 10, 30_000)
            .await
            .unwrap_or_else(|e| panic!("claim: {e}"));
        assert!(!claimed.is_empty(), "pending mails claim");
        for row in &claimed {
            store
                .outbox_mark(row.id, MailOutcome::Sent)
                .await
                .unwrap_or_else(|e| panic!("mark: {e}"));
        }
        assert_eq!(
            store
                .outbox_claim(unix_ms(), 10, 30_000)
                .await
                .unwrap_or_else(|e| panic!("reclaim: {e}"))
                .len(),
            0,
            "sent rows are not reclaimed"
        );
        let bad = mail("notice", "bad@example.com");
        let bad_id = store
            .enqueue_mail(&bad)
            .await
            .unwrap_or_else(|e| panic!("bad enqueue: {e}"))
            .unwrap_or_else(|| panic!("a valid addr-spec enqueues"));
        let claimed = store
            .outbox_claim(unix_ms(), 10, 30_000)
            .await
            .unwrap_or_else(|e| panic!("claim 2: {e}"));
        assert_eq!(claimed.len(), 1);
        store
            .outbox_mark(
                bad_id,
                MailOutcome::Failed {
                    error: "provider 4xx".into(),
                },
            )
            .await
            .unwrap_or_else(|e| panic!("fail mark: {e}"));
        assert_eq!(
            store
                .outbox_claim(unix_ms(), 10, 30_000)
                .await
                .unwrap_or_else(|e| panic!("reclaim 2: {e}"))
                .len(),
            0,
            "a failed row is terminal"
        );
    }

    /// S3-2's session surface on the real dialect: the account-scoped
    /// list, revoke-one bound inside the lookup (a foreign public_id
    /// answers the same None as an unknown), revoke-all-but-one,
    /// change_password's hash+supersede+revoke transaction, the
    /// monotonic touch slide, and `replaced_by` rotation linkage.
    #[tokio::test]
    #[ignore = "needs DURABLE_TEST_DATABASE_URL (CI: neon job runs --include-ignored)"]
    async fn pg_session_lifecycle() {
        let Some(store) = store().await else { return };
        let email = format!("pg-s32-{}@example.com", std::process::id());
        let verify = mail("verify", &email);
        let notice = mail("notice", &email);
        store
            .signup(
                &email,
                "$argon2id$v=19$m=19456,t=2,p=1$AAAAAAAAAAA$fakehashfakehashfakehashfakehashfakeh",
                &hash(1),
                1_800_000_000_000,
                &verify,
                &notice,
            )
            .await
            .unwrap_or_else(|e| panic!("signup: {e}"));
        let account = store
            .account_by_email(&email)
            .await
            .unwrap_or_else(|e| panic!("by email: {e}"))
            .unwrap_or_else(|| panic!("account"));
        let session = |token_hash: [u8; 32]| crate::NewSession {
            account: account.id,
            token_hash,
            csrf_seed: hash(9),
            expires_at_ms: 4_000_000_000_000,
            idle_expires_at_ms: 4_000_000_000_000,
            replaces: None,
            user_agent: Some("pg-test".into()),
            ip: None,
        };
        store
            .create_session(&session(hash(7)))
            .await
            .unwrap_or_else(|e| panic!("s1: {e}"));
        store
            .create_session(&session(hash(8)))
            .await
            .unwrap_or_else(|e| panic!("s2: {e}"));
        // The list answers both rows, newest first, ids opaque.
        let rows = store
            .sessions_for_account(account.id, 10)
            .await
            .unwrap_or_else(|e| panic!("list: {e}"));
        assert_eq!(rows.len(), 2, "two live sessions list");
        assert!(rows.iter().all(|r| r.created_at_ms > 0));
        let s1_public = store
            .session_grant(&hash(7))
            .await
            .unwrap_or_else(|e| panic!("grant: {e}"))
            .unwrap_or_else(|| panic!("live"))
            .public_id;
        let s2_public = store
            .session_grant(&hash(8))
            .await
            .unwrap_or_else(|e| panic!("grant: {e}"))
            .unwrap_or_else(|| panic!("live"))
            .public_id;
        // A second account's sessions can't be revoked through this
        // account's binding: the public_id answers None — the same as
        // an unknown one — and the row lives.
        let email2 = format!("pg-s32b-{}@example.com", std::process::id());
        let verify2 = mail("verify", &email2);
        let notice2 = mail("notice", &email2);
        store
            .signup(
                &email2,
                "$argon2id$v=19$m=19456,t=2,p=1$AAAAAAAAAAA$fakehashfakehashfakehashfakehashfakeh",
                &hash(2),
                1_800_000_000_000,
                &verify2,
                &notice2,
            )
            .await
            .unwrap_or_else(|e| panic!("signup 2: {e}"));
        let account2 = store
            .account_by_email(&email2)
            .await
            .unwrap_or_else(|e| panic!("by email 2: {e}"))
            .unwrap_or_else(|| panic!("account 2"));
        store
            .create_session(&crate::NewSession {
                account: account2.id,
                ..session(hash(11))
            })
            .await
            .unwrap_or_else(|e| panic!("other session: {e}"));
        assert!(store
            .revoke_session_public(account.id, uuid::Uuid::now_v7())
            .await
            .unwrap_or_else(|e| panic!("unknown id: {e}"))
            .is_none());
        assert!(store
            .revoke_session_public(account.id, s2_public)
            .await
            .unwrap_or_else(|e| panic!("foreign? — s2 IS account's: {e}"))
            .is_some());
        // The foreign session of account2 survives account's binding:
        // account can't reach it even if it learned the public_id.
        let foreign_public = store
            .session_grant(&hash(11))
            .await
            .unwrap_or_else(|e| panic!("foreign grant: {e}"))
            .unwrap_or_else(|| panic!("live"))
            .public_id;
        assert!(store
            .revoke_session_public(account.id, foreign_public)
            .await
            .unwrap_or_else(|e| panic!("idor: {e}"))
            .is_none());
        assert!(store
            .session_grant(&hash(11))
            .await
            .unwrap_or_else(|e| panic!("foreign still live: {e}"))
            .is_some());
        // Revoke-all-but-one keeps the named survivor.
        let n = store
            .revoke_sessions_except(account.id, s1_public)
            .await
            .unwrap_or_else(|e| panic!("except: {e}"));
        assert_eq!(n, 0, "s2 was already revoked; only s1 could die — kept");
        assert!(store
            .session_grant(&hash(7))
            .await
            .unwrap_or_else(|e| panic!("survivor: {e}"))
            .is_some());
        // The rotation linkage: a session minted with `replaces` chains
        // back to the row it supersedes.
        store
            .create_session(&crate::NewSession {
                replaces: Some(
                    store
                        .session_grant(&hash(7))
                        .await
                        .unwrap_or_else(|e| panic!("old: {e}"))
                        .unwrap_or_else(|| panic!("live"))
                        .session_id,
                ),
                ..session(hash(12))
            })
            .await
            .unwrap_or_else(|e| panic!("rotated: {e}"));
        // change_password installs the hash, supersedes pending tokens
        // and revokes every live session in one transaction.
        store
            .change_password(
                account.id,
                "$argon2id$v=19$m=19456,t=2,p=1$DDDDDDDDDDD$changedhashchangedhashchangedhashchang",
            )
            .await
            .unwrap_or_else(|e| panic!("change: {e}"));
        let after = store
            .account_by_email(&email)
            .await
            .unwrap_or_else(|e| panic!("after: {e}"))
            .unwrap_or_else(|| panic!("account"));
        assert!(after
            .password_hash
            .as_deref()
            .is_some_and(|h| h.contains("changedhash")));
        assert!(store
            .session_grant(&hash(7))
            .await
            .unwrap_or_else(|e| panic!("old dead: {e}"))
            .is_none());
        assert!(store
            .session_grant(&hash(12))
            .await
            .unwrap_or_else(|e| panic!("rotated dead: {e}"))
            .is_none());
        // The pending verify token died with the change (superseded) —
        // its stale pending hash can never revert this password.
        assert!(store
            .consume_verify_token(&hash(1))
            .await
            .unwrap_or_else(|e| panic!("superseded verify: {e}"))
            .is_none());
        // The touch slides monotonically — a stale (smaller) slide
        // never walks the deadline backward.
        store
            .create_session(&session(hash(13)))
            .await
            .unwrap_or_else(|e| panic!("s4: {e}"));
        store
            .touch_sessions(&[
                crate::SessionTouch {
                    token_hash: hash(13),
                    seen_ms: 5_000_000_000_000,
                    idle_expires_at_ms: 5_000_000_000_000,
                },
                crate::SessionTouch {
                    token_hash: hash(13),
                    seen_ms: 1_000,
                    idle_expires_at_ms: 1_000,
                },
            ])
            .await
            .unwrap_or_else(|e| panic!("touch: {e}"));
        let touched = store
            .session_grant(&hash(13))
            .await
            .unwrap_or_else(|e| panic!("touched: {e}"))
            .unwrap_or_else(|| panic!("live"));
        assert_eq!(
            touched.idle_expires_at_ms, 5_000_000_000_000,
            "GREATEST keeps the furthest slide — never backward"
        );
    }

    /// Pre-deploy S2-1: a claim batch with one unknown-kind row returns
    /// the good rows and fails ONLY the poisoned one — never the whole
    /// rhythm. The CHECK in 0003 makes a bad kind unwritable, so the
    /// test drops it for the fixture then restores it.
    #[tokio::test]
    #[ignore = "needs DURABLE_TEST_DATABASE_URL (CI: neon job runs --include-ignored)"]
    async fn pg_outbox_claim_dead_letters_a_bad_kind_not_the_batch() {
        let Some(store) = store().await else { return };
        sqlx::query("ALTER TABLE email_outbox DROP CONSTRAINT email_outbox_kind_ok")
            .execute(&store.pool)
            .await
            .unwrap_or_else(|e| panic!("drop check: {e}"));
        let unique = format!("pg-s2-{}", std::process::id());
        sqlx::query(
            "INSERT INTO email_outbox \
             (kind, send_to, subject, text, html, idempotency_key, status) \
             VALUES ('bogus-kind', 'b@b.co', 's', 't', '', $1, 'pending')",
        )
        .bind(format!("{unique}-bad"))
        .execute(&store.pool)
        .await
        .unwrap_or_else(|e| panic!("plant bad kind: {e}"));
        sqlx::query(
            "ALTER TABLE email_outbox ADD CONSTRAINT email_outbox_kind_ok \
             CHECK (kind IN ('verify', 'reset', 'invite', 'notice'))",
        )
        .execute(&store.pool)
        .await
        .unwrap_or_else(|e| panic!("restore check: {e}"));
        let good = mail("notice", "g@g.co");
        store
            .enqueue_mail(&Mail {
                idempotency_key: format!("{unique}-good"),
                ..good
            })
            .await
            .unwrap_or_else(|e| panic!("enqueue: {e}"));
        // The claim returns the good row; the batch does not error.
        let claimed = store
            .outbox_claim(unix_ms(), 8, 60_000)
            .await
            .unwrap_or_else(|e| panic!("claim must not fail the batch: {e}"));
        assert!(
            claimed.iter().any(|m| m.mail.send_to == "g@g.co"),
            "the good row was claimed"
        );
        assert!(
            claimed
                .iter()
                .all(|m| { matches!(m.mail.kind, "verify" | "reset" | "invite" | "notice") }),
            "no bad-kind row crosses the boundary"
        );
        // The poisoned row is 'failed' — it can never claim again.
        let bad: (String,) =
            sqlx::query_as("SELECT status FROM email_outbox WHERE idempotency_key = $1")
                .bind(format!("{unique}-bad"))
                .fetch_one(&store.pool)
                .await
                .unwrap_or_else(|e| panic!("read bad row: {e}"));
        assert_eq!(bad.0, "failed");
    }

    fn unix_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }
}
