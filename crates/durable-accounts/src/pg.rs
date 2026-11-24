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
            sqlx::query("SELECT id, public_id, name, handler_secret FROM workspaces WHERE id = $1")
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
                "SELECT id, public_id, name, handler_secret FROM workspaces WHERE public_id = $1",
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
                "SELECT id, public_id, name, handler_secret FROM workspaces \
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
    ) -> Result<ApiKeyInfo, AccountError> {
        let row = bounded(
            sqlx::query(
                "INSERT INTO api_keys (workspace_id, name, key_hash, prefix, permission) \
                 VALUES ($1, $2, $3, $4, $5) RETURNING id",
            )
            .bind(to_i64(workspace, "workspace")?)
            .bind(name)
            .bind(key_hash.as_slice())
            .bind(prefix)
            .bind(permission.as_str())
            .fetch_one(&self.pool),
        )
        .await?;
        let id: i64 = row.try_get("id")?;
        Ok(ApiKeyInfo {
            id: to_u64(id, "api key")?,
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
                "SELECT id, name, prefix, permission, (revoked_at IS NOT NULL) AS revoked \
                 FROM api_keys WHERE workspace_id = $1 ORDER BY id",
            )
            .bind(to_i64(workspace, "workspace")?)
            .fetch_all(&self.pool),
        )
        .await?;
        rows.iter()
            .map(|row| {
                Ok(ApiKeyInfo {
                    id: to_u64(row.try_get("id")?, "api key")?,
                    name: row.try_get("name")?,
                    prefix: row.try_get("prefix")?,
                    permission: Permission::parse(row.try_get::<&str, _>("permission")?)?,
                    revoked: row.try_get("revoked")?,
                })
            })
            .collect()
    }

    async fn list_handlers(
        &self,
        workspace: u64,
    ) -> Result<Vec<HandlerRegistration>, AccountError> {
        let rows = bounded(
            sqlx::query(
                "SELECT name, url FROM handler_registrations \
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
                password_hash,
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
                outbox: Self::issue_verify(
                    &mut tx,
                    account_id,
                    email,
                    token_hash,
                    token_expires_at_ms,
                    password_hash,
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
                  last_seen_at, user_agent, ip) \
                 VALUES ($1, $2, $3, $4, $5, now(), $6, $7::inet)",
            )
            .bind(to_i64(session.account, "account")?)
            .bind(session.token_hash.as_slice())
            .bind(session.csrf_seed.as_slice())
            .bind(ts(session.expires_at_ms))
            .bind(ts(session.idle_expires_at_ms))
            .bind(session.user_agent.as_deref())
            .bind(session.ip.map(|a| a.to_string()))
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

    async fn outbox_mark(&self, id: u64, outcome: MailOutcome) -> Result<(), AccountError> {
        match outcome {
            MailOutcome::Sent => {
                bounded(
                    sqlx::query(
                        "UPDATE email_outbox SET status = 'sent', sent_at = now(), \
                         attempts = attempts + 1, last_error = NULL \
                         WHERE id = $1 AND status = 'pending'",
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
        }
        Ok(())
    }
}

impl Pg {
    /// Supersede pending verify tokens, insert the fresh one, enqueue
    /// the mail — the unit `signup` runs inside its transaction.
    async fn issue_verify(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        account_id: i64,
        email: &str,
        token_hash: &[u8; 32],
        token_expires_at_ms: i64,
        password_hash: &str,
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
            .bind(password_hash)
            .execute(&mut **tx),
        )
        .await?;
        Self::enqueue(tx, verify_mail).await
    }

    /// The per-address pending cap, inside the caller's transaction:
    /// at the cap the mail coalesces into the pending rows it would
    /// duplicate — `None`, and the outbox stays bounded. The advisory
    /// lock `signup` already took on the address serializes this count.
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
        let row = bounded(
            sqlx::query(
                "INSERT INTO email_outbox \
                 (kind, send_to, subject, text, html, idempotency_key) \
                 SELECT $1, $2, $3, $4, $5, $6 \
                 WHERE (SELECT count(*) FROM email_outbox \
                        WHERE send_to = $2 AND status = 'pending') < $7 \
                 RETURNING id",
            )
            .bind(mail.kind)
            .bind(&mail.send_to)
            .bind(&mail.subject)
            .bind(&mail.text)
            .bind(&mail.html)
            .bind(&mail.idempotency_key)
            .bind(crate::mail::MAX_PENDING_PER_ADDR)
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
/// Without it they report a visible skip so a missing secret never reads
/// as a silent pass.
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn url() -> Option<String> {
        match std::env::var("DURABLE_TEST_DATABASE_URL") {
            Ok(url) if !url.is_empty() => Some(url),
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
    async fn pg_the_default_workspace_is_seeded() {
        let Some(store) = store().await else { return };
        let ws = store
            .workspace_by_id(0)
            .await
            .unwrap_or_else(|e| panic!("lookup: {e}"));
        assert!(matches!(ws, Some(Workspace { id: 0, .. })));
    }

    #[tokio::test]
    async fn pg_a_created_key_verifies_and_revokes() {
        let Some(store) = store().await else { return };
        let ws = store
            .create_workspace("pg-alpha")
            .await
            .unwrap_or_else(|e| panic!("ws: {e}"));
        let info = store
            .create_key(ws.id, "ci", Permission::Write, &hash(11), "dr_pg")
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
                url: "https://a.example/h2".into()
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
}
