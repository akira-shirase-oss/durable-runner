use std::str::FromStr;
use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use sqlx::{PgPool, Row};

use crate::{
    AccountError, AccountStore, ApiKeyInfo, HandlerRegistration, KeyGrant, Permission, Workspace,
    STORE_DEADLINE,
};

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
    /// endpoint. Loopback hosts (local development) are exempt.
    pub fn connect(url: &str) -> Result<Self, AccountError> {
        let opts = PgConnectOptions::from_str(url)
            .map_err(|e| AccountError::Store(format!("unusable DATABASE_URL: {e}")))?;
        let host = opts.get_host();
        let bare = host.trim_start_matches('[').trim_end_matches(']');
        let loopback = host.eq_ignore_ascii_case("localhost")
            || bare
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

    async fn list_workspaces(&self, limit: u32) -> Result<Vec<Workspace>, AccountError> {
        let rows = bounded(
            sqlx::query(
                "SELECT id, public_id, name, handler_secret FROM workspaces \
                 ORDER BY id LIMIT $1",
            )
            .bind(i64::from(limit))
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
        bounded(
            sqlx::query(
                "INSERT INTO handler_registrations (workspace_id, name, url) \
                 VALUES ($1, $2, $3) \
                 ON CONFLICT (workspace_id, name) \
                 DO UPDATE SET url = $3, updated_at = now()",
            )
            .bind(to_i64(workspace, "workspace")?)
            .bind(name)
            .bind(url)
            .execute(&self.pool),
        )
        .await?;
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
}

/// Postgres-backed tests run only when `DURABLE_TEST_DATABASE_URL` points
/// at a scratch database (local Postgres in development, Neon in CI).
/// Without it they report a visible skip so a missing secret never reads
/// as a silent pass.
#[cfg(test)]
mod tests {
    use super::*;

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
        // Loopback development targets are exempt — any mode allowed.
        for u in [
            "postgres://u:p@localhost/d",
            "postgres://u:p@127.0.0.1/d",
            "postgres://u:p@[::1]/d?sslmode=disable",
        ] {
            assert!(Pg::connect(u).is_ok(), "{u} must connect");
        }
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
            .create_key(ws.id, "ci", Permission::Admin, &hash(11), "dr_pg")
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
                permission: Permission::Admin
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
}
