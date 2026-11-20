//! Account graph for the S2 SaaS slice: workspaces, API keys, handler
//! registrations and per-workspace signing secrets, backed by Postgres
//! (`Pg`) with an in-memory implementation (`Memory`) for dialect-free
//! tests and compatibility mode.
//!
//! The journal stays the only authority for jobs: this store is read on
//! the authentication boundary and the handler-registry path only, never
//! inside the commit or dispatch path. When the store is unreachable the
//! caller fails closed — new submissions answer `accounts_unavailable`
//! while already-accepted runs keep executing.

mod memory;
mod pg;

pub use memory::Memory;
pub use pg::Pg;

/// What an authenticated bearer may do, mirroring the env-key split that
/// existed before S2: `admin` submits and cancels, `read` only inspects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    Admin,
    Read,
}

impl Permission {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::Read => "read",
        }
    }

    fn parse(raw: &str) -> Result<Self, AccountError> {
        match raw {
            "admin" => Ok(Self::Admin),
            "read" => Ok(Self::Read),
            other => Err(AccountError::Store(format!(
                "unknown api_keys.permission {other:?}"
            ))),
        }
    }
}

/// The result of verifying an API key: the workspace it belongs to and
/// what it may do there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyGrant {
    pub workspace: u64,
    pub permission: Permission,
}

/// A workspace row. `id` is the internal join key used in journal records
/// and index structures; `public_id` (UUIDv7) is the only id allowed on
/// external surfaces so sequential ids stay unguessable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Workspace {
    pub id: u64,
    pub public_id: uuid::Uuid,
    pub name: String,
    /// Per-workspace secret used to sign handler calls (S2-6). It is
    /// generated server-side and never leaves the store on reads that do
    /// not need it — callers must not log or serialize it.
    pub handler_secret: [u8; 32],
}

/// Public-facing description of an issued API key. The secret itself is
/// shown once at creation and only its sha256 is stored.
#[derive(Debug, Clone)]
pub struct ApiKeyInfo {
    pub id: u64,
    pub name: String,
    pub prefix: String,
    pub permission: Permission,
    pub revoked: bool,
}

/// One handler registered for a workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerRegistration {
    pub name: String,
    pub url: String,
}

#[derive(Debug, thiserror::Error)]
pub enum AccountError {
    /// The store could not be reached or answered in time. Callers treat
    /// this as fail-closed (`503 accounts_unavailable`): it never maps to
    /// a negative authentication result.
    #[error("account store unavailable: {0}")]
    Unavailable(String),
    /// The store answered but the operation itself failed.
    #[error("account store: {0}")]
    Store(String),
}

impl From<sqlx::Error> for AccountError {
    fn from(err: sqlx::Error) -> Self {
        match &err {
            sqlx::Error::Io(_)
            | sqlx::Error::PoolTimedOut
            | sqlx::Error::PoolClosed
            | sqlx::Error::Tls(_)
            | sqlx::Error::Protocol(_) => Self::Unavailable(err.to_string()),
            _ => Self::Store(err.to_string()),
        }
    }
}

impl From<sqlx::migrate::MigrateError> for AccountError {
    fn from(err: sqlx::migrate::MigrateError) -> Self {
        Self::Unavailable(err.to_string())
    }
}

/// Deadline for any single store operation. Auth-boundary and registry
/// reads must never block a request indefinitely; the store being slow
/// is the same failure as the store being down.
pub const STORE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

/// The account graph. Implemented by `Pg` (Neon) and `Memory` (tests and
/// dialect-free fixtures). Every method is bounded by `STORE_DEADLINE` in
/// callers that fan out to the network.
#[async_trait::async_trait]
pub trait AccountStore: Send + Sync {
    /// Look up a live (non-revoked) key by its sha256. `Ok(None)` is a
    /// verified negative — the bearer is unknown or revoked — and is only
    /// ever distinguished from `Err` so callers can cache it briefly.
    async fn verify_key(&self, key_hash: &[u8; 32]) -> Result<Option<KeyGrant>, AccountError>;

    async fn workspace_by_id(&self, id: u64) -> Result<Option<Workspace>, AccountError>;
    async fn workspace_by_public_id(
        &self,
        public_id: uuid::Uuid,
    ) -> Result<Option<Workspace>, AccountError>;

    async fn create_workspace(&self, name: &str) -> Result<Workspace, AccountError>;

    /// Workspaces, bounded to `limit` — the caller pages if the graph
    /// ever grows beyond an ops dashboard's view.
    async fn list_workspaces(&self, limit: u32) -> Result<Vec<Workspace>, AccountError>;

    /// Record a newly issued key. `key_hash` is sha256 of the secret the
    /// caller shows once; the store never sees the plaintext.
    async fn create_key(
        &self,
        workspace: u64,
        name: &str,
        permission: Permission,
        key_hash: &[u8; 32],
        prefix: &str,
    ) -> Result<ApiKeyInfo, AccountError>;

    /// Revoke a key. Returns its hash so the caller can invalidate a
    /// cached verification write-through, or `None` if no live key by
    /// that id existed in the workspace.
    async fn revoke_key(
        &self,
        workspace: u64,
        key_id: u64,
    ) -> Result<Option<[u8; 32]>, AccountError>;

    async fn list_keys(&self, workspace: u64) -> Result<Vec<ApiKeyInfo>, AccountError>;

    async fn list_handlers(&self, workspace: u64)
        -> Result<Vec<HandlerRegistration>, AccountError>;

    /// Insert or replace a handler registration for a workspace.
    async fn put_handler(&self, workspace: u64, name: &str, url: &str) -> Result<(), AccountError>;

    /// Delete a handler registration. Returns whether one existed.
    async fn delete_handler(&self, workspace: u64, name: &str) -> Result<bool, AccountError>;

    /// Draw a fresh handler-call signing secret for the workspace and
    /// return it — the caller shows it exactly once (S2-6). `None` means
    /// no such workspace. The old secret stops being valid the moment
    /// this returns; in-flight calls signed with it fail honestly.
    async fn rotate_handler_secret(&self, workspace: u64)
        -> Result<Option<[u8; 32]>, AccountError>;
}
