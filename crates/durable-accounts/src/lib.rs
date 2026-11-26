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

pub mod mail;
mod memory;
pub mod password;
mod pg;

pub use mail::{LogMailer, Mail, MailError, MailOutcome, Mailer, MemoryMailer};
pub use memory::Memory;
pub use pg::Pg;

/// What an authenticated bearer may do, mirroring the env-key split that
/// existed before S2: `admin` submits and cancels, `read` only inspects.
/// The Rust name is `Write`; `admin` stays the stored string (ruling 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    Write,
    Read,
}

impl Permission {
    /// The stored value — what rows and comparisons use.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Write => "admin",
            Self::Read => "read",
        }
    }

    /// The API/UI word — `write` out, `admin` still accepted in.
    pub fn api_str(self) -> &'static str {
        match self {
            Self::Write => "write",
            Self::Read => "read",
        }
    }

    fn parse(raw: &str) -> Result<Self, AccountError> {
        match raw {
            "admin" | "write" => Ok(Self::Write),
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
/// A plan's parsed limits — `plans.limits` jsonb (D-067). A missing key
/// falls back to the Free row's v1 value so a hand-edited row can never
/// silently uncap a meter; unknown keys are ignored (forward compat —
/// a newer build can write a key an older one still serves).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct PlanLimits {
    /// Admitted submits per minute per workspace (token bucket, burst 10).
    pub jobs_per_min: u64,
    /// Accepted-not-finished runs held at once (index count).
    pub live_runs: u64,
    /// Concurrent dispatches — fairness, not admission (recorded, not
    /// enforced by `admit`; the engine's per-ws bound stays the backstop).
    pub in_flight: u64,
    /// Registered handlers per workspace.
    pub handlers: u64,
    /// Max submit input bytes.
    pub max_input_bytes: u64,
    /// Terminal-run retention — informational (the engine's expiry owns it).
    pub retention_days: u64,
    /// Members per workspace, owner included.
    pub members: u64,
    /// Live API keys per workspace.
    pub api_keys: u64,
    /// Workflows per workspace.
    pub workflows: u64,
    /// Handler test calls per hour per workspace.
    pub test_calls_per_hour: u64,
    /// Workspaces one account may own, soft-deleted counted until purged.
    pub workspaces_per_account: u64,
    /// Per-workspace share of the retained index, bytes.
    pub retained_bytes: u64,
    /// Invites one workspace may mint per day (pending or accepted —
    /// the day window counts the mail, not the seat).
    pub invites_per_day: u64,
    /// How far ahead `not_before` may park a run, days.
    pub not_before_days: u64,
}

impl Default for PlanLimits {
    fn default() -> Self {
        Self {
            jobs_per_min: 60,
            live_runs: 500,
            in_flight: 8,
            handlers: 16,
            max_input_bytes: 64 * 1024,
            retention_days: 7,
            members: 5,
            api_keys: 10,
            workflows: 20,
            test_calls_per_hour: 30,
            workspaces_per_account: 3,
            retained_bytes: 16 * 1024 * 1024,
            invites_per_day: 20,
            not_before_days: 30,
        }
    }
}

/// One `plans` row, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub id: String,
    pub name: String,
    pub limits: PlanLimits,
}

/// One workflow row (S3-7) — a named, versioned spec a workspace runs
/// on demand. `public_id` is the only id external surfaces see.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Workflow {
    pub id: u64,
    pub public_id: uuid::Uuid,
    pub workspace: u64,
    pub name: String,
    /// Soft archive (D-072 parallel): set, "run now" refuses and the
    /// list hides it; history stays readable.
    pub archived_at_ms: Option<i64>,
    /// The member account's public id, when a session created it.
    pub created_by: Option<uuid::Uuid>,
    pub created_at_ms: i64,
}

/// One immutable workflow version — there is no update path; a patch
/// writes the next row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowVersion {
    pub version: u64,
    /// The stored spec document (`{version, steps, input_b64}`), kept
    /// verbatim so the journal's replay reads exactly what ran.
    pub spec: serde_json::Value,
    pub created_at_ms: i64,
}

/// Bookkeeping for one "run now" acceptance — the listing's source;
/// the run itself lives in the engine journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowRun {
    /// The engine's internal run id — never leaves the store; handlers
    /// encode through the public id mapping.
    pub run_id: u64,
    pub version: u64,
    pub op_key: String,
    pub created_at_ms: i64,
}

/// What one `purge` pass removed — counts only.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PurgeStats {
    pub workflow_runs: u64,
    pub sessions: u64,
    pub tokens: u64,
    pub invites: u64,
    pub outbox: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Workspace {
    pub id: u64,
    pub public_id: uuid::Uuid,
    pub name: String,
    /// Per-workspace secret used to sign handler calls (S2-6). It is
    /// generated server-side and never leaves the store on reads that do
    /// not need it — callers must not log or serialize it.
    pub handler_secret: [u8; 32],
    /// Soft delete (D-072): set, admission refuses and members stop
    /// seeing the row; job data leaves on the journal's own schedule.
    pub deleted_at_ms: Option<i64>,
}

/// A member row as the API shows it: the account's public handle only —
/// sequential ids never cross the boundary.
#[derive(Debug, Clone)]
pub struct MemberInfo {
    pub account_public_id: uuid::Uuid,
    pub email: String,
    pub role: Role,
    pub since_ms: i64,
}

/// A live workspace invite as the API shows it.
#[derive(Debug, Clone)]
pub struct WorkspaceInvite {
    pub public_id: uuid::Uuid,
    pub email: String,
    pub role: Role,
    pub expires_at_ms: i64,
}

/// The outcome of a member-row mutation. `LastOwner` covers every edit
/// to the owner row: exactly one owner exists (`members_one_owner`), it
/// can never be demoted or removed, and ownership transfers do not exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberChange {
    Changed,
    NotFound,
    LastOwner,
}

/// Public-facing description of an issued API key. The secret itself is
/// shown once at creation and only its sha256 is stored.
#[derive(Debug, Clone)]
pub struct ApiKeyInfo {
    pub id: u64,
    /// Opaque external id — `id` never leaves the process (S3-5).
    pub public_id: uuid::Uuid,
    pub name: String,
    pub prefix: String,
    pub permission: Permission,
    pub revoked: bool,
    /// unix millis; `None` until the first flush lands a use.
    pub last_used_at_ms: Option<i64>,
    /// The member account that issued it (its public id — internal
    /// account ids never leave the store), when self-service created it.
    pub created_by: Option<uuid::Uuid>,
    /// unix millis — the row's birth stamp, surfaced for the dashboard.
    pub created_at_ms: i64,
}

/// One handler registered for a workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerRegistration {
    pub name: String,
    pub url: String,
    /// D-070 health columns: when the last test call ran (unix ms), its
    /// verdict (`ok|failed|refused|lost`) and its latency.
    pub last_test_at_ms: Option<i64>,
    pub last_test_status: Option<String>,
    pub last_test_latency_ms: Option<i64>,
}

/// An account row. `id` is the internal join key — never external;
/// `public_id` (UUIDv7) is the only id allowed on external surfaces.
/// `password_hash` is the argon2id PHC the credential check needs — it
/// is never serialized into a response.
#[derive(Debug, Clone)]
pub struct Account {
    pub id: u64,
    pub public_id: uuid::Uuid,
    pub email: String,
    pub email_verified: bool,
    pub password_hash: Option<String>,
}

/// A live session row joined to its account — the auth boundary's one
/// round trip. Deadlines are unix millis: the store keeps the clock
/// out of the trait and the caller decides liveness.
#[derive(Debug, Clone)]
pub struct SessionGrant {
    pub session_id: u64,
    /// The opaque id the API exposes (`sessions.public_id`) — the
    /// internal id never crosses the boundary.
    pub public_id: uuid::Uuid,
    pub csrf_seed: [u8; 32],
    /// Absolute expiry (`sessions.expires_at`) — never slides.
    pub expires_at_ms: i64,
    /// Idle expiry — slid forward on use (the flush task, D-071).
    pub idle_expires_at_ms: i64,
    pub account: Account,
}

/// Everything `create_session` needs — one bundle so the trait stays
/// readable as the audit fields grow.
#[derive(Debug, Clone)]
pub struct NewSession {
    pub account: u64,
    pub token_hash: [u8; 32],
    pub csrf_seed: [u8; 32],
    /// Absolute expiry (`sessions.expires_at`) — unix millis.
    pub expires_at_ms: i64,
    /// Idle expiry — unix millis; slid forward on use.
    pub idle_expires_at_ms: i64,
    /// Rotation linkage (`sessions.replaced_by`): the session this one
    /// supersedes — the privilege-change mint's chain back to the
    /// revoked session it replaced. `None` for a fresh sign-in.
    pub replaces: Option<u64>,
    /// Truncated by the caller (the boundary owns string bounds).
    pub user_agent: Option<String>,
    pub ip: Option<std::net::IpAddr>,
}

/// One live session row for the `/me/sessions` list — the account's own
/// audit view. `public_id` is the only id that crosses the API; the
/// internal `sessions.id` never serializes (no sequential ids exposed,
/// review S3). Deadlines ride along so the caller applies its clock.
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub public_id: uuid::Uuid,
    pub created_at_ms: i64,
    pub last_seen_at_ms: i64,
    pub expires_at_ms: i64,
    pub idle_expires_at_ms: i64,
    pub user_agent: Option<String>,
    pub ip: Option<String>,
}

/// The password-check target for a live `verify` token (S2-6).
pub struct VerifyPeek {
    /// What the presented password must argon-match: the token's
    /// pending hash, else the account's current hash.
    pub password_hash: Option<String>,
}

/// A pending idle slide (D-071): use of a session is recorded in memory
/// and flushed on an interval rather than written per request — the
/// store row lags by at most the flush interval.
#[derive(Debug, Clone)]
pub struct SessionTouch {
    pub token_hash: [u8; 32],
    /// When the session was last used — unix millis.
    pub seen_ms: i64,
    /// The new idle deadline (`seen_ms` + the server's idle window).
    pub idle_expires_at_ms: i64,
}

/// A batched api-key "used" marker (D-071): key verifies coalesce in
/// memory and land as at most one `last_used_at` UPDATE per key per
/// flush tick — zero per-request store writes on the hot path.
pub struct KeyTouch {
    pub key_hash: [u8; 32],
    /// unix millis.
    pub seen_ms: i64,
}

/// What `signup` decided — the three outcomes the API deliberately
/// cannot distinguish. `outbox` is the inserted mail row the caller may
/// try to deliver now, or `None` when the per-address pending cap
/// coalesced the mail (the fact — account, token — still stands).
#[derive(Debug)]
pub enum SignupOutcome {
    /// A new unverified account + a fresh verify token + verify mail.
    Created { outbox: Option<u64> },
    /// The address existed unverified: the pending verify token was
    /// superseded and a fresh token + mail issued. `account` is the
    /// internal id — the transaction also revoked its sessions, so the
    /// caller needs it to invalidate caches, not to expose.
    VerifyResent { account: u64, outbox: Option<u64> },
    /// The address has a verified account: the notice mail was queued
    /// (no token — the account's sign-in state is untouched).
    Taken { outbox: Option<u64> },
}

/// A live invite row as the invite-only sign-up gate reads it
/// (`DURABLE_INVITE_ONLY`): the token binds the address it was mailed
/// to — the caller refuses when it doesn't match the sign-up's.
#[derive(Debug, Clone)]
pub struct InviteGrant {
    pub workspace: u64,
    pub email: String,
    pub role: Role,
}

/// One outbox row claimed for delivery — the rendered mail plus the
/// bookkeeping the drain needs (`id` to mark, `attempts` to back off).
#[derive(Debug, Clone)]
pub struct OutboxMail {
    pub id: u64,
    /// Send attempts recorded so far — the drain backs off on it.
    pub attempts: i64,
    pub mail: Mail,
}

/// Workspace membership roles — the session side of the authorization
/// matrix. Stored strings; `owner` sits above `admin`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    Viewer,
    Member,
    Admin,
    Owner,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Viewer => "viewer",
            Role::Member => "member",
            Role::Admin => "admin",
            Role::Owner => "owner",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        Some(match raw {
            "viewer" => Role::Viewer,
            "member" => Role::Member,
            "admin" => Role::Admin,
            "owner" => Role::Owner,
            _ => return None,
        })
    }

    /// Writes allowed: owner/admin/member mutate; viewer is read-only.
    pub fn may_write(self) -> bool {
        self >= Role::Member
    }
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
    /// The write would pass the per-workspace handler bound — refused
    /// atomically at the store so a racing pair of PUTs cannot both win
    /// (S2 review #6: the route's pre-check is TOCTOU by nature).
    #[error("handler registry is full ({MAX_HANDLERS_PER_WS} per workspace)")]
    RegistryFull,
    /// A stopgap cap refused the write atomically in the same
    /// transaction/lock as the insert (S2-2): the `limit` name is the
    /// API's `plan_limit` envelope field.
    #[error("limit reached: {0}")]
    PlanLimit(&'static str),
}

/// The plan id every new workspace lands on — the only row the launch
/// ships; the caps read it so the numbers live in `plans`, not code.
pub const DEFAULT_PLAN: &str = "free";

/// S3-7 hard bounds — fixed, never plan-read: at most 50 immutable
/// versions per workflow and 96 KiB per stored spec document.
pub const MAX_WORKFLOW_VERSIONS: u64 = 50;
pub const MAX_WORKFLOW_SPEC_BYTES: usize = 96 * 1024;
/// `workflow_runs` bookkeeping retention — the purge's horizon (7 d).
pub const WORKFLOW_RUNS_RETENTION_MS: i64 = 7 * 86_400_000;

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

/// Handler registrations one workspace may hold — enforced by the
/// store itself so a racing pair of PUTs cannot both fit (the engine's
/// registry enforces the same bound independently).
pub const MAX_HANDLERS_PER_WS: usize = 256;

/// Live sessions one account may hold. Past it, sign-in evicts the
/// least-idle live session in the same transaction — a cap that evicts
/// is honest; a cap that refuses is a self-DoS (D-064).
pub const MAX_SESSIONS_PER_ACCOUNT: usize = 20;

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

    /// Workspaces bounded to `limit`, ordered by internal id; `after`
    /// pages forward so callers that walk the whole graph (the registry
    /// reconciler) are not silently capped at a first page (S3).
    async fn list_workspaces(
        &self,
        limit: u32,
        after: Option<u64>,
    ) -> Result<Vec<Workspace>, AccountError>;

    /// Record a newly issued key. `key_hash` is sha256 of the secret the
    /// caller shows once; the store never sees the plaintext.
    /// `created_by` is the issuing member account on self-service
    /// creates (`None` on the ops path).
    async fn create_key(
        &self,
        workspace: u64,
        name: &str,
        permission: Permission,
        key_hash: &[u8; 32],
        prefix: &str,
        created_by: Option<u64>,
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

    /// A live key of the workspace addressed by its opaque public id —
    /// the member-plane resolution step (S3-5). `None` covers both "not
    /// in this workspace" and "already revoked" so callers cannot
    /// distinguish them.
    async fn key_by_public_id(
        &self,
        workspace: u64,
        public_id: uuid::Uuid,
    ) -> Result<Option<ApiKeyInfo>, AccountError>;

    /// Atomically retire `key_id` and mint its replacement with the same
    /// name and permission — the rotate contract is "old key 401s on
    /// the next call", which a non-atomic create-then-revoke cannot
    /// promise. Returns the new key's info and the old hash (for
    /// write-through cache invalidation); `None` when no live key of
    /// that id exists in the workspace.
    async fn rotate_key(
        &self,
        workspace: u64,
        key_id: u64,
        new_key_hash: &[u8; 32],
        new_prefix: &str,
        created_by: Option<u64>,
    ) -> Result<Option<(ApiKeyInfo, [u8; 32])>, AccountError>;

    /// Apply a batch of coalesced "used" markers — one UPDATE per key
    /// per flush, never one per request (D-071).
    async fn touch_keys(&self, touches: &[KeyTouch]) -> Result<(), AccountError>;

    async fn list_handlers(&self, workspace: u64)
        -> Result<Vec<HandlerRegistration>, AccountError>;

    /// Insert or replace a handler registration for a workspace.
    async fn put_handler(&self, workspace: u64, name: &str, url: &str) -> Result<(), AccountError>;

    /// Stamp one D-070 test call onto the registration's health columns.
    /// `false` when no registration by that name exists in the workspace.
    async fn record_handler_test(
        &self,
        workspace: u64,
        name: &str,
        at_ms: i64,
        status: &str,
        latency_ms: i64,
    ) -> Result<bool, AccountError>;

    /// Delete a handler registration. Returns whether one existed.
    async fn delete_handler(&self, workspace: u64, name: &str) -> Result<bool, AccountError>;

    /// Draw a fresh handler-call signing secret for the workspace and
    /// return it — the caller shows it exactly once (S2-6). `None` means
    /// no such workspace. The old secret stops being valid the moment
    /// this returns; in-flight calls signed with it fail honestly.
    async fn rotate_handler_secret(&self, workspace: u64)
        -> Result<Option<[u8; 32]>, AccountError>;

    // --- S3: accounts, sessions, mail ----------------------------------

    /// Sign up `email` (already lower-cased and policy-checked by the
    /// caller) — one transaction, so a crash can never leave an account
    /// with no way to verify:
    /// - new address → account row (unverified) + verify token + the
    ///   `verify` mail row;
    /// - existing *unverified* address → the pending verify token is
    ///   superseded and a fresh token + `verify` mail row written;
    /// - existing verified address → only the `notice` mail row.
    /// `token_hash` is sha256 of the mailed value; `mail` rows whose
    /// address already sits at the pending cap are coalesced (`outbox:
    /// None`) — the fact still commits.
    async fn signup(
        &self,
        email: &str,
        password_hash: &str,
        token_hash: &[u8; 32],
        token_expires_at_ms: i64,
        verify_mail: &Mail,
        notice_mail: &Mail,
    ) -> Result<SignupOutcome, AccountError>;

    /// The account for `email`, or `None`. The row carries
    /// `password_hash` for the credential check — read paths that
    /// serialize must never expose it.
    async fn account_by_email(&self, email: &str) -> Result<Option<Account>, AccountError>;

    /// Resolve a live (non-revoked) session by its token sha256, joined
    /// to its account. `Ok(None)` is a verified negative — unknown or
    /// revoked; expiry is the caller's check (it owns the clock).
    async fn session_grant(
        &self,
        token_hash: &[u8; 32],
    ) -> Result<Option<SessionGrant>, AccountError>;

    /// Record a new session, evicting the least-idle live sessions past
    /// `MAX_SESSIONS_PER_ACCOUNT` in the same transaction (LRU, never a
    /// refusal).
    async fn create_session(&self, session: &NewSession) -> Result<(), AccountError>;

    /// Revoke the session behind `token_hash`. Returns whether a live
    /// session existed — logout is idempotent either way.
    async fn revoke_session(&self, token_hash: &[u8; 32]) -> Result<bool, AccountError>;

    /// Revoke every live session of `account`. Email verify and
    /// re-sign-up of an unverified address both call it (review #2):
    /// the verify link's sign-up wins, and any session minted before
    /// the mailbox owner proved ownership dies.
    async fn revoke_sessions_for_account(&self, account: u64) -> Result<(), AccountError>;

    // --- S3-2: session lifecycle + /me -------------------------------

    /// The account's live sessions (not revoked), newest first,
    /// bounded by `limit` — the `/me/sessions` list. Expiry filtering
    /// is the caller's check (it owns the clock).
    async fn sessions_for_account(
        &self,
        account: u64,
        limit: u32,
    ) -> Result<Vec<SessionInfo>, AccountError>;

    /// Revoke the session whose `public_id` the caller named — bound to
    /// `account` inside the lookup, so another account's public_id
    /// answers the same `None` as an unknown one (no idor oracle).
    /// `Some(token_hash)` is the revoked session's hash for the
    /// caller's cache bump.
    async fn revoke_session_public(
        &self,
        account: u64,
        public_id: uuid::Uuid,
    ) -> Result<Option<[u8; 32]>, AccountError>;

    /// Revoke every live session of `account` except `keep_public_id`
    /// (revoke-all-but-current). Returns the number revoked.
    async fn revoke_sessions_except(
        &self,
        account: u64,
        keep_public_id: uuid::Uuid,
    ) -> Result<u64, AccountError>;

    /// The privilege-change write (review #2 carried to S3-2): install
    /// `new_password_hash`, supersede every pending token — a verify
    /// token's stale pending hash must never revert the change — and
    /// revoke every session, all in one transaction so a partial win
    /// is impossible.
    async fn change_password(
        &self,
        account: u64,
        new_password_hash: &str,
    ) -> Result<(), AccountError>;

    /// Flush pending idle slides (D-071): each touch stamps
    /// `last_seen_at` and slides `idle_expires_at` on its live session
    /// — never backward, and never onto a revoked row.
    async fn touch_sessions(&self, touches: &[SessionTouch]) -> Result<(), AccountError>;

    /// The member row's role for `account` in `workspace`, or `None`.
    /// ws 0 can never answer: the `workspace_id <> 0` CHECK means no
    /// row exists, and the impls short-circuit it regardless (the ops
    /// plane is key-only).
    async fn member_role(&self, account: u64, workspace: u64)
        -> Result<Option<Role>, AccountError>;

    // --- S3-3: the tenant control plane ------------------------------

    /// `POST /workspaces`: one atomic write — the workspace row plus its
    /// owner member row. Owner rows are minted here and only here; no
    /// member mutation may ever create or move one (D-066).
    async fn create_workspace_for(
        &self,
        account: u64,
        name: &str,
    ) -> Result<Workspace, AccountError>;

    async fn rename_workspace(&self, id: u64, name: &str) -> Result<bool, AccountError>;

    /// Soft delete (D-072): sets `deleted_at` and revokes every live
    /// API key of the workspace in the same step — a key of a deleted
    /// workspace is dead the moment the delete commits. Members and
    /// invites stay on the row for the grace period; members stop
    /// seeing it because `workspaces_for_account` filters.
    async fn delete_workspace(&self, id: u64, now_ms: i64) -> Result<bool, AccountError>;

    /// Every live workspace `account` belongs to, with their role.
    /// Deleted workspaces never appear (members lose them from lists).
    async fn workspaces_for_account(
        &self,
        account: u64,
    ) -> Result<Vec<(Workspace, Role)>, AccountError>;

    /// Workspaces `account` OWNS, soft-deleted included (D-072's "counted
    /// until purged": a delete+recreate cycle must not dodge
    /// `workspaces_per_account`). Owner member rows only — invites and
    /// non-owner seats never inflate the count.
    async fn count_owned_workspaces(&self, account: u64) -> Result<u64, AccountError>;

    /// One `plans` row by id — `None` when no row has that id.
    async fn plan(&self, id: &str) -> Result<Option<Plan>, AccountError>;

    /// The plan `workspace`'s row names (join on `plan_id`; every ws
    /// carries the default 'free' since 0002). Missing plan or ws rows
    /// answer `None`.
    async fn plan_for_workspace(&self, workspace: u64) -> Result<Option<Plan>, AccountError>;

    /// Upsert a `plans` row — a plan change is a row write, never a
    /// deploy (D-067). `limits` is the raw jsonb; it must parse into
    /// `PlanLimits` or the write is refused (a bad row would uncap or
    /// cap everything wrongly).
    async fn put_plan(
        &self,
        id: &str,
        name: &str,
        limits: &serde_json::Value,
    ) -> Result<Plan, AccountError>;

    async fn list_members(&self, workspace: u64) -> Result<Vec<MemberInfo>, AccountError>;

    /// Set `account`'s role in `workspace`. `Role::Owner` is refused by
    /// the caller before this is reached; the owner row itself always
    /// answers `LastOwner` — it is immutable for the workspace's life.
    async fn set_member_role(
        &self,
        workspace: u64,
        account: u64,
        role: Role,
    ) -> Result<MemberChange, AccountError>;

    async fn remove_member(
        &self,
        workspace: u64,
        account: u64,
    ) -> Result<MemberChange, AccountError>;

    async fn account_by_public_id(
        &self,
        public_id: uuid::Uuid,
    ) -> Result<Option<Account>, AccountError>;

    /// Create a live invite. `Ok(None)` is a live duplicate — a pending,
    /// unexpired invite for the same (workspace, email) already exists
    /// (`invites_pending` unique index; the route answers 409
    /// `invite_pending`). `token_hash` is sha256 of the mailed token.
    async fn create_invite(
        &self,
        workspace: u64,
        email: &str,
        role: Role,
        invited_by: u64,
        token_hash: &[u8; 32],
        expires_at_ms: i64,
    ) -> Result<Option<WorkspaceInvite>, AccountError>;

    async fn list_invites(&self, workspace: u64) -> Result<Vec<WorkspaceInvite>, AccountError>;

    /// Revoke a pending invite, bound to `workspace` — an invite id is
    /// only ever addressed under its own workspace (child-idor).
    async fn revoke_invite(
        &self,
        workspace: u64,
        public_id: uuid::Uuid,
    ) -> Result<bool, AccountError>;

    /// Accept a live invite: mark it consumed and write the member row
    /// (ON CONFLICT DO NOTHING — already a member still consumes the
    /// invite and re-answers the grant). `Ok(None)` covers unknown,
    /// consumed, revoked and expired tokens; the caller matched the
    /// session's email to the invite's address before reaching here
    /// (`403 invite_email_mismatch`) — the row is revealed only to the
    /// address it was mailed to.
    async fn accept_invite(
        &self,
        token_hash: &[u8; 32],
        account: u64,
        now_ms: i64,
    ) -> Result<Option<(u64, Role)>, AccountError>;

    /// Decline a live invite by token — marks it revoked. `Ok(false)`
    /// for unknown or already-terminal tokens.
    async fn decline_invite(
        &self,
        token_hash: &[u8; 32],
        now_ms: i64,
    ) -> Result<bool, AccountError>;

    // --- S3-1: email-token consume, outbox drain, invites -----------

    /// Consume a live `verify` token atomically (review #2): mark it
    /// consumed, install its pending password hash, set
    /// `email_verified_at` and revoke every session of the account —
    /// one transaction, so a partial win is impossible. `Ok(None)` is
    /// the single answer for unknown, consumed, superseded and expired
    /// alike (no dead-kind oracle).
    async fn consume_verify_token(
        &self,
        token_hash: &[u8; 32],
    ) -> Result<Option<Account>, AccountError>;

    /// Peek at a live `verify` token WITHOUT consuming it (S2-6): the
    /// verify route argon-checks the presented password against
    /// `VerifyPeek::password_hash` — the token's pending hash, else the
    /// account's current one — and refuses mismatches while the token
    /// stays alive. `Ok(None)` is the dead-token answer, identical to
    /// `consume_verify_token`'s.
    async fn peek_verify_token(
        &self,
        token_hash: &[u8; 32],
    ) -> Result<Option<VerifyPeek>, AccountError>;

    /// Consume a live `reset` token atomically: mark it consumed,
    /// install `new_password_hash`, prove mailbox ownership (sets
    /// `email_verified_at`), supersede the account's other pending
    /// tokens — a stale pending hash can never overwrite the reset —
    /// and revoke every session. One transaction; `Ok(None)` covers
    /// every dead state identically.
    async fn consume_reset_token(
        &self,
        token_hash: &[u8; 32],
        new_password_hash: &str,
    ) -> Result<Option<Account>, AccountError>;

    /// Issue a `reset` token + reset mail for `email` in one
    /// transaction (pending reset tokens superseded first).
    /// `Ok(Some(id))` = mail enqueued; `Ok(None)` = address unknown or
    /// the mail coalesced — the caller's answer is identical.
    async fn issue_reset_token(
        &self,
        email: &str,
        token_hash: &[u8; 32],
        expires_at_ms: i64,
        reset_mail: &Mail,
    ) -> Result<Option<u64>, AccountError>;

    /// Re-issue a `verify` token for an existing *unverified* account:
    /// supersede the pending one, insert fresh (carrying the account's
    /// current password hash so verifying never reverts a reset), and
    /// enqueue the mail. `Ok(None)` = unknown or already verified —
    /// resend-verification's envelope never reveals which.
    async fn reissue_verify(
        &self,
        email: &str,
        token_hash: &[u8; 32],
        expires_at_ms: i64,
        verify_mail: &Mail,
    ) -> Result<Option<u64>, AccountError>;

    /// A live invite by its token hash — the `DURABLE_INVITE_ONLY`
    /// sign-up gate: unaccepted, unrevoked and unexpired. `None`
    /// covers every dead state; the caller matches `email` itself.
    async fn invite_grant(
        &self,
        token_hash: &[u8; 32],
        now_ms: i64,
    ) -> Result<Option<InviteGrant>, AccountError>;

    /// Enqueue a mail outside the token flows (the lockout notice):
    /// the strict addr-spec check, the per-(address,kind) hourly cap
    /// and the idempotency key's uniqueness do the dedupe — `Ok(None)`
    /// when any of them coalesced the mail.
    async fn enqueue_mail(&self, mail: &Mail) -> Result<Option<u64>, AccountError>;

    /// Claim up to `limit` due rows for sending: `pending` and due, or
    /// `sending` past its claim deadline (a worker died between claim
    /// and mark — that reclaim counts a lost attempt). Claims stamp
    /// `next_attempt_at = now + claim_ms`; a row that reaches
    /// `OUTBOX_MAX_ATTEMPTS` stops being claimed and `outbox_sweep`
    /// dead-letters it.
    async fn outbox_claim(
        &self,
        now_ms: i64,
        limit: u32,
        claim_ms: i64,
    ) -> Result<Vec<OutboxMail>, AccountError>;

    /// Sweep the outbox: rows that ran out of attempts become `failed`
    /// for inspection; rows past their `expires_at` become `dropped`
    /// with their bodies cleared — a dead token's link never lingers
    /// in the table. Returns the number of rows swept.
    async fn outbox_sweep(&self, now_ms: i64) -> Result<u64, AccountError>;

    /// Purge terminal rows: `sent` after 24 h, `failed`/`dropped`
    /// after 7 days — the outbox never grows without bound.
    async fn outbox_purge(&self, now_ms: i64) -> Result<u64, AccountError>;

    /// Bookkeep a delivery attempt the caller just made for an outbox
    /// row: `Sent` closes it (only from a live state, so a double mark
    /// is a no-op); `Retry` stamps the error and the next-attempt time
    /// back to `pending`; `Failed` dead-letters it.
    async fn outbox_mark(&self, id: u64, outcome: MailOutcome) -> Result<(), AccountError>;

    // --- S3-7: workflows + the purge ----------------------------------

    /// Create a workflow row in `workspace` (live `workflows` cap is
    /// the ws's plan row, checked inside the same lock/transaction —
    /// D-067's single source). `created_by` is the member account id
    /// when a session created it.
    async fn create_workflow(
        &self,
        workspace: u64,
        name: &str,
        created_by: Option<u64>,
    ) -> Result<Workflow, AccountError>;

    /// Live workflows of `workspace` (archived rows are hidden — they
    /// stay addressable by id for history).
    async fn workflows_for(&self, workspace: u64) -> Result<Vec<Workflow>, AccountError>;

    /// Live rows count only — the `workflows` plan meter.
    async fn count_live_workflows(&self, workspace: u64) -> Result<u64, AccountError>;

    /// A workflow addressed under its own workspace — a foreign
    /// workspace's id answers `None` (child-idor: never another
    /// tenant's row).
    async fn workflow_by_public_id(
        &self,
        workspace: u64,
        public_id: uuid::Uuid,
    ) -> Result<Option<Workflow>, AccountError>;

    /// Append the next version — immutable rows only. More than
    /// `MAX_WORKFLOW_VERSIONS` refuses `PlanLimit("workflow_versions")`;
    /// `spec` size is the caller's bound (it owns the 400 envelope).
    async fn add_workflow_version(
        &self,
        workspace: u64,
        workflow_id: u64,
        spec: &serde_json::Value,
    ) -> Result<WorkflowVersion, AccountError>;

    /// The latest version row of `workflow_id` (ws-bound via the
    /// caller's lookup), or `None` if none exists.
    async fn latest_workflow_version(
        &self,
        workflow_id: u64,
    ) -> Result<Option<WorkflowVersion>, AccountError>;

    /// Every version row of `workflow_id`, newest first (ws-bound via
    /// the caller's lookup). Bounded by `MAX_WORKFLOW_VERSIONS`: a
    /// workflow never holds more.
    async fn workflow_versions(
        &self,
        workflow_id: u64,
    ) -> Result<Vec<WorkflowVersion>, AccountError>;

    /// Rename a live workflow row (ws-bound). `Ok(false)` = missing or
    /// archived — rename never revives history.
    async fn rename_workflow(
        &self,
        workspace: u64,
        workflow_id: u64,
        name: &str,
    ) -> Result<bool, AccountError>;

    /// Soft-archive (D-072 parallel): `archived_at` set once — a
    /// second archive is `Ok(false)`, the route answers `gone`.
    async fn archive_workflow(
        &self,
        workspace: u64,
        workflow_id: u64,
    ) -> Result<bool, AccountError>;

    /// Record one accepted "run now" — deduplicated submits write no
    /// row (the original already recorded).
    async fn record_workflow_run(
        &self,
        workspace: u64,
        workflow_id: u64,
        version: u64,
        op_key: &str,
        run_id: u64,
    ) -> Result<(), AccountError>;

    /// The dedup anchor for "run now": the recorded row for an op key,
    /// if one was accepted. A replay checks this before admit() so a
    /// retried submit does not spend the rate bucket again.
    async fn workflow_run_by_op(
        &self,
        workflow_id: u64,
        op_key: &str,
    ) -> Result<Option<WorkflowRun>, AccountError>;

    /// Newest-first run bookkeeping for one workflow, bounded.
    async fn workflow_runs(
        &self,
        workflow_id: u64,
        limit: u64,
    ) -> Result<Vec<WorkflowRun>, AccountError>;

    /// The bounded sweep (S3-7): workflow_runs older than 7 days,
    /// revoked or expired sessions, consumed/superseded or expired
    /// email tokens, closed or expired invites, terminal outbox rows
    /// past their retention. Returns what each table gave up.
    async fn purge(&self, now_ms: i64) -> Result<PurgeStats, AccountError>;
}

/// Never-panics fuzzing for outside input this crate parses: the
/// `api_keys.permission` value that comes back from a store. A panic is
/// a red-first bug.
#[cfg(test)]
mod fuzz {
    use super::*;
    use proptest::prelude::*;

    fn corpus(name: &str) -> Vec<String> {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fuzz/corpus")
            .join(name);
        std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("corpus {dir:?}: {e}"))
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_ok_and(|t| t.is_file()))
            .filter_map(|e| std::fs::read_to_string(e.path()).ok())
            .collect()
    }

    #[test]
    fn fuzz_permission_seed_corpus() {
        for s in corpus("permission") {
            let _ = Permission::parse(&s);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::default())]
        #[test]
        fn fuzz_permission_never_panics(s in any::<String>()) {
            let _ = Permission::parse(&s);
        }
    }
}
