use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use crate::{
    AccountError, AccountStore, ApiKeyInfo, HandlerRegistration, KeyGrant, Permission, Workspace,
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
    unavailable: AtomicBool,
}

#[derive(Default)]
struct Inner {
    workspaces: HashMap<u64, Workspace>,
    keys: HashMap<u64, KeyRow>,
    handlers: HashMap<(u64, String), String>,
}

struct KeyRow {
    workspace: u64,
    name: String,
    hash: [u8; 32],
    prefix: String,
    permission: Permission,
    revoked: bool,
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
            .create_key(ws.id, "ci", Permission::Admin, &hash(7), "dr_ci")
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
