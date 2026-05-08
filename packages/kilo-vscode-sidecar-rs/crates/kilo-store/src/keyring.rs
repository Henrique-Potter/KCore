//! Keyring shim for OAuth tokens and other long-lived secrets.
//!
//! Per migration plan **Storage and process self-healing invariants → 5**:
//! OAuth tokens (ChatGPT Pro refresh tokens, future provider credentials,
//! MCP OAuth) live behind the OS keychain — DPAPI on Windows, Keychain
//! Services on macOS, libsecret/Secret Service on Linux — not in
//! plaintext under `data/kilo/auth.json`. The current plaintext path is
//! M5-era scaffolding; before stable rollout the storage layer routes
//! through this `KeyringStore` shim with a documented plaintext fallback
//! for environments where the keyring is unavailable (CI, headless
//! containers, locked Linux sessions), gated behind an explicit user
//! opt-in or env flag.
//!
//! This file ships the trait + a [`PlaintextKeyringStore`] fallback that
//! reads/writes `auth.json` exactly as today. A future M14 commit adds
//! `OsKeyringStore` (real DPAPI/Keychain/libsecret) and switches the
//! default. Plaintext mode survives behind `KILO_KEYRING_PLAINTEXT=1`
//! for CI / headless usage.

use std::{
    fs,
    io::{ErrorKind, Read, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

use serde_json::Value;

/// Trait every persisted-secret call site goes through. Implementations
/// MUST be thread-safe; the default plaintext implementation guards with
/// a mutex.
pub trait KeyringStore: Send + Sync {
    /// Read the secret blob for a given service+account pair. Returns
    /// `Ok(None)` when no entry exists; only filesystem/keyring errors
    /// produce `Err`.
    fn get(&self, service: &str, account: &str) -> std::io::Result<Option<Value>>;

    /// Persist the secret blob. The blob is opaque to the keyring layer
    /// and is round-tripped as JSON.
    fn set(&self, service: &str, account: &str, blob: Value) -> std::io::Result<()>;

    /// Remove the secret blob. Idempotent — removing a missing entry is
    /// not an error.
    fn delete(&self, service: &str, account: &str) -> std::io::Result<()>;

    /// Returns `true` when this implementation is the OS keyring (and
    /// thus protects secrets at rest), `false` for the plaintext
    /// fallback. Routes that surface the secret-storage status to the
    /// user (settings panel, telemetry) read this.
    fn is_os_protected(&self) -> bool {
        false
    }
}

/// Plaintext fallback: persists the entire keyring as a single JSON
/// object under `<root>/auth.json`. This is the M5-era scaffolding
/// behavior; preview ships with this as the default and stable will
/// flip to the real OS keyring.
///
/// Activation:
///
/// - Default for now (until M14 wires the OS-keyring impl).
/// - Explicit opt-in via `KILO_KEYRING_PLAINTEXT=1` once OS keyring is
///   the default — for CI, headless sandboxes, locked Linux sessions.
pub struct PlaintextKeyringStore {
    root: PathBuf,
    cache: Mutex<()>,
}

impl PlaintextKeyringStore {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            cache: Mutex::new(()),
        }
    }

    fn path(&self) -> PathBuf {
        self.root.join("auth.json")
    }

    fn read_all(&self) -> std::io::Result<Value> {
        let path = self.path();
        let mut buf = String::new();
        match fs::File::open(&path) {
            Ok(mut f) => {
                f.read_to_string(&mut buf)?;
                if buf.trim().is_empty() {
                    return Ok(Value::Object(Default::default()));
                }
                serde_json::from_str(&buf).or_else(|_| {
                    // Self-healing: corrupt JSON falls back to the empty
                    // default and the next successful write overwrites
                    // the bad file. See **Storage and process
                    // self-healing invariants → 1**.
                    Ok(Value::Object(Default::default()))
                })
            }
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(Value::Object(Default::default())),
            Err(err) => Err(err),
        }
    }

    fn write_all(&self, value: &Value) -> std::io::Result<()> {
        if let Some(dir) = self.path().parent() {
            fs::create_dir_all(dir)?;
        }
        let temp = self.path().with_extension("tmp");
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp)?;
        file.write_all(value.to_string().as_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp, self.path())
    }
}

fn key_for(service: &str, account: &str) -> String {
    format!("{service}::{account}")
}

impl KeyringStore for PlaintextKeyringStore {
    fn get(&self, service: &str, account: &str) -> std::io::Result<Option<Value>> {
        let _guard = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        let all = self.read_all()?;
        Ok(all.get(key_for(service, account)).cloned())
    }

    fn set(&self, service: &str, account: &str, blob: Value) -> std::io::Result<()> {
        let _guard = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        let mut all = self.read_all()?;
        let map = all.as_object_mut().ok_or_else(|| {
            std::io::Error::new(ErrorKind::InvalidData, "auth.json is not a JSON object")
        })?;
        map.insert(key_for(service, account), blob);
        self.write_all(&all)
    }

    fn delete(&self, service: &str, account: &str) -> std::io::Result<()> {
        let _guard = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        let mut all = self.read_all()?;
        if let Some(map) = all.as_object_mut() {
            map.remove(&key_for(service, account));
            self.write_all(&all)?;
        }
        Ok(())
    }

    fn is_os_protected(&self) -> bool {
        false
    }
}

/// Resolve the implementation to use. Currently always returns
/// [`PlaintextKeyringStore`]; once the OS-keyring impl lands the
/// resolver checks `KILO_KEYRING_PLAINTEXT=1` to keep the plaintext
/// path available for CI/headless.
pub fn default_keyring(root: &Path) -> Box<dyn KeyringStore> {
    if std::env::var("KILO_KEYRING_PLAINTEXT").as_deref() == Ok("1") {
        return Box::new(PlaintextKeyringStore::new(root.to_path_buf()));
    }
    // TODO(M14): once `keyring` (or DPAPI/Keychain/libsecret-direct)
    // bindings are added, return an `OsKeyringStore` here. Until then
    // plaintext is the only implementation.
    Box::new(PlaintextKeyringStore::new(root.to_path_buf()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn temp_root(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("kilo-keyring-{name}-{}", std::process::id()));
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn set_then_get_returns_value() {
        let root = temp_root("set-get");
        let store = PlaintextKeyringStore::new(root);
        store
            .set("openai", "default", json!({ "access": "x" }))
            .unwrap();
        let got = store.get("openai", "default").unwrap();
        assert_eq!(got, Some(json!({ "access": "x" })));
    }

    #[test]
    fn delete_removes_value() {
        let root = temp_root("delete");
        let store = PlaintextKeyringStore::new(root);
        store
            .set("openai", "default", json!({ "access": "x" }))
            .unwrap();
        store.delete("openai", "default").unwrap();
        assert_eq!(store.get("openai", "default").unwrap(), None);
    }

    #[test]
    fn missing_file_returns_none_not_error() {
        let root = temp_root("missing");
        let store = PlaintextKeyringStore::new(root);
        assert_eq!(store.get("nope", "nope").unwrap(), None);
    }

    #[test]
    fn corrupt_file_self_heals_to_empty() {
        let root = temp_root("corrupt");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("auth.json"), b"this is not json").unwrap();
        let store = PlaintextKeyringStore::new(root);
        assert_eq!(store.get("openai", "default").unwrap(), None);
        // Next write succeeds and overwrites the corrupt file.
        store.set("openai", "default", json!({ "x": 1 })).unwrap();
        assert_eq!(
            store.get("openai", "default").unwrap(),
            Some(json!({ "x": 1 }))
        );
    }

    #[test]
    fn is_os_protected_is_false_for_plaintext() {
        let root = temp_root("os-flag");
        let store = PlaintextKeyringStore::new(root);
        assert!(!store.is_os_protected());
    }
}
