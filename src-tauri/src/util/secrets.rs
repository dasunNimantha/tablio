//! Storage for connection secrets in the OS credential store.
//!
//! [`SecretStore`] is a tiny get/set/delete interface keyed by connection
//! id and [`SecretField`]. [`KeyringStore`] backs it with the platform
//! store through the `keyring` crate (macOS Keychain, Windows Credential
//! Manager, Secret Service on Linux); [`MemoryStore`] is an in-process
//! implementation for tests.
//!
//! Every call is blocking (a keychain may even show an unlock prompt), so
//! async callers should run them on a blocking thread.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

/// Service name under which every Tablio secret is filed.
const SERVICE: &str = "com.tablio.app";

/// A secret field of a saved connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SecretField {
    /// Database password.
    Password,
    /// SSH password, or the passphrase of the SSH identity file.
    SshPassword,
}

impl SecretField {
    pub const ALL: [SecretField; 2] = [SecretField::Password, SecretField::SshPassword];

    /// Name of the field in `ConnectionConfig` / `connections.json`.
    pub fn json_key(self) -> &'static str {
        match self {
            SecretField::Password => "password",
            SecretField::SshPassword => "ssh_password",
        }
    }

    /// Name of the marker recording that the field's value lives in the
    /// secret store rather than in `connections.json`.
    pub fn marker_key(self) -> &'static str {
        match self {
            SecretField::Password => "password_in_keychain",
            SecretField::SshPassword => "ssh_password_in_keychain",
        }
    }

    fn account(self, connection_id: &str) -> String {
        format!("connection/{}/{}", connection_id, self.json_key())
    }
}

/// A place to keep connection secrets. Errors are plain strings meant for
/// logs; callers treat any error as "store unavailable".
pub trait SecretStore: Send + Sync {
    /// The stored value, or `None` if there is none.
    fn get(&self, connection_id: &str, field: SecretField) -> Result<Option<String>, String>;
    fn set(&self, connection_id: &str, field: SecretField, value: &str) -> Result<(), String>;
    /// Remove the value. Removing a value that does not exist is not an error.
    fn delete(&self, connection_id: &str, field: SecretField) -> Result<(), String>;
}

/// The OS credential store.
///
/// After the first failure that says the platform store itself is not
/// usable (no Secret Service on a headless box, no D-Bus session, ...),
/// every later call fails fast instead of waiting on D-Bus timeouts again
/// for each connection and field.
pub struct KeyringStore {
    unavailable: AtomicBool,
}

impl KeyringStore {
    pub const fn new() -> Self {
        Self {
            unavailable: AtomicBool::new(false),
        }
    }

    fn entry(&self, connection_id: &str, field: SecretField) -> Result<keyring::Entry, String> {
        if self.unavailable.load(Ordering::Relaxed) {
            return Err("OS credential store is unavailable".into());
        }
        keyring::Entry::new(SERVICE, &field.account(connection_id)).map_err(|e| self.fail(e))
    }

    fn fail(&self, e: keyring::Error) -> String {
        if matches!(
            e,
            keyring::Error::PlatformFailure(_) | keyring::Error::NoStorageAccess(_)
        ) {
            self.unavailable.store(true, Ordering::Relaxed);
        }
        e.to_string()
    }
}

impl Default for KeyringStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SecretStore for KeyringStore {
    fn get(&self, connection_id: &str, field: SecretField) -> Result<Option<String>, String> {
        match self.entry(connection_id, field)?.get_password() {
            Ok(v) => Ok(Some(v)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(self.fail(e)),
        }
    }

    fn set(&self, connection_id: &str, field: SecretField, value: &str) -> Result<(), String> {
        self.entry(connection_id, field)?
            .set_password(value)
            .map_err(|e| self.fail(e))
    }

    fn delete(&self, connection_id: &str, field: SecretField) -> Result<(), String> {
        match self.entry(connection_id, field)?.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(self.fail(e)),
        }
    }
}

/// The store used by the app.
pub fn os_store() -> &'static KeyringStore {
    static STORE: KeyringStore = KeyringStore::new();
    &STORE
}

/// In-memory store for tests. `set_failing(true)` makes every call fail,
/// simulating a locked or missing keychain.
#[derive(Default)]
pub struct MemoryStore {
    values: Mutex<HashMap<(String, SecretField), String>>,
    failing: AtomicBool,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_failing(&self, failing: bool) {
        self.failing.store(failing, Ordering::Relaxed);
    }

    pub fn len(&self) -> usize {
        self.values.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn check(&self) -> Result<(), String> {
        if self.failing.load(Ordering::Relaxed) {
            Err("simulated credential store failure".into())
        } else {
            Ok(())
        }
    }
}

impl SecretStore for MemoryStore {
    fn get(&self, connection_id: &str, field: SecretField) -> Result<Option<String>, String> {
        self.check()?;
        Ok(self
            .values
            .lock()
            .unwrap()
            .get(&(connection_id.to_owned(), field))
            .cloned())
    }

    fn set(&self, connection_id: &str, field: SecretField, value: &str) -> Result<(), String> {
        self.check()?;
        self.values
            .lock()
            .unwrap()
            .insert((connection_id.to_owned(), field), value.to_owned());
        Ok(())
    }

    fn delete(&self, connection_id: &str, field: SecretField) -> Result<(), String> {
        self.check()?;
        self.values
            .lock()
            .unwrap()
            .remove(&(connection_id.to_owned(), field));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_store_roundtrip_and_failure() {
        let s = MemoryStore::new();
        assert_eq!(s.get("a", SecretField::Password).unwrap(), None);
        s.set("a", SecretField::Password, "pw").unwrap();
        s.set("a", SecretField::SshPassword, "ssh").unwrap();
        assert_eq!(
            s.get("a", SecretField::Password).unwrap().as_deref(),
            Some("pw")
        );
        s.delete("a", SecretField::Password).unwrap();
        s.delete("a", SecretField::Password).unwrap();
        assert_eq!(s.get("a", SecretField::Password).unwrap(), None);
        assert_eq!(s.len(), 1);
        s.set_failing(true);
        assert!(s.get("a", SecretField::SshPassword).is_err());
        assert!(s.set("a", SecretField::Password, "x").is_err());
        assert!(s.delete("a", SecretField::SshPassword).is_err());
    }

    #[test]
    fn accounts_are_distinct_per_connection_and_field() {
        assert_eq!(
            SecretField::Password.account("abc"),
            "connection/abc/password"
        );
        assert_ne!(
            SecretField::Password.account("abc"),
            SecretField::SshPassword.account("abc")
        );
    }

    /// Talks to the real OS credential store, so it is opt-in:
    /// `cargo test --lib real_os_store -- --ignored`.
    #[test]
    #[ignore]
    fn real_os_store_roundtrip() {
        let s = KeyringStore::new();
        let id = format!("test-{}", uuid::Uuid::new_v4());
        s.set(&id, SecretField::Password, "pässwörd/🔑").unwrap();
        assert_eq!(
            s.get(&id, SecretField::Password).unwrap().as_deref(),
            Some("pässwörd/🔑")
        );
        assert_eq!(s.get(&id, SecretField::SshPassword).unwrap(), None);
        s.delete(&id, SecretField::Password).unwrap();
        s.delete(&id, SecretField::Password).unwrap();
        assert_eq!(s.get(&id, SecretField::Password).unwrap(), None);
    }

    #[test]
    fn keyring_store_with_mock_backend() {
        // keyring's mock builder gives every `Entry` its own credential, so
        // this only exercises the error mapping of `KeyringStore`, not
        // persistence. It must never reach the real OS store.
        keyring::set_default_credential_builder(keyring::mock::default_credential_builder());
        let s = KeyringStore::new();
        assert_eq!(s.get("id", SecretField::Password).unwrap(), None);
        s.set("id", SecretField::Password, "pw").unwrap();
        s.delete("id", SecretField::Password).unwrap();
        assert!(!s.unavailable.load(Ordering::Relaxed));
    }
}
