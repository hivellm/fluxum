//! Durable client state (SPEC-021 CS-040/CS-041) — the optional local store
//! that lets a client render instantly after a restart and replay queued
//! mutations exactly-once.
//!
//! Persistence is **opt-in and off by default** (CS-040): a client
//! constructed without it behaves exactly as before. When enabled, the SDK
//! writes through to a [`PersistenceBackend`] as authoritative updates apply
//! and as the offline queue changes, and on startup it hydrates from the
//! store, then reconciles with the server: the hydrated rows are treated as
//! a previous session's cache, so the fresh `InitialData` produces only the
//! **net difference** — the application renders the persisted state
//! immediately and hears about exactly what changed while it was away
//! (CS-041). Queued mutations replay in submission order under their
//! ORIGINAL idempotency keys (CS-032), so a restart cannot double-apply.
//!
//! # Keying
//!
//! CS-040 keys persisted state by `(server, identity, query)`. The identity
//! is only derived AFTER authentication, so on disk the namespace is
//! `(server, client_id)` — the caller-supplied stable id the offline queue
//! already requires — and the LAST session's identity is stored inside the
//! state. On startup the hydrated identity is checked against the fresh
//! session's: a mismatch (a different user logged in) discards the queued
//! mutations rather than replaying them as someone else, and the cache
//! reconcile against the new identity's `InitialData` removes any rows the
//! new user may not see.
//!
//! # What is persisted, and what is not
//!
//! Subscribed rows per query, the per-query resume offset, the offline
//! queue (keys included), and the session identity. Optimistic OVERLAYS are
//! not: an updater is a closure, not data. A queued call restored from disk
//! replays its server-side effect, and the authoritative `TxUpdate`
//! delivers the resulting rows — the overlay's job (instant feedback for
//! the user who clicked) has no meaning across a restart.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Mutex;

use crate::cache::TableSnapshot;
use crate::idempotency::QueueSnapshot;

/// A platform local store (CS-040): IndexedDB in the browser SDK, a
/// file-per-key directory (or anything else) on native. Keys are opaque
/// UTF-8 strings; values are opaque bytes.
///
/// Implementations must be safe to call from the client's reader thread —
/// writes happen as updates apply.
pub trait PersistenceBackend: Send + Sync {
    /// Store `value` under `key`, replacing any previous value.
    fn put(&self, key: &str, value: &[u8]) -> io::Result<()>;
    /// The value under `key`, or `None`.
    fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>>;
    /// Remove `key`. Removing an absent key is not an error.
    fn delete(&self, key: &str) -> io::Result<()>;
    /// Every stored key starting with `prefix`.
    fn list(&self, prefix: &str) -> io::Result<Vec<String>>;
}

/// A file-per-key [`PersistenceBackend`] over one directory — the native
/// default (CS-040). Keys are hex-encoded into file names, so any key bytes
/// are safe on any filesystem.
pub struct FileBackend {
    dir: PathBuf,
}

impl FileBackend {
    /// A backend rooted at `dir` (created if absent).
    pub fn new(dir: impl Into<PathBuf>) -> io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    fn path_of(&self, key: &str) -> PathBuf {
        self.dir.join(format!("{}.bin", hex(key.as_bytes())))
    }
}

impl PersistenceBackend for FileBackend {
    fn put(&self, key: &str, value: &[u8]) -> io::Result<()> {
        // Write-then-rename: a crash mid-write must not leave a torn value
        // where the previous good one was.
        let target = self.path_of(key);
        let tmp = target.with_extension("tmp");
        std::fs::write(&tmp, value)?;
        std::fs::rename(&tmp, &target)
    }

    fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        match std::fs::read(self.path_of(key)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn delete(&self, key: &str) -> io::Result<()> {
        match std::fs::remove_file(self.path_of(key)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        let mut keys = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let name = entry?.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(encoded) = name.strip_suffix(".bin") else {
                continue; // a .tmp remnant or foreign file
            };
            let Some(bytes) = unhex(encoded) else { continue };
            let Ok(key) = String::from_utf8(bytes) else {
                continue;
            };
            if key.starts_with(prefix) {
                keys.push(key);
            }
        }
        keys.sort();
        Ok(keys)
    }
}

/// An in-memory [`PersistenceBackend`] — the test double, and the parity
/// twin of the TypeScript SDK's `MemoryBackend`.
#[derive(Default)]
pub struct MemoryBackend {
    map: Mutex<HashMap<String, Vec<u8>>>,
}

impl MemoryBackend {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl PersistenceBackend for MemoryBackend {
    fn put(&self, key: &str, value: &[u8]) -> io::Result<()> {
        self.map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key.to_owned(), value.to_vec());
        Ok(())
    }

    fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        Ok(self
            .map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
            .cloned())
    }

    fn delete(&self, key: &str) -> io::Result<()> {
        self.map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(key);
        Ok(())
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        let mut keys: Vec<String> = self
            .map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        keys.sort();
        Ok(keys)
    }
}

// --- The persisted state ------------------------------------------------------

/// The client-level blob: the last session's identity plus the offline
/// queue. One per `(server, client_id)`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PersistedMeta {
    /// The identity the persisted state belongs to (CS-040's identity key).
    #[serde(with = "serde_bytes")]
    pub identity: Vec<u8>,
    /// The offline queue, keys included (CS-032).
    pub queue: QueueSnapshot,
}

/// One subscription's persisted state: the SQL (to resubscribe), the
/// highest applied resume offset (CS-020), and the rows it held.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PersistedQuery {
    /// The subscription's SQL, replayed on startup.
    pub sql: String,
    /// The highest `tx_offset` applied before shutdown.
    pub tx_offset: u64,
    /// Table name → held rows, wire bytes.
    pub tables: Vec<(String, Vec<serde_bytes::ByteBuf>)>,
}

impl PersistedQuery {
    /// The rows as cache snapshots.
    pub fn snapshots(&self) -> Vec<TableSnapshot> {
        self.tables
            .iter()
            .map(|(table, rows)| TableSnapshot {
                table: table.clone(),
                rows: rows.iter().map(|r| r.to_vec()).collect(),
            })
            .collect()
    }
}

/// The client's view of its slice of a backend: key construction plus
/// serialize/deserialize, namespaced by `(server, client_id)`.
pub struct ClientStore {
    backend: std::sync::Arc<dyn PersistenceBackend>,
    prefix: String,
}

impl ClientStore {
    /// A store over `backend`, scoped to this server URL and client id.
    pub fn new(
        backend: std::sync::Arc<dyn PersistenceBackend>,
        server: &str,
        client_id: &str,
    ) -> Self {
        Self {
            backend,
            prefix: format!("fluxum|{server}|{client_id}|"),
        }
    }

    fn meta_key(&self) -> String {
        format!("{}meta", self.prefix)
    }

    fn query_key(&self, sql: &str) -> String {
        // The SQL itself hashed into the key: stable, filesystem-agnostic,
        // and collision-free enough for a per-client handful of queries.
        format!("{}query|{:016x}", self.prefix, fnv1a64(sql.as_bytes()))
    }

    /// Load the meta blob, if present and decodable. A corrupt blob hydrates
    /// as nothing — the client then starts cold, which is always safe.
    pub fn load_meta(&self) -> Option<PersistedMeta> {
        let bytes = self.backend.get(&self.meta_key()).ok()??;
        rmp_serde::from_slice(&bytes).ok()
    }

    /// Write the meta blob. Errors are swallowed by design: persistence is
    /// an optimization, and a full disk must not take the live session down.
    pub fn save_meta(&self, meta: &PersistedMeta) {
        if let Ok(bytes) = rmp_serde::to_vec(meta) {
            let _ = self.backend.put(&self.meta_key(), &bytes);
        }
    }

    /// Load every persisted query, sorted by SQL for determinism.
    pub fn load_queries(&self) -> Vec<PersistedQuery> {
        let Ok(keys) = self.backend.list(&format!("{}query|", self.prefix)) else {
            return Vec::new();
        };
        let mut queries: Vec<PersistedQuery> = keys
            .iter()
            .filter_map(|key| {
                let bytes = self.backend.get(key).ok()??;
                rmp_serde::from_slice(&bytes).ok()
            })
            .collect();
        queries.sort_by(|a, b| a.sql.cmp(&b.sql));
        queries
    }

    /// Write one query's state.
    pub fn save_query(&self, query: &PersistedQuery) {
        if let Ok(bytes) = rmp_serde::to_vec(query) {
            let _ = self.backend.put(&self.query_key(&query.sql), &bytes);
        }
    }

    /// Drop one query's state (the client unsubscribed).
    pub fn delete_query(&self, sql: &str) {
        let _ = self.backend.delete(&self.query_key(sql));
    }

    /// Drop everything under this `(server, client_id)` — the hydrated
    /// identity did not match the fresh session's.
    pub fn clear(&self) {
        if let Ok(keys) = self.backend.list(&self.prefix) {
            for key in keys {
                let _ = self.backend.delete(&key);
            }
        }
    }
}

/// FNV-1a, 64-bit — a tiny stable hash for key derivation (not security).
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    hash
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn sample_query() -> PersistedQuery {
        PersistedQuery {
            sql: "SELECT * FROM Task".into(),
            tx_offset: 42,
            tables: vec![(
                "Task".into(),
                vec![
                    serde_bytes::ByteBuf::from(vec![1, 7]),
                    serde_bytes::ByteBuf::from(vec![2, 9]),
                ],
            )],
        }
    }

    fn roundtrip(backend: Arc<dyn PersistenceBackend>) {
        let store = ClientStore::new(backend, "fluxum://h:1", "cli-1");
        assert!(store.load_meta().is_none(), "cold start hydrates nothing");
        assert!(store.load_queries().is_empty());

        let meta = PersistedMeta {
            identity: vec![7u8; 32],
            queue: QueueSnapshot {
                client_id: "cli-1".into(),
                next_seq: 3,
                pending: Vec::new(),
            },
        };
        store.save_meta(&meta);
        assert_eq!(store.load_meta().unwrap(), meta);

        let query = sample_query();
        store.save_query(&query);
        let loaded = store.load_queries();
        assert_eq!(loaded, vec![query.clone()]);
        assert_eq!(
            loaded[0].snapshots()[0].rows,
            vec![vec![1, 7], vec![2, 9]],
            "snapshots decode back to wire bytes"
        );

        // Re-saving replaces, not appends.
        store.save_query(&query);
        assert_eq!(store.load_queries().len(), 1);

        store.delete_query(&query.sql);
        assert!(store.load_queries().is_empty());
        assert!(store.load_meta().is_some(), "meta untouched by query delete");

        store.clear();
        assert!(store.load_meta().is_none(), "clear drops the namespace");
    }

    #[test]
    fn memory_backend_round_trips_the_full_state() {
        roundtrip(Arc::new(MemoryBackend::new()));
    }

    #[test]
    fn file_backend_round_trips_the_full_state() {
        let dir = std::env::temp_dir().join(format!("fluxum-persist-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        roundtrip(Arc::new(FileBackend::new(&dir).unwrap()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn namespaces_do_not_bleed_across_servers_or_clients() {
        let backend: Arc<dyn PersistenceBackend> = Arc::new(MemoryBackend::new());
        let a = ClientStore::new(Arc::clone(&backend), "fluxum://h:1", "cli-a");
        let b = ClientStore::new(Arc::clone(&backend), "fluxum://h:1", "cli-b");
        let other = ClientStore::new(Arc::clone(&backend), "fluxum://h:2", "cli-a");

        a.save_query(&sample_query());
        assert_eq!(a.load_queries().len(), 1);
        assert!(b.load_queries().is_empty(), "another client id");
        assert!(other.load_queries().is_empty(), "another server");

        b.save_query(&sample_query());
        a.clear();
        assert!(a.load_queries().is_empty());
        assert_eq!(b.load_queries().len(), 1, "clear is namespace-scoped");
    }

    #[test]
    fn a_corrupt_blob_hydrates_as_nothing() {
        let backend = Arc::new(MemoryBackend::new());
        let store = ClientStore::new(
            Arc::clone(&backend) as Arc<dyn PersistenceBackend>,
            "fluxum://h:1",
            "cli-1",
        );
        store.save_query(&sample_query());
        // Overwrite both blobs with garbage a MessagePack decoder rejects.
        for key in backend.list("").unwrap() {
            backend.put(&key, &[0xC1, 0xFF, 0x00]).unwrap();
        }
        store.save_meta(&PersistedMeta {
            identity: vec![0; 32],
            queue: QueueSnapshot {
                client_id: "c".into(),
                next_seq: 0,
                pending: Vec::new(),
            },
        });
        backend.put(&store.meta_key(), b"not messagepack").unwrap();
        assert!(store.load_meta().is_none(), "corrupt meta = cold start");
        assert!(store.load_queries().is_empty(), "corrupt query dropped");
    }

    #[test]
    fn file_backend_survives_foreign_files_and_absent_deletes() {
        let dir = std::env::temp_dir().join(format!("fluxum-persist-x-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let backend = FileBackend::new(&dir).unwrap();
        std::fs::write(dir.join("README.txt"), b"not ours").unwrap();
        std::fs::write(dir.join("zz.tmp"), b"torn write remnant").unwrap();
        backend.put("k", b"v").unwrap();
        assert_eq!(backend.list("").unwrap(), vec!["k".to_owned()]);
        assert_eq!(backend.get("k").unwrap().unwrap(), b"v");
        backend.delete("k").unwrap();
        backend.delete("k").unwrap(); // absent: not an error
        assert!(backend.get("k").unwrap().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
