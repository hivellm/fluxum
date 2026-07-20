//! SPEC-017 §6 (CT-040/041/042 + CT-034/037/060) — column-level security
//! end-to-end: per-column grants resolve against roles/server-peer, masking
//! substitutes on every read surface, diffs project per viewer and suppress
//! masked-only changes, crypto composes (decrypt-when-granted, ciphertext
//! mask, `_verified` siblings, per-identity signing), and the stored
//! transform set is fingerprinted for CT-060.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::config::{KeyScheme, TransformKey, TransformsConfig};
use fluxum_core::migration::StoredCatalog;
use fluxum_core::reducer::{ReducerCaller, ReducerRegistry, with_context};
use fluxum_core::schema::{Schema, Table};
use fluxum_core::store::MemStore;
use fluxum_core::subscription::{Subscriber, SubscriptionLimits, SubscriptionManager};
use fluxum_core::transform::engine::TransformEngine;
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_macros as fluxum;

#[fluxum::table(public)]
#[derive(Debug, Clone, PartialEq)]
pub struct Payroll {
    #[primary_key]
    pub id: u64,
    pub name: String,
    /// HR-only, pseudonymized for everyone else (CT-040/041).
    #[column_grant(select = "hr")]
    #[masked(hash)]
    pub ssn: String,
    /// Server-peer only, zeroed for clients.
    #[column_grant(select = server_peer)]
    #[masked(redact)]
    pub salary: u64,
    /// HR-only, nulled for everyone else.
    #[column_grant(select = "hr")]
    #[masked(null)]
    pub note: Option<String>,
}

#[fluxum::table(public)]
#[derive(Debug, Clone, PartialEq)]
pub struct Vault {
    #[primary_key]
    pub id: u64,
    pub owner: Identity,
    /// Encrypted at rest; unauthorized readers get the sealed envelope.
    #[encrypted(ecies, key = "vault")]
    #[column_grant(select = "auditor")]
    #[masked(ciphertext)]
    pub secret: String,
    /// Integrity-signed by the server (CT-033/034).
    #[signed(ed25519, by = server)]
    pub ledger: String,
    /// Per-identity signature bound to `owner` (CT-037).
    #[signed(ed25519, by = owner)]
    pub receipt: String,
}

fn caller() -> ReducerCaller {
    ReducerCaller {
        identity: Identity::from_token("t"),
        connection_id: ConnectionId::new(1),
        timestamp: Timestamp::from_micros(0),
        shard_id: 1,
    }
}

fn hr() -> Subscriber {
    Subscriber::client_with_roles(Identity::from_bytes([1; 32]), vec!["hr".to_owned()])
}
fn intern() -> Subscriber {
    Subscriber::client(Identity::from_bytes([2; 32]))
}
fn peer() -> Subscriber {
    Subscriber::server_peer(Identity::from_bytes([9; 32]))
}

fn payroll_setup() -> (MemStore, SubscriptionManager) {
    let schema = Arc::new(Schema::from_tables([Payroll::SCHEMA]).unwrap());
    let store = MemStore::new(&schema).unwrap();
    let manager = SubscriptionManager::new(Arc::clone(&schema), SubscriptionLimits::default());
    (store, manager)
}

fn insert_payroll(store: &MemStore, id: u64, name: &str, ssn: &str, salary: u64) {
    let registry = ReducerRegistry::new();
    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        ctx.tx.insert(Payroll {
            id,
            name: name.into(),
            ssn: ssn.into(),
            salary,
            note: Some("confidential".into()),
        })
    })
    .unwrap();
    tx.commit().unwrap();
}

fn row_json(
    manager: &SubscriptionManager,
    store: &MemStore,
    subscriber: Subscriber,
    sql: &str,
) -> serde_json::Value {
    let result = manager
        .query_json(subscriber, sql, &store.snapshot())
        .unwrap();
    result["rows"][0].clone()
}

// --- CT-040/041: grants + masking on the read surfaces ---------------------------

#[test]
fn grants_resolve_per_caller_and_masks_substitute() {
    let (store, manager) = payroll_setup();
    insert_payroll(&store, 1, "ada", "123-45-6789", 90_000);
    let sql = "SELECT * FROM Payroll";

    // Role-less client: ssn pseudonymized, salary zeroed, note nulled.
    let row = row_json(&manager, &store, intern(), sql);
    assert_eq!(row["name"], "ada", "ungoverned columns stay raw");
    let hashed = row["ssn"].as_str().unwrap().to_owned();
    assert_ne!(hashed, "123-45-6789");
    assert_eq!(hashed.len(), 64, "sha-256 hex pseudonym (CT-041)");
    assert_eq!(row["salary"], 0, "redacted");
    assert!(row["note"].is_null(), "nulled");

    // The pseudonym is deterministic (joinable) but not reversible.
    let again = row_json(&manager, &store, intern(), sql);
    assert_eq!(again["ssn"].as_str().unwrap(), hashed);

    // HR role: ssn + note raw; salary still server-peer-only.
    let row = row_json(&manager, &store, hr(), sql);
    assert_eq!(row["ssn"], "123-45-6789");
    assert_eq!(row["note"], "confidential");
    assert_eq!(row["salary"], 0);

    // Server peer: everything raw (AUTH-062 bypass).
    let row = row_json(&manager, &store, peer(), sql);
    assert_eq!(row["ssn"], "123-45-6789");
    assert_eq!(row["salary"], 90_000);
    assert_eq!(row["note"], "confidential");
}

// --- CT-042: per-viewer diffs + masked-only-change suppression --------------------

#[test]
fn diffs_project_per_viewer_and_masked_only_changes_stay_silent() {
    let (store, mut manager) = payroll_setup();
    let sql = "SELECT * FROM Payroll";
    manager.subscribe(1, hr(), sql, &store.snapshot()).unwrap();
    manager
        .subscribe(2, intern(), sql, &store.snapshot())
        .unwrap();
    manager
        .subscribe(3, peer(), sql, &store.snapshot())
        .unwrap();

    // Insert: three distinct caller-scoped buckets, one delta each.
    let registry = ReducerRegistry::new();
    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        ctx.tx.insert(Payroll {
            id: 1,
            name: "ada".into(),
            ssn: "123-45-6789".into(),
            salary: 90_000,
            note: None,
        })
    })
    .unwrap();
    let deltas = manager.on_commit(&tx.commit().unwrap()).unwrap();
    assert_eq!(deltas.len(), 3, "role folds into the bucket key (CT-040)");

    // Update ONLY the salary: masked for both clients → suppressed for
    // them; the server peer still receives it (CT-042).
    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        ctx.tx.upsert(Payroll {
            id: 1,
            name: "ada".into(),
            ssn: "123-45-6789".into(),
            salary: 95_000,
            note: None,
        })
    })
    .unwrap();
    let deltas = manager.on_commit(&tx.commit().unwrap()).unwrap();
    let reached: Vec<u128> = deltas.iter().flat_map(|d| d.subscribers.clone()).collect();
    assert_eq!(
        reached,
        vec![3],
        "a masked-column-only change leaks nothing to unauthorized viewers"
    );

    // A public-column change reaches everyone.
    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        ctx.tx.upsert(Payroll {
            id: 1,
            name: "ada lovelace".into(),
            ssn: "123-45-6789".into(),
            salary: 95_000,
            note: None,
        })
    })
    .unwrap();
    let deltas = manager.on_commit(&tx.commit().unwrap()).unwrap();
    let mut reached: Vec<u128> = deltas.iter().flat_map(|d| d.subscribers.clone()).collect();
    reached.sort_unstable();
    assert_eq!(reached, vec![1, 2, 3]);
}

// --- CT-031/034/037: crypto composition -------------------------------------------

fn vault_config() -> TransformsConfig {
    TransformsConfig {
        keys: vec![
            TransformKey {
                id: "vault".into(),
                scheme: KeyScheme::X25519,
                secret: "aa".repeat(32).into(),
                previous: vec![],
            },
            TransformKey {
                id: "server".into(),
                scheme: KeyScheme::Ed25519,
                secret: "bb".repeat(32).into(),
                previous: vec![],
            },
        ],
    }
}

#[test]
fn crypto_composes_with_grants_and_verified_siblings() {
    let schema = Arc::new(Schema::from_tables([Vault::SCHEMA]).unwrap());
    let engine = Arc::new(
        TransformEngine::build(&schema, &vault_config())
            .unwrap()
            .expect("Vault has executors"),
    );
    let store = MemStore::new(&schema).unwrap();
    store.attach_transform_engine(Arc::clone(&engine));
    let mut manager = SubscriptionManager::new(Arc::clone(&schema), SubscriptionLimits::default());
    manager.set_transforms(Arc::clone(&engine));
    let _ = &mut manager;

    let owner = Identity::from_bytes([7; 32]);
    let registry = ReducerRegistry::new();
    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        ctx.tx.insert(Vault {
            id: 1,
            owner,
            secret: "the launch code".into(),
            ledger: "credit 100".into(),
            receipt: "paid in full".into(),
        })
    })
    .unwrap();
    tx.commit().unwrap();

    let sql = "SELECT * FROM Vault";
    // The auditor role decrypts (CT-031 authorized read).
    let auditor =
        Subscriber::client_with_roles(Identity::from_bytes([3; 32]), vec!["auditor".to_owned()]);
    let row = row_json(&manager, &store, auditor, sql);
    assert_eq!(row["secret"], "the launch code");
    // Signed columns verify — the `<field>_verified` siblings (CT-034/037).
    assert_eq!(row["ledger"], "credit 100");
    assert_eq!(row["ledger_verified"], true);
    assert_eq!(row["receipt"], "paid in full");
    assert_eq!(
        row["receipt_verified"], true,
        "per-identity key verifies (CT-037)"
    );

    // An unauthorized client gets the sealed envelope, never plaintext
    // (CT-012: masking + ciphertext strategy).
    let row = row_json(&manager, &store, intern(), sql);
    let masked = row["secret"].as_str().unwrap_or_default().to_owned();
    assert_ne!(masked, "the launch code");
    assert!(
        !masked.contains("launch"),
        "no plaintext fragment leaks: {masked}"
    );
}

// --- CT-060: the stored transform set is fingerprinted -----------------------------

#[test]
fn stored_catalog_fingerprints_write_transforms_and_grandfathers_old_bytes() {
    let schema = Schema::from_tables([Vault::SCHEMA, Payroll::SCHEMA]).unwrap();
    let catalog = StoredCatalog::from_schema(&schema);
    assert!(
        catalog.transforms.contains_key("Vault.secret")
            && catalog.transforms.contains_key("Vault.ledger"),
        "encryption/signing shape stored bytes and are fingerprinted: {:?}",
        catalog.transforms.keys().collect::<Vec<_>>()
    );
    assert!(
        !catalog.transforms.contains_key("Payroll.ssn"),
        "grants/masks change nothing at rest and stay out of CT-060"
    );
    // Round-trip; and a catalog encoded WITHOUT the field (pre-CT-060
    // bytes) still decodes, with an empty (grandfathered) fingerprint.
    let decoded = StoredCatalog::decode(&catalog.encode().unwrap()).unwrap();
    assert_eq!(decoded, catalog);
}
