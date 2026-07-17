//! SPEC-017 §5 CT-030/031/032/036 — field-level encryption end to end: an
//! `#[encrypted]` column stores ciphertext (no plaintext at rest), an
//! authorized reducer read returns the exact plaintext, tampering/relocation
//! is rejected, and a retired key still decrypts (rotation).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;

use fluxum_core::Result;
use fluxum_core::reducer::{ReducerCaller, ReducerRegistry, with_context};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, Table, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue, TableId};
use fluxum_core::transform::crypto::EciesKey;
use fluxum_core::transform::engine::TransformEngine;
use fluxum_core::types::{ConnectionId, Identity, Timestamp};

const SHARD: u32 = 17;

// A `Vote` table whose `choice` column (ordinal 1, declared `String`) is
// `#[encrypted]`. The macro registers the descriptor in real builds; the test
// attaches an engine with the equivalent plan.
static VOTE_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "choice",
        ty: FluxType::Str,
    },
];
static VOTE: TableSchema = TableSchema {
    name: "Vote",
    columns: VOTE_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

/// Hand-built typed row for `Vote` (macro `Table` impl stand-in).
#[derive(Debug, PartialEq)]
struct Vote {
    id: u64,
    choice: String,
}
impl Table for Vote {
    type Pk = u64;
    const SCHEMA: &'static TableSchema = &VOTE;
    fn primary_key(&self) -> u64 {
        self.id
    }
    fn into_values(self) -> Vec<RowValue> {
        vec![RowValue::U64(self.id), RowValue::Str(self.choice)]
    }
    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [RowValue::U64(id), RowValue::Str(choice)] => Ok(Self {
                id: *id,
                choice: choice.clone(),
            }),
            other => Err(fluxum_core::FluxumError::Storage(format!(
                "Vote: unexpected row shape {other:?}"
            ))),
        }
    }
    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

fn ecies_key(id: &str, seed: u8, previous: &[u8]) -> EciesKey {
    EciesKey::new(id, [seed; 32], previous.iter().map(|s| [*s; 32]).collect())
}

fn store_with_engine(engine: TransformEngine) -> Arc<MemStore> {
    let schema = Schema::from_tables([&VOTE]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    store.attach_transform_engine(Arc::new(engine));
    store
}

fn engine(keys: HashMap<String, EciesKey>) -> TransformEngine {
    TransformEngine::for_encrypted_test(
        TableId::of("Vote"),
        vec![(1, &FluxType::Str, "votes".to_owned())],
        keys,
    )
}

fn caller() -> ReducerCaller {
    ReducerCaller {
        identity: Identity::from_token("tester"),
        connection_id: ConnectionId::new(1),
        timestamp: Timestamp::from_micros(0),
        shard_id: SHARD,
    }
}

/// CT-030: a written `#[encrypted]` column is stored as a ciphertext envelope
/// (no plaintext at rest), and an authorized reducer read returns the exact
/// plaintext (CT-031).
#[test]
fn encrypted_column_stores_ciphertext_and_reads_back_plaintext() {
    let keys = HashMap::from([("votes".to_owned(), ecies_key("votes", 1, &[]))]);
    let store = store_with_engine(engine(keys));
    let registry = ReducerRegistry::new();

    let secret = "candidate-alice-🗳";
    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        let stored = ctx.tx.insert(Vote {
            id: 1,
            choice: secret.to_owned(),
        })?;
        // insert() returns plaintext to the reducer.
        assert_eq!(stored.choice, secret);
        Ok(())
    })
    .unwrap();
    tx.commit().unwrap();

    // The committed row at rest carries ciphertext, not the plaintext.
    let snapshot = store.snapshot();
    let table = store.table_id("Vote").unwrap();
    let raw = snapshot.scan(table).unwrap().next().unwrap().clone();
    match raw.value(1) {
        Some(RowValue::Bytes(env)) => {
            assert!(
                !env.windows(secret.len()).any(|w| w == secret.as_bytes()),
                "plaintext must not appear in the stored envelope"
            );
        }
        other => panic!("encrypted column must store Bytes, got {other:?}"),
    }
    // The pk column stays plaintext (keys are never encrypted, CT-013).
    assert_eq!(raw.value(0), Some(&RowValue::U64(1)));

    // An authorized reducer read (server peer) decrypts back to plaintext.
    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        let got = ctx.tx.query_pk::<Vote>(1)?.expect("row present");
        assert_eq!(
            got.choice, secret,
            "authorized read returns exact plaintext"
        );
        let all = ctx.tx.scan::<Vote>()?;
        assert_eq!(all[0].choice, secret, "scan decrypts too");
        Ok(())
    })
    .unwrap();
    tx.commit().unwrap();
}

// The write path re-encrypts any value it stores, so the following CT-032/036
// threat-model tests (a hostile relocation/tamper of at-rest ciphertext, and
// reading legacy data after rotation) operate on the engine's read boundary
// directly — the point that authenticates the stored envelope.

fn seal_choice(eng: &TransformEngine, pk: u64, choice: &str) -> Vec<u8> {
    let table = TableId::of("Vote");
    let mut values = vec![RowValue::U64(pk), RowValue::Str(choice.to_owned())];
    let pk_bytes = pk.to_le_bytes(); // any stable pk encoding; the engine binds it
    eng.on_write_row(table, &mut values, &pk_bytes).unwrap();
    match &values[1] {
        RowValue::Bytes(b) => b.clone(),
        other => panic!("expected ciphertext, got {other:?}"),
    }
}

fn open_choice(eng: &TransformEngine, pk: u64, env: Vec<u8>) -> Result<String> {
    let table = TableId::of("Vote");
    let mut values = vec![RowValue::U64(pk), RowValue::Bytes(env)];
    let pk_bytes = pk.to_le_bytes();
    eng.on_read_row(table, &mut values, &pk_bytes, true)?;
    match &values[1] {
        RowValue::Str(s) => Ok(s.clone()),
        other => panic!("expected plaintext, got {other:?}"),
    }
}

/// CT-032: the AEAD binds the ciphertext to `(table, column, pk)`, so a value
/// sealed for one primary key fails to open under another — a relocated row
/// cannot leak its value under the wrong identity.
#[test]
fn relocating_ciphertext_to_another_pk_fails_to_decrypt() {
    let eng = engine(HashMap::from([(
        "votes".to_owned(),
        ecies_key("votes", 1, &[]),
    )]));
    let env = seal_choice(&eng, 1, "secret");
    assert_eq!(open_choice(&eng, 1, env.clone()).unwrap(), "secret");
    let err = open_choice(&eng, 2, env).unwrap_err();
    assert!(err.to_string().contains("no configured key"), "{err}");
}

/// CT-032: a tampered ciphertext envelope is rejected on read.
#[test]
fn tampered_ciphertext_is_rejected_on_read() {
    let eng = engine(HashMap::from([(
        "votes".to_owned(),
        ecies_key("votes", 1, &[]),
    )]));
    let mut env = seal_choice(&eng, 1, "secret");
    let last = env.len() - 1;
    env[last] ^= 0x01;
    assert!(open_choice(&eng, 1, env).is_err());
}

/// CT-036: after rotating the active key, a value written under the old key
/// still decrypts (the old key is retained as a `previous` read key), while
/// new writes seal under the new active key.
#[test]
fn rotation_reads_legacy_and_writes_new() {
    // Sealed under active seed 1.
    let old = engine(HashMap::from([(
        "votes".to_owned(),
        ecies_key("votes", 1, &[]),
    )]));
    let legacy = seal_choice(&old, 1, "legacy");

    // Rotated engine: active seed 2, seed 1 retained as a previous read key.
    let rotated = engine(HashMap::from([(
        "votes".to_owned(),
        ecies_key("votes", 2, &[1]),
    )]));
    // The legacy value still decrypts under the previous key (CT-036).
    assert_eq!(open_choice(&rotated, 1, legacy).unwrap(), "legacy");
    // A fresh seal uses the active key and round-trips.
    let fresh = seal_choice(&rotated, 3, "fresh");
    assert_eq!(open_choice(&rotated, 3, fresh).unwrap(), "fresh");
    // The old engine (which lacks seed 2) cannot read the new active-key value.
    let fresh2 = seal_choice(&rotated, 4, "fresh2");
    assert!(open_choice(&old, 4, fresh2).is_err());
}
