//! SPEC-017 §5 CT-030/035 — the production `TransformEngine::build` path over
//! a real `#[fluxum::table]` with an `#[encrypted]` column: the link-time
//! transform registry resolves against the config keyring, a missing key
//! aborts the build (CT-035), and an end-to-end store round-trip encrypts at
//! rest and decrypts for an authorized reducer.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::config::{KeyScheme, TransformKey, TransformsConfig};
use fluxum_core::reducer::{ReducerCaller, ReducerRegistry, with_context};
use fluxum_core::schema::{Schema, Table};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::transform::engine::TransformEngine;
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_macros as fluxum;

#[fluxum::table(public)]
#[derive(Debug, Clone, PartialEq)]
pub struct Ballot {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub voter: Identity,
    #[encrypted(ecies, key = "ballots")]
    pub choice: String,
}

fn config_with_key() -> TransformsConfig {
    TransformsConfig {
        keys: vec![TransformKey {
            id: "ballots".into(),
            scheme: KeyScheme::X25519,
            secret: "11".repeat(32),
            previous: vec![],
        }],
    }
}

fn caller() -> ReducerCaller {
    ReducerCaller {
        identity: Identity::from_token("server"),
        connection_id: ConnectionId::new(0),
        timestamp: Timestamp::from_micros(0),
        shard_id: 1,
    }
}

/// CT-035: `build` fails when an `#[encrypted]` attribute names a key that is
/// not configured — the server would abort startup rather than run without the
/// key material.
#[test]
fn build_aborts_on_a_missing_key() {
    let schema = Schema::from_tables([Ballot::SCHEMA]).unwrap();
    let err = TransformEngine::build(&schema, &TransformsConfig::default()).unwrap_err();
    assert!(err.to_string().contains("not a configured"), "{err}");
}

/// CT-030: `build` over the real registry produces an engine that encrypts the
/// `#[encrypted]` column at rest, and an authorized reducer read returns the
/// exact plaintext.
#[test]
fn build_wires_encryption_end_to_end() {
    let schema = Schema::from_tables([Ballot::SCHEMA]).unwrap();
    let engine = TransformEngine::build(&schema, &config_with_key())
        .unwrap()
        .expect("an #[encrypted] column ⇒ Some(engine)");

    let store = Arc::new(MemStore::new(&schema).unwrap());
    store.attach_transform_engine(Arc::new(engine));
    let registry = ReducerRegistry::new();

    let secret = "candidate-42";
    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        let stored = ctx.tx.insert(Ballot {
            id: 0,
            voter: Identity::from_token("alice"),
            choice: secret.to_owned(),
        })?;
        assert_eq!(stored.choice, secret, "reducer sees plaintext");
        Ok(())
    })
    .unwrap();
    tx.commit().unwrap();

    // At rest: the `choice` column is ciphertext, `voter`/`id` are plaintext.
    let table = store.table_id("Ballot").unwrap();
    let row = store
        .snapshot()
        .scan(table)
        .unwrap()
        .next()
        .unwrap()
        .clone();
    let choice_ord = Ballot::SCHEMA
        .columns
        .iter()
        .position(|c| c.name == "choice")
        .unwrap();
    match row.value(choice_ord as u16) {
        Some(RowValue::Bytes(env)) => assert!(
            !env.windows(secret.len()).any(|w| w == secret.as_bytes()),
            "no plaintext at rest"
        ),
        other => panic!("expected ciphertext, got {other:?}"),
    }

    // Authorized reducer read decrypts.
    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        let all = ctx.tx.scan::<Ballot>()?;
        assert_eq!(all[0].choice, secret, "authorized read returns plaintext");
        Ok(())
    })
    .unwrap();
    tx.commit().unwrap();
}
