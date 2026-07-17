//! The column-transform execution engine (SPEC-017 §3 hooks, §5 crypto).
//!
//! Phase 1 registered the transform *descriptors* per `(table, column)`; this
//! engine turns them into runtime executors and applies them at the storage
//! boundary in canonical order (CT-011: `encrypted` → `signed` on write, the
//! reverse on read):
//!
//! - **on_write** ([`TransformEngine::on_write_row`]): runs after row
//!   validation and before storage. `#[encrypted]` columns are ECIES-sealed
//!   ([`super::crypto`]) so the commit log, cold pages, checkpoints, and
//!   replication stream only ever hold ciphertext (CT-030); `#[signed(by =
//!   server)]` columns get an Ed25519 signature appended over `(table, column,
//!   pk, field_bytes)` (CT-033) — a signed-only field stays in clear text,
//!   only integrity is added. Encrypted values store the ciphertext envelope
//!   as `Bytes`; the AEAD associated data binds them to `(table, column,
//!   primary_key)` (CT-032).
//! - **on_read** ([`TransformEngine::on_read_row`]): verifies + strips a
//!   signature (a failure increments `fluxum_signature_verify_failures_total`
//!   and never drops the row, CT-034) and decrypts `#[encrypted]` columns back
//!   to plaintext **only for an authorized caller** (CT-031). Reducers run as
//!   server peers (AUTH-062) and are always authorized; client-facing reads
//!   keep the ciphertext until phase-4 column-grant resolution.
//!
//! Scope: `#[encrypted(ecies)]` and `#[signed(ed25519, by = server)]` execute
//! here. `#[signed(by = <Identity column>)]` (per-identity keys) and the
//! reducer-facing `<field>_verified` projection sibling ride the phase-4
//! projection/SDK layer; the storage-layer verification + metric are done.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::config::TransformsConfig;
use crate::error::{FluxumError, Result};
use crate::schema::{FluxType, Schema, TableSchema};
use crate::store::row::{decode_value_bytes, encode_value_bytes};
use crate::store::{RowValue, TableId};
use crate::transform::crypto::{EciesKey, SIGNATURE_LEN, SignKey, ecies_open, ecies_seal};
use crate::transform::{SignedBy, TransformDescriptor, registered_column_transforms};

/// The reserved config key id for the `#[signed(by = server)]` signing key.
const SERVER_SIGN_KEY_ID: &str = "server";

/// The signing authority of a `#[signed]` column (CT-033).
#[derive(Clone, Copy)]
enum SignBy {
    /// `by = server` — the server Ed25519 key signs and verifies.
    Server,
}

/// One column's compiled crypto plan.
struct ColumnPlan {
    /// Declared plaintext [`FluxType`] — the decode target after inverse.
    plain_ty: &'static FluxType,
    /// The named ECIES key this column encrypts to (`#[encrypted]`), if any.
    encrypt_key: Option<String>,
    /// The signing authority (`#[signed]`), if any.
    sign: Option<SignBy>,
}

impl ColumnPlan {
    fn has_executor(&self) -> bool {
        self.encrypt_key.is_some() || self.sign.is_some()
    }
}

/// The per-shard transform executor: the resolved keyrings, the compiled
/// per-column plans, and the CT-014/034 read counters (SPEC-017 §5).
#[derive(Default)]
pub struct TransformEngine {
    ecies_keys: HashMap<String, EciesKey>,
    server_sign_key: Option<SignKey>,
    /// `(table_id, ordinal)` → plan, for every column carrying an executor.
    columns: HashMap<(TableId, u16), ColumnPlan>,
    /// Tables that have at least one executor column, for a fast skip.
    tables: HashMap<TableId, Vec<u16>>,
    /// CT-034: signature verifications that failed on read.
    verify_failures: AtomicU64,
    /// CT-014: read-path transform errors (decrypt/decode failures).
    read_errors: AtomicU64,
}

impl std::fmt::Debug for TransformEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransformEngine")
            .field("ecies_keys", &self.ecies_keys.keys().collect::<Vec<_>>())
            .field("has_server_sign_key", &self.server_sign_key.is_some())
            .field("columns", &self.columns.len())
            .finish()
    }
}

impl TransformEngine {
    /// Build the engine from the assembled `schema`, the link-time transform
    /// registry, and `config` key material (CT-035). Fails startup if an
    /// `#[encrypted]`/`#[signed]` attribute names key material that is absent
    /// or of the wrong scheme.
    pub fn build(schema: &Schema, config: &TransformsConfig) -> Result<Option<Self>> {
        let ecies_keys = config.ecies_keys()?;
        let server_sign_key = config.ed25519_keys()?.remove(SERVER_SIGN_KEY_ID);
        let mut columns = HashMap::new();
        let mut tables: HashMap<TableId, Vec<u16>> = HashMap::new();

        for def in registered_column_transforms() {
            let Some(table) = schema.table(def.table) else {
                continue; // registry is process-global; schemas may be subsets
            };
            let Some((ordinal, column)) = column_of(table, def.column) else {
                continue; // validated elsewhere (CT-051)
            };

            let encrypt_key = def.transforms.iter().find_map(|t| match t {
                TransformDescriptor::Encrypted { key, .. } => Some((*key).to_owned()),
                _ => None,
            });
            if let Some(key_name) = &encrypt_key
                && !ecies_keys.contains_key(key_name)
            {
                return Err(FluxumError::Config(format!(
                    "table `{}` column `{}`: #[encrypted] names key `{key_name}`, which is not a \
                     configured X25519 transform key (CT-035)",
                    def.table, def.column
                )));
            }

            let sign = def.transforms.iter().find_map(|t| match t {
                TransformDescriptor::Signed { by, .. } => Some(*by),
                _ => None,
            });
            let sign = match sign {
                None => None,
                Some(SignedBy::Server) => {
                    if server_sign_key.is_none() {
                        return Err(FluxumError::Config(format!(
                            "table `{}` column `{}`: #[signed(by = server)] needs an Ed25519 \
                             transform key with id `{SERVER_SIGN_KEY_ID}` (CT-035)",
                            def.table, def.column
                        )));
                    }
                    Some(SignBy::Server)
                }
                Some(SignedBy::IdentityColumn(_)) => {
                    return Err(FluxumError::Config(format!(
                        "table `{}` column `{}`: #[signed(by = <Identity column>)] (per-identity \
                         keys) is a phase-4 follow-up; use `by = server` for now (CT-033)",
                        def.table, def.column
                    )));
                }
            };

            let plan = ColumnPlan {
                plain_ty: &column.ty,
                encrypt_key,
                sign,
            };
            if !plan.has_executor() {
                continue; // normalize/mask/grant execute elsewhere
            }
            let table_id = TableId::of(table.name);
            tables.entry(table_id).or_default().push(ordinal);
            columns.insert((table_id, ordinal), plan);
        }

        if columns.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self {
            ecies_keys,
            server_sign_key,
            columns,
            tables,
            verify_failures: AtomicU64::new(0),
            read_errors: AtomicU64::new(0),
        }))
    }

    /// Build an engine with explicit per-column plans over one table — the
    /// test seam. Each column is `(ordinal, plain_ty, encrypt_key, signed)`.
    #[doc(hidden)]
    pub fn for_test(
        table: TableId,
        columns: Vec<(u16, &'static FluxType, Option<String>, bool)>,
        ecies_keys: HashMap<String, EciesKey>,
        server_sign_key: Option<SignKey>,
    ) -> Self {
        let mut plans = HashMap::new();
        let mut tables: HashMap<TableId, Vec<u16>> = HashMap::new();
        for (ordinal, plain_ty, encrypt_key, signed) in columns {
            tables.entry(table).or_default().push(ordinal);
            plans.insert(
                (table, ordinal),
                ColumnPlan {
                    plain_ty,
                    encrypt_key,
                    sign: signed.then_some(SignBy::Server),
                },
            );
        }
        Self {
            ecies_keys,
            server_sign_key,
            columns: plans,
            tables,
            verify_failures: AtomicU64::new(0),
            read_errors: AtomicU64::new(0),
        }
    }

    /// Encrypt-only test engine (back-compat helper for the ECIES tests).
    #[doc(hidden)]
    pub fn for_encrypted_test(
        table: TableId,
        columns: Vec<(u16, &'static FluxType, String)>,
        ecies_keys: HashMap<String, EciesKey>,
    ) -> Self {
        Self::for_test(
            table,
            columns
                .into_iter()
                .map(|(o, ty, k)| (o, ty, Some(k), false))
                .collect(),
            ecies_keys,
            None,
        )
    }

    /// Whether `table` has any executor column (fast-path skip).
    pub fn touches(&self, table: TableId) -> bool {
        self.tables.contains_key(&table)
    }

    /// CT-034: total signature verifications that failed on read.
    pub fn verify_failures(&self) -> u64 {
        self.verify_failures.load(Ordering::Relaxed)
    }

    /// CT-014: total read-path transform errors (decrypt/decode failures).
    pub fn read_errors(&self) -> u64 {
        self.read_errors.load(Ordering::Relaxed)
    }

    /// The AEAD/signature context binding a field to its position (CT-032/033):
    /// `table_id ‖ ordinal ‖ primary_key`.
    fn context(table: TableId, ordinal: u16, pk_bytes: &[u8]) -> Vec<u8> {
        let mut ctx = Vec::with_capacity(6 + pk_bytes.len());
        ctx.extend_from_slice(&table.as_u32().to_le_bytes());
        ctx.extend_from_slice(&ordinal.to_le_bytes());
        ctx.extend_from_slice(pk_bytes);
        ctx
    }

    /// Apply the write-path executors to `values` in place (CT-011/030/033):
    /// encrypt, then sign. Called after validation, before storage.
    pub fn on_write_row(
        &self,
        table: TableId,
        values: &mut [RowValue],
        pk_bytes: &[u8],
    ) -> Result<()> {
        let Some(ordinals) = self.tables.get(&table) else {
            return Ok(());
        };
        for &ordinal in ordinals {
            let plan = &self.columns[&(table, ordinal)];
            let Some(slot) = values.get_mut(usize::from(ordinal)) else {
                continue;
            };
            let ctx = Self::context(table, ordinal, pk_bytes);

            // Start from the FluxBIN plaintext bytes.
            let mut bytes = encode_value_bytes(slot)?;
            // 1. Encrypt (CT-030): the field bytes become a ciphertext envelope.
            if let Some(key_name) = &plan.encrypt_key {
                bytes = ecies_seal(self.ecies_key(key_name)?, &bytes, &ctx)?;
            }
            // 2. Sign (CT-033): append an Ed25519 signature over ctx ‖ bytes.
            if let Some(SignBy::Server) = plan.sign {
                let mut msg = ctx.clone();
                msg.extend_from_slice(&bytes);
                let sig = self.server_sign_key()?.sign(&msg);
                bytes.extend_from_slice(&sig);
            }
            if plan.has_executor() {
                *slot = RowValue::Bytes(bytes);
            }
        }
        Ok(())
    }

    /// Apply the read-path executors to `values` in place (CT-031/034):
    /// verify + strip a signature, then decrypt for an authorized caller.
    /// `authorized` is the §6 decision (server peers are always authorized).
    pub fn on_read_row(
        &self,
        table: TableId,
        values: &mut [RowValue],
        pk_bytes: &[u8],
        authorized: bool,
    ) -> Result<()> {
        let Some(ordinals) = self.tables.get(&table) else {
            return Ok(());
        };
        for &ordinal in ordinals {
            let plan = &self.columns[&(table, ordinal)];
            // Decryption is gated on authorization; a purely-signed column is
            // clear text and is always readable.
            if plan.encrypt_key.is_some() && !authorized {
                continue;
            }
            let Some(slot) = values.get_mut(usize::from(ordinal)) else {
                continue;
            };
            let RowValue::Bytes(stored) = slot else {
                continue; // never sealed/signed (mixed path)
            };
            let mut bytes = stored.clone();
            let ctx = Self::context(table, ordinal, pk_bytes);

            // 1. Verify + strip the signature (CT-034 — never drop the row).
            if let Some(SignBy::Server) = plan.sign {
                if bytes.len() < SIGNATURE_LEN {
                    self.read_errors.fetch_add(1, Ordering::Relaxed);
                    return Err(FluxumError::Storage(format!(
                        "signed field on table {} is shorter than the {SIGNATURE_LEN}-byte \
                         signature",
                        table.as_u32()
                    )));
                }
                let split = bytes.len() - SIGNATURE_LEN;
                let mut sig = [0u8; SIGNATURE_LEN];
                sig.copy_from_slice(&bytes[split..]);
                bytes.truncate(split);
                let mut msg = ctx.clone();
                msg.extend_from_slice(&bytes);
                if !self.server_sign_key()?.verify(&msg, &sig) {
                    self.verify_failures.fetch_add(1, Ordering::Relaxed);
                    // CT-034: record the failure, still expose the field.
                }
            }
            // 2. Decrypt (CT-031).
            if let Some(key_name) = &plan.encrypt_key {
                match ecies_open(self.ecies_key(key_name)?, &bytes, &ctx) {
                    Ok((plain, _active)) => bytes = plain,
                    Err(e) => {
                        self.read_errors.fetch_add(1, Ordering::Relaxed);
                        return Err(e);
                    }
                }
            }
            match decode_value_bytes(&bytes, plan.plain_ty) {
                Ok(value) => *slot = value,
                Err(e) => {
                    self.read_errors.fetch_add(1, Ordering::Relaxed);
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    fn ecies_key(&self, name: &str) -> Result<&EciesKey> {
        self.ecies_keys.get(name).ok_or_else(|| {
            FluxumError::Storage(format!("transform key `{name}` is not configured (CT-035)"))
        })
    }

    fn server_sign_key(&self) -> Result<&SignKey> {
        self.server_sign_key.as_ref().ok_or_else(|| {
            FluxumError::Storage("the server Ed25519 signing key is not configured (CT-035)".into())
        })
    }
}

/// The ordinal + schema of a column by name.
fn column_of<'s>(
    table: &'s TableSchema,
    column: &str,
) -> Option<(u16, &'s crate::schema::ColumnSchema)> {
    table
        .columns
        .iter()
        .enumerate()
        .find(|(_, c)| c.name == column)
        .map(|(i, c)| (u16::try_from(i).unwrap_or(u16::MAX), c))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    // End-to-end store write/read wiring is covered in
    // crates/fluxum-core/tests/field_crypto.rs; these unit tests cover the
    // build path, the authorization gate, signing, and the fast-skip branches.
    use super::*;
    use crate::config::{KeyScheme, TransformKey};
    use crate::schema::{ColumnSchema, TableAccess, TableSchema, VisibilityRule};

    static COLS: &[ColumnSchema] = &[
        ColumnSchema {
            name: "id",
            ty: FluxType::U64,
        },
        ColumnSchema {
            name: "secret",
            ty: FluxType::Str,
        },
    ];
    static T: TableSchema = TableSchema {
        name: "Secretive",
        columns: COLS,
        primary_key: &[0],
        auto_inc: None,
        access: TableAccess::Public,
        partition_by: None,
        unique: &[],
        indexes: &[],
        visibility: VisibilityRule::PublicAll,
    };

    fn one_ecies() -> HashMap<String, EciesKey> {
        HashMap::from([("k".to_owned(), EciesKey::new("k", [1u8; 32], vec![]))])
    }

    fn encrypt_engine() -> TransformEngine {
        TransformEngine::for_encrypted_test(
            TableId::of("Secretive"),
            vec![(1, &FluxType::Str, "k".to_owned())],
            one_ecies(),
        )
    }

    fn sign_engine() -> TransformEngine {
        TransformEngine::for_test(
            TableId::of("Secretive"),
            vec![(1, &FluxType::Str, None, true)],
            HashMap::new(),
            Some(SignKey::new("server", [9u8; 32])),
        )
    }

    #[test]
    fn build_over_an_empty_registry_is_none() {
        let schema = Schema::from_tables([&T]).unwrap();
        assert!(
            TransformEngine::build(&schema, &TransformsConfig::default())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn unauthorized_read_keeps_ciphertext() {
        let eng = encrypt_engine();
        let table = TableId::of("Secretive");
        let mut values = vec![RowValue::U64(1), RowValue::Str("top".to_owned())];
        eng.on_write_row(table, &mut values, b"pk").unwrap();
        assert!(matches!(values[1], RowValue::Bytes(_)), "sealed");

        let mut unauth = values.clone();
        eng.on_read_row(table, &mut unauth, b"pk", false).unwrap();
        assert!(matches!(unauth[1], RowValue::Bytes(_)), "still ciphertext");

        eng.on_read_row(table, &mut values, b"pk", true).unwrap();
        assert_eq!(values[1], RowValue::Str("top".to_owned()));
    }

    #[test]
    fn untouched_table_is_a_no_op() {
        let eng = encrypt_engine();
        let other = TableId::of("Elsewhere");
        assert!(!eng.touches(other));
        let mut values = vec![RowValue::U64(1), RowValue::Str("plain".to_owned())];
        eng.on_write_row(other, &mut values, b"pk").unwrap();
        assert_eq!(values[1], RowValue::Str("plain".to_owned()), "untouched");
    }

    #[test]
    fn signed_field_stays_clear_text_and_verifies() {
        let eng = sign_engine();
        let table = TableId::of("Secretive");
        let mut values = vec![RowValue::U64(1), RowValue::Str("public-vote".to_owned())];
        eng.on_write_row(table, &mut values, b"pk").unwrap();
        // Signed-only: the field stays in clear text (CT-033) — the FluxBIN of
        // "public-vote" is present, followed by a 64-byte signature.
        match &values[1] {
            RowValue::Bytes(b) => {
                assert!(
                    b.windows(11).any(|w| w == b"public-vote"),
                    "signed field is clear text"
                );
                assert!(b.len() > SIGNATURE_LEN);
            }
            other => panic!("{other:?}"),
        }
        // Read verifies, strips, and returns the plaintext; no failure counted.
        eng.on_read_row(table, &mut values, b"pk", true).unwrap();
        assert_eq!(values[1], RowValue::Str("public-vote".to_owned()));
        assert_eq!(eng.verify_failures(), 0);
    }

    #[test]
    fn a_tampered_signature_increments_the_metric_without_dropping() {
        let eng = sign_engine();
        let table = TableId::of("Secretive");
        let mut values = vec![RowValue::U64(1), RowValue::Str("vote".to_owned())];
        eng.on_write_row(table, &mut values, b"pk").unwrap();
        if let RowValue::Bytes(b) = &mut values[1] {
            let last = b.len() - 1;
            b[last] ^= 0x01; // corrupt the signature
        }
        // Read still returns the field (CT-034: never drop), metric increments.
        eng.on_read_row(table, &mut values, b"pk", true).unwrap();
        assert_eq!(values[1], RowValue::Str("vote".to_owned()));
        assert_eq!(eng.verify_failures(), 1);
    }

    #[test]
    fn config_keyring_rejects_dups_and_splits_schemes() {
        let dup = TransformsConfig {
            keys: vec![
                TransformKey {
                    id: "k".into(),
                    scheme: KeyScheme::X25519,
                    secret: "ab".repeat(32),
                    previous: vec![],
                },
                TransformKey {
                    id: "k".into(),
                    scheme: KeyScheme::X25519,
                    secret: "cd".repeat(32),
                    previous: vec![],
                },
            ],
        };
        assert!(dup.ecies_keys().is_err());

        let mixed = TransformsConfig {
            keys: vec![TransformKey {
                id: "server".into(),
                scheme: KeyScheme::Ed25519,
                secret: "ab".repeat(32),
                previous: vec![],
            }],
        };
        assert!(
            mixed.ecies_keys().unwrap().is_empty(),
            "ecies skips ed25519"
        );
        assert!(mixed.ed25519_keys().unwrap().contains_key("server"));
    }
}
