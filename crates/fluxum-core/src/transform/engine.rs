//! The column-transform execution engine (SPEC-017 §3 hooks, §5 crypto).
//!
//! Phase 1 registered the transform *descriptors* per `(table, column)`; this
//! engine turns them into runtime executors and applies them at the storage
//! boundary:
//!
//! - **on_write** ([`TransformEngine::on_write_row`]): runs after row
//!   validation and before storage, so the committed row — and therefore the
//!   commit log, cold pages, checkpoints, and replication stream — already
//!   holds ciphertext for every `#[encrypted]` column (CT-011/030). The
//!   plaintext value is FluxBIN-encoded, sealed with ECIES-X25519
//!   ([`super::crypto`]), and stored as `Bytes`; the AEAD associated data
//!   binds it to `(table, column, primary_key)` (CT-032).
//! - **on_read** ([`TransformEngine::on_read_row`]): decrypts `#[encrypted]`
//!   columns back to their plaintext value **only for an authorized caller**
//!   (CT-031). Reducers run as server peers (AUTH-062) and are always
//!   authorized; client-facing reads leave the ciphertext in place until the
//!   phase-4 column-grant resolution authorizes them.
//!
//! This slice implements the `#[encrypted]` executor and its write/read
//! wiring; `#[signed]` verification and `#[masked]`/grant resolution land with
//! the field-security follow-up.

use std::collections::HashMap;

use crate::config::TransformsConfig;
use crate::error::{FluxumError, Result};
use crate::schema::{Schema, TableSchema};
use crate::store::row::{decode_value_bytes, encode_value_bytes};
use crate::store::{RowValue, TableId};
use crate::transform::crypto::{EciesKey, ecies_open, ecies_seal};
use crate::transform::{TransformDescriptor, registered_column_transforms};

/// One column's compiled crypto plan (this slice: encryption only).
struct ColumnPlan {
    /// Declared plaintext [`crate::schema::FluxType`] — the decrypt target.
    plain_ty: &'static crate::schema::FluxType,
    /// The named ECIES key this column encrypts to (`#[encrypted]`), if any.
    encrypt_key: Option<String>,
}

/// The per-shard transform executor: the resolved ECIES keyring plus the
/// compiled per-column plans (SPEC-017 §5).
#[derive(Default)]
pub struct TransformEngine {
    keys: HashMap<String, EciesKey>,
    /// `(table_id, ordinal)` → plan, for every column carrying a transform.
    columns: HashMap<(TableId, u16), ColumnPlan>,
    /// Tables that have at least one transformed column, for a fast skip.
    tables: HashMap<TableId, Vec<u16>>,
}

impl std::fmt::Debug for TransformEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransformEngine")
            .field("keys", &self.keys.keys().collect::<Vec<_>>())
            .field("columns", &self.columns.len())
            .finish()
    }
}

impl TransformEngine {
    /// Build the engine from the assembled `schema`, the link-time transform
    /// registry, and `config` key material (CT-035). Fails if an `#[encrypted]`
    /// attribute names a key that is absent or not an X25519 key.
    pub fn build(schema: &Schema, config: &TransformsConfig) -> Result<Option<Self>> {
        let keys = config.ecies_keys()?;
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
                && !keys.contains_key(key_name)
            {
                return Err(FluxumError::Config(format!(
                    "table `{}` column `{}`: #[encrypted] names key `{key_name}`, which is not a \
                     configured X25519 transform key (CT-035)",
                    def.table, def.column
                )));
            }
            if encrypt_key.is_none() {
                continue; // no executor this slice (normalize/sign/mask later)
            }
            let table_id = TableId::of(table.name);
            tables.entry(table_id).or_default().push(ordinal);
            columns.insert(
                (table_id, ordinal),
                ColumnPlan {
                    plain_ty: &column.ty,
                    encrypt_key,
                },
            );
        }

        if columns.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self {
            keys,
            columns,
            tables,
        }))
    }

    /// Build an engine with an explicit `#[encrypted]` plan over one table —
    /// the test seam (`build` is the production path that reads the link-time
    /// registry). Each `(ordinal, plain_ty, key_name)` names an encrypted
    /// column; `keys` supplies the ECIES key material.
    #[doc(hidden)]
    pub fn for_encrypted_test(
        table: TableId,
        columns: Vec<(u16, &'static crate::schema::FluxType, String)>,
        keys: HashMap<String, EciesKey>,
    ) -> Self {
        let mut plans = HashMap::new();
        let mut tables: HashMap<TableId, Vec<u16>> = HashMap::new();
        for (ordinal, plain_ty, key_name) in columns {
            tables.entry(table).or_default().push(ordinal);
            plans.insert(
                (table, ordinal),
                ColumnPlan {
                    plain_ty,
                    encrypt_key: Some(key_name),
                },
            );
        }
        Self {
            keys,
            columns: plans,
            tables,
        }
    }

    /// Whether `table` has any transformed column (fast-path skip).
    pub fn touches(&self, table: TableId) -> bool {
        self.tables.contains_key(&table)
    }

    /// The AEAD associated data binding a field to its position (CT-032):
    /// `table_id ‖ ordinal ‖ primary_key`.
    fn aad(table: TableId, ordinal: u16, pk_bytes: &[u8]) -> Vec<u8> {
        let mut aad = Vec::with_capacity(6 + pk_bytes.len());
        aad.extend_from_slice(&table.as_u32().to_le_bytes());
        aad.extend_from_slice(&ordinal.to_le_bytes());
        aad.extend_from_slice(pk_bytes);
        aad
    }

    /// Encrypt every `#[encrypted]` column of `values` in place (CT-030/011),
    /// binding each to `(table, column, pk_bytes)`. Called after validation,
    /// before storage — the stored row carries ciphertext.
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
            let Some(key_name) = &plan.encrypt_key else {
                continue;
            };
            let key = self.key(key_name)?;
            let Some(slot) = values.get_mut(usize::from(ordinal)) else {
                continue;
            };
            // Idempotence guard: never double-encrypt an existing envelope
            // (a row read as plaintext and written back re-encrypts once).
            let plaintext = encode_value_bytes(slot)?;
            let envelope = ecies_seal(key, &plaintext, &Self::aad(table, ordinal, pk_bytes))?;
            *slot = RowValue::Bytes(envelope);
        }
        Ok(())
    }

    /// Decrypt every `#[encrypted]` column of an authorized read in place
    /// (CT-031). `authorized` is the §6 decision — server peers are always
    /// authorized; unauthorized callers keep the ciphertext (phase-4 masking).
    pub fn on_read_row(
        &self,
        table: TableId,
        values: &mut [RowValue],
        pk_bytes: &[u8],
        authorized: bool,
    ) -> Result<()> {
        if !authorized {
            return Ok(());
        }
        let Some(ordinals) = self.tables.get(&table) else {
            return Ok(());
        };
        for &ordinal in ordinals {
            let plan = &self.columns[&(table, ordinal)];
            let Some(key_name) = &plan.encrypt_key else {
                continue;
            };
            let key = self.key(key_name)?;
            let Some(slot) = values.get_mut(usize::from(ordinal)) else {
                continue;
            };
            let RowValue::Bytes(envelope) = slot else {
                continue; // already plaintext (never stored, or mixed path)
            };
            let (plaintext, _active) =
                ecies_open(key, envelope, &Self::aad(table, ordinal, pk_bytes))?;
            *slot = decode_value_bytes(&plaintext, plan.plain_ty)?;
        }
        Ok(())
    }

    fn key(&self, name: &str) -> Result<&EciesKey> {
        self.keys.get(name).ok_or_else(|| {
            FluxumError::Storage(format!("transform key `{name}` is not configured (CT-035)"))
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
    // build path, the authorization gate, and the fast-skip branches.
    use super::*;
    use crate::config::{KeyScheme, TransformKey};
    use crate::schema::{ColumnSchema, TableAccess, TableSchema, VisibilityRule};

    static COLS: &[ColumnSchema] = &[
        ColumnSchema {
            name: "id",
            ty: crate::schema::FluxType::U64,
        },
        ColumnSchema {
            name: "secret",
            ty: crate::schema::FluxType::Str,
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

    fn one_key() -> HashMap<String, EciesKey> {
        HashMap::from([("k".to_owned(), EciesKey::new("k", [1u8; 32], vec![]))])
    }

    fn test_engine() -> TransformEngine {
        TransformEngine::for_encrypted_test(
            TableId::of("Secretive"),
            vec![(1, &crate::schema::FluxType::Str, "k".to_owned())],
            one_key(),
        )
    }

    #[test]
    fn build_over_an_empty_registry_is_none() {
        let schema = Schema::from_tables([&T]).unwrap();
        // No #[encrypted] table is registered in this lib-test binary.
        assert!(
            TransformEngine::build(&schema, &TransformsConfig::default())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn unauthorized_read_keeps_ciphertext() {
        let eng = test_engine();
        let table = TableId::of("Secretive");
        let mut values = vec![RowValue::U64(1), RowValue::Str("top".to_owned())];
        eng.on_write_row(table, &mut values, b"pk").unwrap();
        assert!(matches!(values[1], RowValue::Bytes(_)), "sealed");

        // Unauthorized read leaves the ciphertext in place (phase-4 masking).
        let mut unauth = values.clone();
        eng.on_read_row(table, &mut unauth, b"pk", false).unwrap();
        assert!(matches!(unauth[1], RowValue::Bytes(_)), "still ciphertext");

        // Authorized read decrypts.
        eng.on_read_row(table, &mut values, b"pk", true).unwrap();
        assert_eq!(values[1], RowValue::Str("top".to_owned()));
    }

    #[test]
    fn untouched_table_is_a_no_op() {
        let eng = test_engine();
        let other = TableId::of("Elsewhere");
        assert!(!eng.touches(other));
        let mut values = vec![RowValue::U64(1), RowValue::Str("plain".to_owned())];
        eng.on_write_row(other, &mut values, b"pk").unwrap();
        assert_eq!(values[1], RowValue::Str("plain".to_owned()), "untouched");
        eng.on_read_row(other, &mut values, b"pk", true).unwrap();
        assert_eq!(values[1], RowValue::Str("plain".to_owned()));
    }

    #[test]
    fn read_of_a_non_bytes_slot_is_skipped() {
        // A column that was never sealed (plaintext) is left as-is on read.
        let eng = test_engine();
        let table = TableId::of("Secretive");
        let mut values = vec![RowValue::U64(1), RowValue::Str("plain".to_owned())];
        eng.on_read_row(table, &mut values, b"pk", true).unwrap();
        assert_eq!(values[1], RowValue::Str("plain".to_owned()));
    }

    #[test]
    fn config_keyring_rejects_dups_and_skips_ed25519() {
        // A duplicate x25519 id is rejected.
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

        // An ed25519 signing key is skipped by the ECIES key set.
        let mixed = TransformsConfig {
            keys: vec![TransformKey {
                id: "sig".into(),
                scheme: KeyScheme::Ed25519,
                secret: "ab".repeat(32),
                previous: vec![],
            }],
        };
        assert!(mixed.ecies_keys().unwrap().is_empty());
    }
}
