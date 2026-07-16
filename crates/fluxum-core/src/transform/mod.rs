//! Column transforms (SPEC-017): the DTO-in-schema surface.
//!
//! Phase-1 scope: the deterministic value **normalizers** (CT-021/022/023 —
//! money to exact fixed-point, timestamps to canonical UTC, strings to one
//! Unicode spelling), the [`ColumnTransform`] trait + [`TransformCtx`]
//! (CT-010), the self-describing [`TransformDescriptor`]s every attribute
//! parses into, the [`ColumnTransformDef`] link-time registry keyed by
//! `(table, column)` (CT-050 — kept off `ColumnSchema` so the metadata costs
//! no change at its construction sites), and the [`validate_registered`]
//! startup backstop (CT-051). The crypto **executors** (`#[encrypted]`/
//! `#[signed]`) and read-path masking/grants run in phases 3–4; here they are
//! declared, validated, and reflected in `/schema`, not yet applied.

pub mod normalize;

pub use normalize::{datetime_utc, money_from_minor_units, money_from_str, normalize_string};

use crate::error::Result;
use crate::store::RowValue;
use crate::types::Identity;

/// The caller's authorization posture on the read path (CT-010): whether the
/// raw value may be revealed, or a mask must be substituted.
#[derive(Debug, Clone, Copy)]
pub struct TransformCtx<'a> {
    /// The calling identity.
    pub identity: &'a Identity,
    /// Whether the caller is authorized to see the raw (post-inverse) value.
    pub authorized: bool,
    /// Whether the caller is a privileged server peer (always authorized).
    pub is_server_peer: bool,
}

/// One column transform (CT-010): applied on the write path
/// (normalize/encrypt/sign) and reversed or authorized on the read path
/// (decrypt/verify/mask). Phase 1 defines the shape and the deterministic
/// normalizers; the crypto and field-security executors land in phases 3–4.
pub trait ColumnTransform {
    /// Transform a value on the write path (before storage).
    fn on_write(&self, value: RowValue) -> Result<RowValue>;

    /// Reverse or authorize a value on the read path for `ctx`.
    fn on_read(&self, value: RowValue, ctx: &TransformCtx<'_>) -> Result<RowValue>;

    /// The self-describing descriptor for `/schema` and validation.
    fn descriptor(&self) -> TransformDescriptor;
}

/// One column's transform pipeline, registered at link time by
/// `#[fluxum::table]` for every column that declares a transform attribute
/// (CT-050). Keyed by `(table, column)` and resolved against the assembled
/// schema — kept off [`crate::schema::ColumnSchema`] so the additive metadata
/// costs no change at its ~250 construction sites.
pub struct ColumnTransformDef {
    /// The `#[fluxum::table]` struct name.
    pub table: &'static str,
    /// The column (field) name.
    pub column: &'static str,
    /// The transforms in application order (write: top-to-bottom; read: the
    /// reverse).
    pub transforms: &'static [TransformDescriptor],
}

inventory::collect!(ColumnTransformDef);

/// Every column-transform pipeline registered in this binary (linker order).
pub fn registered_column_transforms() -> impl Iterator<Item = &'static ColumnTransformDef> {
    inventory::iter::<ColumnTransformDef>()
}

/// The registered transform pipeline of `(table, column)`, if any.
pub fn column_transforms(table: &str, column: &str) -> Option<&'static [TransformDescriptor]> {
    registered_column_transforms()
        .find(|def| def.table == table && def.column == column)
        .map(|def| def.transforms)
}

/// Startup validation of every registered [`ColumnTransformDef`] against an
/// assembled schema (CT-051) — the runtime backstop behind the proc-macro's
/// compile-time rejections, covering hand-registered defs and cross-crate
/// properties. Called by `Schema::from_tables`; a failure must abort startup.
///
/// Defs whose table is not part of `schema` are skipped: the registry is
/// process-global while a schema may be assembled from an explicit subset
/// (tests, embedders).
pub(crate) fn validate_registered(schema: &crate::schema::Schema) -> Result<()> {
    use crate::error::FluxumError;
    use crate::schema::{FluxType, IndexSchema};

    let fail = |def: &ColumnTransformDef, reason: &str| {
        Err(FluxumError::Schema(format!(
            "table `{}`, column `{}`: {reason}",
            def.table, def.column
        )))
    };

    for def in registered_column_transforms() {
        let Some(table) = schema.table(def.table) else {
            continue;
        };
        let Some((ordinal, column)) = table
            .columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.name == def.column)
        else {
            return fail(def, "transform declared on an unknown column (CT-051)");
        };
        #[allow(clippy::cast_possible_truncation)] // DM-001 caps columns at u16
        let ordinal = ordinal as u16;

        // Key/index columns an #[encrypted] transform may never touch (CT-013).
        let mut protected: Vec<u16> = table.primary_key.to_vec();
        protected.extend(table.unique.iter().flat_map(|set| set.iter().copied()));
        protected.extend(table.partition_by);
        for index in table.indexes {
            match index {
                IndexSchema::BTree { columns } | IndexSchema::Spatial { columns, .. } => {
                    protected.extend(columns.iter().copied());
                }
                // An #[encrypted] full-text column makes no sense — ciphertext
                // is not analyzable (FTS-002); treat it as protected.
                IndexSchema::FullText { column, .. } => protected.push(*column),
            }
        }

        let mut encrypted = 0usize;
        let mut signed = 0usize;
        for transform in def.transforms {
            match transform {
                TransformDescriptor::NormalizeMoney { .. } => {
                    if !matches!(column.ty, FluxType::Decimal) {
                        return fail(
                            def,
                            "#[normalize(money)] requires a `Decimal` column (CT-021)",
                        );
                    }
                }
                TransformDescriptor::NormalizeDatetime => {
                    if !matches!(column.ty, FluxType::Timestamp) {
                        return fail(
                            def,
                            "#[normalize(datetime)] requires a `Timestamp` column (CT-022)",
                        );
                    }
                }
                TransformDescriptor::NormalizeString { .. } => {
                    if !matches!(column.ty, FluxType::Str) {
                        return fail(
                            def,
                            "#[normalize(string)] requires a `String` column (CT-023)",
                        );
                    }
                }
                TransformDescriptor::Encrypted { key, .. } => {
                    encrypted += 1;
                    if key.is_empty() {
                        return fail(def, "#[encrypted] requires a non-empty key name (CT-035)");
                    }
                    if protected.contains(&ordinal) {
                        return fail(
                            def,
                            "#[encrypted] cannot apply to a primary-key, unique, index, \
                             partition, or spatial column (CT-013)",
                        );
                    }
                }
                TransformDescriptor::Signed { by, .. } => {
                    signed += 1;
                    if let SignedBy::IdentityColumn(source) = by {
                        let source_ty = table.column(*source).map(|c| &c.ty);
                        if !matches!(source_ty, Some(FluxType::Identity)) {
                            return fail(
                                def,
                                "#[signed(by = <column>)] must reference an `Identity` column \
                                 (CT-033)",
                            );
                        }
                    }
                }
                TransformDescriptor::Masked { strategy } => {
                    if matches!(strategy, MaskStrategy::Ciphertext)
                        && !def
                            .transforms
                            .iter()
                            .any(|t| matches!(t, TransformDescriptor::Encrypted { .. }))
                    {
                        return fail(
                            def,
                            "#[masked(ciphertext)] requires #[encrypted] on the same column \
                             (CT-041)",
                        );
                    }
                }
                TransformDescriptor::Grant { .. } => {}
            }
        }
        if encrypted > 1 || signed > 1 {
            return fail(
                def,
                "at most one #[encrypted] and one #[signed] per column (CT-002)",
            );
        }
    }
    Ok(())
}

/// Unicode form for `#[normalize(string, form = …)]` (CT-023).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringForm {
    /// Canonical composition (NFC) — the default.
    Nfc,
    /// Compatibility composition (NFKC).
    Nfkc,
}

/// Case handling for `#[normalize(string, case = …)]` (CT-023).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseFold {
    /// Leave case unchanged (the default).
    None,
    /// Unicode case-fold (a `citext`-style case-insensitive key).
    Fold,
    /// Lowercase.
    Lower,
}

/// AEAD encryption scheme for `#[encrypted(scheme, …)]` (CT-030).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CryptoScheme {
    /// ECIES over X25519 + HKDF-SHA-256 + XChaCha20-Poly1305 (CT-030).
    Ecies,
}

/// Signature scheme for `#[signed(scheme, …)]` (CT-033).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignScheme {
    /// Ed25519 (CT-033).
    Ed25519,
}

/// The signing authority for `#[signed(…, by = …)]` (CT-033).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignedBy {
    /// The server key (`by = server`).
    Server,
    /// An `Identity` column, by ordinal (`by = <column>`).
    IdentityColumn(u16),
}

/// Unauthorized-read masking strategy for `#[masked(strategy)]` (CT-041).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaskStrategy {
    /// Replace with null (the default).
    Null,
    /// Fixed redaction marker.
    Redact,
    /// Expose the ciphertext envelope (encrypted columns only).
    Ciphertext,
    /// SHA-256 of the value.
    Hash,
}

/// Per-column read authorization for `#[column_grant(select = …)]` (CT-040).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantScope {
    /// Any authenticated identity.
    Public,
    /// Only the row's owner (row-level `owner_only` identity).
    Owner,
    /// Only privileged server peers.
    ServerPeer,
    /// A named role (RBAC, AUTH-073).
    Role(&'static str),
}

/// A self-describing descriptor of one column transform (CT-050), surfaced in
/// the `/schema` JSON — key **names** only, never secret material — and used by
/// `ServerBuilder::build()` validation (CT-051). Copy + `'static`, so a
/// `&'static [TransformDescriptor]` rides the link-time registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransformDescriptor {
    /// `#[normalize(money, scale = N)]` — exact fixed-point money (CT-021).
    NormalizeMoney {
        /// Fractional digits of the stored `Decimal`.
        scale: u8,
        /// Optional ISO-4217 currency metadata.
        currency: Option<&'static str>,
    },
    /// `#[normalize(datetime)]` — canonical UTC microseconds (CT-022).
    NormalizeDatetime,
    /// `#[normalize(string, …)]` — Unicode canonicalization (CT-023).
    NormalizeString {
        /// Unicode normalization form.
        form: StringForm,
        /// Case handling.
        case: CaseFold,
        /// Whether to trim surrounding whitespace.
        trim: bool,
    },
    /// `#[encrypted(scheme, key = "NAME")]` — AEAD at rest (exec: phase 3, CT-030).
    Encrypted {
        /// The AEAD scheme.
        scheme: CryptoScheme,
        /// The named server key (name only, never the material).
        key: &'static str,
    },
    /// `#[signed(scheme, by = SOURCE)]` — signed field (exec: phase 3, CT-033).
    Signed {
        /// The signature scheme.
        scheme: SignScheme,
        /// The signing authority.
        by: SignedBy,
    },
    /// `#[masked(strategy)]` — unauthorized-read masking (exec: phase 4, CT-041).
    Masked {
        /// The masking strategy.
        strategy: MaskStrategy,
    },
    /// `#[column_grant(select = …)]` — per-column read grant (exec: phase 4, CT-040).
    Grant {
        /// The authorized scope.
        select: GrantScope,
    },
}

impl TransformDescriptor {
    /// Whether this transform runs on the **write** path (normalization and,
    /// later, encryption/signing) versus the read/authorization path.
    pub const fn is_write_transform(&self) -> bool {
        matches!(
            self,
            Self::NormalizeMoney { .. }
                | Self::NormalizeDatetime
                | Self::NormalizeString { .. }
                | Self::Encrypted { .. }
                | Self::Signed { .. }
        )
    }

    /// A stable kind tag for the `/schema` JSON (SDK codegen key).
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::NormalizeMoney { .. } => "normalize.money",
            Self::NormalizeDatetime => "normalize.datetime",
            Self::NormalizeString { .. } => "normalize.string",
            Self::Encrypted { .. } => "encrypted",
            Self::Signed { .. } => "signed",
            Self::Masked { .. } => "masked",
            Self::Grant { .. } => "column_grant",
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::schema::{ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule};

    // One shared column layout: id (pk), s: Str, d: Decimal, t: Timestamp,
    // who: Identity — every CT-051 branch is expressible against it.
    static TV_COLS: &[ColumnSchema] = &[
        ColumnSchema {
            name: "id",
            ty: FluxType::U64,
        },
        ColumnSchema {
            name: "s",
            ty: FluxType::Str,
        },
        ColumnSchema {
            name: "d",
            ty: FluxType::Decimal,
        },
        ColumnSchema {
            name: "t",
            ty: FluxType::Timestamp,
        },
        ColumnSchema {
            name: "who",
            ty: FluxType::Identity,
        },
    ];

    const fn tv_table(name: &'static str) -> TableSchema {
        TableSchema {
            name,
            columns: TV_COLS,
            primary_key: &[0],
            auto_inc: None,
            access: TableAccess::Public,
            partition_by: None,
            unique: &[],
            indexes: &[],
            visibility: VisibilityRule::PublicAll,
        }
    }

    /// Register `def` for a uniquely named table and assert `from_tables`
    /// rejects it with `needle`. Each case uses its own table name, so the
    /// process-global registry never leaks across cases (the skip rule).
    macro_rules! reject_case {
        ($test:ident, $table:ident = $name:literal, $column:literal,
         [$($transform:expr),+ $(,)?], $needle:literal) => {
            static $table: TableSchema = tv_table($name);
            crate::schema::inventory::submit! {
                ColumnTransformDef {
                    table: $name,
                    column: $column,
                    transforms: &[$($transform),+],
                }
            }
            #[test]
            fn $test() {
                let err = match Schema::from_tables([&$table]) {
                    Err(e) => e.to_string(),
                    Ok(_) => panic!("expected {} to fail validation", $name),
                };
                assert!(err.contains($needle), "{err}");
            }
        };
    }

    reject_case!(
        rejects_unknown_column,
        TV_NOCOL = "TvNoCol",
        "missing",
        [TransformDescriptor::NormalizeDatetime],
        "CT-051"
    );
    reject_case!(
        rejects_money_on_non_decimal,
        TV_MONEY = "TvMoney",
        "s",
        [TransformDescriptor::NormalizeMoney {
            scale: 2,
            currency: None,
        }],
        "CT-021"
    );
    reject_case!(
        rejects_datetime_on_non_timestamp,
        TV_DT = "TvDatetime",
        "s",
        [TransformDescriptor::NormalizeDatetime],
        "CT-022"
    );
    reject_case!(
        rejects_string_normalize_on_non_string,
        TV_STR = "TvString",
        "d",
        [TransformDescriptor::NormalizeString {
            form: StringForm::Nfc,
            case: CaseFold::None,
            trim: false,
        }],
        "CT-023"
    );
    reject_case!(
        rejects_empty_key_name,
        TV_KEY = "TvKey",
        "s",
        [TransformDescriptor::Encrypted {
            scheme: CryptoScheme::Ecies,
            key: "",
        }],
        "CT-035"
    );
    reject_case!(
        rejects_encrypted_on_primary_key,
        TV_PK = "TvPk",
        "id",
        [TransformDescriptor::Encrypted {
            scheme: CryptoScheme::Ecies,
            key: "k",
        }],
        "CT-013"
    );
    reject_case!(
        rejects_signed_by_non_identity,
        TV_SIGN = "TvSign",
        "d",
        [TransformDescriptor::Signed {
            scheme: SignScheme::Ed25519,
            by: SignedBy::IdentityColumn(1), // `s` is Str, not Identity
        }],
        "CT-033"
    );
    reject_case!(
        rejects_ciphertext_mask_without_encrypted,
        TV_MASK = "TvMask",
        "s",
        [TransformDescriptor::Masked {
            strategy: MaskStrategy::Ciphertext,
        }],
        "CT-041"
    );
    reject_case!(
        rejects_duplicate_encrypted,
        TV_DUP = "TvDup",
        "s",
        [
            TransformDescriptor::Encrypted {
                scheme: CryptoScheme::Ecies,
                key: "a",
            },
            TransformDescriptor::Encrypted {
                scheme: CryptoScheme::Ecies,
                key: "b",
            },
        ],
        "CT-002"
    );

    // A fully valid pipeline on one table: every write+read descriptor.
    static TV_OK: TableSchema = tv_table("TvOk");
    crate::schema::inventory::submit! {
        ColumnTransformDef {
            table: "TvOk",
            column: "d",
            transforms: &[TransformDescriptor::NormalizeMoney {
                scale: 2,
                currency: Some("EUR"),
            }],
        }
    }
    crate::schema::inventory::submit! {
        ColumnTransformDef {
            table: "TvOk",
            column: "s",
            transforms: &[
                TransformDescriptor::NormalizeString {
                    form: StringForm::Nfkc,
                    case: CaseFold::Lower,
                    trim: true,
                },
                TransformDescriptor::Encrypted {
                    scheme: CryptoScheme::Ecies,
                    key: "k",
                },
                TransformDescriptor::Signed {
                    scheme: SignScheme::Ed25519,
                    by: SignedBy::IdentityColumn(4), // `who`
                },
                TransformDescriptor::Masked {
                    strategy: MaskStrategy::Ciphertext,
                },
                TransformDescriptor::Grant {
                    select: GrantScope::Role("auditor"),
                },
            ],
        }
    }

    #[test]
    fn accepts_a_valid_pipeline_and_resolves_lookups() {
        let schema = Schema::from_tables([&TV_OK]).unwrap();
        assert!(schema.table("TvOk").is_some());
        assert_eq!(column_transforms("TvOk", "s").unwrap().len(), 5);
        assert_eq!(column_transforms("TvOk", "d").unwrap().len(), 1);
        assert!(column_transforms("TvOk", "id").is_none());
        assert!(column_transforms("NoSuchTable", "s").is_none());
    }

    #[test]
    fn descriptor_kinds_and_write_classification_are_stable() {
        let money = TransformDescriptor::NormalizeMoney {
            scale: 2,
            currency: None,
        };
        let string = TransformDescriptor::NormalizeString {
            form: StringForm::Nfc,
            case: CaseFold::Fold,
            trim: false,
        };
        let encrypted = TransformDescriptor::Encrypted {
            scheme: CryptoScheme::Ecies,
            key: "k",
        };
        let signed = TransformDescriptor::Signed {
            scheme: SignScheme::Ed25519,
            by: SignedBy::Server,
        };
        let masked = TransformDescriptor::Masked {
            strategy: MaskStrategy::Null,
        };
        let grant = TransformDescriptor::Grant {
            select: GrantScope::Owner,
        };
        let pairs = [
            (money, "normalize.money", true),
            (
                TransformDescriptor::NormalizeDatetime,
                "normalize.datetime",
                true,
            ),
            (string, "normalize.string", true),
            (encrypted, "encrypted", true),
            (signed, "signed", true),
            (masked, "masked", false),
            (grant, "column_grant", false),
        ];
        for (descriptor, kind, is_write) in pairs {
            assert_eq!(descriptor.kind(), kind);
            assert_eq!(descriptor.is_write_transform(), is_write, "{kind}");
        }
    }
}
