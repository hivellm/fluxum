//! Column-level security: per-column read authorization + dynamic masking
//! (SPEC-017 §6, CT-040/041/042) — the read-side half of field-level
//! security. Resolves each table's `#[column_grant]`/`#[masked]`
//! declarations into [`TablePolicy`]s, decides `authorized` per (caller,
//! column, row), and substitutes the masked value on every read surface
//! when unauthorized. Server peers bypass all grants (AUTH-062); columns
//! without a grant default to `public` (additive, CT-040).

use std::collections::HashMap;

use sha2::{Digest, Sha256};

use crate::schema::{FluxType, Schema, TableSchema, VisibilityRule};
use crate::store::{Row, RowValue, TableId};
use crate::types::Identity;

use super::{GrantScope, MaskStrategy, TransformDescriptor, registered_column_transforms};

/// One column's resolved read policy.
#[derive(Debug, Clone, Copy)]
pub struct ColumnPolicy {
    /// The protected column.
    pub ordinal: u16,
    /// Who may read the raw value (CT-040).
    pub grant: GrantScope,
    /// What an unauthorized caller receives (CT-041; `Redact` when the
    /// column declares no `#[masked]`).
    pub mask: MaskStrategy,
    /// The column's declared (logical) type — masked values must inhabit it.
    pub ty: &'static FluxType,
    /// Whether the column is `#[encrypted]` (the `Ciphertext` strategy is
    /// only meaningful then).
    pub encrypted: bool,
}

/// One table's resolved column policies.
#[derive(Debug, Clone, Default)]
pub struct TablePolicy {
    /// Columns with a non-`public` grant, ordinal-ascending.
    pub columns: Vec<ColumnPolicy>,
    /// The `#[visibility(owner_only(...))]` column, if any — what the
    /// `owner` grant compares against (CT-040).
    pub owner: Option<u16>,
}

/// Resolve the column policies of every table in `schema` (CT-040/041)
/// from the link-time transform registry. Tables and columns without a
/// non-public grant carry no policy (zero read-path cost).
pub fn resolve_policies(schema: &Schema) -> HashMap<TableId, TablePolicy> {
    let mut out: HashMap<TableId, TablePolicy> = HashMap::new();
    for def in registered_column_transforms() {
        let Some(table) = schema.table(def.table) else {
            continue;
        };
        let Some(ordinal) = column_ordinal(table, def.column) else {
            continue;
        };
        let mut grant = None;
        let mut mask = MaskStrategy::Redact;
        let mut encrypted = false;
        for descriptor in def.transforms {
            match descriptor {
                TransformDescriptor::Grant { select } => grant = Some(*select),
                TransformDescriptor::Masked { strategy } => mask = *strategy,
                TransformDescriptor::Encrypted { .. } => encrypted = true,
                _ => {}
            }
        }
        // No grant (or an explicitly public one) = always authorized —
        // masking never applies (CT-040 default).
        let Some(grant) = grant else { continue };
        if grant == GrantScope::Public {
            continue;
        }
        let policy = out.entry(TableId::of(table.name)).or_default();
        policy.owner = match table.visibility {
            VisibilityRule::OwnerOnly { owner } => Some(owner),
            _ => None,
        };
        policy.columns.push(ColumnPolicy {
            ordinal,
            grant,
            mask,
            ty: &table.columns[usize::from(ordinal)].ty,
            encrypted,
        });
    }
    for policy in out.values_mut() {
        policy.columns.sort_by_key(|c| c.ordinal);
    }
    out
}

/// Whether any table in `schema` named `table` carries a non-public grant —
/// what makes its plans caller-scoped (each viewer's masking differs).
pub fn has_column_grants(table: &TableSchema) -> bool {
    registered_column_transforms()
        .filter(|def| def.table == table.name)
        .any(|def| {
            def.transforms.iter().any(|t| {
                matches!(t, TransformDescriptor::Grant { select } if *select != GrantScope::Public)
            })
        })
}

/// The §6 per-column authorization decision (CT-040). `row` supplies the
/// owner column for the `owner` grant; callers resolve server peers BEFORE
/// this (a server-peer read never projects at all).
pub fn authorized(
    policy: &ColumnPolicy,
    owner: Option<u16>,
    viewer: &Identity,
    roles: &[String],
    row: &Row,
) -> bool {
    match &policy.grant {
        GrantScope::Public => true,
        GrantScope::ServerPeer => false,
        GrantScope::Role(role) => roles.iter().any(|r| r == role),
        GrantScope::Owner => owner.is_some_and(|ordinal| {
            row.value(ordinal) == Some(&RowValue::Identity(*viewer))
        }),
    }
}

/// The masked substitute for one unauthorized column (CT-041). `original`
/// is the STORED value (the sealed envelope for `#[encrypted]` columns);
/// `current` is the value after read-path decryption. Every strategy yields
/// a value inhabiting the column's declared type, falling back to `Redact`
/// where it cannot (e.g. `hash` over a numeric column).
pub fn mask_value(policy: &ColumnPolicy, original: &RowValue, current: &RowValue) -> RowValue {
    match policy.mask {
        MaskStrategy::Null => match policy.ty {
            FluxType::Option(_) => RowValue::Optional(None),
            _ => redacted(policy.ty),
        },
        MaskStrategy::Redact => redacted(policy.ty),
        // The envelope IS the stored bytes; leaking it reveals nothing the
        // storage layer doesn't already hold (CT-041). Rendered to inhabit
        // the column's declared type (hex on `Str` columns) so every typed
        // decode path stays valid.
        MaskStrategy::Ciphertext if policy.encrypted => match (policy.ty, original) {
            (FluxType::Bytes, RowValue::Bytes(_)) => original.clone(),
            (FluxType::Str, RowValue::Bytes(sealed)) => {
                RowValue::Str(sealed.iter().map(|b| format!("{b:02x}")).collect())
            }
            _ => redacted(policy.ty),
        },
        MaskStrategy::Ciphertext => redacted(policy.ty),
        MaskStrategy::Hash => {
            let bytes = match current {
                RowValue::Str(s) => s.as_bytes().to_vec(),
                RowValue::Bytes(b) => b.clone(),
                _ => return redacted(policy.ty),
            };
            let digest = Sha256::digest(&bytes);
            match policy.ty {
                FluxType::Str => {
                    RowValue::Str(digest.iter().map(|b| format!("{b:02x}")).collect())
                }
                FluxType::Bytes => RowValue::Bytes(digest.to_vec()),
                _ => redacted(policy.ty),
            }
        }
    }
}

/// The zero/empty value of a column type (the `redact` strategy, CT-041).
fn redacted(ty: &FluxType) -> RowValue {
    match ty {
        FluxType::Bool => RowValue::Bool(false),
        FluxType::I8 => RowValue::I8(0),
        FluxType::I16 => RowValue::I16(0),
        FluxType::I32 => RowValue::I32(0),
        FluxType::I64 => RowValue::I64(0),
        FluxType::U8 => RowValue::U8(0),
        FluxType::U16 => RowValue::U16(0),
        FluxType::U32 => RowValue::U32(0),
        FluxType::U64 => RowValue::U64(0),
        FluxType::F32 => RowValue::F32(0.0),
        FluxType::F64 => RowValue::F64(0.0),
        FluxType::Str => RowValue::Str(String::new()),
        FluxType::Bytes | FluxType::CrdtText => RowValue::Bytes(Vec::new()),
        FluxType::Identity => RowValue::Identity(Identity::from_bytes([0; 32])),
        FluxType::ConnectionId => {
            RowValue::ConnectionId(crate::types::ConnectionId::new(0))
        }
        FluxType::EntityId => RowValue::EntityId(crate::types::EntityId::new(0)),
        FluxType::Timestamp => RowValue::Timestamp(crate::types::Timestamp::from_micros(0)),
        FluxType::Decimal => RowValue::Decimal(crate::types::Decimal::from_parts(0, 0)),
        FluxType::Blob => RowValue::Blob(crate::types::BlobRef::from_bytes([0; 32])),
        FluxType::Option(_) => RowValue::Optional(None),
        FluxType::List(_) => RowValue::List(Vec::new()),
        FluxType::Enum(_) => RowValue::Enum {
            tag: 0,
            payload: Vec::new(),
        },
        FluxType::Struct(_) => RowValue::Struct(Vec::new()),
    }
}

fn column_ordinal(table: &TableSchema, name: &str) -> Option<u16> {
    table
        .columns
        .iter()
        .position(|c| c.name == name)
        .map(|i| u16::try_from(i).unwrap_or(u16::MAX))
}
