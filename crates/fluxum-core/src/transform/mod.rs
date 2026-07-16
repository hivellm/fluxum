//! Column transforms (SPEC-017): the DTO-in-schema write path.
//!
//! This module holds the deterministic value **normalizers** (CT-021/022/023)
//! — money to exact fixed-point, timestamps to canonical UTC — that the
//! `#[normalize(...)]` column attribute applies before a value is stored. The
//! `ColumnTransform` trait, the crypto transforms (`#[encrypted]`/`#[signed]`),
//! field-level security (`#[masked]`/`#[column_grant]`), and the link-time
//! transform registry land in later increments of this task; string/Unicode
//! normalization (CT-023) needs a Unicode-normalization dependency and is
//! deferred with them.

pub mod normalize;

pub use normalize::{datetime_utc, money_from_minor_units, money_from_str};

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
