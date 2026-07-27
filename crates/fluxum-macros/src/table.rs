//! Expansion of `#[fluxum::table]` (SPEC-001 §2–§6).
//!
//! Parses the annotated struct plus its table-level attributes into a
//! `TableSchema` model, rejects every invalid combination the spec requires
//! at compile time (SPEC-001 acceptance 1), and emits:
//!
//! - the struct itself with the helper attributes stripped,
//! - `static` schema data (`TableSchema`, columns, indexes — DM-042),
//! - an `impl fluxum_core::schema::Table` (DM-043), and
//! - a link-time `inventory` registration (DM-040).

use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, format_ident, quote};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{
    Attribute, Expr, Fields, GenericArgument, Ident, ItemStruct, Lit, Meta, PathArguments, Token,
    Type,
};

/// Entry point: never panics, renders parse/validation failures as
/// `compile_error!`.
pub fn expand(args: TokenStream, input: TokenStream) -> TokenStream {
    match try_expand(args, input) {
        Ok(tokens) => tokens,
        Err(err) => err.to_compile_error(),
    }
}

// ---------------------------------------------------------------------------
// Model
// ---------------------------------------------------------------------------

/// Column type from the SPEC-001 §3 universe plus `#[derive(FluxType)]` rich
/// types (mirror of `fluxum_core::schema::FluxType`, macro-side).
#[derive(Clone)]
pub(crate) enum FluxTy {
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    Str,
    Bytes,
    Identity,
    ConnectionId,
    EntityId,
    Timestamp,
    Decimal,
    Blob,
    Opt(Box<FluxTy>),
    List(Box<FluxTy>),
    /// A `#[derive(FluxType)]` enum or nested struct used as a column
    /// (SPEC-023 DMX-030); the payload is the field's Rust type.
    Derived(Box<Type>),
    /// `fluxum_core::crdt::CrdtText` — convergent collaborative text
    /// (SPEC-023 DMX-060), stored as tagged bytes.
    CrdtText,
}

impl FluxTy {
    fn is_float(&self) -> bool {
        matches!(self, Self::F32 | Self::F64)
    }

    /// Tokens constructing the matching `fluxum_core::schema::FluxType`
    /// value in const context (nested references rely on static promotion).
    pub(crate) fn tokens(&self) -> TokenStream {
        let path = quote!(::fluxum_core::schema::FluxType);
        match self {
            Self::Bool => quote!(#path::Bool),
            Self::I8 => quote!(#path::I8),
            Self::I16 => quote!(#path::I16),
            Self::I32 => quote!(#path::I32),
            Self::I64 => quote!(#path::I64),
            Self::U8 => quote!(#path::U8),
            Self::U16 => quote!(#path::U16),
            Self::U32 => quote!(#path::U32),
            Self::U64 => quote!(#path::U64),
            Self::F32 => quote!(#path::F32),
            Self::F64 => quote!(#path::F64),
            Self::Str => quote!(#path::Str),
            Self::Bytes => quote!(#path::Bytes),
            Self::Identity => quote!(#path::Identity),
            Self::ConnectionId => quote!(#path::ConnectionId),
            Self::EntityId => quote!(#path::EntityId),
            Self::Timestamp => quote!(#path::Timestamp),
            Self::Decimal => quote!(#path::Decimal),
            Self::Blob => quote!(#path::Blob),
            Self::Opt(inner) => {
                let inner = inner.tokens();
                quote!(#path::Option(&#inner))
            }
            Self::List(inner) => {
                let inner = inner.tokens();
                quote!(#path::List(&#inner))
            }
            Self::Derived(ty) => {
                quote!(<#ty as ::fluxum_core::schema::FluxTypeDef>::FLUX_TYPE)
            }
            Self::CrdtText => quote!(#path::CrdtText),
        }
    }
}

/// One parsed column.
struct Column {
    ident: Ident,
    ty: Type,
    flux: FluxTy,
    /// Span of a `#[primary_key]` attribute, if present.
    primary_key: Option<Span>,
    /// Span of an `#[auto_inc]` attribute, if present.
    auto_inc: Option<Span>,
    /// The `#[default(expr)]` backfill expression, if present (SPEC-010
    /// MIG-020/MIG-021).
    default: Option<Expr>,
    /// The `#[rename(from = "old")]` source name, if present (SPEC-010
    /// MIG-020/MIG-021).
    rename_from: Option<(String, Span)>,
    /// Parsed transform attributes, canonical CT-011 order (SPEC-017).
    transforms: Vec<TransformDecl>,
    /// Span of an `#[owner]` attribute (ephemeral `ConnectionId` binding,
    /// SPEC-023 DMX-011), if present.
    owner: Option<Span>,
    /// The `#[computed(expr)]` generation expression, if present (SPEC-022
    /// RV-050): a Rust expression over sibling columns, evaluated on write.
    computed: Option<(Expr, Span)>,
    /// `#[check(expr)]` constraints (SPEC-022 RV-030): boolean Rust
    /// expressions over this row's columns, validated on write.
    checks: Vec<(Expr, Span)>,
    /// Span of a `#[not_null]` attribute (RV-030), if present; requires an
    /// `Option`-typed column.
    not_null: Option<Span>,
    /// A `#[references(Parent(col), on_delete = ...)]` foreign key (RV-030/
    /// 032), if present.
    references: Option<RefDecl>,
}

/// One parsed `#[references(Parent(col), on_delete = ...)]` declaration
/// (SPEC-022 RV-030/032).
struct RefDecl {
    /// The referenced parent table's struct name.
    parent: Ident,
    /// The referenced parent column (must be the parent's PK; validated at
    /// store assembly, where the parent's schema is in hand).
    parent_column: Ident,
    /// The RV-032 action: `restrict` (default) | `cascade` | `set_null`.
    on_delete: RefActionTok,
    span: Span,
}

/// Macro-side mirror of `fluxum_core::schema::RefAction`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RefActionTok {
    Restrict,
    Cascade,
    SetNull,
}

impl RefActionTok {
    /// Tokens constructing the matching `RefAction` variant.
    fn tokens(self) -> TokenStream {
        let path = quote!(::fluxum_core::schema::RefAction);
        match self {
            Self::Restrict => quote!(#path::Restrict),
            Self::Cascade => quote!(#path::Cascade),
            Self::SetNull => quote!(#path::SetNull),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Access {
    Private,
    Public,
    Global,
    /// Memory-only, client-visible, non-durable (SPEC-023 DMX-010).
    Ephemeral,
}

enum Visibility {
    OwnerOnly(Ident),
    PublicAll,
    ShardLocal,
    Custom(Ident),
    /// `member_of(Table, key)` — relational visibility (SPEC-022 RV-040).
    MemberOf {
        table: Ident,
        key: Ident,
    },
}

// Domain index names (SPEC-001/SPEC-008); the shared "Tree" postfix is intrinsic.
#[allow(clippy::enum_variant_names)]
enum IndexKind {
    BTree,
    QuadTree,
    RTree,
    /// `#[fulltext(col, [english|simple], [stop_words], [stemming])]`
    /// (SPEC-019 FTS-001).
    FullText {
        language: FtLang,
        stop_words: bool,
        stemming: bool,
    },
}

/// Full-text analyzer language keyword (FTS-010).
#[derive(Clone, Copy)]
enum FtLang {
    Simple,
    English,
}

struct IndexDecl {
    kind: IndexKind,
    columns: Vec<Ident>,
    span: Span,
}

// --- Column transforms (SPEC-017 CT-001..003) -------------------------------

/// One parsed per-column transform attribute. Validated per column (CT-002)
/// and against the table's key/index sets (CT-013) after all columns parse,
/// then emitted as a `fluxum_core::transform::ColumnTransformDef` link-time
/// registration in canonical order (normalize → encrypted → signed → masked →
/// grant, CT-011).
enum TransformDecl {
    Money {
        scale: u8,
        currency: Option<String>,
        span: Span,
    },
    Datetime {
        span: Span,
    },
    Str {
        form: StrForm,
        case: StrCase,
        trim: bool,
        span: Span,
    },
    Encrypted {
        key: String,
        span: Span,
    },
    Signed {
        by: SignedByDecl,
        span: Span,
    },
    Masked {
        strategy: MaskDecl,
        span: Span,
    },
    Grant {
        scope: GrantDecl,
        span: Span,
    },
}

#[derive(Clone, Copy)]
enum StrForm {
    Nfc,
    Nfkc,
}

#[derive(Clone, Copy)]
enum StrCase {
    None,
    Fold,
    Lower,
}

#[derive(Clone, Copy)]
enum MaskDecl {
    Null,
    Redact,
    Ciphertext,
    Hash,
}

enum SignedByDecl {
    Server,
    Column(Ident),
}

enum GrantDecl {
    Public,
    Owner,
    ServerPeer,
    Role(String),
}

impl TransformDecl {
    /// `(attribute name, canonical pipeline position)` — the dedup key
    /// (CT-002) and the CT-011 ordering key.
    fn family(&self) -> (&'static str, u8) {
        match self {
            Self::Money { .. } | Self::Datetime { .. } | Self::Str { .. } => ("#[normalize]", 0),
            Self::Encrypted { .. } => ("#[encrypted]", 1),
            Self::Signed { .. } => ("#[signed]", 2),
            Self::Masked { .. } => ("#[masked]", 3),
            Self::Grant { .. } => ("#[column_grant]", 4),
        }
    }

    fn span(&self) -> Span {
        match self {
            Self::Money { span, .. }
            | Self::Datetime { span }
            | Self::Str { span, .. }
            | Self::Encrypted { span, .. }
            | Self::Signed { span, .. }
            | Self::Masked { span, .. }
            | Self::Grant { span, .. } => *span,
        }
    }
}

// ---------------------------------------------------------------------------
// Expansion
// ---------------------------------------------------------------------------

mod expand;
mod parse;
#[allow(clippy::wildcard_imports)]
use expand::*;
#[allow(clippy::wildcard_imports)]
use parse::*;
pub(crate) use parse::{expand_edge, from_row_value, parse_flux_type, to_row_value};

#[cfg(test)]
mod tests;
