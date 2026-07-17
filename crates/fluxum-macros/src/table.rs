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

fn try_expand(args: TokenStream, input: TokenStream) -> syn::Result<TokenStream> {
    let mut item: ItemStruct = syn::parse2(input)?;

    if !item.generics.params.is_empty() || item.generics.where_clause.is_some() {
        return Err(syn::Error::new(
            item.generics.span(),
            "#[fluxum::table] does not support generic structs (DM-001)",
        ));
    }

    // -- table arguments ----------------------------------------------------
    let mut access: Option<(Access, Span)> = None;
    let mut table_pk: Option<(Vec<Ident>, Span)> = None;
    let mut partition_by: Option<Ident> = None;
    let mut expire_after_us: Option<(i64, Span)> = None;

    let metas = Punctuated::<Meta, Token![,]>::parse_terminated.parse2(args)?;
    for meta in metas {
        let span = meta.span();
        let access_arg = ["private", "public", "global", "ephemeral"]
            .iter()
            .position(|name| meta.path().is_ident(name));
        if let Some(which) = access_arg {
            let this = match which {
                1 => Access::Public,
                2 => Access::Global,
                3 => Access::Ephemeral,
                _ => Access::Private,
            };
            if access.is_some() {
                return Err(syn::Error::new(
                    span,
                    "at most one of `public`, `private`, `global`, `ephemeral` — an ephemeral \
                     table is never global/replicated (DM-005/DM-007, SPEC-023 DMX-012)",
                ));
            }
            access = Some((this, span));
        } else if meta.path().is_ident("primary_key") {
            let list = meta.require_list()?;
            let cols = list.parse_args_with(Punctuated::<Ident, Token![,]>::parse_terminated)?;
            if cols.is_empty() {
                return Err(syn::Error::new(
                    span,
                    "`primary_key(...)` needs at least one column (DM-003)",
                ));
            }
            if table_pk.is_some() {
                return Err(syn::Error::new(
                    span,
                    "duplicate `primary_key(...)` argument",
                ));
            }
            table_pk = Some((cols.into_iter().collect(), span));
        } else if meta.path().is_ident("partition_by") {
            let list = meta.require_list()?;
            if partition_by.is_some() {
                return Err(syn::Error::new(
                    span,
                    "duplicate `partition_by(...)` argument",
                ));
            }
            partition_by = Some(list.parse_args::<Ident>()?);
        } else if meta.path().is_ident("expire_after") {
            let nv = meta.require_name_value()?;
            if expire_after_us.is_some() {
                return Err(syn::Error::new(span, "duplicate `expire_after` argument"));
            }
            let text = meta_value_str(nv).ok_or_else(|| {
                syn::Error::new(
                    nv.span(),
                    "`expire_after` must be a duration string like \"500ms\", \"10s\", \
                     \"5m\", or \"2h\" (DMX-011)",
                )
            })?;
            expire_after_us = Some((parse_duration_us(&text, nv.span())?, span));
        } else {
            return Err(syn::Error::new(
                span,
                "unknown #[fluxum::table] argument: expected `public`, `private`, `global`, \
                 `ephemeral`, `primary_key(col, ...)`, `partition_by(col)`, or \
                 `expire_after = \"...\"` (DM-020)",
            ));
        }
    }
    let access = access.map_or(Access::Private, |(a, _)| a);
    if let Some((_, span)) = expire_after_us
        && access != Access::Ephemeral
    {
        return Err(syn::Error::new(
            span,
            "`expire_after` is only valid on an `ephemeral` table (DMX-011)",
        ));
    }

    // -- companion struct attributes (stripped from the output) --------------
    let mut unique: Vec<Vec<Ident>> = Vec::new();
    let mut indexes: Vec<IndexDecl> = Vec::new();
    let mut visibility: Option<Visibility> = None;
    let mut ttl: Option<(TtlForm, Span)> = None;
    let mut kept_attrs: Vec<Attribute> = Vec::new();

    for attr in std::mem::take(&mut item.attrs) {
        if attr.path().is_ident("unique") {
            let cols = attr.parse_args_with(Punctuated::<Ident, Token![,]>::parse_terminated)?;
            if cols.is_empty() {
                return Err(syn::Error::new(
                    attr.span(),
                    "`#[unique(...)]` needs at least one column (DM-006)",
                ));
            }
            unique.push(cols.into_iter().collect());
        } else if attr.path().is_ident("index") {
            indexes.push(parse_index(&attr)?);
        } else if attr.path().is_ident("spatial") {
            indexes.push(parse_spatial(&attr)?);
        } else if attr.path().is_ident("fulltext") {
            indexes.push(parse_fulltext(&attr)?);
        } else if attr.path().is_ident("visibility") {
            if visibility.is_some() {
                return Err(syn::Error::new(
                    attr.span(),
                    "duplicate #[visibility] attribute",
                ));
            }
            visibility = Some(parse_visibility(&attr)?);
        } else if attr.path().is_ident("ttl") {
            if ttl.is_some() {
                return Err(syn::Error::new(
                    attr.span(),
                    "duplicate `#[ttl]`: a table declares at most one TTL rule (DMX-020)",
                ));
            }
            ttl = Some((parse_ttl(&attr)?, attr.span()));
        } else {
            kept_attrs.push(attr);
        }
    }
    item.attrs = kept_attrs;

    // -- fields → columns ----------------------------------------------------
    let Fields::Named(named) = &mut item.fields else {
        return Err(syn::Error::new(
            item.fields.span(),
            "#[fluxum::table] requires a struct with named fields (DM-001)",
        ));
    };

    let mut columns: Vec<Column> = Vec::new();
    for field in &mut named.named {
        let mut primary_key = None;
        let mut auto_inc = None;
        let mut default = None;
        let mut computed = None;
        let mut checks: Vec<(Expr, Span)> = Vec::new();
        let mut not_null = None;
        let mut references = None;
        let mut rename_from = None;
        let mut transforms: Vec<TransformDecl> = Vec::new();
        let mut owner = None;
        let mut kept: Vec<Attribute> = Vec::new();
        for attr in std::mem::take(&mut field.attrs) {
            if attr.path().is_ident("primary_key") {
                primary_key = Some(attr.span());
            } else if attr.path().is_ident("auto_inc") {
                auto_inc = Some(attr.span());
            } else if attr.path().is_ident("owner") {
                owner = Some(attr.span());
            } else if attr.path().is_ident("normalize") {
                transforms.push(parse_transform_normalize(&attr)?);
            } else if attr.path().is_ident("encrypted") {
                transforms.push(parse_transform_encrypted(&attr)?);
            } else if attr.path().is_ident("signed") {
                transforms.push(parse_transform_signed(&attr)?);
            } else if attr.path().is_ident("masked") {
                transforms.push(parse_transform_masked(&attr)?);
            } else if attr.path().is_ident("column_grant") {
                transforms.push(parse_transform_column_grant(&attr)?);
            } else if attr.path().is_ident("default") {
                if default.is_some() {
                    return Err(syn::Error::new(attr.span(), "duplicate `#[default]`"));
                }
                default = Some(parse_default(&attr)?);
            } else if attr.path().is_ident("computed") {
                if computed.is_some() {
                    return Err(syn::Error::new(attr.span(), "duplicate `#[computed]`"));
                }
                computed = Some((attr.parse_args::<Expr>()?, attr.span()));
            } else if attr.path().is_ident("check") {
                checks.push((attr.parse_args::<Expr>()?, attr.span()));
            } else if attr.path().is_ident("not_null") {
                if not_null.is_some() {
                    return Err(syn::Error::new(attr.span(), "duplicate `#[not_null]`"));
                }
                attr.meta.require_path_only()?;
                not_null = Some(attr.span());
            } else if attr.path().is_ident("references") {
                if references.is_some() {
                    return Err(syn::Error::new(attr.span(), "duplicate `#[references]`"));
                }
                references = Some(parse_references(&attr)?);
            } else if attr.path().is_ident("rename") {
                if rename_from.is_some() {
                    return Err(syn::Error::new(attr.span(), "duplicate `#[rename]`"));
                }
                rename_from = Some((parse_rename(&attr)?, attr.span()));
            } else if attr.path().is_ident("index")
                || attr.path().is_ident("spatial")
                || attr.path().is_ident("fulltext")
                || attr.path().is_ident("unique")
                || attr.path().is_ident("visibility")
            {
                return Err(syn::Error::new(
                    attr.span(),
                    "this is a table-level attribute: write it on the struct, below \
                     #[fluxum::table] (DM-020)",
                ));
            } else {
                kept.push(attr);
            }
        }
        field.attrs = kept;

        let Some(ident) = field.ident.clone() else {
            return Err(syn::Error::new(
                field.span(),
                "expected a named field (DM-001)",
            ));
        };
        // CT-002: at most one attribute of each transform family per column.
        let mut seen_families = [false; 5];
        for transform in &transforms {
            let (name, family) = transform.family();
            if seen_families[usize::from(family)] {
                return Err(syn::Error::new(
                    transform.span(),
                    format!("duplicate `{name}` on one column (CT-002)"),
                ));
            }
            seen_families[usize::from(family)] = true;
        }
        // CT-011 canonical pipeline order: normalize → encrypted → signed →
        // masked → grant, regardless of declaration order.
        transforms.sort_by_key(|t| t.family().1);

        let flux = parse_flux_type(&field.ty)?;
        columns.push(Column {
            ident,
            ty: field.ty.clone(),
            flux,
            primary_key,
            auto_inc,
            default,
            rename_from,
            transforms,
            owner,
            computed,
            checks,
            not_null,
            references,
        });
    }
    if columns.is_empty() {
        return Err(syn::Error::new(
            item.ident.span(),
            "a table must have at least one column (DM-001)",
        ));
    }

    // -- #[rename(from = "...")] consistency (SPEC-010) -----------------------
    for column in &columns {
        let Some((from, span)) = &column.rename_from else {
            continue;
        };
        if column.ident == from.as_str() {
            return Err(syn::Error::new(
                *span,
                "`#[rename(from = ...)]` names the field itself: point it at the column's \
                 previous stored name (MIG-020)",
            ));
        }
        if columns.iter().any(|other| other.ident == from.as_str()) {
            return Err(syn::Error::new(
                *span,
                format!(
                    "`#[rename(from = \"{from}\")]` names a column that is still declared: \
                     a rename source must be the old, removed name (MIG-020)"
                ),
            ));
        }
        let duplicates = columns
            .iter()
            .filter(|other| {
                other
                    .rename_from
                    .as_ref()
                    .is_some_and(|(other_from, _)| other_from == from)
            })
            .count();
        if duplicates > 1 {
            return Err(syn::Error::new(
                *span,
                format!("two columns declare `#[rename(from = \"{from}\")]` (MIG-020)"),
            ));
        }
    }

    let ordinal_of = |ident: &Ident, context: &str| -> syn::Result<u16> {
        columns
            .iter()
            .position(|c| c.ident == *ident)
            .map(|i| u16::try_from(i).unwrap_or(u16::MAX))
            .ok_or_else(|| {
                syn::Error::new(
                    ident.span(),
                    format!("unknown column `{ident}` referenced in {context}"),
                )
            })
    };

    // -- primary key ---------------------------------------------------------
    let field_pks: Vec<usize> = columns
        .iter()
        .enumerate()
        .filter_map(|(i, c)| c.primary_key.map(|_| i))
        .collect();

    if !field_pks.is_empty()
        && let Some((_, span)) = &table_pk
    {
        return Err(syn::Error::new(
            *span,
            "table declares both a `#[primary_key]` field and a table-level \
             `primary_key(...)` argument; declare exactly one (DM-003)",
        ));
    }
    if field_pks.len() > 1 {
        let span = columns[field_pks[1]]
            .primary_key
            .unwrap_or_else(Span::call_site);
        return Err(syn::Error::new(
            span,
            "duplicate `#[primary_key]`: a table has exactly one primary key; for a \
             composite key use the table-level `primary_key(col1, col2, ...)` argument \
             (DM-002/DM-003)",
        ));
    }

    let pk_ordinals: Vec<u16> = if let Some((cols, _)) = &table_pk {
        let mut seen = Vec::new();
        for col in cols {
            let ord = ordinal_of(col, "`primary_key(...)` (DM-003)")?;
            if seen.contains(&ord) {
                return Err(syn::Error::new(
                    col.span(),
                    format!("primary key lists column `{col}` twice (DM-003)"),
                ));
            }
            seen.push(ord);
        }
        seen
    } else if let Some(&i) = field_pks.first() {
        vec![u16::try_from(i).unwrap_or(u16::MAX)]
    } else {
        return Err(syn::Error::new(
            item.ident.span(),
            "table has no primary key: annotate one field with `#[primary_key]` or use \
             the table-level `primary_key(col, ...)` argument (DM-002)",
        ));
    };

    // -- auto_inc ------------------------------------------------------------
    let auto_incs: Vec<usize> = columns
        .iter()
        .enumerate()
        .filter_map(|(i, c)| c.auto_inc.map(|_| i))
        .collect();
    if auto_incs.len() > 1 {
        let span = columns[auto_incs[1]]
            .auto_inc
            .unwrap_or_else(Span::call_site);
        return Err(syn::Error::new(span, "duplicate `#[auto_inc]` (DM-004)"));
    }
    let auto_inc: Option<u16> = match auto_incs.first() {
        None => None,
        Some(&i) => {
            let col = &columns[i];
            let span = col.auto_inc.unwrap_or_else(Span::call_site);
            if pk_ordinals.len() > 1 {
                return Err(syn::Error::new(
                    span,
                    "`#[auto_inc]` is not supported on composite primary keys (DM-004)",
                ));
            }
            let ord = u16::try_from(i).unwrap_or(u16::MAX);
            if pk_ordinals != [ord] {
                return Err(syn::Error::new(
                    span,
                    "`#[auto_inc]` is only valid on the `#[primary_key]` field (DM-004)",
                ));
            }
            if !matches!(col.flux, FluxTy::U64) {
                return Err(syn::Error::new(
                    span,
                    "`#[auto_inc]` requires the primary-key column to be `u64` (DM-004)",
                ));
            }
            Some(ord)
        }
    };

    // -- partition_by ----------------------------------------------------------
    let partition_ordinal: Option<u16> = match &partition_by {
        None => None,
        Some(ident) => {
            if access == Access::Global {
                return Err(syn::Error::new(
                    ident.span(),
                    "`partition_by` cannot be combined with `global`: global tables are \
                     replicated to every shard, not partitioned (DM-008)",
                ));
            }
            Some(ordinal_of(ident, "`partition_by(...)` (DM-008)")?)
        }
    };

    // -- unique ---------------------------------------------------------------
    let unique_ordinals: Vec<Vec<u16>> = unique
        .iter()
        .map(|set| {
            set.iter()
                .map(|col| ordinal_of(col, "`#[unique(...)]` (DM-006)"))
                .collect()
        })
        .collect::<syn::Result<_>>()?;

    // -- indexes ----------------------------------------------------------------
    let mut spatial_seen: Option<(&'static str, Span)> = None;
    let mut index_keys: Vec<(&'static str, Vec<u16>)> = Vec::new();
    let mut index_tokens: Vec<TokenStream> = Vec::new();
    for decl in &indexes {
        let ords: Vec<u16> = decl
            .columns
            .iter()
            .map(|col| match decl.kind {
                IndexKind::BTree => ordinal_of(col, "`#[index(btree(...))]` (DM-030)"),
                IndexKind::FullText { .. } => ordinal_of(col, "`#[fulltext(...)]` (FTS-001)"),
                _ => ordinal_of(col, "`#[spatial(...)]` (DM-032)"),
            })
            .collect::<syn::Result<_>>()?;

        let (tag, tokens) = match decl.kind {
            IndexKind::BTree => {
                // Decimal is not yet a valid B-tree key: a numerically
                // order-preserving memcomparable encoding across mixed scales
                // is deferred (SPEC-017 CT-020).
                for (col, ord) in decl.columns.iter().zip(&ords) {
                    if matches!(columns[usize::from(*ord)].flux, FluxTy::Decimal) {
                        return Err(syn::Error::new(
                            col.span(),
                            format!(
                                "`Decimal` column `{col}` cannot yet be a B-tree index key \
                                 (SPEC-017 CT-020)"
                            ),
                        ));
                    }
                }
                (
                    "btree",
                    quote!(::fluxum_core::schema::IndexSchema::BTree { columns: &[#(#ords),*] }),
                )
            }
            IndexKind::QuadTree | IndexKind::RTree => {
                let (tag, kind) = match decl.kind {
                    IndexKind::QuadTree => (
                        "quadtree",
                        quote!(::fluxum_core::schema::SpatialKind::QuadTree),
                    ),
                    _ => ("rtree", quote!(::fluxum_core::schema::SpatialKind::RTree)),
                };
                if let Some((seen_tag, _)) = spatial_seen
                    && seen_tag != tag
                {
                    return Err(syn::Error::new(
                        decl.span,
                        "a table cannot declare both `quadtree` and `rtree` spatial \
                         indexes (DM-033)",
                    ));
                }
                spatial_seen = Some((tag, decl.span));
                for (col, ord) in decl.columns.iter().zip(&ords) {
                    if !columns[usize::from(*ord)].flux.is_float() {
                        return Err(syn::Error::new(
                            col.span(),
                            format!("spatial index column `{col}` must be `f32` or `f64` (DM-032)"),
                        ));
                    }
                }
                (
                    tag,
                    quote! {
                        ::fluxum_core::schema::IndexSchema::Spatial {
                            kind: #kind,
                            columns: &[#(#ords),*],
                        }
                    },
                )
            }
            IndexKind::FullText {
                language,
                stop_words,
                stemming,
            } => {
                let ord = ords[0];
                let flux = &columns[usize::from(ord)].flux;
                let is_text = matches!(flux, FluxTy::Str)
                    || matches!(flux, FluxTy::Opt(inner) | FluxTy::List(inner) if matches!(**inner, FluxTy::Str));
                if !is_text {
                    return Err(syn::Error::new(
                        decl.columns[0].span(),
                        format!(
                            "`#[fulltext]` column `{}` must be `String`, `Option<String>`, \
                             or `Vec<String>` (FTS-002)",
                            decl.columns[0]
                        ),
                    ));
                }
                let lang = match language {
                    FtLang::Simple => quote!(::fluxum_core::schema::FullTextLanguage::Simple),
                    FtLang::English => quote!(::fluxum_core::schema::FullTextLanguage::English),
                };
                (
                    "fulltext",
                    quote! {
                        ::fluxum_core::schema::IndexSchema::FullText {
                            column: #ord,
                            language: #lang,
                            stop_words: #stop_words,
                            stemming: #stemming,
                        }
                    },
                )
            }
        };
        let key = (tag, ords);
        if index_keys.contains(&key) {
            return Err(syn::Error::new(
                decl.span,
                format!(
                    "duplicate `{tag}` index on the same column set: a column set cannot \
                     be indexed twice with the same index type (DM-033)"
                ),
            ));
        }
        index_keys.push(key);
        index_tokens.push(tokens);
    }

    // -- rich-type key rejection (SPEC-023 DMX-031) ----------------------------
    // Enum/struct columns support equality only; they cannot be a primary key,
    // partition key, unique constraint, or B-tree index key (no derivable
    // memcomparable ordering).
    let mut key_ordinals: Vec<u16> = pk_ordinals.clone();
    key_ordinals.extend(unique_ordinals.iter().flatten().copied());
    key_ordinals.extend(partition_ordinal);
    for (tag, ords) in &index_keys {
        if *tag == "btree" {
            key_ordinals.extend(ords.iter().copied());
        }
    }
    for ord in key_ordinals {
        let col = &columns[usize::from(ord)];
        if matches!(col.flux, FluxTy::Derived(_)) {
            return Err(syn::Error::new(
                col.ident.span(),
                format!(
                    "column `{}` is a `#[derive(FluxType)]` enum/struct and cannot be a primary \
                     key, partition key, unique constraint, or index key — rich types support \
                     equality only (SPEC-023 DMX-031)",
                    col.ident
                ),
            ));
        }
        if matches!(col.flux, FluxTy::CrdtText) {
            return Err(syn::Error::new(
                col.ident.span(),
                format!(
                    "column `{}` is a CrdtText document and cannot be a primary key, partition \
                     key, unique constraint, or index key (SPEC-023 DMX-060)",
                    col.ident
                ),
            ));
        }
    }

    // -- column transforms (SPEC-017 CT-011/013/021..023/030/033/040/041) -------
    // Validate each column's transform pipeline against its type and the
    // table's key/index sets, then emit one link-time ColumnTransformDef per
    // transformed column, descriptors in canonical CT-011 order.
    let mut transform_submits: Vec<TokenStream> = Vec::new();
    {
        let table_name = item.ident.to_string();
        // Ordinals #[encrypted] may never touch (CT-013): keys + every index
        // (B-tree AND spatial).
        let mut encrypt_protected: Vec<u16> = pk_ordinals.clone();
        encrypt_protected.extend(unique_ordinals.iter().flatten().copied());
        encrypt_protected.extend(partition_ordinal);
        for (_tag, ords) in &index_keys {
            encrypt_protected.extend(ords.iter().copied());
        }
        let tf = quote!(::fluxum_core::transform);
        for (i, column) in columns.iter().enumerate() {
            if column.transforms.is_empty() {
                continue;
            }
            let ord = u16::try_from(i).unwrap_or(u16::MAX);
            let has_encrypted = column
                .transforms
                .iter()
                .any(|t| matches!(t, TransformDecl::Encrypted { .. }));
            let mut descriptors: Vec<TokenStream> = Vec::new();
            for transform in &column.transforms {
                let tokens = match transform {
                    TransformDecl::Money {
                        scale,
                        currency,
                        span,
                    } => {
                        if !matches!(column.flux, FluxTy::Decimal) {
                            return Err(syn::Error::new(
                                *span,
                                format!(
                                    "`#[normalize(money)]` requires column `{}` to be `Decimal` \
                                     (CT-021)",
                                    column.ident
                                ),
                            ));
                        }
                        let currency = match currency {
                            Some(c) => quote!(::core::option::Option::Some(#c)),
                            None => quote!(::core::option::Option::None),
                        };
                        quote! {
                            #tf::TransformDescriptor::NormalizeMoney {
                                scale: #scale, currency: #currency,
                            }
                        }
                    }
                    TransformDecl::Datetime { span } => {
                        if !matches!(column.flux, FluxTy::Timestamp) {
                            return Err(syn::Error::new(
                                *span,
                                format!(
                                    "`#[normalize(datetime)]` requires column `{}` to be \
                                     `Timestamp` (CT-022)",
                                    column.ident
                                ),
                            ));
                        }
                        quote!(#tf::TransformDescriptor::NormalizeDatetime)
                    }
                    TransformDecl::Str {
                        form,
                        case,
                        trim,
                        span,
                    } => {
                        if !matches!(column.flux, FluxTy::Str) {
                            return Err(syn::Error::new(
                                *span,
                                format!(
                                    "`#[normalize(string)]` requires column `{}` to be `String` \
                                     (CT-023)",
                                    column.ident
                                ),
                            ));
                        }
                        let form = match form {
                            StrForm::Nfc => quote!(#tf::StringForm::Nfc),
                            StrForm::Nfkc => quote!(#tf::StringForm::Nfkc),
                        };
                        let case = match case {
                            StrCase::None => quote!(#tf::CaseFold::None),
                            StrCase::Fold => quote!(#tf::CaseFold::Fold),
                            StrCase::Lower => quote!(#tf::CaseFold::Lower),
                        };
                        quote! {
                            #tf::TransformDescriptor::NormalizeString {
                                form: #form, case: #case, trim: #trim,
                            }
                        }
                    }
                    TransformDecl::Encrypted { key, span } => {
                        if encrypt_protected.contains(&ord) {
                            return Err(syn::Error::new(
                                *span,
                                format!(
                                    "`#[encrypted]` cannot apply to column `{}`: encrypted \
                                     columns cannot be a primary key, unique, index, partition, \
                                     or spatial column (CT-013)",
                                    column.ident
                                ),
                            ));
                        }
                        quote! {
                            #tf::TransformDescriptor::Encrypted {
                                scheme: #tf::CryptoScheme::Ecies, key: #key,
                            }
                        }
                    }
                    TransformDecl::Signed { by, span } => {
                        let by_tokens = match by {
                            SignedByDecl::Server => quote!(#tf::SignedBy::Server),
                            SignedByDecl::Column(source) => {
                                let source_ord =
                                    ordinal_of(source, "`#[signed(by = ...)]` (CT-033)")?;
                                if !matches!(
                                    columns[usize::from(source_ord)].flux,
                                    FluxTy::Identity
                                ) {
                                    return Err(syn::Error::new(
                                        *span,
                                        format!(
                                            "`#[signed(by = {source})]` must reference an \
                                             `Identity` column (CT-033)"
                                        ),
                                    ));
                                }
                                quote!(#tf::SignedBy::IdentityColumn(#source_ord))
                            }
                        };
                        quote! {
                            #tf::TransformDescriptor::Signed {
                                scheme: #tf::SignScheme::Ed25519, by: #by_tokens,
                            }
                        }
                    }
                    TransformDecl::Masked { strategy, span } => {
                        if matches!(strategy, MaskDecl::Ciphertext) && !has_encrypted {
                            return Err(syn::Error::new(
                                *span,
                                "`#[masked(ciphertext)]` requires `#[encrypted]` on the same \
                                 column (CT-041)",
                            ));
                        }
                        let strategy = match strategy {
                            MaskDecl::Null => quote!(#tf::MaskStrategy::Null),
                            MaskDecl::Redact => quote!(#tf::MaskStrategy::Redact),
                            MaskDecl::Ciphertext => quote!(#tf::MaskStrategy::Ciphertext),
                            MaskDecl::Hash => quote!(#tf::MaskStrategy::Hash),
                        };
                        quote!(#tf::TransformDescriptor::Masked { strategy: #strategy })
                    }
                    TransformDecl::Grant { scope, .. } => {
                        let scope = match scope {
                            GrantDecl::Public => quote!(#tf::GrantScope::Public),
                            GrantDecl::Owner => quote!(#tf::GrantScope::Owner),
                            GrantDecl::ServerPeer => quote!(#tf::GrantScope::ServerPeer),
                            GrantDecl::Role(role) => quote!(#tf::GrantScope::Role(#role)),
                        };
                        quote!(#tf::TransformDescriptor::Grant { select: #scope })
                    }
                };
                descriptors.push(tokens);
            }
            let column_name = column.ident.to_string();
            transform_submits.push(quote! {
                ::fluxum_core::schema::inventory::submit! {
                    #tf::ColumnTransformDef {
                        table: #table_name,
                        column: #column_name,
                        transforms: &[#(#descriptors),*],
                    }
                }
            });
        }
    }

    // -- ephemeral cleanup metadata (SPEC-023 DMX-011) ---------------------------
    // `#[owner]` binds rows to a `ConnectionId` for disconnect cleanup;
    // `expire_after` gives rows a TTL. Both register a link-time EphemeralDef.
    let mut owner_ordinal: Option<u16> = None;
    for (i, column) in columns.iter().enumerate() {
        let Some(span) = column.owner else { continue };
        if access != Access::Ephemeral {
            return Err(syn::Error::new(
                span,
                "`#[owner]` is only valid on an `ephemeral` table (DMX-011)",
            ));
        }
        if owner_ordinal.is_some() {
            return Err(syn::Error::new(
                span,
                "at most one `#[owner]` column per table (DMX-011)",
            ));
        }
        if !matches!(column.flux, FluxTy::ConnectionId) {
            return Err(syn::Error::new(
                span,
                format!(
                    "`#[owner]` column `{}` must be of type `ConnectionId` (DMX-011)",
                    column.ident
                ),
            ));
        }
        owner_ordinal = Some(u16::try_from(i).unwrap_or(u16::MAX));
    }
    let ephemeral_submit: Option<TokenStream> =
        if access == Access::Ephemeral && (owner_ordinal.is_some() || expire_after_us.is_some()) {
            let table_name = item.ident.to_string();
            let owner_tokens = match owner_ordinal {
                Some(ord) => quote!(::core::option::Option::Some(#ord)),
                None => quote!(::core::option::Option::None),
            };
            let expire_tokens = match expire_after_us {
                Some((us, _)) => quote!(::core::option::Option::Some(#us)),
                None => quote!(::core::option::Option::None),
            };
            Some(quote! {
                ::fluxum_core::schema::inventory::submit! {
                    ::fluxum_core::schema::EphemeralDef {
                        table: #table_name,
                        owner: #owner_tokens,
                        expire_after_us: #expire_tokens,
                    }
                }
            })
        } else {
            None
        };

    // `#[ttl(...)]` registers a row-TTL def (SPEC-023 DMX-020). `#[ttl(col)]`
    // resolves the column to an ordinal and requires a `Timestamp` type; the
    // `after` form carries its microseconds directly.
    let ttl_submit: Option<TokenStream> = match &ttl {
        None => None,
        Some((form, _span)) => {
            let table_name = item.ident.to_string();
            let kind = match form {
                TtlForm::Field(col) => {
                    let ord = ordinal_of(col, "`#[ttl(col)]` (DMX-020)")?;
                    if !matches!(columns[usize::from(ord)].flux, FluxTy::Timestamp) {
                        return Err(syn::Error::new(
                            col.span(),
                            format!("`#[ttl]` column `{col}` must be a `Timestamp` (DMX-020)"),
                        ));
                    }
                    quote!(::fluxum_core::schema::TtlKind::Field { column: #ord })
                }
                TtlForm::After(us) => {
                    quote!(::fluxum_core::schema::TtlKind::After { after_us: #us })
                }
            };
            Some(quote! {
                ::fluxum_core::schema::inventory::submit! {
                    ::fluxum_core::schema::TtlDef {
                        table: #table_name,
                        kind: #kind,
                    }
                }
            })
        }
    };

    // -- visibility -------------------------------------------------------------
    let visibility_tokens = match &visibility {
        None => quote!(::fluxum_core::schema::VisibilityRule::PublicAll),
        Some(Visibility::PublicAll) => {
            quote!(::fluxum_core::schema::VisibilityRule::PublicAll)
        }
        Some(Visibility::ShardLocal) => {
            quote!(::fluxum_core::schema::VisibilityRule::ShardLocal)
        }
        Some(Visibility::Custom(f)) => {
            let name = f.to_string();
            quote!(::fluxum_core::schema::VisibilityRule::Custom(#name))
        }
        Some(Visibility::OwnerOnly(col)) => {
            let ord = ordinal_of(col, "`#[visibility(owner_only(...))]` (DM-060)")?;
            if !matches!(columns[usize::from(ord)].flux, FluxTy::Identity) {
                return Err(syn::Error::new(
                    col.span(),
                    format!("`owner_only` column `{col}` must be of type `Identity` (DM-060)"),
                ));
            }
            quote!(::fluxum_core::schema::VisibilityRule::OwnerOnly { owner: #ord })
        }
    };

    // -- computed columns (SPEC-022 RV-050) -------------------------------------
    // Each `#[computed(expr)]` compiles to a link-time `ComputedDef` whose
    // `compute` fn binds the referenced sibling columns to their native types,
    // evaluates the Rust expression, and wraps the result. The store applies it
    // on write, overwriting whatever the reducer set (the column is read-only).
    let struct_ident = &item.ident;
    let name_str = struct_ident.to_string();
    let mut computed_submits: Vec<TokenStream> = Vec::new();
    {
        let by_name: std::collections::HashMap<String, (u16, &FluxTy)> = columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                (
                    c.ident.to_string(),
                    (u16::try_from(i).unwrap_or(u16::MAX), &c.flux),
                )
            })
            .collect();
        for (i, column) in columns.iter().enumerate() {
            let Some((expr, span)) = &column.computed else {
                continue;
            };
            let span = *span;
            if column.primary_key.is_some() || column.auto_inc.is_some() {
                return Err(syn::Error::new(
                    span,
                    "a `#[computed]` column cannot be a primary key or `#[auto_inc]` (RV-050)",
                ));
            }
            if column.default.is_some() {
                return Err(syn::Error::new(
                    span,
                    "a `#[computed]` column cannot also declare `#[default]` — its value is \
                     always derived (RV-050)",
                ));
            }
            if column.owner.is_some() || !column.transforms.is_empty() {
                return Err(syn::Error::new(
                    span,
                    "a `#[computed]` column cannot combine with `#[owner]` or a transform \
                     attribute (RV-050)",
                ));
            }
            let ord = u16::try_from(i).unwrap_or(u16::MAX);
            let self_name = column.ident.to_string();
            let mut bindings: Vec<TokenStream> = Vec::new();
            for name in collect_idents(expr) {
                if name == self_name {
                    return Err(syn::Error::new(
                        span,
                        format!("`#[computed]` column `{self_name}` cannot reference itself"),
                    ));
                }
                if let Some((sib_ord, sib_flux)) = by_name.get(&name) {
                    let ident = format_ident!("{}", name);
                    let idx = usize::from(*sib_ord);
                    let extract =
                        from_row_value(sib_flux, quote!((&__fx_values[#idx])), &name_str, &name);
                    bindings.push(quote!(let #ident = #extract;));
                }
            }
            let wrap = to_row_value(&column.flux, quote!(__fx_result));
            let fn_ident = format_ident!("__fx_compute_{}_{}", struct_ident, column.ident);
            computed_submits.push(quote! {
                #[allow(unused_variables, non_snake_case)]
                fn #fn_ident(
                    __fx_values: &[::fluxum_core::store::RowValue],
                ) -> ::fluxum_core::error::Result<::fluxum_core::store::RowValue> {
                    #(#bindings)*
                    let __fx_result = { #expr };
                    ::core::result::Result::Ok(#wrap)
                }
                ::fluxum_core::schema::inventory::submit! {
                    ::fluxum_core::schema::ComputedDef {
                        table: #name_str,
                        column: #self_name,
                        ordinal: #ord,
                        compute: #fn_ident,
                    }
                }
            });
        }
    }

    // -- declarative constraints (SPEC-022 RV-030/032) ---------------------------
    // `#[check(expr)]` compiles to a link-time `CheckDef` whose predicate fn
    // binds the referenced columns (self included) to their native types;
    // `#[not_null]` and `#[references]` submit plain metadata defs. The store
    // validates all three on every write, before merge.
    let mut constraint_submits: Vec<TokenStream> = Vec::new();
    {
        let by_name: std::collections::HashMap<String, (u16, &FluxTy)> = columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                (
                    c.ident.to_string(),
                    (u16::try_from(i).unwrap_or(u16::MAX), &c.flux),
                )
            })
            .collect();
        for (i, column) in columns.iter().enumerate() {
            let ord = u16::try_from(i).unwrap_or(u16::MAX);
            let self_name = column.ident.to_string();
            for (check_idx, (expr, _span)) in column.checks.iter().enumerate() {
                let mut bindings: Vec<TokenStream> = Vec::new();
                for name in collect_idents(expr) {
                    if let Some((sib_ord, sib_flux)) = by_name.get(&name) {
                        let ident = format_ident!("{}", name);
                        let idx = usize::from(*sib_ord);
                        let extract =
                            from_row_value(sib_flux, quote!((&__fx_values[#idx])), &name_str, &name);
                        bindings.push(quote!(let #ident = #extract;));
                    }
                }
                let expr_str = expr.to_token_stream().to_string();
                let fn_ident = format_ident!(
                    "__fx_check_{}_{}_{}",
                    struct_ident,
                    column.ident,
                    check_idx
                );
                constraint_submits.push(quote! {
                    #[allow(unused_variables, non_snake_case)]
                    fn #fn_ident(
                        __fx_values: &[::fluxum_core::store::RowValue],
                    ) -> ::fluxum_core::error::Result<bool> {
                        #(#bindings)*
                        ::core::result::Result::Ok({ #expr })
                    }
                    ::fluxum_core::schema::inventory::submit! {
                        ::fluxum_core::schema::CheckDef {
                            table: #name_str,
                            column: #self_name,
                            expr: #expr_str,
                            check: #fn_ident,
                        }
                    }
                });
            }
            if let Some(span) = column.not_null {
                if !matches!(column.flux, FluxTy::Opt(_)) {
                    return Err(syn::Error::new(
                        span,
                        format!(
                            "`#[not_null]` on non-Option column `{self_name}`: the type \
                             already forbids None — the attribute is for Option-typed \
                             columns kept nullable on the wire (RV-030)"
                        ),
                    ));
                }
                constraint_submits.push(quote! {
                    ::fluxum_core::schema::inventory::submit! {
                        ::fluxum_core::schema::NotNullDef {
                            table: #name_str,
                            column: #self_name,
                            ordinal: #ord,
                        }
                    }
                });
            }
            if let Some(decl) = &column.references {
                if decl.on_delete == RefActionTok::SetNull
                    && !matches!(column.flux, FluxTy::Opt(_))
                {
                    return Err(syn::Error::new(
                        decl.span,
                        format!(
                            "`on_delete = set_null` requires `{self_name}` to be \
                             Option-typed (RV-032)"
                        ),
                    ));
                }
                if column.computed.is_some() {
                    return Err(syn::Error::new(
                        decl.span,
                        "a `#[computed]` column cannot declare `#[references]` — the \
                         derivation, not the reducer, controls its value (RV-030)",
                    ));
                }
                let parent = decl.parent.to_string();
                let parent_column = decl.parent_column.to_string();
                if parent == name_str {
                    // Self-referential FKs are legal (tree shapes); only the
                    // trivially impossible same-column case is rejected.
                    if parent_column == self_name {
                        return Err(syn::Error::new(
                            decl.span,
                            "`#[references]` cannot target the declaring column itself",
                        ));
                    }
                }
                let action = decl.on_delete.tokens();
                constraint_submits.push(quote! {
                    ::fluxum_core::schema::inventory::submit! {
                        ::fluxum_core::schema::ForeignKeyDef {
                            table: #name_str,
                            column: #self_name,
                            ordinal: #ord,
                            parent_table: #parent,
                            parent_column: #parent_column,
                            on_delete: #action,
                        }
                    }
                });
            }
        }
    }

    let column_tokens = columns.iter().map(|c| {
        let name = c.ident.to_string();
        let ty = c.flux.tokens();
        quote!(::fluxum_core::schema::ColumnSchema { name: #name, ty: #ty })
    });

    let access_tokens = match access {
        Access::Private => quote!(::fluxum_core::schema::TableAccess::Private),
        Access::Public => quote!(::fluxum_core::schema::TableAccess::Public),
        Access::Global => quote!(::fluxum_core::schema::TableAccess::Global),
        Access::Ephemeral => quote!(::fluxum_core::schema::TableAccess::Ephemeral),
    };
    let auto_inc_tokens = match auto_inc {
        Some(ord) => quote!(::core::option::Option::Some(#ord)),
        None => quote!(::core::option::Option::None),
    };
    let partition_tokens = match partition_ordinal {
        Some(ord) => quote!(::core::option::Option::Some(#ord)),
        None => quote!(::core::option::Option::None),
    };
    let unique_tokens = unique_ordinals.iter().map(|set| quote!(&[#(#set),*]));

    let pk_fields: Vec<&Column> = pk_ordinals
        .iter()
        .map(|ord| &columns[usize::from(*ord)])
        .collect();
    let (pk_ty, pk_expr) = if pk_fields.len() == 1 {
        let ty = &pk_fields[0].ty;
        let ident = &pk_fields[0].ident;
        (
            quote!(#ty),
            quote!(::core::clone::Clone::clone(&self.#ident)),
        )
    } else {
        let tys = pk_fields.iter().map(|c| &c.ty);
        let idents = pk_fields.iter().map(|c| &c.ident);
        (
            quote!((#(#tys),*)),
            quote!((#(::core::clone::Clone::clone(&self.#idents)),*)),
        )
    };

    // Typed ⇄ dynamic row conversions (DM-043, SPEC-004 T3.2): the bridge
    // the `TxHandle` typed accessors use to reach the RowValue-based store.
    let ncols = columns.len();
    let into_exprs = columns.iter().map(|c| {
        let ident = &c.ident;
        to_row_value(&c.flux, quote!(self.#ident))
    });
    let field_idents = columns.iter().map(|c| &c.ident);
    let from_exprs = columns.iter().enumerate().map(|(i, c)| {
        let column_name = c.ident.to_string();
        from_row_value(&c.flux, quote!((&values[#i])), &name_str, &column_name)
    });
    let pk_value_exprs = pk_fields.iter().enumerate().map(|(i, c)| {
        let component = if pk_fields.len() == 1 {
            quote!(::core::clone::Clone::clone(pk))
        } else {
            let member = syn::Index::from(i);
            quote!(::core::clone::Clone::clone(&pk.#member))
        };
        to_row_value(&c.flux, component)
    });

    // #[default] / #[rename] column metadata for the SPEC-010 schema diff
    // (MIG-020/MIG-021), registered only when the table declares any.
    let mut default_fns: Vec<TokenStream> = Vec::new();
    let mut default_entries: Vec<TokenStream> = Vec::new();
    let mut rename_entries: Vec<TokenStream> = Vec::new();
    for column in &columns {
        let column_name = column.ident.to_string();
        if let Some(expr) = &column.default {
            let fn_ident = format_ident!("__fluxum_default_{}", column.ident);
            let ty = &column.ty;
            // The type ascription makes a default that does not inhabit the
            // column's Rust type a compile error.
            default_fns.push(quote! {
                fn #fn_ident() -> ::fluxum_core::store::RowValue {
                    let __value: #ty = #expr;
                    ::fluxum_core::migration::IntoRowValue::into_row_value(__value)
                }
            });
            default_entries.push(quote! {
                ::fluxum_core::migration::ColumnDefault {
                    column: #column_name,
                    value: #fn_ident,
                }
            });
        }
        if let Some((from, _)) = &column.rename_from {
            rename_entries.push(quote! {
                ::fluxum_core::migration::ColumnRename {
                    column: #column_name,
                    from: #from,
                }
            });
        }
    }
    let migration_meta = if default_entries.is_empty() && rename_entries.is_empty() {
        quote!()
    } else {
        quote! {
            #(#default_fns)*

            static __FLUXUM_DEFAULTS: &[::fluxum_core::migration::ColumnDefault] =
                &[#(#default_entries),*];
            static __FLUXUM_RENAMES: &[::fluxum_core::migration::ColumnRename] =
                &[#(#rename_entries),*];

            ::fluxum_core::schema::inventory::submit! {
                ::fluxum_core::migration::TableColumnMeta {
                    table: #name_str,
                    defaults: __FLUXUM_DEFAULTS,
                    renames: __FLUXUM_RENAMES,
                }
            }
        }
    };

    Ok(quote! {
        #item

        const _: () = {
            static __FLUXUM_COLUMNS: &[::fluxum_core::schema::ColumnSchema] =
                &[#(#column_tokens),*];
            static __FLUXUM_SCHEMA: ::fluxum_core::schema::TableSchema =
                ::fluxum_core::schema::TableSchema {
                    name: #name_str,
                    columns: __FLUXUM_COLUMNS,
                    primary_key: &[#(#pk_ordinals),*],
                    auto_inc: #auto_inc_tokens,
                    access: #access_tokens,
                    partition_by: #partition_tokens,
                    unique: &[#(#unique_tokens),*],
                    indexes: &[#(#index_tokens),*],
                    visibility: #visibility_tokens,
                };

            impl ::fluxum_core::schema::Table for #struct_ident {
                type Pk = #pk_ty;

                const SCHEMA: &'static ::fluxum_core::schema::TableSchema = &__FLUXUM_SCHEMA;

                fn primary_key(&self) -> Self::Pk {
                    #pk_expr
                }

                fn into_values(self) -> ::std::vec::Vec<::fluxum_core::store::RowValue> {
                    ::std::vec![#(#into_exprs),*]
                }

                fn from_values(
                    values: &[::fluxum_core::store::RowValue],
                ) -> ::fluxum_core::error::Result<Self> {
                    if values.len() != #ncols {
                        return ::core::result::Result::Err(
                            ::fluxum_core::FluxumError::Storage(::std::format!(
                                "table `{}`: row has {} values but the schema declares \
                                 {} columns",
                                #name_str,
                                values.len(),
                                #ncols,
                            )),
                        );
                    }
                    ::core::result::Result::Ok(Self {
                        #(#field_idents: #from_exprs),*
                    })
                }

                fn pk_values(
                    pk: &Self::Pk,
                ) -> ::std::vec::Vec<::fluxum_core::store::RowValue> {
                    ::std::vec![#(#pk_value_exprs),*]
                }
            }

            ::fluxum_core::schema::inventory::submit! {
                ::fluxum_core::schema::TableDef(&__FLUXUM_SCHEMA)
            }

            #(#transform_submits)*

            #ephemeral_submit

            #ttl_submit

            #(#computed_submits)*

            #(#constraint_submits)*

            #migration_meta
        };
    })
}

// ---------------------------------------------------------------------------
// Attribute parsers
// ---------------------------------------------------------------------------

/// Collect every identifier token in a `#[computed]` expression, so each one
/// that names a sibling column can be bound to its native value (SPEC-022
/// RV-050). Scans the raw token stream (recursing into groups) so identifiers
/// inside macro calls like `format!(…)` are found too — only tokens matching a
/// sibling column name are bound, and the generated fn allows unused bindings
/// for a method/type name that happens to match a column. Idents *inside a
/// string literal* (`format!("{id}")` inline capture) are not tokens and are
/// not detected — reference columns as real idents.
fn collect_idents(expr: &Expr) -> std::collections::HashSet<String> {
    fn walk(ts: proc_macro2::TokenStream, out: &mut std::collections::HashSet<String>) {
        for tt in ts {
            match tt {
                proc_macro2::TokenTree::Ident(id) => {
                    out.insert(id.to_string());
                }
                proc_macro2::TokenTree::Group(g) => walk(g.stream(), out),
                _ => {}
            }
        }
    }
    let mut out = std::collections::HashSet::new();
    walk(expr.to_token_stream(), &mut out);
    out
}

/// `#[references(Parent(col))]` or
/// `#[references(Parent(col), on_delete = restrict|cascade|set_null)]`
/// (SPEC-022 RV-030/032). The referenced column must be the parent's
/// primary key — validated at store assembly, where the parent's schema is
/// in hand.
fn parse_references(attr: &Attribute) -> syn::Result<RefDecl> {
    let span = attr.span();
    attr.parse_args_with(|input: syn::parse::ParseStream| {
        let parent: Ident = input.parse()?;
        let content;
        syn::parenthesized!(content in input);
        let parent_column: Ident = content.parse()?;
        if !content.is_empty() {
            return Err(content.error(
                "foreign keys reference exactly one column: `Parent(col)` (RV-030)",
            ));
        }
        let mut on_delete = RefActionTok::Restrict;
        if input.peek(syn::Token![,]) {
            input.parse::<syn::Token![,]>()?;
            let key: Ident = input.parse()?;
            if key != "on_delete" {
                return Err(syn::Error::new(
                    key.span(),
                    "expected `on_delete = restrict|cascade|set_null` (RV-032)",
                ));
            }
            input.parse::<syn::Token![=]>()?;
            let value: Ident = input.parse()?;
            on_delete = match value.to_string().as_str() {
                "restrict" => RefActionTok::Restrict,
                "cascade" => RefActionTok::Cascade,
                "set_null" => RefActionTok::SetNull,
                other => {
                    return Err(syn::Error::new(
                        value.span(),
                        format!(
                            "unknown referential action `{other}`: expected `restrict`, \
                             `cascade`, or `set_null` (RV-032)"
                        ),
                    ));
                }
            };
        }
        if !input.is_empty() {
            return Err(input.error("unexpected tokens after `on_delete = ...`"));
        }
        Ok(RefDecl {
            parent,
            parent_column,
            on_delete,
            span,
        })
    })
}

/// `#[default(expr)]` (SPEC-010 MIG-020): the backfill value used when the
/// column is auto-applied onto existing rows.
fn parse_default(attr: &Attribute) -> syn::Result<Expr> {
    if matches!(attr.meta, Meta::Path(_)) {
        return Err(syn::Error::new(
            attr.span(),
            "expected `#[default(value)]` with the backfill value (MIG-020)",
        ));
    }
    attr.parse_args::<Expr>()
}

/// `#[rename(from = "old")]` (SPEC-010 MIG-020): the column's previous
/// stored name, renamed in place by the startup schema diff.
fn parse_rename(attr: &Attribute) -> syn::Result<String> {
    let usage = || {
        syn::Error::new(
            attr.span(),
            "expected `#[rename(from = \"old_name\")]` (MIG-020)",
        )
    };
    let meta: Meta = attr.parse_args().map_err(|_| usage())?;
    let Meta::NameValue(pair) = &meta else {
        return Err(usage());
    };
    if !pair.path.is_ident("from") {
        return Err(usage());
    }
    let Expr::Lit(lit) = &pair.value else {
        return Err(usage());
    };
    let Lit::Str(name) = &lit.lit else {
        return Err(usage());
    };
    let name = name.value();
    if name.is_empty() {
        return Err(syn::Error::new(
            attr.span(),
            "`#[rename(from = ...)]` needs a non-empty column name (MIG-020)",
        ));
    }
    Ok(name)
}

/// `#[index(btree(col, ...))]` (DM-030/DM-031).
fn parse_index(attr: &Attribute) -> syn::Result<IndexDecl> {
    let meta: Meta = attr.parse_args()?;
    if !meta.path().is_ident("btree") {
        return Err(syn::Error::new(
            meta.span(),
            "expected `#[index(btree(col, ...))]` (DM-030)",
        ));
    }
    let cols = meta
        .require_list()?
        .parse_args_with(Punctuated::<Ident, Token![,]>::parse_terminated)?;
    if cols.is_empty() {
        return Err(syn::Error::new(
            meta.span(),
            "`btree(...)` needs at least one column (DM-030)",
        ));
    }
    Ok(IndexDecl {
        kind: IndexKind::BTree,
        columns: cols.into_iter().collect(),
        span: attr.span(),
    })
}

/// `#[spatial(quadtree(x, y))]` / `#[spatial(rtree(a, b, c, d))]` (DM-032).
fn parse_spatial(attr: &Attribute) -> syn::Result<IndexDecl> {
    let meta: Meta = attr.parse_args()?;
    let (kind, arity, usage) = if meta.path().is_ident("quadtree") {
        (IndexKind::QuadTree, 2, "quadtree(x, y)")
    } else if meta.path().is_ident("rtree") {
        (IndexKind::RTree, 4, "rtree(min_x, min_y, max_x, max_y)")
    } else {
        return Err(syn::Error::new(
            meta.span(),
            "expected `#[spatial(quadtree(x, y))]` or \
             `#[spatial(rtree(min_x, min_y, max_x, max_y))]` (DM-032)",
        ));
    };
    let cols = meta
        .require_list()?
        .parse_args_with(Punctuated::<Ident, Token![,]>::parse_terminated)?;
    if cols.len() != arity {
        return Err(syn::Error::new(
            meta.span(),
            format!("expected exactly {arity} coordinate columns: `{usage}` (DM-032)"),
        ));
    }
    Ok(IndexDecl {
        kind,
        columns: cols.into_iter().collect(),
        span: attr.span(),
    })
}

/// `#[fulltext(col, [simple|english], [stop_words], [stemming])]`
/// (SPEC-019 FTS-001/010). The first item names the indexed text column;
/// the rest are analyzer keywords in any order.
fn parse_fulltext(attr: &Attribute) -> syn::Result<IndexDecl> {
    let items = attr.parse_args_with(Punctuated::<Ident, Token![,]>::parse_terminated)?;
    let mut iter = items.iter();
    let Some(column) = iter.next().cloned() else {
        return Err(syn::Error::new(
            attr.span(),
            "expected `#[fulltext(col, [simple|english], [stop_words], [stemming])]` \
             (FTS-001)",
        ));
    };
    let mut language = FtLang::Simple;
    let mut stop_words = false;
    let mut stemming = false;
    for kw in iter {
        match kw.to_string().as_str() {
            "simple" => language = FtLang::Simple,
            "english" => language = FtLang::English,
            "stop_words" => stop_words = true,
            "stemming" => stemming = true,
            other => {
                return Err(syn::Error::new(
                    kw.span(),
                    format!(
                        "unknown `#[fulltext]` option `{other}`: expected `simple`, \
                         `english`, `stop_words`, or `stemming` (FTS-010)"
                    ),
                ));
            }
        }
    }
    Ok(IndexDecl {
        kind: IndexKind::FullText {
            language,
            stop_words,
            stemming,
        },
        columns: vec![column],
        span: attr.span(),
    })
}

/// A parsed `#[ttl(...)]` declaration (SPEC-023 DMX-020), resolved to a
/// [`TtlDef`](fluxum_core::schema::TtlDef) in codegen.
enum TtlForm {
    /// `#[ttl(col)]` — expire when the named `Timestamp` column is past.
    Field(Ident),
    /// `#[ttl(after = "30m")]` — expire N µs after the last write.
    After(i64),
}

/// `#[ttl(col)]` (absolute expiry from a `Timestamp` column) or
/// `#[ttl(after = "30m")]` (sliding TTL since last write) — SPEC-023 DMX-020.
fn parse_ttl(attr: &Attribute) -> syn::Result<TtlForm> {
    let meta: Meta = attr.parse_args().map_err(|_| {
        syn::Error::new(
            attr.span(),
            "expected `#[ttl(column)]` or `#[ttl(after = \"30m\")]` (DMX-020)",
        )
    })?;
    match meta {
        Meta::Path(path) => {
            let ident = path.get_ident().cloned().ok_or_else(|| {
                syn::Error::new(
                    path.span(),
                    "`#[ttl(column)]` expects a column name (DMX-020)",
                )
            })?;
            Ok(TtlForm::Field(ident))
        }
        Meta::NameValue(nv) if nv.path.is_ident("after") => {
            let Expr::Lit(syn::ExprLit {
                lit: Lit::Str(text),
                ..
            }) = &nv.value
            else {
                return Err(syn::Error::new(
                    nv.value.span(),
                    "`after` must be a duration string like \"30m\", \"10s\", \"500ms\" (DMX-020)",
                ));
            };
            Ok(TtlForm::After(parse_duration_us(
                &text.value(),
                text.span(),
            )?))
        }
        _ => Err(syn::Error::new(
            meta.span(),
            "expected `#[ttl(column)]` or `#[ttl(after = \"30m\")]` (DMX-020)",
        )),
    }
}

/// `#[visibility(owner_only(col) | public_all | shard_local | custom(f))]`
/// (DM-060/DM-061).
fn parse_visibility(attr: &Attribute) -> syn::Result<Visibility> {
    let meta: Meta = attr.parse_args()?;
    if meta.path().is_ident("public_all") {
        meta.require_path_only()?;
        Ok(Visibility::PublicAll)
    } else if meta.path().is_ident("shard_local") {
        meta.require_path_only()?;
        Ok(Visibility::ShardLocal)
    } else if meta.path().is_ident("owner_only") {
        Ok(Visibility::OwnerOnly(meta.require_list()?.parse_args()?))
    } else if meta.path().is_ident("custom") {
        Ok(Visibility::Custom(meta.require_list()?.parse_args()?))
    } else {
        Err(syn::Error::new(
            meta.span(),
            "expected `owner_only(col)`, `public_all`, `shard_local`, or `custom(filter_fn)` \
             (DM-061)",
        ))
    }
}

// ---------------------------------------------------------------------------
// Transform attribute parsers (SPEC-017 CT-001/CT-003)
// ---------------------------------------------------------------------------

/// A `name = ident` argument value, as a string.
fn meta_value_ident(nv: &syn::MetaNameValue) -> Option<String> {
    match &nv.value {
        Expr::Path(p) => p.path.get_ident().map(ToString::to_string),
        _ => None,
    }
}

/// Parse an `expire_after` duration string — `<int>` + `ms`|`s`|`m`|`h` —
/// into microseconds (DMX-011).
fn parse_duration_us(text: &str, span: Span) -> syn::Result<i64> {
    let bad = || {
        syn::Error::new(
            span,
            format!(
                "invalid duration `{text}`: expected `<integer>` + `ms`|`s`|`m`|`h`, e.g. \
                 \"10s\" (DMX-011)"
            ),
        )
    };
    let split = text.find(|c: char| !c.is_ascii_digit()).ok_or_else(bad)?;
    let (digits, unit) = text.split_at(split);
    let value: i64 = digits.parse().map_err(|_| bad())?;
    let per_unit: i64 = match unit {
        "ms" => 1_000,
        "s" => 1_000_000,
        "m" => 60_000_000,
        "h" => 3_600_000_000,
        _ => return Err(bad()),
    };
    let us = value.checked_mul(per_unit).ok_or_else(bad)?;
    if us <= 0 {
        return Err(syn::Error::new(
            span,
            "`expire_after` must be a positive duration (DMX-011)",
        ));
    }
    Ok(us)
}

/// A `name = "literal"` argument value.
fn meta_value_str(nv: &syn::MetaNameValue) -> Option<String> {
    match &nv.value {
        Expr::Lit(syn::ExprLit {
            lit: Lit::Str(s), ..
        }) => Some(s.value()),
        _ => None,
    }
}

/// `#[normalize(money, scale = N[, currency = "ISO"])]` ·
/// `#[normalize(datetime)]` ·
/// `#[normalize(string[, form = nfc|nfkc][, case = fold|lower|none][, trim = bool])]`
/// (CT-021..CT-023).
fn parse_transform_normalize(attr: &Attribute) -> syn::Result<TransformDecl> {
    let span = attr.span();
    let metas = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
    let mut iter = metas.iter();
    let kind = match iter.next() {
        Some(Meta::Path(path)) => path
            .get_ident()
            .map(ToString::to_string)
            .unwrap_or_default(),
        _ => String::new(),
    };
    match kind.as_str() {
        "money" => {
            let mut scale: Option<u8> = None;
            let mut currency: Option<String> = None;
            for meta in iter {
                let nv = meta.require_name_value()?;
                if nv.path.is_ident("scale") {
                    let Expr::Lit(syn::ExprLit {
                        lit: Lit::Int(int), ..
                    }) = &nv.value
                    else {
                        return Err(syn::Error::new(
                            nv.span(),
                            "`scale` must be an integer literal (CT-021)",
                        ));
                    };
                    scale = Some(int.base10_parse::<u8>()?);
                } else if nv.path.is_ident("currency") {
                    currency = Some(meta_value_str(nv).ok_or_else(|| {
                        syn::Error::new(
                            nv.span(),
                            "`currency` must be a string literal, e.g. `currency = \"USD\"` \
                             (CT-021)",
                        )
                    })?);
                } else {
                    return Err(syn::Error::new(
                        meta.span(),
                        "unknown `#[normalize(money)]` argument: expected `scale` or `currency` \
                         (CT-021)",
                    ));
                }
            }
            let Some(scale) = scale else {
                return Err(syn::Error::new(
                    span,
                    "`#[normalize(money, scale = N)]` requires `scale` (CT-021)",
                ));
            };
            Ok(TransformDecl::Money {
                scale,
                currency,
                span,
            })
        }
        "datetime" => {
            if iter.next().is_some() {
                return Err(syn::Error::new(
                    span,
                    "`#[normalize(datetime)]` takes no further arguments — `assume_tz` lands \
                     with the timezone-aware parser (CT-022)",
                ));
            }
            Ok(TransformDecl::Datetime { span })
        }
        "string" => {
            let mut form = StrForm::Nfc;
            let mut case = StrCase::None;
            let mut trim = false;
            for meta in iter {
                let nv = meta.require_name_value()?;
                if nv.path.is_ident("form") {
                    form = match meta_value_ident(nv).as_deref() {
                        Some("nfc") => StrForm::Nfc,
                        Some("nfkc") => StrForm::Nfkc,
                        _ => {
                            return Err(syn::Error::new(
                                nv.span(),
                                "`form` must be `nfc` or `nfkc` (CT-023)",
                            ));
                        }
                    };
                } else if nv.path.is_ident("case") {
                    case = match meta_value_ident(nv).as_deref() {
                        Some("fold") => StrCase::Fold,
                        Some("lower") => StrCase::Lower,
                        Some("none") => StrCase::None,
                        _ => {
                            return Err(syn::Error::new(
                                nv.span(),
                                "`case` must be `fold`, `lower`, or `none` (CT-023)",
                            ));
                        }
                    };
                } else if nv.path.is_ident("trim") {
                    let Expr::Lit(syn::ExprLit {
                        lit: Lit::Bool(b), ..
                    }) = &nv.value
                    else {
                        return Err(syn::Error::new(
                            nv.span(),
                            "`trim` must be `true` or `false` (CT-023)",
                        ));
                    };
                    trim = b.value;
                } else {
                    return Err(syn::Error::new(
                        meta.span(),
                        "unknown `#[normalize(string)]` argument: expected `form`, `case`, or \
                         `trim` (CT-023)",
                    ));
                }
            }
            Ok(TransformDecl::Str {
                form,
                case,
                trim,
                span,
            })
        }
        other => Err(syn::Error::new(
            span,
            format!(
                "unknown normalize kind `{other}`: expected `money`, `datetime`, or `string` \
                 (CT-021..CT-023)"
            ),
        )),
    }
}

/// `#[encrypted(ecies, key = "NAME")]` (CT-030).
fn parse_transform_encrypted(attr: &Attribute) -> syn::Result<TransformDecl> {
    let span = attr.span();
    let metas = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
    let mut iter = metas.iter();
    match iter.next() {
        Some(Meta::Path(p)) if p.is_ident("ecies") => {}
        Some(meta) => {
            return Err(syn::Error::new(
                meta.span(),
                format!(
                    "unknown encryption scheme `{}`: expected `ecies` (CT-030)",
                    meta.to_token_stream()
                ),
            ));
        }
        None => {
            return Err(syn::Error::new(
                span,
                "expected `#[encrypted(ecies, key = \"NAME\")]` (CT-030)",
            ));
        }
    }
    let mut key: Option<String> = None;
    for meta in iter {
        let nv = meta.require_name_value()?;
        if nv.path.is_ident("key") {
            key = Some(meta_value_str(nv).ok_or_else(|| {
                syn::Error::new(nv.span(), "`key` must be a string literal (CT-030)")
            })?);
        } else {
            return Err(syn::Error::new(
                meta.span(),
                "unknown `#[encrypted]` argument: expected `key = \"NAME\"` (CT-030)",
            ));
        }
    }
    match key {
        Some(key) if !key.is_empty() => Ok(TransformDecl::Encrypted { key, span }),
        _ => Err(syn::Error::new(
            span,
            "`#[encrypted(ecies, key = \"NAME\")]` requires a non-empty key name (CT-030/CT-035)",
        )),
    }
}

/// `#[signed(ed25519, by = server | <identity column>)]` (CT-033).
fn parse_transform_signed(attr: &Attribute) -> syn::Result<TransformDecl> {
    let span = attr.span();
    let metas = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
    let mut iter = metas.iter();
    match iter.next() {
        Some(Meta::Path(p)) if p.is_ident("ed25519") => {}
        Some(meta) => {
            return Err(syn::Error::new(
                meta.span(),
                format!(
                    "unknown signature scheme `{}`: expected `ed25519` (CT-033)",
                    meta.to_token_stream()
                ),
            ));
        }
        None => {
            return Err(syn::Error::new(
                span,
                "expected `#[signed(ed25519, by = server | <column>)]` (CT-033)",
            ));
        }
    }
    let mut by: Option<SignedByDecl> = None;
    for meta in iter {
        let nv = meta.require_name_value()?;
        if nv.path.is_ident("by") {
            let Expr::Path(p) = &nv.value else {
                return Err(syn::Error::new(
                    nv.span(),
                    "`by` must be `server` or an `Identity` column name (CT-033)",
                ));
            };
            let Some(ident) = p.path.get_ident() else {
                return Err(syn::Error::new(
                    nv.span(),
                    "`by` must be `server` or an `Identity` column name (CT-033)",
                ));
            };
            by = Some(if ident == "server" {
                SignedByDecl::Server
            } else {
                SignedByDecl::Column(ident.clone())
            });
        } else {
            return Err(syn::Error::new(
                meta.span(),
                "unknown `#[signed]` argument: expected `by = server | <column>` (CT-033)",
            ));
        }
    }
    let Some(by) = by else {
        return Err(syn::Error::new(
            span,
            "`#[signed(ed25519, by = ...)]` requires `by` (CT-033)",
        ));
    };
    Ok(TransformDecl::Signed { by, span })
}

/// `#[masked(null | redact | ciphertext | hash)]` (CT-041).
fn parse_transform_masked(attr: &Attribute) -> syn::Result<TransformDecl> {
    let span = attr.span();
    let ident: Ident = attr.parse_args()?;
    let strategy = match ident.to_string().as_str() {
        "null" => MaskDecl::Null,
        "redact" => MaskDecl::Redact,
        "ciphertext" => MaskDecl::Ciphertext,
        "hash" => MaskDecl::Hash,
        other => {
            return Err(syn::Error::new(
                ident.span(),
                format!(
                    "unknown mask strategy `{other}`: expected `null`, `redact`, `ciphertext`, \
                     or `hash` (CT-041)"
                ),
            ));
        }
    };
    Ok(TransformDecl::Masked { strategy, span })
}

/// `#[column_grant(select = public | owner | server_peer | "role")]` (CT-040).
fn parse_transform_column_grant(attr: &Attribute) -> syn::Result<TransformDecl> {
    let span = attr.span();
    let meta: Meta = attr.parse_args()?;
    let nv = meta.require_name_value()?;
    if !nv.path.is_ident("select") {
        return Err(syn::Error::new(
            meta.span(),
            "expected `#[column_grant(select = public | owner | server_peer | \"role\")]` \
             (CT-040)",
        ));
    }
    let scope = if let Some(role) = meta_value_str(nv) {
        if role.is_empty() {
            return Err(syn::Error::new(
                nv.span(),
                "role name must be non-empty (CT-040)",
            ));
        }
        GrantDecl::Role(role)
    } else {
        match meta_value_ident(nv).as_deref() {
            Some("public") => GrantDecl::Public,
            Some("owner") => GrantDecl::Owner,
            Some("server_peer") => GrantDecl::ServerPeer,
            _ => {
                return Err(syn::Error::new(
                    nv.span(),
                    "`select` must be `public`, `owner`, `server_peer`, or a \"role\" string \
                     (CT-040)",
                ));
            }
        }
    };
    Ok(TransformDecl::Grant { scope, span })
}

// ---------------------------------------------------------------------------
// Type mapping (SPEC-001 §3)
// ---------------------------------------------------------------------------

/// Map a field type to the closed column type universe; anything else —
/// including maps and nested table structs — is a compile error (DM-012).
pub(crate) fn parse_flux_type(ty: &Type) -> syn::Result<FluxTy> {
    let unsupported = || {
        syn::Error::new(
            ty.span(),
            format!(
                "unsupported column type `{}`: column types are the SPEC-001 §3 universe \
                 (bool, i8..i64, u8..u64, f32/f64, String, Vec<u8>, Identity, ConnectionId, \
                 EntityId, Timestamp, Option<T>, Vec<T>) or a `#[derive(FluxType)]` enum/struct \
                 (SPEC-023 DMX-030)",
                ty.to_token_stream()
            ),
        )
    };

    let Type::Path(path) = ty else {
        return Err(unsupported());
    };
    if path.qself.is_some() {
        return Err(unsupported());
    }
    let Some(segment) = path.path.segments.last() else {
        return Err(unsupported());
    };

    let simple = |flux: FluxTy| -> syn::Result<FluxTy> {
        if segment.arguments.is_none() {
            Ok(flux)
        } else {
            Err(unsupported())
        }
    };

    match segment.ident.to_string().as_str() {
        "bool" => simple(FluxTy::Bool),
        "i8" => simple(FluxTy::I8),
        "i16" => simple(FluxTy::I16),
        "i32" => simple(FluxTy::I32),
        "i64" => simple(FluxTy::I64),
        "u8" => simple(FluxTy::U8),
        "u16" => simple(FluxTy::U16),
        "u32" => simple(FluxTy::U32),
        "u64" => simple(FluxTy::U64),
        "f32" => simple(FluxTy::F32),
        "f64" => simple(FluxTy::F64),
        "String" => simple(FluxTy::Str),
        "Identity" => simple(FluxTy::Identity),
        "ConnectionId" => simple(FluxTy::ConnectionId),
        "EntityId" => simple(FluxTy::EntityId),
        "Timestamp" => simple(FluxTy::Timestamp),
        "Decimal" => simple(FluxTy::Decimal),
        "BlobRef" => simple(FluxTy::Blob),
        "CrdtText" => simple(FluxTy::CrdtText),
        "Vec" => {
            let inner = generic_inner(&segment.arguments).ok_or_else(unsupported)?;
            let inner = parse_flux_type(inner)?;
            if matches!(inner, FluxTy::U8) {
                Ok(FluxTy::Bytes)
            } else {
                Ok(FluxTy::List(Box::new(inner)))
            }
        }
        "Option" => {
            let inner = generic_inner(&segment.arguments).ok_or_else(unsupported)?;
            Ok(FluxTy::Opt(Box::new(parse_flux_type(inner)?)))
        }
        "HashMap" | "BTreeMap" => Err(syn::Error::new(
            ty.span(),
            "map types are not valid column types (DM-012): model the relationship with \
             a separate table keyed by an EntityId/u64 column",
        )),
        // Any other path type is taken to be a `#[derive(FluxType)]` enum or
        // nested struct (SPEC-023 DMX-030); generated code carries a
        // `FluxTypeDef` bound, so a type that does not derive it fails with a
        // clear trait-bound error at the use site.
        _ => Ok(FluxTy::Derived(Box::new(ty.clone()))),
    }
}

// ---------------------------------------------------------------------------
// Typed ⇄ dynamic row conversion codegen (DM-043, SPEC-004 T3.2)
// ---------------------------------------------------------------------------

/// An expression converting `expr` (a field value of type `flux`, by value)
/// into the matching `fluxum_core::store::RowValue` variant. Recursive for
/// `Option<T>` / `Vec<T>`.
pub(crate) fn to_row_value(flux: &FluxTy, expr: TokenStream) -> TokenStream {
    let rv = quote!(::fluxum_core::store::RowValue);
    match flux {
        FluxTy::Bool => quote!(#rv::Bool(#expr)),
        FluxTy::I8 => quote!(#rv::I8(#expr)),
        FluxTy::I16 => quote!(#rv::I16(#expr)),
        FluxTy::I32 => quote!(#rv::I32(#expr)),
        FluxTy::I64 => quote!(#rv::I64(#expr)),
        FluxTy::U8 => quote!(#rv::U8(#expr)),
        FluxTy::U16 => quote!(#rv::U16(#expr)),
        FluxTy::U32 => quote!(#rv::U32(#expr)),
        FluxTy::U64 => quote!(#rv::U64(#expr)),
        FluxTy::F32 => quote!(#rv::F32(#expr)),
        FluxTy::F64 => quote!(#rv::F64(#expr)),
        FluxTy::Str => quote!(#rv::Str(#expr)),
        FluxTy::Bytes => quote!(#rv::Bytes(#expr)),
        FluxTy::Identity => quote!(#rv::Identity(#expr)),
        FluxTy::ConnectionId => quote!(#rv::ConnectionId(#expr)),
        FluxTy::EntityId => quote!(#rv::EntityId(#expr)),
        FluxTy::Timestamp => quote!(#rv::Timestamp(#expr)),
        FluxTy::Decimal => quote!(#rv::Decimal(#expr)),
        FluxTy::Blob => quote!(#rv::Blob(#expr)),
        FluxTy::Opt(inner) => {
            let inner = to_row_value(inner, quote!(__fx_inner));
            quote! {
                match #expr {
                    ::core::option::Option::Some(__fx_inner) => #rv::Optional(
                        ::core::option::Option::Some(::std::boxed::Box::new(#inner)),
                    ),
                    ::core::option::Option::None => #rv::Optional(::core::option::Option::None),
                }
            }
        }
        FluxTy::List(inner) => {
            let inner = to_row_value(inner, quote!(__fx_item));
            quote! {
                #rv::List(#expr.into_iter().map(|__fx_item| #inner).collect())
            }
        }
        FluxTy::Derived(_) => {
            quote!(::fluxum_core::schema::FluxTypeDef::to_row_value(#expr))
        }
        // DMX-060: stored as the tagged state encoding.
        FluxTy::CrdtText => quote!(#rv::Bytes(#expr.to_bytes())),
    }
}

/// An expression extracting a typed field value from `src`
/// (a `&fluxum_core::store::RowValue`), cloning payloads out of the shared
/// row. A variant mismatch `return`s a descriptive `FluxumError::Storage`
/// from the enclosing `from_values` — unreachable for rows the store
/// accepted, but never a panic (RED-061 keeps the reducer path unwind-free).
pub(crate) fn from_row_value(
    flux: &FluxTy,
    src: TokenStream,
    table: &str,
    column: &str,
) -> TokenStream {
    let rv = quote!(::fluxum_core::store::RowValue);
    let mismatch = quote! {
        return ::core::result::Result::Err(::fluxum_core::FluxumError::Storage(
            ::std::format!(
                "table `{}`: column `{}` does not inhabit its declared column type (DM-043)",
                #table,
                #column,
            ),
        ))
    };
    let copied = |variant: TokenStream| {
        quote! {
            match #src {
                #rv::#variant(__fx_v) => *__fx_v,
                _ => #mismatch,
            }
        }
    };
    let cloned = |variant: TokenStream| {
        quote! {
            match #src {
                #rv::#variant(__fx_v) => ::core::clone::Clone::clone(__fx_v),
                _ => #mismatch,
            }
        }
    };
    match flux {
        FluxTy::Bool => copied(quote!(Bool)),
        FluxTy::I8 => copied(quote!(I8)),
        FluxTy::I16 => copied(quote!(I16)),
        FluxTy::I32 => copied(quote!(I32)),
        FluxTy::I64 => copied(quote!(I64)),
        FluxTy::U8 => copied(quote!(U8)),
        FluxTy::U16 => copied(quote!(U16)),
        FluxTy::U32 => copied(quote!(U32)),
        FluxTy::U64 => copied(quote!(U64)),
        FluxTy::F32 => copied(quote!(F32)),
        FluxTy::F64 => copied(quote!(F64)),
        FluxTy::Str => cloned(quote!(Str)),
        FluxTy::Bytes => cloned(quote!(Bytes)),
        FluxTy::Identity => copied(quote!(Identity)),
        FluxTy::ConnectionId => copied(quote!(ConnectionId)),
        FluxTy::EntityId => copied(quote!(EntityId)),
        FluxTy::Timestamp => copied(quote!(Timestamp)),
        FluxTy::Decimal => copied(quote!(Decimal)),
        FluxTy::Blob => copied(quote!(Blob)),
        FluxTy::Opt(inner) => {
            let inner = from_row_value(inner, quote!((&**__fx_opt)), table, column);
            quote! {
                match #src {
                    #rv::Optional(::core::option::Option::None) => ::core::option::Option::None,
                    #rv::Optional(::core::option::Option::Some(__fx_opt)) => {
                        ::core::option::Option::Some(#inner)
                    }
                    _ => #mismatch,
                }
            }
        }
        FluxTy::List(inner) => {
            let inner = from_row_value(inner, quote!(__fx_item), table, column);
            quote! {
                match #src {
                    #rv::List(__fx_items) => {
                        let mut __fx_out = ::std::vec::Vec::with_capacity(__fx_items.len());
                        for __fx_item in __fx_items {
                            __fx_out.push(#inner);
                        }
                        __fx_out
                    }
                    _ => #mismatch,
                }
            }
        }
        FluxTy::Derived(ty) => {
            quote! {
                match <#ty as ::fluxum_core::schema::FluxTypeDef>::from_row_value(#src) {
                    ::core::result::Result::Ok(__fx_v) => __fx_v,
                    ::core::result::Result::Err(_) => #mismatch,
                }
            }
        }
        FluxTy::CrdtText => {
            quote! {
                match #src {
                    #rv::Bytes(__fx_bytes) => {
                        match ::fluxum_core::crdt::CrdtText::from_bytes(__fx_bytes) {
                            ::core::result::Result::Ok(__fx_doc) => __fx_doc,
                            ::core::result::Result::Err(_) => #mismatch,
                        }
                    }
                    _ => #mismatch,
                }
            }
        }
    }
}

/// The single `T` of `Vec<T>` / `Option<T>`.
fn generic_inner(arguments: &PathArguments) -> Option<&Type> {
    let PathArguments::AngleBracketed(args) = arguments else {
        return None;
    };
    if args.args.len() != 1 {
        return None;
    }
    match args.args.first() {
        Some(GenericArgument::Type(ty)) => Some(ty),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    //! Validation and codegen of `#[fluxum::table]`, probed on the expansion
    //! functions directly (the trybuild UI suite pins the end-to-end
    //! compile-fail rendering, but runs outside coverage instrumentation).
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use quote::quote;

    fn expand_ok(args: TokenStream, input: TokenStream) -> String {
        try_expand(args, input)
            .expect("expansion must succeed")
            .to_string()
    }

    fn expand_err(args: TokenStream, input: TokenStream) -> String {
        try_expand(args, input)
            .expect_err("expansion must fail")
            .to_string()
    }

    /// A minimal valid table body reused by argument-level tests.
    fn simple_table() -> TokenStream {
        quote! {
            struct Task {
                #[primary_key]
                id: u64,
                title: String,
            }
        }
    }

    // -- entry point ----------------------------------------------------------

    #[test]
    fn expand_renders_failures_as_compile_error() {
        let out = expand(
            TokenStream::new(),
            quote!(
                struct Broken;
            ),
        )
        .to_string();
        assert!(out.contains("compile_error !"), "{out}");
        assert!(out.contains("named fields"), "{out}");
    }

    #[test]
    fn minimal_table_expands_schema_and_registration() {
        let out = expand_ok(TokenStream::new(), simple_table());
        assert!(out.contains("TableSchema"), "{out}");
        assert!(out.contains("TableDef"), "{out}");
        assert!(out.contains("inventory :: submit"), "{out}");
        assert!(out.contains("from_values"), "{out}");
    }

    // -- struct shape -----------------------------------------------------------

    #[test]
    fn rejects_generic_structs_tuple_unit_and_empty_structs() {
        let err = expand_err(
            TokenStream::new(),
            quote! {
                struct T<A> {
                    #[primary_key]
                    id: u64,
                    a: A,
                }
            },
        );
        assert!(err.contains("generic structs (DM-001)"), "{err}");

        let err = expand_err(
            TokenStream::new(),
            quote!(
                struct T(u64);
            ),
        );
        assert!(err.contains("named fields (DM-001)"), "{err}");

        let err = expand_err(
            TokenStream::new(),
            quote!(
                struct T;
            ),
        );
        assert!(err.contains("named fields (DM-001)"), "{err}");

        let err = expand_err(
            TokenStream::new(),
            quote!(
                struct T {}
            ),
        );
        assert!(err.contains("at least one column (DM-001)"), "{err}");
    }

    // -- table arguments ----------------------------------------------------------

    #[test]
    fn access_arguments_expand_and_conflict() {
        let out = expand_ok(quote!(global), simple_table());
        assert!(out.contains("TableAccess :: Global"), "{out}");

        let out = expand_ok(quote!(ephemeral), simple_table());
        assert!(out.contains("TableAccess :: Ephemeral"), "{out}");

        let err = expand_err(quote!(public, private), simple_table());
        assert!(err.contains("at most one of"), "{err}");

        let err = expand_err(quote!(fancy), simple_table());
        assert!(err.contains("unknown #[fluxum::table] argument"), "{err}");
    }

    #[test]
    fn table_level_primary_key_argument_is_validated() {
        let no_field_pk = quote! {
            struct T {
                id: u64,
                region: u32,
            }
        };
        let err = expand_err(quote!(primary_key()), no_field_pk.clone());
        assert!(err.contains("at least one column (DM-003)"), "{err}");

        let err = expand_err(
            quote!(primary_key(id), primary_key(region)),
            no_field_pk.clone(),
        );
        assert!(err.contains("duplicate `primary_key(...)`"), "{err}");

        let err = expand_err(quote!(primary_key(missing)), no_field_pk.clone());
        assert!(err.contains("unknown column `missing`"), "{err}");

        let err = expand_err(quote!(primary_key(id, id)), no_field_pk.clone());
        assert!(err.contains("lists column `id` twice"), "{err}");

        let err = expand_err(quote!(primary_key(title)), simple_table());
        assert!(err.contains("declare exactly one (DM-003)"), "{err}");

        let err = expand_err(TokenStream::new(), no_field_pk);
        assert!(err.contains("no primary key"), "{err}");

        let err = expand_err(
            TokenStream::new(),
            quote! {
                struct T {
                    #[primary_key]
                    a: u64,
                    #[primary_key]
                    b: u64,
                }
            },
        );
        assert!(err.contains("duplicate `#[primary_key]`"), "{err}");
    }

    #[test]
    fn partition_by_is_validated() {
        let table = quote! {
            struct T {
                #[primary_key]
                id: u64,
                region: u32,
            }
        };
        let out = expand_ok(quote!(public, partition_by(region)), table.clone());
        assert!(
            out.contains("partition_by : :: core :: option :: Option :: Some"),
            "{out}"
        );

        let err = expand_err(
            quote!(partition_by(region), partition_by(region)),
            table.clone(),
        );
        assert!(err.contains("duplicate `partition_by(...)`"), "{err}");

        let err = expand_err(quote!(global, partition_by(region)), table);
        assert!(err.contains("cannot be combined with `global`"), "{err}");
    }

    #[test]
    fn expire_after_argument_is_validated() {
        let err = expand_err(quote!(expire_after = "1s"), simple_table());
        assert!(err.contains("only valid on an `ephemeral` table"), "{err}");

        let err = expand_err(
            quote!(ephemeral, expire_after = "1s", expire_after = "2s"),
            simple_table(),
        );
        assert!(err.contains("duplicate `expire_after`"), "{err}");

        let err = expand_err(quote!(ephemeral, expire_after = 5), simple_table());
        assert!(err.contains("must be a duration string"), "{err}");

        let out = expand_ok(quote!(ephemeral, expire_after = "10s"), simple_table());
        assert!(out.contains("EphemeralDef"), "{out}");
        assert!(out.contains("Some (10000000i64)"), "{out}");
        assert!(
            out.contains("owner : :: core :: option :: Option :: None"),
            "{out}"
        );
    }

    #[test]
    fn duration_parsing_accepts_all_units_and_rejects_bad_input() {
        let span = Span::call_site();
        assert_eq!(parse_duration_us("500ms", span).unwrap(), 500_000);
        assert_eq!(parse_duration_us("10s", span).unwrap(), 10_000_000);
        assert_eq!(parse_duration_us("5m", span).unwrap(), 300_000_000);
        assert_eq!(parse_duration_us("2h", span).unwrap(), 7_200_000_000);

        for bad in ["10", "abc", "10d", "9223372036854775807h"] {
            let err = parse_duration_us(bad, span).unwrap_err().to_string();
            assert!(err.contains("invalid duration"), "{bad}: {err}");
        }
        let err = parse_duration_us("0s", span).unwrap_err().to_string();
        assert!(err.contains("positive duration"), "{err}");
    }

    // -- #[owner] / ephemeral metadata (SPEC-023 DMX-011) -----------------------

    #[test]
    fn owner_column_is_validated_and_registered() {
        let owned = quote! {
            struct Presence {
                #[primary_key]
                id: u64,
                #[owner]
                conn: ConnectionId,
            }
        };
        let out = expand_ok(quote!(ephemeral), owned.clone());
        assert!(out.contains("EphemeralDef"), "{out}");
        assert!(
            out.contains("owner : :: core :: option :: Option :: Some (1u16)"),
            "{out}"
        );
        assert!(
            out.contains("expire_after_us : :: core :: option :: Option :: None"),
            "{out}"
        );

        let out = expand_ok(quote!(ephemeral, expire_after = "500ms"), owned.clone());
        assert!(out.contains("Some (1u16)"), "{out}");
        assert!(out.contains("Some (500000i64)"), "{out}");

        let err = expand_err(TokenStream::new(), owned);
        assert!(err.contains("only valid on an `ephemeral` table"), "{err}");

        let err = expand_err(
            quote!(ephemeral),
            quote! {
                struct T {
                    #[primary_key]
                    id: u64,
                    #[owner]
                    a: ConnectionId,
                    #[owner]
                    b: ConnectionId,
                }
            },
        );
        assert!(err.contains("at most one `#[owner]`"), "{err}");

        let err = expand_err(
            quote!(ephemeral),
            quote! {
                struct T {
                    #[primary_key]
                    id: u64,
                    #[owner]
                    conn: u32,
                }
            },
        );
        assert!(err.contains("must be of type `ConnectionId`"), "{err}");
    }

    // -- unique / auto_inc --------------------------------------------------------

    #[test]
    fn unique_constraints_are_validated() {
        let out = expand_ok(
            TokenStream::new(),
            quote! {
                #[unique(title)]
                struct T {
                    #[primary_key]
                    id: u64,
                    title: String,
                }
            },
        );
        assert!(out.contains("unique : & [& [1u16]]"), "{out}");

        let err = expand_err(
            TokenStream::new(),
            quote! {
                #[unique()]
                struct T {
                    #[primary_key]
                    id: u64,
                }
            },
        );
        assert!(err.contains("at least one column (DM-006)"), "{err}");

        let err = expand_err(
            TokenStream::new(),
            quote! {
                #[unique(missing)]
                struct T {
                    #[primary_key]
                    id: u64,
                }
            },
        );
        assert!(err.contains("unknown column `missing`"), "{err}");
    }

    #[test]
    fn auto_inc_is_validated() {
        let err = expand_err(
            TokenStream::new(),
            quote! {
                struct T {
                    #[primary_key]
                    #[auto_inc]
                    a: u64,
                    #[auto_inc]
                    b: u64,
                }
            },
        );
        assert!(err.contains("duplicate `#[auto_inc]`"), "{err}");

        let err = expand_err(
            quote!(primary_key(a, b)),
            quote! {
                struct T {
                    #[auto_inc]
                    a: u64,
                    b: u64,
                }
            },
        );
        assert!(err.contains("composite primary keys"), "{err}");

        let err = expand_err(
            TokenStream::new(),
            quote! {
                struct T {
                    #[primary_key]
                    id: u64,
                    #[auto_inc]
                    n: u64,
                }
            },
        );
        assert!(
            err.contains("only valid on the `#[primary_key]` field"),
            "{err}"
        );

        let err = expand_err(
            TokenStream::new(),
            quote! {
                struct T {
                    #[primary_key]
                    #[auto_inc]
                    id: u32,
                }
            },
        );
        assert!(err.contains("to be `u64` (DM-004)"), "{err}");
    }

    // -- indexes --------------------------------------------------------------------

    #[test]
    fn btree_index_declarations_are_validated() {
        let err = expand_err(
            TokenStream::new(),
            quote! {
                #[index(hash(title))]
                struct T {
                    #[primary_key]
                    id: u64,
                    title: String,
                }
            },
        );
        assert!(
            err.contains("expected `#[index(btree(col, ...))]`"),
            "{err}"
        );

        let err = expand_err(
            TokenStream::new(),
            quote! {
                #[index(btree())]
                struct T {
                    #[primary_key]
                    id: u64,
                }
            },
        );
        assert!(
            err.contains("`btree(...)` needs at least one column"),
            "{err}"
        );

        let err = expand_err(
            TokenStream::new(),
            quote! {
                #[index(btree(price))]
                struct T {
                    #[primary_key]
                    id: u64,
                    price: Decimal,
                }
            },
        );
        assert!(err.contains("cannot yet be a B-tree index key"), "{err}");

        let err = expand_err(
            TokenStream::new(),
            quote! {
                #[index(btree(title))]
                #[index(btree(title))]
                struct T {
                    #[primary_key]
                    id: u64,
                    title: String,
                }
            },
        );
        assert!(err.contains("duplicate `btree` index"), "{err}");
    }

    #[test]
    fn spatial_index_declarations_are_validated() {
        let out = expand_ok(
            TokenStream::new(),
            quote! {
                #[spatial(rtree(ax, ay, bx, by))]
                struct Zone {
                    #[primary_key]
                    id: u64,
                    ax: f64,
                    ay: f64,
                    bx: f64,
                    by: f64,
                }
            },
        );
        assert!(out.contains("SpatialKind :: RTree"), "{out}");

        let err = expand_err(
            TokenStream::new(),
            quote! {
                #[spatial(quadtree(x, y))]
                #[spatial(rtree(ax, ay, bx, by))]
                struct T {
                    #[primary_key]
                    id: u64,
                    x: f32,
                    y: f32,
                    ax: f64,
                    ay: f64,
                    bx: f64,
                    by: f64,
                }
            },
        );
        assert!(err.contains("both `quadtree` and `rtree`"), "{err}");

        let err = expand_err(
            TokenStream::new(),
            quote! {
                #[spatial(quadtree(x, y))]
                struct T {
                    #[primary_key]
                    id: u64,
                    x: u32,
                    y: f32,
                }
            },
        );
        assert!(err.contains("must be `f32` or `f64` (DM-032)"), "{err}");

        let err = expand_err(
            TokenStream::new(),
            quote! {
                #[spatial(kdtree(x, y))]
                struct T {
                    #[primary_key]
                    id: u64,
                    x: f32,
                    y: f32,
                }
            },
        );
        assert!(
            err.contains("expected `#[spatial(quadtree(x, y))]`"),
            "{err}"
        );

        let err = expand_err(
            TokenStream::new(),
            quote! {
                #[spatial(quadtree(x))]
                struct T {
                    #[primary_key]
                    id: u64,
                    x: f32,
                }
            },
        );
        assert!(err.contains("exactly 2 coordinate columns"), "{err}");
    }

    // -- visibility ------------------------------------------------------------------

    #[test]
    fn visibility_rules_expand_and_are_validated() {
        let base = |vis: TokenStream| {
            quote! {
                #[visibility(#vis)]
                struct T {
                    #[primary_key]
                    id: u64,
                    who: Identity,
                    name: String,
                }
            }
        };
        let out = expand_ok(TokenStream::new(), base(quote!(public_all)));
        assert!(out.contains("VisibilityRule :: PublicAll"), "{out}");

        let out = expand_ok(TokenStream::new(), base(quote!(shard_local)));
        assert!(out.contains("VisibilityRule :: ShardLocal"), "{out}");

        let out = expand_ok(TokenStream::new(), base(quote!(custom(my_filter))));
        assert!(
            out.contains("VisibilityRule :: Custom (\"my_filter\")"),
            "{out}"
        );

        let err = expand_err(TokenStream::new(), base(quote!(owner_only(name))));
        assert!(err.contains("must be of type `Identity` (DM-060)"), "{err}");

        let err = expand_err(TokenStream::new(), base(quote!(nope)));
        assert!(err.contains("(DM-061)"), "{err}");

        let err = expand_err(
            TokenStream::new(),
            quote! {
                #[visibility(public_all)]
                #[visibility(shard_local)]
                struct T {
                    #[primary_key]
                    id: u64,
                }
            },
        );
        assert!(err.contains("duplicate #[visibility]"), "{err}");
    }

    // -- field attributes ---------------------------------------------------------------

    #[test]
    fn field_level_misuse_is_rejected() {
        let err = expand_err(
            TokenStream::new(),
            quote! {
                struct T {
                    #[primary_key]
                    id: u64,
                    #[unique(title)]
                    title: String,
                }
            },
        );
        assert!(err.contains("table-level attribute"), "{err}");

        let err = expand_err(
            TokenStream::new(),
            quote! {
                struct T {
                    #[primary_key]
                    id: u64,
                    #[default(1u32)]
                    #[default(2u32)]
                    n: u32,
                }
            },
        );
        assert!(err.contains("duplicate `#[default]`"), "{err}");

        let err = expand_err(
            TokenStream::new(),
            quote! {
                struct T {
                    #[primary_key]
                    id: u64,
                    #[default]
                    n: u32,
                }
            },
        );
        assert!(err.contains("with the backfill value"), "{err}");

        let err = expand_err(
            TokenStream::new(),
            quote! {
                struct T {
                    #[primary_key]
                    id: u64,
                    #[rename(from = "a")]
                    #[rename(from = "b")]
                    n: u32,
                }
            },
        );
        assert!(err.contains("duplicate `#[rename]`"), "{err}");
    }

    #[test]
    fn rename_from_is_validated() {
        let with_rename = |args: TokenStream| {
            quote! {
                struct T {
                    #[primary_key]
                    id: u64,
                    #[rename(#args)]
                    n: u32,
                }
            }
        };
        // Malformed argument shapes all render the usage error.
        for bad in [
            quote!(from(x)),
            quote!(to = "old"),
            quote!(from = old),
            quote!(from = 2),
        ] {
            let err = expand_err(TokenStream::new(), with_rename(bad.clone()));
            assert!(
                err.contains("expected `#[rename(from = \"old_name\")]`"),
                "{bad}: {err}"
            );
        }
        let err = expand_err(TokenStream::new(), with_rename(quote!(from = "")));
        assert!(err.contains("non-empty column name"), "{err}");

        // Consistency: self-rename, still-declared source, duplicate source.
        let err = expand_err(
            TokenStream::new(),
            quote! {
                struct T {
                    #[primary_key]
                    id: u64,
                    #[rename(from = "n")]
                    n: u32,
                }
            },
        );
        assert!(err.contains("names the field itself"), "{err}");

        let err = expand_err(
            TokenStream::new(),
            quote! {
                struct T {
                    #[primary_key]
                    id: u64,
                    #[rename(from = "other")]
                    n: u32,
                    other: u32,
                }
            },
        );
        assert!(err.contains("still declared"), "{err}");

        let err = expand_err(
            TokenStream::new(),
            quote! {
                struct T {
                    #[primary_key]
                    id: u64,
                    #[rename(from = "old")]
                    a: u32,
                    #[rename(from = "old")]
                    b: u32,
                }
            },
        );
        assert!(err.contains("two columns declare"), "{err}");
    }

    // -- rich types (SPEC-023) ------------------------------------------------------------

    #[test]
    fn derived_columns_expand_but_cannot_be_keys() {
        let out = expand_ok(
            TokenStream::new(),
            quote! {
                struct T {
                    #[primary_key]
                    id: u64,
                    status: TaskStatus,
                }
            },
        );
        assert!(out.contains("FluxTypeDef"), "{out}");

        let err = expand_err(
            TokenStream::new(),
            quote! {
                struct T {
                    #[primary_key]
                    status: TaskStatus,
                }
            },
        );
        assert!(err.contains("rich types support"), "{err}");
    }

    // -- column transforms (SPEC-017) ------------------------------------------------------

    #[test]
    fn duplicate_transform_families_are_rejected() {
        // The second attribute of each family drives the CT-002 duplicate
        // error (and TransformDecl::span for every variant).
        let pairs: Vec<(TokenStream, TokenStream)> = vec![
            (
                quote!(normalize(money, scale = 2)),
                quote!(normalize(money, scale = 2)),
            ),
            (
                quote!(normalize(money, scale = 2)),
                quote!(normalize(datetime)),
            ),
            (
                quote!(normalize(money, scale = 2)),
                quote!(normalize(string)),
            ),
            (
                quote!(encrypted(ecies, key = "a")),
                quote!(encrypted(ecies, key = "b")),
            ),
            (
                quote!(signed(ed25519, by = server)),
                quote!(signed(ed25519, by = server)),
            ),
            (quote!(masked(null)), quote!(masked(redact))),
            (
                quote!(column_grant(select = public)),
                quote!(column_grant(select = owner)),
            ),
        ];
        for (first, second) in pairs {
            let err = expand_err(
                TokenStream::new(),
                quote! {
                    struct T {
                        #[primary_key]
                        id: u64,
                        #[#first]
                        #[#second]
                        x: Decimal,
                    }
                },
            );
            assert!(err.contains("(CT-002)"), "{first} + {second}: {err}");
        }
    }

    #[test]
    fn transform_pipeline_expands_in_canonical_order() {
        let out = expand_ok(
            TokenStream::new(),
            quote! {
                struct Payment {
                    #[primary_key]
                    id: u64,
                    #[normalize(money, scale = 2, currency = "USD")]
                    amount: Decimal,
                    #[normalize(money, scale = 4)]
                    fee: Decimal,
                    #[normalize(datetime)]
                    at: Timestamp,
                    #[normalize(string)]
                    plain: String,
                    #[normalize(string, form = nfkc, case = fold, trim = true)]
                    folded: String,
                    #[normalize(string, case = lower)]
                    lowered: String,
                    #[encrypted(ecies, key = "k1")]
                    secret: String,
                    author: Identity,
                    #[signed(ed25519, by = server)]
                    receipt: Vec<u8>,
                    #[signed(ed25519, by = author)]
                    note: String,
                    #[masked(null)]
                    m_null: String,
                    #[masked(redact)]
                    m_redact: String,
                    #[masked(hash)]
                    m_hash: String,
                    #[encrypted(ecies, key = "k2")]
                    #[masked(ciphertext)]
                    m_cipher: String,
                    #[column_grant(select = public)]
                    g_public: String,
                    #[column_grant(select = owner)]
                    g_owner: String,
                    #[column_grant(select = server_peer)]
                    g_peer: String,
                    #[column_grant(select = "auditor")]
                    g_role: String,
                }
            },
        );
        for expected in [
            "ColumnTransformDef",
            "NormalizeMoney",
            "Some (\"USD\")",
            "NormalizeDatetime",
            "NormalizeString",
            "StringForm :: Nfc",
            "StringForm :: Nfkc",
            "CaseFold :: None",
            "CaseFold :: Fold",
            "CaseFold :: Lower",
            "CryptoScheme :: Ecies",
            "SignScheme :: Ed25519",
            "SignedBy :: Server",
            "SignedBy :: IdentityColumn",
            "MaskStrategy :: Null",
            "MaskStrategy :: Redact",
            "MaskStrategy :: Hash",
            "MaskStrategy :: Ciphertext",
            "GrantScope :: Public",
            "GrantScope :: Owner",
            "GrantScope :: ServerPeer",
            "GrantScope :: Role (\"auditor\")",
        ] {
            assert!(out.contains(expected), "missing {expected}: {out}");
        }
    }

    #[test]
    fn transform_column_type_requirements_are_enforced() {
        let single = |attr: TokenStream, ty: TokenStream| {
            quote! {
                struct T {
                    #[primary_key]
                    id: u64,
                    #[#attr]
                    x: #ty,
                }
            }
        };
        let err = expand_err(
            TokenStream::new(),
            single(quote!(normalize(money, scale = 2)), quote!(String)),
        );
        assert!(err.contains("to be `Decimal`"), "{err}");

        let err = expand_err(
            TokenStream::new(),
            single(quote!(normalize(datetime)), quote!(String)),
        );
        assert!(err.contains("to be `Timestamp`"), "{err}");

        let err = expand_err(
            TokenStream::new(),
            single(quote!(normalize(string)), quote!(u64)),
        );
        assert!(err.contains("to be `String`"), "{err}");

        // CT-013: encrypted columns can never be part of a key/index.
        let err = expand_err(
            TokenStream::new(),
            quote! {
                struct T {
                    #[primary_key]
                    #[encrypted(ecies, key = "k")]
                    id: u64,
                }
            },
        );
        assert!(err.contains("(CT-013)"), "{err}");

        // CT-033: `by` must reference an Identity column.
        let err = expand_err(
            TokenStream::new(),
            quote! {
                struct T {
                    #[primary_key]
                    id: u64,
                    title: String,
                    #[signed(ed25519, by = title)]
                    doc: String,
                }
            },
        );
        assert!(err.contains("must reference an"), "{err}");

        // CT-041: ciphertext masking requires encryption on the same column.
        let err = expand_err(
            TokenStream::new(),
            single(quote!(masked(ciphertext)), quote!(String)),
        );
        assert!(err.contains("(CT-041)"), "{err}");
    }

    #[test]
    fn normalize_attribute_arguments_are_validated() {
        let with_attr = |attr: TokenStream| {
            quote! {
                struct T {
                    #[primary_key]
                    id: u64,
                    #[#attr]
                    x: Decimal,
                }
            }
        };
        let cases: Vec<(TokenStream, &str)> = vec![
            (quote!(normalize()), "unknown normalize kind"),
            (quote!(normalize(base64)), "unknown normalize kind `base64`"),
            (
                quote!(normalize(money, scale = "2")),
                "`scale` must be an integer literal",
            ),
            (
                quote!(normalize(money, scale = 2, currency = usd)),
                "`currency` must be a string literal",
            ),
            (
                quote!(normalize(money, scale = 2, foo = 1)),
                "unknown `#[normalize(money)]` argument",
            ),
            (quote!(normalize(money)), "requires `scale`"),
            (
                quote!(normalize(datetime, assume_tz = "utc")),
                "takes no further arguments",
            ),
            (
                quote!(normalize(string, form = weird)),
                "`form` must be `nfc` or `nfkc`",
            ),
            // A literal where an ident is expected (meta_value_ident -> None).
            (
                quote!(normalize(string, form = "nfc")),
                "`form` must be `nfc` or `nfkc`",
            ),
            (
                quote!(normalize(string, case = weird)),
                "`case` must be `fold`, `lower`, or `none`",
            ),
            (
                quote!(normalize(string, trim = "yes")),
                "`trim` must be `true` or `false`",
            ),
            (
                quote!(normalize(string, pad = 4)),
                "unknown `#[normalize(string)]` argument",
            ),
        ];
        for (attr, expected) in cases {
            let err = expand_err(TokenStream::new(), with_attr(attr.clone()));
            assert!(err.contains(expected), "{attr}: {err}");
        }
    }

    #[test]
    fn encrypted_signed_masked_grant_arguments_are_validated() {
        let with_attr = |attr: TokenStream| {
            quote! {
                struct T {
                    #[primary_key]
                    id: u64,
                    #[#attr]
                    x: String,
                }
            }
        };
        let cases: Vec<(TokenStream, &str)> = vec![
            (
                quote!(encrypted(aes256, key = "k")),
                "unknown encryption scheme `aes256`",
            ),
            (quote!(encrypted()), "expected `#[encrypted(ecies"),
            (
                quote!(encrypted(ecies, key = 5)),
                "`key` must be a string literal",
            ),
            (
                quote!(encrypted(ecies, nonce = "n")),
                "unknown `#[encrypted]` argument",
            ),
            (quote!(encrypted(ecies)), "non-empty key name"),
            (quote!(encrypted(ecies, key = "")), "non-empty key name"),
            (
                quote!(signed(rsa, by = server)),
                "unknown signature scheme `rsa`",
            ),
            (quote!(signed()), "expected `#[signed(ed25519"),
            (
                quote!(signed(ed25519, by = "server")),
                "`by` must be `server`",
            ),
            (quote!(signed(ed25519, by = a::b)), "`by` must be `server`"),
            (
                quote!(signed(ed25519, via = server)),
                "unknown `#[signed]` argument",
            ),
            (quote!(signed(ed25519)), "requires `by`"),
            (quote!(masked(zero)), "unknown mask strategy `zero`"),
            (
                quote!(column_grant(insert = public)),
                "expected `#[column_grant(select",
            ),
            (quote!(column_grant(select = "")), "must be non-empty"),
            (
                quote!(column_grant(select = nobody)),
                "`select` must be `public`",
            ),
        ];
        for (attr, expected) in cases {
            let err = expand_err(TokenStream::new(), with_attr(attr.clone()));
            assert!(err.contains(expected), "{attr}: {err}");
        }
    }

    // -- type mapping ----------------------------------------------------------------------

    #[test]
    fn flux_type_universe_maps_and_rejects() {
        let bytes = parse_flux_type(&syn::parse_quote!(Vec<u8>)).unwrap();
        assert!(bytes.tokens().to_string().contains("Bytes"));

        let list = parse_flux_type(&syn::parse_quote!(Vec<i8>)).unwrap();
        assert!(list.tokens().to_string().contains("List"));

        let opt = parse_flux_type(&syn::parse_quote!(Option<i16>)).unwrap();
        assert!(opt.tokens().to_string().contains("Option"));

        let unsupported: Vec<Type> = vec![
            syn::parse_quote!((u32, u32)),
            syn::parse_quote!(<Foo as Bar>::Baz),
            syn::parse_quote!(u32<u8>),
            syn::parse_quote!(Vec),
            syn::parse_quote!(Option<u8, u16>),
            syn::parse_quote!(Option<'static>),
        ];
        for ty in unsupported {
            let err = parse_flux_type(&ty).err().expect("must fail").to_string();
            assert!(err.contains("unsupported column type"), "{err}");
        }

        let maps: [Type; 2] = [
            syn::parse_quote!(HashMap<String, u32>),
            syn::parse_quote!(BTreeMap<String, u32>),
        ];
        for ty in maps {
            let err = parse_flux_type(&ty).err().expect("must fail").to_string();
            assert!(
                err.contains("map types are not valid column types"),
                "{err}"
            );
        }

        // A path type with no segments is impossible to parse but the mapper
        // still rejects it defensively.
        let empty = Type::Path(syn::TypePath {
            qself: None,
            path: syn::Path {
                leading_colon: None,
                segments: syn::punctuated::Punctuated::new(),
            },
        });
        let err = parse_flux_type(&empty)
            .err()
            .expect("must fail")
            .to_string();
        assert!(err.contains("unsupported column type"), "{err}");
    }

    #[test]
    fn row_value_conversions_cover_every_scalar_variant() {
        let cases = [
            (FluxTy::I8, "I8"),
            (FluxTy::I16, "I16"),
            (FluxTy::EntityId, "EntityId"),
        ];
        for (flux, variant) in cases {
            assert!(flux.tokens().to_string().contains(variant));
            assert!(to_row_value(&flux, quote!(v)).to_string().contains(variant));
            assert!(
                from_row_value(&flux, quote!(src), "t", "c")
                    .to_string()
                    .contains(variant)
            );
        }
    }
}
