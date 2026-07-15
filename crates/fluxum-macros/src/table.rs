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

/// Column type from the closed SPEC-001 §3 universe (mirror of
/// `fluxum_core::schema::FluxType`, macro-side).
#[derive(Clone, PartialEq, Eq)]
enum FluxTy {
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
    Opt(Box<FluxTy>),
    List(Box<FluxTy>),
}

impl FluxTy {
    fn is_float(&self) -> bool {
        matches!(self, Self::F32 | Self::F64)
    }

    /// Tokens constructing the matching `fluxum_core::schema::FluxType`
    /// value in const context (nested references rely on static promotion).
    fn tokens(&self) -> TokenStream {
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
            Self::Opt(inner) => {
                let inner = inner.tokens();
                quote!(#path::Option(&#inner))
            }
            Self::List(inner) => {
                let inner = inner.tokens();
                quote!(#path::List(&#inner))
            }
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
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Access {
    Private,
    Public,
    Global,
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
}

struct IndexDecl {
    kind: IndexKind,
    columns: Vec<Ident>,
    span: Span,
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

    let metas = Punctuated::<Meta, Token![,]>::parse_terminated.parse2(args)?;
    for meta in metas {
        let span = meta.span();
        let access_arg = ["private", "public", "global"]
            .iter()
            .position(|name| meta.path().is_ident(name));
        if let Some(which) = access_arg {
            let this = match which {
                1 => Access::Public,
                2 => Access::Global,
                _ => Access::Private,
            };
            if access.is_some() {
                return Err(syn::Error::new(
                    span,
                    "at most one of `public`, `private`, `global` (DM-005/DM-007)",
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
        } else {
            return Err(syn::Error::new(
                span,
                "unknown #[fluxum::table] argument: expected `public`, `private`, `global`, \
                 `primary_key(col, ...)`, or `partition_by(col)` (DM-020)",
            ));
        }
    }
    let access = access.map_or(Access::Private, |(a, _)| a);

    // -- companion struct attributes (stripped from the output) --------------
    let mut unique: Vec<Vec<Ident>> = Vec::new();
    let mut indexes: Vec<IndexDecl> = Vec::new();
    let mut visibility: Option<Visibility> = None;
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
        } else if attr.path().is_ident("visibility") {
            if visibility.is_some() {
                return Err(syn::Error::new(
                    attr.span(),
                    "duplicate #[visibility] attribute",
                ));
            }
            visibility = Some(parse_visibility(&attr)?);
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
        let mut rename_from = None;
        let mut kept: Vec<Attribute> = Vec::new();
        for attr in std::mem::take(&mut field.attrs) {
            if attr.path().is_ident("primary_key") {
                primary_key = Some(attr.span());
            } else if attr.path().is_ident("auto_inc") {
                auto_inc = Some(attr.span());
            } else if attr.path().is_ident("default") {
                if default.is_some() {
                    return Err(syn::Error::new(attr.span(), "duplicate `#[default]`"));
                }
                default = Some(parse_default(&attr)?);
            } else if attr.path().is_ident("rename") {
                if rename_from.is_some() {
                    return Err(syn::Error::new(attr.span(), "duplicate `#[rename]`"));
                }
                rename_from = Some((parse_rename(&attr)?, attr.span()));
            } else if attr.path().is_ident("index")
                || attr.path().is_ident("spatial")
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
        let flux = parse_flux_type(&field.ty)?;
        columns.push(Column {
            ident,
            ty: field.ty.clone(),
            flux,
            primary_key,
            auto_inc,
            default,
            rename_from,
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
            if col.flux != FluxTy::U64 {
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
                _ => ordinal_of(col, "`#[spatial(...)]` (DM-032)"),
            })
            .collect::<syn::Result<_>>()?;

        let (tag, tokens) = match decl.kind {
            IndexKind::BTree => (
                "btree",
                quote!(::fluxum_core::schema::IndexSchema::BTree { columns: &[#(#ords),*] }),
            ),
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
            if columns[usize::from(ord)].flux != FluxTy::Identity {
                return Err(syn::Error::new(
                    col.span(),
                    format!("`owner_only` column `{col}` must be of type `Identity` (DM-060)"),
                ));
            }
            quote!(::fluxum_core::schema::VisibilityRule::OwnerOnly { owner: #ord })
        }
    };

    // -- codegen ------------------------------------------------------------------
    let struct_ident = &item.ident;
    let name_str = struct_ident.to_string();

    let column_tokens = columns.iter().map(|c| {
        let name = c.ident.to_string();
        let ty = c.flux.tokens();
        quote!(::fluxum_core::schema::ColumnSchema { name: #name, ty: #ty })
    });

    let access_tokens = match access {
        Access::Private => quote!(::fluxum_core::schema::TableAccess::Private),
        Access::Public => quote!(::fluxum_core::schema::TableAccess::Public),
        Access::Global => quote!(::fluxum_core::schema::TableAccess::Global),
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

            #migration_meta
        };
    })
}

// ---------------------------------------------------------------------------
// Attribute parsers
// ---------------------------------------------------------------------------

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
// Type mapping (SPEC-001 §3)
// ---------------------------------------------------------------------------

/// Map a field type to the closed column type universe; anything else —
/// including maps and nested table structs — is a compile error (DM-012).
fn parse_flux_type(ty: &Type) -> syn::Result<FluxTy> {
    let unsupported = || {
        syn::Error::new(
            ty.span(),
            format!(
                "unsupported column type `{}`: column types are the closed SPEC-001 §3 \
                 universe (bool, i8..i64, u8..u64, f32/f64, String, Vec<u8>, Identity, \
                 ConnectionId, EntityId, Timestamp, Option<T>, Vec<T>) (DM-010..DM-012)",
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
        "Vec" => {
            let inner = generic_inner(&segment.arguments).ok_or_else(unsupported)?;
            let inner = parse_flux_type(inner)?;
            if inner == FluxTy::U8 {
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
        _ => Err(unsupported()),
    }
}

// ---------------------------------------------------------------------------
// Typed ⇄ dynamic row conversion codegen (DM-043, SPEC-004 T3.2)
// ---------------------------------------------------------------------------

/// An expression converting `expr` (a field value of type `flux`, by value)
/// into the matching `fluxum_core::store::RowValue` variant. Recursive for
/// `Option<T>` / `Vec<T>`.
fn to_row_value(flux: &FluxTy, expr: TokenStream) -> TokenStream {
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
    }
}

/// An expression extracting a typed field value from `src`
/// (a `&fluxum_core::store::RowValue`), cloning payloads out of the shared
/// row. A variant mismatch `return`s a descriptive `FluxumError::Storage`
/// from the enclosing `from_values` — unreachable for rows the store
/// accepted, but never a panic (RED-061 keeps the reducer path unwind-free).
fn from_row_value(flux: &FluxTy, src: TokenStream, table: &str, column: &str) -> TokenStream {
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
