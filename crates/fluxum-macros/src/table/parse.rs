//! Attribute parsers for `#[fluxum::table]` (indexes, visibility, TTL,
//! transforms) and the `#[fluxum::edge]` entry — split from the parent
//! module to honour the file-size convention.

#[allow(clippy::wildcard_imports)]
use super::*;

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
pub(super) fn collect_idents(expr: &Expr) -> std::collections::HashSet<String> {
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

/// Entry point for `#[fluxum::edge]` (SPEC-023 DMX-050): validate the
/// `from`/`to` fields and endpoint arguments, then expand as the equivalent
/// `#[fluxum::table(public, primary_key(from, to))]` with a `btree(from)`
/// neighbor index, plus the link-time `EdgeDef`.
pub fn expand_edge(args: TokenStream, input: TokenStream) -> TokenStream {
    match try_expand_edge(args, input) {
        Ok(tokens) => tokens,
        Err(err) => err.to_compile_error(),
    }
}

pub(super) fn try_expand_edge(args: TokenStream, input: TokenStream) -> syn::Result<TokenStream> {
    use syn::parse::Parser;

    let mut from_table = String::new();
    let mut to_table = String::new();
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("from") {
            let ident: Ident = meta.value()?.parse()?;
            from_table = ident.to_string();
            return Ok(());
        }
        if meta.path.is_ident("to") {
            let ident: Ident = meta.value()?.parse()?;
            to_table = ident.to_string();
            return Ok(());
        }
        Err(meta.error("expected `from = <Table>` / `to = <Table>` (DMX-050)"))
    });
    parser.parse2(args.clone())?;

    let item: syn::ItemStruct = syn::parse2(input)?;
    let has = |name: &str| {
        item.fields
            .iter()
            .any(|f| f.ident.as_ref().is_some_and(|i| i == name))
    };
    if !has("from") || !has("to") {
        return Err(syn::Error::new(
            item.ident.span(),
            "#[fluxum::edge] structs declare `from` and `to` fields (the endpoint keys), \
             plus any property columns (DMX-050)",
        ));
    }
    let name = item.ident.to_string();

    // Delegate to the table expansion with the edge shape imposed.
    let table_args = quote!(public, primary_key(from, to));
    let with_index: TokenStream = quote! {
        #[index(btree(from))]
        #item
    };
    let expanded = try_expand(table_args, with_index)?;
    Ok(quote! {
        #expanded

        ::fluxum_core::schema::inventory::submit! {
            ::fluxum_core::schema::EdgeDef {
                name: #name,
                from_table: #from_table,
                to_table: #to_table,
            }
        }
    })
}

/// `#[references(Parent(col))]` or
/// `#[references(Parent(col), on_delete = restrict|cascade|set_null)]`
/// (SPEC-022 RV-030/032). The referenced column must be the parent's
/// primary key — validated at store assembly, where the parent's schema is
/// in hand.
pub(super) fn parse_references(attr: &Attribute) -> syn::Result<RefDecl> {
    let span = attr.span();
    attr.parse_args_with(|input: syn::parse::ParseStream| {
        let parent: Ident = input.parse()?;
        let content;
        syn::parenthesized!(content in input);
        let parent_column: Ident = content.parse()?;
        if !content.is_empty() {
            return Err(
                content.error("foreign keys reference exactly one column: `Parent(col)` (RV-030)")
            );
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
pub(super) fn parse_default(attr: &Attribute) -> syn::Result<Expr> {
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
pub(super) fn parse_rename(attr: &Attribute) -> syn::Result<String> {
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
pub(super) fn parse_index(attr: &Attribute) -> syn::Result<IndexDecl> {
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
pub(super) fn parse_spatial(attr: &Attribute) -> syn::Result<IndexDecl> {
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
pub(super) fn parse_fulltext(attr: &Attribute) -> syn::Result<IndexDecl> {
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
pub(super) enum TtlForm {
    /// `#[ttl(col)]` — expire when the named `Timestamp` column is past.
    Field(Ident),
    /// `#[ttl(after = "30m")]` — expire N µs after the last write.
    After(i64),
}

/// `#[ttl(col)]` (absolute expiry from a `Timestamp` column) or
/// `#[ttl(after = "30m")]` (sliding TTL since last write) — SPEC-023 DMX-020.
pub(super) fn parse_ttl(attr: &Attribute) -> syn::Result<TtlForm> {
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
pub(super) fn parse_visibility(attr: &Attribute) -> syn::Result<Visibility> {
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
    } else if meta.path().is_ident("member_of") {
        // SPEC-022 RV-040: `member_of(Table, key)`.
        let list = meta.require_list()?;
        let (table, key) = list.parse_args_with(|input: syn::parse::ParseStream| {
            let table: Ident = input.parse()?;
            input.parse::<Token![,]>()?;
            let key: Ident = input.parse()?;
            if !input.is_empty() {
                return Err(input.error("member_of takes exactly (Table, key) (RV-040)"));
            }
            Ok((table, key))
        })?;
        Ok(Visibility::MemberOf { table, key })
    } else {
        Err(syn::Error::new(
            meta.span(),
            "expected `owner_only(col)`, `public_all`, `shard_local`, `custom(filter_fn)`, \
             or `member_of(Table, key)` (DM-061)",
        ))
    }
}

// ---------------------------------------------------------------------------
// Transform attribute parsers (SPEC-017 CT-001/CT-003)
// ---------------------------------------------------------------------------

/// A `name = ident` argument value, as a string.
pub(super) fn meta_value_ident(nv: &syn::MetaNameValue) -> Option<String> {
    match &nv.value {
        Expr::Path(p) => p.path.get_ident().map(ToString::to_string),
        _ => None,
    }
}

/// Parse an `expire_after` duration string — `<int>` + `ms`|`s`|`m`|`h` —
/// into microseconds (DMX-011).
pub(super) fn parse_duration_us(text: &str, span: Span) -> syn::Result<i64> {
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
pub(super) fn meta_value_str(nv: &syn::MetaNameValue) -> Option<String> {
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
pub(super) fn parse_transform_normalize(attr: &Attribute) -> syn::Result<TransformDecl> {
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
pub(super) fn parse_transform_encrypted(attr: &Attribute) -> syn::Result<TransformDecl> {
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
pub(super) fn parse_transform_signed(attr: &Attribute) -> syn::Result<TransformDecl> {
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
pub(super) fn parse_transform_masked(attr: &Attribute) -> syn::Result<TransformDecl> {
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
pub(super) fn parse_transform_column_grant(attr: &Attribute) -> syn::Result<TransformDecl> {
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
pub(super) fn generic_inner(arguments: &PathArguments) -> Option<&Type> {
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
