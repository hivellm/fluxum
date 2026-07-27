//! The `#[fluxum::table]` expansion body — split from the parent module
//! to honour the file-size convention.

#[allow(clippy::wildcard_imports)]
use super::*;

pub(super) fn try_expand(args: TokenStream, input: TokenStream) -> syn::Result<TokenStream> {
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
        Some(Visibility::MemberOf { table, key }) => {
            // The key column must exist HERE; the membership end resolves
            // at schema assembly, where the other table is in hand (RV-040).
            ordinal_of(key, "`#[visibility(member_of(...))]` (RV-040)")?;
            let table = table.to_string();
            let key = key.to_string();
            quote!(::fluxum_core::schema::VisibilityRule::MemberOf {
                table: #table,
                key: #key,
            })
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
                        let extract = from_row_value(
                            sib_flux,
                            quote!((&__fx_values[#idx])),
                            &name_str,
                            &name,
                        );
                        bindings.push(quote!(let #ident = #extract;));
                    }
                }
                let expr_str = expr.to_token_stream().to_string();
                let fn_ident =
                    format_ident!("__fx_check_{}_{}_{}", struct_ident, column.ident, check_idx);
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
                if decl.on_delete == RefActionTok::SetNull && !matches!(column.flux, FluxTy::Opt(_))
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
