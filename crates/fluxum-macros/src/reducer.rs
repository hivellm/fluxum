//! Expansion of the SPEC-004 function attributes (T3.3):
//! `#[fluxum::reducer]` (RED-001/RED-006), the lifecycle hooks
//! `#[fluxum::on_init]` / `#[fluxum::on_shard_start]` /
//! `#[fluxum::on_connect]` / `#[fluxum::on_disconnect]` (RED-010..RED-013),
//! and `#[fluxum::view]` (RED-030/RED-031).
//!
//! Every expansion keeps the annotated function unchanged and submits a def
//! to the matching link-time registry in `fluxum_core::reducer`, exactly
//! like tables (DM-040) and migrations (MIG-010). For reducers the macro
//! additionally generates the RED-001 argument surface from the signature:
//! a pre-transaction check (arity + per-parameter decode) and the dispatch
//! glue — both composed from the same `fluxum_core::reducer::args` helpers,
//! so admission and execution can never disagree about a signature.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{FnArg, ItemFn, Pat, ReturnType};

/// Which lifecycle attribute is being expanded.
#[derive(Clone, Copy)]
pub enum Hook {
    Init,
    ShardStart,
    Connect,
    Disconnect,
}

impl Hook {
    /// The `fluxum_core::reducer::LifecycleKind` variant to submit.
    fn variant(self) -> TokenStream {
        match self {
            Self::Init => quote!(OnInit),
            Self::ShardStart => quote!(OnShardStart),
            Self::Connect => quote!(OnConnect),
            Self::Disconnect => quote!(OnDisconnect),
        }
    }

    fn attribute(self) -> &'static str {
        match self {
            Self::Init => "on_init",
            Self::ShardStart => "on_shard_start",
            Self::Connect => "on_connect",
            Self::Disconnect => "on_disconnect",
        }
    }
}

/// Entry point for `#[fluxum::reducer]`: never panics, renders failures as
/// `compile_error!`.
pub fn expand_reducer(args: TokenStream, input: TokenStream) -> TokenStream {
    match try_expand_reducer(args, input) {
        Ok(tokens) => tokens,
        Err(err) => err.to_compile_error(),
    }
}

/// Entry point for the lifecycle attributes.
pub fn expand_lifecycle(hook: Hook, args: TokenStream, input: TokenStream) -> TokenStream {
    match try_expand_lifecycle(hook, args, input) {
        Ok(tokens) => tokens,
        Err(err) => err.to_compile_error(),
    }
}

/// Entry point for `#[fluxum::view]`.
pub fn expand_view(args: TokenStream, input: TokenStream) -> TokenStream {
    match try_expand_view(args, input) {
        Ok(tokens) => tokens,
        Err(err) => err.to_compile_error(),
    }
}

/// `#[fluxum::view(materialized, table = T, aggregate = sum(col) |
/// count | avg(col) | min(col) | max(col) [, group_by = col])]` or
/// `#[fluxum::view(materialized, table = T, order_by = col [, desc]
/// [, asc], limit = N)]` on a unit struct (SPEC-022 RV-010/012): submits a
/// link-time `MaterializedViewDef` named after the struct. Column existence
/// and types are validated at assembly, where the schema is in hand.
fn try_expand_materialized_view(args: TokenStream, input: TokenStream) -> syn::Result<TokenStream> {
    use syn::parse::Parser;

    let item: syn::ItemStruct = syn::parse2(input).map_err(|_| {
        syn::Error::new(
            args.span(),
            "#[fluxum::view(materialized, …)] annotates a unit struct (RV-010)",
        )
    })?;
    let name = item.ident.to_string();

    let mut table: Option<String> = None;
    let mut aggregate: Option<TokenStream> = None;
    let mut group_by: Option<String> = None;
    let mut order_by: Option<String> = None;
    let mut descending = false;
    let mut limit: Option<u32> = None;

    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("materialized") {
            return Ok(());
        }
        if meta.path.is_ident("table") {
            let ident: syn::Ident = meta.value()?.parse()?;
            table = Some(ident.to_string());
            return Ok(());
        }
        if meta.path.is_ident("aggregate") {
            let expr: syn::Expr = meta.value()?.parse()?;
            aggregate = Some(parse_mv_aggregate(&expr)?);
            return Ok(());
        }
        if meta.path.is_ident("group_by") {
            let ident: syn::Ident = meta.value()?.parse()?;
            group_by = Some(ident.to_string());
            return Ok(());
        }
        if meta.path.is_ident("order_by") {
            let ident: syn::Ident = meta.value()?.parse()?;
            order_by = Some(ident.to_string());
            return Ok(());
        }
        if meta.path.is_ident("desc") {
            descending = true;
            return Ok(());
        }
        if meta.path.is_ident("asc") {
            descending = false;
            return Ok(());
        }
        if meta.path.is_ident("limit") {
            let lit: syn::LitInt = meta.value()?.parse()?;
            limit = Some(lit.base10_parse()?);
            return Ok(());
        }
        Err(meta.error(
            "unknown materialized-view argument: expected table, aggregate, group_by, \
             order_by, desc/asc, limit (RV-010)",
        ))
    });
    parser.parse2(args.clone())?;

    let Some(table) = table else {
        return Err(syn::Error::new(
            args.span(),
            "materialized views need `table = <Table>` (RV-010)",
        ));
    };
    let (aggregate_tokens, top_n_tokens) = match (&aggregate, &order_by, limit) {
        (Some(agg), None, None) => (
            quote!(::core::option::Option::Some(#agg)),
            quote!(::core::option::Option::None),
        ),
        (None, Some(column), Some(n)) => (
            quote!(::core::option::Option::None),
            quote! {
                ::core::option::Option::Some(::fluxum_core::reducer::MvTopN {
                    column: #column,
                    descending: #descending,
                    limit: #n,
                })
            },
        ),
        _ => {
            return Err(syn::Error::new(
                args.span(),
                "declare `aggregate = …` (with optional group_by) OR `order_by = … , \
                 limit = N` — exactly one (RV-010/012)",
            ));
        }
    };
    if group_by.is_some() && aggregate.is_none() {
        return Err(syn::Error::new(
            args.span(),
            "`group_by` applies to aggregate views only (RV-010)",
        ));
    }
    let group_tokens = match &group_by {
        Some(column) => quote!(::core::option::Option::Some(#column)),
        None => quote!(::core::option::Option::None),
    };

    Ok(quote! {
        #item

        ::fluxum_core::schema::inventory::submit! {
            ::fluxum_core::reducer::MaterializedViewDef {
                name: #name,
                table: #table,
                aggregate: #aggregate_tokens,
                group_by: #group_tokens,
                top_n: #top_n_tokens,
            }
        }
    })
}

/// Parse `count` | `sum(col)` | `avg(col)` | `min(col)` | `max(col)`.
fn parse_mv_aggregate(expr: &syn::Expr) -> syn::Result<TokenStream> {
    let path = quote!(::fluxum_core::reducer::MvAggregate);
    match expr {
        syn::Expr::Path(p) if p.path.is_ident("count") => Ok(quote!(#path::Count)),
        syn::Expr::Call(call) => {
            let syn::Expr::Path(func) = call.func.as_ref() else {
                return Err(syn::Error::new(
                    call.span(),
                    "expected sum|avg|min|max(col)",
                ));
            };
            let Some(kind) = func.path.get_ident().map(ToString::to_string) else {
                return Err(syn::Error::new(
                    call.span(),
                    "expected sum|avg|min|max(col)",
                ));
            };
            let column = match call.args.first() {
                Some(syn::Expr::Path(p)) if call.args.len() == 1 => {
                    p.path.get_ident().map(ToString::to_string)
                }
                _ => None,
            };
            let Some(column) = column else {
                return Err(syn::Error::new(
                    call.span(),
                    "aggregate takes exactly one column name (RV-010)",
                ));
            };
            let variant = match kind.as_str() {
                "sum" => quote!(Sum),
                "avg" => quote!(Avg),
                "min" => quote!(Min),
                "max" => quote!(Max),
                other => {
                    return Err(syn::Error::new(
                        call.span(),
                        format!("unknown aggregate `{other}`: count, sum, avg, min, max (RV-010)"),
                    ));
                }
            };
            Ok(quote!(#path::#variant(#column)))
        }
        other => Err(syn::Error::new(
            other.span(),
            "expected `count` or `sum|avg|min|max(col)` (RV-010)",
        )),
    }
}

/// Entry point for `#[fluxum::tick(rate = N)]`.
pub fn expand_tick(args: TokenStream, input: TokenStream) -> TokenStream {
    match try_expand_tick(args, input) {
        Ok(tokens) => tokens,
        Err(err) => err.to_compile_error(),
    }
}

/// Entry point for `#[fluxum::schedule(delay_ms = N, ...)]`.
pub fn expand_schedule(args: TokenStream, input: TokenStream) -> TokenStream {
    match try_expand_schedule(args, input) {
        Ok(tokens) => tokens,
        Err(err) => err.to_compile_error(),
    }
}

/// Shared shape validation: synchronous, non-generic, at least the context
/// parameter, every later parameter a plain `ident: Type`. Returns the
/// typed (ident, type) parameter list after the context. Shared with the
/// RV-031 trigger attributes (`crate::trigger`).
pub(crate) fn check_shape(
    item: &ItemFn,
    attribute: &str,
) -> syn::Result<Vec<(syn::Ident, syn::Type)>> {
    if let Some(asyncness) = &item.sig.asyncness {
        return Err(syn::Error::new(
            asyncness.span(),
            format!(
                "#[fluxum::{attribute}] functions are synchronous: they run inside one \
                 transaction on the shard's single writer (TXN-010)"
            ),
        ));
    }
    if !item.sig.generics.params.is_empty() || item.sig.generics.where_clause.is_some() {
        return Err(syn::Error::new(
            item.sig.generics.span(),
            format!("#[fluxum::{attribute}] functions cannot be generic"),
        ));
    }
    let mut inputs = item.sig.inputs.iter();
    match inputs.next() {
        Some(FnArg::Typed(ctx)) if matches!(*ctx.ty, syn::Type::Reference(_)) => {}
        other => {
            return Err(syn::Error::new(
                other.map_or_else(|| item.sig.inputs.span(), Spanned::span),
                format!(
                    "#[fluxum::{attribute}] functions take a context reference as their \
                     first parameter (RED-001/RED-031)"
                ),
            ));
        }
    }
    let mut params = Vec::new();
    for input in inputs {
        let FnArg::Typed(typed) = input else {
            return Err(syn::Error::new(
                input.span(),
                format!("#[fluxum::{attribute}]: `self` parameters are not supported"),
            ));
        };
        let Pat::Ident(ident) = &*typed.pat else {
            return Err(syn::Error::new(
                typed.pat.span(),
                format!(
                    "#[fluxum::{attribute}]: parameters must be plain identifiers \
                     (pattern parameters are not supported)"
                ),
            ));
        };
        params.push((ident.ident.clone(), (*typed.ty).clone()));
    }
    Ok(params)
}

fn reject_args(args: &TokenStream, attribute: &str, extra: &str) -> syn::Result<()> {
    if args.is_empty() {
        return Ok(());
    }
    Err(syn::Error::new(
        args.span(),
        format!("#[fluxum::{attribute}] takes no arguments{extra}"),
    ))
}

/// Parse the RED-050 `max_rate = "N/s"` declaration into calls-per-second.
fn parse_max_rate(lit: &syn::Lit) -> syn::Result<u32> {
    let syn::Lit::Str(s) = lit else {
        return Err(syn::Error::new(
            lit.span(),
            "`max_rate` is a string like \"5/s\" (RED-050)",
        ));
    };
    let value = s.value();
    let rate = value
        .strip_suffix("/s")
        .and_then(|n| n.parse::<u32>().ok())
        .filter(|n| *n >= 1);
    rate.ok_or_else(|| {
        syn::Error::new(
            s.span(),
            format!("invalid max_rate \"{value}\": expected \"N/s\" with N >= 1 (RED-050)"),
        )
    })
}

fn try_expand_reducer(args: TokenStream, input: TokenStream) -> syn::Result<TokenStream> {
    let parsed = ScheduleArgs::parse(args, "reducer", &["max_rate"])?;
    let max_rate = match parsed.values.get("max_rate") {
        Some(lit) => parse_max_rate(lit)?,
        None => 0,
    };
    let item: ItemFn = syn::parse2(input)?;
    let params = check_shape(&item, "reducer")?;
    if matches!(item.sig.output, ReturnType::Default) {
        return Err(syn::Error::new(
            item.sig.span(),
            "#[fluxum::reducer] functions return Result<(), String> (RED-060)",
        ));
    }

    let fn_ident = &item.sig.ident;
    let name_str = fn_ident.to_string();
    let arity = params.len();

    let check_lines = params.iter().enumerate().map(|(index, (ident, ty))| {
        let param_name = ident.to_string();
        quote! {
            let _ = ::fluxum_core::reducer::args::decode_arg::<#ty>(
                #name_str, args, #index, #param_name,
            )?;
        }
    });
    let decode_lines = params.iter().enumerate().map(|(index, (ident, ty))| {
        let param_name = ident.to_string();
        quote! {
            let #ident: #ty = ::fluxum_core::reducer::args::decode_arg(
                #name_str, args, #index, #param_name,
            )?;
        }
    });
    let param_idents: Vec<&syn::Ident> = params.iter().map(|(ident, _)| ident).collect();
    let signature_params = params.iter().map(|(ident, ty)| {
        let param_name = ident.to_string();
        let ty_str = quote!(#ty).to_string();
        quote! {
            ::fluxum_core::reducer::ParamDef { name: #param_name, ty: #ty_str }
        }
    });
    let returns_str = match &item.sig.output {
        ReturnType::Type(_, ty) => quote!(#ty).to_string(),
        ReturnType::Default => String::new(),
    };

    Ok(quote! {
        #item

        const _: () = {
            fn __fluxum_check_args(
                args: &[::fluxum_core::reducer::FluxValue],
            ) -> ::fluxum_core::Result<()> {
                ::fluxum_core::reducer::args::check_arity(#name_str, args, #arity)?;
                #(#check_lines)*
                Ok(())
            }

            fn __fluxum_handler(
                ctx: &::fluxum_core::reducer::ReducerContext<'_, '_, '_>,
                args: &[::fluxum_core::reducer::FluxValue],
            ) -> ::fluxum_core::Result<()> {
                ::fluxum_core::reducer::args::check_arity(#name_str, args, #arity)?;
                #(#decode_lines)*
                match #fn_ident(ctx #(, #param_idents)*) {
                    Ok(_) => Ok(()),
                    Err(message) => Err(::fluxum_core::FluxumError::Reducer(
                        ::std::string::ToString::to_string(&message),
                    )),
                }
            }

            ::fluxum_core::schema::inventory::submit! {
                ::fluxum_core::reducer::ReducerDef {
                    name: #name_str,
                    handler: __fluxum_handler,
                    check_args: __fluxum_check_args,
                    client_callable: true,
                    max_rate_per_sec: #max_rate,
                }
            }

            // SPEC-011 SDK-001: publish the call signature for `/schema` and
            // every SDK generator. Types are the source spelling, which is
            // what a generator maps into its own type system.
            ::fluxum_core::schema::inventory::submit! {
                ::fluxum_core::reducer::ReducerSignature {
                    reducer: #name_str,
                    params: &[#(#signature_params),*],
                    returns: #returns_str,
                }
            }
        };
    })
}

fn try_expand_lifecycle(
    hook: Hook,
    args: TokenStream,
    input: TokenStream,
) -> syn::Result<TokenStream> {
    let attribute = hook.attribute();
    reject_args(&args, attribute, "")?;
    let item: ItemFn = syn::parse2(input)?;
    let params = check_shape(&item, attribute)?;
    if !params.is_empty() {
        return Err(syn::Error::new(
            item.sig.inputs.span(),
            format!(
                "#[fluxum::{attribute}] functions take exactly one parameter: \
                 `ctx: &ReducerContext` (RED-010..RED-013; connection metadata \
                 arrives through the context)"
            ),
        ));
    }

    let fn_ident = &item.sig.ident;
    let name_str = fn_ident.to_string();
    let variant = hook.variant();

    Ok(quote! {
        #item

        const _: () = {
            fn __fluxum_hook(
                ctx: &::fluxum_core::reducer::ReducerContext<'_, '_, '_>,
            ) -> ::fluxum_core::Result<()> {
                match #fn_ident(ctx) {
                    Ok(()) => Ok(()),
                    Err(message) => Err(::fluxum_core::FluxumError::Reducer(
                        ::std::string::ToString::to_string(&message),
                    )),
                }
            }

            ::fluxum_core::schema::inventory::submit! {
                ::fluxum_core::reducer::LifecycleDef {
                    kind: ::fluxum_core::reducer::LifecycleKind::#variant,
                    name: #name_str,
                    handler: __fluxum_hook,
                }
            }
        };
    })
}

fn try_expand_view(args: TokenStream, input: TokenStream) -> syn::Result<TokenStream> {
    // SPEC-022 RV-010: `#[fluxum::view(materialized, …)]` on a unit struct
    // declares an incrementally-maintained pushed view.
    let args_str = args.to_string();
    if args_str.starts_with("materialized") {
        return try_expand_materialized_view(args, input);
    }
    reject_args(&args, "view", "")?;
    let item: ItemFn = syn::parse2(input)?;
    let params = check_shape(&item, "view")?;
    if matches!(item.sig.output, ReturnType::Default) {
        return Err(syn::Error::new(
            item.sig.span(),
            "#[fluxum::view] functions return a serializable value (RED-030)",
        ));
    }

    let fn_ident = &item.sig.ident;
    let name_str = fn_ident.to_string();
    let arity = params.len();
    let handler_ident = format_ident!("__fluxum_view_{}", fn_ident);

    let decode_lines = params.iter().enumerate().map(|(index, (ident, ty))| {
        let param_name = ident.to_string();
        quote! {
            let #ident: #ty = ::fluxum_core::reducer::args::decode_arg(
                #name_str, args, #index, #param_name,
            )?;
        }
    });
    let param_idents: Vec<&syn::Ident> = params.iter().map(|(ident, _)| ident).collect();

    Ok(quote! {
        #item

        const _: () = {
            fn #handler_ident(
                ctx: &::fluxum_core::reducer::ViewContext<'_>,
                args: &[::fluxum_core::reducer::FluxValue],
            ) -> ::fluxum_core::Result<::fluxum_core::reducer::view::serde_json::Value> {
                ::fluxum_core::reducer::args::check_arity(#name_str, args, #arity)?;
                #(#decode_lines)*
                let result = #fn_ident(ctx #(, #param_idents)*);
                ::fluxum_core::reducer::view::serde_json::to_value(result).map_err(|e| {
                    ::fluxum_core::FluxumError::Storage(::std::format!(
                        "view `{}`: result serialization failed: {e}",
                        #name_str
                    ))
                })
            }

            ::fluxum_core::schema::inventory::submit! {
                ::fluxum_core::reducer::ViewDef {
                    name: #name_str,
                    handler: #handler_ident,
                }
            }
        };
    })
}

/// Integer/bool attribute-argument parser for `#[fluxum::tick]` /
/// `#[fluxum::schedule]` (`name = literal` pairs, like `#[fluxum::migration]`).
struct ScheduleArgs {
    values: std::collections::HashMap<String, syn::Lit>,
}

impl ScheduleArgs {
    fn parse(args: TokenStream, attribute: &str, allowed: &[&str]) -> syn::Result<Self> {
        use syn::parse::Parser;
        let metas = syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated
            .parse2(args)?;
        let mut values = std::collections::HashMap::new();
        for meta in &metas {
            let span = meta.span();
            let syn::Meta::NameValue(pair) = meta else {
                return Err(syn::Error::new(
                    span,
                    format!("#[fluxum::{attribute}] arguments are `name = value` pairs"),
                ));
            };
            let Some(ident) = pair.path.get_ident() else {
                return Err(syn::Error::new(span, "expected a plain argument name"));
            };
            let name = ident.to_string();
            if !allowed.contains(&name.as_str()) {
                return Err(syn::Error::new(
                    span,
                    format!(
                        "unknown #[fluxum::{attribute}] argument `{name}` (expected one of: {})",
                        allowed.join(", ")
                    ),
                ));
            }
            let syn::Expr::Lit(lit) = &pair.value else {
                return Err(syn::Error::new(
                    pair.value.span(),
                    format!("`{name}` must be a literal"),
                ));
            };
            if values.insert(name.clone(), lit.lit.clone()).is_some() {
                return Err(syn::Error::new(
                    span,
                    format!("duplicate `{name}` argument"),
                ));
            }
        }
        Ok(Self { values })
    }

    fn int(&self, name: &str) -> syn::Result<Option<u64>> {
        match self.values.get(name) {
            None => Ok(None),
            Some(syn::Lit::Int(int)) => int.base10_parse::<u64>().map(Some),
            Some(other) => Err(syn::Error::new(
                other.span(),
                format!("`{name}` must be an integer literal"),
            )),
        }
    }

    fn bool(&self, name: &str) -> syn::Result<bool> {
        match self.values.get(name) {
            None => Ok(false),
            Some(syn::Lit::Bool(b)) => Ok(b.value),
            Some(other) => Err(syn::Error::new(
                other.span(),
                format!("`{name}` must be a bool literal"),
            )),
        }
    }
}

/// The scheduled-function shape shared by `#[fluxum::tick]` and
/// `#[fluxum::schedule]`: `fn(ctx: &ReducerContext) -> Result<(), String>`,
/// registered as a zero-argument reducer (schedule-only unless
/// `client_callable = true`, RED-025).
fn scheduled_reducer_submission(
    item: &ItemFn,
    attribute: &str,
    client_callable: bool,
) -> syn::Result<(String, TokenStream)> {
    let params = check_shape(item, attribute)?;
    if !params.is_empty() {
        return Err(syn::Error::new(
            item.sig.inputs.span(),
            format!(
                "#[fluxum::{attribute}] functions take exactly one parameter: \
                 `ctx: &ReducerContext` (RED-020/RED-021; dynamic arguments go \
                 through ctx.schedule_after)"
            ),
        ));
    }
    if matches!(item.sig.output, ReturnType::Default) {
        return Err(syn::Error::new(
            item.sig.span(),
            format!("#[fluxum::{attribute}] functions return Result<(), String> (RED-060)"),
        ));
    }
    let fn_ident = &item.sig.ident;
    let name_str = fn_ident.to_string();
    let submission = quote! {
        fn __fluxum_check_args(
            args: &[::fluxum_core::reducer::FluxValue],
        ) -> ::fluxum_core::Result<()> {
            ::fluxum_core::reducer::args::check_arity(#name_str, args, 0usize)
        }

        fn __fluxum_handler(
            ctx: &::fluxum_core::reducer::ReducerContext<'_, '_, '_>,
            args: &[::fluxum_core::reducer::FluxValue],
        ) -> ::fluxum_core::Result<()> {
            ::fluxum_core::reducer::args::check_arity(#name_str, args, 0usize)?;
            match #fn_ident(ctx) {
                Ok(()) => Ok(()),
                Err(message) => Err(::fluxum_core::FluxumError::Reducer(
                    ::std::string::ToString::to_string(&message),
                )),
            }
        }

        ::fluxum_core::schema::inventory::submit! {
            ::fluxum_core::reducer::ReducerDef {
                name: #name_str,
                handler: __fluxum_handler,
                check_args: __fluxum_check_args,
                client_callable: #client_callable,
                max_rate_per_sec: 0u32,
            }
        }
    };
    Ok((name_str, submission))
}

fn try_expand_tick(args: TokenStream, input: TokenStream) -> syn::Result<TokenStream> {
    let args_span = args.span();
    let parsed = ScheduleArgs::parse(args, "tick", &["rate", "client_callable"])?;
    let Some(rate) = parsed.int("rate")? else {
        return Err(syn::Error::new(
            args_span,
            "missing `rate = N`: write `#[fluxum::tick(rate = N)]` (Hz, RED-020)",
        ));
    };
    if rate == 0 || rate > 1_000_000 {
        return Err(syn::Error::new(
            args_span,
            "`rate` must be 1..=1_000_000 Hz (RED-020)",
        ));
    }
    let client_callable = parsed.bool("client_callable")?;
    let item: ItemFn = syn::parse2(input)?;
    let (name_str, submission) = scheduled_reducer_submission(&item, "tick", client_callable)?;
    let rate_u32 = u32::try_from(rate)
        .map_err(|_| syn::Error::new(args_span, "`rate` must fit in u32 (RED-020)"))?;

    Ok(quote! {
        #item

        const _: () = {
            #submission

            ::fluxum_core::schema::inventory::submit! {
                ::fluxum_core::scheduler::TickDef {
                    name: #name_str,
                    rate_hz: #rate_u32,
                }
            }
        };
    })
}

fn try_expand_schedule(args: TokenStream, input: TokenStream) -> syn::Result<TokenStream> {
    let args_span = args.span();
    let parsed = ScheduleArgs::parse(
        args,
        "schedule",
        &["delay_ms", "every_ms", "client_callable"],
    )?;
    let delay_ms = parsed.int("delay_ms")?;
    let every_ms = parsed.int("every_ms")?;
    if delay_ms.is_none() && every_ms.is_none() {
        return Err(syn::Error::new(
            args_span,
            "missing `delay_ms = N` (and/or `every_ms = M` for a recurring \
             schedule): write `#[fluxum::schedule(delay_ms = N)]` (RED-021)",
        ));
    }
    if every_ms == Some(0) {
        return Err(syn::Error::new(
            args_span,
            "`every_ms` must be >= 1 (use a one-shot `delay_ms` schedule instead)",
        ));
    }
    let client_callable = parsed.bool("client_callable")?;
    let item: ItemFn = syn::parse2(input)?;
    let (name_str, submission) = scheduled_reducer_submission(&item, "schedule", client_callable)?;

    // First firing after `delay_ms` (default: one period for recurring).
    let period_us = every_ms.unwrap_or(0).saturating_mul(1_000);
    let delay_us = delay_ms
        .unwrap_or(every_ms.unwrap_or(0))
        .saturating_mul(1_000);
    let delay_us = i64::try_from(delay_us)
        .map_err(|_| syn::Error::new(args_span, "`delay_ms` overflows the µs clock"))?;
    let period_us = i64::try_from(period_us)
        .map_err(|_| syn::Error::new(args_span, "`every_ms` overflows the µs clock"))?;

    Ok(quote! {
        #item

        const _: () = {
            #submission

            ::fluxum_core::schema::inventory::submit! {
                ::fluxum_core::scheduler::ScheduleDef {
                    name: #name_str,
                    delay_us: #delay_us,
                    period_us: #period_us,
                }
            }
        };
    })
}

#[cfg(test)]
mod tests {
    //! Shape validation of the T3.3 function attributes, probed on the
    //! expansion functions directly (the UI suite pins the end-to-end
    //! compile-fail rendering).
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn expand_err(result: syn::Result<TokenStream>) -> String {
        result.expect_err("expansion must fail").to_string()
    }

    #[test]
    fn reducer_expands_check_and_handler() {
        let out = try_expand_reducer(
            TokenStream::new(),
            quote! {
                fn send_message(
                    ctx: &ReducerContext,
                    channel: u32,
                    text: String,
                ) -> Result<(), String> {
                    Ok(())
                }
            },
        )
        .unwrap()
        .to_string();
        assert!(out.contains("ReducerDef"), "{out}");
        assert!(out.contains("check_arity"), "{out}");
        assert!(out.contains("send_message"), "{out}");
    }

    #[test]
    fn reducer_parses_max_rate() {
        let out = try_expand_reducer(
            quote!(max_rate = "5/s"),
            quote! { fn f(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        )
        .unwrap()
        .to_string();
        assert!(out.contains("max_rate_per_sec : 5u32"), "{out}");

        // Undeclared: unlimited.
        let out = try_expand_reducer(
            TokenStream::new(),
            quote! { fn f(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        )
        .unwrap()
        .to_string();
        assert!(out.contains("max_rate_per_sec : 0u32"), "{out}");

        for bad in [quote!(max_rate = "5/min"), quote!(max_rate = "0/s")] {
            let err = expand_err(try_expand_reducer(
                bad,
                quote! { fn f(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
            ));
            assert!(err.contains("N/s"), "{err}");
        }
        let err = expand_err(try_expand_reducer(
            quote!(max_rate = 5),
            quote! { fn f(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        ));
        assert!(err.contains("string like"), "{err}");
        let err = expand_err(try_expand_reducer(
            quote!(version = 2),
            quote! { fn f(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        ));
        assert!(err.contains("unknown #[fluxum::reducer] argument"), "{err}");
    }

    #[test]
    fn reducer_rejects_bad_shapes() {
        let err = expand_err(try_expand_reducer(
            TokenStream::new(),
            quote! { async fn f(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        ));
        assert!(err.contains("synchronous"), "{err}");

        let err = expand_err(try_expand_reducer(
            TokenStream::new(),
            quote! { fn f<T>(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        ));
        assert!(err.contains("generic"), "{err}");

        let err = expand_err(try_expand_reducer(
            TokenStream::new(),
            quote! { fn f() -> Result<(), String> { Ok(()) } },
        ));
        assert!(err.contains("context reference"), "{err}");

        let err = expand_err(try_expand_reducer(
            TokenStream::new(),
            quote! { fn f(ctx: ReducerContext) -> Result<(), String> { Ok(()) } },
        ));
        assert!(err.contains("context reference"), "{err}");

        let err = expand_err(try_expand_reducer(
            TokenStream::new(),
            quote! { fn f(ctx: &ReducerContext, (a, b): (u32, u32)) -> Result<(), String> { Ok(()) } },
        ));
        assert!(err.contains("plain identifiers"), "{err}");

        let err = expand_err(try_expand_reducer(
            TokenStream::new(),
            quote! { fn f(ctx: &ReducerContext) {} },
        ));
        assert!(err.contains("Result<(), String>"), "{err}");
    }

    #[test]
    fn lifecycle_expands_and_rejects_extra_params() {
        let out = try_expand_lifecycle(
            Hook::Init,
            TokenStream::new(),
            quote! { fn init(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        )
        .unwrap()
        .to_string();
        assert!(out.contains("LifecycleDef"), "{out}");
        assert!(out.contains("OnInit"), "{out}");

        for hook in [Hook::ShardStart, Hook::Connect, Hook::Disconnect] {
            let out = try_expand_lifecycle(
                hook,
                TokenStream::new(),
                quote! { fn h(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
            )
            .unwrap()
            .to_string();
            assert!(out.contains("LifecycleDef"), "{out}");
        }

        let err = expand_err(try_expand_lifecycle(
            Hook::Connect,
            TokenStream::new(),
            quote! { fn h(ctx: &ReducerContext, extra: u32) -> Result<(), String> { Ok(()) } },
        ));
        assert!(err.contains("exactly one parameter"), "{err}");

        let err = expand_err(try_expand_lifecycle(
            Hook::Init,
            quote!(nope),
            quote! { fn h(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        ));
        assert!(err.contains("no arguments"), "{err}");
    }

    #[test]
    fn tick_expands_and_validates_rate() {
        let out = try_expand_tick(
            quote::quote!(rate = 60),
            quote! { fn beat(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        )
        .unwrap()
        .to_string();
        assert!(out.contains("TickDef"), "{out}");
        assert!(out.contains("client_callable : false"), "{out}");

        let err = expand_err(try_expand_tick(
            TokenStream::new(),
            quote! { fn beat(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        ));
        assert!(err.contains("missing `rate = N`"), "{err}");

        let err = expand_err(try_expand_tick(
            quote::quote!(rate = 0),
            quote! { fn beat(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        ));
        assert!(err.contains("1..=1_000_000"), "{err}");

        let err = expand_err(try_expand_tick(
            quote::quote!(rate = "fast"),
            quote! { fn beat(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        ));
        assert!(err.contains("integer literal"), "{err}");

        let err = expand_err(try_expand_tick(
            quote::quote!(rate = 60),
            quote! { fn beat(ctx: &ReducerContext, extra: u32) -> Result<(), String> { Ok(()) } },
        ));
        assert!(err.contains("exactly one parameter"), "{err}");
    }

    #[test]
    fn schedule_expands_and_validates_arguments() {
        let out = try_expand_schedule(
            quote::quote!(delay_ms = 50, every_ms = 100, client_callable = true),
            quote! { fn sweep(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        )
        .unwrap()
        .to_string();
        assert!(out.contains("ScheduleDef"), "{out}");
        assert!(out.contains("client_callable : true"), "{out}");

        // every_ms alone: first firing defaults to one period.
        let out = try_expand_schedule(
            quote::quote!(every_ms = 100),
            quote! { fn sweep(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        )
        .unwrap()
        .to_string();
        assert!(out.contains("100000i64"), "{out}");

        let err = expand_err(try_expand_schedule(
            TokenStream::new(),
            quote! { fn sweep(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        ));
        assert!(err.contains("missing `delay_ms = N`"), "{err}");

        let err = expand_err(try_expand_schedule(
            quote::quote!(delay_ms = 50, every_ms = 0),
            quote! { fn sweep(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        ));
        assert!(err.contains("`every_ms` must be >= 1"), "{err}");

        let err = expand_err(try_expand_schedule(
            quote::quote!(cron = "* * *"),
            quote! { fn sweep(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        ));
        assert!(
            err.contains("unknown #[fluxum::schedule] argument"),
            "{err}"
        );

        let err = expand_err(try_expand_schedule(
            quote::quote!(delay_ms = 50, delay_ms = 60),
            quote! { fn sweep(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        ));
        assert!(err.contains("duplicate `delay_ms`"), "{err}");

        let err = expand_err(try_expand_schedule(
            quote::quote!(delay_ms = 50, client_callable = 1),
            quote! { fn sweep(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        ));
        assert!(err.contains("bool literal"), "{err}");
    }

    #[test]
    fn public_entry_points_render_failures_as_compile_error() {
        // Every expand_* wrapper turns a syn::Error into compile_error!.
        let bad_fn = || quote! { fn f() {} };
        for out in [
            expand_reducer(TokenStream::new(), bad_fn()),
            expand_lifecycle(Hook::Init, TokenStream::new(), bad_fn()),
            expand_view(TokenStream::new(), bad_fn()),
            expand_tick(quote!(rate = 60), bad_fn()),
            expand_schedule(quote!(delay_ms = 50), bad_fn()),
        ] {
            let out = out.to_string();
            assert!(out.contains("compile_error !"), "{out}");
        }
    }

    #[test]
    fn schedule_argument_shapes_are_validated() {
        // Meta that is not `name = value`.
        let err = expand_err(try_expand_tick(
            quote!(rate(60)),
            quote! { fn beat(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        ));
        assert!(err.contains("`name = value` pairs"), "{err}");

        // Multi-segment argument path.
        let err = expand_err(try_expand_tick(
            quote!(a::b = 1),
            quote! { fn beat(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        ));
        assert!(err.contains("plain argument name"), "{err}");

        // Non-literal value for a known argument.
        let err = expand_err(try_expand_tick(
            quote!(rate = fast),
            quote! { fn beat(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        ));
        assert!(err.contains("`rate` must be a literal"), "{err}");
    }

    #[test]
    fn scheduled_functions_require_a_result_return_type() {
        let err = expand_err(try_expand_tick(
            quote!(rate = 60),
            quote! { fn beat(ctx: &ReducerContext) {} },
        ));
        assert!(err.contains("Result<(), String> (RED-060)"), "{err}");
    }

    #[test]
    fn late_self_receiver_is_rejected() {
        // syn refuses to parse a receiver after the first parameter, so the
        // defensive branch is probed on a hand-assembled signature.
        let mut item: ItemFn = syn::parse_quote! {
            fn f(ctx: &ReducerContext) -> Result<(), String> { Ok(()) }
        };
        let donor: ItemFn = syn::parse_quote! { fn g(&self) {} };
        let receiver = donor.sig.inputs.first().expect("receiver").clone();
        item.sig.inputs.push(receiver);
        let err = check_shape(&item, "reducer")
            .expect_err("self must be rejected")
            .to_string();
        assert!(err.contains("`self` parameters are not supported"), "{err}");
    }

    #[test]
    fn view_expands_and_requires_a_return_type() {
        let out = try_expand_view(
            TokenStream::new(),
            quote! { fn stats(ctx: &ViewContext, top_n: u32) -> Vec<u64> { vec![] } },
        )
        .unwrap()
        .to_string();
        assert!(out.contains("ViewDef"), "{out}");
        assert!(out.contains("to_value"), "{out}");

        let err = expand_err(try_expand_view(
            TokenStream::new(),
            quote! { fn stats(ctx: &ViewContext) {} },
        ));
        assert!(err.contains("serializable value"), "{err}");
    }
}
