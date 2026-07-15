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

/// Shared shape validation: synchronous, non-generic, at least the context
/// parameter, every later parameter a plain `ident: Type`. Returns the
/// typed (ident, type) parameter list after the context.
fn check_shape(item: &ItemFn, attribute: &str) -> syn::Result<Vec<(syn::Ident, syn::Type)>> {
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

fn try_expand_reducer(args: TokenStream, input: TokenStream) -> syn::Result<TokenStream> {
    reject_args(
        &args,
        "reducer",
        " yet: `max_rate` lands with T3.5 (RED-050) and `version` with RED-007",
    )?;
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
    fn reducer_rejects_bad_shapes() {
        let err = expand_err(try_expand_reducer(
            quote!(max_rate = "5/s"),
            quote! { fn f(ctx: &ReducerContext) -> Result<(), String> { Ok(()) } },
        ));
        assert!(err.contains("T3.5"), "{err}");

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
