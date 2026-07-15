//! Expansion of `#[fluxum::migration(version = N)]` (SPEC-010 MIG-010).
//!
//! Keeps the annotated function unchanged and submits a
//! `fluxum_core::migration::MigrationDef` to the link-time registry, so the
//! startup runner (`MigrationRunner`) collects it exactly like tables and
//! reducers (SPEC-001 DM-040). The function must have the MIG-011 shape:
//!
//! ```ignore
//! #[fluxum::migration(version = 2)]
//! fn migrate_v2(ctx: &mut MigrationContext) -> fluxum::Result<()> {
//!     ctx.add_column("task", "priority", FluxValue::U8(0))
//! }
//! ```

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{Expr, ItemFn, Lit, Meta, Token};

/// Entry point: never panics, renders parse/validation failures as
/// `compile_error!`.
pub fn expand(args: TokenStream, input: TokenStream) -> TokenStream {
    match try_expand(args, input) {
        Ok(tokens) => tokens,
        Err(err) => err.to_compile_error(),
    }
}

fn try_expand(args: TokenStream, input: TokenStream) -> syn::Result<TokenStream> {
    let args_span = args.span();
    let item: ItemFn = syn::parse2(input)?;

    // -- version argument ----------------------------------------------------
    let mut version: Option<u32> = None;
    let metas = Punctuated::<Meta, Token![,]>::parse_terminated.parse2(args)?;
    for meta in &metas {
        let span = meta.span();
        if meta.path().is_ident("version") {
            if version.is_some() {
                return Err(syn::Error::new(span, "duplicate `version = N` argument"));
            }
            let Meta::NameValue(pair) = meta else {
                return Err(syn::Error::new(
                    span,
                    "expected `#[fluxum::migration(version = N)]` (MIG-010)",
                ));
            };
            let Expr::Lit(lit) = &pair.value else {
                return Err(syn::Error::new(
                    pair.value.span(),
                    "`version` must be an integer literal (MIG-010)",
                ));
            };
            let Lit::Int(int) = &lit.lit else {
                return Err(syn::Error::new(
                    lit.span(),
                    "`version` must be an integer literal (MIG-010)",
                ));
            };
            version = Some(int.base10_parse::<u32>()?);
        } else {
            return Err(syn::Error::new(
                span,
                "unknown #[fluxum::migration] argument: expected `version = N` (MIG-010)",
            ));
        }
    }
    let Some(version) = version else {
        return Err(syn::Error::new(
            args_span,
            "missing `version = N`: write `#[fluxum::migration(version = N)]` (MIG-010)",
        ));
    };
    if version < 2 {
        return Err(syn::Error::new(
            args_span,
            "migration version must be >= 2: version 1 is the initial schema \
             (fluxum::schema_version! defaults to 1, MIG-001/MIG-010)",
        ));
    }

    // -- function shape --------------------------------------------------------
    if let Some(asyncness) = &item.sig.asyncness {
        return Err(syn::Error::new(
            asyncness.span(),
            "migration functions are synchronous: they run inside one startup \
             transaction (MIG-040)",
        ));
    }
    if !item.sig.generics.params.is_empty() || item.sig.generics.where_clause.is_some() {
        return Err(syn::Error::new(
            item.sig.generics.span(),
            "migration functions cannot be generic (MIG-010)",
        ));
    }
    if item.sig.inputs.len() != 1 {
        return Err(syn::Error::new(
            item.sig.inputs.span(),
            "migration functions take exactly one argument: \
             `ctx: &mut MigrationContext` (MIG-011)",
        ));
    }

    let fn_ident = &item.sig.ident;
    let name_str = fn_ident.to_string();

    Ok(quote! {
        #item

        const _: () = {
            ::fluxum_core::schema::inventory::submit! {
                ::fluxum_core::migration::MigrationDef {
                    version: #version,
                    name: #name_str,
                    run: #fn_ident,
                }
            }
        };
    })
}

#[cfg(test)]
mod tests {
    //! Argument/shape validation of `#[fluxum::migration]`, probed on the
    //! expansion function directly (the compile-fail UI suite exercises the
    //! same errors end-to-end but outside coverage instrumentation).
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn good_fn() -> TokenStream {
        quote! {
            fn migrate_v2(ctx: &mut MigrationContext) -> fluxum::Result<()> {
                Ok(())
            }
        }
    }

    fn expand_err(args: TokenStream, input: TokenStream) -> String {
        try_expand(args, input)
            .expect_err("expansion must fail")
            .to_string()
    }

    #[test]
    fn valid_input_registers_a_migration_def() {
        let out = try_expand(quote::quote!(version = 2), good_fn())
            .unwrap()
            .to_string();
        assert!(out.contains("MigrationDef"), "{out}");
        assert!(out.contains("migrate_v2"), "{out}");
    }

    #[test]
    fn duplicate_version_argument_is_rejected() {
        let err = expand_err(quote::quote!(version = 2, version = 3), good_fn());
        assert!(err.contains("duplicate"), "{err}");
    }

    #[test]
    fn version_must_be_a_name_value_pair() {
        // Bare path.
        let err = expand_err(quote::quote!(version), good_fn());
        assert!(err.contains("version = N"), "{err}");
        // List form.
        let err = expand_err(quote::quote!(version(2)), good_fn());
        assert!(err.contains("version = N"), "{err}");
    }

    #[test]
    fn version_must_be_an_integer_literal() {
        // Non-literal expression.
        let err = expand_err(quote::quote!(version = 1 + 1), good_fn());
        assert!(err.contains("integer literal"), "{err}");
        // Wrong literal kind.
        let err = expand_err(quote::quote!(version = "2"), good_fn());
        assert!(err.contains("integer literal"), "{err}");
    }

    #[test]
    fn unknown_arguments_are_rejected() {
        let err = expand_err(quote::quote!(target = 2), good_fn());
        assert!(err.contains("unknown #[fluxum::migration] argument"), "{err}");
    }

    #[test]
    fn missing_and_below_two_versions_are_rejected() {
        let err = expand_err(TokenStream::new(), good_fn());
        assert!(err.contains("missing `version = N`"), "{err}");
        let err = expand_err(quote::quote!(version = 1), good_fn());
        assert!(err.contains(">= 2"), "{err}");
    }

    #[test]
    fn async_generic_and_wrong_arity_functions_are_rejected() {
        let err = expand_err(
            quote::quote!(version = 2),
            quote! {
                async fn migrate_v2(ctx: &mut MigrationContext) -> fluxum::Result<()> {
                    Ok(())
                }
            },
        );
        assert!(err.contains("synchronous"), "{err}");

        let err = expand_err(
            quote::quote!(version = 2),
            quote! {
                fn migrate_v2<T>(ctx: &mut MigrationContext) -> fluxum::Result<()> {
                    Ok(())
                }
            },
        );
        assert!(err.contains("cannot be generic"), "{err}");

        let err = expand_err(
            quote::quote!(version = 2),
            quote! {
                fn migrate_v2(ctx: &mut MigrationContext, extra: u32) -> fluxum::Result<()> {
                    Ok(())
                }
            },
        );
        assert!(err.contains("exactly one argument"), "{err}");
    }
}
