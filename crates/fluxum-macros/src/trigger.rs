//! Expansion of the SPEC-022 RV-031 declarative trigger attributes:
//! `#[fluxum::on_insert(Table)]` / `#[fluxum::on_update(Table)]` /
//! `#[fluxum::on_delete(Table)]`.
//!
//! Each expansion keeps the annotated function unchanged and submits a
//! `fluxum_core::reducer::TriggerDef` to the link-time registry, exactly
//! like reducers (RED-006). The generated glue decodes the store's row
//! values into the typed table struct and runs the hook **inside the same
//! transaction** as the mutation that fired it — an `Err` rolls the whole
//! transaction back.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{Ident, ItemFn, ReturnType};

/// Which trigger attribute is being expanded.
#[derive(Clone, Copy)]
pub enum Kind {
    Insert,
    Update,
    Delete,
}

impl Kind {
    fn attribute(self) -> &'static str {
        match self {
            Self::Insert => "on_insert",
            Self::Update => "on_update",
            Self::Delete => "on_delete",
        }
    }

    /// The `fluxum_core::store::TriggerKind` variant to submit.
    fn variant(self) -> TokenStream {
        match self {
            Self::Insert => quote!(Insert),
            Self::Update => quote!(Update),
            Self::Delete => quote!(Delete),
        }
    }

    /// Row parameters after the context: `on_update` receives `(old, new)`,
    /// the others one row.
    fn row_params(self) -> usize {
        match self {
            Self::Update => 2,
            Self::Insert | Self::Delete => 1,
        }
    }
}

/// Entry point: never panics, renders failures as `compile_error!`.
pub fn expand(kind: Kind, args: TokenStream, input: TokenStream) -> TokenStream {
    match try_expand(kind, args, input) {
        Ok(tokens) => tokens,
        Err(err) => err.to_compile_error(),
    }
}

fn try_expand(kind: Kind, args: TokenStream, input: TokenStream) -> syn::Result<TokenStream> {
    let attribute = kind.attribute();
    let args_span = args.span();
    let table: Ident = syn::parse2(args).map_err(|_| {
        syn::Error::new(
            args_span,
            format!("#[fluxum::{attribute}(Table)] names the watched #[fluxum::table] struct"),
        )
    })?;
    let item: ItemFn = syn::parse2(input)?;
    let params = crate::reducer::check_shape(&item, attribute)?;
    if params.len() != kind.row_params() {
        let shape = match kind {
            Kind::Insert | Kind::Delete => format!("(ctx: &ReducerContext, row: &{table})"),
            Kind::Update => format!("(ctx: &ReducerContext, old: &{table}, new: &{table})"),
        };
        return Err(syn::Error::new(
            item.sig.inputs.span(),
            format!("#[fluxum::{attribute}({table})] functions take {shape} (RV-031)"),
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
    let table_str = table.to_string();
    let glue_ident = format_ident!("__fx_trigger_{}", fn_ident);
    let variant = kind.variant();

    let missing =
        |which: &str| format!("{attribute} dispatched without {which} row (RV-031 invariant)");
    let body = match kind {
        Kind::Insert => {
            let msg = missing("a new");
            quote! {
                let __fx_row = __fx_new.ok_or_else(|| {
                    ::fluxum_core::FluxumError::Storage(::std::string::String::from(#msg))
                })?;
                let __fx_typed =
                    <#table as ::fluxum_core::schema::Table>::from_values(__fx_row.values())?;
                #fn_ident(ctx, &__fx_typed)
            }
        }
        Kind::Delete => {
            let msg = missing("an old");
            quote! {
                let __fx_row = __fx_old.ok_or_else(|| {
                    ::fluxum_core::FluxumError::Storage(::std::string::String::from(#msg))
                })?;
                let __fx_typed =
                    <#table as ::fluxum_core::schema::Table>::from_values(__fx_row.values())?;
                #fn_ident(ctx, &__fx_typed)
            }
        }
        Kind::Update => {
            let msg_old = missing("an old");
            let msg_new = missing("a new");
            quote! {
                let __fx_old_row = __fx_old.ok_or_else(|| {
                    ::fluxum_core::FluxumError::Storage(::std::string::String::from(#msg_old))
                })?;
                let __fx_new_row = __fx_new.ok_or_else(|| {
                    ::fluxum_core::FluxumError::Storage(::std::string::String::from(#msg_new))
                })?;
                let __fx_typed_old =
                    <#table as ::fluxum_core::schema::Table>::from_values(__fx_old_row.values())?;
                let __fx_typed_new =
                    <#table as ::fluxum_core::schema::Table>::from_values(__fx_new_row.values())?;
                #fn_ident(ctx, &__fx_typed_old, &__fx_typed_new)
            }
        }
    };

    Ok(quote! {
        #item

        const _: () = {
            #[allow(non_snake_case, unused_variables)]
            fn #glue_ident(
                ctx: &::fluxum_core::reducer::ReducerContext<'_, '_, '_>,
                __fx_old: ::core::option::Option<&::fluxum_core::store::Row>,
                __fx_new: ::core::option::Option<&::fluxum_core::store::Row>,
            ) -> ::fluxum_core::Result<()> {
                match { #body } {
                    Ok(_) => Ok(()),
                    Err(message) => Err(::fluxum_core::FluxumError::Reducer(
                        ::std::string::ToString::to_string(&message),
                    )),
                }
            }

            ::fluxum_core::schema::inventory::submit! {
                ::fluxum_core::reducer::TriggerDef {
                    table: #table_str,
                    kind: ::fluxum_core::store::TriggerKind::#variant,
                    name: #name_str,
                    handler: #glue_ident,
                }
            }
        };
    })
}
