//! `#[derive(FluxType)]` — rich column types (SPEC-023 DMX-030).
//!
//! Admits tagged-union enums (variants carrying payloads) and nested structs
//! as `#[fluxum::table]` columns. The derive generates an implementation of
//! `fluxum_core::schema::FluxTypeDef`: the `FLUX_TYPE` column descriptor plus
//! the value bridge to/from the store's dynamic `RowValue`. FluxBIN encodes an
//! enum as a `u8` variant tag followed by the variant payload, and a struct as
//! its fields in declaration order — reusing the same per-type conversions the
//! `#[fluxum::table]` macro uses for columns.

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::spanned::Spanned;
use syn::{Data, DataEnum, DeriveInput, Fields, Ident};

use crate::table::{from_row_value, parse_flux_type, to_row_value};

pub(crate) fn expand(input: TokenStream) -> TokenStream {
    try_expand(input).unwrap_or_else(syn::Error::into_compile_error)
}

fn try_expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;
    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(syn::Error::new(
            input.generics.span(),
            "#[derive(FluxType)] does not support generic types (SPEC-023 DMX-030)",
        ));
    }
    let name = &input.ident;
    let name_str = name.to_string();
    let body = match &input.data {
        Data::Struct(data) => expand_struct(&name_str, &data.fields)?,
        Data::Enum(data) => expand_enum(&name_str, data)?,
        Data::Union(_) => {
            return Err(syn::Error::new(
                input.span(),
                "#[derive(FluxType)] cannot be derived for unions (SPEC-023 DMX-030)",
            ));
        }
    };
    let Body {
        flux_type,
        to_row_value,
        from_row_value,
    } = body;
    Ok(quote! {
        #[automatically_derived]
        impl ::fluxum_core::schema::FluxTypeDef for #name {
            const FLUX_TYPE: ::fluxum_core::schema::FluxType = #flux_type;

            fn to_row_value(self) -> ::fluxum_core::store::RowValue {
                #to_row_value
            }

            fn from_row_value(
                value: &::fluxum_core::store::RowValue,
            ) -> ::core::result::Result<Self, ::fluxum_core::FluxumError> {
                #from_row_value
            }
        }
    })
}

struct Body {
    flux_type: TokenStream,
    to_row_value: TokenStream,
    from_row_value: TokenStream,
}

fn bind(prefix: &str, i: usize) -> Ident {
    Ident::new(&format!("{prefix}{i}"), Span::call_site())
}

fn mismatch(name: &str, what: &str) -> TokenStream {
    let msg = format!("type `{name}`: {what} (SPEC-023 DMX-030)");
    quote! {
        return ::core::result::Result::Err(
            ::fluxum_core::FluxumError::Storage(::std::string::String::from(#msg))
        )
    }
}

fn expand_struct(name: &str, fields: &Fields) -> syn::Result<Body> {
    let Fields::Named(named) = fields else {
        return Err(syn::Error::new(
            fields.span(),
            "#[derive(FluxType)] on a struct requires named fields (SPEC-023 DMX-030)",
        ));
    };
    let mut field_schema = Vec::new();
    let mut to_items = Vec::new();
    let mut from_fields = Vec::new();
    let mut slice_binds = Vec::new();
    for (i, field) in named.named.iter().enumerate() {
        let Some(ident) = field.ident.as_ref() else {
            return Err(syn::Error::new(field.span(), "expected a named field"));
        };
        let fname = ident.to_string();
        let flux = parse_flux_type(&field.ty)?;
        let ty_tokens = flux.tokens();
        field_schema.push(quote! {
            ::fluxum_core::schema::FieldSchema { name: #fname, ty: #ty_tokens }
        });
        to_items.push(to_row_value(&flux, quote!(self.#ident)));
        let src = bind("__fx_f", i);
        let from_expr = from_row_value(&flux, quote!(#src), name, &fname);
        from_fields.push(quote!(#ident: #from_expr));
        slice_binds.push(src);
    }
    let arity = named.named.len();
    let bad_shape = mismatch(name, "value does not inhabit this struct");
    Ok(Body {
        flux_type: quote! {
            ::fluxum_core::schema::FluxType::Struct(&::fluxum_core::schema::StructSchema {
                name: #name,
                fields: &[ #(#field_schema),* ],
            })
        },
        to_row_value: quote! {
            ::fluxum_core::store::RowValue::Struct(::std::vec![ #(#to_items),* ])
        },
        from_row_value: quote! {
            match value {
                ::fluxum_core::store::RowValue::Struct(__fx_fields)
                    if __fx_fields.len() == #arity =>
                {
                    let [ #(#slice_binds),* ] = __fx_fields.as_slice() else { #bad_shape };
                    ::core::result::Result::Ok(Self { #(#from_fields),* })
                }
                _ => #bad_shape,
            }
        },
    })
}

fn expand_enum(name: &str, data: &DataEnum) -> syn::Result<Body> {
    if data.variants.is_empty() {
        return Err(syn::Error::new(
            data.enum_token.span(),
            "#[derive(FluxType)] enum must have at least one variant (SPEC-023 DMX-030)",
        ));
    }
    if data.variants.len() > usize::from(u8::MAX) + 1 {
        return Err(syn::Error::new(
            data.enum_token.span(),
            "#[derive(FluxType)] enum supports at most 256 variants (u8 FluxBIN tag) \
             (SPEC-023 DMX-030)",
        ));
    }
    let mut variant_schema = Vec::new();
    let mut to_arms = Vec::new();
    let mut from_arms = Vec::new();
    for (index, variant) in data.variants.iter().enumerate() {
        let tag = u32::try_from(index).unwrap_or(u32::MAX);
        let vident = &variant.ident;
        let vname = vident.to_string();
        match &variant.fields {
            Fields::Unit => {
                variant_schema.push(quote! {
                    ::fluxum_core::schema::VariantSchema { name: #vname, payload: &[] }
                });
                to_arms.push(quote! {
                    Self::#vident => ::fluxum_core::store::RowValue::Enum {
                        tag: #tag, payload: ::std::vec![],
                    }
                });
                let bad = mismatch(name, "unit variant carried a payload");
                from_arms.push(quote! {
                    #tag => {
                        if !__fx_payload.is_empty() { #bad }
                        ::core::result::Result::Ok(Self::#vident)
                    }
                });
            }
            Fields::Unnamed(unnamed) => {
                let mut payload_ty = Vec::new();
                let mut pat_binds = Vec::new();
                let mut to_vals = Vec::new();
                let mut slice_binds = Vec::new();
                let mut ctor_vals = Vec::new();
                for (j, field) in unnamed.unnamed.iter().enumerate() {
                    let flux = parse_flux_type(&field.ty)?;
                    payload_ty.push(flux.tokens());
                    let pb = bind("__fx_a", j);
                    to_vals.push(to_row_value(&flux, quote!(#pb)));
                    pat_binds.push(pb);
                    let sb = bind("__fx_p", j);
                    let col = format!("{vname}.{j}");
                    ctor_vals.push(from_row_value(&flux, quote!(#sb), name, &col));
                    slice_binds.push(sb);
                }
                variant_schema.push(quote! {
                    ::fluxum_core::schema::VariantSchema {
                        name: #vname, payload: &[ #(#payload_ty),* ],
                    }
                });
                to_arms.push(quote! {
                    Self::#vident( #(#pat_binds),* ) => ::fluxum_core::store::RowValue::Enum {
                        tag: #tag, payload: ::std::vec![ #(#to_vals),* ],
                    }
                });
                let bad = mismatch(name, "enum variant payload arity mismatch");
                from_arms.push(quote! {
                    #tag => {
                        let [ #(#slice_binds),* ] = __fx_payload.as_slice() else { #bad };
                        ::core::result::Result::Ok(Self::#vident( #(#ctor_vals),* ))
                    }
                });
            }
            Fields::Named(named) => {
                let mut payload_ty = Vec::new();
                let mut pat_fields = Vec::new();
                let mut to_vals = Vec::new();
                let mut slice_binds = Vec::new();
                let mut ctor_fields = Vec::new();
                for (j, field) in named.named.iter().enumerate() {
                    let Some(fident) = field.ident.as_ref() else {
                        return Err(syn::Error::new(field.span(), "expected a named field"));
                    };
                    let flux = parse_flux_type(&field.ty)?;
                    payload_ty.push(flux.tokens());
                    let pb = bind("__fx_a", j);
                    pat_fields.push(quote!(#fident: #pb));
                    to_vals.push(to_row_value(&flux, quote!(#pb)));
                    let sb = bind("__fx_p", j);
                    let col = format!("{vname}.{fident}");
                    let from_expr = from_row_value(&flux, quote!(#sb), name, &col);
                    ctor_fields.push(quote!(#fident: #from_expr));
                    slice_binds.push(sb);
                }
                variant_schema.push(quote! {
                    ::fluxum_core::schema::VariantSchema {
                        name: #vname, payload: &[ #(#payload_ty),* ],
                    }
                });
                to_arms.push(quote! {
                    Self::#vident { #(#pat_fields),* } => ::fluxum_core::store::RowValue::Enum {
                        tag: #tag, payload: ::std::vec![ #(#to_vals),* ],
                    }
                });
                let bad = mismatch(name, "enum variant payload arity mismatch");
                from_arms.push(quote! {
                    #tag => {
                        let [ #(#slice_binds),* ] = __fx_payload.as_slice() else { #bad };
                        ::core::result::Result::Ok(Self::#vident { #(#ctor_fields),* })
                    }
                });
            }
        }
    }
    let bad_tag = mismatch(name, "enum variant tag out of range");
    let bad_shape = mismatch(name, "value is not an enum");
    Ok(Body {
        flux_type: quote! {
            ::fluxum_core::schema::FluxType::Enum(&::fluxum_core::schema::EnumSchema {
                name: #name,
                variants: &[ #(#variant_schema),* ],
            })
        },
        to_row_value: quote! {
            match self { #(#to_arms),* }
        },
        from_row_value: quote! {
            match value {
                ::fluxum_core::store::RowValue::Enum { tag: __fx_tag, payload: __fx_payload } => {
                    match *__fx_tag {
                        #(#from_arms)*
                        _ => #bad_tag,
                    }
                }
                _ => #bad_shape,
            }
        },
    })
}

#[cfg(test)]
mod tests {
    //! Rejection branches of `#[derive(FluxType)]`, probed on the expansion
    //! functions directly (the UI suite pins the end-to-end rendering).
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn expand_err(input: TokenStream) -> String {
        try_expand(input).expect_err("derive must fail").to_string()
    }

    #[test]
    fn generic_types_are_rejected() {
        let err = expand_err(quote! { struct G<T> { a: T } });
        assert!(err.contains("does not support generic types"), "{err}");

        let err = expand_err(quote! { struct W where u32: Copy { a: u32 } });
        assert!(err.contains("does not support generic types"), "{err}");
    }

    #[test]
    fn unions_are_rejected() {
        let err = expand_err(quote! { union U { a: u32 } });
        assert!(err.contains("cannot be derived for unions"), "{err}");
    }

    #[test]
    fn tuple_structs_are_rejected() {
        let err = expand_err(quote! { struct P(u32); });
        assert!(err.contains("requires named fields"), "{err}");
    }

    #[test]
    fn empty_and_oversized_enums_are_rejected() {
        let err = expand_err(quote! { enum Never {} });
        assert!(err.contains("at least one variant"), "{err}");

        let variants = (0..=256).map(|i| quote::format_ident!("V{i}"));
        let err = expand_err(quote! { enum Big { #(#variants),* } });
        assert!(err.contains("at most 256 variants"), "{err}");
    }

    #[test]
    fn nameless_named_fields_are_rejected_defensively() {
        // Unparseable via source text (named fields always carry idents), so
        // the defensive branches are probed on mutated syntax trees.
        let input: DeriveInput = syn::parse_quote! { struct S { a: u32 } };
        let Data::Struct(mut data) = input.data else {
            panic!("expected struct data");
        };
        if let Fields::Named(named) = &mut data.fields {
            named.named.first_mut().expect("field").ident = None;
        }
        let err = expand_struct("S", &data.fields)
            .err()
            .expect("must fail")
            .to_string();
        assert!(err.contains("expected a named field"), "{err}");

        let input: DeriveInput = syn::parse_quote! { enum E { A { x: u32 } } };
        let Data::Enum(mut data) = input.data else {
            panic!("expected enum data");
        };
        if let Fields::Named(named) = &mut data.variants.first_mut().expect("variant").fields {
            named.named.first_mut().expect("field").ident = None;
        }
        let err = expand_enum("E", &data)
            .err()
            .expect("must fail")
            .to_string();
        assert!(err.contains("expected a named field"), "{err}");
    }
}
