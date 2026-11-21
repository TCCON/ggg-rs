use darling::{FromDeriveInput, FromField, FromMeta, ast};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, quote_spanned};
use syn::GenericParam;
use syn::spanned::Spanned;

use crate::common_subset::{FieldKind, ValueKind, resolve_kind};

/// How a `#[concat(skip)]` field gets its value in the concatenated struct.
#[derive(Debug, Clone, Copy)]
enum SkipMode {
    /// Every item must have the same value, which is cloned from item 0.
    Equal,
    /// Item 0's value is cloned, whatever the other items hold.
    First,
}

impl FromMeta for SkipMode {
    /// A bare `skip` requires equality, the safe choice.
    fn from_word() -> darling::Result<Self> {
        Ok(Self::Equal)
    }

    fn from_string(value: &str) -> darling::Result<Self> {
        match value {
            "equal" => Ok(Self::Equal),
            "first" => Ok(Self::First),
            other => Err(darling::Error::custom(format!(
                "unknown skip mode \"{other}\", expected \"equal\" or \"first\""
            ))),
        }
    }
}

#[derive(FromField)]
#[darling(attributes(concat))]
struct ConcatField {
    ident: Option<syn::Ident>,
    ty: syn::Type,

    #[darling(default)]
    skip: Option<SkipMode>,

    #[darling(default)]
    kind: Option<String>,

    #[darling(default)]
    axis: Option<usize>,
}

#[derive(FromDeriveInput)]
#[darling(attributes(concat), supports(struct_named))]
struct ConcatInput {
    ident: syn::Ident,
    vis: syn::Visibility,
    generics: syn::Generics,
    data: ast::Data<darling::util::Ignored, ConcatField>,
}

/// Generate an expression concatenating the values yielded by `parts`, an
/// iterator over references to the `Vec`s or arrays to join.
fn combine(
    value: &ValueKind,
    parts: TokenStream2,
    ident: &syn::Ident,
    err: &syn::Ident,
) -> TokenStream2 {
    match value {
        ValueKind::Vec => quote! {
            #parts.flat_map(|v| v.iter().cloned()).collect()
        },
        ValueKind::NdArray { axis } => quote! {{
            let views: ::std::vec::Vec<_> = #parts.map(|v| v.view()).collect();
            ::ndarray::concatenate(::ndarray::Axis(#axis), &views).map_err(|source| {
                #err::Shape {
                    field: stringify!(#ident),
                    source,
                }
            })?
        }},
    }
}

pub(crate) fn expand(input: TokenStream) -> TokenStream {
    let derive_input = syn::parse_macro_input!(input as syn::DeriveInput);
    let parsed = match ConcatInput::from_derive_input(&derive_input) {
        Ok(p) => p,
        Err(e) => return e.write_errors().into(),
    };

    let name = &parsed.ident;
    let vis = &parsed.vis;
    let err = format_ident!("{}ConcatError", name);
    let fields = parsed
        .data
        .take_struct()
        .expect("darling's supports(struct_named) guarantees this")
        .fields;

    let mut field_inits = Vec::new();
    let mut eq_bounds = Vec::new();
    let mut errors = darling::Error::accumulator();

    for f in &fields {
        let ident = f.ident.clone().unwrap();
        let kind = resolve_kind(
            &ident,
            &f.ty,
            f.skip.is_some(),
            f.kind.as_deref(),
            f.axis,
            "concat",
        );
        let Some(kind) = errors.handle(kind) else {
            continue;
        };
        let init = match kind {
            FieldKind::Vec => combine(
                &ValueKind::Vec,
                quote!(items.iter().map(|it| &it.#ident)),
                &ident,
                &err,
            ),
            FieldKind::NdArray { axis } => combine(
                &ValueKind::NdArray { axis },
                quote!(items.iter().map(|it| &it.#ident)),
                &ident,
                &err,
            ),
            FieldKind::HashMap { value } => {
                let combined = combine(&value, quote!(parts.into_iter()), &ident, &err);
                quote! {{
                    let first_map = &items[0].#ident;
                    // Equal lengths plus every key of item 0 being present
                    // in each item means the key sets are identical.
                    for (i, it) in items.iter().enumerate().skip(1) {
                        if it.#ident.len() != first_map.len() {
                            return ::core::result::Result::Err(#err::KeyMismatch {
                                field: stringify!(#ident),
                                item: i,
                            });
                        }
                    }
                    first_map
                        .keys()
                        .map(|k| {
                            let mut parts = ::std::vec::Vec::with_capacity(items.len());
                            for (i, it) in items.iter().enumerate() {
                                match it.#ident.get(k) {
                                    ::core::option::Option::Some(v) => parts.push(v),
                                    ::core::option::Option::None => {
                                        return ::core::result::Result::Err(#err::KeyMismatch {
                                            field: stringify!(#ident),
                                            item: i,
                                        });
                                    }
                                }
                            }
                            ::core::result::Result::Ok((k.clone(), #combined))
                        })
                        .collect::<::core::result::Result<_, #err>>()?
                }}
            }
            FieldKind::Skip => match f.skip.expect("resolve_kind only returns Skip when set") {
                SkipMode::First => quote!(items[0].#ident.clone()),
                SkipMode::Equal => {
                    // Spanned on the type so a missing `PartialEq` points at this field.
                    let ty = &f.ty;
                    eq_bounds.push(quote_spanned!(ty.span()=> #ty: ::core::cmp::PartialEq));
                    quote! {{
                        let first = &items[0].#ident;
                        for (i, it) in items.iter().enumerate().skip(1) {
                            if it.#ident != *first {
                                return ::core::result::Result::Err(#err::Unequal {
                                    field: stringify!(#ident),
                                    item: i,
                                });
                            }
                        }
                        first.clone()
                    }}
                }
            },
        };
        field_inits.push(quote!(#ident: #init));
    }

    // Emit every field-level error at once instead of stopping at the first.
    if let Err(e) = errors.finish() {
        return e.write_errors().into();
    }

    let mut generics = parsed.generics.clone();
    for param in generics.params.iter_mut() {
        if let GenericParam::Type(type_param) = param {
            type_param.bounds.push(syn::parse_quote!(Clone));
        }
    }
    let where_preds = &mut generics.make_where_clause().predicates;
    for bound in eq_bounds {
        where_preds.push(syn::parse_quote!(#bound));
    }
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let err_doc = format!("The error returned by [`{name}::concat`].");

    let expanded = quote! {
        #[doc = #err_doc]
        #[derive(Debug)]
        #vis enum #err {
            /// `concat` was given an empty slice, so there is nothing to build `Self` from.
            Empty,
            /// The arrays in `field` could not be concatenated, usually because
            /// they differ in shape along an axis other than the concatenation axis.
            Shape {
                field: &'static str,
                source: ::ndarray::ShapeError,
            },
            /// The map in `field` of item `item` does not have the same set of keys
            /// as the map in item 0.
            KeyMismatch { field: &'static str, item: usize },
            /// The skipped `field` of item `item` is not equal to that of item 0.
            Unequal { field: &'static str, item: usize },
        }

        impl ::core::fmt::Display for #err {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                match self {
                    Self::Empty => write!(f, "cannot concatenate zero items"),
                    Self::Shape { field, source } => {
                        write!(f, "cannot concatenate field `{field}`: {source}")
                    }
                    Self::KeyMismatch { field, item } => write!(
                        f,
                        "field `{field}` of item {item} does not have the same keys as item 0"
                    ),
                    Self::Unequal { field, item } => write!(
                        f,
                        "field `{field}` of item {item} differs from item 0, \
                         but must be equal to concatenate"
                    ),
                }
            }
        }

        impl ::std::error::Error for #err {
            fn source(&self) -> ::core::option::Option<&(dyn ::std::error::Error + 'static)> {
                match self {
                    Self::Shape { source, .. } => ::core::option::Option::Some(source),
                    _ => ::core::option::Option::None,
                }
            }
        }

        impl #impl_generics #name #ty_generics #where_clause {
            pub fn concat(items: &[Self]) -> ::core::result::Result<Self, #err> {
                if items.is_empty() {
                    return ::core::result::Result::Err(#err::Empty);
                }
                ::core::result::Result::Ok(Self {
                    #(#field_inits,)*
                })
            }
        }
    };

    TokenStream::from(expanded)
}
