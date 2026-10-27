use darling::{FromDeriveInput, FromField, ast};
use proc_macro::TokenStream;
use quote::quote;
use syn::GenericParam;

use crate::common_subset::{FieldKind, ValueKind, resolve_kind};

#[derive(FromField)]
#[darling(attributes(subset))]
struct SubsetField {
    ident: Option<syn::Ident>,
    ty: syn::Type,

    #[darling(default)]
    skip: bool,

    #[darling(default)]
    kind: Option<String>,

    #[darling(default)]
    axis: Option<usize>,
}

#[derive(FromDeriveInput)]
#[darling(attributes(subset), supports(struct_named))]
struct SubsetInput {
    ident: syn::Ident,
    generics: syn::Generics,
    data: ast::Data<darling::util::Ignored, SubsetField>,
}

pub(crate) fn expand(input: TokenStream) -> TokenStream {
    let derive_input = syn::parse_macro_input!(input as syn::DeriveInput);
    let parsed = match SubsetInput::from_derive_input(&derive_input) {
        Ok(p) => p,
        Err(e) => return e.write_errors().into(),
    };

    let name = &parsed.ident;
    let fields = parsed
        .data
        .take_struct()
        .expect("darling's supports(struct_named) guarantees this")
        .fields;

    let mut vec_idents = Vec::new();
    let mut array_idents = Vec::new();
    let mut array_axes = Vec::new();
    let mut hashmap_vec_idents = Vec::new();
    let mut hashmap_array_idents = Vec::new();
    let mut hashmap_array_axes = Vec::new();
    let mut skip_idents = Vec::new();
    let mut errors = darling::Error::accumulator();

    for f in &fields {
        if let Some(kind) = errors.handle(resolve_kind(
            f.ident.as_ref().unwrap(),
            &f.ty,
            f.skip,
            f.kind.as_deref(),
            f.axis,
            "subset",
        )) {
            let ident = f.ident.clone().unwrap();
            match kind {
                FieldKind::Vec => vec_idents.push(ident),
                FieldKind::NdArray { axis } => {
                    array_idents.push(ident);
                    array_axes.push(axis);
                }
                FieldKind::HashMap {
                    value: ValueKind::Vec,
                } => hashmap_vec_idents.push(ident),
                FieldKind::HashMap {
                    value: ValueKind::NdArray { axis },
                } => {
                    hashmap_array_idents.push(ident);
                    hashmap_array_axes.push(axis);
                }
                FieldKind::Skip => skip_idents.push(ident),
            }
        }
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
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let expanded = quote! {
        impl #impl_generics #name #ty_generics #where_clause {
            pub fn subset(&self, indices: &[usize]) -> Self {
                Self {
                    #(
                        #vec_idents: indices.iter().map(|&i| self.#vec_idents[i].clone()).collect(),
                    )*
                    #(
                        #array_idents: self.#array_idents.select(::ndarray::Axis(#array_axes), indices),
                    )*
                    #(
                        #hashmap_vec_idents: self.#hashmap_vec_idents
                            .iter()
                            .map(|(k, v)| (
                                k.clone(),
                                indices.iter().map(|&i| v[i].clone()).collect(),
                            ))
                            .collect(),
                    )*
                    #(
                        #hashmap_array_idents: self.#hashmap_array_idents
                            .iter()
                            .map(|(k, v)| (
                                k.clone(),
                                v.select(::ndarray::Axis(#hashmap_array_axes), indices),
                            ))
                            .collect(),
                    )*
                    #(
                        #skip_idents: self.#skip_idents.clone(),
                    )*
                }
            }
        }
    };

    TokenStream::from(expanded)
}
