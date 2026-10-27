//! Field classification shared by the `Subset` and `Concat` derives, which
//! both operate on the same set of container types.

use syn::Type;

pub(crate) enum ValueKind {
    Vec,
    NdArray { axis: usize },
}

pub(crate) enum FieldKind {
    Vec,
    NdArray { axis: usize },
    HashMap { value: ValueKind },
    Skip,
}

/// Given a `Vec<T>` or `Array*<T>` type, classify it. Reused for both
/// top-level fields and HashMap values.
fn classify_container(
    ty: &Type,
    explicit_kind: Option<&str>,
    explicit_axis: Option<usize>,
) -> Result<ValueKind, darling::Error> {
    if let Some(kind) = explicit_kind {
        return match kind {
            "vec" => Ok(ValueKind::Vec),
            "array" => Ok(ValueKind::NdArray {
                axis: explicit_axis.unwrap_or(0),
            }),
            other => Err(darling::Error::custom(format!("unknown kind \"{other}\""))),
        };
    }
    if let Type::Path(type_path) = ty
        && let Some(seg) = type_path.path.segments.last()
    {
        let ident = seg.ident.to_string();
        if ident == "Vec" {
            return Ok(ValueKind::Vec);
        }
        if ident.starts_with("Array") {
            return Ok(ValueKind::NdArray {
                axis: explicit_axis.unwrap_or(0),
            });
        }
    }
    Err(darling::Error::custom("couldn't infer container kind"))
}

/// Extract the `V` type param out of `HashMap<K, V>`.
fn hashmap_value_type(ty: &Type) -> Option<&Type> {
    if let Type::Path(type_path) = ty {
        let seg = type_path.path.segments.last()?;
        if seg.ident != "HashMap" {
            return None;
        }
        if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
            // args: [K, V] — value is the second generic argument.
            if let Some(syn::GenericArgument::Type(v_ty)) = args.args.iter().nth(1) {
                return Some(v_ty);
            }
        }
    }
    None
}

/// Decide how a field is handled from its type and its `skip`/`kind`/`axis`
/// options. `attr` is the derive's attribute name (e.g. `"subset"`), used in
/// error messages that suggest an annotation.
pub(crate) fn resolve_kind(
    ident: &syn::Ident,
    ty: &Type,
    skip: bool,
    kind: Option<&str>,
    axis: Option<usize>,
    attr: &str,
) -> Result<FieldKind, darling::Error> {
    if skip {
        if kind.is_some() || axis.is_some() {
            return Err(
                darling::Error::custom("`skip` cannot be combined with `kind` or `axis`")
                    .with_span(ident),
            );
        }
        return Ok(FieldKind::Skip);
    }

    if let Some(kind) = kind {
        return match kind {
            "array" => Ok(FieldKind::NdArray {
                axis: axis.unwrap_or(0),
            }),
            "vec" => {
                if axis.is_some() {
                    return Err(darling::Error::custom(
                        "`axis` has no effect on `kind = \"vec\"` fields",
                    )
                    .with_span(ident));
                }
                Ok(FieldKind::Vec)
            }
            other => Err(darling::Error::custom(format!(
                "unknown kind \"{other}\", expected \"vec\" or \"array\""
            ))
            .with_span(ident)),
        };
    }

    // Handle HashMaps of vecs/arrays
    if let Some(value_ty) = hashmap_value_type(ty) {
        let value_kind =
            classify_container(value_ty, kind, axis).map_err(|e| e.with_span(ident))?;
        return Ok(FieldKind::HashMap { value: value_kind });
    }

    // Fallback: infer from the type's last path segment.
    if let Type::Path(type_path) = ty
        && let Some(seg) = type_path.path.segments.last()
    {
        let ty_ident = seg.ident.to_string();
        if ty_ident == "Vec" {
            if axis.is_some() {
                return Err(
                    darling::Error::custom("`axis` has no effect on inferred Vec fields")
                        .with_span(ident),
                );
            }
            return Ok(FieldKind::Vec);
        }
        if ty_ident.starts_with("Array") {
            return Ok(FieldKind::NdArray {
                axis: axis.unwrap_or(0),
            });
        }
    }

    Err(darling::Error::custom(format!(
        "couldn't infer field kind — annotate with #[{attr}(kind = \"vec\")], \
         #[{attr}(kind = \"array\")], or #[{attr}(skip)]"
    ))
    .with_span(ident))
}
