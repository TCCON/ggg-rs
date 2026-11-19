Adds a `write_netcdf` method that writes a struct's fields to a netCDF file.

# Summary

This will add one or both of following methods to the type:

```ignore
pub fn write_netcdf(&self, grp: &mut netcdf::GroupMut<'_>) -> netcdf::Result<()>
pub fn write_netcdf_context(&self, grp: &mut netcdf::GroupMut<'_>, ctx: &Ctx) -> netcdf::Result<()>
```

The first is added if
- the `#[netcdf(context(...))]` annotation is not present on the struct,
- the `#[netcdf(context(...))]` annotation only specifies the `read` direction, or
- the `Ctx` type implements the [`Default`] trait and `default` is added to the
  annotation, e.g. `context(write = Ctx, default)`.

The second is added if the `#[netcdf(context(write | ty = Ctx))]` annotation is present.

The following attributes modify the behavior of the macro:

**On the struct itself:**

- [`#[netcdf(context(...))]`](#context): modifies several functions used below to accept
  an additional argument of type `&Ctx` to allow passing runtime-dependent information
  into the writer. The type can be specified as `write = Ctx` to only apply to `WriteNetcdf`
  or `ty = Ctx` to define the same context type for `WriteNetcdf` and [`ReadNetcdf`].
- [`#[netcdf(rename_all = "...")]`](#rename_all): change how all fields map to variable names using
  serde-style case conventions.
- [`#[netcdf(describe)]`](#describe): write each field's doc comment as an attribute.
- [`#[netcdf(attrs(func = path::to::fn))]`](#attrs-struct-level): write one or more group-level attributes
  with a function that has the signature `fn(&Self, &mut netcdf::GroupMut<'_>) -> netcdf::Result<()>`.
  It gains a third argument of type `&Ctx` if the `context(write = Ctx)` annotation is present.

**On fields:**

Metadata-related annotations:
- [`#[netcdf(dimensions("d0", "d1", ...))]`](#dimensions):
  lists the dimension names for this variable. If omitted, dimension names will be inferred if needed.
- [`#[netcdf(rename = "name")]`](#rename): set an explicit netCDF name.

Annotations that alter how the field is written:
- [`#[netcdf(attribute)]`](#attribute): write this field as a group attribute, rather than a variable.
- [`#[netcdf(skip)]`](#skip): skip writing this field.

Attribute-related annotations:
- [`#[netcdf(describe)]`](#describeno_describe): control how the doc comment is written as an attribute for
  this field.
- [`#[netcdf(attr(name = "units", value = "K"))]`](#attr): add a static attribute to the variable.
  May be repeated.
- [`#[netcdf(attr(name = "...", func = path::to::fn))]`](#attr): add an attribute to the
  variable, computed from a function with the signature `fn(&Self, &FieldTy, &str) -> V`,
  which receives this struct, the field values, and field name.
- [`#[netcdf(attrs(func = path::to::fn))]`](#attrs-field-level): calls the given function to write attributes
  to the variable. The function must have the signature
  `fn(&Self, &FieldTy, &str, &str, &mut netcdf::VariableMut<'_>) -> netcdf::Result<()>`,
  it receives the structure, field values, field name, netCDF variable name,
  and netCDF variable. With `#[netcdf(context(write = Ctx))]`, it receives
  `&Ctx` as a sixth argument.

Annotations that customize the Rust -> netCDF type mapping:
- [`#[netcdf(map(write = path::to::fn))]`](#map): transform the field values before writing.
  The function must have the signature `fn(&Self, &FieldTy, &str) -> Out`; it receives
  the structure instance, field values, and field name and must return netCDF-compatible
  values.
- [`#[netcdf(map_units(write = path::to::fn))]`](#map_units): like `map`, but returns `(Out, Into<AttributeValue>)`,
  where the second element will be written to the "units" attribute.
- [`#[netcdf(kind = "values" | "array" | "string")]`](#kind): control how the values are
  written to the netCDF file; required with the `map` or `map_units` attributes.
  Otherwise, this is usually inferred.
- [`#[netcdf(scale_offset(scale = .., offset = .., nc_type = ".."))]`](#scale_offset): scale and/or
  offset the values to pack into a smaller type. Also writes the appropriate attributes
  for a reader to undo the scale/offset.
- [`#[netcdf(custom(write = path::to::fn))]`](#custom): provide a function to create and write
  the variable itself. The function must have the signature
  `fn(&Self, &FieldTy, &str, &str, &mut netcdf::GroupMut<'_>) -> netcdf::Result<()>`,
  and receives the structure, field values, field name, expected netCDF name,
  and the netCDF group. If `#[netcdf(context(write = Ctx))]` is present, it receives
  `&Ctx` as a sixth argument.

# Usage

The first supplied method,

```ignore
pub fn write_netcdf(&self, grp: &mut netcdf::GroupMut<'_>) -> netcdf::Result<()>
```

writes each field as a netCDF *variable* (or a *group attribute*) into the
given group. Pass the file root — `file.root_mut().unwrap()` — to write at
the top level, or a subgroup from `file.add_group(...)`. Any dimension named
by a field that does not yet exist in the file is created automatically,
sized from that field's length/shape.

By default the write strategy is inferred from each field's type:
`ndarray` `Array*` fields use [`netcdf::VariableMut::put`], `Vec`/slice fields
use [`netcdf::VariableMut::put_values`], `String`/`Vec<String>` fields use
[`netcdf::VariableMut::put_string`], and remaining scalars use
`put_value`. Variables are named after their fields unless renamed.
Dimensions can be specified with the `#[netcdf(dimensions(...))]` field attribute
as shown in the following example.

```no_run
use ggg_rs_derive::WriteNetcdf;
use ndarray::Array2;

#[derive(WriteNetcdf)]
#[netcdf(rename_all = "camelCase")]
struct Data {
    // 1-D data sharing the "time" dimension (created on first use).
    #[netcdf(dimensions("time"))]
    xco2: Vec<f64>,

    // 2-D array; dimensions map to axes in order.
    #[netcdf(dimensions("time", "level"))]
    prior_profile: Array2<f64>,

    // A per-observation string is written with put_string.
    #[netcdf(dimensions("time"))]
    spectrum_name: Vec<String>,
}

# fn main() -> netcdf::Result<()> {
let data = Data {
    xco2: vec![400.0, 401.0],
    prior_profile: Array2::zeros((2, 3)),
    spectrum_name: vec!["pa2004".into(), "pa2005".into()],
};
let mut file = netcdf::create("out.nc")?;
let mut root = file.root_mut().unwrap();
data.write_netcdf(&mut root)?;
# Ok(()) }
```

If omitted, they are inferred:

- For `Vec` types, the dimension name matches the variable name.
- For `ndarray::Array` types, the dimension names take the variable name plus `_0`, `_1`, etc.

Including this attribute is an error for scalar typed fields or fields written as attributes.

# Container annotations

## `context`

`#[netcdf(context(write = Ctx))]` modifies the generated code to pass
`&Ctx` to any functions injected via annotations. This is useful for
additional data or metadata required to write attributes, scale values,
etc. A simple example would be to include history from an input file:

```no_run
use ggg_rs_derive::WriteNetcdf;

#[derive(WriteNetcdf)]
#[netcdf(
    context(write = String),
    attrs(func = add_history)
)]
struct Data {
    #[netcdf(dimensions("time"))]
    xco2: Vec<f64>
}

fn add_history(data: &Data, grp: &mut netcdf::GroupMut<'_>, hist: &String) -> netcdf::Result<()> {
    grp.add_attribute("history", format!("{hist}\nFile out.nc created with context"))
        .map(|_| ())
}

let history = "File demo.nc created as an example".to_string();
let data_out = Data { xco2: vec![400.0, 401.0, 402.0] };

let mut file = netcdf::create("out.nc").unwrap();
let mut root = file.root_mut().unwrap();
data_out.write_netcdf_context(&mut root, &history).unwrap();
```

## `rename_all`

`#[netcdf(rename_all = "...")]` applies a serde-style case convention
(`snake_case`, `SCREAMING_SNAKE_CASE`, `kebab-case`, `camelCase`,
`PascalCase`, `lowercase`, `UPPERCASE`, `SCREAMING-KEBAB-CASE`) to every
field's netCDF name.

## `describe`

`#[netcdf(describe)]` — write each field's doc comment as a variable
attribute. The bare word uses the attribute name `"description"`; the
`#[netcdf(describe = "long_name")]` form chooses a different attribute name.
Fields with no doc comment contribute nothing, and individual fields can
override the attribute name or opt out (see the field attributes below).

```no_run
use ggg_rs_derive::WriteNetcdf;

#[derive(WriteNetcdf)]
#[netcdf(describe = "long_name")]
struct Data {
    /// Column-averaged dry-air mole fraction of CO2
    // Writes a `long_name` attribute with the text above.
    #[netcdf(dimensions("time"))]
    xco2: Vec<f64>,
}
```

## `attrs` (struct level)

`#[netcdf(attrs(func = path::to::fn))]` writes any number of *group*
attributes with a function:

`fn(&Self, &mut netcdf::GroupMut<'_>) -> netcdf::Result<()>`

With a container `context`, the function takes a final `&Ctx` argument too.
It runs after every field has been written, including the variables' data,
so it can read the variables and dimensions already in the group to derive
attribute values. `attrs(...)` may be repeated; the functions run in the
order written. `ReadNetcdf` ignores it.

```no_run
use ggg_rs_derive::WriteNetcdf;

fn global_attrs(data: &Data, grp: &mut netcdf::GroupMut<'_>) -> netcdf::Result<()> {
    grp.add_attribute("n_obs", data.xco2.len() as i32)?;
    grp.add_attribute("conventions", "CF-1.8")?;
    Ok(())
}

#[derive(WriteNetcdf)]
#[netcdf(attrs(func = global_attrs))]
struct Data {
    #[netcdf(dimensions("time"))]
    xco2: Vec<f64>,
}
```

# Field annotations

## `dimensions`

`#[netcdf(dimensions("d0", "d1", ...))]` is required, unless `netcdf(attribute)`
is present. It lists the dimension
names that map to each dimension of the field. `Vec` types only need a single
dimension, while `ndarray` multidimensional arrays need one per dimension.
The dimensions will be created automatically if needed, but note that dimensions
from parent groups will be inherited. That is, if a dimension "time" in the root
group has length 5, and you declare that a field in the "support" subgroup needs
a time dimension with length 10, you will get an error.

```no_run
use ggg_rs_derive::WriteNetcdf;
use ndarray::Array2;

#[derive(WriteNetcdf)]
struct Data {
    // Vec types need 1 dimension
    #[netcdf(dimensions("time"))]
    v: Vec<f32>,

    // So do 1D arrays, note that the type can be prefixed
    // with `ndarray`
    #[netcdf(dimensions("time"))]
    a1: ndarray::Array1<f64>,

    // 2D arrays need two dimensions, "frame" will be the `Axis(0)`
    // length, "footprint" the `Axis(1)` length.
    #[netcdf(dimensions("frame", "footprint"))]
    a2: Array2<i64>
}
```

## `rename`

`#[netcdf(rename = "name")]` allows you to set an explicit variable/attribute name.
 It overrides `rename_all`, if that is present on the struct.

```no_run
use ggg_rs_derive::WriteNetcdf;

#[derive(WriteNetcdf)]
#[netcdf(rename_all = "camelCase")]
struct Data {
    // This data will be written to the "utc_time" variable
    // instead of a "timestamps" variables.
    #[netcdf(dimensions("time"), rename = "utc_time")]
    timestamps: Vec<i64>,

    // This would be written to "wmoScale" because of the
    // struct-level `rename_all`
    #[netcdf(dimensions("time"))]
    wmo_scale: Vec<String>
}
```

## `skip`

`#[netcdf(skip)]` will skip writing this field.

```no_run
use ggg_rs_derive::WriteNetcdf;

#[derive(WriteNetcdf)]
struct Data {
    // Only xco2 gets written, spectrum does not.
    #[netcdf(skip)]
    spectrum: Vec<String>,
    #[netcdf(dimensions("time"))]
    xco2: Vec<f64>
}
```

## `attribute`

`#[netcdf(attribute)]` indicates that the value of this field must be written
as a group attribute, not a variable. This allows you to save scalar metadata
as attributes, rather than scalar variables.

```no_run
use ggg_rs_derive::WriteNetcdf;

#[derive(WriteNetcdf)]
struct Data {
    #[netcdf(dimensions("time"))]
    xco2: Vec<f64>,
    // Creates a group attribute called `doi` containing this string.
    #[netcdf(attribute)]
    doi: String,
}
```

## `describe`/`no_describe`

`#[netcdf(describe)]` / `#[netcdf(describe = "name")]` / `#[netcdf(no_describe)]`
overrides the container-level `describe` for a single field, in the same way
`rename` overrides `rename_all`. The bare word turns descriptions on for this
field (using the inherited attribute name), the `= "name"` form also changes
the attribute name, and `no_describe` opts the field out even when the
container enables descriptions for everything.

```no_run
use ggg_rs_derive::WriteNetcdf;

#[derive(WriteNetcdf)]
#[netcdf(describe = "long_name")]
struct Data {
    /// Column-averaged dry-air mole fraction of CO2
    // Written to the "long_name" attribute (inherited from the container).
    #[netcdf(dimensions("time"))]
    xco2: Vec<f64>,

    /// Surface pressure
    // This field overrides the attribute name to "comment".
    #[netcdf(dimensions("time"), describe = "comment")]
    psurf: Vec<f64>,

    /// Internal bookkeeping counter
    // No description attribute is written for this field.
    #[netcdf(dimensions("time"), no_describe)]
    counter: Vec<i32>,
}
```

## `attr`

`#[netcdf(attr(name = "units", value = "K"))]` will attach a constant attribute
  to the variable. Use `attr(name = "...", func = path::to::fn)` if the attribute
  needs to be computed. The function must take 3 arguments, a reference to the struct,
  a reference to the field values, and the field name: `fn(&Self, &FieldTy, &str) -> V`
 `V` must implement `Into<AttributeValue>`. You can specify multiple attributes.

```no_run
use ggg_rs_derive::WriteNetcdf;

// This function only uses the field name, but it could use the values
// of the field or the entire structure - the latter can be useful if
// you need to distill an attribute from a separate field. Here we use
// generics without trait bounds so that this function could apply to
// any field.
fn infer_long_name<'s, D, V>(_data: &D, _values: &V, name: &'s str) -> &'s str {
    match name {
        "xco2" => "carbon_dioxide_column_average",
        _ => name,
    }
}

#[derive(WriteNetcdf)]
struct Data {
    // spectrum will have two attributes: `description` (string) and `public` (integer)
    #[netcdf(
        dimensions("time"),
        attr( name = "description", value = "name of the near-IR spectrum" ),
        attr( name = "public", value = 0 )
    )]
    spectrum: Vec<String>,

    // xco2 will have three attributes: `description` (string), `public` (integer), and
    // long_name (string).
    #[netcdf(
        dimensions("time"),
        attr( name = "description", value = "column average mole ratio of CO2 to dry air" ),
        attr( name = "public", value = 1 ),
        attr( name = "long_name", func = infer_long_name ),
    )]
    xco2: Vec<f64>
}
```

## `attrs` (field level)


`#[netcdf(attrs(func = path::to::fn))]` writes any number of attributes onto
the field's variable with one function, for sets of attributes computed
together or whose names are only known at run time. Rather than returning a
value, the function writes to the variable directly:

`fn(&Self, &FieldTy, &str, &str, &mut netcdf::VariableMut<'_>) -> netcdf::Result<()>`

The two `&str` arguments are the Rust field name and the netCDF name, in that
order. With a container `context`, the function takes a final `&Ctx`
argument too. It runs after every other attribute source (the description,
packing attributes, `attr(...)`, and `map_units` units), so it can overwrite
any of them. `attrs(...)` may be repeated; the functions run in the order
written, and it can be combined with `attr(...)`. It is an error with `skip`,
with `attribute` (there is no variable to write onto), and with `custom`
(whose function can write its own attributes). `ReadNetcdf` ignores it.

The variable's values have already been written when the function runs, so
it can read them back from `var` if that's more convenient than using the
field value. One consequence: `_FillValue` cannot be set this way, because
netCDF requires it before any data is written.

```no_run
use ggg_rs_derive::WriteNetcdf;

fn xco2_attrs(
    _data: &Data, values: &[f64], _field: &str, _nc_name: &str,
    var: &mut netcdf::VariableMut<'_>,
) -> netcdf::Result<()> {
    let (lo, hi) = values.iter().fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), &v| {
        (lo.min(v), hi.max(v))
    });
    var.put_attribute("units", "ppm")?;
    var.put_attribute("valid_min", lo)?;
    var.put_attribute("valid_max", hi)?;
    Ok(())
}

#[derive(WriteNetcdf)]
struct Data {
    #[netcdf(dimensions("time"), attrs(func = xco2_attrs))]
    xco2: Vec<f64>,
}
```

## `map`

`#[netcdf(map(write = path::to::fn))]` allows you to define a function to transform
 the value before writing. The function must take 3 arguments, a reference to the struct,
 a reference to the field values, and the field name: `fn(&Self, &FieldTy, &str) -> Out`.
 This allows you to work around types that cannot be directly written to netCDF files,
 such as [`chrono::DateTime`] instances. Note that this must be combined with the "kind"
 attribute, as the macro will not be able to infer the output type.

The direction is part of the syntax and cannot be omitted. Reading needs the
*inverse* function, so `ReadNetcdf` looks for `map(read = ...)` instead — see
the [`ReadNetcdf`] docs. A struct that derives both puts both halves in the
same `map(...)`: `map(write = to_nc, read = from_nc)`.

```no_run
use ggg_rs_derive::WriteNetcdf;
use chrono::{DateTime, Utc};

fn datetime_to_timestamp(_data: &Data, times: &[DateTime<Utc>], _name: &str) -> Vec<i64> {
    times.iter().map(|t| t.timestamp()).collect()
}

#[derive(WriteNetcdf)]
struct Data {
    // `DateTime<_>` doesn't implement `NcTypeDescriptor`, so we have to change it
    // to a type that does. Note that we also provide the "kind" attribute.
    #[netcdf(dimensions("time"), map(write = datetime_to_timestamp), kind = "values")]
    times: Vec<DateTime<Utc>>
}
```

## `map_units`

`#[netcdf(map_units(write = path::to::fn))]` is like `map`, but the function returns
`(Out, String)` and the returned string is written as the `units` attribute.
This version can also accept a `units_attr` option to change which attribute the units
are written to; by default, they are written to "units".

```no_run
use ggg_rs_derive::WriteNetcdf;
use chrono::{DateTime, Utc};

fn datetime_to_timestamp(_data: &Data, times: &[DateTime<Utc>], _name: &str) -> (Vec<i64>, &'static str) {
    let timestamps = times.iter().map(|t| t.timestamp()).collect();
    (timestamps, "seconds since 1970-01-01 00:00:00")
}

#[derive(WriteNetcdf)]
struct Data {
    // This is a better version of the above example, because the times variable
    // now gets the right units.
    #[netcdf(dimensions("time"), map_units(write = datetime_to_timestamp), kind = "values")]
    times: Vec<DateTime<Utc>>
}
```

## `kind`

`#[netcdf(kind = "values" | "array" | "string")]` sets how the field is written;
  "values" uses the [`netcdf::VariableMut::put_values`] method, "array" the
  [`netcdf::VariableMut::put`] method, and "string" will iterate over the elements,
  calling [`netcdf::VariableMut::put_string`] for each. This is needed when a type
  alias prevents inference of the type from its name, or when you use the `map` or
  `map_units` attribute to change the type.

```no_run
use ggg_rs_derive::WriteNetcdf;

type Matrix = ndarray::Array2<f32>;

#[derive(WriteNetcdf)]
struct Data {
    // The macro doesn't know that `Matrix` is actually
    // an array, so it tries to write it as a scalar by default,
    // which fails at compilation. The `kind` attribute tells it
    // to treat this as an array.
    #[netcdf(dimensions("obs", "obs"), kind = "array")]
    covariance: Matrix
}
```

## `scale_offset`

`#[netcdf(scale_offset(scale = .., offset = .., nc_type = ".."))]` packs the
field into a smaller on-disk type, following the netCDF attribute conventions.
Values are stored as `(value - offset) / scale`, and the `scale_factor` and
`add_offset` attributes are written so that a reader — including
[`ReadNetcdf`], which inverts this automatically — can recover the original
numbers.

`scale` and `offset` are individually optional, but at least one is required.
`nc_type` is mandatory and names the packed type (`"i8"`, `"u8"`, `"i16"`,
`"u16"`, `"i32"`, `"u32"`, `"i64"`, `"u64"`, `"f32"`, `"f64"`): without it the
variable would keep the field's own type and packing would save nothing.

```no_run
use ggg_rs_derive::WriteNetcdf;

#[derive(WriteNetcdf)]
struct Data {
    // Stored as `short xco2(time)` with 0.01 ppm resolution around a 400 ppm
    // baseline — a quarter the size of the f64 values in memory.
    #[netcdf(
        dimensions("time"),
        scale_offset(scale = 0.01, offset = 400.0, nc_type = "i16")
    )]
    xco2: Vec<f64>,
}
```

Three things worth knowing:

* The field's element type must be `f32` or `f64`. Packing an integer field
  has nothing to divide, so it is a compile error, as is a type alias the
  macro cannot see inside (use a concrete `Vec<f64>` or `Array<f64, _>`).
* Values are rounded with `round_ties_even`, matching `numpy.around` as used
  by netCDF4-python's packing, and a value that does not fit the packed type
  is a runtime error rather than a silently clamped one.
* `scale_offset` cannot be combined with `map`/`map_units`: a mapped value's
  type is opaque to the macro, so it cannot check that what gets written is
  floating-point. Pack inside the map function instead.

## `custom`

`#[netcdf(custom(write = path::to::fn))]` goes a step further than `map`: rather
than converting the value and leaving the derived code to write it, the function
does the whole write itself. Use it when a field doesn't map onto a single
variable, e.g. it should be split across several variables or needs dimensions
the derive can't express. The signature is

`fn(&Self, &FieldTy, &str, &str, &mut netcdf::GroupMut<'_>) -> netcdf::Result<()>`

where the two `&str` arguments are the Rust field name and the netCDF name
(after `rename`/`rename_all`), in that order. As with `map`, the direction is
required; `ReadNetcdf` uses the `read = ...` half. The only other option
`custom` can be combined with is `rename`. Since the function owns the write,
`skip`, `kind`, `dimensions`, `attribute`, `map`, `map_units`, `scale_offset`, `attr`,
`attrs`, `describe`, and `no_describe` are errors on a `custom` field, and
a container `describe` skips it.

```no_run
use ggg_rs_derive::WriteNetcdf;

fn write_coords(
    _data: &Data, coords: &[(f64, f64)], _field: &str, nc_name: &str,
    grp: &mut netcdf::GroupMut<'_>,
) -> netcdf::Result<()> {
    let lats: Vec<f64> = coords.iter().map(|c| c.0).collect();
    let lons: Vec<f64> = coords.iter().map(|c| c.1).collect();
    grp.add_dimension("site", coords.len())?;
    grp.add_variable::<f64>(&format!("{nc_name}_lat"), &["site"])?.put_values(&lats, ..)?;
    grp.add_variable::<f64>(&format!("{nc_name}_lon"), &["site"])?.put_values(&lons, ..)?;
    Ok(())
}

#[derive(WriteNetcdf)]
struct Data {
    #[netcdf(custom(write = write_coords))]
    coords: Vec<(f64, f64)>,
}
```

A `custom` write function can also receive a value supplied at run time
through the container-level `context` annotation. With `context`, the method is:

`fn(&Self, &FieldTy, &str, &str, &mut netcdf::GroupMut<'_>, &Ctx) -> netcdf::Result<()>`



[`netcdf::VariableMut::put`]: https://docs.rs/netcdf/0.12.1/netcdf/struct.VariableMut.html#method.put
[`netcdf::VariableMut::put_values`]: https://docs.rs/netcdf/0.12.1/netcdf/struct.VariableMut.html#method.put_values
[`netcdf::VariableMut::put_string`]: https://docs.rs/netcdf/0.12.1/netcdf/struct.VariableMut.html#method.put_string
[`chrono::DateTime`]: https://docs.rs/chrono/0.4/chrono/struct.DateTime.html