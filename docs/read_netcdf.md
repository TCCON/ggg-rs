Adds a `read_netcdf` associated function that builds a struct from a netCDF file.

# Summary

This will add one or both of the following methods to the type:

```ignore
pub fn read_netcdf(grp: &netcdf::Group<'_>) -> netcdf::Result<Self>
pub fn read_netcdf_context(grp: &netcdf::Group<'_>, ctx: &Ctx) -> netcdf::Result<Self>
```

The first is added if
- the `#[netcdf(context(...))]` annotation is not present on the struct,
- the `#[netcdf(context(...))]` annotation only specifies the `write` direction, or
- the `Ctx` type implements the [`Default`] trait and `default` is added to the
  annotation, e.g. `context(read = Ctx, default)`.

The second is added if the `#[netcdf(context(read | ty = Ctx))]` annotation is present.

The following attributes modify the behavior of the macro:

**On the struct itself:**

- [`#[netcdf(context(...))]`](#context): modifies several functions used below to accept
  an additional argument of type `&Ctx` to allow passing runtime-dependent information
  into the writer. The type can be specified as `read = Ctx` to only apply to `ReadNetcdf`
  or `ty = Ctx` to define the same context type for `ReadNetcdf` and [`WriteNetcdf`].
  See [the section under WriteNetcdf](WriteNetcdf#context) and [below](#run-time-context)
  for details.
- `#[netcdf(rename_all = "...")]`: change how all fields map to variable names using
  serde-style case conventions. See [the section under WriteNetcdf](WriteNetcdf#rename_all)
  for details.
- [`#[netcdf(unpack(...))]`](#unpack): change how scales and offsets are applied to map
  integer values in the netCDF file back to float values in the Rust code.
- [`#[netcdf(on_fill = "...")]`](#on_fill): controls how fill values are handled in variables.
  The `on_fill(float = "...", int = "...")` form sets float and integer fields separately.

**On fields:**

Metadata-related annotations:
- `#[netcdf(dimensions("d0", "d1", ...))]` lists the expected dimension names for this variable. 
  Causes the reader to return an error if the dimensions do not match.
  See [the section under WriteNetcdf](WriteNetcdf#dimensions) for details.
- `#[netcdf(rename = "name")]`: set an explicit netCDF name.
  See [the section under WriteNetcdf](WriteNetcdf#rename) for details.

Annotations that alter how the field is read:
- `#[netcdf(attribute)]`: read this field from a group attribute, rather than a variable.
  The field type must implement [`TryFrom`] for [`netcdf::AttributeValue`].
- `#[netcdf(skip)]`: skip reading this field; the field type must implement [`Default`].

Annotations that customize the netCDF to Rust type mapping:

- [`#[netcdf(map(read = path::to::fn))]`](#map): transforms the value after it is read
  to map to Rust types not natively mapped to netCDF files. The function must have the
  signature `fn(NcTy, &str, &str, &netcdf::Variable<'_>) -> netcdf::Result<Out>`, and receives
  the value from the file, field name, netCDF variable name, and the variable from which
  the values were read. If a read context is present, this receives `&Ctx` as the fifth argument.
- [`#[netcdf(map_units(read = path::to::fn))]`](#map_units): like `map`, but receives the
  units of the values as the fifth argument, making the function signature
  `fn(NcTy, &str, &str, &netcdf::Variable<'_>, Option<&str>) -> netcdf::Result<Out>`.
  If a read context is present, this receives `&Ctx` as the sixth argument.
- `#[netcdf(kind = "values" | "array" | "string")]`: control how the values are
  read from the netCDF file; required with the `map` or `map_units` attributes.
  Otherwise, this is usually inferred. See [the section under WriteNetcdf](WriteNetcdf#kind) for details.
- [`#[netcdf(unpack(...))]`](#unpack-field-level): override a container-level
  `unpack`.
- [`#[netcdf(on_fill = "...")]`](#on_fill-field-level): override a container-level
  `on_fill`.
- [`#[netcdf(custom(read = path::to::fn))]`](#custom): provide a function that
  takes over reading the values. The function must have the signature
  `fn(&str, &str, &netcdf::Group<'_>) -> netcdf::Result<FieldTy>`, and receives
  the field name, netCDF variable name, and the containing group. If a read
  context is present, this receives `&Ctx` as the fourth argument.

# Usage

One or both of the following associated functions are added:

```ignore
pub fn read_netcdf(grp: &netcdf::Group<'_>) -> netcdf::Result<Self>
pub fn read_netcdf_context(grp: &netcdf::Group<'_>, ctx: &Ctx) -> netcdf::Result<Self>
```

As discussed [above](#summary), whether each one is added depends on the
presence of the `#[netcdf(context(...))]` annotation.

This derive is the counterpart to [`WriteNetcdf`] and shares several annotations:
`rename`, `rename_all`, `kind`, `dimensions`, `attribute`, and `skip` all
mean the same thing in both directions, so one set of annotations serves
both derives. See [`WriteNetcdf`] for definitions of those attributes.

Note that this is an *associated function*, not a method —
there is no `self` yet — so you call it as `Data::read_netcdf(&root)`.

Fields marked with `#[netcdf(skip)]` are filled in with [`Default::default()`],
thus a skipped field's type must implement [`Default`].

# Container annotations

`ReadNetcdf` accepts a subset of the container-level annotations that [`WriteNetcdf`]
does, specifically `context(...)` and `rename_all = "..."`. See the
[container annotations section of `WriteNetcdf`](WriteNetcdf#container-annotations)
for details on those.

## `unpack`

If the variable carries a `scale_factor` or `add_offset` attribute and
the field type has a visible `f32` or `f64` a. , the values read from the file are
transformed by `value * scale_factor + add_offset`. This is the inverse of what
[`WriteNetcdf`]'s `scale_offset` does, so a packed `short` variable arrives
as the physical `f64` quantity it represents.

`#[netcdf(unpack(...))]` controls this behavior. There are several forms:

- `unpack` by itself turns both scaling and offset on,
- `unpack = false` turns both off,
- `unpack(scale = true | false, offset = true | false)` allows control 
  of each separately; they default to `true` if unspecified.

Individual fields can override this with their own `unpack` annotations:

```no_run
use ggg_rs_derive::ReadNetcdf;

#[derive(ReadNetcdf)]
#[netcdf(unpack(offset = false))]
struct Data {
    // Scaled but not offset, inherited from the container.
    #[netcdf(dimensions("time"))]
    xco2: Vec<f64>,

    // Exactly as stored in the file.
    #[netcdf(dimensions("time"), unpack = false)]
    raw_counts: Vec<f64>,

    // Both halves, overriding the container.
    #[netcdf(dimensions("time"), unpack)]
    psurf: Vec<f64>,
}
```

**Note that a field-level `unpack` replaces, rather than merges with, the container-level attribute.**
In the above example, this means that setting `unpack(scale = false)` to a field would result
in that field having the offset _applied_, even though the container annotation disables that.

Unpacking only applies to `f32` and `f64` fields, because a fractional scale
factor cannot be applied to an integer without truncating it. Integer fields
are therefore passed through untouched, including under a container-level
`unpack` — a struct is free to mix the two. Asking for it on a specific
non-float field is a compile error, as is asking for it on a type alias the
macro cannot see inside.

**Note about combination with `map` or `map_units`:** a container-level `unpack`
has no effect on a field annotated with `map` or `map_units`, as it assumes that
the mapping function will handle that. To have the scale and/or offset applied
to what the mapping function receives, you _must_ annotate the field itself
with `unpack`.


## `on_fill`

A value matching the variable's fill value or its `missing_value` attribute
is detected *before* scaling — `_FillValue` is stored in the packed
representation, so once it has been multiplied by the scale factor it is
indistinguishable from real data. `on_fill` chooses what happens to it:

| Value | Float fields | Integer fields |
|---|---|---|
| `"nan"` | replace with `NaN`, leaving it unscaled (**float default**) | compile error: an integer cannot hold NaN |
| `"keep"` | leave the raw stored value in place, unscaled | leave the value as is (**integer default**) |
| `"ignore"` | do not look for fill values; scale them like any other value | same as `"keep"` |
| `"error"` | fail, naming the variable and the index | fail, naming the variable and the index |

On the container, it takes one of two forms:

- `#[netcdf(on_fill = "keep" | "ignore" | "error")]` applies to float and
  integer fields alike. `on_fill = "nan"` is a compile error here, since it
  cannot apply to integers.
- `#[netcdf(on_fill(float = "...", int = "..."))]` sets the two separately.
  Either key may be omitted, leaving that class at its default. This is the
  only way to write `"nan"` on the container, as `float = "nan"`.

On a field, only `on_fill = "..."` is accepted, and it replaces the
container's setting for that field.

### Which setting applies

Each field's behavior is resolved independently, taking the first of these
that exists:

1. the field's own `on_fill = "..."`;
2. the container's setting *for that field's class*: the single value from
   `on_fill = "..."`, or the matching `float`/`int` key from
   `on_fill(float = "...", int = "...")`;
3. the class default: `"nan"` for floats, `"keep"` for integers.

So the defaults apply when neither the field nor the container says anything
about that class. In particular, `on_fill(int = "error")` leaves float fields
at `"nan"`, and `on_fill(float = "error")` leaves integer fields at `"keep"`.

A field's class comes from its declared element type:

| Element type | Class |
|---|---|
| `f32`, `f64` | float |
| `i8`, `u8`, `i16`, `u16`, `i32`, `u32`, `i64`, `u64` | integer |
| anything else (strings, `bool`, `isize`/`usize`, 128-bit integers, type aliases) | none: fill values are never checked |

The element type is read from a `Vec<T>`, an `ndarray` `Array*<T>`, or a
scalar `T`. A type alias is opaque to the macro even if it names one of the
types above, so it falls in the last row. A container setting skips every
field in that row, but a field-level `on_fill` other than `"ignore"` on one
is a compile error rather than being silently ignored.

Fields annotated with `map` or `map_units` are an exception. Their declared
type is the map function's output, which says nothing about the type in the
file, so by default they are not checked at all: neither the container
setting nor the class default reaches them. Giving the field its own
`on_fill` or `unpack` opts it in. It is then treated as a float field, and
its setting is resolved as above (with `unpack` alone, that means the
container's `float` setting, or `"nan"`).

For example:

```no_run
use ggg_rs_derive::ReadNetcdf;

#[derive(ReadNetcdf)]
#[netcdf(on_fill(int = "error"))]
struct Data {
    // Float, no field setting, container sets no `float` key:
    // float default, "nan".
    #[netcdf(dimensions("time"))]
    xco2: Vec<f64>,

    // Integer, no field setting: container `int`, "error".
    #[netcdf(dimensions("time"))]
    flag: Vec<i32>,

    // Integer, field setting wins over the container: "keep".
    #[netcdf(dimensions("time"), on_fill = "keep")]
    count: Vec<u16>,
}
```

A second, larger example combining the container and field forms:

```no_run
use ggg_rs_derive::ReadNetcdf;

#[derive(ReadNetcdf)]
#[netcdf(on_fill(float = "error", int = "error"))]
struct Data {
    // A gap in this variable means the file is not usable,
    // so we keep the container behavior of "error".
    #[netcdf(dimensions("time"))]
    xco2: Vec<f64>,

    // Here gaps are expected, so they become NaN.
    #[netcdf(dimensions("time"), on_fill = "nan")]
    psurf: Vec<f64>,

    // A quality flag that must never be missing.
    #[netcdf(dimensions("time"))]
    flag: Vec<i32>,
}
```

Integer values are compared with the fill value exactly, so 64-bit values
close to the fill value are not mistaken for it.

Two details that will eventually surprise someone:

* **An undeclared fill value still counts.** The fill value comes from
  netCDF itself (`nc_inq_var_fill`), which reports the *type's default* —
  `-32767` for `short`, and so on — for any variable that never wrote a
  `_FillValue` attribute. Such values are masked. This matches
  netCDF4-python, whose auto-masking falls back to `default_fillvals` the
  same way. If a variable legitimately uses that value as data, set
  `on_fill = "ignore"` for the field.
* **Byte variables are exempt from that fallback.** For an `i8`/`u8`
  variable, only an explicitly written `_FillValue` is honoured, following
  Unidata's advice that a byte range is too narrow to assume one of its
  values means "missing".

`missing_value` is read as an attribute and may list several sentinels.
`valid_range`, `valid_min`, and `valid_max` are not yet honoured.

# Field annotations

Some annotations are omitted here because they are described in the
[field annotations section of `WriteNetcdf`](WriteNetcdf#field-annotations).

## `map`

`#[netcdf(map(read = path::to::fn))]`: transforms the value after it is read.
The function takes the value from the file, the Rust field name, the netCDF
variable name, and the variable, and returns a [`netcdf::Result`] containing
the new type (which must match the structure field type):

`fn(NcTy, &str, &str, &netcdf::Variable<'_>) -> netcdf::Result<Out>`

With a read [context](#run-time-context), the function takes a final `&Ctx`
argument too.

Note that, unlike the corresponding functions for [`WriteNetcdf`], there is no
`&Self` argument, because `read_netcdf` is still constructing `Self` when the
function runs. Further, the function is fallible: the data comes from a file this
program did not necessarily write, so conversion could fail. The error type
is that from the netcdf crate. The variable instance is provided so that
you can access other information about the data (e.g., attributes, dimensions)
if needed.

## `map_units`

`#[netcdf(map_units(read = path::to::fn))]`: the given function will also
receive the variable's units. By default, the units are read from the "units" attribute,
but this can be overridden with the `units_attr` option to `map_units`, e.g.,
`#[netcdf(map_units(read = path::to::fn, units_attr = "long_units"))]`.
The signature of the function given to `read` is:

`fn(NcTy, &str, &str, &netcdf::Variable<'_>, Option<&str>) -> netcdf::Result<Out>`

plus a final `&Ctx` with a read context. The two `&str` arguments are the Rust
field name and the netCDF variable name, as for `map`, and the `Option<&str>` will contain the units string if the attribute
is present. If that attribute is not present, this will be `None`.
This is a convenience to avoid needing boilerplate to get the units
attribute from the variable instance in each `map_units` function.

As with `WriteNetcdf`, `kind` is required whenever a mapping applies,
since the type in the file no longer matches the field's declared type.

```no_run
use ggg_rs_derive::ReadNetcdf;


fn ppm_to_frac(
    values: Vec<f64>,
    name: &str,
    _nc_name: &str,
    _var: &netcdf::Variable<'_>,
    units: Option<&str>,
) -> netcdf::Result<Vec<f64>> {
    match units {
        Some("ppm") => Ok(values.into_iter().map(|v| v / 1.0e6).collect()),
        other => Err(netcdf::Error::Str(
            format!("field '{name}' expected units of 'ppm', found {other:?}")
        )),
    }
}

#[derive(ReadNetcdf)]
struct Data {
    #[netcdf(
        dimensions("time"),
        map_units(read = ppm_to_frac),
        kind = "values"
    )]
    xluft: Vec<f64>,
}
```

A `map(...)` that names only one direction is an error in the derive that
needs the other half, so a half-finished mapping is caught at compile time
rather than silently reading or writing the field unmapped. Supplying only
`write = ...` is therefore fine for a struct that derives `WriteNetcdf`
alone, and becomes an error as soon as `ReadNetcdf` is added.

## `unpack` (field level)

Individual fields can alter how they use the "scale_factor" and "add_offset"
attributes. See the [container level `unpack`](#unpack) for details.

## `on_fill` (field level)

Individual fields can alter how they handle fill values.
See the [container level `on_fill`](#on_fill) for details, and
[Which setting applies](#which-setting-applies) for how a field's setting
combines with the container's.

## `custom`

`#[netcdf(custom(read = path::to::fn))]` hands the whole read of a field to
a function, rather than reading it with the derived code and converting it
as `map` does. It is the counterpart to `custom(write = ...)` on
[`WriteNetcdf`]. The signature is

`fn(&str, &str, &netcdf::Group<'_>) -> netcdf::Result<FieldTy>`

where the two `&str` arguments are the Rust field name and the netCDF name
(after `rename`/`rename_all`), in that order. The only other option `custom`
can be combined with is `rename`. `kind`, `dimensions`, `attribute`, `map`,
`map_units`, `unpack`, and `on_fill` are errors on a `custom` field, and a
container `unpack`/`on_fill` skips it: any unpacking is the function's job.
Note that `custom` functions must accept an additional argument if the `context`
annotation is added to the structure; see [below](#run-time-context).

```no_run
use ggg_rs_derive::ReadNetcdf;

fn read_coords(
    _field: &str, nc_name: &str, grp: &netcdf::Group<'_>,
) -> netcdf::Result<Vec<(f64, f64)>> {
    let get = |suffix: &str| {
        let name = format!("{nc_name}_{suffix}");
        grp.variable(&name)
            .ok_or_else(|| netcdf::Error::NotFound(format!("variable: '{name}'")))?
            .get_values::<f64, _>(..)
    };
    Ok(get("lat")?.into_iter().zip(get("lon")?).collect())
}

#[derive(ReadNetcdf)]
struct Data {
    #[netcdf(custom(read = read_coords))]
    coords: Vec<(f64, f64)>,
}
```

### Run-time context

`custom`, `map`, and `map_units` functions sometimes need inputs that aren't
in the file or the struct, such as a lookup table loaded separately. Reading
can't get these from `Self`, since `Self` doesn't exist yet. This can be
addressed by adding the container level `#[netcdf(context(...))]` annotation.
As described [above](#summary), this changes the generated `read_netcdf`
function, and every `custom`, `map`, and `map_units` read function then takes
`&Ctx` as its final argument.

Only a single context value can be passed.
If many inputs are needed, you would bundle them together in one context struct
or tuple. A borrowing context type is written with `'_`, e.g.
`context(ty = "Lookup<'_>")`. 

[`netcdf::Result`]: https://docs.rs/netcdf/0.12.1/netcdf/type.Result.html
[`netcdf::AttributeValue`]: https://docs.rs/netcdf/0.12.1/netcdf/enum.AttributeValue.html