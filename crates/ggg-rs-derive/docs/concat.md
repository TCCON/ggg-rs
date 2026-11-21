Adds a `concat(&[Self])` associated function that joins instances of a struct
by concatenating their [`Vec`] or ndarray Array fields.

# Summary

This is the inverse of [`Subset`]. The generated function has the signature

```ignore
pub fn concat(items: &[Self]) -> Result<Self, {Struct}ConcatError>
```

where `{Struct}ConcatError` is an error enum generated alongside it (see
[Errors](#errors) below). The following annotations change the macro's
behavior:

**On fields:**

- [`#[concat(skip)]`](#skip): skip concatenating this field, also controls
  what to do instead.
- [`#[concat(kind = "...")]`](#kind): declare how to handle this field, e.g.
  as a vector, array, etc. Needed only if the type cannot be inferred.
- [`#[concat(axis = i)]`](#axis): controls along which axis multi-dimensional
  arrays are concatenated.

# Usage

As with `Subset`, fields must be `Vec`s, ndarray
`Array*`s, or `HashMap`s whose values are one of those. Multi-dimensional arrays
are concatenated along their first axis by default:

```rust
use ggg_rs_derive::Concat;
use ndarray::{Array1, array};

#[derive(Concat)]
struct Data {
    v: Vec<i32>,
    a1: Array1<f32>,
    a2: ndarray::Array2<i64>
}

let first = Data {
    v: vec![100, 200],
    a1: array![10.0, 20.0],
    a2: array![[1, -1], [2, -2]]
};
let second = Data {
    v: vec![300],
    a1: array![30.0],
    a2: array![[3, -3]]
};

let all = Data::concat(&[first, second]).unwrap();
assert_eq!(all.v, vec![100, 200, 300]);
assert_eq!(all.a1, array![10., 20., 30.]);
assert_eq!(all.a2, array![[1, -1], [2, -2], [3, -3]]);
```

For `HashMap` fields, every item must have the same set of keys; the values
for each key are concatenated.

# Field annotations

`Concat` reads its own `#[concat(...)]` attributes, so a struct deriving both
`Subset` and `Concat` needs both `#[subset(...)]` and `#[concat(...)]` on fields
that need options.

## `skip`

`concat(skip)` marks a field that is not concatenated. Every item must hold an
equal value for it, which is copied into the result, so the field's type must
implement [`PartialEq`] and [`Clone`]. `concat(skip = "equal")` is the same thing,
spelled out. Use `concat(skip = "first")` instead to take the first item's value
without checking the others (only [`Clone`] is required):

```rust
use ggg_rs_derive::Concat;

#[derive(Concat, Debug)]
struct Data {
    v: Vec<i32>,
    // Mixing data on different calibration scales would be an error.
    #[concat(skip)]
    scale: &'static str,
    // Only the first item's label is kept.
    #[concat(skip = "first")]
    label: &'static str,
}

let a = Data { v: vec![1], scale: "X2019", label: "a" };
let b = Data { v: vec![2], scale: "X2019", label: "b" };
let all = Data::concat(&[a, b]).unwrap();
assert_eq!(all.v, vec![1, 2]);
assert_eq!(all.scale, "X2019");
assert_eq!(all.label, "a");

let c = Data { v: vec![3], scale: "X2007", label: "c" };
let d = Data { v: vec![4], scale: "X2019", label: "d" };
assert!(Data::concat(&[c, d]).is_err());
```

## `kind`

`concat(kind = "vec|array")` indicates whether the field should be concatenated
as a `Vec` or `ndarray::Array*`, which is necessary if the type is an alias:

```rust
use ggg_rs_derive::Concat;
use ndarray::array;

type NVector = Vec<f64>;
type Matrix = ndarray::Array2<f64>;

#[derive(Concat)]
struct Data {
    #[concat(kind = "vec")]
    v: NVector,

    #[concat(kind = "array")]
    a: Matrix
}

let a = Data { v: vec![1.0], a: array![[1., 10.]] };
let b = Data { v: vec![2.0], a: array![[2., 20.]] };
let all = Data::concat(&[a, b]).unwrap();

assert_eq!(all.v, vec![1.0, 2.0]);
assert_eq!(all.a, array![[1., 10.], [2., 20.]]);
```

## `axis`

`concat(axis = i)` concatenates along a different axis of an `ndarray` than
the first:

```rust
use ggg_rs_derive::Concat;
use ndarray::{Array2, array};

#[derive(Concat)]
struct Data {
    #[concat(axis = 1)]
    a: Array2<i32>
}

let a = Data { a: array![[1, 2], [10, 20]] };
let b = Data { a: array![[3], [30]] };
let all = Data::concat(&[a, b]).unwrap();
assert_eq!(all.a, array![[1, 2, 3], [10, 20, 30]]);
```

# Errors

The generated `{Struct}ConcatError` enum has the same visibility as the struct,
implements [`std::error::Error`], and has these variants:

* `Empty`: `items` was an empty slice, so there is nothing to build from.
* `Shape { field, source }`: ndarray could not concatenate the arrays in `field`,
  usually because they differ in shape along an axis other than the concatenation
  axis, or the axis is out of bounds. `source` is the [`ndarray::ShapeError`].
* `KeyMismatch { field, item }`: the `HashMap` in `field` of item `item` does not
  have the same keys as item 0.
* `Unequal { field, item }`: the `skip` field `field` of item `item` is not
  equal to item 0's.

```rust
use ggg_rs_derive::Concat;
use ndarray::{Array2, array};

#[derive(Concat, Debug)]
struct Data {
    a: Array2<i32>
}

let a = Data { a: array![[1, 2]] };
let b = Data { a: array![[3, 4, 5]] };
match Data::concat(&[a, b]) {
    Err(DataConcatError::Shape { field, .. }) => assert_eq!(field, "a"),
    other => panic!("unexpected result: {other:?}"),
}
```

[`ndarray::ShapeError`]: https://docs.rs/ndarray/0.17.2/ndarray/struct.ShapeError.html