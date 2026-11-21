Adds a `subset(&[usize])` method to a struct to subset [`Vec`] or ndarray Array fields by indices.

# Summary

Add the following method to a struct:

```ignore
pub fn subset(&self, indices: &[usize]) -> Self
```

that returns a new instance of the struct with its fields subset to the given indices.

The following annotations change the behavior of the macro:

**On fields:**

- [`#[subset(skip)]`](#skip): mark this field not to be subset.
- [`#[subset(kind = "vec|array")]`](#kind): indicate how to subset this field,
  needed when the type cannot be inferred.
- [`#[subset(axis = i)]`](#axis): controls which axis an `ndarray` is subset on.

# Usage

In basic usage, annotating a structure with `#[derive(Subset)]` will implement the `.subset`
method. This requires that all fields be `Vec`, `ndarray` Array, or `HashMap` types. Specifically, the
types must be `Vec`, start with `Array`, or start with `ndarray::Array`. The default `.subset`
method will then subset multi-dimensional arrays along their first dimension:

```rust
use ggg_rs_derive::Subset;
use ndarray::{Array1, array};

#[derive(Subset)]
struct Data {
    v: Vec<i32>,
    a1: Array1<f32>,
    a2: ndarray::Array2<i64>
}

let orig = Data {
    v: vec![100, 200, 300],
    a1: array![10.0, 20.0, 30.0],
    a2: array![[1, -1], [2, -2], [3, -3]]
};

let sub = orig.subset(&[0, 2]);
assert_eq!(sub.v, vec![100, 300]);
assert_eq!(sub.a1, array![10., 30.]);
assert_eq!(sub.a2, array![[1, -1], [3, -3]]);
```

Note that `.subset()` will panic if any of the indices are out of bounds
for any of the fields.

`HashMap` fields will be recursed into, so vectors or arrays inside the map
will also be subset.

The inverse operation, joining several instances back into one, is [`Concat`].

# Field annotations

## `skip`

`subset(skip)` allows you to skip subsetting fields, either because they cannot
or should not be subset:

```rust
use ggg_rs_derive::Subset;

#[derive(Subset)]
struct Data {
    v: Vec<i32>,
    #[subset(skip)]
    s: &'static str
}

let orig = Data { v: vec![1, 2], s: "Hello, world!" };
let sub = orig.subset(&[1]);
assert_eq!(sub.v, vec![2]);
assert_eq!(sub.s, "Hello, world!");
```

## `kind`

`subset(kind = "vec|array")` allows you to indicate whether the field should be subset
as a `Vec` or `ndarray::Array*`. This is necessary if, for example, you have a type
alias:

```rust
use ggg_rs_derive::Subset;
use ndarray::array;

type NVector = Vec<f64>;
type Matrix = ndarray::Array2<f64>;

#[derive(Subset)]
struct Data {
    #[subset(kind = "vec")]
    v: NVector,

    #[subset(kind = "array")]
    a: Matrix
}

let orig = Data {
    v: vec![100.0, 200.0],
    a: array![[1., 10.], [2., 20.]]
};
let sub = orig.subset(&[0]);

assert_eq!(sub.v, vec![100.0]);
assert_eq!(sub.a, array![[1.0, 10.0]]);
```

## `axis`

`subset(axis = i)` allows you to subset along a different axis of an
`ndarray` than the first:

```rust
use ggg_rs_derive::Subset;
use ndarray::{Array2, array};

#[derive(Subset)]
struct Data {
    #[subset(axis = 1)]
    a: Array2<i32>
}

let orig = Data { a: array![[1, 2, 3, 4], [10, 20, 30, 40]] };
let sub = orig.subset(&[1, 3]);
assert_eq!(sub.a, array![[2, 4], [20, 40]]);
```