use ggg_rs_derive::Concat;
use ndarray::{Array1, Array2, array};

#[derive(Concat, Debug, PartialEq)]
struct Dataset {
    ids: Vec<u64>,
    arr1d: Array1<f64>,
    arr2d: ndarray::Array2<f64>,
}

#[test]
fn concatenates_vec_and_array_fields() {
    let a = Dataset {
        ids: vec![10, 20],
        arr1d: array![1.0, 2.0],
        arr2d: array![[1.0, -1.0], [2.0, -2.0]],
    };
    let b = Dataset {
        ids: vec![30],
        arr1d: array![3.0],
        arr2d: array![[3.0, -3.0]],
    };
    let c = Dataset {
        ids: vec![40, 50, 60],
        arr1d: array![4.0, 5.0, 6.0],
        arr2d: array![[4.0, -4.0], [5.0, -5.0], [6.0, -6.0]],
    };

    let all = Dataset::concat(&[a, b, c]).unwrap();

    assert_eq!(all.ids, vec![10, 20, 30, 40, 50, 60]);
    assert_eq!(all.arr1d, array![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
    assert_eq!(
        all.arr2d,
        array![
            [1.0, -1.0],
            [2.0, -2.0],
            [3.0, -3.0],
            [4.0, -4.0],
            [5.0, -5.0],
            [6.0, -6.0]
        ]
    );
}

#[test]
fn single_item_is_unchanged() {
    let a = Dataset {
        ids: vec![1, 2, 3],
        arr1d: array![10.0, 20.0, 30.0],
        arr2d: array![[1.0], [2.0], [3.0]],
    };
    let expected = Dataset {
        ids: a.ids.clone(),
        arr1d: a.arr1d.clone(),
        arr2d: a.arr2d.clone(),
    };
    assert_eq!(Dataset::concat(&[a]).unwrap(), expected);
}

#[test]
fn empty_items_within_slice_contribute_nothing() {
    let a = Dataset {
        ids: vec![],
        arr1d: Array1::zeros(0),
        arr2d: Array2::zeros((0, 2)),
    };
    let b = Dataset {
        ids: vec![7],
        arr1d: array![7.0],
        arr2d: array![[7.0, 8.0]],
    };
    let all = Dataset::concat(&[a, b]).unwrap();
    assert_eq!(all.ids, vec![7]);
    assert_eq!(all.arr1d, array![7.0]);
    assert_eq!(all.arr2d, array![[7.0, 8.0]]);
}

#[test]
fn empty_slice_is_an_error() {
    let err = Dataset::concat(&[]).unwrap_err();
    assert!(matches!(err, DatasetConcatError::Empty), "got {err:?}");
    assert_eq!(err.to_string(), "cannot concatenate zero items");
    assert!(std::error::Error::source(&err).is_none());
}

#[test]
fn mismatched_array_shape_is_an_error() {
    let a = Dataset {
        ids: vec![1],
        arr1d: array![1.0],
        arr2d: array![[1.0, 2.0]],
    };
    // Second dimension differs (3 vs. 2), so axis 0 concatenation is impossible.
    let b = Dataset {
        ids: vec![2],
        arr1d: array![2.0],
        arr2d: array![[1.0, 2.0, 3.0]],
    };
    let err = Dataset::concat(&[a, b]).unwrap_err();
    match &err {
        DatasetConcatError::Shape { field, source } => {
            assert_eq!(*field, "arr2d");
            assert_eq!(source.kind(), ndarray::ErrorKind::IncompatibleShape);
        }
        other => panic!("expected Shape error, got {other:?}"),
    }
    assert!(
        err.to_string()
            .starts_with("cannot concatenate field `arr2d`: "),
        "got {err}"
    );
    let source = std::error::Error::source(&err).expect("Shape error should have a source");
    assert!(source.downcast_ref::<ndarray::ShapeError>().is_some());
}

type Sneaky = Vec<i32>;
type Matrix = Array2<i32>;

// Concatenate along a different axis of a 2D array and override vec vs. array
#[derive(Concat, Debug, PartialEq)]
struct UsingOptions {
    #[concat(kind = "vec")]
    v: Sneaky,
    #[concat(kind = "array")]
    m: Matrix,
    #[concat(axis = 1)]
    arr: Array2<i32>,
}

#[test]
fn test_field_options() {
    let a = UsingOptions {
        v: vec![1, 2],
        m: array![[1, 2]],
        arr: array![[10, 20], [100, 200]],
    };
    let b = UsingOptions {
        v: vec![3],
        m: array![[3, 4], [5, 6]],
        arr: array![[30], [300]],
    };
    let all = UsingOptions::concat(&[a, b]).unwrap();
    assert_eq!(all.v, vec![1, 2, 3]);
    assert_eq!(all.m, array![[1, 2], [3, 4], [5, 6]]);
    assert_eq!(all.arr, array![[10, 20, 30], [100, 200, 300]]);
}

#[derive(Concat, Debug)]
struct BadAxis {
    #[concat(axis = 2)]
    arr: Array2<i32>,
}

#[test]
fn axis_out_of_bounds_is_an_error() {
    let a = BadAxis { arr: array![[1]] };
    let b = BadAxis { arr: array![[2]] };
    match BadAxis::concat(&[a, b]).unwrap_err() {
        BadAxisConcatError::Shape { field, source } => {
            assert_eq!(field, "arr");
            assert_eq!(source.kind(), ndarray::ErrorKind::OutOfBounds);
        }
        other => panic!("expected Shape error, got {other:?}"),
    }
}

// Generic struct case
#[derive(Concat, Debug, PartialEq)]
struct Generic<T: Clone> {
    values: Vec<T>,
    arr1d: Array1<T>,
    arr2d: Array2<T>,
}

#[test]
fn works_with_generics() {
    let s = |v: &str| v.to_string();
    let a = Generic {
        values: vec![s("a")],
        arr1d: array![s("A")],
        arr2d: array![[s("a"), s("A")]],
    };
    let b = Generic {
        values: vec![s("b"), s("c")],
        arr1d: array![s("B"), s("C")],
        arr2d: array![[s("b"), s("B")], [s("c"), s("C")]],
    };
    let all = Generic::concat(&[a, b]).unwrap();
    assert_eq!(all.values, vec![s("a"), s("b"), s("c")]);
    assert_eq!(all.arr1d, array![s("A"), s("B"), s("C")]);
    assert_eq!(
        all.arr2d,
        array![[s("a"), s("A")], [s("b"), s("B")], [s("c"), s("C")]]
    );
}
