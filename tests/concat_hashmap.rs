use std::collections::HashMap;

use ggg_rs_derive::Concat;
use ndarray::{Array1, Array2, array};

#[derive(Debug, Concat)]
struct AllMaps {
    vec_map: HashMap<&'static str, Vec<i32>>,
    arr1_map: HashMap<&'static str, Array1<i32>>,
    #[concat(axis = 1)]
    arr2_map: HashMap<&'static str, Array2<i32>>,
}

fn maps(a: i32, b: i32) -> AllMaps {
    AllMaps {
        vec_map: HashMap::from_iter([("a", vec![a]), ("b", vec![b, b])]),
        arr1_map: HashMap::from_iter([("c", array![a]), ("d", array![b, b])]),
        arr2_map: HashMap::from_iter([("e", array![[a], [-a]]), ("f", array![[b], [-b]])]),
    }
}

#[test]
fn concat_maps() {
    let all = AllMaps::concat(&[maps(1, 2), maps(3, 4)]).unwrap();
    assert_eq!(
        all.vec_map,
        HashMap::from_iter([("a", vec![1, 3]), ("b", vec![2, 2, 4, 4])])
    );
    assert_eq!(
        all.arr1_map,
        HashMap::from_iter([("c", array![1, 3]), ("d", array![2, 2, 4, 4])])
    );
    assert_eq!(
        all.arr2_map,
        HashMap::from_iter([
            ("e", array![[1, 3], [-1, -3]]),
            ("f", array![[2, 4], [-2, -4]]),
        ])
    );
}

#[test]
fn missing_key_is_an_error() {
    let mut bad = maps(5, 6);
    bad.arr1_map.remove("d");
    match AllMaps::concat(&[maps(1, 2), maps(3, 4), bad]).unwrap_err() {
        AllMapsConcatError::KeyMismatch { field, item } => {
            assert_eq!(field, "arr1_map");
            assert_eq!(item, 2);
        }
        other => panic!("expected KeyMismatch, got {other:?}"),
    }
}

#[test]
fn extra_key_is_an_error() {
    let mut bad = maps(3, 4);
    bad.vec_map.insert("z", vec![0]);
    let err = AllMaps::concat(&[maps(1, 2), bad]).unwrap_err();
    match &err {
        AllMapsConcatError::KeyMismatch { field, item } => {
            assert_eq!(*field, "vec_map");
            assert_eq!(*item, 1);
        }
        other => panic!("expected KeyMismatch, got {other:?}"),
    }
    assert_eq!(
        err.to_string(),
        "field `vec_map` of item 1 does not have the same keys as item 0"
    );
}

#[test]
fn same_count_different_keys_is_an_error() {
    let mut bad = maps(3, 4);
    let v = bad.vec_map.remove("a").unwrap();
    bad.vec_map.insert("y", v);
    match AllMaps::concat(&[maps(1, 2), bad]).unwrap_err() {
        AllMapsConcatError::KeyMismatch { field, item } => {
            assert_eq!(field, "vec_map");
            assert_eq!(item, 1);
        }
        other => panic!("expected KeyMismatch, got {other:?}"),
    }
}

#[test]
fn mismatched_shape_in_map_is_an_error() {
    let mut bad = maps(3, 4);
    // 3 rows instead of 2, so axis 1 concatenation is impossible
    bad.arr2_map.insert("e", array![[3], [-3], [0]]);
    match AllMaps::concat(&[maps(1, 2), bad]).unwrap_err() {
        AllMapsConcatError::Shape { field, source } => {
            assert_eq!(field, "arr2_map");
            assert_eq!(source.kind(), ndarray::ErrorKind::IncompatibleShape);
        }
        other => panic!("expected Shape, got {other:?}"),
    }
}
