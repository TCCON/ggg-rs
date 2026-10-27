use ggg_rs_derive::{Concat, Subset};
use ndarray::{Array1, array};

#[derive(Concat, Subset, Debug, Clone, PartialEq)]
struct WithSkips {
    v: Vec<i32>,
    a: Array1<f64>,
    #[concat(skip)]
    #[subset(skip)]
    scale: String,
    #[concat(skip = "equal")]
    #[subset(skip)]
    levels: Array1<f64>,
    #[concat(skip = "first")]
    #[subset(skip)]
    label: String,
}

fn item(v: i32, scale: &str, label: &str) -> WithSkips {
    WithSkips {
        v: vec![v],
        a: array![v as f64],
        scale: scale.to_string(),
        levels: array![1000.0, 500.0],
        label: label.to_string(),
    }
}

#[test]
fn equal_skip_fields_are_kept() {
    let all = WithSkips::concat(&[item(1, "X2019", "a"), item(2, "X2019", "b")]).unwrap();
    assert_eq!(all.v, vec![1, 2]);
    assert_eq!(all.a, array![1.0, 2.0]);
    assert_eq!(all.scale, "X2019");
    assert_eq!(all.levels, array![1000.0, 500.0]);
}

#[test]
fn skip_first_takes_first_value_unchecked() {
    let all = WithSkips::concat(&[item(1, "X", "first"), item(2, "X", "second")]).unwrap();
    assert_eq!(all.label, "first");
}

#[test]
fn unequal_skip_field_is_an_error() {
    let items = [
        item(1, "X2019", "a"),
        item(2, "X2019", "a"),
        item(3, "X2007", "a"),
    ];
    let err = WithSkips::concat(&items).unwrap_err();
    match &err {
        WithSkipsConcatError::Unequal { field, item } => {
            assert_eq!(*field, "scale");
            assert_eq!(*item, 2);
        }
        other => panic!("expected Unequal, got {other:?}"),
    }
    assert_eq!(
        err.to_string(),
        "field `scale` of item 2 differs from item 0, but must be equal to concatenate"
    );
}

#[test]
fn explicit_equal_mode_is_checked() {
    let mut bad = item(2, "X", "a");
    bad.levels = array![1000.0, 400.0];
    match WithSkips::concat(&[item(1, "X", "a"), bad]).unwrap_err() {
        WithSkipsConcatError::Unequal { field, item } => {
            assert_eq!(field, "levels");
            assert_eq!(item, 1);
        }
        other => panic!("expected Unequal, got {other:?}"),
    }
}

#[test]
fn inverts_subset() {
    let orig = WithSkips {
        v: vec![1, 2, 3, 4, 5],
        a: array![1.0, 2.0, 3.0, 4.0, 5.0],
        scale: "X".to_string(),
        levels: array![1.0],
        label: "l".to_string(),
    };
    let parts = [
        orig.subset(&[0, 1]),
        orig.subset(&[2]),
        orig.subset(&[3, 4]),
    ];
    assert_eq!(WithSkips::concat(&parts).unwrap(), orig);
}

// The struct itself doesn't require `T: PartialEq`; the derive adds that bound
// to the `concat` impl because `tag` is a skip-equal field.
#[derive(Concat, Debug)]
struct GenericSkip<T: Clone> {
    v: Vec<T>,
    #[concat(skip)]
    tag: T,
}

#[test]
fn generic_skip_field_gets_partial_eq_bound() {
    let a = GenericSkip {
        v: vec![1u8],
        tag: 7,
    };
    let b = GenericSkip {
        v: vec![2u8],
        tag: 9,
    };
    match GenericSkip::concat(&[a, b]).unwrap_err() {
        GenericSkipConcatError::Unequal { field, item } => {
            assert_eq!(field, "tag");
            assert_eq!(item, 1);
        }
        other => panic!("expected Unequal, got {other:?}"),
    }
}
