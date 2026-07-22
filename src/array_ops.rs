use itertools::Itertools;
use ndarray::{ArrayBase, Axis, Ix1};
use std::marker::PhantomData;
use uom::si::{Dimension as UomDimension, Quantity, Units};

/// A trait used to convert different size floats and [`uom`] Quantities to f64
/// values for array operations.
pub trait FloatConversion {
    /// Convert a value into an f64
    fn into_f64(self) -> f64;

    /// Convert an f64 back into this type
    fn from_f64(val: f64) -> Self;
}

impl FloatConversion for f32 {
    fn into_f64(self) -> f64 {
        self as f64
    }

    fn from_f64(val: f64) -> Self {
        val as f32
    }
}

impl FloatConversion for f64 {
    fn into_f64(self) -> f64 {
        self
    }

    fn from_f64(val: f64) -> Self {
        val
    }
}

impl<D: UomDimension + ?Sized, U: Units<f32> + ?Sized> FloatConversion for Quantity<D, U, f32> {
    fn into_f64(self) -> f64 {
        self.value as f64
    }

    fn from_f64(val: f64) -> Self {
        Self {
            dimension: PhantomData,
            units: PhantomData,
            value: val as f32,
        }
    }
}

impl<D: UomDimension + ?Sized, U: Units<f64> + ?Sized> FloatConversion for Quantity<D, U, f64> {
    fn into_f64(self) -> f64 {
        self.value
    }

    fn from_f64(val: f64) -> Self {
        Self {
            dimension: PhantomData,
            units: PhantomData,
            value: val,
        }
    }
}

/// Compute a weighted mean along the first dimension of the given array.
///
/// `arr` must contain a type that implements [`FloatConversion`] and be [`Copy`].
/// This means that f32, f64, and [`uom::si::Quantity`] values will work.
/// `weights` must be a 1D array the same length as the first dimension of `arr`.
/// The returned array will have one fewer dimension than `arr` (missing the first one).
/// If the length of the first dimension was 0, this will return `None`.
///
/// # Panics
/// If `weights` is not the same length as the first dimension of `arr`.
pub fn weighted_mean<T, S1, S2, D>(
    arr: &ArrayBase<S1, D>,
    weights: &ArrayBase<S2, Ix1>,
) -> Option<ndarray::Array<T, D::Smaller>>
where
    T: FloatConversion + Copy,
    S1: ndarray::Data<Elem = T>,
    S2: ndarray::Data<Elem = f64>,
    D: ndarray::Dimension + ndarray::RemoveAxis,
{
    let mut total_weight = 0.0;
    let mut running_sum = None;
    for (slice, &wt) in arr.axis_iter(Axis(0)).zip_eq(weights.iter()) {
        total_weight += wt;
        let weighted_slice = slice.mapv(|q| q.into_f64() * wt);
        if let Some(rs) = running_sum.take() {
            running_sum = Some(rs + weighted_slice);
        } else {
            running_sum = Some(weighted_slice);
        }
    }

    if let Some(rs) = running_sum.take() {
        let out_arr = (rs / total_weight).mapv(|v| T::from_f64(v));
        Some(out_arr)
    } else {
        None
    }
}
