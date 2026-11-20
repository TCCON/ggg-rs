// Generated from quantity_template.rs.in and quantities.toml by xtask.
// Do not edit directly; edit the template or table, then run `make sourcegen`.
use serde::Serialize;
use uom::{
    num_traits::Num,
    si::angle::{Angle, Unit},
    si::Units,
    Conversion,
};

/// Convert a scalar angle to units given by `N` and return the float value and
/// abbreviation [`uom`] uses.
///
/// The `N` generic is the unit, and `V` is the float type (`f32` or `f64`). `U`
/// can be inferred, so you will usually leave it as a placeholder.
///
/// ```
/// use ggg_rs::units::angle::scalar_to_value_and_abbr;
/// use uom::si::{angle::degree, f64::Angle};
///
/// let q = Angle::new::<degree>(90.0);
/// // Notice only the first generic is specified to indicate the unit.
/// let (v, abbr) = scalar_to_value_and_abbr::<degree, _, _>(q);
/// assert!((v - 90.0).abs() < 1e-5);
/// assert_eq!(abbr, "°")
/// ```
///
/// # See also
///
/// [`array_to_value_and_abbr`] to convert an array.
pub fn scalar_to_value_and_abbr<N, U, V>(angle: Angle<U, V>) -> (V, &'static str)
where
    N: Unit + Conversion<V, T = V::T>,
    U: Units<V> + ?Sized,
    V: Num + Conversion<V>,
{
    (angle.get::<N>(), N::abbreviation())
}

/// Convert an array of [`Angle`] instances to plain floats and return the UOM unit abbreviation.
///
/// As with [`scalar_to_value_and_abbr`], `N` is the actual unit type, and is the only generic that
/// usually needs specified.
pub fn array_to_value_and_abbr<N, U, V, D>(
    angles: ndarray::ArrayView<Angle<U, V>, D>,
) -> (ndarray::Array<V, D>, &'static str)
where
    N: Unit + Conversion<V, T = V::T>,
    U: Units<V> + ?Sized,
    V: Num + Conversion<V>,
    D: ndarray::Dimension,
{
    let out = angles.map(|q| q.get::<N>());
    (out, N::abbreviation())
}

/// Serialize a [`Angle`] in particular units given by the `N` type parameter.
/// In most cases, you will only need to specify `N` and can leave the others
/// as `_` placeholders.
///
/// ```
/// use ggg_rs::units::angle::serialize_in;
/// use uom::si::{angle::degree, f64::Angle};
///
/// #[derive(serde::Serialize)]
/// struct Data {
///     #[serde(serialize_with = "serialize_in::<degree, _, _, _>")]
///     solzen: Angle
/// }
/// ```
///
/// See [`serialize_opt_in`] if this is an `Option<Angle>`.
pub fn serialize_in<N, U, V, S>(q: &Angle<U, V>, s: S) -> Result<S::Ok, S::Error>
where
    N: Unit + Conversion<V, T = V::T>,
    U: Units<V> + ?Sized,
    V: Num + Conversion<V> + serde::Serialize,
    S: serde::Serializer,
{
    let v = q.get::<N>();
    v.serialize(s)
}

/// A version of [`serialize_in`] that works for `Option<Angle>` types.
pub fn serialize_opt_in<N, U, V, S>(opt_q: &Option<Angle<U, V>>, s: S) -> Result<S::Ok, S::Error>
where
    N: Unit + Conversion<V, T = V::T>,
    U: Units<V> + ?Sized,
    V: Num + Conversion<V> + serde::Serialize,
    S: serde::Serializer,
{
    if let Some(q) = opt_q {
        serialize_in::<N, U, V, S>(q, s)
    } else {
        None::<V>.serialize(s)
    }
}

/// Function to interface with [`ggg_rs_derive::WriteNetcdf`], specifically
/// the `#[netcdf(map_units = "...")]` annotation. This allows converting
/// an array of [`Angle`] instances into floats that can be written to a netCDF
/// file and automatically converts them to the appropriate units.
///
/// If you need the inverse function for reading a netCDF file, see
/// [`crate::units::map_values_to_quantity`].
///
/// ```no_run
/// use ggg_rs_derive::WriteNetcdf;
/// use ndarray::Array1;
/// use ggg_rs::units;
/// use uom::si::{angle::degree, f64::Angle};
///
///
/// #[derive(WriteNetcdf)]
/// struct Data {
///     // Notice that only the desired units (`degree`) need specified
///     // in the generics, and the annotation accepts the turbofish syntax.
///     #[netcdf(
///         dimensions("time"),
///         map_units(write = units::angle::map_units::<degree, _, _, _, _>),
///         kind = "array"
///     )]
///     solzen: Array1<Angle>
/// }
/// ```
pub fn map_units<N, U, V, D, A>(
    _data: &A,
    angles: &ndarray::Array<Angle<U, V>, D>,
    _name: &str,
) -> (ndarray::Array<V, D>, &'static str)
where
    N: Unit + Conversion<V, T = V::T>,
    U: Units<V> + ?Sized,
    V: Num + Conversion<V>,
    D: ndarray::Dimension,
{
    array_to_value_and_abbr::<N, U, V, D>(angles.view())
}

/// Function to interface with [`ggg_rs_derive::WriteNetcdf`], specifically
/// the `#[netcdf(map = "...")]` annotation. This allows converting
/// an array of [`Angle`] instances into floats that can be written to a netCDF
/// file and automatically converts them to the appropriate units. Unlike
/// [`map_units`], this only returns the array, not the units string, which can
/// be preferable if the UOM units are not the string you want to use.
///
/// ```no_run
/// use ggg_rs_derive::WriteNetcdf;
/// use ndarray::Array1;
/// use ggg_rs::units;
/// use uom::si::{angle::degree, f64::Angle};
///
///
/// #[derive(WriteNetcdf)]
/// struct Data {
///     // Notice that only the desired units (`degree`) need specified
///     // in the generics, and the annotation accepts the turbofish syntax.
///     #[netcdf(
///         dimensions("time"),
///         map(write = units::angle::map::<degree, _, _, _, _>),
///         kind = "array",
///         attr( name = "units", value = "degrees" )
///     )]
///     solzen: Array1<Angle>
/// }
/// ```
pub fn map<N, U, V, D, A>(
    _data: &A,
    angles: &ndarray::Array<Angle<U, V>, D>,
    _name: &str,
) -> ndarray::Array<V, D>
where
    N: Unit + Conversion<V, T = V::T>,
    U: Units<V> + ?Sized,
    V: Num + Conversion<V>,
    D: ndarray::Dimension,
{
    array_to_value_and_abbr::<N, U, V, D>(angles.view()).0
}
