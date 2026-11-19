use serde::Serialize;
use uom::{
    num_traits::Num,
    si::pressure::{Pressure, Unit},
    si::Units,
    Conversion,
};

/// Convert a scalar pressure to units given by `N` and return the float value and
/// abbreviation [`uom`] uses.
///
/// The `N` generic is the unit, and `V` is the float type (`f32` or `f64`). `U`
/// can be inferred, so you will usually leave it as a placeholder.
///
/// ```
/// use ggg_rs::units::pressure::scalar_to_value_and_abbr;
/// use uom::si::{pressure::pascal, f64::Pressure};
///
/// let q = Pressure::new::<pascal>(101325.0);
/// // Notice only the first generic is specified to indicate the unit.
/// let (v, abbr) = scalar_to_value_and_abbr::<pascal, _, _>(q);
/// assert_eq!(v, 101325.0);
/// assert_eq!(abbr, "Pa")
/// ```
///
/// # See also
///
/// [`array_to_value_and_abbr`] to convert an array.
pub fn scalar_to_value_and_abbr<N, U, V>(pressure: Pressure<U, V>) -> (V, &'static str)
where
    U: Units<V> + ?Sized,
    V: Num + Conversion<V>,
    N: Unit + Conversion<V, T = V::T>,
{
    (pressure.get::<N>(), N::abbreviation())
}

/// Convert an array of [`Pressure`] instances to plain floats and return the UOM unit abbreviation.
///
/// As with [`scalar_to_value_and_abbr`], `N` is the actual unit type, and is the only generic that
/// usually needs specified.
pub fn array_to_value_and_abbr<N, U, V, D>(
    pressures: ndarray::ArrayView<Pressure<U, V>, D>,
) -> (ndarray::Array<V, D>, &'static str)
where
    U: Units<V> + ?Sized,
    V: Num + Conversion<V>,
    N: Unit + Conversion<V, T = V::T>,
    D: ndarray::Dimension,
{
    let out = pressures.map(|q| q.get::<N>());
    (out, N::abbreviation())
}

/// Serialize a [`Pressure`] in particular units given by the `N` type parameter.
/// In most cases, you will only need to specify `N` and can leave the others
/// as `_` placeholders.
///
/// ```
/// use ggg_rs::units::pressure::serialize_in;
/// use uom::si::{pressure::pascal, f64::Pressure};
///
/// #[derive(serde::Serialize)]
/// struct Data {
///     #[serde(serialize_with = "serialize_in::<pascal, _, _, _>")]
///     psurf: Pressure
/// }
/// ```
///
/// See [`serialize_opt_in`] if this is an `Option<Pressure>`.
pub fn serialize_in<N, U, V, S>(q: &Pressure<U, V>, s: S) -> Result<S::Ok, S::Error>
where
    N: Unit + Conversion<V, T = V::T>,
    U: Units<V> + ?Sized,
    V: Num + Conversion<V> + serde::Serialize,
    S: serde::Serializer,
{
    let v = q.get::<N>();
    v.serialize(s)
}

/// A version of [`serialize_in`] that works for `Option<Pressure>` types.
pub fn serialize_opt_in<N, U, V, S>(opt_q: &Option<Pressure<U, V>>, s: S) -> Result<S::Ok, S::Error>
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
/// an array of [`Pressure`] instances into floats that can be written to a netCDF
/// file and automatically converts them to the appropriate units.
///
/// If you need the inverse function for reading a netCDF file, see
/// [`crate::units::map_values_to_quantity`].
///
/// ```no_run
/// use ggg_rs_derive::WriteNetcdf;
/// use ndarray::Array1;
/// use ggg_rs::units;
/// use uom::si::{pressure::pascal, f64::Pressure};
///
///
/// #[derive(WriteNetcdf)]
/// struct Data {
///     // Notice that only the desired units (`pascal`) need specified
///     // in the generics, and the annotation accepts the turbofish syntax.
///     #[netcdf(
///         dimensions("time"),
///         map_units(write = units::pressure::map_units::<pascal, _, _, _, _>),
///         kind = "array"
///     )]
///     p_surf: Array1<Pressure>
/// }
/// ```
pub fn map_units<N, U, V, D, A>(
    _data: &A,
    pressures: &ndarray::Array<Pressure<U, V>, D>,
    _name: &str,
) -> (ndarray::Array<V, D>, &'static str)
where
    U: Units<V> + ?Sized,
    V: Num + Conversion<V>,
    N: Unit + Conversion<V, T = V::T>,
    D: ndarray::Dimension,
{
    array_to_value_and_abbr::<N, U, V, D>(pressures.view())
}

/// Function to interface with [`ggg_rs_derive::WriteNetcdf`], specifically
/// the `#[netcdf(map = "...")]` annotation. This allows converting
/// an array of [`Pressure`] instances into floats that can be written to a netCDF
/// file and automatically converts them to the appropriate units. Unlike
/// [`map_units`], this only returns the array, not the units string, which can
/// be preferable if the UOM units are not the string you want to use.
///
/// ```no_run
/// use ggg_rs_derive::WriteNetcdf;
/// use ndarray::Array1;
/// use ggg_rs::units;
/// use uom::si::{pressure::pascal, f64::Pressure};
///
///
/// #[derive(WriteNetcdf)]
/// struct Data {
///     // Notice that only the desired units (`pascal`) need specified
///     // in the generics, and the annotation accepts the turbofish syntax.
///     #[netcdf(
///         dimensions("time"),
///         map(write = units::pressure::map::<pascal, _, _, _, _>),
///         kind = "array",
///         attr( name = "units", value = "Pascals" )
///     )]
///     solzen: Array1<Pressure>
/// }
/// ```
pub fn map<N, U, V, D, A>(
    _data: &A,
    pressures: &ndarray::Array<Pressure<U, V>, D>,
    _name: &str,
) -> ndarray::Array<V, D>
where
    U: Units<V> + ?Sized,
    V: Num + Conversion<V>,
    N: Unit + Conversion<V, T = V::T>,
    D: ndarray::Dimension,
{
    array_to_value_and_abbr::<N, U, V, D>(pressures.view()).0
}
