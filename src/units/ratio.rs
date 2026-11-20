// Generated from quantity_template.rs.in and quantities.toml by xtask.
// Do not edit directly; edit the template or table, then run `make sourcegen`.
use serde::Serialize;
use uom::{
    num_traits::Num,
    si::ratio::{Ratio, Unit},
    si::Units,
    Conversion,
};

/// Convert a scalar ratio to units given by `N` and return the float value and
/// abbreviation [`uom`] uses.
///
/// The `N` generic is the unit, and `V` is the float type (`f32` or `f64`). `U`
/// can be inferred, so you will usually leave it as a placeholder.
///
/// ```
/// use ggg_rs::units::ratio::scalar_to_value_and_abbr;
/// use uom::si::{ratio::part_per_million, f64::Ratio};
///
/// let q = Ratio::new::<part_per_million>(400.0);
/// // Notice only the first generic is specified to indicate the unit.
/// let (v, abbr) = scalar_to_value_and_abbr::<part_per_million, _, _>(q);
/// assert!((v - 400.0).abs() < 1e-5);
/// assert_eq!(abbr, "ppm")
/// ```
///
/// # See also
///
/// [`array_to_value_and_abbr`] to convert an array.
pub fn scalar_to_value_and_abbr<N, U, V>(ratio: Ratio<U, V>) -> (V, &'static str)
where
    N: Unit + Conversion<V, T = V::T>,
    U: Units<V> + ?Sized,
    V: Num + Conversion<V>,
{
    (ratio.get::<N>(), N::abbreviation())
}

/// Convert an array of [`Ratio`] instances to plain floats and return the UOM unit abbreviation.
///
/// As with [`scalar_to_value_and_abbr`], `N` is the actual unit type, and is the only generic that
/// usually needs specified.
pub fn array_to_value_and_abbr<N, U, V, D>(
    ratios: ndarray::ArrayView<Ratio<U, V>, D>,
) -> (ndarray::Array<V, D>, &'static str)
where
    N: Unit + Conversion<V, T = V::T>,
    U: Units<V> + ?Sized,
    V: Num + Conversion<V>,
    D: ndarray::Dimension,
{
    let out = ratios.map(|q| q.get::<N>());
    (out, N::abbreviation())
}

/// Serialize a [`Ratio`] in particular units given by the `N` type parameter.
/// In most cases, you will only need to specify `N` and can leave the others
/// as `_` placeholders.
///
/// ```
/// use ggg_rs::units::ratio::serialize_in;
/// use uom::si::{ratio::part_per_million, f64::Ratio};
///
/// #[derive(serde::Serialize)]
/// struct Data {
///     #[serde(serialize_with = "serialize_in::<part_per_million, _, _, _>")]
///     xco2: Ratio
/// }
/// ```
///
/// See [`serialize_opt_in`] if this is an `Option<Ratio>`.
pub fn serialize_in<N, U, V, S>(q: &Ratio<U, V>, s: S) -> Result<S::Ok, S::Error>
where
    N: Unit + Conversion<V, T = V::T>,
    U: Units<V> + ?Sized,
    V: Num + Conversion<V> + serde::Serialize,
    S: serde::Serializer,
{
    let v = q.get::<N>();
    v.serialize(s)
}

/// A version of [`serialize_in`] that works for `Option<Ratio>` types.
pub fn serialize_opt_in<N, U, V, S>(opt_q: &Option<Ratio<U, V>>, s: S) -> Result<S::Ok, S::Error>
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
/// an array of [`Ratio`] instances into floats that can be written to a netCDF
/// file and automatically converts them to the appropriate units.
///
/// If you need the inverse function for reading a netCDF file, see
/// [`crate::units::map_values_to_quantity`].
///
/// ```no_run
/// use ggg_rs_derive::WriteNetcdf;
/// use ndarray::Array1;
/// use ggg_rs::units;
/// use uom::si::{ratio::part_per_million, f64::Ratio};
///
///
/// #[derive(WriteNetcdf)]
/// struct Data {
///     // Notice that only the desired units (`part_per_million`) need specified
///     // in the generics, and the annotation accepts the turbofish syntax.
///     #[netcdf(
///         dimensions("time"),
///         map_units(write = units::ratio::map_units::<part_per_million, _, _, _, _>),
///         kind = "array"
///     )]
///     xco2: Array1<Ratio>
/// }
/// ```
pub fn map_units<N, U, V, D, A>(
    _data: &A,
    ratios: &ndarray::Array<Ratio<U, V>, D>,
    _rs_name: &str,
    _nc_name: &str,
) -> (ndarray::Array<V, D>, &'static str)
where
    N: Unit + Conversion<V, T = V::T>,
    U: Units<V> + ?Sized,
    V: Num + Conversion<V>,
    D: ndarray::Dimension,
{
    array_to_value_and_abbr::<N, U, V, D>(ratios.view())
}

/// Like [`map_units`], but with an additional argument to swallow
/// a context type passed to helper functions like this.
pub fn map_units_ctx<N, U, V, D, A, Ctx>(
    _data: &A,
    ratios: &ndarray::Array<Ratio<U, V>, D>,
    _rs_name: &str,
    _nc_name: &str,
    _ctx: Ctx
) -> (ndarray::Array<V, D>, &'static str)
where
    N: Unit + Conversion<V, T = V::T>,
    U: Units<V> + ?Sized,
    V: Num + Conversion<V>,
    D: ndarray::Dimension,
{
    array_to_value_and_abbr::<N, U, V, D>(ratios.view())
}

/// Function to interface with [`ggg_rs_derive::WriteNetcdf`], specifically
/// the `#[netcdf(map = "...")]` annotation. This allows converting
/// an array of [`Ratio`] instances into floats that can be written to a netCDF
/// file and automatically converts them to the appropriate units. Unlike
/// [`map_units`], this only returns the array, not the units string, which can
/// be preferable if the UOM units are not the string you want to use.
///
/// ```no_run
/// use ggg_rs_derive::WriteNetcdf;
/// use ndarray::Array1;
/// use ggg_rs::units;
/// use uom::si::{ratio::part_per_million, f64::Ratio};
///
///
/// #[derive(WriteNetcdf)]
/// struct Data {
///     // Notice that only the desired units (`part_per_million`) need specified
///     // in the generics, and the annotation accepts the turbofish syntax.
///     #[netcdf(
///         dimensions("time"),
///         map(write = units::ratio::map::<part_per_million, _, _, _, _>),
///         kind = "array",
///         attr( name = "units", value = "parts_per_million" )
///     )]
///     xco2: Array1<Ratio>
/// }
/// ```
pub fn map<N, U, V, D, A>(
    _data: &A,
    ratios: &ndarray::Array<Ratio<U, V>, D>,
    _rs_name: &str,
    _nc_name: &str,
) -> ndarray::Array<V, D>
where
    N: Unit + Conversion<V, T = V::T>,
    U: Units<V> + ?Sized,
    V: Num + Conversion<V>,
    D: ndarray::Dimension,
{
    array_to_value_and_abbr::<N, U, V, D>(ratios.view()).0
}

/// Like [`map`], but with an extra argument to swallow 
/// a context type passed to helper functions like this.
pub fn map_ctx<N, U, V, D, A, Ctx>(
    _data: &A,
    ratios: &ndarray::Array<Ratio<U, V>, D>,
    _rs_name: &str,
    _nc_name: &str,
    _ctx: Ctx
) -> ndarray::Array<V, D>
where
    N: Unit + Conversion<V, T = V::T>,
    U: Units<V> + ?Sized,
    V: Num + Conversion<V>,
    D: ndarray::Dimension,
{
    array_to_value_and_abbr::<N, U, V, D>(ratios.view()).0
}