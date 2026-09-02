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
pub fn scalar_to_value_and_abbr<N, U, V>(angle: Ratio<U, V>) -> (V, &'static str)
where
    U: Units<V> + ?Sized,
    V: Num + Conversion<V>,
    N: Unit + Conversion<V, T = V::T>,
{
    (angle.get::<N>(), N::abbreviation())
}

/// Convert an array of [`Ratio`] instances to plain floats and return the UOM unit abbreviation.
///
/// As with [`scalar_to_value_and_abbr`], `N` is the actual unit type, and is the only generic that
/// usually needs specified.
pub fn array_to_value_and_abbr<N, U, V, D>(
    angles: ndarray::ArrayView<Ratio<U, V>, D>,
) -> (ndarray::Array<V, D>, &'static str)
where
    U: Units<V> + ?Sized,
    V: Num + Conversion<V>,
    N: Unit + Conversion<V, T = V::T>,
    D: ndarray::Dimension,
{
    let out = angles.map(|q| q.get::<N>());
    (out, N::abbreviation())
}

/// Function to interface with [`ggg_rs_derive::WriteNetcdf`], specifically
/// the `#[netcdf(map_units = "...")]` annotation. This allows converting
/// an array of [`Ratio`] instances into floats that can be written to a netCDF
/// file and automatically converts them to the appropriate units.
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
///         map_units = units::ratio::map_units::<part_per_million, _, _, _, _>,
///         kind = "array"
///     )]
///     xco2: Array1<Ratio>
/// }
/// ```
pub fn map_units<N, U, V, D, A>(
    _data: &A,
    angles: &ndarray::Array<Ratio<U, V>, D>,
    _name: &str,
) -> (ndarray::Array<V, D>, &'static str)
where
    U: Units<V> + ?Sized,
    V: Num + Conversion<V>,
    N: Unit + Conversion<V, T = V::T>,
    D: ndarray::Dimension,
{
    array_to_value_and_abbr::<N, U, V, D>(angles.view())
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
///         map = units::ratio::map::<part_per_million, _, _, _, _>,
///         kind = "array",
///         attr( name = "units", value = "parts_per_million" )
///     )]
///     xco2: Array1<Ratio>
/// }
/// ```
pub fn map<N, U, V, D, A>(
    _data: &A,
    angles: &ndarray::Array<Ratio<U, V>, D>,
    _name: &str,
) -> ndarray::Array<V, D>
where
    U: Units<V> + ?Sized,
    V: Num + Conversion<V>,
    N: Unit + Conversion<V, T = V::T>,
    D: ndarray::Dimension,
{
    array_to_value_and_abbr::<N, U, V, D>(angles.view()).0
}
