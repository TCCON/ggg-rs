use std::fmt::Display;

#[derive(Debug)]
pub struct UnknownUnitError {
    pub quantity: Option<&'static str>,
    pub unit: String,
}

impl UnknownUnitError {
    fn new<S: ToString>(quantity: &'static str, unit: S) -> Self {
        Self {
            quantity: Some(quantity),
            unit: unit.to_string(),
        }
    }

    fn new_no_quantity<S: ToString>(unit: S) -> Self {
        Self {
            quantity: None,
            unit: unit.to_string(),
        }
    }
}

impl Display for UnknownUnitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // WORKAROUND: vscode rust-analyzer is not handling macro expansion
        // properly when returned from a braced block. Remove the `let r =`
        // later once this is fixed.
        if let Some(q) = self.quantity {
            let r = write!(f, "Unknown {} unit '{}'", q, self.unit);
            r
        } else {
            let r = write!(f, "Unknown unit '{}'", self.unit);
            r
        }
    }
}

impl std::error::Error for UnknownUnitError {}

/// Helper function that normalizes units into values that [`uom`] can understand.
///
/// [`uom`] does not necessarily know CF-compliant units. If there are units in the
/// TCCON files that need to be mapped to a unit [`uom`] can parse, this function
/// handles that.
pub(crate) fn uom_unit(orig_unit: &str) -> &str {
    match orig_unit {
        // "1" is convention for unitless. Mapping to an empty string works for ratios
        // in uom, but if we have other unitless quantities, this may need adjusted.
        "1" => "",
        "degrees_north" => "°",
        "degrees_east" => "°",
        _ => orig_unit,
    }
}

/// Convert an array from a plain numeric type to a [`uom::si::Quantity`].
///
/// The generic parameter `T` is the type that the input array has (e.g., `i32`, `f64`),
/// and it must be a valid netCDF type. `D` defines the dimensionality
/// that the array will has (usually [`ndarray::Ix1`], [`ndarray::IxDyn`],
/// or similar). The first generic parameter `Q` is the quantity
/// that the array is converted to, e.g. [`uom::si::f32::Pressure`].
/// Since [`uom`] defines different quantities for different numeric
/// types, be sure that the `uom` numeric type matches `T`, i.e.,
/// if `T = f32` then use quantities under [`uom::si::f32`].
///
/// `units` must be a string that is either an abbreviation
/// known to [`uom`] or one that [`crate::units::uom_unit`] knows
/// how to convert to one [`uom`] knows. Currently the best
/// way to find this list of units is to look at the `uom` source
/// code, e.g. for [length units](https://docs.rs/uom/latest/src/uom/si/length.rs.html).
///
/// If the units are not understood by `uom` or `ggg_rs`, this will
/// return an [`UnknownUnitError`]. Otherwise, it returns an array
/// with the same shape and dimensionality as `arr` but containing
/// type `Q`.
pub fn convert_array<Q, T, D>(
    arr: ndarray::Array<T, D>,
    units: &str,
) -> Result<ndarray::Array<Q, D>, UnknownUnitError>
where
    T: Copy + netcdf::NcTypeDescriptor,
    D: ndarray::Dimension,
    Q: std::str::FromStr<Err = uom::str::ParseQuantityError> + Copy + std::ops::Mul<T, Output = Q>,
{
    let uom_units = crate::units::uom_unit(&units);
    let conversion = Q::from_str(&format!("1.0 {uom_units}"))
        .map_err(|_| UnknownUnitError::new_no_quantity(units))?;
    let arr = arr.mapv(|x| conversion * x);
    Ok(arr)
}

/// Convert a scalar value from a plain numeric type to a [`uom::si::Quantity`].
///
/// This follows the same rules as [`convert_array`], except that it takes a
/// scalar quantity rather than an array.
pub fn convert_scalar<Q, T>(value: T, units: &str) -> Result<Q, UnknownUnitError>
where
    T: Copy + netcdf::NcTypeDescriptor,
    Q: std::str::FromStr<Err = uom::str::ParseQuantityError> + Copy + std::ops::Mul<T, Output = Q>,
{
    let uom_units = crate::units::uom_unit(&units);
    let conversion = Q::from_str(&format!("1.0 {uom_units}"))
        .map_err(|_| UnknownUnitError::new_no_quantity(units))?;
    Ok(conversion * value)
}

pub enum Quantity {
    DMF,
    Pressure,
}

impl Quantity {
    fn from_base_unit(&self, unit: &str) -> Result<f32, UnknownUnitError> {
        match self {
            Quantity::DMF => parts_to(unit),
            Quantity::Pressure => pascals_to(unit),
        }
    }
}

pub fn unit_conv_factor(
    old_unit: &str,
    new_unit: &str,
    quantity: Quantity,
) -> Result<f32, UnknownUnitError> {
    let fac1 = quantity.from_base_unit(old_unit)?;
    let fac2 = quantity.from_base_unit(new_unit)?;
    Ok(fac2 / fac1)
}

fn parts_to(dmf_unit: &str) -> Result<f32, UnknownUnitError> {
    match dmf_unit {
        "parts" => Ok(1.0),
        "1" => Ok(1.0),
        "ppm" => Ok(1e6),
        "ppb" => Ok(1e9),
        "ppt" => Ok(1e12),
        _ => Err(UnknownUnitError::new("mole fraction", dmf_unit)),
    }
}

pub fn dmf_long_name(dmf_unit: &str) -> Result<&'static str, UnknownUnitError> {
    match dmf_unit {
        "parts" | "1" => Ok("parts"),
        "ppm" => Ok("parts per million"),
        "ppb" => Ok("parts per billion"),
        "ppt" => Ok("parts per trillion"),
        _ => Err(UnknownUnitError::new("mole fraction", dmf_unit)),
    }
}

fn pascals_to(pres_unit: &str) -> Result<f32, UnknownUnitError> {
    match pres_unit {
        "hPa" => Ok(1e-2),
        "atm" => Ok(1.0 / 101325.0),
        _ => Err(UnknownUnitError::new("pressure", pres_unit)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_scalar() {
        let n = convert_scalar::<uom::si::f64::Time, f64>(1.0, "days").unwrap();
        let n_hr = n.get::<uom::si::time::hour>();
        eprintln!("n = {n:?}, n_hr = {n_hr}");
        assert_eq!(n_hr, 24.0);
    }
}
