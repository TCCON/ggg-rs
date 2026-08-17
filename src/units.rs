use std::{fmt::Display, str::FromStr};

use serde_with::serde_as;

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

/// A marker type for deserializing unit-aware quantities.
///
/// [`uom`] does not provide `serde` implementations for its
/// quantities. To deserialize these from configuration files,
/// we need to use an intermediate type like this.
///
/// To use, you will need the [`serde_with`] crate and to annotate
/// the structure being deserialized with the `#[serde_with::serde_as]`
/// attribute before the `#[derive(serde::Deserialize)]` one.
/// Then annotate the fields containing `uom` `Quantity` types
/// with `#[serde_as(as = ...)` like in the following example:
///
/// ```rust
/// # use ggg_rs::units::DeQuantity;
/// #[serde_with::serde_as]
/// #[derive(serde::Deserialize)]
/// struct UnitAwareConfig {
///     #[serde_as(as = "DeQuantity")]
///     p_scalar: uom::si::f64::Pressure,
///     #[serde_as(as = "Vec<DeQuantity>")]
///     p_vector: Vec<uom::si::f64::Pressure>,
///     #[serde_as(as = "Option<DeQuantity>")]
///     p_option: Option<uom::si::f64::Ratio>,
///     #[serde_as(as = "[DeQuantity; 3]")]
///     p_array: [uom::si::f64::Ratio; 3],
/// }
/// ```
///
/// Note that `serde_as` only natively supports standard Rust types,
/// so deserializing to an [`ndarray::Array`] can't be done, for instance.
pub struct DeQuantity;

impl<'de, Q> serde_with::DeserializeAs<'de, Q> for DeQuantity
where
    Q: FromStr<Err = uom::str::ParseQuantityError>,
{
    fn deserialize_as<D>(deserializer: D) -> Result<Q, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: std::borrow::Cow<'de, str> = serde::Deserialize::deserialize(deserializer)?;
        let res1 = Q::from_str(&s);

        let err1 = match res1 {
            Ok(q) => return Ok(q),
            Err(e) => e,
        };

        if let Some((valstr, unitstr)) = s.split_once(char::is_whitespace) {
            let newunit = uom_unit(unitstr);
            if newunit != unitstr {
                let newstr = format!("{valstr} {newunit}");
                match Q::from_str(&newstr) {
                        Ok(q) => return Ok(q),
                        Err(_) => {
                            return Err(serde::de::Error::custom(format!(
                                "Unable to parse value strings '{s}' or '{newstr}' as a unit-aware quantity: {err1}"
                            )))
                        }
                    }
            }
        }

        Err(serde::de::Error::custom(format!(
            "Unable to parse value string '{s}' as a unit-aware quantity: {err1}"
        )))
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
    T: Copy,
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
    T: Copy,
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
    use uom::si::pressure::hectopascal;

    #[test]
    fn test_convert_scalar() {
        let n = convert_scalar::<uom::si::f64::Time, f64>(1.0, "days").unwrap();
        let n_hr = n.get::<uom::si::time::hour>();
        eprintln!("n = {n:?}, n_hr = {n_hr}");
        assert_eq!(n_hr, 24.0);
    }

    // #[test]
    // fn test_deserialize_sq() {
    //     #[derive(serde::Deserialize)]
    //     struct Test {
    //         q: SerdeQuantity<uom::si::f64::Pressure>,
    //     }
    //     let s = "q = '1013.0 hPa'";
    //     let test_struct: Test = toml::from_str(s).unwrap();
    //     assert_eq!(
    //         test_struct.q.0,
    //         uom::si::f64::Pressure::new::<hectopascal>(1013.0)
    //     )
    // }

    #[test]
    fn test_deserialize_quantity_via_as() {
        #[serde_with::serde_as]
        #[derive(serde::Deserialize)]
        struct Test {
            #[serde_as(as = "DeQuantity")]
            q: uom::si::f64::Pressure,
        }

        // Check that units are accounted for, and the deserialized
        // value equals the expected.
        let s = "q = '1013.0 hPa'";
        let test_struct: Test = toml::from_str(s).unwrap();
        assert_eq!(
            test_struct.q,
            uom::si::f64::Pressure::new::<hectopascal>(1013.0)
        );

        // Check the converse, that deserializing with the wrong
        // units results in a mismatch...
        let s = "q = '1013.0 Pa'";
        let test_struct: Test = toml::from_str(s).unwrap();
        assert_ne!(
            test_struct.q,
            uom::si::f64::Pressure::new::<hectopascal>(1013.0)
        );

        // ...but that magnitudes are equal after proper unit conversion.
        let s = "q = '101300.0 Pa'";
        let test_struct: Test = toml::from_str(s).unwrap();
        assert_eq!(
            test_struct.q,
            uom::si::f64::Pressure::new::<hectopascal>(1013.0)
        );

        // And finally, check that this works inside a container.
        #[serde_with::serde_as]
        #[derive(serde::Deserialize)]
        struct TestVec {
            #[serde_as(as = "Vec<DeQuantity>")]
            v: Vec<uom::si::f64::Pressure>,
        }
        let s = "v = ['1013.0 hPa', '101300.0 Pa']";
        let test_struct: TestVec = toml::from_str(s).unwrap();
        let expected = vec![
            uom::si::f64::Pressure::new::<hectopascal>(1013.0),
            uom::si::f64::Pressure::new::<hectopascal>(1013.0),
        ];
        assert_eq!(test_struct.v, expected);
    }
}
