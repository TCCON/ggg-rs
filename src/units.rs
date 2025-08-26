use std::fmt::Display;

#[derive(Debug)]
pub struct UnknownUnitError {
    pub quantity: &'static str,
    pub unit: String,
}

impl UnknownUnitError {
    fn new<S: ToString>(quantity: &'static str, unit: S) -> Self {
        Self {
            quantity,
            unit: unit.to_string(),
        }
    }
}

impl Display for UnknownUnitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unknown {} unit '{}'", self.quantity, self.unit)
    }
}

impl std::error::Error for UnknownUnitError {}

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
