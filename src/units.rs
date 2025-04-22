use std::fmt::Display;

#[derive(Debug)]
pub struct UnknownUnitError {
    pub quantity: &'static str,
    pub unit: String
}

impl UnknownUnitError {
    fn new<S: ToString>(quantity: &'static str, unit: S) -> Self {
        Self { quantity, unit: unit.to_string() }
    }
}

impl Display for UnknownUnitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unknown {} unit '{}'", self.quantity, self.unit)
    }
}

impl std::error::Error for UnknownUnitError {}



pub fn dmf_conv_factor(old_unit: &str, new_unit: &str) -> Result<f32, UnknownUnitError> {
    let fac1 = parts_to(old_unit)?;
    let fac2 = parts_to(new_unit)?;
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
        _ => Err(UnknownUnitError::new("mole fraction", dmf_unit))
    }
}