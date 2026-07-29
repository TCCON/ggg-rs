use chrono::Utc;
use fortformat::de::FortValue;
use indexmap::IndexMap;
use mlua::LuaSerdeExt;
use serde::Deserialize;

use ggg_rs::{
    sunrun::{ExpandedSunrunRow, Instrument, Object, SunrunDefaults},
    utils::GggError,
};

/// A struct representing the configuration for a site's sunrun, i.e. its `$GGGPATH/tccon/xx_sunrun.toml`.
#[derive(Debug, Deserialize)]
pub(crate) struct SiteConfig {
    /// Settings that are applied to every spectrum.
    pub(crate) constants: StaticSiteInfo,

    /// Edits that can be applied to specific spectra or time ranges.
    #[serde(default)]
    pub(crate) edits: Vec<ModRow>,
}

impl SiteConfig {
    /// Get the configured lower and upper wavenumber bounds for a spectrum.
    ///
    /// This looks at the detector label in the spectrum name. Returns an error
    /// if none of the configured detectors match that label. Note that a spectrum
    /// name being too short is _not_ an error. That is, if the label is configured
    /// to be in character 16, but the spectrum name is only 12 characters long,
    /// a warning will be printed but this function will check the other detectors.
    /// This is deliberate in case different detectors produce spectra with different
    /// name  lengths.
    pub(crate) fn get_nus_nue(&self, specname: &str) -> Result<(f64, f64), GggError> {
        for det in self.constants.detectors.iter() {
            // Both indices input as 1-based, but the end is also inclusive,
            // so it doesn't need adjusted to 0-based.
            let i1_opt = specname
                .char_indices()
                .nth(det.label_index_start - 1)
                .map(|i| i.0);
            let i2_opt = specname
                .char_indices()
                .nth(det.label_index_stop)
                .map(|i| i.0);

            let (i1, i2) = if let (Some(i1), Some(i2)) = (i1_opt, i2_opt) {
                (i1, i2)
            } else {
                log::warn!("Spectrum name '{specname}' was too short for the detector expecting a label between characters {} and {}", det.label_index_start, det.label_index_stop);
                continue;
            };

            let label = &specname[i1..i2];
            if label == det.label {
                log::debug!(
                    "Spectrum '{specname}' has detector label '{label}', setting nus & nue to {} & {}",
                    det.start_freq,
                    det.end_freq
                );
                return Ok((det.start_freq, det.end_freq));
            }
        }
        Err(GggError::Custom(format!(
            "No detector found matching spectrum '{specname}'"
        )))
    }
}

/// A struct holding the configured information that applies to all spectra.
#[derive(Debug, Deserialize)]
pub(crate) struct StaticSiteInfo {
    pub(crate) instrument: Instrument,
    pub(crate) object: Object,
    pub(crate) detectors: Vec<Detector>,
    pub(crate) defaults: SunrunDefaults,
}

/// A struct that defines one possible detector.
#[derive(Debug, Deserialize)]
pub(crate) struct Detector {
    /// The substring in the spectrum name that identifies this detector.
    pub(crate) label: String,
    /// Index start for the detector label in the spectrum name, 1-based
    pub(crate) label_index_start: usize,
    /// Index end for the detector label in the spectrum name, 1-based & inclusive
    pub(crate) label_index_stop: usize,
    pub(crate) start_freq: f64,
    pub(crate) end_freq: f64,
}

/// A struct representing one configured edit to a sunrun row.
#[derive(Debug, PartialEq, Deserialize)]
pub(crate) struct ModRow {
    #[serde(flatten)]
    pub(crate) target: ModRowTarget,
    #[serde(flatten)]
    pub(crate) change: ModRowValues,
}

impl ModRow {
    /// Apply this modification to the row, if it is applicable.
    /// If not, the row is returned unchanged.
    pub(crate) fn apply(
        &self,
        row: ExpandedSunrunRow,
        lua_engine: &mlua::Lua,
    ) -> Result<ExpandedSunrunRow, GggError> {
        if !self.target.applies_to_row(&row) {
            Ok(row)
        } else {
            self.change.apply(row, lua_engine)
        }
    }
}

/// A struct that defines what spectra a defined edit applies to.
#[derive(Debug, PartialEq, Deserialize)]
#[serde(untagged)]
pub(crate) enum ModRowTarget {
    /// This edit applies to spectra in a range of times, latitudes, and longitudes.
    /// It is set up so that the default lat/lon ranges cover the full globe.
    /// To define an edit using this variant, it must include the `time_range` key.
    Range {
        // TODO: This has to be a full YYYY-MM-DDTHH:MM:SSZ format. Changing
        // to toml::value::Datetime didn't help. Will try to make this more
        // user friendly in the future.
        time_range: (chrono::DateTime<Utc>, chrono::DateTime<Utc>),
        #[serde(default = "default_lat_range")]
        lat_range: (f64, f64),
        #[serde(default = "default_lon_range")]
        lon_range: (f64, f64),
    },

    /// This edit applies to one specific spectrum.
    Spectrum { spectrum: String },
}

impl ModRowTarget {
    fn applies_to_row(&self, row: &ExpandedSunrunRow) -> bool {
        match self {
            ModRowTarget::Range {
                time_range,
                lat_range,
                lon_range,
            } => {
                if row.zpd_time < time_range.0 || row.zpd_time > time_range.1 {
                    return false;
                }
                if row.oblat < lat_range.0 || row.oblat > lat_range.1 {
                    return false;
                }
                if row.oblon < lon_range.0 || row.oblon > lon_range.1 {
                    return false;
                }
                return true;
            }
            // TODO: allow glob-like matching
            ModRowTarget::Spectrum { spectrum } => return spectrum == &row.spectrum_file_name,
        }
    }
}

fn default_lat_range() -> (f64, f64) {
    (-91.0, 91.0)
}

fn default_lon_range() -> (f64, f64) {
    (-181.0, 181.0)
}

/// Definition of how to update values in a single row.
///
/// The order of operations will be replace values, run lua.
#[derive(Debug, PartialEq, serde::Deserialize)]
pub(crate) struct ModRowValues {
    /// A mapping from **lower cased** column names in the sun run to the new value to insert.
    ///
    #[serde(default)]
    pub(crate) replace: IndexMap<String, FortValue>,
    /// A string of Lua code that edits the row.
    ///
    /// The row will be passed as the `r` variable, and the columns can be accessed
    /// as r.COLUMN. Note that the column names are all lower cased, regardless
    /// of their case in the sunrun itself. To set a column value, assign it in the
    /// Lua code, e.g. `r.pout = r.pout + 0.7`. A multiline string allows multiple
    /// Lua statements to be executed.
    ///
    /// The underlying Lua engine loads some of the Lua standard libraries by default.
    /// The list is given at <https://docs.rs/mlua/latest/mlua/struct.StdLib.html>;
    /// libraries with no crate feature limits (e.g., `math`), or enabled with the
    /// `lua55` feature (e.g., `UTF8`) should be available. Do not use libraries
    /// marked as "unsafe".
    #[serde(default)]
    pub(crate) lua: Option<String>,
}

impl ModRowValues {
    /// Apply the defined edits to this row.
    ///
    /// Note that if this modification specifies multiple
    /// ways of editing the row, then earlier ones will affect
    /// the values that later ones receive. For example, if
    /// this is set to replace `pout` with 1000.0, then the Lua
    /// script will get a row with `pout = 1000.0`, regardless of
    /// what the original value was.
    fn apply(
        &self,
        row: ExpandedSunrunRow,
        lua_engine: &mlua::Lua,
    ) -> Result<ExpandedSunrunRow, GggError> {
        let row = self.do_replace(row)?;
        let row = self.do_lua(row, lua_engine)?;
        Ok(row)
    }

    /// Execute simple value replacement.
    fn do_replace(&self, mut row: ExpandedSunrunRow) -> Result<ExpandedSunrunRow, GggError> {
        replace_string(
            &mut row.spectrum_file_name,
            "spectrum_file_name",
            &self.replace,
        )?;
        replace_object(&mut row.obj, "obj", &self.replace)?;
        replace_float(&mut row.tcorr, "tcorr", &self.replace)?;
        replace_float(&mut row.oblat, "oblat", &self.replace)?;
        replace_float(&mut row.oblon, "oblon", &self.replace)?;
        replace_float(&mut row.tins, "tins", &self.replace)?;
        replace_float(&mut row.pins, "pins", &self.replace)?;
        replace_float(&mut row.hins, "hins", &self.replace)?;
        replace_float(&mut row.tout, "tout", &self.replace)?;
        replace_float(&mut row.pout, "pout", &self.replace)?;
        replace_float(&mut row.hout, "hout", &self.replace)?;
        replace_float(&mut row.sia, "sia", &self.replace)?;
        replace_float(&mut row.fvsi, "fvsi", &self.replace)?;
        replace_float(&mut row.wspd, "wspd", &self.replace)?;
        replace_float(&mut row.wdir, "wdir", &self.replace)?;
        replace_float(&mut row.nus, "nus", &self.replace)?;
        replace_float(&mut row.nue, "nue", &self.replace)?;
        replace_float(&mut row.fsf, "fsf", &self.replace)?;
        replace_float(&mut row.lasf, "lasf", &self.replace)?;
        replace_float(&mut row.wavtkr, "wavtkr", &self.replace)?;
        replace_float(&mut row.aipl, "aipl", &self.replace)?;
        replace_float(&mut row.tm, "tm", &self.replace)?;
        Ok(row)
    }

    /// Execute a Lua script to edit the row.
    ///
    /// Takes the Lua engine as an argument so that the
    /// interpreter only needs spun up once, rather than
    /// per row. This likely means that variables set from
    /// previous rows will still be present.
    fn do_lua(
        &self,
        row: ExpandedSunrunRow,
        lua_engine: &mlua::Lua,
    ) -> Result<ExpandedSunrunRow, GggError> {
        if let Some(lua_str) = self.lua.as_deref() {
            let row_table = lua_engine.to_value(&row).map_err(|e| {
                GggError::custom(format!(
                    "Could not convert row into a Lua table, error was: {e}"
                ))
            })?;
            let globals = lua_engine.globals();
            globals.set("r", row_table).map_err(|e| {
                GggError::custom(format!(
                    "Error setting sunrun row as global variable in Lua, error was: {e}"
                ))
            })?;
            lua_engine.load(lua_str).exec().map_err(|e| {
                GggError::custom(format!("Error executing Lua code on sunrun row {:?}.\nCode was:\n\n{lua_str}\n\nError was:\n\n{e}", row))
            })?;

            let new_row_value: mlua::Value = globals.get("r").map_err(|e| {
                GggError::custom(format!(
                    "Error returning sunrun row from Lua, error was: {e}"
                ))
            })?;
            let new_row = lua_engine.from_value(new_row_value).map_err(|e| {
                GggError::custom(format!(
                    "Error converting modified sunrun row from Lua, error was: {e}"
                ))
            })?;
            Ok(new_row)
        } else {
            Ok(row)
        }
    }
}

/// Helper function to replace a string value in a row with one from the edit replacements map.
fn replace_string(
    s: &mut String,
    key: &str,
    replacements: &IndexMap<String, FortValue>,
) -> Result<(), GggError> {
    if let Some(val) = replacements.get(key) {
        if let FortValue::Char(new_s) = val {
            *s = new_s.clone();
        } else {
            return Err(GggError::Custom(format!(
                "Configured replacements had a value for '{key}' that was not a string"
            )));
        }
    }
    Ok(())
}

/// Helper function to replace an [`Object`] value in a row with one from the edit replacements map.
fn replace_object(
    o: &mut Object,
    key: &str,
    replacements: &IndexMap<String, FortValue>,
) -> Result<(), GggError> {
    if let Some(val) = replacements.get(key) {
        if let FortValue::Integer(new_i) = val {
            let new_i8 = i8::try_from(*new_i).map_err(|_| {
                GggError::custom(format!(
                    "The replacement value for '{key}' is too large to fit in an i8"
                ))
            })?;
            let new_o = Object::try_from(new_i8)?;
            *o = new_o.clone();
        } else {
            return Err(GggError::Custom(format!(
                "Configured replacements had a value for '{key}' that was not an integer"
            )));
        }
    }
    Ok(())
}

/// Helper function to replace a float value in a row with one from the edit replacements map.
fn replace_float(
    f: &mut f64,
    key: &str,
    replacements: &IndexMap<String, FortValue>,
) -> Result<(), GggError> {
    if let Some(val) = replacements.get(key) {
        match val {
            FortValue::Integer(new_i) => *f = *new_i as f64,
            FortValue::Real(new_f) => *f = *new_f,
            FortValue::Logical(_) | FortValue::Char(_) => {
                return Err(GggError::Custom(format!(
                    "Configured replacements had a value for '{key}' that was not a number"
                )))
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::assert_eq;

    use super::*;
    use ggg_rs::test_utils::{utc_dt_ymd, TestResult};

    #[test]
    fn test_de_mod_row_tgt_time_range() {
        let toml_str = r#"time_range = ["2020-01-01T00:00:00Z", "2020-01-31T00:00:00Z"]"#;
        let tgt: ModRowTarget = toml::from_str(toml_str).unwrap_print();
        let expected = ModRowTarget::Range {
            time_range: (utc_dt_ymd(2020, 1, 1), utc_dt_ymd(2020, 1, 31)),
            lat_range: default_lat_range(),
            lon_range: default_lon_range(),
        };
        assert_eq!(expected, tgt);
    }

    #[test]
    fn test_de_mod_row_time_range() {
        let toml_str = r#"
        time_range = ["2020-01-01T00:00:00Z", "2020-01-31T00:00:00Z"]
        replace = {"tout" = 25.0, "pout" = 1000.0}
        lua = "r.lasf = 1.1 * r.lasf"
        "#;
        let de_mod_row: ModRow = toml::from_str(toml_str).unwrap_print();
        let expected_mod_row = ModRow {
            target: ModRowTarget::Range {
                time_range: (utc_dt_ymd(2020, 1, 1), utc_dt_ymd(2020, 1, 31)),
                lat_range: default_lat_range(),
                lon_range: default_lon_range(),
            },
            change: ModRowValues {
                replace: IndexMap::from_iter([
                    ("tout".to_string(), FortValue::Real(25.0)),
                    ("pout".to_string(), FortValue::Real(1000.0)),
                ]),
                lua: Some("r.lasf = 1.1 * r.lasf".to_string()),
            },
        };
        assert_eq!(expected_mod_row, de_mod_row)
    }

    #[test]
    fn test_de_mod_row_time_lat_lon_range() {
        let toml_str = r#"
        time_range = ["2020-01-01T00:00:00Z", "2020-01-31T00:00:00Z"]
        lat_range = [45.0, 46.0]
        lon_range = [-91, -90]
        replace = {"tout" = 25.0, "pout" = 1000.0}
        lua = "r.lasf = 1.1 * r.lasf"
        "#;
        let de_mod_row: ModRow = toml::from_str(toml_str).unwrap_print();
        let expected_mod_row = ModRow {
            target: ModRowTarget::Range {
                time_range: (utc_dt_ymd(2020, 1, 1), utc_dt_ymd(2020, 1, 31)),
                lat_range: (45.0, 46.0),
                lon_range: (-91.0, -90.0),
            },
            change: ModRowValues {
                replace: IndexMap::from_iter([
                    ("tout".to_string(), FortValue::Real(25.0)),
                    ("pout".to_string(), FortValue::Real(1000.0)),
                ]),
                lua: Some("r.lasf = 1.1 * r.lasf".to_string()),
            },
        };
        assert_eq!(expected_mod_row, de_mod_row)
    }

    #[test]
    fn test_de_mod_row_spectrum() {
        let toml_str = r#"
        spectrum = "pa20040721saaaaa.043"
        replace = {"tout" = 25.0, "pout" = 1000.0}
        "#;
        let de_mod_row: ModRow = toml::from_str(toml_str).unwrap_print();
        let expected_mod_row = ModRow {
            target: ModRowTarget::Spectrum {
                spectrum: "pa20040721saaaaa.043".to_string(),
            },
            change: ModRowValues {
                replace: IndexMap::from_iter([
                    ("tout".to_string(), FortValue::Real(25.0)),
                    ("pout".to_string(), FortValue::Real(1000.0)),
                ]),
                lua: None,
            },
        };
        assert_eq!(expected_mod_row, de_mod_row)
    }
}
