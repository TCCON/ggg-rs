use std::cell::Cell;

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
        if !self.target.applies_to_row(&row, lua_engine)? {
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
    Spectrum {
        #[serde(deserialize_with = "de_glob_pattern")]
        spectrum: glob::Pattern,
    },

    /// This edit applies if the lua condition given returns "true"
    LuaCondition { condition: String },
}

fn de_glob_pattern<'de, D>(deserializer: D) -> Result<glob::Pattern, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    glob::Pattern::new(&s).map_err(|e| serde::de::Error::custom(e.to_string()))
}

impl ModRowTarget {
    fn applies_to_row(
        &self,
        row: &ExpandedSunrunRow,
        lua_engine: &mlua::Lua,
    ) -> Result<bool, GggError> {
        match self {
            ModRowTarget::Range {
                time_range,
                lat_range,
                lon_range,
            } => {
                if time_range.1 <= time_range.0 {
                    return Err(GggError::Custom(format!(
                        "End time ({}) is less than or equal to the start time ({}) of an edit block",
                        time_range.1, time_range.0
                    )));
                }
                if lat_range.1 <= lat_range.0 {
                    return Err(GggError::Custom(format!(
                        "Max latitude ({}) is less than or equal to the min latitude ({}) of an edit block",
                        lat_range.1, lat_range.0
                    )));
                }
                if lon_range.1 <= lon_range.0 {
                    return Err(GggError::Custom(format!(
                        "Max longitude ({}) is less than or equal to the min longitude ({}) of an edit block",
                        lon_range.1, lon_range.0
                    )));
                }

                if row.zpd_time < time_range.0 || row.zpd_time > time_range.1 {
                    return Ok(false);
                }
                if row.oblat < lat_range.0 || row.oblat > lat_range.1 {
                    return Ok(false);
                }
                if row.oblon < lon_range.0 || row.oblon > lon_range.1 {
                    return Ok(false);
                }
                return Ok(true);
            }
            ModRowTarget::Spectrum { spectrum } => {
                return Ok(spectrum.matches(&row.spectrum_file_name))
            }
            ModRowTarget::LuaCondition { condition } => {
                set_row_in_lua(row, lua_engine)?;
                let applies: bool = lua_engine.load(condition.as_str()).eval()
                .map_err(|e| GggError::custom(format!("Error executing Lua condition code on sunrun row {:?}.\nCode was:\n\n{condition}\n\nError was:\n\n{e}", row)))?;
                Ok(applies)
            }
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

    /// A string of Lua code that will be executed immediately before the first time it edits any row.
    ///
    /// This will only run once through the entirety of `create_sunrun_rs`. Any variables defined
    /// here will be available to access in the `lua` string.
    #[serde(default)]
    pub(crate) init_lua: Option<String>,
    #[serde(default)]
    lua_init_run: Cell<bool>,

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
    #[allow(unused)] // used in tests
    fn new(
        replace: IndexMap<String, FortValue>,
        lua: Option<String>,
        init_lua: Option<String>,
    ) -> Self {
        Self {
            replace,
            init_lua,
            lua_init_run: Cell::new(false),
            lua,
        }
    }
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
        let globals = set_row_in_lua(&row, lua_engine)?;
        if let (Some(init_str), false) = (self.init_lua.as_deref(), self.lua_init_run.get()) {
            lua_engine.load(init_str).exec().map_err(|e| {
                GggError::custom(format!(
                    "Error initializing lua.\nCode was:\n\n{init_str}\n\nError was:\n\n{e}"
                ))
            })?;
            self.lua_init_run.set(true);
        }
        if let Some(lua_str) = self.lua.as_deref() {
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

fn set_row_in_lua(
    row: &ExpandedSunrunRow,
    lua_engine: &mlua::Lua,
) -> Result<mlua::Table, GggError> {
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
    Ok(globals)
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
    use std::{assert_eq, eprintln, path::PathBuf};

    use super::*;
    use approx::assert_abs_diff_eq;
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
            change: ModRowValues::new(
                IndexMap::from_iter([
                    ("tout".to_string(), FortValue::Real(25.0)),
                    ("pout".to_string(), FortValue::Real(1000.0)),
                ]),
                Some("r.lasf = 1.1 * r.lasf".to_string()),
                None,
            ),
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
            change: ModRowValues::new(
                IndexMap::from_iter([
                    ("tout".to_string(), FortValue::Real(25.0)),
                    ("pout".to_string(), FortValue::Real(1000.0)),
                ]),
                Some("r.lasf = 1.1 * r.lasf".to_string()),
                None,
            ),
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
                spectrum: glob::Pattern::new("pa20040721saaaaa.043").unwrap(),
            },
            change: ModRowValues::new(
                IndexMap::from_iter([
                    ("tout".to_string(), FortValue::Real(25.0)),
                    ("pout".to_string(), FortValue::Real(1000.0)),
                ]),
                None,
                None,
            ),
        };
        assert_eq!(expected_mod_row, de_mod_row)
    }

    #[test]
    fn test_match_glob_pattern() {
        let lua = mlua::Lua::new();

        let test_matcher = ModRowTarget::Spectrum {
            spectrum: glob::Pattern::new("pa20040721saaaa?.001").unwrap(),
        };
        let mut sunrun_row = ggg_rs::sunrun::ExpandedSunrunRow::default();
        sunrun_row.spectrum_file_name = "pa20040721saaaaa.001".to_string();
        assert!(
            test_matcher.applies_to_row(&sunrun_row, &lua).unwrap(),
            "Glob pattern did not match 'a' spectrum"
        );
        sunrun_row.spectrum_file_name = "pa20040721saaaab.001".to_string();
        assert!(
            test_matcher.applies_to_row(&sunrun_row, &lua).unwrap(),
            "Glob pattern did not match 'b' spectrum"
        );
        sunrun_row.spectrum_file_name = "pa20040721saaaaa.002".to_string();
        assert!(
            !test_matcher.applies_to_row(&sunrun_row, &lua).unwrap(),
            "Glob pattern incorrectly matched spectrum with different run number"
        );

        let test_matcher = ModRowTarget::Spectrum {
            spectrum: glob::Pattern::new("pa20040721saaaa?.*").unwrap(),
        };
        for i in 1..10 {
            sunrun_row.spectrum_file_name = format!("pa20040721saaaaa.{i:03}");
            assert!(
                test_matcher.applies_to_row(&sunrun_row, &lua).unwrap(),
                "Glob * pattern did not match 'a' spectrum with varying extension: {}",
                sunrun_row.spectrum_file_name
            );

            sunrun_row.spectrum_file_name = format!("oc20040721saaaaa.{i:03}");
            assert!(
                !test_matcher.applies_to_row(&sunrun_row, &lua).unwrap(),
                "Glob * pattern incorrectly matched spectrum from different site with varying extension: {}",
                sunrun_row.spectrum_file_name
            );
        }
    }

    #[test]
    fn test_conditional_lua() {
        let lua = mlua::Lua::new();
        let mut sunrun_row = ggg_rs::sunrun::ExpandedSunrunRow::default();
        sunrun_row.pout = 1000.0;
        sunrun_row.tout = 25.0;
        let test_matcher = ModRowTarget::LuaCondition {
            condition: "r.pout > 900".to_string(),
        };
        assert!(
            test_matcher.applies_to_row(&sunrun_row, &lua).unwrap(),
            "Simple pout condition did not work"
        );
        let test_matcher = ModRowTarget::LuaCondition {
            condition: "r.pout < 900 or r.tout > 20".to_string(),
        };
        assert!(
            dbg!(test_matcher.applies_to_row(&sunrun_row, &lua).unwrap()),
            "pout or tout condition did not work"
        );
        let test_matcher = ModRowTarget::LuaCondition {
            condition: "r.pout > 900 and r.pout < 1100".to_string(),
        };
        assert!(
            test_matcher.applies_to_row(&sunrun_row, &lua).unwrap(),
            "pout and condition did not work"
        );
        let test_matcher = ModRowTarget::LuaCondition {
            condition: "return r.pout > 900 and r.pout < 1100".to_string(),
        };
        assert!(
            test_matcher.applies_to_row(&sunrun_row, &lua).unwrap(),
            "statement with return did not work"
        );
        let test_matcher = ModRowTarget::LuaCondition {
            condition: "   return r.pout > 900 and r.pout < 1100".to_string(),
        };
        assert!(
            test_matcher.applies_to_row(&sunrun_row, &lua).unwrap(),
            "statement with return and leading whitespace did not work"
        );

        let test_matcher = ModRowTarget::LuaCondition {
            condition: "r.pout < 900".to_string(),
        };
        assert!(
            !test_matcher.applies_to_row(&sunrun_row, &lua).unwrap(),
            "negated statement did not work"
        );

        // NB, return is required for multi line conditions
        let test_matcher = ModRowTarget::LuaCondition {
            condition: "plim = 900\nreturn r.pout > plim".to_string(),
        };
        assert!(
            test_matcher.applies_to_row(&sunrun_row, &lua).unwrap(),
            "multiline statement did not work"
        );

        let test_matcher = ModRowTarget::LuaCondition {
            condition: r#"
            if r.pout > 900 then
                return true
            end
            return r.tout < 20 or r.tout > 30
            "#
            .to_string(),
        };
        assert!(
            test_matcher.applies_to_row(&sunrun_row, &lua).unwrap(),
            "multiline statement with early return did not work"
        );
    }

    #[test]
    fn test_lua_edit_string() {
        // construct a sunrun row to run through a Lua edit
        let row = ExpandedSunrunRow::default();
        let edit = ModRow {
            target: ModRowTarget::LuaCondition {
                condition: "true".to_string(),
            },
            change: ModRowValues::new(IndexMap::new(), Some("r.pout = 500.0".to_string()), None),
        };
        let lua = mlua::Lua::new();
        let new_row = edit.apply(row, &lua).unwrap();
        assert_abs_diff_eq!(new_row.pout, 500.0);
    }

    #[test]
    fn test_lua_init() {
        // use the lua init option to set a value used to edit
        // rows
        let row = ExpandedSunrunRow::default();
        let edit = ModRow {
            target: ModRowTarget::LuaCondition {
                condition: "true".to_string(),
            },
            change: ModRowValues::new(
                IndexMap::new(),
                Some("r.pout = new_p".to_string()),
                Some("new_p = 750.0".to_string()),
            ),
        };
        let lua = mlua::Lua::new();
        let new_row = edit.apply(row, &lua).unwrap();
        assert_abs_diff_eq!(new_row.pout, 750.0);
    }

    #[test]
    fn test_book_examples() {
        // Since the book shows snippets of the configuration, we need to define
        // structures that those snippets can deserialize into. (We don't want to
        // make the fields in the main module optional, since they must be provided
        // in normal use). Where possible, we check that both field names and types
        // match the real structures. When we have to use a different type that has
        // only the included fields, we have to just check the field names.
        ggg_rs::test_struct_check_field_types! {
            #[derive(Debug, Deserialize)]
            struct InstrObj {
                instrument: Instrument,
                object: Object,
            } : StaticSiteInfo
        }

        ggg_rs::test_struct_check_field_types! {
            #[derive(Debug, Deserialize)]
            struct Defaults {
                defaults: SunrunDefaults,
            } : StaticSiteInfo
        }

        ggg_rs::test_struct_check_field_types! {
            #[derive(Debug, Deserialize)]
            struct Detectors {
                detectors: Vec<Detector>
            } : StaticSiteInfo
        }

        ggg_rs::test_struct_check_fields! {
            #[derive(Debug, Deserialize)]
            struct OnlyInstObj {
                constants: InstrObj
            } : SiteConfig
        }
        ggg_rs::test_struct_check_fields! {
            #[derive(Debug, Deserialize)]
            struct OnlyDefaults {
                constants: Defaults
            } : SiteConfig
        }

        ggg_rs::test_struct_check_fields! {
            #[derive(Debug, Deserialize)]
            struct OnlyDetectors {
                constants: Detectors
            } : SiteConfig
        }

        ggg_rs::test_struct_check_field_types! {
            #[derive(Debug, Deserialize)]
            struct OnlyEdits {
                edits: Vec<ModRow>
            } : SiteConfig
        }

        // Now actually check the blocks, using the tags (e.g., "+defaults")
        // in the fenced blocks to choose which structure to deserialize as.
        let lua = mlua::Lua::new();
        let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let book_subdir = crate_root.join("book/src/setup/create_sunrun_rs");
        let md_files = book_subdir
            .read_dir()
            .expect("should be able to get files from the book subdir")
            .map(|e| e.expect("should be able to get md files from book").path());
        let block_iter = ggg_rs::test_utils::iter_fenced_blocks("toml", md_files);
        for block in block_iter {
            let block = block.expect("should be able to read fenced block");
            let res = match block.subtag.as_deref() {
                None => {
                    eprintln!("Deserializing as full configuration");
                    toml::from_str::<SiteConfig>(&block.text).map(|_| None)
                }
                Some("constants") => {
                    eprintln!("Deserializing as OnlyConstants");
                    toml::from_str::<OnlyInstObj>(&block.text).map(|_| None)
                }
                Some("defaults") => {
                    eprintln!("Deserializing as OnlyDefaults");
                    toml::from_str::<OnlyDefaults>(&block.text).map(|_| None)
                }
                Some("detectors") => {
                    eprintln!("Deserializing as OnlyDetectors");
                    toml::from_str::<OnlyDetectors>(&block.text).map(|_| None)
                }
                Some("edits") => {
                    // TODO: test blocks with Lua to make sure the lua is valid -
                    //
                    eprintln!("Deserializing as OnlyEdits");
                    toml::from_str::<OnlyEdits>(&block.text).map(|v| Some(v.edits))
                }
                Some(s) => {
                    unimplemented!("Unimplemented subtag for site_config deserialization: {s}");
                }
            };
            assert!(
                res.is_ok(),
                "could not deserialize an example in line {} of file {}:\n\n{}\n\nerror was\n\n{}",
                block.line,
                block.file.display(),
                block.text,
                res.unwrap_err()
            );

            if let Ok(Some(edits)) = res {
                let lua_res = test_book_lua_snippet(&edits, &lua);
                assert!(
                    lua_res.is_ok(),
                    "could not execute lua in example at line {} of file {}:\n\n{}\n\nerror was\n\n{}",
                    block.line,
                    block.file.display(),
                    block.text,
                    lua_res.unwrap_err()
                )
            }
        }
    }

    fn test_book_lua_snippet(edits: &[ModRow], lua_engine: &mlua::Lua) -> Result<(), GggError> {
        // We're not trying to check if the Lua gives the right values, just
        // that it runs successfully.
        let dummy_row = ExpandedSunrunRow::default();
        for edit in edits {
            if let ModRowTarget::LuaCondition { condition: _ } = edit.target {
                edit.target.applies_to_row(&dummy_row, lua_engine)?;
            }
            edit.change.do_lua(dummy_row.clone(), lua_engine)?;
        }
        Ok(())
    }
}
