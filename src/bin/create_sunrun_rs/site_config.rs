use chrono::Utc;
use fortformat::de::FortValue;
use indexmap::IndexMap;
use serde::Deserialize;

use ggg_rs::{
    sunrun::{Instrument, Object, SunrunDefaults},
    utils::GggError,
};

#[derive(Debug, Deserialize)]
pub(crate) struct SiteConfig {
    pub(crate) constants: StaticSiteInfo,
}

impl SiteConfig {
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

#[derive(Debug, Deserialize)]
pub(crate) struct StaticSiteInfo {
    pub(crate) instrument: Instrument,
    pub(crate) object: Object,
    pub(crate) detectors: Vec<Detector>,
    pub(crate) defaults: SunrunDefaults,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Detector {
    pub(crate) label: String,
    // Index start for the detector label in the spectrum name, 1-based
    pub(crate) label_index_start: usize,
    // Index end for the detector label in the spectrum name, 1-based & inclusive
    pub(crate) label_index_stop: usize,
    pub(crate) start_freq: f64,
    pub(crate) end_freq: f64,
}

#[derive(Debug, PartialEq, Deserialize)]
pub(crate) struct ModRow {
    #[serde(flatten)]
    target: ModRowTarget,
    #[serde(flatten)]
    change: ModRowValues,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(untagged)]
pub(crate) enum ModRowTarget {
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
    Spectrum {
        spectrum: String,
    },
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
    #[serde(default)]
    pub(crate) replace: IndexMap<String, FortValue>,
    #[serde(default)]
    pub(crate) lua: Option<String>,
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
