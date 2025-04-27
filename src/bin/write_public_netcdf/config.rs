//! The public netCDF writer must strike a balance between being strict enough
//! to ensure that the required variable for standard TCCON usage are included
//! in normal operation, but also be flexible enough to allow non-standard usage.
//! To enable more flexible use, the writer by default requires the standard TCCON
//! variables be present, but can be configured to change the required variables.
//!
//! The configuration file uses [TOML format](https://toml.io/en/). The configuration
//! file can be broadly broken down into five sections:
//!
//! - auxiliary variables,
//! - derived variables,
//! - Xgas variable sets,
//! - Xgas discovery, and
//! - default settings.
//!
//! # Auxiliary variables
//!
//! Auxiliary variables are those which are not directly related to one of the target
//! Xgases but which provide useful information about the observations. Common examples
//! are time, latitude, longitude, solar zenith angle, etc. These are defined in the
//! `aux` section of the TOML file as an [array of tables](https://toml.io/en/v1.0.0#array-of-tables).
//!
//! The simplest way to define an auxiliary variable to copy is to give the name of the
//! private variable in the netCDF file and what value to use as the long name:
//!
//! ```toml
//! [[aux]]
//! private_varname = "solzen"
//! long_name = "solar zenith angle"
//! ```
//!
//! This will copy the variable `solzen` from the private netCDF file along with all its
//! attributes _except_ `standard_name` and `precision`, add the `long_name` attribute,
//! and put the variable's data (subsetting to `flag == 0` data) into the public file as
//! `solzen`. Note that the `long_name` value should follow the
//! [CF conventions meaning](https://cfconventions.org/cf-conventions/cf-conventions.html#long-name).
//! We prefer `long_name` over `standard_name` because the
//! [available standard names](https://cfconventions.org/Data/cf-standard-names/current/build/cf-standard-name-table.html)
//! do not adequately describe remotely sensed quantities.
//!
//! If instead you wanted to rename the variable in the public file, you can add the
//! `public_varname` field:
//!
//! ```toml
//! [[aux]]
//! private_varname = "solzen"
//! public_varname = "solar_zenith_angle"
//! long_name = "solar zenith angle"
//! ```
//!
//! This would rename the variable to `solar_zenith_angle` in the public file, but otherwise
//! behave identically to above.
//!
//! You can also control the attributes copied through two more fields, `attr_overrides` and
//! `attr_to_remove`. `attr_overrides` is a TOML table of attibute names and values that will be
//! added to the public variable. If an attribute is listed in the private file with the
//! same name as an override, the override value in the config takes precedence. The latter
//! is an array of attribute names to skip copying if present. (If one of these attributes is
//! not present in the private file, nothing happens.) An example:
//!
//! ```toml
//! [[aux]]
//! private_varname = "day"
//! long_name = "day of year"
//! attr_overrides = {units = "Julian day", description = "1-based day of year"}
//! attr_to_remove = ["vmin", "vmax"]
//! ```
//!
//! This will add or replace the attributes `units` and `description` in the public file with those
//! given here, and ensure that the `vmin` and `vmax` attributes are not copied. Take note, specifying
//! `attr_to_remove` overrides the default list of `standard_name` and `precision`; this can be useful
//! if you want to retain those (you can do so by specifying `attr_to_remove = []`), but if you want to
//! exclude them, you must add them to your list.
//!
//! Finally, by default any auxiliary variable listed here must be found in the private netCDF file, or
//! the public writer stops with an error. To change this behavior so that a variable is optional, add
//! the `required = false` field to an aux variable:
//!
//! ```toml
//! [[aux]]
//! private_varname = "day"
//! long_name = "day of year"
//! required = false
//! ```
//!
//! Each auxiliary variable to copy will have its own `[[aux]]` section, for example:
//!
//! ```toml
//! [[aux]]
//! private_varname = "time"
//! long_name = "zero path difference UTC time"
//!
//! [[aux]]
//! private_varname = "year"
//! long_name = "year"
//!
//! [[aux]]
//! private_varname = "day"
//! long_name = "day of year"
//! attr_overrides = {units = "Julian day", description = "1-based day of year"}
//!
//! [[aux]]
//! private_varname = "solzen"
//! long_name = "solar zenith angle"
//! ```
//!
//! By default, any of the standard TCCON auxiliary variables not listed will be added. See the [Defaults](#defaults)
//! section below for how to modify that behavior.
//!
//! # Derived variables
//!
//! Derived variables are similar to auxiliary variables in that they are not directly associated with a single Xgas.
//! Unlike auxiliary variables, these cannot be simply copied from the private netCDF file. Instead, they must be
//! computed from one or more private variables. Because of that, there are a specific set of these variables pre-defined
//! by the public writer.
//!
//! # Xgases
//!
//! # Xgas discovery
//!
//! # Other sections
//! ## Gas long names
//! For Xgases discovered automatically, rather than specified explicitly, we still want to be able to include the gas's
//! proper name in the `long_name` attributes, rather than just its chemical formula. This section allows you to map
//! the formula (e.g., "co2") to the proper name (e.g., "carbon dioxide"), e.g.:
//!
//! ```toml
//! [gas_long_names]
//! co2 = "carbon dioxide"
//! ch4 = "methane"
//! co = "carbon monoxide"
//! ```
//!
//! Note that these are the gases, not Xgases. A default list is included if not turned off in the [Defaults](#defaults)
//! section. See the source code for [`DEFAULT_GAS_LONG_NAMES`] for the current list. You can override any of those without
//! turning off the defaults; e.g., setting `h2o = "dihydrogen monoxide"` in this section will replace the default of "water".
//!
//! # Defaults
//!
//! Unlike other sections, the `[defaults]` section does not define variables to copy; instead, it modifies how the
//! other sections are filled in. If this section is omitted, then each of the other sections will add any missing TCCON
//! standard variables to the actual configuration. The following options are available to change that behavior:
//!
//! - `disable_all` (bool): setting this to `true` will ensure that no TCCON standard variables are added in any section.
//!   Its default value is `false`.
//! - `aux_vars` (bool): setting this to `false` will prevent TCCON standard auxiliary variables from being added in the
//!   `aux` section. Its default value is `true`.
//! - `gas_long_names` (bool): setting this to `false` will prevent the standard list of chemical formulae to proper gas
//!   names being added to `[gas_long_names]`. Its default value is `true`.
use std::{collections::HashSet, io::Read, path::Path};

use indexmap::IndexMap;
use serde::{de::Error, Deserialize};

use crate::{
    copying::XgasAncillary,
    discovery::{XgasMatchMethod, XgasMatchRule},
    AuxVarCopy, XgasCopy,
};

pub(crate) static STANDARD_TCCON_TOML: &'static str = include_str!("tccon_configs/standard.toml");

pub(crate) static DEFAULT_GAS_LONG_NAMES: &'static [(&'static str, &'static str)] = &[
    ("co2", "carbon dioxide"),
    ("ch4", "methane"),
    ("n2o", "nitrous oxide"),
    ("co", "carbon monoxide"),
    ("h2o", "water"),
    ("hdo", "semiheavy water"),
    ("hf", "hydrofluoric acid"),
    ("o3", "ozone"),
];

static DEFAULT_EXCLUDE_GASES: &'static [&'static str] = &["th2o", "fco2", "zco2"];

/// Configuration for the public netCDF writer.
///
/// Users should see the [module level documentation](crate::config) for information
/// on how to write or modify the configuration file. The structure level documentation
/// here is for developers.
///
/// This will normally be read from a TOML file with the [`Config::from_toml_file`]
/// function. This function will deserialize the structure from TOML as normal, but
/// also potentially add missing default values to the various sections depending on the
/// settings in the `defaults` field. If deserialized manually (e.g., because it needed
/// to be deserialized from a different format than TOML), then one should call the
/// [`Config::maybe_add_defaults`] method after deserialization, which provides this
/// behavior.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// Configurations for which auxiliary variables to copy, along with attributes
    /// to update or remove.
    #[serde(default)]
    pub(crate) aux: Vec<AuxVarCopy>,

    /// A mapping of gas abbreviations (e.g., "co2") to their proper names
    /// (e.g., "carbon dioxide").
    #[serde(default)]
    pub(crate) gas_long_names: IndexMap<String, String>,

    #[serde(default)]
    pub(crate) xgas: Vec<XgasCopy>,

    #[serde(default)]
    pub(crate) discovery: XgasDiscoveryConfig,

    /// Toggles for whether to add default values to each section.
    ///
    /// # Developer note
    /// This is included as a private field because the intended behavior is that
    /// this is only references when first loading the configuration to determine
    /// whether to add default values to each section, and after that, the writer
    /// simply looks at the other fields. Originally, I considered having a `MetaConfig`
    /// structure that held defaults and a flattened `Config`, and then deserializing
    /// `Config` through `MetaConfig` (so the defaults could be dropped entirely),
    /// but ran afoul of [bugs around serde flattening](https://github.com/toml-rs/toml/issues/589).
    /// Not only was it giving the wrong location for the missing field, but seemed to
    /// be incorrectly ignoring defaults on the inner config.
    #[serde(default)]
    defaults: DefaultsConfig,
}

impl Config {
    pub(crate) fn from_toml_str(s: &str) -> Result<Self, toml::de::Error> {
        let mut config: Config = toml::from_str(s)?;
        config.maybe_add_defaults();
        Ok(config)
    }

    pub(crate) fn from_toml_file(p: &Path) -> Result<Self, toml::de::Error> {
        let mut f = std::fs::File::open(p).map_err(|e| {
            toml::de::Error::custom(format!("error opening TOML file {}: {e}", p.display()))
        })?;
        let mut buf = String::new();
        f.read_to_string(&mut buf).map_err(|e| {
            toml::de::Error::custom(format!("error reading TOML file {}: {e}", p.display()))
        })?;
        Self::from_toml_str(&buf)
    }

    pub(crate) fn maybe_add_defaults(&mut self) {
        if self.defaults.aux_vars && !self.defaults.disable_all {
            add_default_aux_vars(self);
        }

        if self.defaults.gas_long_names && !self.defaults.disable_all {
            add_default_gas_long_names(self);
        }

        if self.defaults.xgas_rules && !self.defaults.disable_all {
            add_default_xgas_rules(self);
        }

        if self.defaults.excluded_gases && !self.defaults.disable_all {
            add_default_exclude_gases(self);
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        let mut me = Self {
            aux: Default::default(),
            gas_long_names: Default::default(),
            xgas: Default::default(),
            discovery: Default::default(),
            defaults: Default::default(),
        };
        me.maybe_add_defaults();
        me
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct XgasDiscoveryConfig {
    #[serde(default)]
    pub(crate) excluded_xgas_variables: Vec<String>,
    #[serde(default)]
    pub(crate) excluded_gases: Vec<String>,
    #[serde(default)]
    pub(crate) rules: Vec<XgasMatchRule>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct DefaultsConfig {
    #[serde(default)]
    disable_all: bool,
    #[serde(default = "default_true")]
    aux_vars: bool,
    #[serde(default = "default_true")]
    gas_long_names: bool,
    #[serde(default = "default_true")]
    xgas_rules: bool,
    #[serde(default = "default_true")]
    excluded_gases: bool,
}

impl Default for DefaultsConfig {
    fn default() -> Self {
        Self {
            aux_vars: true,
            gas_long_names: true,
            xgas_rules: true,
            excluded_gases: true,
            disable_all: false,
        }
    }
}

/// Helper function for serde default attributes
pub(crate) fn default_true() -> bool {
    true
}

/// Helper function for default attributes
pub(crate) fn default_attr_remove() -> Vec<String> {
    vec!["precision".to_string(), "standard_name".to_string()]
}

/// Helper function for the default list of auxiliary variables.
pub(crate) fn default_aux_vars() -> Vec<AuxVarCopy> {
    vec![
        AuxVarCopy::new("time", "time", true),
        AuxVarCopy::new("year", "year", true),
        AuxVarCopy::new("day", "day of year", true),
        AuxVarCopy::new("prior_altitude", "altitude a.s.l.", true),
        AuxVarCopy::new("ak_altitude", "altitude a.s.l.", true),
        AuxVarCopy::new("ak_pressure", "pressure", true),
        AuxVarCopy::new("hour", "UTC hour", true),
        AuxVarCopy::new("lat", "latitude", true),
        AuxVarCopy::new("long", "longitude", true),
        AuxVarCopy::new("zobs", "observation altitude", true),
        AuxVarCopy::new("zmin", "pressure altitude", true),
        AuxVarCopy::new("solzen", "solar zenith angle", true),
        AuxVarCopy::new("azim", "solar azimuth angle", true),
        AuxVarCopy::new("tout", "atmospheric temperature", true),
        AuxVarCopy::new("pout", "surface pressure", true),
        AuxVarCopy::new("hout", "atmospheric humidity", true),
        AuxVarCopy::new("sia", "average solar intensity", true),
        AuxVarCopy::new("fvsi", "fractional variation in solar intensity", true),
        AuxVarCopy::new("wspd", "wind speed", true),
        AuxVarCopy::new("wdir", "wind direction", true),
        AuxVarCopy::new("o2_mean_mole_fraction", "dry atmospheric mole fraction of oxygen", true),
        AuxVarCopy::new("integration_operator", "integration operator", true),
        AuxVarCopy::new("o2_7885_am_o2", "airmass", true)
            .with_public_varname("airmass")
            .with_attr_override("units", "1")
            .with_attr_override("description", "airmass computed as the total vertical column of O2 divided by the total slant column of O2 retrieved from the window centered at 7885 cm-1."),
    ]
}

pub(crate) fn default_ancillary_infer_first() -> XgasAncillary {
    XgasAncillary::InferredIfFirst
}

pub(crate) fn default_ancillary_infer() -> XgasAncillary {
    XgasAncillary::Inferred
}

fn add_default_aux_vars(config: &mut Config) {
    let aux_var_names: HashSet<String> = config
        .aux
        .iter()
        .map(|aux| aux.private_varname.clone())
        .collect();

    for default_aux in default_aux_vars() {
        if !aux_var_names.contains(&default_aux.private_varname) {
            config.aux.push(default_aux);
        }
    }
}

fn add_default_gas_long_names(config: &mut Config) {
    for (gas, name) in DEFAULT_GAS_LONG_NAMES {
        let gas = gas.to_string();
        if !config.gas_long_names.contains_key(&gas) {
            config.gas_long_names.insert(gas, name.to_string());
        }
    }
}

fn add_default_xgas_rules(config: &mut Config) {
    let pattern = "^x(?<gas>[a-z0-9]+)$".to_string();

    for rule in config.discovery.rules.iter() {
        if rule.is_given_regex(&pattern) {
            return;
        }
    }

    let mut std_rule = XgasMatchRule::new(
        XgasMatchMethod::regex_from_string(pattern)
            .expect("default Xgas regular expression must be valid"),
    );
    std_rule.traceability_scale = Some(XgasAncillary::OptInferredIfFirst);
    config.discovery.rules.push(std_rule);
}

fn add_default_exclude_gases(config: &mut Config) {
    for &gas in DEFAULT_EXCLUDE_GASES {
        if !config
            .discovery
            .excluded_gases
            .iter()
            .any(|g| gas == g.as_str())
        {
            config.discovery.excluded_gases.push(gas.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        // Test that an empty config correctly defaults to including the default
        // values.
        let cfg = Config::from_toml_str("").expect("deserialization should not fail");
        assert_eq!(cfg.aux.len(), default_aux_vars().len());
        assert_eq!(cfg.gas_long_names.len(), DEFAULT_GAS_LONG_NAMES.len());

        // Test that if the [defaults] section is given and empty, the individual
        // default option fields still say to include the default values.
        let toml_str = "[defaults]";
        let cfg = Config::from_toml_str(toml_str).expect("deserialization should not fail");
        assert_eq!(cfg.aux.len(), default_aux_vars().len());
        assert_eq!(cfg.gas_long_names.len(), DEFAULT_GAS_LONG_NAMES.len());

        // Test that explicitly requesting defaults works.
        let toml_str = r#"[defaults]
        aux_vars = true
        gas_long_names = true
        "#;
        let cfg = Config::from_toml_str(toml_str).expect("deserialization should not fail");
        assert_eq!(cfg.aux.len(), default_aux_vars().len());
        assert_eq!(cfg.gas_long_names.len(), DEFAULT_GAS_LONG_NAMES.len());

        // Test that the disable_all option for [defaults] works.
        let toml_str = r#"[defaults]
        disable_all = true"#;
        let cfg = Config::from_toml_str(toml_str).expect("deserialization should not fail");
        assert_eq!(cfg.aux.len(), 0);
        assert_eq!(cfg.gas_long_names.len(), 0);
    }
}
