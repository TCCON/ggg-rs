//! Interface for the configuration of which variables to copy
use std::{collections::HashSet, io::Read, path::Path};

use indexmap::IndexMap;
use serde::{de::Error, Deserialize};

use crate::{
    constants::DEFAULT_GAS_LONG_NAMES,
    copying::XgasAncillary,
    discovery::{AncillaryDiscoveryMethod, XgasMatchMethod, XgasMatchRule},
    AuxVarCopy, ComputedVariable, XgasCopy,
};

pub(crate) static STANDARD_TCCON_TOML: &'static str = include_str!("tccon_configs/standard.toml");

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

    #[serde(default)]
    pub(crate) computed: Vec<ComputedVariable>,

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
        config.finalize();
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

    fn finalize(&mut self) {
        if self.defaults.aux_vars && !self.defaults.disable_all {
            add_default_aux_vars(self);
        }

        if self.defaults.gas_long_names && !self.defaults.disable_all {
            add_default_gas_long_names(self);
        }

        if self.defaults.xgas_rules && !self.defaults.disable_all {
            add_default_xgas_rules(self);
        }

        // Always do this; we allow the full name to default to an empty
        // string when deserializing, but we don't want that to make it to
        // the actual writing. This ensures that those strings are populated
        // with something.
        add_defined_xgas_full_names(self);
    }
}

impl Default for Config {
    fn default() -> Self {
        let mut me = Self {
            aux: Default::default(),
            computed: Default::default(),
            gas_long_names: Default::default(),
            xgas: Default::default(),
            discovery: Default::default(),
            defaults: Default::default(),
        };
        me.finalize();
        me
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct XgasDiscoveryConfig {
    #[serde(default)]
    pub(crate) rule: Vec<XgasMatchRule>,
    #[serde(default)]
    pub(crate) excluded_xgas_variables: Vec<String>,
    #[serde(default)]
    pub(crate) excluded_gases: Vec<String>,
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
}

impl Default for DefaultsConfig {
    fn default() -> Self {
        Self {
            aux_vars: true,
            gas_long_names: true,
            xgas_rules: true,
            disable_all: false,
        }
    }
}

/// Helper function for serde default attributes
pub(crate) fn default_true() -> bool {
    true
}

pub(crate) fn default_empty_string() -> String {
    "".to_string()
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
            .with_public_name("airmass")
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
        .map(|aux| aux.private_name.clone())
        .collect();

    for default_aux in default_aux_vars() {
        if !aux_var_names.contains(&default_aux.private_name) {
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
    let pattern = "^x(?<gas>[a-z][a-z0-9]*)$".to_string();

    for rule in config.discovery.rule.iter() {
        if rule.is_given_regex(&pattern) {
            return;
        }
    }

    let mut std_rule = XgasMatchRule::new(
        XgasMatchMethod::regex_from_string(pattern, None)
            .expect("default Xgas regular expression must be valid"),
    );
    std_rule.traceability_scale = Some(AncillaryDiscoveryMethod::OptInferredIfFirst);
    config.discovery.rule.push(std_rule);
}

fn add_defined_xgas_full_names(config: &mut Config) {
    for xgas in config.xgas.iter_mut() {
        if xgas.gas_full_name().is_empty() {
            log::debug!(
                "No full name specified for {}, will insert one based on the gas abbreviation",
                xgas.xgas_varname()
            );
            if let Some(name) = config.gas_long_names.get(xgas.gas()) {
                xgas.set_gas_full_name(name.to_string());
            } else {
                xgas.set_gas_full_name(xgas.gas().to_string());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

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

    #[test]
    fn test_book_examples() {
        let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let book_subdir = crate_root.join("book/src/postproc/write_public_netcdf");
        let md_files = book_subdir
            .read_dir()
            .expect("should be able to get files from the book subdir")
            .map(|e| e.expect("should be able to get md files from book").path());
        let block_iter = ggg_rs::test_utils::iter_fenced_blocks("toml", md_files);
        for block in block_iter {
            let block = block.expect("should be able to read fenced block");
            let res = Config::from_toml_str(&block.text);
            assert!(
                res.is_ok(),
                "could not deserialize an example in line {} of file {}:\n\n{}\n\nerror was\n\n{}",
                block.line,
                block.file.display(),
                block.text,
                res.unwrap_err()
            );
        }
    }
}
