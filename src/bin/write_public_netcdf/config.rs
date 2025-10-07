//! Interface for the configuration of which variables to copy
use std::{
    borrow::Cow,
    collections::VecDeque,
    convert::Infallible,
    fmt::Display,
    io::Read,
    path::{Path, PathBuf},
    str::FromStr,
};

use figment::{providers::Format, Figment};
use indexmap::IndexMap;
use itertools::Itertools;
use serde::Deserialize;

use crate::{
    constants::DEFAULT_GAS_LONG_NAMES,
    copying::{CopyGlobalAttr, PriorProfCopy, XgasAncInferOptions, XgasAncillary},
    discovery::{AncillaryDiscoveryMethod, XgasMatchMethod, XgasMatchRule},
    AuxVarCopy, ComputedVariable, XgasCopy,
};

pub(crate) static COMMON_TOML: &'static str = include_str!("included_configs/common.toml");
pub(crate) static STANDARD_TCCON_TOML: &'static str =
    include_str!("included_configs/tccon_standard.toml");
pub(crate) static EXTENDED_TCCON_TOML: &'static str =
    include_str!("included_configs/tccon_extended.toml");
pub(crate) static STANDARD_EM27_TOML: &'static str =
    include_str!("included_configs/em27sun_standard.toml");

#[derive(Debug, thiserror::Error)]
pub(crate) enum ConfigError {
    #[error(transparent)]
    IncludeError(#[from] figment::Error),
    #[error("Error reading file {}: {e}", p.display())]
    IoError { p: PathBuf, e: std::io::Error },
    #[error("Error deserializing string: {0}")]
    StringDeser(toml::de::Error),
    #[error("Error deserializing file {p}: {e}")]
    Deserialization { p: String, e: toml::de::Error },
}

impl ConfigError {
    fn io<P: Into<PathBuf>>(p: P, e: std::io::Error) -> Self {
        Self::IoError { p: p.into(), e }
    }

    fn deser<P: Display>(p: P, e: toml::de::Error) -> Self {
        Self::Deserialization {
            p: format!("{p}"),
            e,
        }
    }
}

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

    #[serde(default)]
    pub(crate) extra_priors: Vec<PriorProfCopy>,

    /// A mapping of gas abbreviations (e.g., "co2") to their proper names
    /// (e.g., "carbon dioxide").
    #[serde(default)]
    pub(crate) gas_long_names: IndexMap<String, String>,

    #[serde(default)]
    pub(crate) xgas: Vec<XgasCopy>,

    #[serde(default)]
    pub(crate) discovery: XgasDiscoveryConfig,

    #[serde(default)]
    pub(crate) global_attributes: AttributeConfig,

    /// If given, this will be included before "public"
    /// in the output file extension, e.g. setting this
    /// to "extended." will result in the output file being
    /// named `STEM.extended.public...`
    #[serde(default)]
    pub(crate) extra_extension: Option<String>,

    /// Toggles for whether to add default values to each section.
    ///
    /// # Developer note
    /// This is included as a private field because the intended behavior is that
    /// this is only referenced when first loading the configuration to determine
    /// whether to add default values to each section, and after that, the writer
    /// simply looks at the other fields. Originally, I considered having a `MetaConfig`
    /// structure that held defaults and a flattened `Config`, and then deserializing
    /// `Config` through `MetaConfig` (so the defaults could be dropped entirely),
    /// but ran afoul of [bugs around serde flattening](https://github.com/toml-rs/toml/issues/589).
    /// Not only was it giving the wrong location for the missing field, but seemed to
    /// be incorrectly ignoring defaults on the inner config.
    #[serde(default)]
    defaults: DefaultsConfig,

    /// Sources to merge with this configuration.
    ///
    /// # Developer note
    /// This field is included to allow the deny_unknown_fields annotation.
    /// In normal use, this field will not be referenced, as it must be parsed
    /// before the configuration can be fully deserialized.
    #[serde(default)]
    #[allow(dead_code)]
    include: Vec<String>,
}

impl Config {
    /// Load a configuration from a string already in memory.
    pub(crate) fn from_toml_str(s: &str) -> Result<Self, ConfigError> {
        let mut fig = Figment::new().adjoin(figment::providers::Toml::string(s));
        let first_includes = Self::get_includes_from_config_str(&s)
            .map(|v| VecDeque::from(v))
            .map_err(|e| ConfigError::StringDeser(e))?;
        let all_includes = Self::collect_included_configs(first_includes)?;
        for incl in all_includes {
            fig = fig.adjoin(incl.into_provider());
        }
        let mut config: Config = fig.extract()?;
        config.finalize();
        Ok(config)
    }

    /// Load a configuration from a path to a TOML file.
    ///
    /// This function should be preferred over reading in a TOML file as a string
    /// and passing that string to [`Config::from_toml_str`], as this function will
    /// provide better error messages pointing to the top-level path if there is a problem.
    pub(crate) fn from_toml_file(p: PathBuf) -> Result<Self, ConfigError> {
        let mut fig = Figment::new().adjoin(figment::providers::Toml::file(&p));
        let s = read_file(&p)?;
        let first_includes = Self::get_includes_from_config_str(&s)
            .map(|v| VecDeque::from(v))
            .map_err(|e| ConfigError::deser(p.display(), e))?;
        let all_includes = Self::collect_included_configs(first_includes)?;
        for incl in all_includes {
            fig = fig.adjoin(incl.into_provider());
        }
        let mut config: Config = fig.extract()?;
        config.finalize();
        Ok(config)
    }

    fn collect_included_configs(
        mut to_check: VecDeque<IncludeSource>,
    ) -> Result<Vec<IncludeSource>, ConfigError> {
        let mut paths_out = vec![];
        loop {
            let next_source = match to_check.pop_front() {
                Some(p) => p,
                None => break,
            };

            let s = next_source.as_cow_str()?;

            let includes = Self::get_includes_from_config_str(&s)
                .map_err(|e| ConfigError::deser(&next_source, e))?;
            // We add the includes in reverse order so that they come off the
            // front in the order that they were in the config. This also ensures
            // that we do a depth-first recursion.
            for incl in includes.into_iter().rev() {
                to_check.push_front(incl);
            }
            paths_out.push(next_source);
        }
        Ok(paths_out)
    }

    fn get_includes_from_config_str(s: &str) -> Result<Vec<IncludeSource>, toml::de::Error> {
        // TODO: make paths relative to the configuration they are taken from.
        let includes: IncludesConfig = toml::from_str(&s)?;
        let sources = includes
            .include
            .into_iter()
            .map(|s| IncludeSource::from_str(&s).unwrap())
            .collect_vec();
        Ok(sources)
    }

    fn finalize(&mut self) {
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
            extra_priors: Default::default(),
            gas_long_names: Default::default(),
            xgas: Default::default(),
            discovery: Default::default(),
            global_attributes: Default::default(),
            extra_extension: Default::default(),
            defaults: Default::default(),
            include: Default::default(),
        };
        me.finalize();
        me
    }
}

#[derive(Debug, Deserialize)]
struct IncludesConfig {
    #[serde(default)]
    include: Vec<String>,
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

#[derive(Debug, Default, Deserialize)]
pub(crate) struct AttributeConfig {
    must_copy: Vec<String>,
    copy_if_present: Vec<String>,
}

impl AttributeConfig {
    pub(crate) fn make_attr_list(&self) -> Vec<CopyGlobalAttr> {
        let mut attrs = vec![];
        for name in self.must_copy.iter() {
            attrs.push(CopyGlobalAttr::MustCopy {
                name: name.to_string(),
            });
        }
        for name in self.copy_if_present.iter() {
            attrs.push(CopyGlobalAttr::CopyIfPresent {
                name: name.to_string(),
            });
        }
        attrs
    }
}

#[derive(Debug, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct DefaultsConfig {
    #[serde(default)]
    disable_all: bool,
    #[serde(default = "default_true")]
    gas_long_names: bool,
    #[serde(default = "default_true")]
    xgas_rules: bool,
}

impl Default for DefaultsConfig {
    fn default() -> Self {
        Self {
            gas_long_names: true,
            xgas_rules: true,
            disable_all: false,
        }
    }
}

enum IncludeSource {
    Path(PathBuf),
    Common,
    TcconStandard,
    TcconExtended,
    Em27SunStandard,
}

impl FromStr for IncludeSource {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "COMMON" => Ok(Self::Common),
            "TCCON_STANDARD" => Ok(Self::TcconStandard),
            "TCCON_EXTENDED" => Ok(Self::TcconExtended),
            "EM27SUN_STANDARD" => Ok(Self::Em27SunStandard),
            _ => Ok(Self::Path(PathBuf::from(s))),
        }
    }
}

impl Display for IncludeSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IncludeSource::Path(p) => write!(f, "{}", p.display()),
            IncludeSource::Common => write!(f, "Common standard config"),
            IncludeSource::TcconStandard => write!(f, "TCCON standard config"),
            IncludeSource::TcconExtended => write!(f, "TCCON extended config"),
            IncludeSource::Em27SunStandard => write!(f, "EM27/SUN standard config"),
        }
    }
}

impl IncludeSource {
    fn into_provider(self) -> figment::providers::Data<figment::providers::Toml> {
        match self {
            IncludeSource::Path(p) => figment::providers::Toml::file(p),
            IncludeSource::Common => figment::providers::Toml::string(COMMON_TOML),
            IncludeSource::TcconStandard => figment::providers::Toml::string(STANDARD_TCCON_TOML),
            IncludeSource::TcconExtended => figment::providers::Toml::string(EXTENDED_TCCON_TOML),
            IncludeSource::Em27SunStandard => figment::providers::Toml::string(STANDARD_EM27_TOML),
        }
    }

    fn as_cow_str<'a>(&'a self) -> Result<Cow<'a, str>, ConfigError> {
        match self {
            IncludeSource::Path(path) => Ok(Cow::Owned(read_file(path)?)),
            IncludeSource::Common => Ok(Cow::Borrowed(COMMON_TOML)),
            IncludeSource::TcconStandard => Ok(Cow::Borrowed(STANDARD_TCCON_TOML)),
            IncludeSource::TcconExtended => Ok(Cow::Borrowed(EXTENDED_TCCON_TOML)),
            IncludeSource::Em27SunStandard => Ok(Cow::Borrowed(STANDARD_EM27_TOML)),
        }
    }
}

fn read_file(p: &Path) -> Result<String, ConfigError> {
    let mut f = std::fs::File::open(p).map_err(|e| ConfigError::io(p, e))?;
    let mut buf = String::new();
    f.read_to_string(&mut buf)
        .map_err(|e| ConfigError::io(p, e))?;
    Ok(buf)
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

pub(crate) fn default_ancillary_infer_first() -> XgasAncillary {
    XgasAncillary::Inferred(XgasAncInferOptions::new_if_first(None))
}

pub(crate) fn default_ancillary_infer() -> XgasAncillary {
    XgasAncillary::Inferred(XgasAncInferOptions::new_required(None))
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
    std_rule.traceability_scale = Some(AncillaryDiscoveryMethod::Inferred(
        XgasAncInferOptions::opt_new_if_first(None),
    ));
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
        assert_eq!(cfg.aux.len(), 0);
        assert_eq!(cfg.gas_long_names.len(), DEFAULT_GAS_LONG_NAMES.len());

        // Test that if the [defaults] section is given and empty, the individual
        // default option fields still say to include the default values.
        let toml_str = "[defaults]";
        let cfg = Config::from_toml_str(toml_str).expect("deserialization should not fail");
        assert_eq!(cfg.aux.len(), 0);
        assert_eq!(cfg.gas_long_names.len(), DEFAULT_GAS_LONG_NAMES.len());

        // Test that explicitly requesting defaults works.
        let toml_str = r#"[defaults]
        gas_long_names = true
        "#;
        let cfg = Config::from_toml_str(toml_str).expect("deserialization should not fail");
        assert_eq!(cfg.aux.len(), 0);
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

    #[test]
    fn test_standard_tccon_config() {
        Config::from_toml_str(STANDARD_TCCON_TOML)
            .expect("deserializing the standard TCCON configuration should not fail");
    }

    #[test]
    fn test_extended_tccon_config() {
        Config::from_toml_str(EXTENDED_TCCON_TOML)
            .expect("deserializing the extended TCCON configuration should not fail");
    }
}
