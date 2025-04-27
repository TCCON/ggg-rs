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
//! private_name = "solzen"
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
//! `public_name` field:
//!
//! ```toml
//! [[aux]]
//! private_name = "solzen"
//! public_name = "solar_zenith_angle"
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
//! private_name = "day"
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
//! private_name = "day"
//! long_name = "day of year"
//! required = false
//! ```
//!
//! Each auxiliary variable to copy will have its own `[[aux]]` section, for example:
//!
//! ```toml
//! [[aux]]
//! private_name = "time"
//! long_name = "zero path difference UTC time"
//!
//! [[aux]]
//! private_name = "year"
//! long_name = "year"
//!
//! [[aux]]
//! private_name = "day"
//! long_name = "day of year"
//! attr_overrides = {units = "Julian day", description = "1-based day of year"}
//!
//! [[aux]]
//! private_name = "solzen"
//! long_name = "solar zenith angle"
//! ```
//!
//! By default, any of the standard TCCON auxiliary variables not listed will be added. See the [Defaults](#defaults)
//! section below for how to modify that behavior.
//!
//! # Computed variables
//!
//! Computed variables are similar to auxiliary variables in that they are not directly associated with a single Xgas.
//! Unlike auxiliary variables, these cannot be simply copied from the private netCDF file. Instead, they must be
//! computed from one or more private variables. Because of that, there are a specific set of these variables pre-defined
//! by the public writer. Currently only one computed variable type exists, "prior_source". You can specify it in the
//! configuration as follows:
//!
//! ```toml
//! [[computed]]
//! type = "prior_source"
//! ```
//!
//! By default, this creates a public variable named "apriori_data_source". You can change this with the `public_name` field, e.g.:
//!
//! ```toml
//! [[computed]]
//! type = "prior_source"
//! public_name = "geos_source_set"
//! ```
//!
//! # Xgas discovery
//!
//! Usually we do not want to specify every single Xgas to copy; instead, we want the writer to scan the private file to identify
//! Xgases and copy everything that matches. This both saves a lot of tedious typing in the configuration and minimizes the possibilty
//! of copy-paste errors.
//!
//! ## Rules
//!
//! The first part of the discovery section is a list of rules for how to find Xgas variables. These come in two variants:
//!
//! 1. Suffix rules: these look for variables that start with something starting with an Xgas-like pattern
//!    and ending in the given suffix. The full regex is `^x([a-z][a-z0-9]*)_\w+_{suffix}$`, where `{suffix}` is the provided suffix.
//!    Note that the suffix is passed through [`regex::escape`] to ensure that any special characters are escaped; it will only
//!    be treated as a literal.
//! 2. Regex rules: these allow you to specify a regular expression to match variables names. The regex _must_ include a named
//!    capture group with the name "gas" that extracts the physical gas abbreviation (i.e., the `gas` value in an `Xgas` entry).
//!    This looks like `(?<gas>...)` where the `...` is the regular subexpression that matches that part of the string.
//!
//! By default, the configuration will add a single regex rule that matches the pattern `^x(?<gas>[a-z][a-z0-9]*)$`.
//! You can disable this by setting `xgas_rules = false` in the [`[defaults]`](#defaults) section of the config.
//! This rule is designed to match basic Xgas variables, e.g., "xch4", "xn2o", etc.
//!
//! The rules also include default settings for the prior profile, prior column average, averaging kernel (and its slant Xgas bins),
//! and the traceability scale. These can be specified the same way as described [in the Xgases ancillary subsection](#ancillary-variable-specifications).
//! and the defaults are the same as well.
//!
//! An example of a regular expression rule that uses the default ways to infer its ancillary variables is:
//!
//! ```toml
//! [[discovery.rule]]
//! regex = '^column_average_(?<gas>\w+)$'
//! ```
//!
//! Two things to note are:
//!
//! 1. The regular expression is inside single quotes; this is how TOML specifies literal strings and it the
//!    most convenient way to write regexes that include backslashes. (Otherwise TOML itself will intepret them
//!    as escape characters.)
//! 2. The regex includes `^` and `$` to anchor the start and end of the pattern. In most cases, you will probably
//!    want to do so as well to avoid matching arbitrary parts of variable names.
//!
//! An example of a suffix rule that also indicates that variables matching this rule should not include averaging
//! kernels or the traceability scale is:
//!
//! ```toml
//! [[discovery.rule]]
//! suffix = "mir"
//! ak = { type = "omit" }
//! traceability_scale = { type = "omit" }
//! ```
//!
//! Note that the suffix rule contains a "suffix" key, while the regular expression rule has a "regex" key -
//! this is how they are distinguished. Also note that rules are checked in order, and a variable is added following
//! the first rule that matches. This means that if a variable matches multiple rules, then its ancillary variables will
//! be set up following the first rule that matched.
//!
//! ## Exclusions
//!
//! The second part of the discovery section are lists of gases or variables to exclude. The first option is
//! ``excluded_xgas_variables``. If a variable's private file name matches one of the names in that list, it
//! will not be copied even if it matches one of the rules. The other option is `excluded_gases`, which matches
//! not the variable name, but the physical gas. The easiest way to explain this is to consider the standard TCCON
//! configuration:
//!
//! ```toml
//! excluded_xgas_variables = ["xo2"]
//! excluded_gases = ["th2o", "fco2", "zco2"]
//! ```
//!
//! `excluded_xgas_variables` specifically excludes the "xo2" variable; this would match the default regex rule meant to capture Xgases
//! measured on the primary detector, but we don't want to include it because it is not useful for data users. However,
//! O2 measured on a silicon detector may be useful, so we do not want to exclude all O2 variables. `excluded_gases` lists
//! three gases that we want to exclude no matter what detector they are retrieved from. "fco2" and "zco2" are diagnostic
//! windows (for channelling and zero-level offset, respectively) and so will be included once for each detector. "th2o"
//! is temperature sensitive water, which is generally confusing for the average user, so we want to ensure that it is
//! also excluded from every detector.
//!
//!
//! # Xgases
//!
//! This section allows you to list specific Xgas variables to copy, along with some or all of the ancillary variables needed
//! to properly interpret them. Usually, you will not specify each Xgas by hand, but instead will use the [discovery](#xgas-discovery)
//! capability of the writer to automatically find each Xgas to copy. However, variables explicitly listed in this section take
//! precedence over those found by the discovery rules. This leads to two cases where you might specify an Xgas in this section:
//!
//! 1. The Xgas does not follow the common naming convention, thus making it difficult to discover with simple rules, or difficult
//!    to map from the Xgas variable to the ancillary variable.
//! 2. The Xgas needs a different way to handle one of its ancillary variables that the default discovery does.
//!
//! Each Xgas has the following options:
//!
//! - `xgas` (required): the variable name
//! - `gas` (required): the physical gas name, e.g., "co2" for all the various CO2 variables (regular, `wco2`, and `lco2`).
//!   This is used to match up to, e.g., the priors which do not distinguish between the different spectra windows.
//! - `gas_long` (optional): the full name of the gas instead of its abbreviation, e.g. "carbon dioxide" for CO2. If not given,
//!   then the configuration will try to find the `gas` value in its [`[gas_long_names]`](#gas-long-names) section and use that,
//!   falling back on the `gas` value if the gas is not defined in the gas long names section.
//! - `prior_profile` (optional): how to copy the a priori profile.
//! - `prior_xgas` (optional): how to copy the a priori column average.
//! - `ak` (optional): how to copy the averaging kernels.
//! - `slant_bin` (optional): how to find the slant Xgas bin variable needed to expand the AKs.
//! - `traceability_scale` (optional): how to find the variable containing the WMO or analogous scale for this data.
//!
//! `prior_profile`, `prior_xgas`, `ak`, `slant_bin`, and `traceability_scale` can all be defined following the syntax
//! in [Ancillary variable specifications](#ancillary-variable-specifications), below. Note that `slant_bin` is a special
//! case in that it will only be used if the AKs are to be copied, but cannot be omitted in that case.
//!
//! To illustrate the two main use cases for this section, here is an excerpt from the standard TCCON configuration:
//!
//! ```toml
//! [[xgas]]
//! xgas = "xluft"
//! gas = "luft"
//! gas_long = "dry air"
//! prior_profile = { type = "omit" }
//! prior_xgas = { type = "omit" }
//! ak = { type = "omit" }
//!
//! [[xgas]]
//! xgas = "xco2_x2019"
//! gas = "co2"
//! prior_profile = { type = "specified_if_first", private_name = "prior_1co2", public_name = "prior_co2" }
//! prior_xgas = { type = "specified_if_first", private_name = "prior_xco2_x2019", public_name = "prior_xco2" }
//! ak = { type = "specified_if_first", private_name = "ak_xco2" }
//! slant_bin = { type = "specified", private_name = "ak_slant_xco2_bin" }
//!
//! [[xgas]]
//! xgas = "xwco2_x2019"
//! gas = "co2"
//! prior_profile = { type = "specified_if_first", private_name = "prior_1co2", public_name = "prior_co2" }
//! prior_xgas = { type = "specified_if_first", private_name = "prior_xwco2_x2019", public_name = "prior_xco2" }
//! ak = { type = "specified_if_first", private_name = "ak_xwco2" }
//! slant_bin = { type = "specified", private_name = "ak_slant_xwco2_bin" }
//! ```
//!
//! First we have `xluft`. This variable would be discovered by the [default rule](#rules); however, that rule will
//! require prior information and AKs. The prior information is not useful for Xluft, so we want to avoid copying
//! that to reduce the number of extraneous variables, and there are no AKs for Xluft. Thus we specify "omit" for each
//! of these to tell the writer not to look for them. We do not have to include `slant_bin`, because omitting the AKs
//! implicitly skips that, and `traceability_scale` can be left as normal because there is an "aicf_xluft_scale" variable
//! in the private netCDF files.
//!
//! Second we have `xco2_x2019` and `xwco2_x2019`. (We have omitted the x2007 variables and `lco2_x2019` for brevity).
//! These would not be discovered by the [default rule](#rules). Further, the mapping to their prior and AK variables is
//! unique: all the CO2 Xgas variables can share the prior profiles and column averages, and each "flavor" of CO2 (regular,
//! wCO2, or lCO2) can use the same AKs whether it is on the X2007 or X2019 scale. Thus, we not only define that these
//! variables need copied, but that we want to rename the prior variables to just "prior_co2" and "prior_xco2" and only
//! copy these the first time we find them. We also ensure that the AKs and slant bins point to the correct variables.
//!
//! ## Ancillary variable specifications
//!
//! The ancillary variables (prior profile, prior Xgas, AK, slant bin, and traceability scale) can be defined
//! as one of the following six types:
//!
//! - `inferred`: indicates that this ancillary variable must be included and should not conflict with any other
//!   variable. The private and public variable names will be inferred from the Xgas and gas names.
//! - `inferred_if_first`: similar to `inferred`, except that it is not an error if the variable already exists
//!   in the public file. This is meant to support cases like described for CO2 above, where multiple Xgases share
//!   a single ancillary variable - as long as one Xgas copies it, the other Xgases do not need to.
//!   **Note that the writer does not check that the outputs are equal!**
//! - `opt_inferred_if_first`: similar to `inferred_if_first`, except that if the private variable does not exist,
//!   then this variable will be skipped instead of causing an error. This is intended for [Xgas discovery rules](#rules)
//!   more than [explicit Xgas definitions](#xgases).
//! - `specified`: allows you to specify exactly which variable to copy with the `private_name` field. You can also
//!   give the `public_name` field to indicate what the variable name in the output file should be; if that is omitted,
//!   then the public variable will have the same name as the private variable. It is an error if the public variable
//!   already exists.
//! - `specified_if_first`: similar to `specified`, except that it is not an error if the public variable already exists.
//!   (This is analogous to the relationship between `inferred` and `inferred_if_first`). This has the same `private_name`
//!   and `public_name` fields as `specified`.
//! - `omit`: indicates that this variable should not be copied.
//!   
//! ## Ancillary variable name inference
//!
//! The writer uses the following rules when asked to infer ancillary variable names. In these, `{xgas_var}` indicates the Xgas
//! variable name and `{gas}` the physical gas name.
//!
//! - `prior_profile`: looks for a private variable named `prior_1{gas}` and writes to a variable named `prior_{gas}`.
//! - `prior_xgas`: looks for a private variable named `prior_{xgas_var}` and writes to the same variable.
//! - `ak`: looks for a private variable named `ak_{xgas_var}` and writes to the same variable.
//! - `slant_bin`: looks for a private variable named `ak_slant_{xgas_var}_bin`. This is not written, it is only used to expand
//!   the AKs to one-per-spectrum.
//! - `traceability_scale`: looks for a private variable named `aicf_{xgas_var}_scale`. The result is always written to the
//!   `wmo_or_analogous_scale` attribute of the Xgas variable; that cannot be altered by this configuration.
//!
//! # Gas long names
//! Ideally, all Xgases should include their proper name in the `long_name` attribute, rather than just its abbreviation.
//! This section allows you to map the formula (e.g., "co2") to the proper name (e.g., "carbon dioxide"), e.g.:
//!
//! ```toml
//! [gas_long_names]
//! co2 = "carbon dioxide"
//! ch4 = "methane"
//! co = "carbon monoxide"
//! ```
//!
//! Note that the keys are the gases, not Xgases. A default list is included if not turned off in the [`[Defaults]`](#defaults)
//! section. See the source code for [`DEFAULT_GAS_LONG_NAMES`] for the current list. You can override any of those without
//! turning off the defaults; e.g., setting `h2o = "dihydrogen monoxide"` in this section will replace the default of "water".
//!
//! Of course, when [explicitly defining an Xgas to copy](#xgases), you can write in the proper name as the `gas_long` value.
//! This section is most useful for automatically discovered Xgases, but it can also be useful when defining multiple Xgas variables
//! that refer to the same physical gas, as the standard TCCON configuration does with CO2.
//!
//! # Defaults
//!
//! Unlike other sections, the `[defaults]` section does not define variables to copy; instead, it modifies how the
//! other sections are filled in. If this section is omitted, then each of the other sections will add any missing TCCON
//! standard variables to the actual configuration. The following boolean options are available to change that behavior:
//!
//! - `disable_all`: setting this to `true` will ensure that no TCCON standard variables are added in any section.
//! - `aux_vars`: setting this to `false` will prevent TCCON standard auxiliary variables from being added in the
//!   `aux` section.
//! - `gas_long_names`: setting this to `false` will prevent the standard list of chemical formulae to proper gas
//!   names being added to `[gas_long_names]`.
//! - `xgas_rules`: setting this to `false` will prevent the standard list of patterns to match when looking for Xgas
//!   variables from being added to `[discovery.rules]`.
//!
//! # Debugging
//!
//! If variables are not being copied correctly, increase the verbosity of `write_public_netcdf` by adding `-v`
//! or `-vv` to the command line. The first will activate debug output, which includes a lot of information about
//! Xgas discovery. `-vv` will also activate trace-level logging, which will output even more information about the
//! configuration as the program read it.
use std::{collections::HashSet, io::Read, path::Path};

use indexmap::IndexMap;
use serde::{de::Error, Deserialize};

use crate::{
    copying::XgasAncillary,
    discovery::{XgasMatchMethod, XgasMatchRule},
    AuxVarCopy, ComputedVariable, XgasCopy,
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
    ("hcl", "hydrochloric acid"),
    ("o3", "ozone"),
];

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
    pub(crate) rules: Vec<XgasMatchRule>,
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
