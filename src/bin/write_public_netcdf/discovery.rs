use std::{collections::HashSet, fmt::Display, hash::RandomState, str::FromStr};

use indexmap::IndexMap;
use itertools::Itertools;
use regex::Regex;
use serde::Deserialize;

use crate::{copying::XgasAncillary, XgasCopy};

#[derive(Debug, thiserror::Error)]
pub(crate) enum DiscoveryError {
    #[error("There is a problem with the regex pattern '{pattern}': {error}")]
    BadRegex { pattern: String, error: String },
}

impl DiscoveryError {
    fn bad_regex<P: ToString, E: ToString>(pattern: P, error: E) -> Self {
        Self::BadRegex {
            pattern: pattern.to_string(),
            error: error.to_string(),
        }
    }
}

// -------------------------------- //
// Discovery configuration elements //
// -------------------------------- //

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum XgasMatchDeser {
    Suffix(String),
    Regex(String),
}

impl TryFrom<XgasMatchDeser> for XgasMatchMethod {
    type Error = DiscoveryError;

    fn try_from(value: XgasMatchDeser) -> Result<Self, Self::Error> {
        match value {
            XgasMatchDeser::Suffix(suf) => Ok(Self::Suffix(suf)),
            XgasMatchDeser::Regex(pat) => Self::regex_from_string(pat),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(try_from = "XgasMatchDeser")]
pub(crate) enum XgasMatchMethod {
    Suffix(String),
    Regex(Regex, String),
}

impl XgasMatchMethod {
    pub(crate) fn regex_from_string(pattern: String) -> Result<Self, DiscoveryError> {
        let re = Regex::from_str(&pattern).map_err(|e| {
            DiscoveryError::bad_regex(pattern.clone(), format!("it is not a valid pattern ({e})"))
        })?;
        // We require that the regex provide a capture group named "gas" to find the gas name
        // both for the XgasCopy instance and to check if it should be excluded. If the user
        // forgot to include such a capture group, error now.
        if !re.capture_names().any(|name| name == Some("gas")) {
            return Err(DiscoveryError::bad_regex(
                pattern.clone(),
                "while this is a valid regex, it must contain a named capture group, 'gas'",
            ));
        }
        Ok(Self::Regex(re, pattern.to_string()))
    }
}

impl Display for XgasMatchMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            XgasMatchMethod::Suffix(suffix) => write!(f, "suffix '{suffix}'"),
            XgasMatchMethod::Regex(_, pattern) => write!(f, "regex '{pattern}'"),
        }
    }
}

fn match_xgas_var_suffix<'a>(varname: &'a str, suffix: &str) -> Option<&'a str> {
    if varname.is_empty() {
        return None;
    }

    if !varname.starts_with('x') || varname.ends_with(suffix) {
        return None;
    }

    let xgas = varname
        .split('_')
        .next()
        .expect("splitting a variable name on an underscore should yield at least one part");
    // Since we know the starting value is 'x' (an ASCII character), slicing off the first byte
    // should remove it.
    Some(&xgas[1..])
}

fn match_xgas_var_regex<'a>(varname: &'a str, re: &Regex) -> Option<&'a str> {
    let cap = re.captures(varname)?;
    let gas = cap
        .name("gas")
        .expect("Xgas discovery regular expressions must include a named capture group 'gas'");
    Some(gas.as_str())
}

#[derive(Debug, Deserialize)]
pub(crate) struct XgasMatchRule {
    pattern: XgasMatchMethod,
    #[serde(default)]
    pub(crate) prior_profile: Option<XgasAncillary>,
    #[serde(default)]
    pub(crate) prior_xgas: Option<XgasAncillary>,
    #[serde(default)]
    pub(crate) ak: Option<XgasAncillary>,
    #[serde(default)]
    pub(crate) slant_bin: Option<XgasAncillary>,
    #[serde(default)]
    pub(crate) traceability_scale: Option<XgasAncillary>,
}

impl XgasMatchRule {
    pub(crate) fn new(pattern: XgasMatchMethod) -> Self {
        Self {
            pattern,
            prior_profile: None,
            prior_xgas: None,
            ak: None,
            slant_bin: None,
            traceability_scale: None,
        }
    }

    pub(crate) fn is_given_regex(&self, pattern: &str) -> bool {
        match &self.pattern {
            XgasMatchMethod::Suffix(_) => false,
            XgasMatchMethod::Regex(_, p) => p == pattern,
        }
    }

    fn get_gas_opt<'a>(&self, varname: &'a str) -> Option<&'a str> {
        match &self.pattern {
            XgasMatchMethod::Suffix(suffix) => match_xgas_var_suffix(varname, suffix),
            XgasMatchMethod::Regex(regex, _) => match_xgas_var_regex(varname, regex),
        }
    }
}

pub(crate) fn discover_xgas_vars<G: AsRef<str>, V: AsRef<str>>(
    defined_xgas_vars: &[XgasCopy],
    discovery_rules: &[XgasMatchRule],
    excluded_gases: &[G],
    excluded_variables: &[V],
    gas_long_names: &IndexMap<String, String>,
    private_file: &netcdf::File,
) -> error_stack::Result<Vec<XgasCopy>, DiscoveryError> {
    // First, make a set of the private variables already to be copied so that we don't double copy any.
    let copied_varnames: HashSet<String, RandomState> = HashSet::from_iter(
        defined_xgas_vars
            .iter()
            .map(|v| v.xgas_varname().to_string()),
    );

    // Next, extract all of the private variable names, that way we're not reading from the netCDF file
    // a bunch, just in case the netCDF interface doesn't cache that information in memory. We can
    let private_varnames = private_file.variables().map(|var| var.name()).collect_vec();

    let mut xgas_vars = vec![];
    for varname in private_varnames.iter() {
        if let Some((rule, gas)) = should_add_xgas_var(
            discovery_rules,
            varname,
            &copied_varnames,
            excluded_gases,
            excluded_variables,
        ) {
            let long_name = gas_long_names
                .get(gas)
                .map(|name| name.as_str())
                .unwrap_or(gas)
                .to_string();
            let new_xgas = XgasCopy::new_from_discovery(varname, gas, long_name, rule);

            xgas_vars.push(new_xgas);
        }
    }

    Ok(xgas_vars)
}

fn should_add_xgas_var<'a, 'r, G: AsRef<str>, V: AsRef<str>>(
    rules: &'r [XgasMatchRule],
    varname: &'a str,
    copied_varnames: &HashSet<String>,
    excluded_gases: &[G],
    excluded_variables: &[V],
) -> Option<(&'r XgasMatchRule, &'a str)> {
    if copied_varnames.contains(varname) {
        log::debug!("Variable '{varname}' is an explicitly defined Xgas to copy, discovery rules will not apply.");
        return None;
    }

    if excluded_variables.iter().any(|v| v.as_ref() == varname) {
        log::debug!("Variable '{varname}' is explicitly excluded from copying, discovery rules will not apply.");
        return None;
    }

    for rule in rules {
        let gas = if let Some(gas) = rule.get_gas_opt(varname) {
            gas
        } else {
            continue;
        };

        if excluded_gases.iter().any(|ex| ex.as_ref() == gas) {
            log::debug!("Variable '{varname}' will not be included: it matched Xgas discovery rule [{}] but its gas ({gas}) is listed as a gas to exclude", rule.pattern);
            return None;
        } else {
            log::debug!(
                "Variable '{varname}' will be included: it matched Xgas discovery rule [{}]",
                rule.pattern
            );
            return Some((rule, gas));
        }
    }

    None
}
