use std::{collections::HashSet, fmt::Display, hash::RandomState, str::FromStr};

use indexmap::IndexMap;
use itertools::Itertools;
use regex::Regex;
use serde::Deserialize;

use crate::XgasCopy;

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
#[serde(untagged, rename_all = "snake_case")]
enum XgasMatchDeser {
    Suffix {
        suffix: String,
        new_suffix: Option<String>,
    },
    Regex {
        regex: String,
        rep_pattern: Option<String>,
    },
}

impl TryFrom<XgasMatchDeser> for XgasMatchMethod {
    type Error = DiscoveryError;

    fn try_from(value: XgasMatchDeser) -> Result<Self, Self::Error> {
        match value {
            XgasMatchDeser::Suffix { suffix, new_suffix } => {
                Ok(Self::suffix_from_string(suffix, new_suffix))
            }
            XgasMatchDeser::Regex {
                regex: pattern,
                rep_pattern,
            } => Self::regex_from_string(pattern, rep_pattern),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(try_from = "XgasMatchDeser")]
pub(crate) enum XgasMatchMethod {
    Suffix {
        re: Regex,
        suf: String,
        new_suf: Option<String>,
    },
    Regex {
        re: Regex,
        pat: String,
        rep_pat: Option<String>,
    },
}

impl PartialEq for XgasMatchMethod {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Suffix {
                    re: _,
                    suf: l_suf,
                    new_suf: l_new_suf,
                },
                Self::Suffix {
                    re: _,
                    suf: r_suf,
                    new_suf: r_new_suf,
                },
            ) => l_suf == r_suf && l_new_suf == r_new_suf,
            (
                Self::Regex {
                    re: _,
                    pat: l_pat,
                    rep_pat: l_rep,
                },
                Self::Regex {
                    re: _,
                    pat: r_pat,
                    rep_pat: r_rep,
                },
            ) => l_pat == r_pat && l_rep == r_rep,
            _ => false,
        }
    }
}

impl XgasMatchMethod {
    pub(crate) fn suffix_from_string(suffix: String, new_suffix: Option<String>) -> Self {
        let suffix = regex::escape(&suffix);
        // Apologies to future me for this regex but:
        //  - (?<base> ... ) is needed to capture the whole variable name except the suffix in case
        //    we have to do a substitution
        //  - x(?<gas>[a-z][a-z0-9]*) will match e.g. xco2, xch4, etc.
        //  - we do not allow for any interveneing parts to avoid confusion with intermediate variables.
        let pattern = format!(r"^(?<base>x(?<gas>[a-z][a-z0-9]*))_{suffix}$");
        let re = Regex::from_str(&pattern).expect(
            "Xgas discovery suffix failed to compile into a regular expression (this is a bug)",
        );
        Self::Suffix {
            re,
            suf: suffix,
            new_suf: new_suffix,
        }
    }

    pub(crate) fn regex_from_string(
        pattern: String,
        replace_pattern: Option<String>,
    ) -> Result<Self, DiscoveryError> {
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
        Ok(Self::Regex {
            re,
            pat: pattern.to_string(),
            rep_pat: replace_pattern,
        })
    }

    pub(crate) fn infer_xgas_public(&self, xgas_private: &str) -> Option<String> {
        match self {
            XgasMatchMethod::Suffix {
                re,
                suf: _,
                new_suf,
            } => Self::infer_xgas_public_suffix(xgas_private, re, new_suf.as_deref()),

            XgasMatchMethod::Regex {
                re,
                pat: _,
                rep_pat,
            } => Self::infer_xgas_public_regex(xgas_private, re, rep_pat.as_deref()),
        }
    }

    fn infer_xgas_public_suffix(
        xgas: &str,
        re: &Regex,
        new_suffix: Option<&str>,
    ) -> Option<String> {
        if let Some(new_suf) = new_suffix {
            let pattern = format!("${{base}}_{}", regex::escape(new_suf));
            let xgas_public = re.replace_all(xgas, &pattern).to_string();
            Some(xgas_public)
        } else {
            None
        }
    }

    fn infer_xgas_public_regex(
        xgas: &str,
        re: &Regex,
        replace_pattern: Option<&str>,
    ) -> Option<String> {
        if let Some(pat) = replace_pattern {
            Some(re.replace_all(xgas, pat).to_string())
        } else {
            None
        }
    }
}

impl Display for XgasMatchMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            XgasMatchMethod::Suffix {
                re: _,
                suf,
                new_suf: _,
            } => write!(f, "suffix '{suf}'"),
            XgasMatchMethod::Regex {
                re: _,
                pat,
                rep_pat: _,
            } => write!(f, "regex '{pat}'"),
        }
    }
}

fn match_xgas_var_regex<'a>(varname: &'a str, re: &Regex) -> Option<&'a str> {
    let cap = re.captures(varname)?;
    let gas = cap
        .name("gas")
        .expect("Xgas discovery regular expressions must include a named capture group 'gas'");
    Some(gas.as_str())
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone, Copy)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AncillaryDiscoveryMethod {
    Inferred,
    InferredIfFirst,
    OptInferredIfFirst,
    Omit,
}

#[derive(Debug, Deserialize, PartialEq)]
pub(crate) struct XgasMatchRule {
    #[serde(flatten)]
    pattern: XgasMatchMethod,
    #[serde(default)]
    pub(crate) xgas_error: Option<AncillaryDiscoveryMethod>,
    #[serde(default)]
    pub(crate) prior_profile: Option<AncillaryDiscoveryMethod>,
    #[serde(default)]
    pub(crate) prior_xgas: Option<AncillaryDiscoveryMethod>,
    #[serde(default)]
    pub(crate) ak: Option<AncillaryDiscoveryMethod>,
    #[serde(default)]
    pub(crate) slant_bin: Option<AncillaryDiscoveryMethod>,
    #[serde(default)]
    pub(crate) traceability_scale: Option<AncillaryDiscoveryMethod>,
}

impl XgasMatchRule {
    pub(crate) fn new(pattern: XgasMatchMethod) -> Self {
        Self {
            pattern,
            xgas_error: None,
            prior_profile: None,
            prior_xgas: None,
            ak: None,
            slant_bin: None,
            traceability_scale: None,
        }
    }

    pub(crate) fn is_given_regex(&self, pattern: &str) -> bool {
        match &self.pattern {
            XgasMatchMethod::Suffix {
                re: _,
                suf: _,
                new_suf: _,
            } => false,
            XgasMatchMethod::Regex {
                re: _,
                pat,
                rep_pat: _,
            } => pat == pattern,
        }
    }

    fn get_gas_opt<'a>(&self, varname: &'a str) -> Option<&'a str> {
        match &self.pattern {
            XgasMatchMethod::Suffix {
                re,
                suf: _,
                new_suf: _,
            } => match_xgas_var_regex(varname, re),
            XgasMatchMethod::Regex {
                re,
                pat: _,
                rep_pat: _,
            } => match_xgas_var_regex(varname, re),
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
            let xgas_public = rule.pattern.infer_xgas_public(&varname);
            let long_name = gas_long_names
                .get(gas)
                .map(|name| name.as_str())
                .unwrap_or(gas)
                .to_string();
            let new_xgas = XgasCopy::new_from_discovery(varname, xgas_public, gas, long_name, rule);

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

#[cfg(test)]
mod tests {
    use crate::discovery::{AncillaryDiscoveryMethod, XgasMatchMethod};

    use super::XgasMatchRule;

    #[test]
    fn test_de_simple_rules() {
        let toml_str = r#"suffix = "insb""#;
        let rule: XgasMatchRule =
            toml::from_str(toml_str).expect("deserialization should not fail");
        if let XgasMatchMethod::Suffix {
            re: _,
            suf,
            new_suf: _,
        } = rule.pattern
        {
            assert_eq!(suf, "insb")
        } else {
            assert!(false, "wrong type of match method")
        }

        let toml_str = r#"regex = 'column_average_(?<gas>\w+)'"#;
        let rule: XgasMatchRule =
            toml::from_str(toml_str).expect("deserialization should not fail");
        if let XgasMatchMethod::Regex {
            re: _,
            pat,
            rep_pat: _,
        } = rule.pattern
        {
            assert_eq!(pat, r"column_average_(?<gas>\w+)")
        } else {
            assert!(false, "wrong type of match method")
        }
    }

    #[test]
    fn test_de_with_ancillary() {
        let toml_str = r#"suffix = "mir"
        ak = { type = "omit" }
        traceability_scale = { type = "omit" }"#;
        let rule: XgasMatchRule =
            toml::from_str(toml_str).expect("deserialization should not fail");
        let expected = XgasMatchRule {
            pattern: XgasMatchMethod::suffix_from_string("mir".to_string(), None),
            xgas_error: None,
            prior_profile: None,
            prior_xgas: None,
            ak: Some(AncillaryDiscoveryMethod::Omit),
            slant_bin: None,
            traceability_scale: Some(AncillaryDiscoveryMethod::Omit),
        };
        assert_eq!(rule, expected);
    }
}
