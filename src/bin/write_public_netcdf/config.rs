use indexmap::IndexMap;
use serde::Deserialize;

use crate::{copying::XgasAncillary, AuxVarCopy};

pub(crate) static DEFAULT_GAS_LONG_NAMES: &'static[(&'static str, &'static str)] = &[
    ("co2", "carbon dioxide"),
    ("ch4", "methane"),
    ("n2o", "nitrous oxide"),
    ("co", "carbon monoxide"),
    ("h2o", "water"),
    ("hdo", "semiheavy water"),
    ("hf", "hydrofluoric acid"),
    ("o3", "ozone")
];

#[derive(Debug, Deserialize)]
pub(crate) struct Config {
    #[serde(default = "default_aux_vars")]
    pub(crate) aux: Vec<AuxVarCopy>,

    /// A mapping of gas abbreviations (e.g., "co2") to their proper names
    /// (e.g., "carbon dioxide"). When reading from a file, several gases
    /// will have names set by default (those defined by [`DEFAULT_GAS_LONG_NAMES`]).
    /// You can override the values (e.g., specify `h2o = "dihydrogen monoxide"` if
    /// you want to use the chemical name instead of just "water"). Alternatively,
    /// if you do not want the defaults at all, include `no_defaults = ""`. The value
    /// does not matter, the prescence of the `no_defaults` key is all that is needed
    /// to disable the default long names.
    #[serde(deserialize_with = "de_gas_long_names")]
    pub(crate) gas_long_names: IndexMap<String, String>
}

pub(crate) fn default_true() -> bool {
    true
}

pub(crate) fn default_attr_remove() -> Vec<String> {
    vec![
        "precision".to_string(),
        "standard_name".to_string()
    ]
}

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

fn de_gas_long_names<'de, D>(deserializer: D) -> Result<IndexMap<String, String>, D::Error>
where D: serde::Deserializer<'de>
{
    let mut cfg_names = IndexMap::<String, String>::deserialize(deserializer)?;

    // Should we include the defaults?
    let no_defaults = cfg_names.get("no_defaults").is_some();
    if no_defaults {
        return Ok(cfg_names);
    }

    
    for (gas, name) in DEFAULT_GAS_LONG_NAMES {
        let gas = gas.to_string();
        if !cfg_names.contains_key(&gas) {
            cfg_names.insert(gas, name.to_string());
        }
    }
    Ok(cfg_names)
}