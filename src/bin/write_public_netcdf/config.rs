//! Configure which variables are copied from the private files
//! 
//! The configuration file will be a TOML file with at least one section, `variables`.
//! There are several ways to define which variables are copied, as shown in the following
//! example
//! 
//! ```toml
//! [[variables]]
//! private_name = "prior_altitude"
//! public_name = "prior_altitude"
//! 
//! 
//! [[variables]]
//! private_names = ["pout", "tout", "hout"]
//! 
//! [[variables]]
//! private_names = ["prior_1co2", "prior_1ch4", "prior_1co"]
//! public_names = ["prior_co2", "prior_ch4", "prior_co"]
//! 
//! [[variables]]
//! private_name_pattern = ["ak_x[a-z0-9]+"]
//! ```

use std::collections::HashMap;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct PublicNcConfig {
    pub(crate) variables: Vec<VariableDef>,
    #[serde(default)]
    pub(crate) mappings: HashMap<String, HashMap<String, String>>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ConfigErrorType {
    
}

#[derive(Debug, Deserialize)]
pub(crate) enum TransferMethod {
    Copy(String)
}

#[derive(Debug, Deserialize)]
pub(crate) enum VariableDef {
    Single(CopyOneVar),
    Group(CopyManyVars)
}

/// A mapping from a single private variable to a single public variable
#[derive(Debug, Deserialize)]
pub(crate) struct CopyOneVar {
    /// The name of the variable in the private files
    pub(crate) private_name: String,

    /// The name to give the variable in the public files
    pub(crate) public_name: String,

    /// The CF-compliant standard name attribute value. If not given,
    /// will use the "standard_name" attribute 
    pub(crate) standard_name: Option<String>,

    /// Units to assume the private file values are in when converting.
    /// If not given, will use the "units" attribute on the private variable
    /// and will error if that variable does not have a "units" attribute
    #[serde(default)]
    pub(crate) private_units: Option<String>,

    /// Units to output the public file value in. Must be convertible from
    /// the private units. If not given, will use the same units as the private
    /// file.
    #[serde(default)]
    pub(crate) public_units: Option<String>,

    /// Value to use for the long name attribute. If not given, will copy the
    /// value from the private file and will error if that attribute is not
    /// present.
    #[serde(default)]
    pub(crate) long_name: Option<String>,

    /// Additional attributes to copy from the private files. Specifying
    /// any of the attributes specifically called out as fields has no effect.
    #[serde(default)]
    pub(crate) copy_attributes: Vec<String>,

    /// Additional attributes to add to this variable.
    #[serde(default)]
    pub(crate) add_attributes: HashMap<String, String>
}


#[derive(Debug, Deserialize)]
pub(crate) struct CopyManyVars {

}
