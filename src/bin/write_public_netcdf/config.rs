use std::collections::HashMap;

use regex::Regex;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct PublicNcConfig {
    pub(crate) variables: Vec<VariableDef>,
    #[serde(default)]
    pub(crate) mappings: HashMap<String, HashMap<String, String>>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ConfigErrorType {
    #[error("invalid regex: {0}")]
    Regex(#[from] regex::Error),
    #[error("map key '{0}' is not defined")]
    Map(String),
}

#[derive(Debug, Deserialize)]
pub(crate) enum TransferMethod {
    Copy
}

impl Default for TransferMethod {
    fn default() -> Self {
        Self::Copy
    }
}

#[derive(Debug, Deserialize)]
pub(crate) enum VariableDef {
    Single(SingleVariable),
    Group(VariableGroup)
}

/// A mapping from a single private variable to a single public variable
#[derive(Debug, Deserialize)]
pub(crate) struct SingleVariable {
    /// The name of the variable in the private files
    pub(crate) private_name: String,

    /// The name to give the variable in the public files
    pub(crate) public_name: String,

    /// The CF-compliant standard name attribute value. This must
    /// be given, as the private files do not include such an attribute.
    pub(crate) standard_name: String,

    /// How to create the public variables. Default is to copy them.
    #[serde(default)]
    pub(crate) transfer: TransferMethod,

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

    /// Additional attributes to add to this variable.
    #[serde(default)]
    pub(crate) extra_attributes: HashMap<String, String>
}


#[derive(Debug, Deserialize)]
pub(crate) struct VariableGroup {

}
