use std::{convert::Infallible, path::PathBuf, str::FromStr};

use ggg_rs::{tccon::input_config::PrefixEntry, utils::GggError};

use crate::dimensions::{DimensionWithValues, Dimension};

#[derive(Debug, thiserror::Error)]
pub enum TranscriptionError {
    #[error("Error reading file {}: {cause}", file.display())]
    ReadError{file: PathBuf, cause: String},
    #[error("Error reading file {} at line {line}: {cause}", file.display())]
    ReadErrorAtLine{file: PathBuf, line: usize, cause: String},
    #[error("In file {}: {problem}", file.display())]
    UnexpectedEvent{file: PathBuf, problem: String},
    #[error("Error writing variable {variable} to netCDF: {inner}")]
    WriteError{variable: String, inner: netcdf::Error},
    #[error("{0}")]
    Custom(String),
}


pub enum GroupLocation {
    Root,
    Subgroup(String)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataGroup {
    InGaAs,
    InGaAsExperimental,
    Si,
    InSb,
    Other(String)
}

impl Default for DataGroup {
    fn default() -> Self {
        Self::InGaAs
    }
}

impl DataGroup {
    /// Return the location in the file this group goes in a netCDF4 hierarchical file
    pub fn name(&self) -> GroupLocation {
        match self {
            Self::InGaAs => GroupLocation::Root,
            Self::InGaAsExperimental => GroupLocation::Subgroup("ingaas_experimental".to_string()),
            Self::Si => GroupLocation::Subgroup("si_experimental".to_string()),
            Self::InSb => GroupLocation::Subgroup("insb_experimental".to_string()),
            Self::Other(s) => GroupLocation::Subgroup(format!("{s}_experimental"))
        }
    }

    /// Return the suffix to append to variables in this group in a flat netCDF3-64bit or netCDF4 file
    pub fn suffix(&self) -> Option<String> {
        // TODO: some of this might need to move into a "netcdf utility" library so they can be shared with
        // the public writer.
        match self {
            Self::InGaAs => None,
            Self::InGaAsExperimental => Some("_experimental".to_string()),
            Self::Si => Some("_si".to_string()),
            Self::InSb => Some("_insb".to_string()),
            Self::Other(s) => Some(format!("_{s}"))
        }
    }
}


impl FromStr for DataGroup {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_ref() {
            "ingaas" => Ok(Self::InGaAs),
            "ingaas_experimental" => Ok(Self::InGaAsExperimental),
            "si" => Ok(Self::Si),
            "insb" => Ok(Self::InSb),
            _ => Ok(Self::Other(s.to_string()))
        }
    }
}