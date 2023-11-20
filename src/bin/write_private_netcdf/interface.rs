use std::path::PathBuf;

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
    WriteError{variable: String, inner: netcdf::error::Error}
}


pub enum GroupLocation {
    Root,
    Subgroup(String)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataGroup {
    InGaAs,
    InGaAsExperimental,
    Si,
    InSb,
}

impl DataGroup {
    /// Return the location in the file this group goes in a netCDF4 hierarchical file
    pub fn name(&self) -> GroupLocation {
        match self {
            Self::InGaAs => GroupLocation::Root,
            Self::InGaAsExperimental => GroupLocation::Subgroup("ingaas_experimental".to_string()),
            Self::Si => GroupLocation::Subgroup("si_experimental".to_string()),
            Self::InSb => GroupLocation::Subgroup("insb_experimental".to_string()),
        }
    }

    /// Return the suffix to append to variables in this group in a flat netCDF3-64bit or netCDF4 file
    pub fn suffix(&self) -> Option<&'static str> {
        // TODO: some of this might need to move into a "netcdf utility" library so they can be shared with
        // the public writer.
        match self {
            Self::InGaAs => None,
            Self::InGaAsExperimental => Some("_experimental"),
            Self::Si => Some("_si"),
            Self::InSb => Some("_insb"),
        }
    }
}
