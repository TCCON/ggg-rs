use std::path::PathBuf;

use chrono::{NaiveDateTime, DateTime, Utc};
use ndarray::Array1;

#[derive(Debug, thiserror::Error)]
pub enum TranscriptionError {
    #[error("Error reading file {}: {cause}", file.display())]
    ReadError{file: PathBuf, cause: String},
    #[error("Error reading file {} at line {line}: {cause}", file.display())]
    ReadErrorAtLine{file: PathBuf, line: usize, cause: String},
    #[error("Error writing variable {variable} to netCDF: {inner}")]
    WriteError{variable: String, inner: netcdf::error::Error}
}

/// A trait representing one source of data to copy to the netCDF file
/// 
/// This trait may be implemented for a struct representing a single file
/// (e.g. the runlog) or representing a class of files (e.g. post-processing
/// files). It is generally expected that such a structure will need to
/// partially parse the input file upon creation, as it must later be able
/// to provide information about netCDF dimensions and variables without
/// possibility of an error. However, it may error while transcribing the
/// data from 
pub trait DataSource {
    fn provided_dimensions(&self) -> &[DimensionWithValues];
    fn required_dimensions(&self) -> &[Dimension];
    fn required_groups(&self) -> &[DataGroup];
    fn variable_names(&self) -> &[String];
    fn write_variables(&mut self, nc_grp: &mut netcdf::GroupMut, group: DataGroup) -> Result<(), TranscriptionError>;
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

#[derive(Debug, thiserror::Error)]
pub enum DimensionError {
    
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dimension {
    Time,
    PriorTime,
    PriorAltitude,
    CellIndex,
    SpectrumNameLength,
    AkAltitude,
    AkSlantXgasBin,
    StringLength(usize)
}

pub enum DimensionWithValues {
    Time(Array1<DateTime<Utc>>, Array1<String>), 
    PriorTime(Array1<NaiveDateTime>),
    PriorAltitude(Array1<f32>),
    CellIndex,
    SpectrumNameLength(usize),
    AkAltitude(Array1<f32>),
    AkSlantXgasBin,
    StringLength(usize)
}

impl DimensionWithValues {
    pub fn len(&self) -> usize {
        match self {
            DimensionWithValues::Time(t, _) => t.len(),
            DimensionWithValues::PriorTime(t) => t.len(),
            DimensionWithValues::PriorAltitude(alt) => alt.len(),
            DimensionWithValues::CellIndex => 2,
            DimensionWithValues::SpectrumNameLength(n) => *n,
            DimensionWithValues::AkAltitude(alt) => alt.len(),
            DimensionWithValues::AkSlantXgasBin => 15,
            DimensionWithValues::StringLength(n) => *n,
        }
    }

    pub fn name(&self) -> String {
        match self {
            DimensionWithValues::Time(_, _) => "time".to_string(),
            DimensionWithValues::PriorTime(_) => "prior_time".to_string(),
            DimensionWithValues::PriorAltitude(_) => "prior_altitude".to_string(),
            DimensionWithValues::CellIndex => "cell_index".to_string(),
            DimensionWithValues::SpectrumNameLength(_) => "specname".to_string(),
            DimensionWithValues::AkAltitude(_) => "ak_altitude".to_string(),
            DimensionWithValues::AkSlantXgasBin => "ak_slant_xgas_bin".to_string(),
            DimensionWithValues::StringLength(n) => format!("a{n}"),
        }
    }
}