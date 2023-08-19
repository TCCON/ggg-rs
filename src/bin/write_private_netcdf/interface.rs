use std::path::PathBuf;

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
    fn provided_dimensions(&self) -> &[Dimension];
    fn required_dimensions(&self) -> &[&str];
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
    #[error("Source file provided the wrong type for dimension '{dimname}', expected {expected}, got {got}")]
    WrongType{dimname: String, expected: DimensionType, got: DimensionType},
}

pub struct Dimension {
    /// The name to give the dimension and its corresponding variable
    name: String,

    /// The array of values that make up this dimension
    values: DimensionValues,

    /// The list of attributes to write to this dimension's variable
    attributes: Vec<(String, netcdf::AttrValue)>,

    /// Whether this dimension should be written at the start of the netCDF
    /// file (if it is needed) or wait until the first time it is required
    write_at_start: bool,

    /// Whether this dimension has been written to the netCDF file yet
    written: bool,
}

impl Dimension {
    pub fn new(name: String, values: DimensionValues, attributes: Vec<(String, netcdf::AttrValue)>, write_at_start: bool) -> Self {
        Self { name, values, attributes, write_at_start, written: false }
    }

    pub fn new_time(name: String, values: DimensionValues, write_at_start: bool) -> Self {
        if values.is_datetime() {
            let attributes = vec![
                ("units".to_string(), netcdf::AttrValue::Str("seconds since 1970-01-01 00:00:00".to_string())),
                ("calendar".to_string(), netcdf::AttrValue::Str("gregorian".to_string()))
            ];

            Self { 
                name,
                values,
                attributes,
                write_at_start,
                written: false
            }
        } else {
            panic!("Cannot call Dimension::new_time with values not of variant DimensionValue::DateTime")
        }
    }
    /// Returns the indices to copy from a data source along the dimension
    /// 
    /// This is used to match up data from a source file to a defined dimension in the
    /// netCDF file, e.g. spectrum name. Given the values of the dimension in the source
    /// file, the returned `Vec` contains the indices for that dimension in the order the
    /// should be copied to the netCDF file. An index may be `None`, indicating that no
    /// value in `file_dim_values` matched the value for that position, and writing to
    /// that position should be skipped.
    pub fn indices_to_copy(&self, file_dim_values: &DimensionValues) -> Result<Vec<Option<usize>>, DimensionError> {
        todo!()
    }

    /// Write the dimension's values to the provided netCDF variable
    pub fn write_values(&self, nc_var: &mut netcdf::VariableMut) -> Result<(), DimensionError> {
        todo!()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum DimensionType {
    UInt32,
    UInt64,
    Int32,
    Int64,
    Float32,
    Float64,
    String,
}

pub enum DimensionValues {
    UInt32(ndarray::Array1<u32>),
    UInt64(ndarray::Array1<u64>),
    Int32(ndarray::Array1<i32>),
    Int64(ndarray::Array1<i64>),
    Float32(ndarray::Array1<f32>),
    Float64(ndarray::Array1<f32>),
    String(ndarray::Array1<String>),
    DateTime(ndarray::Array1<chrono::NaiveDateTime>)
}

impl DimensionValues {
    fn is_datetime(&self) -> bool {
        if let Self::DateTime(_) = self {
            true
        } else {
            false
        }
    }
}