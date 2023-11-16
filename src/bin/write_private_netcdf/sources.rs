use std::{path::{PathBuf, Path}, str::FromStr, fmt::Display};

use chrono::{DateTime, Utc};
use ggg_rs::{runlogs::FallibleRunlog, cit_spectrum_name::CitSpectrumName};
use log::warn;
use ndarray::Array1;

use crate::interface::{DataGroup, TranscriptionError};
use crate::dimensions::{Dimension, DimensionWithValues};

#[derive(Debug, Clone, Copy)]
pub enum DataSourceType {
    Runlog,
    ColFile,
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
pub trait DataSource: Display {
    fn source_type(&self) -> DataSourceType;
    fn file(&self) -> &Path;
    fn provided_dimensions(&self) -> &[DimensionWithValues];
    fn required_dimensions(&self) -> &[Dimension];
    fn required_groups(&self) -> &[DataGroup];
    fn variable_names(&self) -> &[String];
    fn write_variables(&mut self, nc_grp: &mut netcdf::GroupMut, group: DataGroup) -> Result<(), TranscriptionError>;
}

pub(crate) struct DataSourceList(Vec<Box<dyn DataSource>>);

impl DataSourceList {
    pub(crate) fn add_source<T: DataSource + 'static>(&mut self, source: T) {
        let boxed = Box::new(source);
        self.0.push(boxed);
    }

    pub(crate) fn get_runlog_path(&self) -> Option<&Path> {
        for source in self.0.iter() {
            if let DataSourceType::Runlog = source.source_type() {
                return Some(source.file())
            }

        }
        None
    }
}

impl Default for DataSourceList {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl DataSourceList {
    pub fn iter(&self) -> std::slice::Iter<'_, Box<dyn DataSource>> {
        self.0.iter()
    }
}

pub struct TcconRunlog {
    runlog: PathBuf,
    variables: Vec<String>,
    dimensions: Vec<DimensionWithValues>,
}

impl TcconRunlog {
    pub fn new(runlog: PathBuf) -> Result<Self, TranscriptionError> {
        let (times, master_spectra, max_specname_len) = Self::read_dims(&runlog)?;
        let time_dim = DimensionWithValues::Time(times, master_spectra);
        let specname_dim = DimensionWithValues::SpectrumNameLength(max_specname_len);

        let variables = vec![]; // TODO: define the variables we want from the runlog
        Ok(Self { runlog, variables, dimensions: vec![time_dim, specname_dim] })
    }

    fn read_dims(runlog: &Path) -> Result<(Array1<DateTime<Utc>>, Array1<String>, usize), TranscriptionError> {
        let runlog_handle = FallibleRunlog::open(runlog)
            .map_err(|e| TranscriptionError::ReadError { file: runlog.to_path_buf(), cause: e.to_string() })?;

        let mut last_master_spec = None;
        let mut last_master_time = None;
        let mut master_detector = None;
        let mut times = vec![];
        let mut spectra = vec![];
        let mut max_specname_length = 0;
        for (line, res) in runlog_handle.into_line_iter() {
            // Handle the case where reading & parsing the next line of the runlog fails
            let rl_rec = match res {
                Ok(r) => r,
                Err(e) => return Err(TranscriptionError::ReadErrorAtLine { file: runlog.to_owned(), line, cause: e.to_string() })
            };

            // We need information about the spectrum and ZPD time - make sure we can get that successfully
            let spectrum = CitSpectrumName::from_str(&rl_rec.spectrum_name)
                .map_err(|e| TranscriptionError::ReadErrorAtLine { 
                    file: runlog.to_path_buf(), line, cause: e.to_string()
                })?;

            let zpd_time = rl_rec.zpd_time()
                .ok_or_else(|| TranscriptionError::ReadErrorAtLine { 
                    file: runlog.to_path_buf(), line, cause: "Invalid ZPD time".to_string()
                })?;

            // For the time dimension, we want to use the "master" spectra, since the secondary spectra
            // should have the same ZPD time as the corresponding master spectrum. (We know this isn't the
            // case, and Opus occasionally writes out incorrect ZPD times for the second detector.) In the
            // output, any data coming from the secondary detector will be slotted into the same time index
            // as the master detector's data.
            if master_detector.is_none() {
                // First time through the loop - take the detector from the first spectrum as the "master"
                master_detector = Some(spectrum.detector());
            }
            
            if master_detector == Some(spectrum.detector()) {
                // This is one of the master spectra, add its data to the dimension arrays
                last_master_spec = Some(rl_rec.spectrum_name.clone());
                last_master_time = Some(zpd_time);

                times.push(zpd_time);
                max_specname_length = max_specname_length.max(rl_rec.spectrum_name.as_bytes().len());
                spectra.push(rl_rec.spectrum_name);
            } else if last_master_time != Some(zpd_time) {
                // This is *not* a master spectrum, but it has a different ZPD time than the last master
                // spectrum. This happens occasionally (due to an Opus bug we think). We should be able to
                // handle it later in the code, but this is a root cause for a lot of problems, so print a
                // warning.
                warn!("Spectrum {} has a different ZPD time than its corresponding master spectrum ({}) in runlog {}.",
                      rl_rec.spectrum_name, last_master_spec.as_deref().unwrap_or("?"), runlog.display());
            }
        }
        
        let times = Array1::from_vec(times);
        let spectra = Array1::from_vec(spectra);

        Ok((times, spectra, max_specname_length))
    }
}

impl DataSource for TcconRunlog {
    fn source_type(&self) -> DataSourceType {
        DataSourceType::Runlog    
    }

    fn file(&self) -> &Path {
        &self.runlog    
    }

    fn provided_dimensions(&self) -> &[DimensionWithValues] {
        &self.dimensions
    }

    fn required_dimensions(&self) -> &[Dimension] {
        &[Dimension::Time, Dimension::SpectrumNameLength]
    }

    fn required_groups(&self) -> &[DataGroup] {
        &[DataGroup::InGaAs]
    }

    fn variable_names(&self) -> &[String] {
        &self.variables
    }

    fn write_variables(&mut self, nc_grp: &mut netcdf::GroupMut, group: crate::interface::DataGroup) -> Result<(), crate::interface::TranscriptionError> {
        todo!()
    }
}

impl Display for TcconRunlog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let filename = if let Some(name) = self.runlog.file_name() {
            name.to_string_lossy()
        } else {
            self.runlog.to_string_lossy()
        };

        write!(f, "runlog ({filename})")
    }
}