use std::{path::{PathBuf, Path}, str::FromStr};

use chrono::{DateTime, Utc};
use ggg_rs::{runlogs::FallibleRunlog, cit_spectrum_name::CitSpectrumName};
use log::warn;
use ndarray::Array1;

use crate::interface::{DataSource, Dimension, DataGroup, TranscriptionError, DimensionWithValues};

pub struct TcconRunlog {
    runlog: PathBuf,
    variables: Vec<String>,
    dimensions: Vec<DimensionWithValues>,
}

impl TcconRunlog {
    pub fn new(runlog: PathBuf) -> Result<Self, TranscriptionError> {
        let (times, master_spectra) = Self::read_dims(&runlog)?;
        let time_dim = DimensionWithValues::Time(times, master_spectra);

        let variables = vec![]; // TODO: define the variables we want from the runlog
        Ok(Self { runlog, variables, dimensions: vec![time_dim] })
    }

    fn read_dims(runlog: &Path) -> Result<(Array1<DateTime<Utc>>, Array1<String>), TranscriptionError> {
        let runlog_handle = FallibleRunlog::open(runlog)
            .map_err(|e| TranscriptionError::ReadError { file: runlog.to_path_buf(), cause: e.to_string() })?;

        let mut last_master_spec = None;
        let mut last_master_time = None;
        let mut master_detector = None;
        let mut times = vec![];
        let mut spectra = vec![];
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

        Ok((times, spectra))
    }
}

impl DataSource for TcconRunlog {
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