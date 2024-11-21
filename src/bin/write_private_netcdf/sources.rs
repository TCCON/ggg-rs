use std::{path::{PathBuf, Path}, str::FromStr, fmt::Display, collections::HashMap};

use chrono::{DateTime, Utc};
use error_stack::ResultExt;
use ggg_rs::{cit_spectrum_name::{CitSpectrumName, NoDetectorSpecName}, runlogs::{FallibleRunlog, RunlogDataRec}};
use log::warn;
use ndarray::Array1;
use netcdf::Extents;

use crate::{attributes::{self, FixedVar}, error::SetupError, interface::{DataGroup, TranscriptionError}};
use crate::dimensions::{Dimension, DimensionWithValues};

#[derive(Debug, Clone, Copy)]
pub enum DataSourceType {
    Runlog,
    PostprocFile(PostprocSourceType),
    ColFile,
}

#[derive(Debug, Clone, Copy)]
pub enum PostprocSourceType {
    VswFile,
    TswFile,
    VavFile,
    TavFile,
    VswAdaFile,
    VavAdaFile,
    VavAdaAiaFile,
}

impl Display for PostprocSourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PostprocSourceType::VswFile => write!(f, ".vsw file"),
            PostprocSourceType::TswFile => write!(f, ".tsw file"),
            PostprocSourceType::VavFile => write!(f, ".vav file"),
            PostprocSourceType::TavFile => write!(f, ".tav file"),
            PostprocSourceType::VswAdaFile => write!(f, ".vsw.ada file"),
            PostprocSourceType::VavAdaFile => write!(f, ".vav.ada file"),
            PostprocSourceType::VavAdaAiaFile => write!(f, ".vav.ada.aia file"),
        }
    }
}

impl TryFrom<&Path> for PostprocSourceType {
    type Error = SetupError;

    fn try_from(value: &Path) -> Result<Self, Self::Error> {
        let name = value.file_name()
            .map(|n| n.to_string_lossy())
            .unwrap_or_else(|| "".into());

        if name.ends_with(".vav.ada.aia") { return Ok(Self::VavAdaAiaFile); }
        if name.ends_with(".vav.ada") { return Ok(Self::VavAdaFile); }
        if name.ends_with(".vsw.ada") { return Ok(Self::VswAdaFile); }
        if name.ends_with(".tav") { return Ok(Self::TavFile); }
        if name.ends_with(".vav") { return Ok(Self::VavFile); }
        if name.ends_with(".tsw") { return Ok(Self::TswFile); }
        if name.ends_with(".vsw") { return Ok(Self::VswFile); }

        Err(SetupError::FileKindError { 
            path: value.to_path_buf(), 
            kind: "post-processing" 
        })
    }
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
    fn write_variables(&mut self, nc_grp: &mut netcdf::GroupMut, group: DataGroup) -> error_stack::Result<(), TranscriptionError>;
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

struct RunlogData {
    year: Vec<i32>,
    day: Vec<i32>,
    others: HashMap<FixedVar, Vec<f32>>,
    order: Vec<FixedVar>,
}

impl RunlogData {
    fn push(&mut self, rec: &RunlogDataRec, curr_index: usize) {
        if self.year.len() + 1 != curr_index {
            panic!("Expected to add element {curr_index} to year, actually adding {}", self.year.len() + 1)
        }
        self.year.push(rec.year);

        if self.day.len() + 1 != curr_index {
            panic!("Expected to add element {curr_index} to year, actually adding {}", self.day.len() + 1)
        }
        self.day.push(rec.day);

        self._push_helper(rec, FixedVar::Hour, rec.hour, curr_index);
        self._push_helper(rec, FixedVar::Lat, rec.obs_lat, curr_index);
        self._push_helper(rec, FixedVar::Lon, rec.obs_lon, curr_index);
        self._push_helper(rec, FixedVar::Zobs, rec.obs_alt, curr_index);
        self._push_helper(rec, FixedVar::Solzen, rec.asza, curr_index);
        self._push_helper(rec, FixedVar::Azim, rec.azim, curr_index);
        self._push_helper(rec, FixedVar::Osds, rec.osds, curr_index);
        self._push_helper(rec, FixedVar::Opd, rec.opd, curr_index);
        self._push_helper(rec, FixedVar::Fovi, rec.fovi, curr_index);
        self._push_helper(rec, FixedVar::Amal, rec.amal, curr_index);
        self._push_helper(rec, FixedVar::Graw, rec.delta_nu, curr_index);
        self._push_helper(rec, FixedVar::Tins, rec.tins, curr_index);
        self._push_helper(rec, FixedVar::Pins, rec.pins, curr_index);
        self._push_helper(rec, FixedVar::Tout, rec.tout, curr_index);
        self._push_helper(rec, FixedVar::Pout, rec.pout, curr_index);
        self._push_helper(rec, FixedVar::Hout, rec.hout, curr_index);
        self._push_helper(rec, FixedVar::Sia, rec.sia, curr_index);
        self._push_helper(rec, FixedVar::Fvsi, rec.fvsi, curr_index);
        self._push_helper(rec, FixedVar::Wspd, rec.wspd, curr_index);
        self._push_helper(rec, FixedVar::Wdir, rec.wdir, curr_index);
    }

    fn _push_helper(&mut self, rec: &RunlogDataRec, key: FixedVar, value: f64, curr_index: usize) {
        let v = if let Some(v) = self.others.get_mut(&key) {
            v
        } else {
            // do this rather than .expect() so we can insert the key in the message
            panic!("{key} not initialized; if this is a new runlog variable, ensure it is added to RunlogData::default()");
        };

        if v.len() + 1 != curr_index {
            panic!("Expected to add element {curr_index} to {key}, actually adding {}", v.len() + 1)
        }

        v.push(value as f32);
    }

    fn write_to_nc(&self, grp: &mut netcdf::GroupMut) -> error_stack::Result<(), TranscriptionError> {
        // TODO: make use of error stack - needs the TranscriptionError to r
        let mut var = grp.add_variable::<i32>(&FixedVar::Year.to_string(), &[&Dimension::Time.to_string()])
            .change_context_lazy(|| TranscriptionError::WriteError { variable: "year".to_string() })?;
        var.put_values(&self.year, Extents::All)
            .change_context_lazy(|| TranscriptionError::WriteError { variable: "year".to_string() })?;
        attributes::year_attrs().write_attrs(&mut var)
            .change_context_lazy(|| TranscriptionError::WriteError { variable: "year".to_string() })?;

        let mut var = grp.add_variable::<i32>(&FixedVar::Day.to_string(), &[&Dimension::Time.to_string()])
            .change_context_lazy(|| TranscriptionError::WriteError { variable: "day".to_string() })?;
        var.put_values(&self.day, Extents::All)
            .change_context_lazy(|| TranscriptionError::WriteError { variable: "day".to_string() })?;
        attributes::day_attrs().write_attrs(&mut var)
        .change_context_lazy(|| TranscriptionError::WriteError { variable: "day".to_string() })?;

        for key in self.order.iter() {
            let values = self.others.get(key).expect("All variables in `order` should have a corresponding entry in `others`");
            let mut var = grp.add_variable::<f32>(&key.to_string(), &[&Dimension::Time.to_string()])
                .change_context_lazy(|| TranscriptionError::WriteError { variable: key.to_string() })?;
            var.put_values(&values, Extents::All)
                .change_context_lazy(|| TranscriptionError::WriteError { variable: key.to_string() })?;
            key.write_attrs(&mut var)
                .change_context_lazy(|| TranscriptionError::WriteError { variable: key.to_string() })?;
        }
        Ok(())
    }
}

impl Default for RunlogData {
    fn default() -> Self {
        let order = vec![FixedVar::Hour, FixedVar::Lat, FixedVar::Lon, FixedVar::Zobs, FixedVar::Solzen,
                                        FixedVar::Azim, FixedVar::Osds, FixedVar::Opd, FixedVar::Fovi, FixedVar::Amal,
                                        FixedVar::Graw, FixedVar::Tins, FixedVar::Pins, FixedVar::Tout, FixedVar::Pout,
                                        FixedVar::Hout, FixedVar::Sia, FixedVar::Fvsi, FixedVar::Wspd, FixedVar::Wdir];
        let others = HashMap::from_iter(
            order.iter().copied().map(|k| (k, vec![]))
        );
        Self { 
            year: Default::default(),
            day: Default::default(),
            others,
            order
        }
    }
}

pub struct TcconRunlog {
    runlog: PathBuf,
    variables: Vec<String>,
    dimensions: Vec<DimensionWithValues>,
    spectrum_to_index: HashMap<NoDetectorSpecName, usize>,
    variable_data: RunlogData,
}

impl TcconRunlog {
    pub fn new(runlog: PathBuf) -> Result<Self, TranscriptionError> {
        let (times, master_spectra, spectrum_to_index, variable_data, max_specname_len) = Self::read_dims_and_vars(&runlog)?;
        let time_dim = DimensionWithValues::Time(times, master_spectra);
        let specname_dim = DimensionWithValues::SpectrumNameLength(max_specname_len);

        let variables = vec![]; // TODO: define the variables we want from the runlog - this should be all the aux variables except zmin
        Ok(Self { runlog, variables, spectrum_to_index, dimensions: vec![time_dim, specname_dim], variable_data })
    }

    fn read_dims_and_vars(runlog: &Path) -> Result<(Array1<DateTime<Utc>>, Array1<String>, HashMap<NoDetectorSpecName, usize>, RunlogData, usize), TranscriptionError> {
        let runlog_handle = FallibleRunlog::open(runlog)
            .map_err(|e| TranscriptionError::ReadError { file: runlog.to_path_buf(), cause: e.to_string() })?;

        let mut last_spec = None;
        let mut last_time = None;
        let mut times = vec![];
        let mut spectra = vec![];
        let mut time_index_mapping = HashMap::new();
        let mut curr_time_index: usize = 0;
        let mut max_specname_length = 0;
        let mut variable_data = RunlogData::default();

        for (line, res) in runlog_handle.into_line_iter() {
            // Handle the case where reading & parsing the next line of the runlog fails
            let rl_rec = match res {
                Ok(r) => r,
                Err(e) => return Err(TranscriptionError::ReadErrorAtLine { file: runlog.to_owned(), line, cause: e.to_string() })
            };

            // We need information about the spectrum and ZPD time - make sure we can get that successfully
            let spectrum = CitSpectrumName::from_str(&rl_rec.spectrum_name)
                .map(NoDetectorSpecName::from)
                .map_err(|e| TranscriptionError::ReadErrorAtLine { 
                    file: runlog.to_path_buf(), line, cause: e.to_string()
                })?;

            let zpd_time = rl_rec.zpd_time()
                .ok_or_else(|| TranscriptionError::ReadErrorAtLine { 
                    file: runlog.to_path_buf(), line, cause: "Invalid ZPD time".to_string()
                })?;

            // For the time dimension, we want to use the ZPD from the first spectrum in the runlog for a
            // given measurement This assumes that a runlog will always have the secondary detector's spectrum
            // immediately follow the primary detector's when both are available. We can check that this is the
            // case because we keep track of the spectra that we find, ignoring the detector. (We know that, 
            // while both should have the same ZPD time, this isn't always the case, and Opus occasionally 
            // writes out incorrect ZPD times for the second detector.) In the output, any data coming from the
            // secondary detector will be slotted into the same time index as the primary detector's data if there
            // is a primary detector.

            let is_new_obs = if let Some(ls) = &last_spec {
                if ls != &spectrum {
                    last_spec = Some(spectrum.to_owned());
                    last_time = Some(zpd_time);
                    true
                } else {
                    false
                }
            } else {
                last_spec = Some(spectrum.to_owned());
                last_time = Some(zpd_time);
                true
            };
            
            if is_new_obs && time_index_mapping.contains_key(&spectrum) {
                // This should mean that a spectrum has the same name (ignoring the detector)
                // as a spectrum we already encountered. This means the runlog is formatted in
                // a way we don't expect. Weird runlog ordering is the root cause for a lot
                // of errors, so reject this runlog.
                return Err(TranscriptionError::UnexpectedEvent { 
                    file: runlog.to_path_buf(), 
                    problem: format!("spectrum {} occurs separately in the runlog from another spectrum that shares the same name (but with potentially a different detector). This is not allowed; all spectra for a given observation must occur together in the runlog.", 
                                     spectrum.0.spectrum())
                })
            } else if is_new_obs {
                max_specname_length = max_specname_length.max(spectrum.0.spectrum().as_bytes().len());
                times.push(zpd_time);
                spectra.push(spectrum.0.spectrum().to_string());
                time_index_mapping.insert(spectrum, curr_time_index);
                variable_data.push(&rl_rec, curr_time_index);
                curr_time_index += 1;
            } else if last_time != Some(zpd_time) {
                // This is *not* the first spectrum for the obs, but it has a different ZPD time than the last
                // spectrum. This happens occasionally (due to an Opus bug we think). We should be able to
                // handle it later in the code, but this is a root cause for a lot of problems, so print a
                // warning.
                warn!("Spectrum {} has a different ZPD time than the first spectrum with the same name (ignoring the detector) ({}) in runlog {}.",
                      rl_rec.spectrum_name, 
                      last_spec.as_ref().map(|s| s.0.spectrum()).unwrap_or("?"), 
                      runlog.display());
            }
        }
        
        let times = Array1::from_vec(times);
        let spectra = Array1::from_vec(spectra);

        Ok((times, spectra, time_index_mapping, variable_data, max_specname_length))
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

    fn write_variables(&mut self, nc_grp: &mut netcdf::GroupMut, group: crate::interface::DataGroup) -> error_stack::Result<(), crate::interface::TranscriptionError> {
        if let crate::interface::DataGroup::InGaAs = group {
            // Only write to the standard group
            self.variable_data.write_to_nc(nc_grp)?;
        }
        Ok(())
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


pub struct PostprocFile {
    file: PathBuf,
    file_type: PostprocSourceType,
    groups_included: Vec<DataGroup>,
    variables: Vec<String>,
}

impl PostprocFile {
    fn aux_to_write(&self) -> &[&str] {
        if let PostprocSourceType::VavAdaAiaFile = self.file_type {
            &["zmin"]
        } else {
            &[]
        }
    }
}

impl Display for PostprocFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let filename = if let Some(name) = self.file.file_name() {
            name.to_string_lossy()
        } else {
            self.file.to_string_lossy()
        };

        write!(f, "{} ({filename})", self.file_type)
    }
}

impl DataSource for PostprocFile {
    fn source_type(&self) -> DataSourceType {
        DataSourceType::PostprocFile(self.file_type)
    }

    fn file(&self) -> &Path {
        &self.file
    }

    fn provided_dimensions(&self) -> &[DimensionWithValues] {
        &[]
    }

    fn required_dimensions(&self) -> &[Dimension] {
        &[Dimension::Time]
    }

    fn required_groups(&self) -> &[DataGroup] {
        &self.groups_included
    }

    fn variable_names(&self) -> &[String] {
        &self.variables
    }

    fn write_variables(&mut self, nc_grp: &mut netcdf::GroupMut, group: DataGroup) -> error_stack::Result<(), TranscriptionError> {
        todo!()
    }
}