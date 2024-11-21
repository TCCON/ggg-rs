use std::{collections::{HashMap, HashSet}, fmt::Display, path::{Path, PathBuf}, str::FromStr};

use chrono::{DateTime, Utc};
use error_stack::ResultExt;
use ggg_rs::{cit_spectrum_name::{CitSpectrumName, NoDetectorSpecName}, output_files::{open_and_iter_postproc_file, PostprocType}, runlogs::{FallibleRunlog, RunlogDataRec}, tccon::input_config::TcconWindowPrefixes, utils::parse_window_name};
use itertools::Itertools;
use log::warn;
use ndarray::Array1;
use netcdf::Extents;

use crate::{attributes::{self, FixedVar}, error::SetupError, interface::{DataGroup, TranscriptionError}};
use crate::dimensions::{Dimension, DimensionWithValues};

#[derive(Debug, Clone)]
pub enum DataSourceType {
    Runlog,
    PostprocFile(PostprocType),
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
    fn write_variables(&mut self, nc_grp: &mut netcdf::GroupMut, group: &DataGroup) -> error_stack::Result<(), TranscriptionError>;
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
            .map_err(|e| TranscriptionError::WriteError { variable: "year".to_string(), inner: e })?;
        var.put_values(&self.year, Extents::All)
            .map_err(|e| TranscriptionError::WriteError { variable: "year".to_string(), inner: e })?;
        attributes::year_attrs().write_attrs(&mut var)
            .map_err(|e| TranscriptionError::WriteError { variable: "year".to_string(), inner: e })?;

        let mut var = grp.add_variable::<i32>(&FixedVar::Day.to_string(), &[&Dimension::Time.to_string()])
            .map_err(|e| TranscriptionError::WriteError { variable: "day".to_string(), inner: e })?;
        var.put_values(&self.day, Extents::All)
            .map_err(|e| TranscriptionError::WriteError { variable: "day".to_string(), inner: e })?;
        attributes::day_attrs().write_attrs(&mut var)
        .map_err(|e| TranscriptionError::WriteError { variable: "day".to_string(), inner: e })?;

        for key in self.order.iter() {
            let values = self.others.get(key).expect("All variables in `order` should have a corresponding entry in `others`");
            let mut var = grp.add_variable::<f32>(&key.to_string(), &[&Dimension::Time.to_string()])
                .map_err(|e| TranscriptionError::WriteError { variable: key.to_string(), inner: e })?;
            var.put_values(&values, Extents::All)
                .map_err(|e| TranscriptionError::WriteError { variable: key.to_string(), inner: e })?;
            key.write_attrs(&mut var)
                .map_err(|e| TranscriptionError::WriteError { variable: key.to_string(), inner: e })?;
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

    pub fn get_spec_index(&self, spectrum: &str) -> Option<usize> {
        let ndspec = NoDetectorSpecName::new(spectrum).ok()?;
        self.spectrum_to_index.get(&ndspec).map(|i| *i)
    }

    fn read_dims_and_vars(runlog: &Path) -> Result<(Array1<DateTime<Utc>>, Array1<String>, HashMap<NoDetectorSpecName, usize>, RunlogData, usize), TranscriptionError> {
        let runlog_handle = FallibleRunlog::open(runlog)
            .map_err(|e| TranscriptionError::ReadError { file: runlog.to_path_buf(), cause: e.to_string() })?;

        let mut last_spec = None;
        let mut last_time = None;
        let mut times = vec![];
        let mut spectra = vec![];
        let mut time_index_mapping = HashMap::new();
        let mut spec_check: HashSet<NoDetectorSpecName> = HashSet::new();
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
            
            time_index_mapping.insert(spectrum.clone(), curr_time_index);
            if is_new_obs && spec_check.contains(&spectrum) {
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

    fn write_variables(&mut self, nc_grp: &mut netcdf::GroupMut, group: &crate::interface::DataGroup) -> error_stack::Result<(), crate::interface::TranscriptionError> {
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
    /// Path to the file read
    path: PathBuf,

    /// Which type of post-processing file this is.
    postproc_type: PostprocType,

    /// The list of unique groups required in the netCDF file 
    data_groups: Vec<DataGroup>,

    /// A map from the names of the variables in the postprocessing file
    /// to the index in `output_var_names` contain the corresponding netCDF
    /// variable name
    variable_map: HashMap<String, usize>,

    /// A map containing the actual data for each variable in the postprocessing
    /// file, indexed by the same keys as `variable_map`.
    variable_data: HashMap<String, Array1<f32>>,

    /// A map from the names of the variables in the postprocessing file
    /// to the index in `data_groups` containing the group for which this
    /// variable will go.
    group_map: HashMap<String, usize>,

    /// The list of netCDF variable names.
    output_var_names: Vec<String>
}

impl PostprocFile {
    pub(crate) fn new(path: PathBuf, runlog: &TcconRunlog) -> error_stack::Result<Self, TranscriptionError> {
        let mut this = Self::init_self(path)?;
        this.load_data(runlog)?;
        Ok(this)
    }

    fn init_self(path: PathBuf) -> error_stack::Result<Self, TranscriptionError> {
        let postproc_type = PostprocType::from_path(&path).ok_or_else(|| {
            TranscriptionError::Custom(format!("Could not determine file type of post processing file {}", path.display()))
        })?;
        
        // Use the iterator convenience function to get the first row
        // of the data - if it doesn't exist, that's an error because
        // there really should be data in these files.
        let (header, mut it) = open_and_iter_postproc_file(&path)
            .change_context_lazy(|| TranscriptionError::ReadError { 
                file: path.clone(), 
                cause: "error while opening the file".into()
            })?;
        let nrow = header.nrec;

        let row = it.next().ok_or_else(|| {
            TranscriptionError::ReadError {
                file: path.clone(),
                cause: "no data rows - post processing files are expected to have at least one data row".into()
            }
        })?.change_context_lazy(|| TranscriptionError::Custom(
            format!("Error reading first data row of post processing file {}", path.display())
        ))?;

        // We need to check if there are any InGaAs or InSb gases. We'll need the file that collate_tccon_results
        // uses. TODO: allow specifying a different prefix file to be consistent with collate_tccon_results.
        let detector_config = TcconWindowPrefixes::new_standard_opt()
            .change_context_lazy(|| TranscriptionError::Custom("Error getting the standard prefix definition file".to_string()))?
            .ok_or_else(|| TranscriptionError::Custom("Standard secondary detector prefix file not found".to_string()))?;

        let mut variable_map = HashMap::new();
        let mut group_map = HashMap::new();
        let mut data_groups = vec![];
        let mut output_var_names = vec![];
        for column_name in row.retrieved.keys() {
            let entry = detector_config.get_entry(&column_name)
                .map_err(|e| TranscriptionError::Custom(e.to_string()))?;

            let this_data_group = entry.nc_group.as_deref().map(|s| DataGroup::from_str(&s).unwrap())
                .unwrap_or_default();
            let idx = if !data_groups.contains(&this_data_group) {
                data_groups.push(this_data_group);
                data_groups.len() - 1
            } else {
                data_groups.iter().position(|el| el == &this_data_group)
                .expect("data_groups should already contain the data group for this variable")
            };

            group_map.insert(column_name.to_string(), idx);
            let nc_varname = Self::map_var_name(column_name, &postproc_type);
            output_var_names.push(nc_varname);
            variable_map.insert(column_name.to_string(), output_var_names.len()-1);
        }


        let variable_data = HashMap::from_iter(
            variable_map.keys().map(|k| (k.to_string(), Array1::from_elem((nrow,), f32::MAX)))
        );
        
        Ok(Self { path, postproc_type, data_groups, variable_map, variable_data, group_map, output_var_names })
    }

    fn load_data(&mut self, runlog: &TcconRunlog) -> error_stack::Result<(), TranscriptionError> {
        let (_, it) = open_and_iter_postproc_file(&self.path)
            .change_context_lazy(|| TranscriptionError::ReadError { 
                file: self.path.to_path_buf(), 
                cause: "error while opening the file".into()
            })?;

        for row in it {
            let row = row.change_context_lazy(|| TranscriptionError::ReadError { 
                file: self.path.to_path_buf(), cause: "error reading data row".to_string()
            })?;

            let idx = runlog.get_spec_index(&row.auxiliary.spectrum)
                .ok_or_else(|| TranscriptionError::Custom(
                    format!("Spectrum '{}' in {} was not present in the runlog", row.auxiliary.spectrum, self.path.display())
                ))?;

            for (key, value) in row.retrieved.iter() {
                let arr = self.variable_data.get_mut(key)
                    .expect("All data variables must be pre-populated by init_self");
                arr[idx] = *value as f32;
            }
        }

        Ok(())
    }
    
    fn map_var_name(column_name: &str, postproc_type: &PostprocType) -> String {
        match postproc_type {
            PostprocType::Vsw => format!("vsw_{column_name}"),
            PostprocType::Tsw => format!("tsw_{column_name}"),
            PostprocType::Vav => format!("column_{column_name}"),
            PostprocType::Tav => format!("vsf_{column_name}"),
            PostprocType::VswAda => format!("vsw_ada_{column_name}"),
            PostprocType::VavAda => format!("ada_{column_name}"),
            PostprocType::VavAdaAia => format!("{column_name}"),
            PostprocType::Other(s) => todo!("{s}_{column_name}"),
        }
    }

    fn make_long_name(&self, orig_varname: &str) -> String {
        let err_str = if orig_varname.contains("error") {
            "_error"
        } else {
            ""
        };

        match &self.postproc_type {
            PostprocType::Vsw => format!("{orig_varname}_column_density{err_str}"),
            PostprocType::Tsw => format!("{orig_varname}_vmr_scale_factor{err_str}"),
            PostprocType::Vav => format!("{orig_varname}_column_density{err_str}"),
            PostprocType::Tav => format!("{orig_varname}_vmr_scale_factor{err_str}"),
            PostprocType::VswAda => format!("{orig_varname}_column_average_mole_fraction{err_str}"),
            PostprocType::VavAda => format!("{orig_varname}_column_average_mole_fraction{err_str}"),
            PostprocType::VavAdaAia => format!("{orig_varname}_column_average_mole_fraction{err_str}"),
            PostprocType::Other(s) => format!("{s}_{orig_varname}{err_str}"),
        }
    }

    fn make_description(&self, orig_varname: &str) -> String {
        let err_str = if orig_varname.contains("error") {
            " error"
        } else {
            ""
        };

        match &self.postproc_type {
            PostprocType::Vsw => {
                let (gas, center) = parse_window_name(orig_varname).expect(".vsw variable name should be of form GAS_CENTER");
                format!("{gas} total column density{err_str} from the window centered at {center:.0} cm-1")
            },
            PostprocType::Tsw => {
                let (gas, center) = parse_window_name(orig_varname).expect(".vsw variable name should be of form GAS_CENTER");
                format!("{gas} VMR scale factor{err_str} from the window centered at {center:.0} cm-1")
            },
            PostprocType::Vav => format!("{orig_varname} total column density{err_str}"),
            PostprocType::Tav => format!("{orig_varname} VMR scale factor{err_str}"),
            PostprocType::VswAda => {
                let (gas, center) = parse_window_name(orig_varname).expect(".vsw variable name should be of form GAS_CENTER");
                format!("{gas} column average mole fraction{err_str} from the window centered at {center:.0} cm-1, after airmass dependence is removed but before tying to the WMO scale")
            },
            PostprocType::VavAda => format!("{orig_varname} column average mole fraction{err_str}, after airmass dependence is removed but before tying to the WMO scale"),
            PostprocType::VavAdaAia => format!("{orig_varname} column average mole fraction{err_str}, with airmass dependence removed and the tie to the WMO scale applied"),
            PostprocType::Other(s) => format!("{s} {orig_varname}"),
        }
    }

    fn aux_to_write(&self) -> &[&str] {
        if let PostprocType::VavAdaAia = self.postproc_type {
            &["zmin"]
        } else {
            &[]
        }
    }
}

impl Display for PostprocFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let filename = if let Some(name) = self.path.file_name() {
            name.to_string_lossy()
        } else {
            self.path.to_string_lossy()
        };

        write!(f, "{} ({filename})", self.postproc_type)
    }
}

impl DataSource for PostprocFile {
    fn source_type(&self) -> DataSourceType {
        DataSourceType::PostprocFile(self.postproc_type.clone())
    }

    fn file(&self) -> &Path {
        &self.path
    }

    fn provided_dimensions(&self) -> &[DimensionWithValues] {
        &[]
    }

    fn required_dimensions(&self) -> &[Dimension] {
        &[Dimension::Time]
    }

    fn required_groups(&self) -> &[DataGroup] {
        &self.data_groups
    }

    fn variable_names(&self) -> &[String] {
        &self.output_var_names
    }

    fn write_variables(&mut self, nc_grp: &mut netcdf::GroupMut, group: &DataGroup) -> error_stack::Result<(), TranscriptionError> {
        // Get the variables for just this group in the order they were listed in the output vector - that
        // gives us control to group variables together in ncdump output.
        let mut vars_to_write = self.group_map.iter()
            .filter_map(|(name, grpidx)| {
                let grp = &self.data_groups[*grpidx];
                if grp == group {
                    Some(name.as_str())
                } else {
                    None
                }
            }).collect_vec();
        vars_to_write.sort_unstable_by_key(|name| self.variable_map.get(*name).expect("All variables must be in the variable_map"));

        for var in vars_to_write {
            let nameidx = *self.variable_map.get(var).expect("All variables must be defined in the variable_map");
            let outname = &self.output_var_names[nameidx];
            let values = self.variable_data.get(var).expect("All variables must be present in the variable_data map");
            // TODO: these errors should have context indicating what action failed
            let mut new_var = nc_grp.add_variable::<f32>(&outname, &[Dimension::Time.to_string().as_str()])
                .map_err(|e| TranscriptionError::WriteError { variable: outname.to_string(), inner: e })?;
            new_var.put(netcdf::Extents::All, values.view())
                .map_err(|e| TranscriptionError::WriteError { variable: outname.to_string(), inner: e })?;
            new_var.put_attribute("units", "mol mol-1")
                .map_err(|e| TranscriptionError::WriteError { variable: outname.to_string(), inner: e })?;
            new_var.put_attribute("long_name", self.make_long_name(var))
                .map_err(|e| TranscriptionError::WriteError { variable: outname.to_string(), inner: e })?;
            new_var.put_attribute("description", self.make_description(var))
                .map_err(|e| TranscriptionError::WriteError { variable: outname.to_string(), inner: e })?;
        }
        todo!()
    }
}