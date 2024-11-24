use std::{borrow::Cow, cell::RefCell, collections::HashMap, fmt::Display, path::Path, sync::{Arc, Mutex}};

use ndarray::{Array, ArrayD};
use netcdf::{AttributeValue, GroupMut, NcPutGet};
use crate::errors::{VarError, WriteError};

/// The general trait representing a source of data (usually a GGG output file)
/// 
/// Types implementing this must be [`Send`] so that loading data can be parallelized.
/// This likely means that the netCDF dataset handle will need to be stored in an
/// `Arc<Mutex<RefCell<>>>` to ensure each provider can get exclusive access to the
/// dataset handle while actually writing.
/// 
/// These types must also implement [`Display`], and should do so by printing a simple
/// description of what type of file this provider represents (e.g. "runlog"), not a
/// long path to said file. This will be used in error messages to indicate that conditions
/// for a provider to work correctly were not met.
pub(crate) trait DataProvider: Display + Send {
    /// If this file defines the length for any dimension (e.g. the runlog defines
    /// the length of the "time" dimension by the number of unique observations), then
    /// it must return a list of dimension names and their required lengths. These will
    /// be gotten before any variables are written, and if two providers give different
    /// lengths for the same dimension, the writer will throw an error.
    /// 
    /// If a dimension should have an associated variable, then the provider must
    /// write that variable in its `write_data_to_nc` method.
    fn dimension_lengths(&self) -> Cow<[(&'static str, usize)]>;

    /// This must list the dimensions that the this provider requires before writing its
    /// variables. If the lengths for any are not given by one of the providers, the
    /// writer will throw an error.
    fn dimensions_required(&self) -> Cow<[&'static str]>;

    /// Write all the data for this source to the netCDF file.
    /// 
    /// Generally, this function should load the data itself, then use `writer` to actually
    /// write it. Loading in this function can allow multiple providers to operate in parallel
    /// to load and only block each other when they need to write to the netCDF file. Using
    /// a [`GroupWriter`] to write the variable instead of directly accessing the netCDF file
    /// allows `writer` to handle putting variables in the correct groups.
    /// 
    /// Providers that write along the "time" dimension must ensure that they use `spec_indexer`
    /// to put their data at the right index for its spectrum.
    fn write_data_to_nc(&self, spec_indexer: &SpectrumIndexer, writer: &dyn GroupWriter) -> error_stack::Result<(), WriteError>;
}

/// A type that maps spectrum names to indices along the "time" dimension.
pub(crate) struct SpectrumIndexer {
    spectrum_indices: HashMap<String, usize>
}

impl SpectrumIndexer {
    /// Create a new indexer from a hash map of spectrum names to time indices.
    /// 
    /// For multi-detector runlogs, all spectra taken simultaneously should have the same
    /// index. It is expected that any values produced from different detector's spectra for
    /// the same index will have different variable names (i.e., if both spectra all retrieving
    /// "xco2", then one will have a prefix to indicate which detector it came from).
    pub(crate) fn new(spectrum_indices: HashMap<String, usize>) -> Self {
        Self { spectrum_indices }
    }

    /// Return the "time" index (0-based) for a given spectrum name, or `None` if the
    /// spectrum was listed.
    pub(crate) fn get_index_for_spectrum(&self, spectrum: &str) -> Option<usize> {
        self.spectrum_indices.get(spectrum).map(|i| *i)
    }
}

/// A trait representing a list of possible groups for variables
pub(crate) trait VarGroup {
    /// Return the name to use for the netCDF group when writing a hierarchical file.
    /// Use "/" to indicate that a variable should go in the root group.
    fn group_name(&self) -> &str;

    /// Return the suffix to append to the variable name when writing a flat file.
    /// Note that this is appended with no character in between, so if you want e.g.
    /// and underscore to come between the name and suffix, include it at the start of
    /// the suffix. Alternatively, if you want no suffix, simple return an empty string.
    fn suffix(&self) -> &str;
}

/// The allowed variable groups for standard TCCON and EM27/SUN data.
#[derive(Debug, Clone, Copy)]
pub(crate) enum StdDataGroup {
    /// Standard, well-validated variables retrieved from near-IR InGaAs spectra
    InGaAs,
    /// New, less well-validated variables also retrieved from near-IR InGaAs spectra
    InGaAsExperimental,
    /// Variables retrieved from visible Si spectra.
    Si,
    /// Variables retrieved from mid-IR InSb spectra.
    InSb,
}

impl VarGroup for StdDataGroup {
    fn group_name(&self) -> &'static str {
        match self {
            Self::InGaAs => "/",
            Self::InGaAsExperimental => "INGAAS_EXPERIMENTAL",
            Self::Si => "SI_EXPERIMENTAL",
            Self::InSb => "INSB_EXPERIMENTAL",
        }
    }

    fn suffix(&self) -> &'static str {
        match self {
            Self::InGaAs => "",
            Self::InGaAsExperimental => "",
            Self::Si => "_si",
            Self::InSb => "_insb",
        }
    }
}

/// A trait representing a generic variable to be written to the netCDF file.
/// This was necessary to allow the `GroupWriter` trait to be object safe by
/// taking a dynamic trait object of this type instead of having the data type
/// be a generic parameter. We will pretty much always use instances of
/// [`ConcreteVarToBe`] to write variables.
pub(crate) trait VarToBe {
    /// Given the group to write to, this function must create the variable
    /// (with the given suffix on its name), write the data, and write any attributes.
    fn write(&self, ncgrp: &mut GroupMut, var_suffix: &str) -> netcdf::Result<()>;
}

/// A structure holding the data to be written to a netCDF variable.
/// 
/// Because of lifetime limitations, [`GroupWriter`]s cannot return a variable
/// from a group if they have to get the group out of the file within their functions.
/// To get around this, [`GroupWriter`] methods taken instances of this struct and 
/// write to the variable directly in their functions.
pub(crate) struct ConcreteVarToBe<T: NcPutGet> {
    name: String,
    dimensions: Vec<&'static str>,
    data: ArrayD<T>,
    long_name: String,
    units: String,
    source_file_name: String,
    source_file_sha256: String,
    extra_attrs: Vec<(String, AttributeValue)>
}

impl<T: NcPutGet> ConcreteVarToBe<T> {
    /// Create a new `ConcreteVarToBe`, computing the source file checksum automatically.
    /// 
    /// If you are creating multiple variables from the same source file, it will be
    /// more efficient to compute the SHA256 checksum once yourself (with the
    /// [`ggg_rs::utils::file_sha256_hexdigest`] function) and use the
    /// [`VarToBe::new_with_checksum`] constructor instead. Otherwise the checksum
    /// will be computed each time this function is called.
    /// 
    /// # Parameters
    /// - `name`: the desired variable name.
    /// - `dimensions`: the dimension names for this variable; they must give the
    ///   correct shape to match `data`.
    /// - `data`: the N-dimensional array containing the data to write.
    /// - `long_name`: a human-readable name for this variable, created as an attribute
    /// - `units`: the units that `data` are in
    /// - `source_file`: path to the original GGG output file that this data came from.
    /// 
    /// To include additional attributes beyond "long_name", "units", "source_file_name",
    /// and "source_file_path" (with the last two determined from `source_file`), use the
    /// [`VarToBe::add_attribute`] method.
    /// 
    /// # Errors
    /// Returns an error if:
    /// - `source_file` does not exist,
    /// - `source_file` does not have a base name
    /// - reading the contents of `source_file` to calculate its checksum fails.
    pub(crate) fn new<N: ToString, L: ToString, U: ToString, D: ndarray::Dimension>(
        name: N,
        dimensions: Vec<&'static str>,
        data: Array<T, D>,
        long_name: L,
        units: U,
        source_file: &Path
    ) -> Result<Self, VarError> {
        if !source_file.exists() {
            return Err(VarError::SourceFileMissing { name: name.to_string(), path: source_file.to_path_buf() });
        }

        let source_file_name = source_file.file_name()
            .ok_or_else(|| VarError::SourceFileError { 
                name: name.to_string(),
                path: source_file.to_path_buf(),
                problem: "could not get file base name".to_string()
            })?.to_string_lossy().to_string();
        let source_file_sha256 = ggg_rs::utils::file_sha256_hexdigest(source_file)
            .map_err(|e| VarError::SourceFileError {
                name: name.to_string(),
                path: source_file.to_path_buf(),
                problem: format!("error occurred calculating checksum ({e})")
            })?;
        Ok(Self {
            name: name.to_string(),
            dimensions,
            data: data.into_dyn(),
            long_name: long_name.to_string(),
            units: units.to_string(),
            source_file_name,
            source_file_sha256,
            extra_attrs: vec![]
        })
    }

    /// An alternate constructor that is more efficient if you have already calculated the checksum
    /// for the source file.
    /// 
    /// All parameters are the same as `new`, except `source_file_name` (now the base name of the
    /// original GGG source file) and `source_file_sha256` (the SHA256 checksum of the source file
    /// as a hex string).
    pub(crate) fn new_with_checksum<N: ToString, L: ToString, U: ToString>(
        name: N,
        dimensions: Vec<&'static str>,
        data: ArrayD<T>,
        long_name: L,
        units: U,
        source_file_name: String,
        source_file_sha256: String
    ) -> Self {
        Self {
            name: name.to_string(),
            dimensions,
            data,
            long_name: long_name.to_string(),
            units: units.to_string(),
            source_file_name,
            source_file_sha256,
            extra_attrs: vec![]
        }
    }

    /// Add an additional attribute to the variable to be.
    /// 
    /// `attname` will be the attribute name and `attvalue` its value. Note that "long_name", "units",
    /// "source_file_name", and "source_file_sha256" attributes will always be added.
    pub(crate) fn add_attribute<N: ToString, V: Into<AttributeValue>>(&mut self, attname: N, attvalue: V) {
        let attname = attname.to_string();
        let attvalue = attvalue.into();
        self.extra_attrs.push((attname, attvalue));
    }
}

impl<T: NcPutGet> VarToBe for ConcreteVarToBe<T> {
    fn write(&self, ncgrp: &mut GroupMut, var_suffix: &str) -> netcdf::Result<()> {
        let full_name = format!("{}{var_suffix}", self.name);
        let mut ncvar = ncgrp.add_variable::<T>(&full_name, &self.dimensions)?;
        ncvar.put(netcdf::Extents::All, self.data.view())?;
        ncvar.put_attribute("long_name", self.long_name.as_str())?;
        ncvar.put_attribute("units", self.units.as_str())?;
        ncvar.put_attribute("source_file_name", self.source_file_name.as_str())?;
        ncvar.put_attribute("source_file_sha256", self.source_file_sha256.as_str())?;
        for (attname, attvalue) in self.extra_attrs.iter() {
            ncvar.put_attribute(&attname, attvalue.to_owned())?;
        }
        Ok(())
    }
}

/// An interface to the underlying netCDF file.
/// 
/// GGG netCDF files can either be written "flat" (with all variables in the root group,
/// possibly with a suffix to distinguish secondary detector or experimental variables)
/// or "hierarchical" (with secondary detector or experimental variables in child groups).
/// This type will handle writing variables to the appropriate location and with the
/// appropriate name depending on which option is selected.
/// 
/// Any implementors will need to have internal state that keeps track of which file format
/// was requested. [`DataGroup`] provides methods to get the correct suffix or group name
/// for data in that group; see the its documentation for details.
pub(crate) trait GroupWriter: Send + Sync {
    /// Get the length of the given dimension, return `None` if it is not found.
    fn get_dim_length(&self, dimname: &str) -> Option<usize>;

    /// Write a single variable to the netCDF file
    fn write_variable(&self, variable: &dyn VarToBe, group: &dyn VarGroup) -> Result<(), WriteError>;

    /// Write a list of variables to the netCDF file.
    /// 
    /// Implementors should ensure that these variables will be written together in the netCDF file
    /// even if different data providers are running in parallel and calling this.
    fn write_many_variables(&self, variables: &[&dyn VarToBe], group: &dyn VarGroup) -> Result<(), WriteError> {
        for &variable in variables {
            self.write_variable(variable, group)?;
        }
        Ok(())
    }
}


/// An implementation of [`GroupWriter`] for TCCON and EM27/SUN data.
#[derive(Debug, Clone)]
pub(crate) struct StdGroupWriter {
    nc_dset: Arc<Mutex<RefCell<netcdf::FileMut>>>,
    dim_lengths: HashMap<String, usize>,
    use_groups: bool
}

impl StdGroupWriter {
    pub(crate) fn new(nc_dset: netcdf::FileMut, use_groups: bool) -> Self {
        let dim_iter = nc_dset.dimensions()
            .map(|dim| (dim.name(), dim.len()));
        let dim_lengths = HashMap::from_iter(dim_iter);
        let nc_dset = Arc::new(Mutex::new(RefCell::new(nc_dset)));
        Self { nc_dset, dim_lengths, use_groups }
    }
}

impl GroupWriter for StdGroupWriter {
    fn get_dim_length(&self, dimname: &str) -> Option<usize> {
        self.dim_lengths.get(dimname).map(|s| *s)
    }

    fn write_variable(&self, variable: &dyn VarToBe, group: &dyn VarGroup) -> Result<(), WriteError> {
        let nc_lock = self.nc_dset.lock()
            .expect("NetCDF mutex was poisoned");
        let mut nc_dset = nc_lock.borrow_mut();
        Self::write_variable_inner(&mut nc_dset, variable, group, self.use_groups)
    }
    
    /// Write multiple variables to the netCDF file sequentially.
    /// 
    /// This version of the method ensures that all the variables given are written
    /// one after the other, with no opportunity for other data providers to intersperse
    /// their own variables, so prefer this function if you want to keep variables from
    /// the same source grouped together in the netCDF file.
    fn write_many_variables(&self, variables: &[&dyn VarToBe], group: &dyn VarGroup) -> Result<(), WriteError> {
        let nc_lock = self.nc_dset.lock()
            .expect("NetCDF mutex was poisoned");
        let mut nc_dset = nc_lock.borrow_mut();
        for &variable in variables {
            Self::write_variable_inner(&mut nc_dset, variable, group, self.use_groups)?;
        }
        Ok(())
    }

    
}

impl StdGroupWriter {
    fn write_variable_inner(nc_dset: &mut netcdf::FileMut, variable: &dyn VarToBe, group: &dyn VarGroup, use_groups: bool) -> Result<(), WriteError> {
        if use_groups {
            let grp_name = group.group_name();
            let mut grp = if grp_name == "/" {
                nc_dset.root_mut().expect("Should be able to access the root group")
            } else if nc_dset.group(grp_name)?.is_some() {
                nc_dset.group_mut(grp_name)?.unwrap()
            } else {
                nc_dset.add_group(grp_name)?
            };

            variable.write(&mut grp, "")?;
        } else {
            let suffix = group.suffix();
            let mut grp = nc_dset.root_mut().expect("Should be able to access the root group");
            variable.write(&mut grp, suffix)?;
        };

        Ok(())
    }
}