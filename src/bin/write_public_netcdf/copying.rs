use std::marker::PhantomData;

use error_stack::ResultExt;
use indexmap::IndexMap;
use itertools::Itertools;
use ndarray::{ArrayD, ArrayView1, ArrayViewD, Axis};
use netcdf::{Extents, NcPutGet};
use num_traits::Zero;

use crate::constants::TIME_DIM_NAME;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CopyError {
    #[error("Tried access index {index} on an array dimension with length {array_len}")]
    BadIndex{index: usize, array_len: usize},
    #[error("Tried to subset a zero-dimensional array")]
    ZeroDArray,
    #[error("Private file is missing the required variable '{0}'")]
    MissingReqVar(String),
    #[error("Dimension '{dimname}' has length {dim_len_in_file} in the public file, but the variable '{varname}' expects it to have length {dim_len_in_var}")]
    DimLenMismatch{dimname: String, varname: String, dim_len_in_file: usize, dim_len_in_var: usize},
    #[error("Not implemented: {0}")]
    NotImplemented(String),
    #[error("An error occurred while {0}")]
    Context(String),
}

impl CopyError {
    fn dim_len_mismatch<D: ToString, V: ToString>(dimname: D, varname: V, len_in_file: usize, len_in_var: usize) -> Self {
        Self::DimLenMismatch {
            dimname: dimname.to_string(),
            varname: varname.to_string(),
            dim_len_in_file: len_in_file,
            dim_len_in_var: len_in_var
        }
    }

    pub(crate) fn not_implemented<S: ToString>(case: S) -> Self {
        Self::NotImplemented(case.to_string())
    }

    pub(crate) fn context<S: ToString>(ctx: S) -> Self {
        Self::Context(ctx.to_string())
    }
}

pub(crate) trait CopySet {
    fn copy(&self, private_file: &netcdf::File, public_file: &mut netcdf::FileMut, time_subsetter: &Subsetter) -> error_stack::Result<(), CopyError>;
    fn other_vars_req(&self) -> Vec<&str> { vec![] }
}

pub(crate) struct Subsetter {
    keep_inds: Vec<usize>,
}

impl Subsetter {
    pub(crate) fn from_flag(flag: ArrayView1<i32>) -> Self {
        let it = flag.iter()
            .enumerate()
            .filter_map(|(i, &f)| {
                if f == 0 {
                    Some(i)
                } else {
                    None
                }
            });
        let keep_inds = Vec::from_iter(it);
        Self { keep_inds }
    }

    pub(crate) fn len(&self) -> usize {
        self.keep_inds.len()
    }

    pub(crate) fn subset_nd_var<T: Copy + Zero>(&self, var: ArrayViewD<T>, along_axis: usize) -> Result<ArrayD<T>, CopyError> {
        let mut shape = Vec::from_iter(var.shape().iter().map(|x| *x));
        if shape.len() == 0 {
            return Err(CopyError::ZeroDArray)
        } else {
            shape[0] = self.len();
        }

        let mut out = ArrayD::zeros(shape);
        for (i_out, &i_in) in self.keep_inds.iter().enumerate() {
            let mut out_slice = out.index_axis_mut(Axis(along_axis), i_out);
            let in_slice = var.index_axis(Axis(along_axis), i_in);
            out_slice.assign(&in_slice);
        }
        Ok(out)

    }
}

#[derive(Debug)]
pub(crate) struct AuxVarCopy<T: Copy + Zero + NcPutGet> {
    /// The variable from the private file to copy.
    pub(crate) private_varname: String,

    /// The name to give the variable in the output file. If `None`, the
    /// variable will have the same name as in the private file.
    pub(crate) public_varname: Option<String>,

    /// Value to use for the long name attribute.
    pub(crate) long_name: String,

    /// Additional attributes to add, or values to replace private file
    /// attributes.
    pub(crate) attr_overrides: IndexMap<String, netcdf::AttributeValue>,

    /// A list of private attributes to remove.
    pub(crate) attr_to_remove: Vec<String>,

    /// Whether this variable is required or can be skipped if
    /// not present in the source file
    pub(crate) required: bool,

    /// Dummy field to mark the desired data type in the output
    data_type: PhantomData<T>
}

impl<T: Copy + Zero + NcPutGet> AuxVarCopy<T> {
    pub(crate) fn new<P: ToString, L: ToString>(private_varname: P, long_name: L, required: bool) -> Self {
        Self {
            private_varname: private_varname.to_string(),
            public_varname: None,
            long_name: long_name.to_string(),
            attr_overrides: IndexMap::new(),
            attr_to_remove: vec!["precision".to_string(), "standard_name".to_string()],
            required,
            data_type: PhantomData
        }
    }

    pub(crate) fn with_public_varname<P: ToString>(mut self, public_varname: P) -> Self {
        self.public_varname = Some(public_varname.to_string());
        self
    }

    pub(crate) fn with_attr_override<N: ToString, V: Into<netcdf::AttributeValue>>(mut self, attr_name: N, attr_value: V) -> Self {
        let attr_name = attr_name.to_string();
        let attr_value = attr_value.into();
        self.attr_overrides.insert(attr_name, attr_value);
        self
    }
}

impl<T: Copy + Zero + NcPutGet> CopySet for AuxVarCopy<T> {
    fn copy(&self, private_file: &netcdf::File, public_file: &mut netcdf::FileMut, time_subsetter: &Subsetter) -> error_stack::Result<(), CopyError> {
        // Will need to create a variable with the same dimensions, then copy the good subset of values
        // and the attributes.
        let private_var = if let Some(var) = private_file.variable(&self.private_varname) {
            var
        } else if self.required {
            return Err(CopyError::MissingReqVar(self.private_varname.clone()).into())
        } else {
            log::info!("Not copying {} as it is not present in the private file", self.private_varname);
            return Ok(())
        };

        let public_varname = self.public_varname
            .as_deref()
            .unwrap_or(&self.private_varname);

        let dims = private_var.dimensions()
            .iter()
            .map(|dim| dim.name())
            .collect_vec();
        let dims_str = dims.iter()
            .map(|dim| dim.as_str())
            .collect_vec();

        // Create the variable, which needs its dimensions created first.
        // Handling missing dimensions here is easier than trying to collect a list of
        // all dimensions that we need.
        add_needed_dims(public_file, &private_var)
            .change_context_lazy(|| CopyError::context(format!("creating public variable '{public_varname}'")))?;
        let mut public_var = public_file.add_variable::<T>(public_varname, &dims_str)
            .change_context_lazy(|| CopyError::context(format!("creating public variable '{public_varname}'")))?;

        // Check that the first dimension is time - if so, we need to subset before we write the data.
        // Eventually, we could allow time to be in a different position, but
        // this is good enough for now.
        let priv_data = private_var.get::<T, _>(Extents::All)
            .change_context_lazy(|| CopyError::context(format!("copying aux var '{}'", self.private_varname)))?;
        let do_subset = private_var.dimensions().get(0).is_some_and(|dim| dim.name() == TIME_DIM_NAME);
        let data = if do_subset {
            time_subsetter.subset_nd_var(priv_data.view(), 0)?
        } else {
            priv_data
        };
        public_var.put(Extents::All, data.view())
            .change_context_lazy(|| CopyError::context(format!("writing data to public variable '{public_varname}'")))?;

        // Finally handle the attributes. Start by inserting the attributes we have specified, then copy any attributes not excluded
        // or overridden
        public_var.put_attribute("long_name", self.long_name.as_str())
            .change_context_lazy(|| CopyError::context(format!("adding 'long_name' attribute to public variable '{public_varname}'")))?;
        for (att_name, att_value) in self.attr_overrides.iter() {
            public_var.put_attribute(&att_name, att_value.to_owned())
                .change_context_lazy(|| CopyError::context(format!("adding '{att_name}' attribute to public variable '{public_varname}'")))?;
        }
        for att in private_var.attributes() {
            let att_name = att.name();
            if att_name != "long_name" && !self.attr_overrides.contains_key(att_name) && !self.attr_to_remove.iter().any(|a| a == att_name) {
                let att_value = att.value()
                    .change_context_lazy(|| CopyError::context(format!("getting original value of attribute '{att_name}' from private variable '{}'", self.private_varname)))?;
                public_var.put_attribute(att_name, att_value)
                    .change_context_lazy(|| CopyError::context(format!("adding '{att_name}' to public variable '{public_varname}'")))?;
            }
        }
        Ok(())
    }
}

pub(crate) struct XgasCopy<T: Copy> {
    xgas: String,
    prior_profile: XgasAncillary,
    prior_xgas: XgasAncillary,
    ak: XgasAncillary,
    traceability_scale: XgasAncillary,
    data_type: PhantomData<T>,
}

impl<T: Copy> CopySet for XgasCopy<T> {
    fn copy(&self, private_file: &netcdf::File, public_file: &mut netcdf::FileMut, time_subsetter: &Subsetter) -> error_stack::Result<(), CopyError> {
        // Copy the xgas and its error, get the WMO scale and make it an attribute, copy the prior profile,
        // prior Xgas, and averaging kernels.
        todo!()
    }

    fn other_vars_req(&self) -> Vec<&str> {
        let mut vars = vec![];
        if let XgasAncillary::Shared(v) = &self.prior_profile {
            vars.push(v.as_str());
        }
        if let XgasAncillary::Shared(v) = &self.prior_xgas {
            vars.push(v.as_str());
        }
        if let XgasAncillary::Shared(v) = &self.ak {
            vars.push(v.as_str());
        }
        vars
    }
}

pub(crate) enum XgasAncillary {
    /// Infer which private variable to copy from the Xgas variable name
    Inferred,
    /// Copy the specified private variable (the `XgasCopy` instance will assign the correct public name)
    Specified(String),
    /// Assume that another Xgas will provide the necessary variable
    Shared(String),
    /// Do not create this ancillary variable
    Omit,
}

fn add_needed_dims(public_file: &mut netcdf::FileMut, private_var: &netcdf::Variable) -> error_stack::Result<(), CopyError> {
    for var_dim in private_var.dimensions() {
        if !check_dim_exists(var_dim, public_file, &private_var.name())? {
            public_file.add_dimension(&var_dim.name(), var_dim.len())
            .change_context_lazy(|| CopyError::context(format!("creating dimension '{}'", var_dim.name())))?;
        }
    }
    Ok(())
}

fn check_dim_exists(var_dim: &netcdf::Dimension, public_file: &netcdf::File, varname: &str) -> Result<bool, CopyError> {
    if var_dim.name() == "time" {
        // Special case: time shrinks because we select flag == 0 data, so it
        // will be written at the beginning of the run
        return Ok(true)
    }

    for extant_dim in public_file.dimensions() {
        if extant_dim.name() == var_dim.name() {
            if extant_dim.len() != var_dim.len() {
                return Err(CopyError::dim_len_mismatch(var_dim.name(), varname, extant_dim.len(), var_dim.len()))
            } else {
                return Ok(true)
            }
        }
    }
    Ok(false)
}