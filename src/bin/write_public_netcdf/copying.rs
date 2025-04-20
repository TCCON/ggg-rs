use std::{marker::PhantomData, ops::Mul};

use error_stack::ResultExt;
use ggg_rs::{nc_utils::NcArray, units::dmf_conv_factor};
use indexmap::IndexMap;
use itertools::Itertools;
use ndarray::{ArrayD, ArrayView1, ArrayViewD, Axis};
use netcdf::{AttributeValue, Extents, NcTypeDescriptor};
use num_traits::Zero;

use crate::constants::TIME_DIM_NAME;


/// Represents an error that occurred while copying a variable
/// to the public file.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CopyError {
    /// Indicates that the program tried to access an out-of-bounds element on an
    /// existing array.
    #[error("Tried access index {index} on an array dimension with length {array_len}")]
    BadIndex{index: usize, array_len: usize},

    /// Indicates that the input private file is missing a variable that was
    /// expected to be present.
    #[error("Private file is missing the required variable '{0}'")]
    MissingReqVar(String),

    /// Indicates that the input private file is missing an attribute (either
    /// on a group or variable) that was expected to be present.
    #[error("Private file is missing the required attribute '{attr}' under '{parent}'")]
    MissingReqAttr{parent: String, attr: String},

    /// Indicates that a dimension shared among multiple variables has a different
    /// expected length for one variable than it was defined with.
    #[error("Dimension '{dimname}' has length {dim_len_in_file} in the public file, but the variable '{varname}' expects it to have length {dim_len_in_var}")]
    DimLenMismatch{dimname: String, varname: String, dim_len_in_file: usize, dim_len_in_var: usize},

    /// This is a wrapper error used to provide more context to an underlying error.    
    #[error("An error occurred while {0}")]
    Context(String),
}

impl CopyError {
    fn missing_req_attr<P: ToString, A: ToString>(parent: P, attr: A) -> Self {
        Self::MissingReqAttr { parent: parent.to_string(), attr: attr.to_string() }
    }

    fn dim_len_mismatch<D: ToString, V: ToString>(dimname: D, varname: V, len_in_file: usize, len_in_var: usize) -> Self {
        Self::DimLenMismatch {
            dimname: dimname.to_string(),
            varname: varname.to_string(),
            dim_len_in_file: len_in_file,
            dim_len_in_var: len_in_var
        }
    }

    pub(crate) fn context<S: ToString>(ctx: S) -> Self {
        Self::Context(ctx.to_string())
    }
}

pub(crate) trait CopySet {
    fn copy(&self, private_file: &netcdf::File, public_file: &mut netcdf::FileMut, time_subsetter: &Subsetter) -> error_stack::Result<(), CopyError>;
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

    pub(crate) fn subset_nd_array<T: Copy + Zero>(&self, arr: ArrayViewD<T>, along_axis: usize) -> Result<ArrayD<T>, CopyError> {
        let mut shape = Vec::from_iter(arr.shape().iter().map(|x| *x));
        if shape.len() == 0 {
            // If we somehow got a 0-D array, then there is nothing to subset - 
            // return it as-is
            return Ok(arr.to_owned())
        } else {
            shape[0] = self.len();
        }

        let mut out = ArrayD::zeros(shape);
        for (i_out, &i_in) in self.keep_inds.iter().enumerate() {
            let mut out_slice = out.index_axis_mut(Axis(along_axis), i_out);
            let in_slice = arr.index_axis(Axis(along_axis), i_in);
            out_slice.assign(&in_slice);
        }
        Ok(out)

    }

    pub(crate) fn subset_generic_array(&self, arr: &NcArray, along_axis: usize) -> Result<NcArray, CopyError> {
        match arr {
            NcArray::I8(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::I8(arr))
            },
            NcArray::I16(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::I16(arr))
            },
            NcArray::I32(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::I32(arr))
            },
            NcArray::I64(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::I64(arr))
            },
            NcArray::U8(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::U8(arr))
            },
            NcArray::U16(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::U16(arr))
            },
            NcArray::U32(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::U32(arr))
            },
            NcArray::U64(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::U64(arr))
            },
            NcArray::F32(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::F32(arr))
            },
            NcArray::F64(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::F64(arr))
            },
            NcArray::Char(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::U8(arr))
            },
        }
    }
}

#[derive(Debug)]
pub(crate) struct AuxVarCopy {
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

}

impl AuxVarCopy {
    pub(crate) fn new<P: ToString, L: ToString>(private_varname: P, long_name: L, required: bool) -> Self {
        Self {
            private_varname: private_varname.to_string(),
            public_varname: None,
            long_name: long_name.to_string(),
            attr_overrides: IndexMap::new(),
            attr_to_remove: vec!["precision".to_string(), "standard_name".to_string()],
            required,
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

impl CopySet for AuxVarCopy {
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

        copy_variable_general(
            public_file,
            &private_var,
            public_varname,
            time_subsetter,
            &self.long_name,
            &self.attr_overrides,
            &self.attr_to_remove,
        )
    }
}

pub(crate) struct XgasCopy<T: Copy + Zero + NcTypeDescriptor> {
    xgas: String,
    gas: String,
    gas_long: String,
    prior_profile: XgasAncillary,
    prior_xgas: XgasAncillary,
    ak: XgasAncillary,
    traceability_scale: XgasAncillary,
    data_type: PhantomData<T>,
}

impl<T: Copy + Zero + NcTypeDescriptor> XgasCopy<T> {
    /// Create a set of variables to copy for an Xgas with the ancillary/supporting variables
    /// configured as follows: the prior profile, prior Xgas, and AK will be copied unless
    /// they were copied by an earlier Xgas set, while the traceability scale must not collide
    /// with any other variables. All these variable names will be inferred.
    /// 
    /// The inputs are:
    /// 
    /// - `xgas:` the variable name in the private file
    /// - `gas:` the gas name that will be used to find the prior profile, prior Xgas, and AK variables.
    /// Normally this will just be `xgas` without the leading "x", but you must specify this in case the
    /// `xgas` variable has a suffix (e.g., for a secondary detector) or otherwise is not simply "x" + gas.
    /// (For example, the CO2 variables specifying X2007 or X2019 WMO scales.)
    /// 
    /// Note that the type of the Xgas and associated variables must be defined with the generic parameter, `T`:
    /// 
    /// ```
    /// # use crate::copying::XgasCopy;
    /// 
    /// let xgas = XgasCopy::<f32>::new("xch4", "ch4", "methane");
    /// ```
    /// 
    /// This should be a float type in all normal use cases. `f32` is normally sufficiently precise.
    pub(crate) fn new<X: ToString, GS: ToString, GL: ToString>(xgas: X, gas: GS, gas_long: GL) -> Self {
        Self {
            xgas: xgas.to_string(),
            gas: gas.to_string(),
            gas_long: gas_long.to_string(),
            prior_profile: XgasAncillary::InferredIfFirst,
            prior_xgas: XgasAncillary::InferredIfFirst,
            ak: XgasAncillary::InferredIfFirst,
            traceability_scale: XgasAncillary::Inferred,
            data_type: PhantomData
        }
    }

    fn xgas_error_name(&self) -> String {
        let mut parts = self.xgas.split('_').collect_vec();
        parts.insert(1, "error");
        parts.join("_")
    }

    fn infer_prior_xgas_names(&self) -> (String, String) {
        // these should be the same in the standard case
        let private_name = format!("prior_{}", self.xgas);
        let public_name = private_name.clone();
        (private_name, public_name)
    }

    fn infer_prior_prof_names(&self) -> (String, String) {
        let gas = &self.gas;
        let private_name = format!("prior_1{gas}");
        let public_name = format!("prior_{gas}");
        (private_name, public_name)
    }

    fn infer_ak_names(&self) -> (String, String) {
        // these should be the same in the standard case
        let private_name = format!("ak_{}", self.xgas);
        let public_name = private_name.clone();
        (private_name, public_name)
    }
}

impl<T: Copy + Zero + NcTypeDescriptor + Mul<Output = T> + From<f32>> CopySet for XgasCopy<T> {
    fn copy(&self, private_file: &netcdf::File, public_file: &mut netcdf::FileMut, time_subsetter: &Subsetter) -> error_stack::Result<(), CopyError> {
        // Copy the xgas and its error, get the WMO scale and make it an attribute, copy the prior profile,
        // prior Xgas, and averaging kernels.

        // TODO: find the AICF variable and extract the WMO scale if present.

        // Grab the units from the Xgas variable - we will need them to ensure that the
        // prior profile and prior Xgas are in the same units

        let xgas_var = private_file.variable(&self.xgas)
            .ok_or_else(|| CopyError::MissingReqVar(self.xgas.clone()))?;
        let gas_units = get_string_attr(&xgas_var, "units")
            .change_context_lazy(|| CopyError::context(format!("getting the {} units", self.xgas)))?;

        // Now copy the Xgas itself
        copy_vmr_variable_from_dset::<T, &str>(
            private_file,
            public_file,
            &self.xgas,
            &self.xgas,
            time_subsetter,
            &format!("column average {} mole fraction", self.gas_long),
            IndexMap::new(),
            &[],
        &gas_units
        )?;

        // And its error value
        let error_name = self.xgas_error_name();
        copy_vmr_variable_from_dset::<T, &str>(
            private_file,
            public_file,
            &error_name,
            &error_name,
            time_subsetter,
            &format!("column average {} mole fraction error", self.gas_long),
            IndexMap::new(),
            &[],
            &gas_units
        )?;

        // And the prior Xgas value
        let opt = self.prior_xgas.get_var_names_opt(public_file, || self.infer_prior_xgas_names());
        if let Some((private_prxgas_name, public_prxgas_name)) = opt {
            copy_vmr_variable_from_dset::<T, &str>(
                private_file,
                public_file,
                &private_prxgas_name,
                &public_prxgas_name,
                time_subsetter,
                &format!("a priori {} column average", self.gas_long),
                IndexMap::new(),
                &[],
                &gas_units
            )?;
        }


        // Now the a priori profiles. They will not be expanded,
        // that must now be done in the private files.
        let opt = self.prior_profile.get_var_names_opt(&public_file, || self.infer_prior_prof_names());
        if let Some((private_prior_name, public_prior_name)) = opt {
            copy_vmr_variable_from_dset::<T, &str>(
                private_file,
                public_file,
                &private_prior_name,
                &public_prior_name,
                time_subsetter,
                &format!("a priori {} profile", self.gas_long),
                IndexMap::new(),
                &[],
                &gas_units
            )?;
        }

        // Likewise for the AKs
        let opt = self.ak.get_var_names_opt(&public_file, || self.infer_ak_names());
        if let Some((private_ak_name, public_ak_name)) = opt {
            let ak_var = private_file.variable(&private_ak_name)
                .ok_or_else(|| CopyError::MissingReqVar(private_ak_name))?;
            copy_variable_general::<&str>(
                public_file,
                &ak_var,
                &public_ak_name,
                time_subsetter,
                &format!("{} averaging kernel", self.gas_long),
                &IndexMap::new(),
                &[]
            )?;
        }

        Ok(())
    }
}

pub(crate) enum XgasAncillary {
    /// Infer which private variable to copy from the Xgas variable name
    Inferred,
    /// Infer which private variable to copy from the Xgas variable name,
    /// but do not copy if that variable as already been copied to the
    /// public file.
    InferredIfFirst,
    /// Copy the specified private variable (the `XgasCopy` instance will assign the correct public name)
    Specified{private_name: String, public_name: Option<String>},
    /// Assume that another Xgas will provide the necessary variable
    CopyIfFirst{private_name: String, public_name: Option<String>},
    /// Do not create this ancillary variable
    Omit,
}

impl XgasAncillary {
    /// Get the private and public variable names.
    /// 
    /// `infer_names_fxn` must be a closure that takes no arguments and returns
    /// the private and public names as inferred from the Xgas variable that
    /// this ancillary variable supports.
    fn get_var_names<F>(&self, infer_names_fxn: F) -> (String, String)
    where F: FnOnce() -> (String, String) {
        match self {
            XgasAncillary::Inferred => infer_names_fxn(),
            XgasAncillary::InferredIfFirst => infer_names_fxn(),
            XgasAncillary::Specified { private_name, public_name } => {
                let public_name = public_name
                    .as_deref()
                    .unwrap_or(&private_name)
                    .to_owned();
                (private_name.to_owned(), public_name)
            },
            XgasAncillary::CopyIfFirst { private_name, public_name } => {
                let public_name = public_name
                    .as_deref()
                    .unwrap_or(&private_name)
                    .to_owned();
                (private_name.to_owned(), public_name)
            },
            XgasAncillary::Omit => infer_names_fxn(),
        }
    }

    /// Combines `get_var_names` and `do_copy`: if the variables should not be copied,
    /// returns `None`, otherwise returns `Some((private_name, public_name))`.
    /// Note that unlike `get_var_names`, the `infer_names_fxn` closure must be
    /// able to be called repeatedly due to an implementation detail.
    fn get_var_names_opt<F>(&self, public_file: &netcdf::File, infer_names_fxn: F) -> Option<(String, String)> 
    where F: Fn() -> (String, String) {
        if !self.do_copy(public_file, || infer_names_fxn()) {
            None
        } else {
            Some(self.get_var_names(infer_names_fxn))
        }
    }

    /// Should the private variable be copied?
    /// This checks if the variable should always be copied,
    /// never be copied, or if that depends on whether it was
    /// previously copied, 
    fn do_copy<F>(&self, public_file: &netcdf::File, infer_names_fxn: F) -> bool
    where F: Fn() -> (String, String) {
        match self {
            XgasAncillary::Inferred => true,
            XgasAncillary::InferredIfFirst => {
                let (_, public_name) = infer_names_fxn();
                public_file.variable(&public_name).is_none()
            }
            XgasAncillary::Specified { private_name: _, public_name: _ } => true,
            XgasAncillary::CopyIfFirst { private_name, public_name } => {
                let public_name = public_name
                    .as_deref()
                    .unwrap_or(&private_name);
                public_file.variable(public_name).is_none()
            },
            XgasAncillary::Omit => false,
        }
    }
}

// ---------------- //
// HELPER FUNCTIONS //
// ---------------- //

/// Check if the dimensions named in `private_var` exist in `public_file`,
/// if not, create them. Note that they are created with the same length,
/// so if you need a different length (e.g., like "time" does because of
/// subsetting), best if you create those dimensions before copying any variables.
fn add_needed_dims(public_file: &mut netcdf::FileMut, private_var: &netcdf::Variable) -> error_stack::Result<(), CopyError> {
    for var_dim in private_var.dimensions() {
        if !check_dim_exists(var_dim, public_file, &private_var.name())? {
            public_file.add_dimension(&var_dim.name(), var_dim.len())
            .change_context_lazy(|| CopyError::context(format!("creating dimension '{}'", var_dim.name())))?;
        }
    }
    Ok(())
}

/// Return `true` if `var_dim` exists in `public_file`, `false` otherwise.
/// Also checks that the lengths are equal for variables that already exist.
/// `varname` is only used in an error message for clarity.
/// 
/// Note: "time" is assumed to always exist, since it is subset in the public files.
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

/// Helper function that copies a variable with mole fraction data.
/// This ensures that the units match `target_unit`, which should
/// normally be the unit that the Xgas values are in.
fn copy_vmr_variable_from_dset<T: Copy + Zero + NcTypeDescriptor + Mul<Output = T> + From<f32>, S: AsRef<str>>(
    private_file: &netcdf::File,
    public_file: &mut netcdf::FileMut,
    private_varname: &str,
    public_varname: &str,
    time_subsetter: &Subsetter,
    long_name: &str,
    mut attr_overrides: IndexMap<String, AttributeValue>,
    attr_to_remove: &[S],
    target_unit: &str,
) -> error_stack::Result<(), CopyError> {
    let private_var = private_file.variable(private_varname)
        .ok_or_else(|| CopyError::MissingReqVar(private_varname.to_string()))?;
    let var_unit = get_string_attr(&private_var, "units")
        .change_context_lazy(|| CopyError::context(format!("getting units for {private_varname} to scale to the primary Xgas variable unit")))?;

    let data = private_var.get::<T, _>(Extents::All)
        .change_context_lazy(|| CopyError::context(format!("reading variable '{private_varname}'")))?;
    let do_subset_along = find_subset_dim(&private_var, TIME_DIM_NAME);
    let mut data = if let Some(idim) = do_subset_along {
        time_subsetter.subset_nd_array(data.view(), idim)?
    } else {
        data
    };

    // Only do a conversion if the units are different. This saves some
    // multiplying and avoids any weird floating point error
    if var_unit != target_unit {
        let conv_factor = dmf_conv_factor(&var_unit, target_unit)
            .map(|fac| T::from(fac))
           .change_context_lazy(|| CopyError::context(format!("getting conversion factor for {private_varname} to scale to the primary Xgas variable unit")))?;
        data.mapv_inplace(|el| el * conv_factor);
        attr_overrides.insert("units".to_string(), target_unit.into());
    }
    
    let mut public_var = copy_var_pre_write_helper::<T>(public_file, &private_var, public_varname)?;
    public_var.put(data.view(), Extents::All)
        .change_context_lazy(|| CopyError::context(format!("writing variable '{public_varname}'")))?;

    copy_var_attr_write_helper(&private_var, &mut public_var, long_name, &attr_overrides, attr_to_remove)?;
    
    Ok(())
}

/// Helper function to copy variable data generically. Unlike `copy_vmr_variable_from_dset`,
/// this does not need to know the variable type ahead of time.
fn copy_variable_general<S: AsRef<str>>(
    public_file: &mut netcdf::FileMut,
    private_var: &netcdf::Variable,
    public_varname: &str,
    time_subsetter: &Subsetter,
    long_name: &str,
    attr_overrides: &IndexMap<String, AttributeValue>,
    attr_to_remove: &[S],
) -> error_stack::Result<(), CopyError> {
    let private_varname = private_var.name();
    let generic_array = NcArray::get_from(private_var)
        .change_context_lazy(|| CopyError::context(format!("copying variable '{private_varname}'")))?;

    // Find the time dimension, assuming it does not occur more than once.
    let do_subset_along = find_subset_dim(private_var, TIME_DIM_NAME);
    let generic_array = if let Some(idim) = do_subset_along {
        time_subsetter.subset_generic_array(&generic_array, idim)?
    } else {
        generic_array
    };

    let mut public_var = match generic_array {
        NcArray::I8(arr) => {
            let mut pubv = copy_var_pre_write_helper::<i8>(public_file, private_var, public_varname)?;
            pubv.put(arr.view(), Extents::All)
                .change_context_lazy(|| CopyError::context(format!("writing variable '{public_varname}'")))?;
            pubv        
        },
        NcArray::I16(arr) => {
            let mut pubv = copy_var_pre_write_helper::<i16>(public_file, private_var, public_varname)?;
            pubv.put(arr.view(), Extents::All)
                .change_context_lazy(|| CopyError::context(format!("writing variable '{public_varname}'")))?;
            pubv        
        },
        NcArray::I32(arr) => {
            let mut pubv = copy_var_pre_write_helper::<i32>(public_file, private_var, public_varname)?;
            pubv.put(arr.view(), Extents::All)
                .change_context_lazy(|| CopyError::context(format!("writing variable '{public_varname}'")))?;
            pubv        
        },
        NcArray::I64(arr) => {
            let mut pubv = copy_var_pre_write_helper::<i64>(public_file, private_var, public_varname)?;
            pubv.put(arr.view(), Extents::All)
                .change_context_lazy(|| CopyError::context(format!("writing variable '{public_varname}'")))?;
            pubv        
        },
        NcArray::U8(arr) => {
            let mut pubv = copy_var_pre_write_helper::<u8>(public_file, private_var, public_varname)?;
            pubv.put(arr.view(), Extents::All)
                .change_context_lazy(|| CopyError::context(format!("writing variable '{public_varname}'")))?;
            pubv        
        },
        NcArray::U16(arr) => {
            let mut pubv = copy_var_pre_write_helper::<u16>(public_file, private_var, public_varname)?;
            pubv.put(arr.view(), Extents::All)
                .change_context_lazy(|| CopyError::context(format!("writing variable '{public_varname}'")))?;
            pubv        
        },
        NcArray::U32(arr) => {
            let mut pubv = copy_var_pre_write_helper::<u32>(public_file, private_var, public_varname)?;
            pubv.put(arr.view(), Extents::All)
                .change_context_lazy(|| CopyError::context(format!("writing variable '{public_varname}'")))?;
            pubv        
        },
        NcArray::U64(arr) => {
            let mut pubv = copy_var_pre_write_helper::<u64>(public_file, private_var, public_varname)?;
            pubv.put(arr.view(), Extents::All)
                .change_context_lazy(|| CopyError::context(format!("writing variable '{public_varname}'")))?;
            pubv        
        },
        NcArray::F32(arr) => {
            let mut pubv = copy_var_pre_write_helper::<f32>(public_file, private_var, public_varname)?;
            pubv.put(arr.view(), Extents::All)
                .change_context_lazy(|| CopyError::context(format!("writing variable '{public_varname}'")))?;
            pubv        
        },
        NcArray::F64(arr) => {
            let mut pubv = copy_var_pre_write_helper::<f64>(public_file, private_var, public_varname)?;
            pubv.put(arr.view(), Extents::All)
                .change_context_lazy(|| CopyError::context(format!("writing variable '{public_varname}'")))?;
            pubv        
        },
        NcArray::Char(arr) => {
            let mut pubv = copy_var_pre_write_helper::<u8>(public_file, private_var, public_varname)?;
            pubv.put(arr.view(), Extents::All)
                .change_context_lazy(|| CopyError::context(format!("writing variable '{public_varname}'")))?;
            pubv        
        },
    };

    copy_var_attr_write_helper(private_var, &mut public_var, long_name, attr_overrides, attr_to_remove)?;
    Ok(())
}


/// Centralizes the logic before writing data: adds needed dimensions and creates the public variable.
fn copy_var_pre_write_helper<'v, T: Copy + Zero + NcTypeDescriptor>(
    public_file: &'v mut netcdf::FileMut,
    private_var: &netcdf::Variable,
    public_varname: &str,
) -> error_stack::Result<netcdf::VariableMut<'v>, CopyError> {
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
    if dims_str.len() > 1 {
        // Assume that we always want compression on 2D variables. For public files,
        // this is a reasonable assumption, since they will usually be time x level.
        public_var.set_compression(9, true)
            .change_context_lazy(|| CopyError::context(format!("setting compresson on public variable '{public_varname}'")))?;
    }
    Ok(public_var)
}

/// Centralizes logic for attributes: adds "long_name" and copies/writes attributes based
/// on the overrides and `attr_to_remove` values.
fn copy_var_attr_write_helper<S: AsRef<str>>(
    private_var: &netcdf::Variable,
    public_var: &mut netcdf::VariableMut,
    long_name: &str,
    attr_overrides: &IndexMap<String, AttributeValue>,
    attr_to_remove: &[S],
) -> error_stack::Result<(), CopyError> {
    let private_varname = private_var.name();
    let public_varname = public_var.name();
    public_var.put_attribute("long_name", long_name)
        .change_context_lazy(|| CopyError::context(format!("adding 'long_name' attribute to public variable '{public_varname}'")))?;
    for (att_name, att_value) in attr_overrides.iter() {
        public_var.put_attribute(&att_name, att_value.to_owned())
            .change_context_lazy(|| CopyError::context(format!("adding '{att_name}' attribute to public variable '{public_varname}'")))?;
    }
    for att in private_var.attributes() {
        let att_name = att.name();
        if att_name != "long_name" && !attr_overrides.contains_key(att_name) && !attr_to_remove.iter().any(|a| a.as_ref() == att_name) {
            let att_value = att.value()
                .change_context_lazy(|| CopyError::context(format!("getting original value of attribute '{att_name}' from private variable '{private_varname}'")))?;
            public_var.put_attribute(att_name, att_value)
                .change_context_lazy(|| CopyError::context(format!("adding '{att_name}' to public variable '{public_varname}'")))?;
        }
    }
    Ok(())
}

fn get_string_attr(var: &netcdf::Variable, attr: &str) -> error_stack::Result<String, CopyError> {
    let res: Result<String, _> = var
        .attribute_value(attr)
        .ok_or_else(|| CopyError::missing_req_attr(var.name(), attr))?
        .change_context_lazy(|| CopyError::context(format!("could not read '{attr}' attribute on {}", var.name())))?
        .try_into();
    res.change_context_lazy(|| CopyError::context(format!("could not convert '{attr}' attribute on {} into a string", var.name())))
}

fn find_subset_dim(var: &netcdf::Variable, dimname: &str) -> Option<usize> {
    var.dimensions()
        .iter()
        .enumerate()
        .fold(None, |acc, (idim, dim)| {
            if dim.name() == dimname {
                Some(idim)
            } else {
                acc
            }
        })
}