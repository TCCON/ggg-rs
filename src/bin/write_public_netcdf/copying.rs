use std::{marker::PhantomData, ops::Mul};


use compute_helpers::add_geos_version_variable;
use error_stack::ResultExt;
use ggg_rs::{nc_utils::NcArray, units::dmf_long_name};
use indexmap::IndexMap;
use itertools::Itertools;
use ndarray::{ArrayD, ArrayView1, ArrayViewD, Axis};
use netcdf::{AttributeValue, NcTypeDescriptor};
use num_traits::Zero;
use serde::{Deserializer, Deserialize};

use crate::{config::default_attr_remove, constants::{PRIOR_INDEX_VARNAME, TIME_DIM_NAME}};
use copy_utils::{add_needed_dims, add_needed_new_dims, find_subset_dim, get_string_attr};
use xgas_helpers::{convert_dmf_array, expand_prior_profiles_from_file, expand_slant_xgas_binned_aks_from_file, get_traceability_scale, write_extrapolation_flags};
use copy_helpers::{copy_variable_general, copy_variable_new_data, copy_vmr_variable_from_dset};

mod copy_utils;
mod xgas_helpers;
mod copy_helpers;
mod compute_helpers;

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

    #[error("Variable '{varname}' has an inconsistent value at index {index} along dimension {dimension} (both 0-based)")]
    InconsistentValue{varname: String, dimension: usize, index: usize},

    /// This is a wrapper error used to provide more context to an underlying error.    
    #[error("An error occurred while {0}")]
    Context(String),

    /// A type representing a general error that does not need a specific variant.
    #[error("{0}")]
    Custom(String),
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

    fn inconsistent_value<V: ToString>(varname: V, dimension: usize, index: usize) -> Self {
        Self::InconsistentValue { varname: varname.to_string(), dimension, index }
    }

    pub(crate) fn context<S: ToString>(ctx: S) -> Self {
        Self::Context(ctx.to_string())
    }

    pub(crate) fn custom<S: ToString>(msg: S) -> Self {
        Self::Custom(msg.to_string())
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

#[derive(Debug, Deserialize, PartialEq)]
pub(crate) struct AuxVarCopy {
    /// The variable from the private file to copy.
    pub(crate) private_varname: String,

    /// The name to give the variable in the output file. If `None`, the
    /// variable will have the same name as in the private file.
    #[serde(default)]
    pub(crate) public_varname: Option<String>,

    /// Value to use for the long name attribute.
    pub(crate) long_name: String,

    /// Additional attributes to add, or values to replace private file
    /// attributes.
    #[serde(default, deserialize_with = "de_attribute_overrides")]
    pub(crate) attr_overrides: IndexMap<String, netcdf::AttributeValue>,

    /// A list of private attributes to remove.
    #[serde(default = "crate::config::default_attr_remove")]
    pub(crate) attr_to_remove: Vec<String>,

    /// Whether this variable is required or can be skipped if
    /// not present in the source file
    #[serde(default = "crate::config::default_true")]
    pub(crate) required: bool,

}

impl AuxVarCopy {
    pub(crate) fn new<P: ToString, L: ToString>(private_varname: P, long_name: L, required: bool) -> Self {
        Self {
            private_varname: private_varname.to_string(),
            public_varname: None,
            long_name: long_name.to_string(),
            attr_overrides: IndexMap::new(),
            attr_to_remove: crate::config::default_attr_remove(),
            required,
        }
    }

    #[allow(dead_code)] // needed at least for testing
    pub(crate) fn new_keep_attrs<P: ToString, L: ToString>(private_varname: P, long_name: L, required: bool) -> Self {
        Self {
            private_varname: private_varname.to_string(),
            public_varname: None,
            long_name: long_name.to_string(),
            attr_overrides: IndexMap::new(),
            attr_to_remove: vec![],
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

    #[allow(dead_code)] // needed at least for testing
    pub(crate) fn with_attr_remove<S: ToString>(mut self, attr_name: S) -> Self {
        let attr_name = attr_name.to_string();
        self.attr_to_remove.push(attr_name);
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

#[derive(Debug, Deserialize)]
pub(crate) struct XgasCopy<T: Copy + Zero + NcTypeDescriptor> {
    xgas: String,
    gas: String,
    gas_long: String,
    prior_profile: XgasAncillary,
    prior_xgas: XgasAncillary,
    ak: XgasAncillary,
    slant_bin: XgasAncillary,
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
            slant_bin: XgasAncillary::Inferred,
            traceability_scale: XgasAncillary::Inferred,
            data_type: PhantomData
        }
    }

    pub(crate) fn new_from_varname(varname: &str, gas_long_names: IndexMap<String, String>) -> Result<Self, CopyError> {
        if !varname.starts_with('x') {
            return Err(CopyError::custom(format!("Expected Xgas variable name ('{varname}') to start with 'x'")));
        }

        let xgas = varname.split('_')
            .next()
            .ok_or_else(|| CopyError::custom(format!("Expected Xgas variable name ('{varname}') to have at least one part when split on '_'")))?
            .to_string();

        // Already checked that it starts with "x", so just take everything after the first character.
        let gas: String = xgas.chars().get(1..).collect();
        let gas_long = gas_long_names.get(&gas)
            .map(|name| name.to_string())
            .unwrap_or_else(|| gas.clone());
        Ok(Self::new(xgas, gas, gas_long))
    }

    fn xgas_error_name(&self) -> String {
        let mut parts = self.xgas.split('_').collect_vec();
        parts.insert(1, "error");
        parts.join("_")
    }

    fn airmass_name(&self) -> &str {
        "o2_7885_am_o2"
    }

    fn infer_traceability_names(&self) -> (String, String) {
        let name = format!("aicf_{}_scale", self.xgas);
        (name.clone(), name)
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

    fn infer_slant_xgas_bin_name(&self) -> String {
        format!("ak_slant_{}_bin", self.xgas)
    }

    fn slant_bin_name(&self) -> String {
        match self.slant_bin {
            XgasAncillary::Inferred => self.infer_slant_xgas_bin_name(),
            XgasAncillary::InferredIfFirst => self.infer_slant_xgas_bin_name(),
            XgasAncillary::Specified { ref private_name, public_name: _ } => private_name.to_string(),
            XgasAncillary::CopyIfFirst { ref private_name, public_name: _ } => private_name.to_string(),
            XgasAncillary::Omit => self.infer_slant_xgas_bin_name(),
        }
    }
}

impl<T: Copy + Zero + NcTypeDescriptor + Mul<Output = T> + From<f32>> CopySet for XgasCopy<T> {
    fn copy(&self, private_file: &netcdf::File, public_file: &mut netcdf::FileMut, time_subsetter: &Subsetter) -> error_stack::Result<(), CopyError> {
        // Copy the xgas and its error, get the WMO scale and make it an attribute, copy the prior profile,
        // prior Xgas, and averaging kernels.

        // Grab the units from the Xgas variable - we will need them to ensure that the
        // prior profile and prior Xgas are in the same units. Also go ahead and get+subset
        // the Xgas value, as we'll need that for the AKs.

        let xgas_var = private_file.variable(&self.xgas)
            .ok_or_else(|| CopyError::MissingReqVar(self.xgas.clone()))?;
        let gas_units = get_string_attr(&xgas_var, "units")
            .change_context_lazy(|| CopyError::context(format!("getting the {} units", self.xgas)))?;
        let long_units = dmf_long_name(&gas_units)
            .unwrap_or(&gas_units);
        let attr_to_remove = default_attr_remove();
        let traceability_scale = if let Some((private_scale_name, _)) = self.traceability_scale.get_var_names_opt(public_file, || self.infer_traceability_names()) {
            let scale = get_traceability_scale(private_file, &private_scale_name)?;
            if scale.is_empty() {
                "N/A".to_string()
            } else {
                scale
            }
        } else {
            "N/A".to_string()
        };

        // Now copy the Xgas itself
        copy_vmr_variable_from_dset::<f32, _>(
            private_file,
            public_file,
            &self.xgas,
            &self.xgas,
            time_subsetter,
            &format!("column average {} mole fraction", self.gas_long),
            IndexMap::from_iter([
                ("wmo_or_analogous_scale".to_string(), traceability_scale.into())
            ]),
            &attr_to_remove,
        &gas_units,
        )?;

        // And its error value
        let error_name = self.xgas_error_name();
        copy_vmr_variable_from_dset::<f32, _>(
            private_file,
            public_file,
            &error_name,
            &error_name,
            time_subsetter,
            &format!("column average {} mole fraction error", self.gas_long),
            IndexMap::new(),
            &attr_to_remove,
            &gas_units
        )?;

        // And the prior Xgas value
        let opt = self.prior_xgas.get_var_names_opt(public_file, || self.infer_prior_xgas_names());
        if let Some((private_prxgas_name, public_prxgas_name)) = opt {
            copy_vmr_variable_from_dset::<f32, _>(
                private_file,
                public_file,
                &private_prxgas_name,
                &public_prxgas_name,
                time_subsetter,
                &format!("a priori {} column average", self.gas_long),
                IndexMap::new(),
                &attr_to_remove,
                &gas_units
            )?;
        }


        // Now the a priori profiles. They will (for now) be expanded
        // here.
        let opt = self.prior_profile.get_var_names_opt(&public_file, || self.infer_prior_prof_names());
        if let Some((private_prior_name, public_prior_name)) = opt {
            let prior_var = private_file.variable(&private_prior_name)
                .ok_or_else(|| CopyError::MissingReqVar(private_prior_name.clone()))?;
            let prior_data = expand_prior_profiles_from_file(
                private_file, 
                &private_prior_name,
                PRIOR_INDEX_VARNAME,
                &gas_units,
                time_subsetter
            )?;
            let level_dim_name = prior_var.dimensions()
                .get(1)
                .ok_or_else(|| CopyError::custom(format!("Expected '{private_prior_name}' to have altitude as the second dimension, but it has fewer than 2 dimensions")))?
                .name();
            let attr_overrides = IndexMap::from_iter([
                ("units".to_string(), gas_units.as_str().into()),
                ("long_units".to_string(), format!("{long_units} (wet mole fraction)").into()),
                ("description".to_string(), format!("a priori profile of {}", self.gas_long).into())
            ]);

            copy_variable_new_data(
                public_file,
                &prior_var,
                &public_prior_name,
                prior_data.into_dyn().view(),
                vec![TIME_DIM_NAME.to_string(), level_dim_name],
                &format!("a priori {} profile", self.gas_long),
                &attr_overrides,
                &attr_to_remove
            )?;
        }

        // Likewise for the AKs
        let opt = self.ak.get_var_names_opt(&public_file, || self.infer_ak_names());
        if let Some((private_ak_name, public_ak_name)) = opt {
            let ak_var = private_file.variable(&private_ak_name)
                .ok_or_else(|| CopyError::MissingReqVar(private_ak_name.clone()))?;
                
            let (expanded_aks, ak_extrap_flags) = expand_slant_xgas_binned_aks_from_file(
                private_file,
                &self.xgas,
                self.airmass_name(),
                &private_ak_name,
                &self.slant_bin_name(),
                time_subsetter,
                Some(500)
            )?;

            let extrap_flag_varname = format!("extrapolation_flags_{public_ak_name}");
            let level_dim_name = ak_var.dimensions()
                .get(0)
                .ok_or_else(|| CopyError::custom(format!("Expected AK variable '{private_ak_name}' to have altitude as the first dimension, but had no dimensions")))?
                .name();

            copy_variable_new_data(
                public_file,
                &ak_var,
                &public_ak_name,
                expanded_aks.into_dyn().view(),
                vec![TIME_DIM_NAME.to_string(), level_dim_name],
                &format!("{} averaging kernel", self.gas_long),
                &IndexMap::from_iter([("ancillary_variables".to_string(), extrap_flag_varname.as_str().into())]),
                &attr_to_remove
            )?;

            write_extrapolation_flags(public_file, &public_ak_name, &extrap_flag_varname, ak_extrap_flags.view())?;
        }

        Ok(())
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
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

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub(crate) enum ComputedVariables {
    PriorSource{public_varname: Option<String>}
}

impl CopySet for ComputedVariables {
    fn copy(&self, private_file: &netcdf::File, public_file: &mut netcdf::FileMut, time_subsetter: &Subsetter) -> error_stack::Result<(), CopyError> {
        match self {
            ComputedVariables::PriorSource { public_varname } => {
                let pubname = public_varname
                    .as_deref()
                    .unwrap_or("apriori_data_source");
                add_geos_version_variable(private_file, public_file, pubname, time_subsetter)
            },
        }
    }
}

// ---------------- //
// HELPER FUNCTIONS //
// ---------------- //

fn de_attribute_overrides<'de, D>(deserializer: D) -> Result<IndexMap<String,AttributeValue>, D::Error>
where D: Deserializer<'de>
{
    let map = toml::Table::deserialize(deserializer)?;
    let mut attr_overrides = IndexMap::new();
    for (attr, val) in map.into_iter() {
        let attr_val = match val {
            toml::Value::String(s) => AttributeValue::Str(s),
            toml::Value::Integer(i) => AttributeValue::Longlong(i),
            toml::Value::Float(f) => AttributeValue::Double(f),
            toml::Value::Boolean(b) => AttributeValue::Ushort(b as u16),
            toml::Value::Datetime(datetime) => {
                let dstr = datetime.to_string();
                AttributeValue::Str(dstr)
            },
            toml::Value::Array(values) => {
                let vstr = values.into_iter()
                    .map(|v| v.to_string())
                    .collect_vec();
                AttributeValue::Strs(vstr)
            },
            toml::Value::Table(_) => {
                return Err(serde::de::Error::custom(format!(
                    "While reading attribute overrides, got a table for the value of attribute '{attr}'. Tables cannot be converted to netCDF attribute values."
                )))
            },
        };
        attr_overrides.insert(attr, attr_val);
    }
    Ok(attr_overrides)
}





#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_de_aux_var() {
        let toml_str = r#"private_varname = "time"
        long_name = "zero path difference time"
        "#;
        let aux_de: AuxVarCopy = toml::from_str(toml_str)
            .expect("deserialization should work");
        let aux_val = AuxVarCopy::new("time", "zero path difference time", true);
        assert_eq!(aux_de, aux_val);
    }

    #[test]
    fn test_de_aux_var_pub_name() {
        let toml_str = r#"private_varname = "year"
        long_name = "year"
        public_varname = "decimal_year"
        "#;
        let aux_de: AuxVarCopy = toml::from_str(toml_str)
            .expect("deserialization should work");
        let aux_val = AuxVarCopy::new("year", "year", true)
            .with_public_varname("decimal_year");
        assert_eq!(aux_de, aux_val);
    }

    #[test]
    fn test_de_aux_var_attrs() {
        let toml_str = r#"private_varname = "day"
        long_name = "day of year"
        attr_overrides = {units = "Julian day", description = "1-based day of year"}
        attr_to_remove = ["vmin", "vmax"]
        "#;
        let aux_de: AuxVarCopy = toml::from_str(toml_str)
            .expect("deserialization should work");

        let aux_val = AuxVarCopy::new_keep_attrs("day", "day of year", true)
            .with_attr_override("units", "Julian day")
            .with_attr_override("description", "1-based day of year")
            .with_attr_remove("vmin")
            .with_attr_remove("vmax");

        assert_eq!(aux_de, aux_val);
    }

    #[test]
    fn test_de_aux_var_not_req() {
        let toml_str = r#"private_varname = "hour"
        long_name = "UTC hour"
        required = false
        "#;
        let aux_de: AuxVarCopy = toml::from_str(toml_str)
            .expect("deserialization should work");

        let aux_val = AuxVarCopy::new("hour", "UTC hour", false);
        assert_eq!(aux_de, aux_val);
    }

    #[test]
    fn test_de_xgas_anc_inferred() {
        let toml_str = r#"type = "inferred""#;
        let anc_de: XgasAncillary = toml::from_str(toml_str)
           .expect("deserialization should work");
        assert_eq!(anc_de, XgasAncillary::Inferred);
    }

    #[test]
    fn test_de_xgas_anc_inferred_if_first() {
        let toml_str = r#"type = "inferred_if_first""#;
        let anc_de: XgasAncillary = toml::from_str(toml_str)
           .expect("deserialization should work");
        assert_eq!(anc_de, XgasAncillary::InferredIfFirst);
    }

    #[test]
    fn test_de_xgas_anc_specified() {
        let toml_str = r#"type = "specified"
        private_name = "prior_xco2""#;
        let anc_de: XgasAncillary = toml::from_str(toml_str)
           .expect("deserialization should work");
        assert_eq!(anc_de, XgasAncillary::Specified { private_name: "prior_xco2".to_string(), public_name: None });

        let toml_str = r#"type = "specified"
        private_name = "prior_1co2"
        public_name = "prior_co2"
        "#;
        let anc_de: XgasAncillary = toml::from_str(toml_str)
           .expect("deserialization should work");
        assert_eq!(anc_de, XgasAncillary::Specified { private_name: "prior_1co2".to_string(), public_name: Some("prior_co2".to_string()) });
    }

    #[test]
    fn test_de_xgas_anc_copy_if_first() {
        let toml_str = r#"type = "copy_if_first"
        private_name = "prior_xco2""#;
        let anc_de: XgasAncillary = toml::from_str(toml_str)
           .expect("deserialization should work");
        assert_eq!(anc_de, XgasAncillary::CopyIfFirst { private_name: "prior_xco2".to_string(), public_name: None });

        let toml_str = r#"type = "copy_if_first"
        private_name = "prior_1co2"
        public_name = "prior_co2"
        "#;
        let anc_de: XgasAncillary = toml::from_str(toml_str)
           .expect("deserialization should work");
        assert_eq!(anc_de, XgasAncillary::CopyIfFirst { private_name: "prior_1co2".to_string(), public_name: Some("prior_co2".to_string()) });
    }

    #[test]
    fn test_de_xgas_anc_omit() {
        let toml_str = r#"type = "omit""#;
        let anc_de: XgasAncillary = toml::from_str(toml_str)
           .expect("deserialization should work");
        assert_eq!(anc_de, XgasAncillary::Omit);
    }
}