use chrono::NaiveDate;
use compute_helpers::add_geos_version_variable;
use error_stack::ResultExt;
use ggg_rs::{nc_utils::NcArray, units::dmf_long_name};
use indexmap::IndexMap;
use itertools::Itertools;
use ndarray::{ArrayD, ArrayView1, ArrayViewD, Axis};
use netcdf::AttributeValue;
use num_traits::Zero;
use serde::{Deserialize, Deserializer};

use crate::{
    config::default_attr_remove,
    constants::{PRIOR_INDEX_VARNAME, PROGRAM_NAME, TIME_DIM_NAME},
    discovery::{AncillaryDiscoveryMethod, XgasMatchRule},
};
use copy_helpers::{copy_variable_general, copy_variable_new_data, copy_vmr_variable_from_dset};
use copy_utils::{
    add_needed_dims, add_needed_new_dims, find_subset_dim, get_root_string_attr, get_string_attr,
};
use xgas_helpers::{
    convert_dmf_array, expand_prior_profiles_from_file, expand_slant_xgas_binned_aks_from_file,
    get_traceability_scale, write_extrapolation_flags,
};

mod compute_helpers;
mod copy_helpers;
mod copy_utils;
mod xgas_helpers;

/// Represents an error that occurred while copying a variable
/// to the public file.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CopyError {
    /// Indicates that the input private file is missing a variable that was
    /// expected to be present.
    #[error("Private file is missing the required variable '{0}'")]
    MissingReqVar(String),

    /// Indicates that the input private file is missing an attribute (either
    /// on a group or variable) that was expected to be present.
    #[error("Private file is missing the required attribute '{attr}' under '{parent}'")]
    MissingReqAttr { parent: String, attr: String },

    /// Indicates that a dimension shared among multiple variables has a different
    /// expected length for one variable than it was defined with.
    #[error("Dimension '{dimname}' has length {dim_len_in_file} in the public file, but the variable '{varname}' expects it to have length {dim_len_in_var}")]
    DimLenMismatch {
        dimname: String,
        varname: String,
        dim_len_in_file: usize,
        dim_len_in_var: usize,
    },

    #[error("Variable '{varname}' has an inconsistent value at index {index} along dimension {dimension} (both 0-based)")]
    InconsistentValue {
        varname: String,
        dimension: usize,
        index: usize,
    },

    /// This is a wrapper error used to provide more context to an underlying error.    
    #[error("An error occurred while {0}")]
    Context(String),

    /// A type representing a general error that does not need a specific variant.
    #[error("{0}")]
    Custom(String),
}

impl CopyError {
    fn missing_req_attr<P: ToString, A: ToString>(parent: P, attr: A) -> Self {
        Self::MissingReqAttr {
            parent: parent.to_string(),
            attr: attr.to_string(),
        }
    }

    fn dim_len_mismatch<D: ToString, V: ToString>(
        dimname: D,
        varname: V,
        len_in_file: usize,
        len_in_var: usize,
    ) -> Self {
        Self::DimLenMismatch {
            dimname: dimname.to_string(),
            varname: varname.to_string(),
            dim_len_in_file: len_in_file,
            dim_len_in_var: len_in_var,
        }
    }

    fn inconsistent_value<V: ToString>(varname: V, dimension: usize, index: usize) -> Self {
        Self::InconsistentValue {
            varname: varname.to_string(),
            dimension,
            index,
        }
    }

    pub(crate) fn context<S: ToString>(ctx: S) -> Self {
        Self::Context(ctx.to_string())
    }

    pub(crate) fn custom<S: ToString>(msg: S) -> Self {
        Self::Custom(msg.to_string())
    }
}

pub(crate) trait CopySet {
    fn copy(
        &self,
        private_file: &netcdf::File,
        public_file: &mut netcdf::FileMut,
        time_subsetter: &Subsetter,
    ) -> error_stack::Result<(), CopyError>;
}

pub(crate) struct Subsetter {
    keep_inds: Vec<usize>,
}

impl Subsetter {
    pub(crate) fn from_flag(flag: ArrayView1<i32>) -> Self {
        let it = flag
            .iter()
            .enumerate()
            .filter_map(|(i, &f)| if f == 0 { Some(i) } else { None });
        let keep_inds = Vec::from_iter(it);
        Self { keep_inds }
    }

    pub(crate) fn add_cutoff_date(&mut self, nc_times: ArrayView1<f64>, end_date: NaiveDate) {
        let end_datetime = end_date.and_hms_opt(0, 0, 0).unwrap();
        let end_timestamp = end_datetime.and_utc().timestamp() as f64;
        let had_data = !self.keep_inds.is_empty();

        self.keep_inds.retain(|&i| {
            let t = nc_times.get(i)
                .expect(&format!("Tried to get index {i} of the netCDF times, but this was beyond the end of the times array."));
            *t < end_timestamp
        });

        if self.keep_inds.is_empty() && had_data {
            log::warn!("No data present after data end date, {end_date}. Reduce the data latency days or move the data latency date forward to have public data.");
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.keep_inds.len()
    }

    pub(crate) fn subset_nd_array<T: Copy + Zero>(
        &self,
        arr: ArrayViewD<T>,
        along_axis: usize,
    ) -> Result<ArrayD<T>, CopyError> {
        let mut shape = Vec::from_iter(arr.shape().iter().map(|x| *x));
        if shape.len() == 0 {
            // If we somehow got a 0-D array, then there is nothing to subset -
            // return it as-is
            return Ok(arr.to_owned());
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

    pub(crate) fn subset_generic_array(
        &self,
        arr: &NcArray,
        along_axis: usize,
    ) -> Result<NcArray, CopyError> {
        match arr {
            NcArray::I8(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::I8(arr))
            }
            NcArray::I16(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::I16(arr))
            }
            NcArray::I32(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::I32(arr))
            }
            NcArray::I64(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::I64(arr))
            }
            NcArray::U8(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::U8(arr))
            }
            NcArray::U16(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::U16(arr))
            }
            NcArray::U32(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::U32(arr))
            }
            NcArray::U64(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::U64(arr))
            }
            NcArray::F32(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::F32(arr))
            }
            NcArray::F64(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::F64(arr))
            }
            NcArray::Char(arr) => {
                let arr = self.subset_nd_array(arr.view(), along_axis)?;
                Ok(NcArray::U8(arr))
            }
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct AuxVarCopy {
    /// The variable from the private file to copy.
    pub(crate) private_name: String,

    /// The name to give the variable in the output file. If `None`, the
    /// variable will have the same name as in the private file.
    #[serde(default)]
    pub(crate) public_name: Option<String>,

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
    pub(crate) fn new<P: ToString, L: ToString>(
        private_name: P,
        long_name: L,
        required: bool,
    ) -> Self {
        Self {
            private_name: private_name.to_string(),
            public_name: None,
            long_name: long_name.to_string(),
            attr_overrides: IndexMap::new(),
            attr_to_remove: crate::config::default_attr_remove(),
            required,
        }
    }

    #[allow(dead_code)] // needed at least for testing
    pub(crate) fn new_keep_attrs<P: ToString, L: ToString>(
        private_name: P,
        long_name: L,
        required: bool,
    ) -> Self {
        Self {
            private_name: private_name.to_string(),
            public_name: None,
            long_name: long_name.to_string(),
            attr_overrides: IndexMap::new(),
            attr_to_remove: vec![],
            required,
        }
    }

    pub(crate) fn with_public_name<P: ToString>(mut self, public_name: P) -> Self {
        self.public_name = Some(public_name.to_string());
        self
    }

    pub(crate) fn with_attr_override<N: ToString, V: Into<netcdf::AttributeValue>>(
        mut self,
        attr_name: N,
        attr_value: V,
    ) -> Self {
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
    fn copy(
        &self,
        private_file: &netcdf::File,
        public_file: &mut netcdf::FileMut,
        time_subsetter: &Subsetter,
    ) -> error_stack::Result<(), CopyError> {
        // Will need to create a variable with the same dimensions, then copy the good subset of values
        // and the attributes.
        let private_var = if let Some(var) = private_file.variable(&self.private_name) {
            var
        } else if self.required {
            return Err(CopyError::MissingReqVar(self.private_name.clone()).into());
        } else {
            log::info!(
                "Not copying {} as it is not present in the private file",
                self.private_name
            );
            return Ok(());
        };

        let public_name = self.public_name.as_deref().unwrap_or(&self.private_name);

        copy_variable_general(
            public_file,
            &private_var,
            public_name,
            time_subsetter,
            &self.long_name,
            &self.attr_overrides,
            &self.attr_to_remove,
        )
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PriorProfCopy {
    /// The variable from the private file to copy.
    pub(crate) private_name: String,

    /// The name to give the variable in the output file. If `None`, the
    /// variable will have the same name as in the private file.
    #[serde(default)]
    pub(crate) public_name: Option<String>,

    /// Value to use for the long name attribute.
    pub(crate) long_name: String,

    /// Desired unit for VMR profiles. Should be `None` for other
    /// types of profiles, as those conversions are not yet implemented.
    #[serde(default)]
    pub(crate) target_vmr_unit: Option<String>,

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

impl PriorProfCopy {
    pub(crate) fn new<P: ToString, L: ToString>(
        private_name: P,
        long_name: L,
        required: bool,
    ) -> Self {
        Self {
            private_name: private_name.to_string(),
            public_name: None,
            long_name: long_name.to_string(),
            target_vmr_unit: None,
            attr_overrides: IndexMap::new(),
            attr_to_remove: crate::config::default_attr_remove(),
            required,
        }
    }

    pub(crate) fn with_public_name<P: ToString>(mut self, public_name: P) -> Self {
        self.public_name = Some(public_name.to_string());
        self
    }

    pub(crate) fn set_attr_overrides(
        mut self,
        overrides: IndexMap<String, AttributeValue>,
    ) -> Self {
        self.attr_overrides = overrides;
        self
    }

    pub(crate) fn with_vmr_units<U: ToString>(mut self, units: U) -> Self {
        self.target_vmr_unit = Some(units.to_string());
        self
    }
}

impl CopySet for PriorProfCopy {
    fn copy(
        &self,
        private_file: &netcdf::File,
        public_file: &mut netcdf::FileMut,
        time_subsetter: &Subsetter,
    ) -> error_stack::Result<(), CopyError> {
        let public_name = self.public_name.as_deref().unwrap_or(&self.private_name);

        let prior_var = if let Some(var) = private_file.variable(&self.private_name) {
            var
        } else if self.required {
            return Err(CopyError::MissingReqVar(self.private_name.clone()).into());
        } else {
            log::info!(
                "Not copying {} as it is not present in the private file",
                self.private_name
            );
            return Ok(());
        };

        let prior_data = expand_prior_profiles_from_file(
            private_file,
            &self.private_name,
            PRIOR_INDEX_VARNAME,
            self.target_vmr_unit.as_deref(),
            time_subsetter,
        )?;

        let mut new_dims = prior_var
            .dimensions()
            .iter()
            .map(|d| d.name())
            .collect_vec();
        new_dims[0] = TIME_DIM_NAME.to_string();

        let mut attr_overrides = self.attr_overrides.clone();
        // Give the user a warning that units and long units will be ignored: .insert() returns Some(_) if
        // there was already a value for that key.
        if let Some(gas_units) = self.target_vmr_unit.as_deref() {
            if attr_overrides
                .insert("units".to_string(), gas_units.into())
                .is_some()
            {
                log::warn!(
                    "The 'units' attribute cannot be overridden for public variable {public_name} because it must match the target VMR units"
                )
            }

            let long_units = dmf_long_name(&gas_units).unwrap_or(&gas_units);
            if attr_overrides
                .insert(
                    "long_units".to_string(),
                    format!("{long_units} (wet mole fraction)").into(),
                )
                .is_some()
            {
                log::warn!("The 'long_units' attribute cannot be overidden for public variable {public_name} because it must match the target VMR units")
            }
        }

        copy_variable_new_data(
            public_file,
            &prior_var,
            &public_name,
            prior_data.view(),
            new_dims,
            &self.long_name,
            &attr_overrides,
            &self.attr_to_remove,
        )?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct XgasCopy {
    /// The name of the Xgas variable to copy
    xgas: String,

    /// The name to give the Xgas variable in the public file
    #[serde(default)]
    xgas_public: Option<String>,

    #[serde(default, deserialize_with = "de_attribute_overrides")]
    xgas_attr_overrides: IndexMap<String, AttributeValue>,

    /// The abbreviation of the physical gas, e.g., both `wco2` and `lco2`
    /// should set this to "co2". This can be used to identify variables that
    /// have the same priors, for example.
    gas: String,

    /// Determines whether it is an error if this Xgas is missing from the
    /// file. The default (`true`) means that it is an error if missing,
    /// while `false` means that it is allowed for this gas to be missing.
    #[serde(default = "crate::config::default_true")]
    pub(crate) required: bool,

    /// The proper name of the gas, e.g. "carbon dioxide" for CO2. This will
    /// be used in netCDF attributes. It is acceptable to insert the abbreviation
    /// as a fallback, though this is not preferred.
    #[serde(default = "crate::config::default_empty_string")]
    gas_long: String,

    #[serde(default = "crate::config::default_ancillary_infer")]
    xgas_error: XgasAncillary,

    #[serde(default, deserialize_with = "de_attribute_overrides")]
    xgas_error_attr_overrides: IndexMap<String, AttributeValue>,

    /// How/whether to copy the a priori profiles.
    #[serde(default = "crate::config::default_ancillary_infer_first")]
    prior_profile: XgasAncillary,

    #[serde(default, deserialize_with = "de_attribute_overrides")]
    prior_profile_attr_overrides: IndexMap<String, AttributeValue>,

    /// How/whether to copy the a prior column average.
    #[serde(default = "crate::config::default_ancillary_infer_first")]
    prior_xgas: XgasAncillary,

    #[serde(default, deserialize_with = "de_attribute_overrides")]
    prior_xgas_attr_overrides: IndexMap<String, AttributeValue>,

    /// How/whether to copy the averaging kernels.
    #[serde(default = "crate::config::default_ancillary_infer_first")]
    ak: XgasAncillary,

    #[serde(default, deserialize_with = "de_attribute_overrides")]
    ak_attr_overrides: IndexMap<String, AttributeValue>,

    /// Where to find the slant Xgas bins that correspond to the AKs.
    /// This is a special case, and will only be accessed if the AKs are
    /// needed, but MUST be available in that case. Thus, this should never
    /// be `XgasAncillary::Omit`.
    #[serde(default = "crate::config::default_ancillary_infer")]
    slant_bin: XgasAncillary,

    /// How/whether to find the traceability scale for the gas. This should
    /// point to a character variable that lists the scale for each observation;
    /// the writer can then check if this is consistent and collapse it to a
    /// single attribute.
    #[serde(default = "crate::config::default_ancillary_infer")]
    traceability_scale: XgasAncillary,
}

impl XgasCopy {
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
    /// - `gas_long`: the full name of the gas, as opposed to its abbreviation. For example, "carbon dioxide"
    /// instead of "co2".
    #[allow(dead_code)]
    pub(crate) fn new<X: ToString, GS: ToString, GL: ToString>(
        xgas: X,
        gas: GS,
        gas_long: GL,
    ) -> Self {
        Self {
            xgas: xgas.to_string(),
            xgas_public: None,
            xgas_attr_overrides: IndexMap::new(),
            gas: gas.to_string(),
            gas_long: gas_long.to_string(),
            required: true,
            xgas_error: XgasAncillary::Inferred,
            xgas_error_attr_overrides: IndexMap::new(),
            prior_profile: XgasAncillary::InferredIfFirst,
            prior_profile_attr_overrides: IndexMap::new(),
            prior_xgas: XgasAncillary::InferredIfFirst,
            prior_xgas_attr_overrides: IndexMap::new(),
            ak: XgasAncillary::InferredIfFirst,
            ak_attr_overrides: IndexMap::new(),
            slant_bin: XgasAncillary::Inferred,
            traceability_scale: XgasAncillary::Inferred,
        }
    }

    pub(crate) fn new_from_discovery<X: ToString, XP: ToString, GS: ToString, GL: ToString>(
        xgas: X,
        xgas_public: Option<XP>,
        gas: GS,
        gas_long: GL,
        rule: &XgasMatchRule,
    ) -> Self {
        let xgas_error = rule
            .xgas_error
            .map(|x| x.into())
            .unwrap_or(XgasAncillary::Inferred);
        let prior_profile = rule
            .prior_profile
            .map(|x| x.into())
            .unwrap_or(XgasAncillary::InferredIfFirst);
        let prior_xgas = rule
            .prior_xgas
            .map(|x| x.into())
            .unwrap_or(XgasAncillary::InferredIfFirst);
        let ak = rule
            .ak
            .map(|x| x.into())
            .unwrap_or(XgasAncillary::InferredIfFirst);
        let slant_bin = rule
            .slant_bin
            .map(|x| x.into())
            .unwrap_or(XgasAncillary::Inferred);
        let traceability_scale = rule
            .traceability_scale
            .map(|x| x.into())
            .unwrap_or(XgasAncillary::Inferred);
        Self {
            xgas: xgas.to_string(),
            xgas_public: xgas_public.map(|name| name.to_string()),
            xgas_attr_overrides: rule.xgas_attr_overrides.clone(),
            gas: gas.to_string(),
            gas_long: gas_long.to_string(),
            required: true, // if we discovered this Xgas, it must be present
            xgas_error,
            xgas_error_attr_overrides: rule.xgas_error_attr_overrides.clone(),
            prior_profile,
            prior_profile_attr_overrides: rule.prior_profile_attr_overrides.clone(),
            prior_xgas,
            prior_xgas_attr_overrides: rule.prior_xgas_attr_overrides.clone(),
            ak,
            ak_attr_overrides: rule.ak_attr_overrides.clone(),
            slant_bin,
            traceability_scale,
        }
    }

    pub(crate) fn xgas_varname(&self) -> &str {
        &self.xgas
    }

    pub(crate) fn gas(&self) -> &str {
        &self.gas
    }

    pub(crate) fn gas_from_xgas(&self) -> &str {
        let s = if let Some((s, _)) = self.xgas_varname().split_once('_') {
            s
        } else {
            self.xgas_varname()
        };

        s.trim_matches('x')
    }

    pub(crate) fn gas_full_name(&self) -> &str {
        &self.gas_long
    }

    pub(crate) fn set_gas_full_name(&mut self, name: String) {
        self.gas_long = name;
    }

    fn airmass_name(&self) -> &str {
        "o2_7885_am_o2"
    }

    fn infer_xgas_error_names(&self) -> (String, String) {
        fn make_name(xn: &str) -> String {
            let mut parts = xn.split('_').collect_vec();
            parts.insert(1, "error");
            parts.join("_")
        }

        let private_name = make_name(&self.xgas);
        let public_name = self
            .xgas_public
            .as_deref()
            .map(|name| make_name(name))
            .unwrap_or_else(|| private_name.clone());
        (private_name, public_name)
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
            XgasAncillary::OptInferredIfFirst => self.infer_slant_xgas_bin_name(),
            XgasAncillary::Specified {
                ref private_name,
                public_name: _,
            } => private_name.to_string(),
            XgasAncillary::SpecifiedIfFirst {
                ref private_name,
                public_name: _,
            } => private_name.to_string(),
            XgasAncillary::Omit => self.infer_slant_xgas_bin_name(),
        }
    }

    fn maybe_add_traceability_scale_attr(
        &self,
        private_file: &netcdf::File,
        public_file: &netcdf::File,
        attr_overrides: &mut IndexMap<String, AttributeValue>,
    ) -> error_stack::Result<(), CopyError> {
        if attr_overrides.contains_key("wmo_or_analogous_scale") {
            return Ok(());
        }

        let scale_varnames =
            self.traceability_scale
                .get_var_names_opt(private_file, public_file, || {
                    self.infer_traceability_names()
                });

        if let Some((private_scale_name, _)) = scale_varnames {
            log::debug!(
                "Getting {} traceability scale from {private_scale_name}",
                self.xgas
            );
            let scale = get_traceability_scale(private_file, &private_scale_name)?;
            if !scale.is_empty() {
                attr_overrides.insert("wmo_or_analogous_scale".to_string(), scale.into());
            }
        } else {
            log::debug!(
                "Not getting traceability scale for {} from any variable",
                self.xgas
            );
        };

        Ok(())
    }
}

impl CopySet for XgasCopy {
    fn copy(
        &self,
        private_file: &netcdf::File,
        public_file: &mut netcdf::FileMut,
        time_subsetter: &Subsetter,
    ) -> error_stack::Result<(), CopyError> {
        // Copy the xgas and its error, get the WMO scale and make it an attribute, copy the prior profile,
        // prior Xgas, and averaging kernels.

        // Grab the units from the Xgas variable - we will need them to ensure that the
        // prior profile and prior Xgas are in the same units. Also go ahead and get+subset
        // the Xgas value, as we'll need that for the AKs.

        let xgas_var = if let Some(var) = private_file.variable(&self.xgas) {
            var
        } else if self.required {
            return Err(CopyError::MissingReqVar(self.xgas.clone()).into());
        } else {
            log::info!("Optional Xgas '{}' not found, skipping", self.xgas);
            return Ok(());
        };

        let gas_units = get_string_attr(&xgas_var, "units").change_context_lazy(|| {
            CopyError::context(format!("getting the {} units", self.xgas))
        })?;
        let gas_units = if gas_units.is_empty() {
            log::info!(
                "Units for {} were an empty string, assuming this should be unscaled mole fraction",
                self.xgas
            );
            "parts"
        } else {
            &gas_units
        };

        // Get any existing "ancillary_variables" attribute. We won't put it into the attr_overrides
        // yet because we need to know which ancillary variables are actually available. We'll insert it
        // manually at the end.
        let mut ancillary_vars = match get_string_attr(&xgas_var, "ancillary_variables") {
            Ok(mut s) => {
                s.push(' ');
                s
            }
            Err(e) => {
                let inner = e.current_context();
                if let CopyError::MissingReqAttr { parent: _, attr: _ } = inner {
                    "".to_string()
                } else {
                    return Err(e);
                }
            }
        };

        let attr_to_remove = default_attr_remove();
        let mut attr_overrides = self.xgas_attr_overrides.clone();
        if !attr_overrides.contains_key("description") {
            let new_desc = xgas_helpers::update_xgas_description(&xgas_var, self.gas_from_xgas())?;
            attr_overrides.insert("description".to_string(), new_desc);
        }
        self.maybe_add_traceability_scale_attr(private_file, public_file, &mut attr_overrides)?;

        // Now copy the Xgas itself
        let public_xgas_name = self.xgas_public.as_deref().unwrap_or(&self.xgas);
        copy_vmr_variable_from_dset::<f32, _>(
            private_file,
            public_file,
            &self.xgas,
            public_xgas_name,
            time_subsetter,
            &format!("column average {} mole fraction", self.gas_long),
            attr_overrides,
            &attr_to_remove,
            &gas_units,
        )?;

        // And its error value
        let error_names_opt = self
            .xgas_error
            .get_var_names_opt(private_file, public_file, || self.infer_xgas_error_names());
        if let Some((private_error_name, public_error_name)) = error_names_opt {
            copy_vmr_variable_from_dset::<f32, _>(
                private_file,
                public_file,
                &private_error_name,
                &public_error_name,
                time_subsetter,
                &format!("column average {} mole fraction error", self.gas_long),
                self.xgas_error_attr_overrides.clone(),
                &attr_to_remove,
                &gas_units,
            )?;

            ancillary_vars.push_str(&public_error_name);
            ancillary_vars.push(' ');
        }

        // And the prior Xgas value
        let prxgas_names_opt = self
            .prior_xgas
            .get_var_names_opt(private_file, public_file, || self.infer_prior_xgas_names());
        if let Some((private_prxgas_name, public_prxgas_name)) = prxgas_names_opt {
            let mut attr_overrides = self.prior_xgas_attr_overrides.clone();
            attr_overrides.insert(
                "description".to_string(),
                format!(
                    "Column-average mole fraction calculated from the PRIOR profile of {}",
                    self.gas_full_name()
                )
                .into(),
            );
            copy_vmr_variable_from_dset::<f32, _>(
                private_file,
                public_file,
                &private_prxgas_name,
                &public_prxgas_name,
                time_subsetter,
                &format!("a priori {} column average", self.gas_long),
                attr_overrides,
                &attr_to_remove,
                &gas_units,
            )?;

            ancillary_vars.push_str(&public_prxgas_name);
            ancillary_vars.push(' ');
        }

        // Now the a priori profiles. They will (for now) be expanded
        // here.
        let opt = self
            .prior_profile
            .get_var_names_opt(private_file, &public_file, || self.infer_prior_prof_names());
        if let Some((private_prior_name, public_prior_name)) = opt {
            ancillary_vars.push_str(&public_prior_name);
            ancillary_vars.push(' ');

            let long_name = format!("a priori {} profile", self.gas_long);
            let mut attr_overrides = self.prior_profile_attr_overrides.clone();
            // description is allowed to be overridden. units and long_units are not, but those are handled in the
            // prior profile variable type.
            attr_overrides
                .entry("description".to_string())
                .or_insert_with(|| format!("a priori profile of {}", self.gas_long).into());

            let prior_copier = PriorProfCopy::new(private_prior_name, long_name, true)
                .with_public_name(public_prior_name)
                .with_vmr_units(gas_units)
                .set_attr_overrides(attr_overrides);

            prior_copier.copy(private_file, public_file, time_subsetter)?;
        }

        // Likewise for the AKs
        let opt = self
            .ak
            .get_var_names_opt(private_file, &public_file, || self.infer_ak_names());
        if let Some((private_ak_name, public_ak_name)) = opt {
            let ak_var = private_file
                .variable(&private_ak_name)
                .ok_or_else(|| CopyError::MissingReqVar(private_ak_name.clone()))?;

            let (expanded_aks, ak_extrap_flags) = expand_slant_xgas_binned_aks_from_file(
                private_file,
                &self.xgas,
                self.airmass_name(),
                &private_ak_name,
                &self.slant_bin_name(),
                time_subsetter,
                Some(500),
            )?;

            let extrap_flag_varname = format!("extrapolation_flags_{public_ak_name}");
            let level_dim_name = ak_var.dimensions()
                .get(0)
                .ok_or_else(|| CopyError::custom(format!("Expected AK variable '{private_ak_name}' to have altitude as the first dimension, but had no dimensions")))?
                .name();

            let mut attr_overrides = self.ak_attr_overrides.clone();
            if attr_overrides
                .insert(
                    "ancillary_variables".to_string(),
                    extrap_flag_varname.as_str().into(),
                )
                .is_some()
            {
                log::warn!("The 'ancillary_variables' attribute cannot be overridden for public variable {public_ak_name}")
            }

            copy_variable_new_data(
                public_file,
                &ak_var,
                &public_ak_name,
                expanded_aks.into_dyn().view(),
                vec![TIME_DIM_NAME.to_string(), level_dim_name],
                &format!("{} averaging kernel", self.gas_long),
                &attr_overrides,
                &attr_to_remove,
            )?;

            write_extrapolation_flags(
                public_file,
                &public_ak_name,
                &extrap_flag_varname,
                ak_extrap_flags.view(),
            )?;

            ancillary_vars.push_str(&public_ak_name);
            ancillary_vars.push(' ');
        }

        let ancillary_vars = ancillary_vars.trim_end();
        let mut public_xgas_var = public_file.variable_mut(public_xgas_name)
            .expect("public Xgas variable must exist - it should have been created earlier in this function");
        public_xgas_var
            .put_attribute("ancillary_variables", ancillary_vars)
            .change_context_lazy(|| {
                CopyError::context(format!(
                    "updating the 'ancillary_variables' attribute for {public_xgas_name}"
                ))
            })?;

        Ok(())
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum XgasAncillary {
    /// Infer which private variable to copy from the Xgas variable name
    Inferred,
    /// Infer which private variable to copy from the Xgas variable name,
    /// but do not copy if that variable has already been copied to the
    /// public file.
    InferredIfFirst,
    /// Infer which private variable to copy from the Xgas variable name,
    /// but do not copy if that variable has already been copied OR it does
    /// not exist in the private file.
    OptInferredIfFirst,
    /// Copy the specified private variable (the `XgasCopy` instance will assign the correct public name)
    Specified {
        private_name: String,
        public_name: Option<String>,
    },
    /// Assume that another Xgas will provide the necessary variable
    SpecifiedIfFirst {
        private_name: String,
        public_name: Option<String>,
    },
    /// Do not create this ancillary variable
    Omit,
}

impl From<AncillaryDiscoveryMethod> for XgasAncillary {
    fn from(value: AncillaryDiscoveryMethod) -> Self {
        match value {
            AncillaryDiscoveryMethod::Inferred => Self::Inferred,
            AncillaryDiscoveryMethod::InferredIfFirst => Self::InferredIfFirst,
            AncillaryDiscoveryMethod::OptInferredIfFirst => Self::OptInferredIfFirst,
            AncillaryDiscoveryMethod::Omit => Self::Omit,
        }
    }
}

impl XgasAncillary {
    /// Get the private and public variable names.
    ///
    /// `infer_names_fxn` must be a closure that takes no arguments and returns
    /// the private and public names as inferred from the Xgas variable that
    /// this ancillary variable supports.
    fn get_var_names<F>(&self, infer_names_fxn: F) -> (String, String)
    where
        F: FnOnce() -> (String, String),
    {
        match self {
            XgasAncillary::Inferred => infer_names_fxn(),
            XgasAncillary::InferredIfFirst => infer_names_fxn(),
            XgasAncillary::OptInferredIfFirst => infer_names_fxn(),
            XgasAncillary::Specified {
                private_name,
                public_name,
            } => {
                let public_name = public_name.as_deref().unwrap_or(&private_name).to_owned();
                (private_name.to_owned(), public_name)
            }
            XgasAncillary::SpecifiedIfFirst {
                private_name,
                public_name,
            } => {
                let public_name = public_name.as_deref().unwrap_or(&private_name).to_owned();
                (private_name.to_owned(), public_name)
            }
            XgasAncillary::Omit => infer_names_fxn(),
        }
    }

    /// Combines `get_var_names` and `do_copy`: if the variables should not be copied,
    /// returns `None`, otherwise returns `Some((private_name, public_name))`.
    /// Note that unlike `get_var_names`, the `infer_names_fxn` closure must be
    /// able to be called repeatedly due to an implementation detail.
    fn get_var_names_opt<F>(
        &self,
        private_file: &netcdf::File,
        public_file: &netcdf::File,
        infer_names_fxn: F,
    ) -> Option<(String, String)>
    where
        F: Fn() -> (String, String),
    {
        if !self.do_copy(private_file, public_file, || infer_names_fxn()) {
            None
        } else {
            Some(self.get_var_names(infer_names_fxn))
        }
    }

    /// Should the private variable be copied?
    /// This checks if the variable should always be copied,
    /// never be copied, or if that depends on whether it was
    /// previously copied,
    fn do_copy<F>(
        &self,
        private_file: &netcdf::File,
        public_file: &netcdf::File,
        infer_names_fxn: F,
    ) -> bool
    where
        F: Fn() -> (String, String),
    {
        match self {
            XgasAncillary::Inferred => true,
            XgasAncillary::InferredIfFirst => {
                let (private_name, public_name) = infer_names_fxn();
                let do_copy = public_file.variable(&public_name).is_none();
                if !do_copy {
                    log::debug!("Not copying variable '{private_name}' as public variable '{public_name}' was already copied");
                }
                do_copy
            }
            XgasAncillary::OptInferredIfFirst => {
                let (private_name, public_name) = infer_names_fxn();
                if private_file.variable(&private_name).is_none() {
                    log::debug!(
                        "Optional private variable '{private_name}' does not exist, so not copying"
                    );
                    return false;
                }
                let do_copy = public_file.variable(&public_name).is_none();
                if !do_copy {
                    log::debug!("Not copying variable '{private_name}' as public variable '{public_name}' was already copied");
                }
                do_copy
            }
            XgasAncillary::Specified {
                private_name: _,
                public_name: _,
            } => true,
            XgasAncillary::SpecifiedIfFirst {
                private_name,
                public_name,
            } => {
                let public_name = public_name.as_deref().unwrap_or(&private_name);
                let do_copy = public_file.variable(public_name).is_none();
                if !do_copy {
                    log::debug!("Not copying variable '{private_name}' as public variable '{public_name}' was already copied");
                }
                do_copy
            }
            XgasAncillary::Omit => false,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ComputedVariable {
    PriorSource { public_name: Option<String> },
}

impl CopySet for ComputedVariable {
    fn copy(
        &self,
        private_file: &netcdf::File,
        public_file: &mut netcdf::FileMut,
        time_subsetter: &Subsetter,
    ) -> error_stack::Result<(), CopyError> {
        match self {
            ComputedVariable::PriorSource { public_name } => {
                let pubname = public_name.as_deref().unwrap_or("apriori_data_source");
                add_geos_version_variable(private_file, public_file, pubname, time_subsetter)
            }
        }
    }
}

pub(crate) fn copy_attributes(
    private_file: &netcdf::File,
    public_file: &mut netcdf::FileMut,
    attributes: &[CopyGlobalAttr],
) -> error_stack::Result<(), CopyError> {
    add_history_attr(private_file, public_file)?;
    for attr in attributes {
        attr.copy(private_file, public_file)?;
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CopyGlobalAttr {
    MustCopy { name: String },
    CopyIfPresent { name: String },
}

impl CopyGlobalAttr {
    fn copy(
        &self,
        private_file: &netcdf::File,
        public_file: &mut netcdf::FileMut,
    ) -> error_stack::Result<(), CopyError> {
        let (attr_name, must_copy) = match self {
            CopyGlobalAttr::MustCopy { name } => (name, true),
            CopyGlobalAttr::CopyIfPresent { name } => (name, false),
        };

        let opt_attr = private_file.attribute(&attr_name);
        if let Some(attr) = opt_attr {
            let value = attr.value().change_context_lazy(|| {
                CopyError::context(format!("reading global attribute {attr_name}"))
            })?;

            public_file
                .add_attribute(&attr_name, value)
                .change_context_lazy(|| {
                    CopyError::context(format!("writing global attribute {attr_name}"))
                })?;
        } else if must_copy {
            return Err(CopyError::missing_req_attr("/", attr_name).into());
        }

        Ok(())
    }
}

fn add_history_attr(
    private_file: &netcdf::File,
    public_file: &mut netcdf::FileMut,
) -> error_stack::Result<(), CopyError> {
    let mut history = if private_file.attribute("history").is_none() {
        "".to_string()
    } else {
        let mut s = get_root_string_attr(private_file, "history")?;
        s.push('\n');
        s
    };

    let priv_name = private_file
        .path()
        .change_context_lazy(|| CopyError::context("getting path to the private file"))?;
    let priv_name = priv_name
        .file_name()
        .ok_or_else(|| CopyError::custom("Could not get file base name of the private file"))?
        .to_string_lossy();
    let program_version = env!("CARGO_PKG_VERSION");
    let now = chrono::Utc::now();
    history.push_str(&format!(
        "{}: generated public file from private/engineering file {priv_name} with {PROGRAM_NAME} from GGG-RS v{program_version}",
        now.format("%Y-%m-%d %H:%M:%S %Z")
    ));
    public_file
        .add_attribute("history", history)
        .change_context_lazy(|| CopyError::context("writing global attribute 'history'"))?;
    Ok(())
}

// ---------------- //
// HELPER FUNCTIONS //
// ---------------- //

pub(crate) fn de_attribute_overrides<'de, D>(
    deserializer: D,
) -> Result<IndexMap<String, AttributeValue>, D::Error>
where
    D: Deserializer<'de>,
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
        let toml_str = r#"private_name = "time"
        long_name = "zero path difference time"
        "#;
        let aux_de: AuxVarCopy = toml::from_str(toml_str).expect("deserialization should work");
        let aux_val = AuxVarCopy::new("time", "zero path difference time", true);
        assert_eq!(aux_de, aux_val);
    }

    #[test]
    fn test_de_aux_var_pub_name() {
        let toml_str = r#"private_name = "year"
        long_name = "year"
        public_name = "decimal_year"
        "#;
        let aux_de: AuxVarCopy = toml::from_str(toml_str).expect("deserialization should work");
        let aux_val = AuxVarCopy::new("year", "year", true).with_public_name("decimal_year");
        assert_eq!(aux_de, aux_val);
    }

    #[test]
    fn test_de_aux_var_attrs() {
        let toml_str = r#"private_name = "day"
        long_name = "day of year"
        attr_overrides = {units = "Julian day", description = "1-based day of year"}
        attr_to_remove = ["vmin", "vmax"]
        "#;
        let aux_de: AuxVarCopy = toml::from_str(toml_str).expect("deserialization should work");

        let aux_val = AuxVarCopy::new_keep_attrs("day", "day of year", true)
            .with_attr_override("units", "Julian day")
            .with_attr_override("description", "1-based day of year")
            .with_attr_remove("vmin")
            .with_attr_remove("vmax");

        assert_eq!(aux_de, aux_val);
    }

    #[test]
    fn test_de_aux_var_not_req() {
        let toml_str = r#"private_name = "hour"
        long_name = "UTC hour"
        required = false
        "#;
        let aux_de: AuxVarCopy = toml::from_str(toml_str).expect("deserialization should work");

        let aux_val = AuxVarCopy::new("hour", "UTC hour", false);
        assert_eq!(aux_de, aux_val);
    }

    #[test]
    fn test_de_xgas_anc_inferred() {
        let toml_str = r#"type = "inferred""#;
        let anc_de: XgasAncillary = toml::from_str(toml_str).expect("deserialization should work");
        assert_eq!(anc_de, XgasAncillary::Inferred);
    }

    #[test]
    fn test_de_xgas_anc_inferred_if_first() {
        let toml_str = r#"type = "inferred_if_first""#;
        let anc_de: XgasAncillary = toml::from_str(toml_str).expect("deserialization should work");
        assert_eq!(anc_de, XgasAncillary::InferredIfFirst);
    }

    #[test]
    fn test_de_xgas_anc_specified() {
        let toml_str = r#"type = "specified"
        private_name = "prior_xco2""#;
        let anc_de: XgasAncillary = toml::from_str(toml_str).expect("deserialization should work");
        assert_eq!(
            anc_de,
            XgasAncillary::Specified {
                private_name: "prior_xco2".to_string(),
                public_name: None
            }
        );

        let toml_str = r#"type = "specified"
        private_name = "prior_1co2"
        public_name = "prior_co2"
        "#;
        let anc_de: XgasAncillary = toml::from_str(toml_str).expect("deserialization should work");
        assert_eq!(
            anc_de,
            XgasAncillary::Specified {
                private_name: "prior_1co2".to_string(),
                public_name: Some("prior_co2".to_string())
            }
        );
    }

    #[test]
    fn test_de_xgas_anc_specified_if_first() {
        let toml_str = r#"type = "specified_if_first"
        private_name = "prior_xco2""#;
        let anc_de: XgasAncillary = toml::from_str(toml_str).expect("deserialization should work");
        assert_eq!(
            anc_de,
            XgasAncillary::SpecifiedIfFirst {
                private_name: "prior_xco2".to_string(),
                public_name: None
            }
        );

        let toml_str = r#"type = "specified_if_first"
        private_name = "prior_1co2"
        public_name = "prior_co2"
        "#;
        let anc_de: XgasAncillary = toml::from_str(toml_str).expect("deserialization should work");
        assert_eq!(
            anc_de,
            XgasAncillary::SpecifiedIfFirst {
                private_name: "prior_1co2".to_string(),
                public_name: Some("prior_co2".to_string())
            }
        );
    }

    #[test]
    fn test_de_xgas_anc_omit() {
        let toml_str = r#"type = "omit""#;
        let anc_de: XgasAncillary = toml::from_str(toml_str).expect("deserialization should work");
        assert_eq!(anc_de, XgasAncillary::Omit);
    }

    #[test]
    fn test_de_xgas_simple() {
        let toml_str = r#"xgas = "xco2"
        gas = "co2"
        gas_long = "carbon dioxide"
        "#;

        let xgas_de: XgasCopy = toml::from_str(toml_str).expect("deserialization should not fail");
        let xgas_expected = XgasCopy::new("xco2", "co2", "carbon dioxide");
        assert_eq!(xgas_de, xgas_expected);
    }

    #[test]
    fn test_de_xgas_full() {
        let toml_str = r#"xgas = "xco2"
        gas = "co2"
        gas_long = "carbon dioxide"
        prior_profile = { type = "specified_if_first", private_name = "prior_1co2", public_name = "prior_co2" }
        prior_xgas = { type = "specified_if_first", private_name = "prior_xco2_x2019", public_name = "prior_xco2" }
        ak = { type = "specified_if_first", private_name = "ak_xco2" }
        slant_bin = { type = "specified", private_name = "ak_slant_xco2_bin" }
        "#;

        let xgas_de: XgasCopy = toml::from_str(toml_str).expect("deserialization should not fail");
        let mut xgas_expected = XgasCopy::new("xco2", "co2", "carbon dioxide");
        xgas_expected.prior_profile = XgasAncillary::SpecifiedIfFirst {
            private_name: "prior_1co2".to_string(),
            public_name: Some("prior_co2".to_string()),
        };
        xgas_expected.prior_xgas = XgasAncillary::SpecifiedIfFirst {
            private_name: "prior_xco2_x2019".to_string(),
            public_name: Some("prior_xco2".to_string()),
        };
        xgas_expected.ak = XgasAncillary::SpecifiedIfFirst {
            private_name: "ak_xco2".to_string(),
            public_name: None,
        };
        xgas_expected.slant_bin = XgasAncillary::Specified {
            private_name: "ak_slant_xco2_bin".to_string(),
            public_name: None,
        };

        assert_eq!(xgas_de, xgas_expected);
    }

    #[test]
    fn test_de_xgas_omits() {
        let toml_str = r#"xgas = "xluft"
            gas = "luft"
            gas_long = "dry air"
            prior_profile = { type = "omit" }
            prior_xgas = { type = "omit" }
            ak = { type = "omit" }"#;
        let xgas_de: XgasCopy = toml::from_str(toml_str).expect("deserialization should not fail");
        let mut xgas_expected = XgasCopy::new("xluft", "luft", "dry air");
        xgas_expected.prior_profile = XgasAncillary::Omit;
        xgas_expected.prior_xgas = XgasAncillary::Omit;
        xgas_expected.ak = XgasAncillary::Omit;
        assert_eq!(xgas_de, xgas_expected);
    }
}
