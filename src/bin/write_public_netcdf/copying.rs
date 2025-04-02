use std::marker::PhantomData;

use indexmap::IndexMap;
use itertools::Itertools;
use ndarray::{Array1, ArrayD, ArrayView1, ArrayViewD, Axis};
use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CopyError {
    #[error("Tried access index {index} on an array dimension with length {array_len}")]
    BadIndex{index: usize, array_len: usize},
    #[error("Tried to subset a zero-dimensional array")]
    ZeroDArray,
}

pub(crate) trait CopySet {
    fn copy(&self, private_file: &netcdf::File, public_file: &mut netcdf::FileMut) -> error_stack::Result<(), CopyError>;
    fn other_vars_req(&self) -> Vec<&str> { vec![] }
}

pub(crate) struct Subsetter {
    keep_inds: Vec<usize>,
}

impl Subsetter {
    fn len(&self) -> usize {
        self.keep_inds.len()
    }

    fn subset_nd_var<T: Copy>(&self, var: ArrayViewD<T>, along_axis: usize) -> Result<ArrayD<T>, CopyError> {
        let mut shape = Vec::from_iter(var.shape().iter().map(|x| *x));
        if shape.len() == 0 {
            return Err(CopyError::ZeroDArray)
        } else {
            shape[0] = self.len();
        }

        let mut out = ArrayD::uninit(shape);
        for (i_out, &i_in) in self.keep_inds.iter().enumerate() {
            let mut out_slice = out.index_axis_mut(Axis(along_axis), i_out);
            let in_slice = var.index_axis(Axis(along_axis), i_in);
            out_slice.assign(&in_slice);
        }
        Ok(out)

    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct AuxVarCopy<T: Copy> {
    /// The variable from the private file to copy.
    private_var: String,

    /// The name to give the variable in the output file. If `None`, the
    /// variable will have the same name as in the private file.
    public_var: Option<String>,

    /// Value to use for the long name attribute.
    long_name: String,

    /// Additional attributes to add, or values to replace private file
    /// attributes.
    attr_overrides: IndexMap<String, netcdf::AttributeValue>,

    /// A list of private attributes to remove.
    attr_to_remove: Vec<String>,

    /// Dummy field to mark the desired data type in the output
    data_type: PhantomData<T>
}

impl<T: Copy> CopySet for AuxVarCopy<T> {
    fn copy(&self, private_file: &netcdf::File, public_file: &mut netcdf::FileMut) -> error_stack::Result<(), CopyError> {
        // Will need to create a variable with the same dimensions, then copy the good subset of values
        // and the attributes.
        todo!()
    }
}

pub(crate) struct XgasCopy<T: Copy> {
    xgas: String,
    prior_profile: XgasAncillary,
    prior_xgas: XgasAncillary,
    ak: XgasAncillary,
    traceability_scale: XgasAncillary,
}

impl<T: Copy> CopySet for XgasCopy<T> {
    fn copy(&self, private_file: &netcdf::File, public_file: &mut netcdf::FileMut) -> error_stack::Result<(), CopyError> {
        // Copy the xgas and its error, get the WMO scale and make it an attribute, copy the prior profile,
        // prior Xgas, and averaging kernels.
        todo!()
    }

    fn other_vars_req(&self) -> Vec<&str> {
        let mut vars = vec![];
        if let XgasAncillary::Shared(v) = &self.prior_profile {
            vars.push(v);
        }
        if let XgasAncillary::Shared(v) = &self.prior_xgas {
            vars.push(v);
        }
        if let XgasAncillary::Shared(v) = &self.ak {
            vars.push(v);
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