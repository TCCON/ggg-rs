//! Intermediate level utility functions to support copying variables
use error_stack::ResultExt;
use ndarray::{Array, Dimension};
use netcdf::{Extents, NcTypeDescriptor};
use num_traits::Zero;

use crate::TIME_DIM_NAME;

use super::{CopyError, Subsetter};


pub(super) fn read_and_subset_req_var<T: NcTypeDescriptor + Copy + Zero, D: Dimension>(
    file: &netcdf::File,
    varname: &str,
    time_subsetter: &Subsetter
) -> error_stack::Result<Array<T, D>, CopyError> {
    let var = file.variable(varname)
        .ok_or_else(|| CopyError::MissingReqVar(varname.to_string()))?;
    
    let arr = var.get::<T, _>(Extents::All)
        .change_context_lazy(|| CopyError::context(format!("reading variable '{varname}'")))?;
    
    let arr = if let Some(idim) = find_subset_dim(&var, TIME_DIM_NAME) {
        time_subsetter.subset_nd_array(arr.view(), idim)
            .change_context_lazy(|| CopyError::context("subsetting '{varname}'"))?
    } else {
        arr
    };

    let arr = arr.into_dimensionality::<D>()
        .change_context_lazy(|| CopyError::context(format!("converting variable '{varname}' dimensionality")))?;
    Ok(arr)
}


pub(super) fn get_string_attr(var: &netcdf::Variable, attr: &str) -> error_stack::Result<String, CopyError> {
    let res: Result<String, _> = var
        .attribute_value(attr)
        .ok_or_else(|| CopyError::missing_req_attr(var.name(), attr))?
        .change_context_lazy(|| CopyError::context(format!("could not read '{attr}' attribute on {}", var.name())))?
        .try_into();
    res.change_context_lazy(|| CopyError::context(format!("could not convert '{attr}' attribute on {} into a string", var.name())))
}

pub(super) fn find_subset_dim(var: &netcdf::Variable, dimname: &str) -> Option<usize> {
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


/// Check if the dimensions named in `private_var` exist in `public_file`,
/// if not, create them. Note that they are created with the same length,
/// so if you need a different length (e.g., like "time" does because of
/// subsetting), best if you create those dimensions before copying any variables.
pub(super) fn add_needed_dims(public_file: &mut netcdf::FileMut, private_var: &netcdf::Variable) -> error_stack::Result<(), CopyError> {
    for var_dim in private_var.dimensions() {
        if !check_dim_exists(var_dim, public_file, &private_var.name())? {
            public_file.add_dimension(&var_dim.name(), var_dim.len())
            .change_context_lazy(|| CopyError::context(format!("creating dimension '{}'", var_dim.name())))?;
        }
    }
    Ok(())
}

pub(super) fn add_needed_new_dims<S: AsRef<str>>(public_file: &mut netcdf::FileMut, private_var: &netcdf::Variable, dimnames: &[S]) -> error_stack::Result<(), CopyError> {
    for dim in private_var.dimensions() {
        if dimnames.iter().any(|n| n.as_ref() == dim.name().as_str()) && !check_dim_exists(dim, public_file, &private_var.name())? {
            public_file.add_dimension(&dim.name(), dim.len())
            .change_context_lazy(|| CopyError::context(format!("creating dimension '{}'", dim.name())))?;
        }
    }
    Ok(())
}

/// Return `true` if `var_dim` exists in `public_file`, `false` otherwise.
/// Also checks that the lengths are equal for variables that already exist.
/// `varname` is only used in an error message for clarity.
/// 
/// Note: "time" is assumed to always exist, since it is subset in the public files.
pub(super) fn check_dim_exists(var_dim: &netcdf::Dimension, public_file: &netcdf::File, varname: &str) -> Result<bool, CopyError> {
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