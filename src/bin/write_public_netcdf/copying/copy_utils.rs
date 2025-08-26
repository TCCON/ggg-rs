//! Intermediate level utility functions to support copying variables
use std::ops::Add;

use error_stack::ResultExt;
use ndarray::{Array, ArrayView1, Dimension};
use netcdf::{types::NcVariableType, Extents, NcTypeDescriptor};
use num_traits::Zero;

use crate::TIME_DIM_NAME;

use super::{CopyError, Subsetter};

/// Read a required variable that does not need subset to `flag == 0` data.
///
/// If a variable _may_ need subset, it is safe to use [`read_and_subset_req_var`],
/// and that function will identify if the time dimension is present. This function
/// is a convenience for cases where subsetting definitely will not be necessary
/// and you do not have the subsetter available.
pub(super) fn read_req_var<T: NcTypeDescriptor + Copy + Zero, D: Dimension>(
    file: &netcdf::File,
    varname: &str,
) -> error_stack::Result<Array<T, D>, CopyError> {
    let var = file
        .variable(varname)
        .ok_or_else(|| CopyError::MissingReqVar(varname.to_string()))?;

    let arr = var
        .get::<T, _>(Extents::All)
        .change_context_lazy(|| CopyError::context(format!("reading variable '{varname}'")))?;

    let arr = arr.into_dimensionality::<D>().change_context_lazy(|| {
        CopyError::context(format!("converting variable '{varname}' dimensionality"))
    })?;

    Ok(arr)
}

/// Read a required variable that may need subset to `flag == 0` data.
///
/// If the given variable does not have a time dimension, then no subsetting
/// will be performed. Otherwise, subsetting will occur along the time dimension,
/// defined by [`TIME_DIM_NAME`].
///
/// If you need a function to read a variable without a [`Subsetter`] available,
/// use [`read_req_var`].
pub(super) fn read_and_subset_req_var<T: NcTypeDescriptor + Copy + Zero, D: Dimension>(
    file: &netcdf::File,
    varname: &str,
    time_subsetter: &Subsetter,
) -> error_stack::Result<Array<T, D>, CopyError> {
    let var = file
        .variable(varname)
        .ok_or_else(|| CopyError::MissingReqVar(varname.to_string()))?;

    let arr = var
        .get::<T, _>(Extents::All)
        .change_context_lazy(|| CopyError::context(format!("reading variable '{varname}'")))?;

    let arr = if let Some(idim) = find_subset_dim(&var, TIME_DIM_NAME) {
        time_subsetter
            .subset_nd_array(arr.view(), idim)
            .change_context_lazy(|| CopyError::context("subsetting '{varname}'"))?
    } else {
        arr
    };

    let arr = arr.into_dimensionality::<D>().change_context_lazy(|| {
        CopyError::context(format!("converting variable '{varname}' dimensionality"))
    })?;
    Ok(arr)
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
pub(super) fn add_needed_dims(
    public_file: &mut netcdf::FileMut,
    private_var: &netcdf::Variable,
) -> error_stack::Result<(), CopyError> {
    for var_dim in private_var.dimensions() {
        if !check_dim_exists(var_dim, public_file, &private_var.name())? {
            public_file
                .add_dimension(&var_dim.name(), var_dim.len())
                .change_context_lazy(|| {
                    CopyError::context(format!("creating dimension '{}'", var_dim.name()))
                })?;
        }
    }
    Ok(())
}

pub(super) fn add_needed_new_dims<S: AsRef<str>>(
    public_file: &mut netcdf::FileMut,
    private_var: &netcdf::Variable,
    dimnames: &[S],
) -> error_stack::Result<(), CopyError> {
    for dim in private_var.dimensions() {
        if dimnames.iter().any(|n| n.as_ref() == dim.name().as_str())
            && !check_dim_exists(dim, public_file, &private_var.name())?
        {
            public_file
                .add_dimension(&dim.name(), dim.len())
                .change_context_lazy(|| {
                    CopyError::context(format!("creating dimension '{}'", dim.name()))
                })?;
        }
    }
    Ok(())
}

/// Return `true` if `var_dim` exists in `public_file`, `false` otherwise.
/// Also checks that the lengths are equal for variables that already exist.
/// `varname` is only used in an error message for clarity.
///
/// Note: "time" is assumed to always exist, since it is subset in the public files.
pub(super) fn check_dim_exists(
    var_dim: &netcdf::Dimension,
    public_file: &netcdf::File,
    varname: &str,
) -> Result<bool, CopyError> {
    if var_dim.name() == "time" {
        // Special case: time shrinks because we select flag == 0 data, so it
        // will be written at the beginning of the run
        return Ok(true);
    }

    for extant_dim in public_file.dimensions() {
        if extant_dim.name() == var_dim.name() {
            if extant_dim.len() != var_dim.len() {
                return Err(CopyError::dim_len_mismatch(
                    var_dim.name(),
                    varname,
                    extant_dim.len(),
                    var_dim.len(),
                ));
            } else {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Wrapper around unsigned bytes to represent a netCDF character type
///
/// From https://docs.rs/netcdf/0.11.0/netcdf/trait.NcTypeDescriptor.html#char-type,
/// in netCDF v0.11, i8 and u8 are not considered equivalent to an NC_CHAR type.
/// Therefore, to read an NC_CHAR-type variable, we create this structure to
/// hold a byte as a character.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct NcChar(u8);
unsafe impl NcTypeDescriptor for NcChar {
    fn type_descriptor() -> NcVariableType {
        NcVariableType::Char
    }
}

impl From<NcChar> for u8 {
    fn from(value: NcChar) -> Self {
        value.0
    }
}

impl From<&NcChar> for u8 {
    fn from(value: &NcChar) -> Self {
        value.0
    }
}

impl From<&NcChar> for char {
    fn from(value: &NcChar) -> Self {
        char::from(value.0)
    }
}

impl Zero for NcChar {
    fn zero() -> Self {
        Self(0)
    }

    fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

impl Add for NcChar {
    type Output = NcChar;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

pub(super) fn chars_to_string(char_arr: ArrayView1<NcChar>) -> String {
    let byte_it = char_arr.into_iter().map(|c| char::from(c));
    let s = String::from_iter(byte_it);
    s.trim_end_matches('\0').to_string()
}
