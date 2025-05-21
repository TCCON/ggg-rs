//! High level helpers to copy variables for specific use cases
use std::ops::Mul;

use error_stack::ResultExt;
use ggg_rs::nc_utils::NcArray;
use indexmap::IndexMap;
use itertools::Itertools;
use ndarray::ArrayViewD;
use netcdf::{AttributeValue, Extents, NcTypeDescriptor};
use num_traits::Zero;

use crate::TIME_DIM_NAME;

use super::{
    add_needed_dims, add_needed_new_dims, convert_dmf_array, find_subset_dim, get_string_attr,
    CopyError, Subsetter,
};

/// Helper function that copies a variable with mole fraction data.
/// This ensures that the units match `target_unit`, which should
/// normally be the unit that the Xgas values are in.
pub(super) fn copy_vmr_variable_from_dset<
    T: Copy + Zero + NcTypeDescriptor + Mul<Output = T> + From<f32>,
    S: AsRef<str>,
>(
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
    log::debug!(
        "Copying private variable '{private_varname}' to public variable '{public_varname}'"
    );
    let private_var = private_file
        .variable(private_varname)
        .ok_or_else(|| CopyError::MissingReqVar(private_varname.to_string()))?;
    let var_unit = get_string_attr(&private_var, "units").change_context_lazy(|| {
        CopyError::context(format!(
            "getting units for {private_varname} to scale to the primary Xgas variable unit"
        ))
    })?;
    let var_unit = if var_unit.is_empty() {
        log::info!(
            "Units for {private_varname} were an empty string, assuming this should be unscaled mole fraction",
        );
        "parts"
    } else {
        &var_unit
    };

    let data = private_var
        .get::<T, _>(Extents::All)
        .change_context_lazy(|| {
            CopyError::context(format!("reading variable '{private_varname}'"))
        })?;
    let do_subset_along = find_subset_dim(&private_var, TIME_DIM_NAME);
    let data = if let Some(idim) = do_subset_along {
        time_subsetter.subset_nd_array(data.view(), idim)?
    } else {
        data
    };

    let data = convert_dmf_array(data, &var_unit, target_unit)
           .change_context_lazy(|| CopyError::context(format!("getting conversion factor for {private_varname} to scale to the primary Xgas variable unit")))?;
    if attr_overrides
        .insert("units".to_string(), target_unit.into())
        .is_some()
    {
        log::warn!(
            "The 'units' attribute cannot be overridden for public variable {public_varname}"
        )
    }

    let mut public_var =
        copy_var_pre_write_helper::<T>(public_file, &private_var, public_varname, None)?;
    public_var
        .put(data.view(), Extents::All)
        .change_context_lazy(|| {
            CopyError::context(format!("writing variable '{public_varname}'"))
        })?;

    copy_var_attr_write_helper(
        &private_var,
        &mut public_var,
        long_name,
        &attr_overrides,
        attr_to_remove,
    )?;

    Ok(())
}

pub(super) fn copy_variable_new_data<S: AsRef<str>>(
    public_file: &mut netcdf::FileMut,
    private_var: &netcdf::Variable,
    public_varname: &str,
    data: ArrayViewD<f32>,
    dims: Vec<String>,
    long_name: &str,
    attr_overrides: &IndexMap<String, AttributeValue>,
    attr_to_remove: &[S],
) -> error_stack::Result<(), CopyError> {
    log::debug!(
        "Transforming private variable '{}' into public variable '{public_varname}'",
        private_var.name()
    );
    let mut public_var =
        copy_var_pre_write_helper::<f32>(public_file, private_var, public_varname, Some(dims))?;
    public_var.put(data, Extents::All).change_context_lazy(|| {
        CopyError::context(format!("writing variable '{public_varname}'"))
    })?;
    copy_var_attr_write_helper(
        private_var,
        &mut public_var,
        long_name,
        attr_overrides,
        attr_to_remove,
    )?;
    Ok(())
}

/// Helper function to copy variable data generically. Unlike `copy_vmr_variable_from_dset`,
/// this does not need to know the variable type ahead of time.
pub(super) fn copy_variable_general<S: AsRef<str>>(
    public_file: &mut netcdf::FileMut,
    private_var: &netcdf::Variable,
    public_varname: &str,
    time_subsetter: &Subsetter,
    long_name: &str,
    attr_overrides: &IndexMap<String, AttributeValue>,
    attr_to_remove: &[S],
) -> error_stack::Result<(), CopyError> {
    let private_varname = private_var.name();
    log::debug!(
        "Copying private variable '{private_varname}' to public variable '{public_varname}'"
    );

    let generic_array = NcArray::get_from(private_var).change_context_lazy(|| {
        CopyError::context(format!("copying variable '{private_varname}'"))
    })?;
    // Find the time dimension, assuming it does not occur more than once.
    let do_subset_along = find_subset_dim(private_var, TIME_DIM_NAME);
    let generic_array = if let Some(idim) = do_subset_along {
        time_subsetter.subset_generic_array(&generic_array, idim)?
    } else {
        generic_array
    };

    let mut public_var = match generic_array {
        NcArray::I8(arr) => {
            let mut pubv =
                copy_var_pre_write_helper::<i8>(public_file, private_var, public_varname, None)?;
            pubv.put(arr.view(), Extents::All).change_context_lazy(|| {
                CopyError::context(format!("writing variable '{public_varname}'"))
            })?;
            pubv
        }
        NcArray::I16(arr) => {
            let mut pubv =
                copy_var_pre_write_helper::<i16>(public_file, private_var, public_varname, None)?;
            pubv.put(arr.view(), Extents::All).change_context_lazy(|| {
                CopyError::context(format!("writing variable '{public_varname}'"))
            })?;
            pubv
        }
        NcArray::I32(arr) => {
            let mut pubv =
                copy_var_pre_write_helper::<i32>(public_file, private_var, public_varname, None)?;
            pubv.put(arr.view(), Extents::All).change_context_lazy(|| {
                CopyError::context(format!("writing variable '{public_varname}'"))
            })?;
            pubv
        }
        NcArray::I64(arr) => {
            let mut pubv =
                copy_var_pre_write_helper::<i64>(public_file, private_var, public_varname, None)?;
            pubv.put(arr.view(), Extents::All).change_context_lazy(|| {
                CopyError::context(format!("writing variable '{public_varname}'"))
            })?;
            pubv
        }
        NcArray::U8(arr) => {
            let mut pubv =
                copy_var_pre_write_helper::<u8>(public_file, private_var, public_varname, None)?;
            pubv.put(arr.view(), Extents::All).change_context_lazy(|| {
                CopyError::context(format!("writing variable '{public_varname}'"))
            })?;
            pubv
        }
        NcArray::U16(arr) => {
            let mut pubv =
                copy_var_pre_write_helper::<u16>(public_file, private_var, public_varname, None)?;
            pubv.put(arr.view(), Extents::All).change_context_lazy(|| {
                CopyError::context(format!("writing variable '{public_varname}'"))
            })?;
            pubv
        }
        NcArray::U32(arr) => {
            let mut pubv =
                copy_var_pre_write_helper::<u32>(public_file, private_var, public_varname, None)?;
            pubv.put(arr.view(), Extents::All).change_context_lazy(|| {
                CopyError::context(format!("writing variable '{public_varname}'"))
            })?;
            pubv
        }
        NcArray::U64(arr) => {
            let mut pubv =
                copy_var_pre_write_helper::<u64>(public_file, private_var, public_varname, None)?;
            pubv.put(arr.view(), Extents::All).change_context_lazy(|| {
                CopyError::context(format!("writing variable '{public_varname}'"))
            })?;
            pubv
        }
        NcArray::F32(arr) => {
            let mut pubv =
                copy_var_pre_write_helper::<f32>(public_file, private_var, public_varname, None)?;
            pubv.put(arr.view(), Extents::All).change_context_lazy(|| {
                CopyError::context(format!("writing variable '{public_varname}'"))
            })?;
            pubv
        }
        NcArray::F64(arr) => {
            let mut pubv =
                copy_var_pre_write_helper::<f64>(public_file, private_var, public_varname, None)?;
            pubv.put(arr.view(), Extents::All).change_context_lazy(|| {
                CopyError::context(format!("writing variable '{public_varname}'"))
            })?;
            pubv
        }
        NcArray::Char(arr) => {
            let mut pubv =
                copy_var_pre_write_helper::<u8>(public_file, private_var, public_varname, None)?;
            pubv.put(arr.view(), Extents::All).change_context_lazy(|| {
                CopyError::context(format!("writing variable '{public_varname}'"))
            })?;
            pubv
        }
    };

    copy_var_attr_write_helper(
        private_var,
        &mut public_var,
        long_name,
        attr_overrides,
        attr_to_remove,
    )?;
    Ok(())
}

/// Centralizes the logic before writing data: adds needed dimensions and creates the public variable.
pub(super) fn copy_var_pre_write_helper<'v, T: Copy + Zero + NcTypeDescriptor>(
    public_file: &'v mut netcdf::FileMut,
    private_var: &netcdf::Variable,
    public_varname: &str,
    new_dims: Option<Vec<String>>,
) -> error_stack::Result<netcdf::VariableMut<'v>, CopyError> {
    let dims = if let Some(dims) = new_dims {
        add_needed_new_dims(public_file, private_var, &dims)?;
        dims
    } else {
        let dims = private_var
            .dimensions()
            .iter()
            .map(|dim| dim.name())
            .collect_vec();

        // Create the variable, which needs its dimensions created first.
        // Handling missing dimensions here is easier than trying to collect a list of
        // all dimensions that we need.
        add_needed_dims(public_file, &private_var).change_context_lazy(|| {
            CopyError::context(format!("creating public variable '{public_varname}'"))
        })?;
        dims
    };
    let dims_str = dims.iter().map(|dim| dim.as_str()).collect_vec();

    let mut public_var = public_file
        .add_variable::<T>(public_varname, &dims_str)
        .change_context_lazy(|| {
            CopyError::context(format!("creating public variable '{public_varname}'"))
        })?;
    if dims_str.len() > 1 {
        // Assume that we always want compression on 2D variables. For public files,
        // this is a reasonable assumption, since they will usually be time x level.
        public_var
            .set_compression(9, true)
            .change_context_lazy(|| {
                CopyError::context(format!(
                    "setting compresson on public variable '{public_varname}'"
                ))
            })?;
    }
    Ok(public_var)
}

/// Centralizes logic for attributes: adds "long_name" and copies/writes attributes based
/// on the overrides and `attr_to_remove` values.
pub(super) fn copy_var_attr_write_helper<S: AsRef<str>>(
    private_var: &netcdf::Variable,
    public_var: &mut netcdf::VariableMut,
    long_name: &str,
    attr_overrides: &IndexMap<String, AttributeValue>,
    attr_to_remove: &[S],
) -> error_stack::Result<(), CopyError> {
    let private_varname = private_var.name();
    let public_varname = public_var.name();
    public_var
        .put_attribute("long_name", long_name)
        .change_context_lazy(|| {
            CopyError::context(format!(
                "adding 'long_name' attribute to public variable '{public_varname}'"
            ))
        })?;
    for (att_name, att_value) in attr_overrides.iter() {
        public_var
            .put_attribute(&att_name, att_value.to_owned())
            .change_context_lazy(|| {
                CopyError::context(format!(
                    "adding '{att_name}' attribute to public variable '{public_varname}'"
                ))
            })?;
    }
    for att in private_var.attributes() {
        let att_name = att.name();
        if att_name != "long_name"
            && !attr_overrides.contains_key(att_name)
            && !attr_to_remove.iter().any(|a| a.as_ref() == att_name)
        {
            let att_value = att.value()
                .change_context_lazy(|| CopyError::context(format!("getting original value of attribute '{att_name}' from private variable '{private_varname}'")))?;
            public_var
                .put_attribute(att_name, att_value)
                .change_context_lazy(|| {
                    CopyError::context(format!(
                        "adding '{att_name}' to public variable '{public_varname}'"
                    ))
                })?;
        }
    }
    Ok(())
}
