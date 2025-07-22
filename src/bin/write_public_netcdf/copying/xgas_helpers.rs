//! High level helper functions for Xgas ancillary variables.
use std::ops::Mul;

use error_stack::ResultExt;
use ggg_rs::{
    nc_utils,
    units::{dmf_conv_factor, UnknownUnitError},
};
use ndarray::{Array1, Array2, ArrayD, ArrayView1, Ix1, Ix2};
use netcdf::{Extents, NcTypeDescriptor};
use num_traits::Zero;

use crate::{copying::copy_utils::NcChar, TIME_DIM_NAME};

use super::{
    copy_utils::{chars_to_string, read_and_subset_req_var},
    find_subset_dim, get_string_attr, CopyError, Subsetter,
};

pub(super) fn update_xgas_description(
    xgas_var: &netcdf::Variable,
    gas: &str,
) -> error_stack::Result<netcdf::AttributeValue, CopyError> {
    let curr_descr = get_string_attr(xgas_var, "description").change_context_lazy(|| {
        CopyError::context(format!("updating the description for {}", xgas_var.name()))
    })?;

    // In my test file, the capitalization of column_o2 was not consistent. We want to keep
    // the capitalization of any extra description, hence the use of an index from the lowercased
    // string to do the split.
    let lower_desc = curr_descr.to_ascii_lowercase();
    let split_on = "/column_o2";
    let extra_desc = if let Some(i) = lower_desc.find(split_on) {
        let iextra = i + split_on.len();
        curr_descr[iextra..].trim()
    } else {
        return Ok(curr_descr.into());
    };

    if extra_desc.is_empty() {
        Ok(format!("o2_mean_mole_fraction * column_{gas} / column_o2").into())
    } else {
        Ok(format!("o2_mean_mole_fraction * column_{gas} / column_o2, {extra_desc}").into())
    }
}

pub(super) fn convert_dmf_array<T>(
    mut data: ArrayD<T>,
    orig_unit: &str,
    target_unit: &str,
) -> Result<ArrayD<T>, UnknownUnitError>
where
    T: Copy + Zero + NcTypeDescriptor + Mul<Output = T> + From<f32>,
{
    // Only do a conversion if the units are different. This saves some
    // multiplying and avoids any weird floating point error
    if orig_unit != target_unit {
        let conv_factor = dmf_conv_factor(&orig_unit, target_unit)?;
        data.mapv_inplace(|el| el * T::from(conv_factor));
    }
    Ok(data)
}

pub(super) fn expand_slant_xgas_binned_aks_from_file(
    private_file: &netcdf::File,
    xgas_varname: &str,
    airmass_varname: &str,
    ak_varname: &str,
    slant_bin_varname: &str,
    time_subsetter: &Subsetter,
    nsamples: Option<usize>,
) -> error_stack::Result<(Array2<f32>, Array1<i8>), CopyError> {
    // Read in all the variables we need from the private file
    let xgas = read_and_subset_req_var::<f32, Ix1>(private_file, xgas_varname, time_subsetter)?;
    let airmass =
        read_and_subset_req_var::<f32, Ix1>(private_file, airmass_varname, time_subsetter)?;
    let aks = read_and_subset_req_var::<f32, Ix2>(private_file, ak_varname, time_subsetter)?;
    let slant_xgas_bins =
        read_and_subset_req_var::<f32, Ix1>(private_file, slant_bin_varname, time_subsetter)?;

    // We need the Xgas-related units too
    let xgas_var = private_file
        .variable(xgas_varname)
        .expect("should be able to get Xgas variable as we previously read data from it");
    let xgas_units = get_string_attr(&xgas_var, "units").change_context_lazy(|| {
        CopyError::context(format!("getting {xgas_varname} units for AK expansion"))
    })?;

    let slant_bin_var = private_file
        .variable(slant_bin_varname)
        .expect("should be able to get slant Xgas variable as we previously read data from it");
    let slant_bin_units = get_string_attr(&slant_bin_var, "units").change_context_lazy(|| {
        CopyError::context(format!(
            "getting {slant_bin_varname} units for AK expansion"
        ))
    })?;

    let slant_xgas_values = xgas * airmass;
    let (expanded_aks, extrap_flags) = nc_utils::expand_slant_xgas_binned_aks(
        slant_xgas_values.view(),
        &xgas_units,
        slant_xgas_bins,
        &slant_bin_units,
        aks.view(),
        nsamples,
    )
    .change_context_lazy(|| CopyError::custom(format!("expanding '{ak_varname}'")))?;

    Ok((expanded_aks, extrap_flags))
}

pub(super) fn expand_prior_profiles_from_file(
    private_file: &netcdf::File,
    prior_varname: &str,
    prior_index_varname: &str,
    target_unit: Option<&str>,
    time_subsetter: &Subsetter,
) -> error_stack::Result<ArrayD<f32>, CopyError> {
    // Get the compact prior profiles and convert them to the target units
    let prior_var = private_file
        .variable(prior_varname)
        .ok_or_else(|| CopyError::MissingReqVar(prior_varname.to_string()))?;
    let prior_data = prior_var.get(Extents::All).change_context_lazy(|| {
        CopyError::context(format!(
            "getting data for prior profile variable '{prior_varname}'"
        ))
    })?;
    let prior_unit = get_string_attr(&prior_var, "units")
        .change_context_lazy(|| CopyError::context("getting units for priors during expansion"))?;
    let prior_data = if let Some(unit) = target_unit {
        convert_dmf_array(prior_data, &prior_unit, unit).change_context_lazy(|| {
            CopyError::context(format!(
                "converting prior profile variable '{prior_varname}' units"
            ))
        })?
    } else {
        prior_data
    };

    // Get the prior index. Read it as a u32 so that the later conversion to usize is less likely to fail
    // (e.g., on 32-bit systems).
    let prior_index_var = private_file
        .variable(prior_index_varname)
        .ok_or_else(|| CopyError::MissingReqVar(prior_index_varname.to_string()))?;
    let prior_index = prior_index_var
        .get::<u32, _>(Extents::All)
        .change_context_lazy(|| {
            CopyError::context(format!(
                "getting data for prior index variable '{prior_index_varname}'"
            ))
        })?
        .mapv(|v| v as usize);

    let prior_index = if let Some(idim) = find_subset_dim(&prior_index_var, TIME_DIM_NAME) {
        time_subsetter.subset_nd_array(prior_index.view(), idim)?
    } else {
        prior_index
    };

    // Expand the array
    let prior_index = prior_index
        .into_dimensionality::<Ix1>()
        .change_context_lazy(|| {
            CopyError::context(format!(
                "converting prior index '{prior_index_varname}' to 1D"
            ))
        })?;
    let expanded_priors = nc_utils::expand_priors(prior_data.view(), prior_index.view())
        .change_context_lazy(|| {
            CopyError::context(format!("expanding prior variable '{prior_varname}'"))
        })?;

    Ok(expanded_priors)
}

pub(super) fn write_extrapolation_flags(
    public_file: &mut netcdf::FileMut,
    ak_varname: &str,
    extrap_flag_varname: &str,
    extrap_flags: ArrayView1<i8>,
) -> error_stack::Result<(), CopyError> {
    let flag_values = vec![-2i8, -1i8, 0i8, 1i8, 2i8];
    let flag_meanings = r#"clamped_to_min_slant_xgas
extrapolated_below_lowest_slant_xgas_bin
interpolated_normally
extrapolated_above_largest_slant_xgas_bin
clamped_to_max_slant_xgas"#;
    let flag_usage =
        "Please see https://tccon-wiki.caltech.edu/Main/GGG2020DataChanges for more information";

    let mut var = public_file
        .add_variable::<i8>(&extrap_flag_varname, &[TIME_DIM_NAME])
        .change_context_lazy(|| {
            CopyError::context(format!("adding variable '{extrap_flag_varname}'"))
        })?;

    var.put_attribute("long_name", format!("{ak_varname} extrapolation flags"))
        .change_context_lazy(|| {
            CopyError::context(format!(
                "adding 'long_name' attribute to variable '{extrap_flag_varname}'"
            ))
        })?;
    var.put_attribute("flag_values", flag_values)
        .change_context_lazy(|| {
            CopyError::context(format!(
                "adding 'flag_values' attribute to variable '{extrap_flag_varname}'"
            ))
        })?;
    var.put_attribute("flag_meanings", flag_meanings)
        .change_context_lazy(|| {
            CopyError::context(format!(
                "adding 'flag_meanings' attribute to variable '{extrap_flag_varname}'"
            ))
        })?;
    var.put_attribute("usage", flag_usage)
        .change_context_lazy(|| {
            CopyError::context(format!(
                "adding 'meanings' attribute to variable '{extrap_flag_varname}'"
            ))
        })?;

    var.put(extrap_flags, Extents::All)
        .change_context_lazy(|| {
            CopyError::context(format!("writing data for variable '{extrap_flag_varname}'"))
        })?;
    Ok(())
}

pub(super) fn get_traceability_scale(
    private_file: &netcdf::File,
    scale_varname: &str,
) -> error_stack::Result<String, CopyError> {
    let scale_var = private_file
        .variable(scale_varname)
        .ok_or_else(|| CopyError::MissingReqVar(scale_varname.to_string()))?;
    // In the GGG2020.1 private files, these variables should be characters (not strings),
    // we also aren't subsetting by time because this *should* be the same for all spectra.
    let scale_chars = scale_var
        .get::<NcChar, _>(Extents::All)
        .change_context_lazy(|| {
            CopyError::context(format!(
                "getting traceability scale variable '{scale_varname}' data"
            ))
        })?
        .into_dimensionality::<Ix2>()
        .change_context_lazy(|| {
            CopyError::context(format!(
                "converting traceability scale variable '{scale_varname}' to 2D"
            ))
        })?;

    // Check that all slices match the first one - we require that all of the spectra are on the same scale to collapse it into
    // an attribute.
    let (nspec, _) = scale_chars.dim();
    if nspec < 1 {
        return Err(CopyError::custom(format!(
            "Traceability scale variable '{scale_varname}' is length 0 along the first dimension"
        ))
        .into());
    }

    let scale_bytes = scale_chars.row(0);
    for (i, r) in scale_chars.rows().into_iter().enumerate() {
        if scale_bytes != r {
            return Err(CopyError::inconsistent_value(scale_varname, 0, i).into());
        }
    }
    let scale = chars_to_string(scale_bytes.view());
    Ok(scale)
}
