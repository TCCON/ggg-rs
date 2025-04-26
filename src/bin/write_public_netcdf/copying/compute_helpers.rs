//! High-level helper functions for computed variables

use std::i8;

use error_stack::ResultExt;
use ggg_rs::nc_utils;
use itertools::Itertools;
use ndarray::{Array1, Ix1, Ix2};
use netcdf::Extents;

use crate::{
    constants::PRIOR_INDEX_VARNAME,
    copying::{copy_utils::chars_to_string, find_subset_dim},
    TIME_DIM_NAME,
};

use super::{copy_utils::NcChar, CopyError, Subsetter};

const FPIT_MET_FPIT_CHM: i8 = 0;
const IT_MET_IT_CHM: i8 = 1;
const FPIT_MET_IT_CHM: i8 = 2;
const NON_STD_PRIORS: i8 = i8::MAX;

const GEOS_FLAGS: [i8; 4] = [
    FPIT_MET_FPIT_CHM,
    IT_MET_IT_CHM,
    FPIT_MET_IT_CHM,
    NON_STD_PRIORS,
];
static GEOS_FLAG_MEANINGS: &'static [&'static str] = &[
    "all a priori from GEOS FP-IT",
    "all a priori from GEOS IT",
    "met a priori from GEOS FP-IT, CO a priori from GEOS IT",
    "non-standard a priori source set",
];

pub(super) fn add_geos_version_variable(
    private_file: &netcdf::File,
    public_file: &mut netcdf::FileMut,
    public_varname: &str,
    time_subsetter: &Subsetter,
) -> error_stack::Result<(), CopyError> {
    let source_flags = make_geos_version_array(private_file, PRIOR_INDEX_VARNAME, time_subsetter)?;

    let mut var = public_file
        .add_variable::<i8>(public_varname, &[TIME_DIM_NAME])
        .change_context_lazy(|| {
            CopyError::context(format!(
                "creating GEOS version variable, '{public_varname}'"
            ))
        })?;
    var.set_fill_value(i8::MIN).change_context_lazy(|| {
        CopyError::context(format!(
            "setting fill value for GEOS version variable, '{public_varname}'"
        ))
    })?;

    // Attributes
    var.put_attribute("long_name", "a priori data source")
        .change_context_lazy(|| {
            CopyError::context(format!(
                "adding 'long_name' attribute to GEOS version variable, '{public_varname}'"
            ))
        })?;

    var.put_attribute(
        "usage",
        "Used to identify different combination of meteorological and chemical a priori information used in the retrieval. See https://tccon-wiki.caltech.edu/Main/GGG2020DataChanges for additional information."
    ).change_context_lazy(|| CopyError::context(format!(
        "adding 'usage' attribute to GEOS version variable, '{public_varname}'"
    )))?;

    var.put_attribute("flag_values", Vec::from_iter(GEOS_FLAGS.iter().copied()))
        .change_context_lazy(|| {
            CopyError::context(format!(
                "adding 'flag_values' attribute to GEOS version variable, '{public_varname}'"
            ))
        })?;

    var.put_attribute("flag_meanings", GEOS_FLAG_MEANINGS)
        .change_context_lazy(|| {
            CopyError::context(format!(
                "adding 'flag_meanings' attribute to GEOS version variable, '{public_varname}'"
            ))
        })?;

    // Write data
    var.put(source_flags.view(), Extents::All)
        .change_context_lazy(|| {
            CopyError::context(format!(
                "writing data to GEOS version variable '{public_varname}'"
            ))
        })?;

    Ok(())
}

fn make_geos_version_array(
    private_file: &netcdf::File,
    prior_index_varname: &str,
    time_subsetter: &Subsetter,
) -> error_stack::Result<Array1<i8>, CopyError> {
    let met3d_versions =
        get_geos_version_strings(private_file, "met3d", prior_index_varname, time_subsetter)?;
    let met2d_versions =
        get_geos_version_strings(private_file, "met2d", prior_index_varname, time_subsetter)?;
    let chm3d_versions =
        get_geos_version_strings(private_file, "chm3d", prior_index_varname, time_subsetter)?;

    let n = if met2d_versions.len() != met3d_versions.len() {
        return Err(CopyError::custom(
            "GEOS met2d and met3d version variables do not have the same length",
        )
        .into());
    } else if met3d_versions.len() != chm3d_versions.len() {
        return Err(CopyError::custom(
            "GEOS met3d and chm3d version variables do not have the same length",
        )
        .into());
    } else {
        met2d_versions.len()
    };

    let mut prior_source_flags = Array1::<i8>::from_elem([n], i8::MIN);
    for (i, el) in prior_source_flags.iter_mut().enumerate() {
        let m3dv = met3d_versions
            .get(i)
            .expect("met3d_versions should have the same length as the output source flags");
        let m2dv = met2d_versions
            .get(i)
            .expect("met2d_versions should have the same length as the output source flags");
        let c3dv = chm3d_versions
            .get(i)
            .expect("chm3d_versions should have the same length as the output source flags");

        let met_is_fpit = m3dv.starts_with("fpit") && m2dv.starts_with("fpit");
        let chm_is_fpit = c3dv.starts_with("fpit");
        let met_is_it = m3dv.starts_with("it") && m2dv.starts_with("it");
        let chm_is_it = c3dv.starts_with("it");

        if met_is_fpit && chm_is_fpit {
            *el = FPIT_MET_FPIT_CHM;
        } else if met_is_it && chm_is_it {
            *el = IT_MET_IT_CHM;
        } else if met_is_fpit && chm_is_it {
            *el = FPIT_MET_IT_CHM;
        } else {
            *el = NON_STD_PRIORS;
        }
    }

    Ok(prior_source_flags)
}

fn get_geos_version_strings(
    private_file: &netcdf::File,
    geos_key: &str,
    prior_index_varname: &str,
    time_subsetter: &Subsetter,
) -> error_stack::Result<Vec<String>, CopyError> {
    let varname = format!("geos_{geos_key}_version");
    let var = private_file
        .variable(&varname)
        .ok_or_else(|| CopyError::MissingReqVar(varname.clone()))?;
    let chars = var.get::<NcChar, _>(Extents::All).change_context_lazy(|| {
        CopyError::context(format!("getting data from variable '{varname}'"))
    })?;
    let chars = chars.into_dimensionality::<Ix2>().change_context_lazy(|| {
        CopyError::context(format!("converting '{varname}' into a 2D array"))
    })?;

    let prior_index_var = private_file
        .variable(prior_index_varname)
        .ok_or_else(|| CopyError::MissingReqVar(prior_index_varname.to_string()))?;
    // Read the prior index as a u32 so that the later conversion to usize is less likely to fail
    // (e.g., on 32-bit systems)
    let prior_index = prior_index_var
        .get::<u32, _>(Extents::All)
        .change_context_lazy(|| {
            CopyError::context(format!(
                "getting data from variable '{prior_index_varname}'"
            ))
        })?;

    let prior_index = if let Some(idim) = find_subset_dim(&prior_index_var, TIME_DIM_NAME) {
        time_subsetter.subset_nd_array(prior_index.view(), idim)?
    } else {
        prior_index
    };
    let prior_index = prior_index
        .into_dimensionality::<Ix1>()
        .change_context_lazy(|| {
            CopyError::context(format!(
                "converting '{prior_index_varname}' into a 1D array"
            ))
        })?
        .mapv(|v| v as usize);

    let chars =
        nc_utils::expand_priors(chars.view(), prior_index.view()).change_context_lazy(|| {
            CopyError::context(format!("expanding the variable '{varname}'"))
        })?;

    let versions = chars
        .rows()
        .into_iter()
        .map(|r| chars_to_string(r))
        .collect_vec();
    Ok(versions)
}
