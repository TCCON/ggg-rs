use std::{io::Read, path::Path};

use error_stack::ResultExt;
use indexmap::IndexMap;
use interp::interp_slice;
use itertools::Itertools;
use ndarray::{Array1, Array2, ArrayD, ArrayView1, ArrayView2, ArrayViewD};
use netcdf::{
    types::{FloatType, IntType},
    Extents,
};
use num_traits::Zero;
use serde::{de::Error, Deserialize};

use crate::{
    units::{unit_conv_factor, Quantity},
    utils::{GggError, GggNcError},
};

/// A type that can hold a variety of arrays that might be stored
/// in a netCDF file. It is best created by reading from a netCDF
/// variable with its `get_from` method.
pub enum NcArray {
    I8(ArrayD<i8>),
    I16(ArrayD<i16>),
    I32(ArrayD<i32>),
    I64(ArrayD<i64>),
    U8(ArrayD<u8>),
    U16(ArrayD<u16>),
    U32(ArrayD<u32>),
    U64(ArrayD<u64>),
    F32(ArrayD<f32>),
    F64(ArrayD<f64>),
    Char(ArrayD<u8>),
}

impl NcArray {
    /// Retrieve data from a netCDF variable and construct the appropriate variant.
    ///
    /// # Panics
    /// Compound, opaque, enum, and variable length types are not supported, and
    /// may never be, due to their rarity.
    pub fn get_from(var: &netcdf::Variable) -> netcdf::Result<Self> {
        match var.vartype() {
            netcdf::types::NcVariableType::Compound(_) => {
                unimplemented!("reading netCDF Compound types as a generic array")
            }
            netcdf::types::NcVariableType::Opaque(_) => {
                unimplemented!("reading netCDF Opaque types as a generic array")
            }
            netcdf::types::NcVariableType::Enum(_) => {
                unimplemented!("reading netCDF Enum types as a generic array")
            }
            netcdf::types::NcVariableType::Vlen(_) => {
                unimplemented!("reading netCDF variable length types as a generic array")
            }
            netcdf::types::NcVariableType::String => todo!(),
            netcdf::types::NcVariableType::Int(IntType::I8) => {
                let values = var.get::<i8, _>(Extents::All)?;
                Ok(Self::I8(values))
            }
            netcdf::types::NcVariableType::Int(IntType::I16) => {
                let values = var.get::<i16, _>(Extents::All)?;
                Ok(Self::I16(values))
            }
            netcdf::types::NcVariableType::Int(IntType::I32) => {
                let values = var.get::<i32, _>(Extents::All)?;
                Ok(Self::I32(values))
            }
            netcdf::types::NcVariableType::Int(IntType::I64) => {
                let values = var.get::<i64, _>(Extents::All)?;
                Ok(Self::I64(values))
            }
            netcdf::types::NcVariableType::Int(IntType::U8) => {
                let values = var.get::<u8, _>(Extents::All)?;
                Ok(Self::U8(values))
            }
            netcdf::types::NcVariableType::Int(IntType::U16) => {
                let values = var.get::<u16, _>(Extents::All)?;
                Ok(Self::U16(values))
            }
            netcdf::types::NcVariableType::Int(IntType::U32) => {
                let values = var.get::<u32, _>(Extents::All)?;
                Ok(Self::U32(values))
            }
            netcdf::types::NcVariableType::Int(IntType::U64) => {
                let values = var.get::<u64, _>(Extents::All)?;
                Ok(Self::U64(values))
            }
            netcdf::types::NcVariableType::Float(FloatType::F32) => {
                let values = var.get::<f32, _>(Extents::All)?;
                Ok(Self::F32(values))
            }
            netcdf::types::NcVariableType::Float(FloatType::F64) => {
                let values = var.get::<f64, _>(Extents::All)?;
                Ok(Self::F64(values))
            }
            netcdf::types::NcVariableType::Char => {
                let values = var.get::<u8, _>(Extents::All)?;
                Ok(Self::Char(values))
            }
        }
    }

    /// Create a variable in a netCDF group and write this data to it.
    /// Since this writes data, if you need to set options on the variable
    /// that must be done pre-write (e.g., compression), you must match
    /// on this enum's variants and create the variable yourself (for now at least).
    pub fn put_to<'g>(
        &self,
        grp: &'g mut netcdf::GroupMut,
        name: &str,
        dims: &[&str],
    ) -> netcdf::Result<netcdf::VariableMut<'g>> {
        match self {
            NcArray::I8(arr) => {
                let mut var = grp.add_variable::<i8>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            }
            NcArray::I16(arr) => {
                let mut var = grp.add_variable::<i16>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            }
            NcArray::I32(arr) => {
                let mut var = grp.add_variable::<i32>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            }
            NcArray::I64(arr) => {
                let mut var = grp.add_variable::<i64>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            }
            NcArray::U8(arr) => {
                let mut var = grp.add_variable::<u8>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            }
            NcArray::U16(arr) => {
                let mut var = grp.add_variable::<u16>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            }
            NcArray::U32(arr) => {
                let mut var = grp.add_variable::<u32>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            }
            NcArray::U64(arr) => {
                let mut var = grp.add_variable::<u64>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            }
            NcArray::F32(arr) => {
                let mut var = grp.add_variable::<f32>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            }
            NcArray::F64(arr) => {
                let mut var = grp.add_variable::<f64>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            }
            NcArray::Char(arr) => {
                let mut var = grp.add_variable::<u8>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            }
        }
    }
}

// ----------------------------------------- //
// Helper functions for expanding the priors //
// ----------------------------------------- //

/// Convert priors in the condensed format used in GGG2020.0 private
/// files (with one profile per time) to a full set of one profile
/// per spectrum.
///
/// `compact_priors` is the array of priors in the
/// one-per-time format, with `prior_time` as the first dimension.
/// `prior_index` is the array of 0-based indices mapping each prior
/// to the spectra it applies to.
///
/// # Errors
/// A custom [`GggError`] if any prior index is out of bounds for the
/// `compact_priors` array's first dimension.
pub fn expand_priors<T: Zero + Copy>(
    compact_priors: ArrayViewD<T>,
    prior_index: ArrayView1<usize>,
) -> Result<ArrayD<T>, GggError> {
    let ntimes = prior_index.len();
    let n_compact_rows = compact_priors.shape()[0];
    let mut new_shape = vec![ntimes];
    new_shape.extend_from_slice(&compact_priors.shape()[1..]);
    let mut expanded_priors = ArrayD::<T>::zeros(new_shape);
    for (itime, &index) in prior_index.iter().enumerate() {
        if index >= n_compact_rows {
            return Err(GggError::custom(format!(
                "Prior index {index} at position {itime} is out-of-bounds"
            )));
        }
        // .row() panics if the index is out of bounds, hence the check above
        let orig_prof = compact_priors.index_axis(ndarray::Axis(0), index);
        expanded_priors
            .index_axis_mut(ndarray::Axis(0), itime)
            .assign(&orig_prof);
    }
    Ok(expanded_priors)
}

// -------------------------------------- //
// Helper functions for expanding the AKs //
// -------------------------------------- //

/// Expand averaging kernels from the lookup table format to a one-per-spectrum
/// array.
///
/// Parameters:
/// - `slant_xgas_values`: the per-spectrum Xgas times airmass values. When
/// reading from the private files, airmass should be the O2 airmass (usually
/// `o2_7885_am_o2`).
/// - `xgas_units`: the units of the `slant_xgas_values` data, e.g. "ppm" or "ppb".
/// - `slant_xgas_bins`: the slant Xgas values defined for the AK look up table
/// - `bin_units`: the units of `slant_xgas_bins`, e.g., "ppm" or "ppb".
/// - `aks`: the averaging kernels in look up table format
/// - `nsamples`: if `Some(n)`, then the AKs will be quantized to `n` distinct
///    slant Xgas values within the bounds of the slant Xgas bins. This allows
///    the AKs to compress more efficiently in netCDF files.
///
/// # Errors
/// Returns an error if:
/// - custom [`GggError`] it cannot convert the slant Xgas values and bins to consistent units
///   (usually because the unit is not known)
/// - custom [`GggError`] if `slant_xgas_bins` is empty.
pub fn expand_slant_xgas_binned_aks(
    slant_xgas_values: ArrayView1<f32>,
    xgas_units: &str,
    slant_xgas_bins: Array1<f32>,
    bin_units: &str,
    aks: ArrayView2<f32>,
    nsamples: Option<usize>,
) -> Result<(Array2<f32>, Array1<i8>), GggError> {
    let min_extrap_slant = 0.0;
    // First, check that the slant Xgas values and bins are in the same unit; if not, convert
    // the bins, since that should be the smaller array.
    let slant_xgas_bins = if xgas_units == bin_units {
        slant_xgas_bins
    } else {
        let cf = unit_conv_factor(bin_units, xgas_units, Quantity::DMF).map_err(|e| {
            GggError::custom(format!(
                "Error converting AK slant Xgas bins to proper unit: {e}"
            ))
        })?;
        slant_xgas_bins * cf
    };
    let slant_xgas_bins = slant_xgas_bins.as_standard_layout();

    let (min_bin, max_bin) = match slant_xgas_bins.iter().minmax() {
        itertools::MinMaxResult::NoElements => {
            return Err(GggError::custom(
                "slant_xgas_bins should not have zero elements",
            ))
        }
        itertools::MinMaxResult::OneElement(&v) => (v, v),
        itertools::MinMaxResult::MinMax(&v1, &v2) => (v1, v2),
    };

    let (slant_xgas_values, extrap_flags) = compute_quantized_slant_xgas(
        slant_xgas_values,
        min_extrap_slant,
        min_bin,
        max_bin,
        nsamples,
    );

    // Assume that the AKs have altitude as the first dimension.
    let nlev = aks.dim().0;
    let ntime = slant_xgas_values.len();
    let mut aks_out = Array2::<f32>::zeros([ntime, nlev]);

    // Now interpolate each level
    let slant_xgas_bins_slice = slant_xgas_bins.as_slice()
        .expect("Should be able to take a slice of slant_xgas_bins, as we convert to standard layout at the start of the function");
    let slant_xgas_values_slice = slant_xgas_values.as_standard_layout();
    let slant_xgas_values_slice = slant_xgas_values_slice.as_slice().expect(
        "Should be able to take a slice of slant_xgas_value, as we convert to standard layout",
    );
    for i in 0..nlev {
        let ak_row_in = aks.row(i);
        let ak_row_in = ak_row_in.as_standard_layout();
        let ak_row_in = ak_row_in.as_slice().expect(
            "Should be able to convert an AK row to a slice, as we convert to standard layout",
        );
        let ak_interp = interp_slice(
            slant_xgas_bins_slice,
            ak_row_in,
            slant_xgas_values_slice,
            &interp::InterpMode::Extrapolate,
        );
        let ak_interp = Array1::from_vec(ak_interp);
        aks_out.column_mut(i).assign(&ak_interp);
    }
    Ok((aks_out, extrap_flags))
}

fn compute_quantized_slant_xgas(
    slant_xgas_values: ArrayView1<f32>,
    min_extrap_slant: f32,
    min_interp_slant: f32,
    max_interp_slant: f32,
    nsamples: Option<usize>,
) -> (Array1<f32>, Array1<i8>) {
    let mut quant_slant = Array1::<f32>::zeros([slant_xgas_values.len()]);
    let mut extrap_flags = Array1::<i8>::zeros([slant_xgas_values.len()]);

    if let Some(nsamp) = nsamples {
        let nsamp_main = nsamp as f32;
        let nsamp_extrap = (nsamp / 10) as f32;

        for (i, v) in slant_xgas_values.iter().copied().enumerate() {
            if v < min_extrap_slant {
                quant_slant[i] = min_extrap_slant;
                extrap_flags[i] = -2;
            } else if v >= min_extrap_slant && v < min_interp_slant {
                quant_slant[i] = quantize(v, min_extrap_slant, min_interp_slant, nsamp_extrap);
                extrap_flags[i] = -1;
            } else if v >= min_interp_slant && v <= max_interp_slant {
                quant_slant[i] = quantize(v, min_interp_slant, max_interp_slant, nsamp_main);
            } else {
                quant_slant[i] = max_interp_slant;
                extrap_flags[i] = 2;
            }
        }
    } else {
        for (i, v) in slant_xgas_values.iter().copied().enumerate() {
            quant_slant[i] = v.clamp(min_extrap_slant, max_interp_slant);
            if v < min_extrap_slant {
                extrap_flags[i] = -2;
            } else if v > max_interp_slant {
                extrap_flags[i] = 2;
            }
        }
    }

    (quant_slant, extrap_flags)
}

fn quantize(v: f32, minval: f32, maxval: f32, n: f32) -> f32 {
    // Normalize and limit to [0, 1]
    let vn = (v - minval) / (maxval - minval);
    let vn = vn.clamp(0.0, 1.0);
    // Round to one of n values in [0, 1)
    let vi = (vn * (n - 1.0)).round() / (n - 1.0);
    // Restore original magnitude
    vi * (maxval - minval) + minval
}

/// Interpolate an array of averaging kernels from their standard pressures
/// to new, per-observation pressures.
///
/// Typically this will be used to move the
/// AKs from their lookup table pressure levels to the same pressure levels
/// as the a priori profiles. Parameters:
///
/// - `aks`: the array of averaging kernels. The first dimension is the observation
/// and must be the same length as the first dimension of `target_pressure' and its
/// second dimension must be its levels and be the same length as `ak_pressure`.
/// - `ak_pressure`: the array of pressures that the AKs are originally defined on.
/// It is assumed that each row of `aks` is on the same pressure levels.
/// - `target_pressures`: the array of pressures to which the AKs will be interpolated.
/// Each row of this array will be the vector of pressures for the corresponding row
/// of `aks`.
///
/// # Errors
/// Will return an error if:the shapes of the inputs are inconsistent (custom [`GggError`]).
pub fn interp_aks_to_new_pressures(
    aks: ArrayView2<f32>,
    ak_pressure: Array1<f32>,
    target_pressures: ArrayView2<f32>,
) -> error_stack::Result<Array2<f32>, GggError> {
    if aks.shape()[0] != target_pressures.shape()[0] {
        return Err(GggError::Custom(
            "First dimension of `aks` is a different length from the first dimension of `target_pressures`"
                .to_string(),
        )
        .into());
    }

    if aks.shape()[1] != ak_pressure.len() {
        return Err(GggError::Custom(
            "Second dimension of `aks` is a different length from the `ak_pressure` vector"
                .to_string(),
        )
        .into());
    }

    // The interp crate expects that the x coordinate is increasing, so
    // check that the ak_pressure vector is surface-to-space and flip if
    // needed.
    let (ak_pressure, flip_aks) = if ak_pressure.len() > 1 && ak_pressure[1] - ak_pressure[0] < 0.0
    {
        log::debug!("Will flip AKs during interpolation so pressure is increasing");
        let p = ak_pressure.iter().copied().rev().collect_vec();
        (p, true)
    } else {
        log::debug!("Will not flip AKs during interpolation; pressure is already increasing");
        let p = ak_pressure.to_vec();
        (p, false)
    };

    // Now we can simply iterate over the AKs and interpolate/extrapolate them
    // to the target pressures.
    let n_obs = target_pressures.shape()[0];
    let n_lev = target_pressures.shape()[1];
    let mut aks_out = Array2::from_elem([n_obs, n_lev], f32::NAN);

    ndarray::Zip::from(aks_out.rows_mut())
        .and(aks.rows())
        .and(target_pressures.rows())
        .for_each(|mut out_row, in_row, pres_row| {
            let tmp = if flip_aks {
                let in_row = in_row.iter().copied().rev().collect_vec();
                interp_slice(
                    &ak_pressure,
                    &in_row,
                    pres_row.as_standard_layout().as_slice().expect(
                        "pres_row array to slice should succeed after converting to standard layout",
                    ),
                    &interp::InterpMode::Extrapolate,
                )
                // We don't need to reverse the output because the prior pressure row
                // wasn't reversed - so the interpolation already outputs it in the
                // correct order.
            } else {
                interp_slice(
                    &ak_pressure,
                    &in_row.as_standard_layout().as_slice().expect(
                        "in_row array to slice should succeed after converting to standard layout",
                    ),
                    pres_row.as_standard_layout().as_slice().expect(
                        "pres_row array to slice should succeed after converting to standard layout",
                    ),
                    &interp::InterpMode::Extrapolate,
                )
            };
            let tmp = Array1::from_vec(tmp);
            out_row.assign(&tmp);
        });

    Ok(aks_out)
}

// ---------------- //
// Metadata helpers //
// ---------------- //

/// A structure representing metadata for a single TCCON site.
#[derive(Debug, Deserialize)]
pub struct NcSiteMetadata {
    /// A long (human readable) name for the site. Traditionally,
    /// this is the location name with a two-digit extension used to
    /// identify different instruments at the same location, e.g.
    /// "caltech01", "lauder03".
    pub long_name: String,

    /// The number of days after aquisition that data will be withheld
    /// from the public files. For example, a value of 30 means that data
    /// taken on 1 Apr would not go into the public files until 1 May.
    /// This should be written as an integer in new files, but has
    /// historically been written as a string in the GGG2020 JSON files.
    #[serde(deserialize_with = "release_lag_de_helper")]
    pub release_lag: u32,

    /// The physical location of the site, usually "City, Country" or
    /// similar format appropriate for the location.
    pub location: String,

    /// The name and email address of the person to contact with questions
    /// about the data. Expected format is "Name <Email>".
    pub contact: String,

    /// The revision of the data, typically "R0", "R1", etc.
    pub data_revision: String,

    /// The DOI of the data, starting with "10." (not "https://doi.org").
    /// May be omitted if the DOI has not been assigned yet.
    pub data_doi: Option<String>,

    /// A plain-text citation for the data. This will be assigned by CaltechData
    /// with information provided by the sites regarding creators and contributors.
    /// May be omitted if the CaltechData record has not been created.
    pub data_reference: Option<String>,

    /// A plain-text citation for the site. This will ideally be a journal article
    /// that describes the location and site setup. May be omitted if no such
    /// reference exists.
    pub site_reference: Option<String>,
}

/// Helper function that handles deserializing the release lag in site_info files, which
/// must be an integer in TOML files and will be a string in JSON files.
///
/// Eventually, the site info file should be converted to a TOML file, but that will require
/// updates to the metadata portal. That'll be a GGG2020.2 thing I think.
fn release_lag_de_helper<'de, D>(de: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(de)?;
    match value {
        serde_json::Value::String(s) => {
            let v: u32 = s.trim().parse()
            .map_err(|e| D::Error::custom(format!(
                "if given as a string, release_lag must be parsable as an unsigned integer (parsing error was: {e}"
            )))?;
            Ok(v)
        }
        serde_json::Value::Number(n) => {
            let v = n.as_u64().ok_or_else(|| {
                D::Error::custom("if given as a number, release_lag must be an unsigned integer")
            })?;
            // Should be safe to cast to a u32, release lags should be <= 366, so a 4B max should not
            // ever be reached.
            Ok(v as u32)
        }
        _ => {
            let e = D::Error::custom("release_lag must be a string or number");
            Err(e)
        }
    }
}

/// Read a TOML or JSON file defining site metadata.
///
/// The file must have a table (a.k.a. map or dict)
/// with the site IDs as keys and subtables that have
/// keys matching the fields in [`NcSiteMetadata`].
/// In terms of file format, TOML will be preferred going
/// forward.
///
/// # Errors
/// Returns a [`GggError::CouldNotRead`] if either the file
/// had the wrong structure or it does not have a `.toml` or
/// `.json` extension.
pub fn read_nc_site_metadata(
    site_info_file: &Path,
) -> Result<IndexMap<String, NcSiteMetadata>, GggError> {
    match site_info_file.extension() {
        Some(ext) => {
            if ext == "json" {
                log::debug!(
                    "Reading site info file '{}' as a JSON file",
                    site_info_file.display()
                );
                return read_nc_site_metadata_json(site_info_file)
                    .map_err(|e| GggError::could_not_read(site_info_file.to_path_buf(), e));
            } else if ext == "toml" {
                log::debug!(
                    "Reading site info file '{}' as a TOML file",
                    site_info_file.display()
                );
                return read_nc_site_metadata_toml(site_info_file)
                    .map_err(|e| GggError::could_not_read(site_info_file.to_path_buf(), e));
            }
        }
        None => (),
    }

    Err(GggError::could_not_read(
        site_info_file.to_path_buf(),
        "Unknown file extension, expected .toml or .json",
    ))
}

fn read_nc_site_metadata_toml(
    site_info_file: &Path,
) -> Result<IndexMap<String, NcSiteMetadata>, toml::de::Error> {
    let mut f = std::fs::File::open(site_info_file).map_err(|e| {
        toml::de::Error::custom(format!(
            "Error opening metadata file, {}: {e}",
            site_info_file.display()
        ))
    })?;
    let mut buf = String::new();
    f.read_to_string(&mut buf).map_err(|e| {
        toml::de::Error::custom(format!(
            "Error reading the contents of metadata file {}: {e}",
            site_info_file.display()
        ))
    })?;
    toml::from_str(&buf)
}

fn read_nc_site_metadata_json(
    site_info_file: &Path,
) -> Result<IndexMap<String, NcSiteMetadata>, serde_json::Error> {
    let f = std::fs::File::open(site_info_file).map_err(|e| {
        serde_json::Error::custom(format!(
            "error opening metadata file, {}: {e}",
            site_info_file.display()
        ))
    })?;
    serde_json::from_reader(f)
}

/// Retrieve the value of a string attribute on a netCDF variable, group, or file.
///
/// # See also
///
/// - [`get_string_attr_from_file`]: for when you have the file open, but need to
/// get an attribute from another variable.
pub fn get_string_attr<O: GetNcAttr>(
    object: O,
    attr: &str,
) -> error_stack::Result<String, GggNcError> {
    let vres = object.get_attr_val(attr)?;

    let sres: Result<String, _> = vres.try_into();

    sres.change_context_lazy(|| {
        GggNcError::context(format!(
            "Could not convert attribute '{attr}' on {} to a string",
            object.description()
        ))
    })
}

/// Retrieve a string attribute's value from a variable in an open file.
///
/// # See also
///
/// - [`get_string_attr`]: for when you already have the file, group,
/// or variable from which you wish to access the attribute.
pub fn get_string_attr_from_file(
    file: &netcdf::File,
    varname: &str,
    attr: &str,
) -> error_stack::Result<String, GggNcError> {
    let var = file
        .variable(varname)
        .ok_or_else(|| GggNcError::MissingVar {
            variable: varname.to_string(),
            group: None,
        })?;

    get_string_attr(&var, attr)
}

/// A trait used to allow generic access to netCDF attribute values across
/// different netCDF constructs (files, groups, variables).
pub trait GetNcAttr {
    /// Get the value of a given attribute
    fn get_attr_val(&self, attr: &str) -> Result<netcdf::AttributeValue, GggNcError>;
    /// Describe the object we are getting the attribute from - useful in error messages.
    fn description(&self) -> String;
}

impl<'v> GetNcAttr for &netcdf::Variable<'v> {
    fn get_attr_val(&self, attr: &str) -> Result<netcdf::AttributeValue, GggNcError> {
        self.attribute_value(attr)
            .ok_or_else(|| GggNcError::missing_var_attr::<_, _, String>(attr, self.name(), None))?
            .map_err(|e| GggNcError::from(e))
    }

    fn description(&self) -> String {
        format!("variable '{}'", self.name())
    }
}

impl<'g> GetNcAttr for &netcdf::Group<'g> {
    fn get_attr_val(&self, attr: &str) -> Result<netcdf::AttributeValue, GggNcError> {
        self.attribute_value(attr)
            .ok_or_else(|| GggNcError::missing_group_attr(attr, self.name()))?
            .map_err(|e| GggNcError::from(e))
    }

    fn description(&self) -> String {
        format!("group '{}'", self.name())
    }
}

impl GetNcAttr for &netcdf::File {
    fn get_attr_val(&self, attr: &str) -> Result<netcdf::AttributeValue, GggNcError> {
        self.attribute(attr)
            .ok_or_else(|| GggNcError::missing_group_attr(attr, ""))?
            .value()
            .map_err(|e| GggNcError::from(e))
    }

    fn description(&self) -> String {
        "group '/'".to_string()
    }
}

// ------------- //
// Value helpers //
// ------------- //

/// Convert a single timestamp from a TCCON netCDF file into a datetime.
pub fn convert_nc_timestamp(ts: f64) -> chrono::DateTime<chrono::Utc> {
    let nanos = (ts * 1e9).trunc() as i64;
    chrono::DateTime::from_timestamp_nanos(nanos)
}
