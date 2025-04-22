use interp::interp_slice;
use itertools::Itertools;
use ndarray::{Array1, Array2, ArrayD, ArrayView1, ArrayView2};
use netcdf::{types::{FloatType, IntType}, Extents};
use num_traits::Zero;

use crate::{units::dmf_conv_factor, utils::GggError};

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
            },
            netcdf::types::NcVariableType::Opaque(_) => {
                unimplemented!("reading netCDF Opaque types as a generic array")
            },
            netcdf::types::NcVariableType::Enum(_) => {
                unimplemented!("reading netCDF Enum types as a generic array")
            },
            netcdf::types::NcVariableType::Vlen(_) => {
                unimplemented!("reading netCDF variable length types as a generic array")
            },
            netcdf::types::NcVariableType::String => todo!(),
            netcdf::types::NcVariableType::Int(IntType::I8) => {
                let values = var.get::<i8, _>(Extents::All)?;
                Ok(Self::I8(values))
            },
            netcdf::types::NcVariableType::Int(IntType::I16) => {
                let values = var.get::<i16, _>(Extents::All)?;
                Ok(Self::I16(values))
            },
            netcdf::types::NcVariableType::Int(IntType::I32) => {
                let values = var.get::<i32, _>(Extents::All)?;
                Ok(Self::I32(values))
            },
            netcdf::types::NcVariableType::Int(IntType::I64) => {
                let values = var.get::<i64, _>(Extents::All)?;
                Ok(Self::I64(values))
            },
            netcdf::types::NcVariableType::Int(IntType::U8) => {
                let values = var.get::<u8, _>(Extents::All)?;
                Ok(Self::U8(values))
            },
            netcdf::types::NcVariableType::Int(IntType::U16) => {
                let values = var.get::<u16, _>(Extents::All)?;
                Ok(Self::U16(values))
            },
            netcdf::types::NcVariableType::Int(IntType::U32) => {
                let values = var.get::<u32, _>(Extents::All)?;
                Ok(Self::U32(values))
            },
            netcdf::types::NcVariableType::Int(IntType::U64) => {
                let values = var.get::<u64, _>(Extents::All)?;
                Ok(Self::U64(values))
            },
            netcdf::types::NcVariableType::Float(FloatType::F32) => {
                let values = var.get::<f32, _>(Extents::All)?;
                Ok(Self::F32(values))
            },
            netcdf::types::NcVariableType::Float(FloatType::F64) => {
                let values = var.get::<f64, _>(Extents::All)?;
                Ok(Self::F64(values))
            },
            netcdf::types::NcVariableType::Char => {
                let values = var.get::<u8, _>(Extents::All)?;
                Ok(Self::Char(values))
            },
        }
    }

    /// Create a variable in a netCDF group and write this data to it.
    /// Since this writes data, if you need to set options on the variable
    /// that must be done pre-write (e.g., compression), you must match
    /// on this enum's variants and create the variable yourself (for now at least).
    pub fn put_to<'g>(&self, grp: &'g mut netcdf::GroupMut, name: &str, dims: &[&str]) -> netcdf::Result<netcdf::VariableMut<'g>> {
        match self {
            NcArray::I8(arr) => {
                let mut var = grp.add_variable::<i8>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
            NcArray::I16(arr) => {
                let mut var = grp.add_variable::<i16>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
            NcArray::I32(arr) => {
                let mut var = grp.add_variable::<i32>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
            NcArray::I64(arr) => {
                let mut var = grp.add_variable::<i64>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
            NcArray::U8(arr) => {
                let mut var = grp.add_variable::<u8>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
            NcArray::U16(arr) => {
                let mut var = grp.add_variable::<u16>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
            NcArray::U32(arr) => {
                let mut var = grp.add_variable::<u32>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
            NcArray::U64(arr) => {
                let mut var = grp.add_variable::<u64>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
            NcArray::F32(arr) => {
                let mut var = grp.add_variable::<f32>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
            NcArray::F64(arr) => {
                let mut var = grp.add_variable::<f64>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
            NcArray::Char(arr) => {
                let mut var = grp.add_variable::<u8>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
        }
    }
}

// ----------------------------------------- //
// Helper functions for expanding the priors //
// ----------------------------------------- //

pub fn expand_priors<T: Zero + Copy>(
    compact_priors: ArrayView2<T>,
    prior_index: ArrayView1<usize>,
) -> Result<Array2<T>, GggError> {
    let ntimes = prior_index.len();
    let nlev = compact_priors.dim().1;
    let mut expanded_priors = Array2::<T>::zeros([ntimes, nlev]);
    for (itime, &index) in prior_index.iter().enumerate() {
        if index >= compact_priors.nrows() {
            return Err(GggError::custom(format!(
                "Prior index {index} at position {itime} is out-of-bounds"
            )))
        }
        // .row() panics if the index is out of bounds, hence the check above
        let orig_prof = compact_priors.row(index);
        expanded_priors.row_mut(itime).assign(&orig_prof);
    }
    Ok(expanded_priors)
}

// -------------------------------------- //
// Helper functions for expanding the AKs //
// -------------------------------------- //

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
        let cf = dmf_conv_factor(bin_units, xgas_units)
            .map_err(|e| GggError::custom(format!("Error converting AK slant Xgas bins to proper unit: {e}")))?;
        slant_xgas_bins * cf
    };
    let slant_xgas_bins = slant_xgas_bins.as_standard_layout();

    let (min_bin, max_bin) = match slant_xgas_bins.iter().minmax() {
        itertools::MinMaxResult::NoElements => return Err(GggError::custom("slant_xgas_bins should not have zero elements")),
        itertools::MinMaxResult::OneElement(&v) => (v, v),
        itertools::MinMaxResult::MinMax(&v1, &v2) => (v1, v2),
    };

    let (slant_xgas_values, extrap_flags) = compute_quantized_slant_xgas(slant_xgas_values, min_extrap_slant, min_bin, max_bin, nsamples);

    // Assume that the AKs have altitude as the first dimension.
    let nlev = aks.dim().0;
    let ntime = slant_xgas_values.len();
    let mut aks_out = Array2::<f32>::zeros([ntime, nlev]);

    // Now interpolate each level
    let slant_xgas_bins_slice = slant_xgas_bins.as_slice()
        .expect("Should be able to take a slice of slant_xgas_bins, as we convert to standard layout at the start of the function");
    let slant_xgas_values_slice = slant_xgas_values.as_standard_layout();
    let slant_xgas_values_slice = slant_xgas_values_slice.as_slice()
        .expect("Should be able to take a slice of slant_xgas_value, as we convert to standard layout");
    for i in 0..nlev {
        let ak_row_in = aks.row(i);
        let ak_row_in = ak_row_in.as_standard_layout();
        let ak_row_in = ak_row_in.as_slice()
            .expect("Should be able to convert an AK row to a slice, as we convert to standard layout");
        let ak_interp = interp_slice(slant_xgas_bins_slice, ak_row_in, slant_xgas_values_slice, &interp::InterpMode::Extrapolate);
        let ak_interp = Array1::from_vec(ak_interp);
        aks_out.column_mut(i).assign(&ak_interp);
    }
    Ok((aks_out, extrap_flags))

}

fn compute_quantized_slant_xgas(slant_xgas_values: ArrayView1<f32>, min_extrap_slant: f32, min_interp_slant: f32, max_interp_slant: f32, nsamples: Option<usize>)
-> (Array1<f32>, Array1<i8>) {
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