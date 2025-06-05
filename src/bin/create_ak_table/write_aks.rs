use error_stack::ResultExt;
use netcdf::Extents;

use crate::{
    calc_aks::{AkTable, AkTableSet},
    naming::{ak_varname, PRES_VAR_NAME, SZA_DIM_NAME, Z_DIM_NAME},
    AppendMode,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum WriteError {
    #[error("An error occurred while {0}")]
    Context(String),
}

impl WriteError {
    fn context<S: ToString>(msg: S) -> Self {
        Self::Context(msg.to_string())
    }
}

///
pub(crate) fn write_aks_to_dset(
    ds: &mut netcdf::FileMut,
    aks: &AkTableSet,
    append_mode: &AppendMode,
) -> error_stack::Result<(), WriteError> {
    add_dims(ds, aks)?;
    for (gas, gas_aks) in aks.tables.iter() {
        add_ak(ds, gas, gas_aks, append_mode)?;
    }
    Ok(())
}

fn add_dims(ds: &mut netcdf::FileMut, aks: &AkTableSet) -> error_stack::Result<(), WriteError> {
    if ds.dimension(Z_DIM_NAME).is_none() {
        ds.add_dimension(Z_DIM_NAME, aks.pressure.len())
            .change_context_lazy(|| WriteError::context("creating the altitude dimension"))?;

        // Writing the altitude variable
        let mut z_var = ds
            .add_variable::<f64>(Z_DIM_NAME, &[Z_DIM_NAME])
            .change_context_lazy(|| WriteError::context("creating the altitude variable"))?;
        z_var
            .put_attribute("description", "Altitude levels for the AK vertical grid")
            .change_context_lazy(|| {
                WriteError::context("adding the 'description' attribute to the altitude variable")
            })?;
        z_var.put_attribute("units", "km").change_context_lazy(|| {
            WriteError::context("adding the 'units' attribute to the altitude variable")
        })?;
        z_var
            .put(aks.altitude.view(), Extents::All)
            .change_context_lazy(|| WriteError::context("writing the altitude variable"))?;

        // Writing the pressure variable
        let mut p_var = ds
            .add_variable::<f64>(PRES_VAR_NAME, &[Z_DIM_NAME])
            .change_context_lazy(|| WriteError::context("creating the pressure variable"))?;
        p_var
            .put_attribute(
                "description",
                "Mean pressure levels for the AK vertical grid",
            )
            .change_context_lazy(|| {
                WriteError::context("adding the 'description' attribute to the pressure variable")
            })?;
        p_var
            .put_attribute("units", "hPa")
            .change_context_lazy(|| {
                WriteError::context("adding the 'units' attribute to the pressure variable")
            })?;
        p_var
            .put(aks.pressure.view(), Extents::All)
            .change_context_lazy(|| WriteError::context("writing the pressure variable"))?;
    }

    // Writing the SZA bin variable
    if ds.dimension(SZA_DIM_NAME).is_none() {
        ds.add_dimension(SZA_DIM_NAME, aks.sza_bin_centers.len())
            .change_context_lazy(|| WriteError::context("creating the SZA bin dimension"))?;

        let mut sza_var = ds
            .add_variable::<f64>(SZA_DIM_NAME, &[SZA_DIM_NAME])
            .change_context_lazy(|| WriteError::context("creating the SZA bin variable"))?;
        sza_var
            .put_attribute(
                "description",
                "middle of the SZA bins to which the AKs are assigned",
            )
            .change_context_lazy(|| {
                WriteError::context("adding the 'description' attribute to the SZA bin variable")
            })?;
        sza_var
            .put_attribute("units", "degrees")
            .change_context_lazy(|| {
                WriteError::context("adding the 'units' attribute to the SZA bin variable")
            })?;
        sza_var
            .put(aks.sza_bin_centers.view(), Extents::All)
            .change_context_lazy(|| WriteError::context("writing the SZA bin variable"))?;
    }
    Ok(())
}

fn add_ak(
    ds: &mut netcdf::FileMut,
    gas: &str,
    table: &AkTable,
    append_mode: &AppendMode,
) -> error_stack::Result<(), WriteError> {
    let varname = ak_varname(gas);
    if ds.variable(&varname).is_some() {
        match append_mode {
            AppendMode::No => panic!("{varname} exists - this should not happen when we are overwriting the existing file."),
            AppendMode::Keep => return Ok(()),
            AppendMode::Error => panic!("{varname} exists - this should have already been checked before we got to writing the AKs."),
        }
    }

    let bin_dim = match table.bins {
        crate::calc_aks::AkBinType::SZA => SZA_DIM_NAME,
    };
    let mut var = ds
        .add_variable::<f64>(&varname, &[Z_DIM_NAME, bin_dim])
        .change_context_lazy(|| {
            WriteError::context(format!("creating the '{varname}' variable"))
        })?;
    var.put_attribute("description", format!("X{gas} column averaging kernels"))
        .change_context_lazy(|| {
            WriteError::context(format!(
                "adding the 'description' attribute to the '{varname}' variable"
            ))
        })?;
    var.put(table.aks.view(), Extents::All)
        .change_context_lazy(|| WriteError::context(format!("writing the '{varname}' variable")))?;
    Ok(())
}
