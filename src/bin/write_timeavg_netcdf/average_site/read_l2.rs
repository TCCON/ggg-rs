use std::path::Path;

use error_stack::ResultExt;
use ggg_rs::{
    nc_utils::{
        self, convert_nc_timestamp, error_var_name, get_string_attr, get_string_attr_on_var,
        get_var_data, get_var_data_quantity,
    },
    time_utils,
    utils::GggNcError,
};
use ndarray::{Array1, Ix1, Ix2};
use uom::si::{
    self,
    f32::{Angle, Pressure, Ratio},
};

use crate::data_structures::Level2Data;

/// Required variables to average XCO2
pub(super) const CO2_VAR_DEF: SourceVarDef = SourceVarDef {
    private_prior_var: "prior_1co2",
    public_prior_var: "prior_co2",
    ak_var: "ak_xco2",
    ak_bin_var: "ak_slant_xco2_bin",
    xgas_var: "xco2_x2019",
    wmo_scale_var: "aicf_xco2_x2019_scale",
};

/// Required variables to average XCH4
pub(super) const CH4_VAR_DEF: SourceVarDef = SourceVarDef {
    private_prior_var: "prior_1ch4",
    public_prior_var: "prior_ch4",
    ak_var: "ak_xch4",
    ak_bin_var: "ak_slant_xch4_bin",
    xgas_var: "xch4",
    wmo_scale_var: "aicf_xch4_scale",
};

/// A struct defining the set of variables and other setting required
/// to average a particular gas.
pub(super) struct SourceVarDef {
    private_prior_var: &'static str,
    public_prior_var: &'static str,
    ak_var: &'static str,
    ak_bin_var: &'static str,
    xgas_var: &'static str,
    wmo_scale_var: &'static str,
}

/// Read data for a single Xgas from a private or public L2 TCCON file.
pub(crate) fn read_file(
    nc_file: &Path,
    var_def: &SourceVarDef,
) -> error_stack::Result<Level2Data, GggNcError> {
    log::info!("Reading L2 data from file {}", nc_file.display());
    let ds = netcdf::open(nc_file).map_err(|e| GggNcError::NcErr(e))?;
    let is_private = ds.variable("flag").is_some();
    if is_private {
        read_private_file(&ds, var_def)
    } else {
        read_public_file(&ds, var_def)
    }
}

/// Read a private L2 TCCON file.
///
/// This will set the `is_public` variable to `0` for data after
/// today minus the release lag.
///
/// For GGG2020.1, this handles expanding the priors and AKs. In the future,
/// this will be part of the private netCDF writer.
fn read_private_file(
    ds: &netcdf::File,
    var_def: &SourceVarDef,
) -> error_stack::Result<Level2Data, GggNcError> {
    let mut data = read_either_file(
        ds,
        "prior_1h2o",
        var_def.private_prior_var,
        var_def.ak_var,
        var_def.xgas_var,
    )?;

    // Replace the fill value flag array with the actual values
    data.flag = get_var_data(ds, "flag")?;

    // Expand the prior variables
    log::debug!("Expanding priors");
    let prior_index = get_var_data::<u32, Ix1>(ds, "prior_index")?.mapv(|i| i as usize);
    data.prior_h2o_wet = nc_utils::expand_priors_2d(data.prior_h2o_wet.view(), prior_index.view())
        .change_context_lazy(|| GggNcError::Context("Error expanding H2O priors".to_string()))?;
    data.prior_wet = nc_utils::expand_priors_2d(data.prior_wet.view(), prior_index.view())
        .change_context_lazy(|| GggNcError::Context("Error expanding wet priors".to_string()))?;
    data.prior_dry = nc_utils::expand_priors_2d(data.prior_dry.view(), prior_index.view())
        .change_context_lazy(|| GggNcError::Context("Error expanding dry priors".to_string()))?;

    // Expand the AKs. This duplicates a lot of logic from the public writer, but this can also go away
    // in GGG2020.2.
    log::debug!("Expanding AKs");
    let airmass = get_var_data::<f32, Ix1>(ds, "o2_7885_am_o2")?;
    let slant_xgas = Array1::from_iter(
        data.xgas
            .iter()
            .zip(airmass)
            .map(|(x, a)| x.get::<uom::si::ratio::ratio>() * a),
    );
    let ak_bins = get_var_data::<f32, Ix1>(ds, var_def.ak_bin_var)?;
    let ak_bin_units = get_string_attr_on_var(ds, var_def.ak_bin_var, None, "units")?;
    let tmp = nc_utils::expand_slant_xgas_binned_aks(
        slant_xgas.view(),
        "parts",
        ak_bins,
        &ak_bin_units,
        data.avg_kernel.view(),
        Some(500),
    )
    .change_context_lazy(|| GggNcError::context("Error while expanding averaging kernels"))?;
    data.avg_kernel = tmp.0;

    // Get which calibration scale this gas is tied to.
    log::debug!(
        "Extracting traceability scale from {}",
        var_def.wmo_scale_var
    );
    data.xgas_wmo_scale = nc_utils::get_traceability_scale(ds, var_def.wmo_scale_var)?;

    // Update is_public based on release lag
    // TODO: verify that this works - hard to test because the data I keep around is old enough its all public.
    let release_lag_str = get_string_attr(ds, "release_lag")?;
    let release_lag = nc_utils::parse_release_lag(&release_lag_str)?;
    let last_public_time = chrono::Utc::now() - release_lag;
    log::debug!(
        "With release lag of {release_lag_str}, last public time will be {last_public_time}"
    );
    data.is_public
        .iter_mut()
        .zip(data.utc_time.iter())
        .for_each(|(is_pub, t)| {
            if *t <= last_public_time {
                *is_pub = 1;
            } else {
                *is_pub = 0;
            }
        });
    Ok(data)
}

/// Read a public TCCON file.
///
/// The `is_public` field on the returned data will be all 1s
/// since the data is assumed to be public.
fn read_public_file(
    ds: &netcdf::File,
    var_def: &SourceVarDef,
) -> error_stack::Result<Level2Data, GggNcError> {
    let mut data = read_either_file(
        ds,
        "prior_h2o",
        var_def.public_prior_var,
        var_def.ak_var,
        var_def.xgas_var,
    )?;
    data.xgas_wmo_scale =
        get_string_attr_on_var(ds, var_def.xgas_var, None, "wmo_or_analogous_scale")?;
    // Replace the flag fill values with 0s, since public files only contain flag == 0 values (as of GGG2020.1).
    data.flag.mapv_inplace(|_| 0);
    Ok(data)
}

fn read_either_file(
    ds: &netcdf::File,
    prior_h2o_var: &str,
    prior_var: &str,
    ak_var: &str,
    xgas_var: &str,
) -> error_stack::Result<Level2Data, GggNcError> {
    let file_path = ds
        .path()
        .change_context_lazy(|| GggNcError::custom("Could not get path of input L2 file"))?;
    let tccon_site_id =
        ggg_rs::utils::site_id_from_filename(&file_path).change_context_lazy(|| {
            GggNcError::context("Error getting the site ID from the input L2 file name")
        })?;
    let station_id = crate::station_ids::get_mip_station_id(&tccon_site_id, None)
        .change_context_lazy(|| {
            GggNcError::context(format!(
                "Error getting MIP station ID for the input L2 file {}",
                file_path.display()
            ))
        })?;

    let utc_time = get_var_data::<f64, Ix1>(&ds, "time")?.mapv(|ts| convert_nc_timestamp(ts));
    let is_public = Array1::<i8>::ones(utc_time.dim());
    let latitude = get_var_data_quantity::<f32, Ix1, Angle>(&ds, "lat")?;
    let longitude = get_var_data_quantity::<f32, Ix1, Angle>(&ds, "long")?;
    let sza = get_var_data_quantity::<f32, Ix1, Angle>(&ds, "solzen")?;
    let p_surf = get_var_data_quantity::<f32, Ix1, Pressure>(&ds, "pout")?;
    let p_levels_prior = get_var_data_quantity::<f32, Ix2, Pressure>(&ds, "prior_pressure")?;
    let p_levels_ak = get_var_data_quantity::<f32, Ix1, Pressure>(ds, "ak_pressure")?;
    let prior_h2o_wet = get_var_data_quantity::<f32, Ix2, Ratio>(&ds, prior_h2o_var)?;
    let prior_wet = get_var_data_quantity::<f32, Ix2, Ratio>(&ds, prior_var)?;
    // let prior_dry = prior_wet / (-prior_h2o_wet + Ratio::new(1.0));
    let avg_kernel = get_var_data::<f32, Ix2>(&ds, ak_var)?;
    let xgas = get_var_data_quantity::<f32, Ix1, Ratio>(&ds, xgas_var)?;
    let xgas_error = get_var_data_quantity::<f32, Ix1, Ratio>(&ds, &error_var_name(xgas_var))?;

    let unit_ratio = Ratio::new::<si::ratio::ratio>(1.0);
    let one_minus_h2o = prior_h2o_wet.map(|x| unit_ratio - *x);
    let prior_dry = prior_wet.clone() / one_minus_h2o;

    let solar_time = Array1::from_iter(
        utc_time
            .iter()
            .zip(longitude.iter())
            .map(|(t, x)| time_utils::solar_apparent_time(*x, *t)),
    );

    // flag is only found in private files; set it to fill values here, and require the file-specific functions
    // to replace it
    let flag = Array1::<i32>::from_elem(utc_time.dim(), i32::MIN);

    Ok(Level2Data {
        utc_time,
        solar_time,
        station_id,
        is_public,
        latitude,
        longitude,
        flag,
        sza,
        p_surf,
        p_levels_prior,
        p_levels_ak,
        prior_h2o_wet,
        prior_dry,
        prior_wet,
        avg_kernel,
        xgas,
        xgas_error,
        xgas_wmo_scale: String::new(),
    })
}
