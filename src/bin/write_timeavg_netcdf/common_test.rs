use std::path::Path;

use ggg_rs::{
    nc_utils::{self, GetNcVar},
    test_utils::{
        compare_to_netcdf_quantities, compare_to_netcdf_quantities_with_tolerance,
        compare_to_netcdf_values_approx, compare_to_netcdf_values_approx_with_tolerance,
        compare_to_netcdf_values_eq,
    },
};
use ndarray::Ix1;

use crate::data_structures::{Gas, Level2Data, TimeAvgData};

pub(crate) fn check_l2_data(expected_ds: &netcdf::File, l2_data: Level2Data) {
    // The UTC times are best done by converting the expected values to DateTime types and checking
    // equality, though this does assume millisecond precision between the netCDF file and the test.
    // Unfortunately, the test files from python all have different epochs (thanks, xarray) so we
    // do have to parse it.
    let expected_utc_times = read_time(expected_ds, "time");
    let expected_ts = expected_utc_times.mapv(|t| t.timestamp_millis());
    let actual_ts = l2_data.utc_time.mapv(|t| t.timestamp_millis());
    assert!(
        approx::abs_diff_eq!(expected_ts, actual_ts, epsilon = 2),
        "UTC times do not match.\nExpected = {expected_utc_times:?}\nActual = {:?}",
        l2_data.utc_time
    );

    // The rest of the values can be directly compared to the expected ones in the netCDF file.
    // Note that it does not currently have the solar time, that will need added in the future.
    // Lat and long need a bit larger tolerance than the default
    compare_to_netcdf_quantities::<f32, _, _>(&expected_ds, "long", &l2_data.longitude);
    compare_to_netcdf_quantities::<f32, _, _>(&expected_ds, "lat", &l2_data.latitude);
    compare_to_netcdf_quantities::<f32, _, _>(&expected_ds, "solzen", &l2_data.sza);
    compare_to_netcdf_quantities::<f32, _, _>(&expected_ds, "pout", &l2_data.p_surf);
    compare_to_netcdf_values_eq::<i32, _, _>(expected_ds, "flag", &l2_data.flag);
    compare_to_netcdf_values_eq::<i8, _, _>(expected_ds, "public", &l2_data.is_public);
    compare_to_netcdf_quantities::<f32, _, _>(&expected_ds, "xco2_x2019", &l2_data.xgas);
    compare_to_netcdf_quantities::<f32, _, _>(
        &expected_ds,
        "xco2_error_x2019",
        &l2_data.xgas_error,
    );
    compare_to_netcdf_values_approx::<f32, _, _>(expected_ds, "ak_xco2", &l2_data.avg_kernel);
    compare_to_netcdf_quantities::<f32, _, _>(&expected_ds, "ak_pressure", &l2_data.p_levels_ak);
    compare_to_netcdf_quantities::<f32, _, _>(&expected_ds, "prior_co2", &l2_data.prior_wet);
    compare_to_netcdf_quantities::<f32, _, _>(&expected_ds, "prior_h2o", &l2_data.prior_h2o_wet);
    compare_to_netcdf_quantities::<f32, _, _>(
        &expected_ds,
        "prior_pressure",
        &l2_data.p_levels_prior,
    );
}

pub(crate) struct CheckTimeAvgFields {
    pub(crate) solar_time: &'static str,
    pub(crate) cdate: &'static str,
    pub(crate) num_spec: &'static str,
    pub(crate) longitude: &'static str,
    pub(crate) latitude: &'static str,
    pub(crate) sza: &'static str,
    pub(crate) pout: &'static str,
    pub(crate) pres_prior: &'static str,
    pub(crate) ak_pres: &'static str,
    pub(crate) h2o_prior: &'static str,
    pub(crate) wet_gas_prior: &'static str,
    pub(crate) ak: &'static str,
    pub(crate) xgas: &'static str,
    pub(crate) xgas_std: &'static str,
    pub(crate) public: &'static str,
    pub(crate) dry_gas_prior: Option<&'static str>,
    pub(crate) station_id: Option<&'static str>,
    pub(crate) obs_id: Option<&'static str>,
}

impl CheckTimeAvgFields {
    pub(crate) fn default_vs_python_for_gas(gas: &Gas) -> Self {
        match gas {
            Gas::CO2 => Self {
                solar_time: "bin_center_time",
                cdate: "binned_xco2_time",
                num_spec: "binned_xco2_num_spec",
                longitude: "long",
                latitude: "lat",
                sza: "binned_xco2_sza",
                pout: "binned_xco2_pout",
                pres_prior: "binned_co2_pres_prior",
                ak_pres: "ak_pressure",
                h2o_prior: "binned_co2_h2o_prior",
                wet_gas_prior: "binned_co2_prior",
                ak: "binned_xco2_ak",
                xgas: "binned_xco2",
                xgas_std: "binned_xco2_std",
                public: "public",
                dry_gas_prior: None,
                station_id: None,
                obs_id: None,
            },
            Gas::CH4 => todo!(),
        }
    }
}

impl Default for CheckTimeAvgFields {
    fn default() -> Self {
        Self {
            solar_time: "solar_time_bin",
            cdate: "cdate",
            num_spec: "num_soundings",
            longitude: "longitude",
            latitude: "latitude",
            sza: "sza",
            pout: "p_surf",
            pres_prior: "p_levels_prior",
            ak_pres: "p_levels_ak",
            h2o_prior: "prior_h2o",
            wet_gas_prior: "prior_mixing_tccon",
            ak: "avg_kernel",
            xgas: "column_mixing",
            xgas_std: "sigma_column_mixing",
            public: "public",
            dry_gas_prior: Some("prior_mixing"),
            station_id: Some("station_id"),
            obs_id: Some("obs_id"),
        }
    }
}

pub(crate) fn check_timeavg_files(
    expected_file: &Path,
    test_file: &Path,
    gas: Gas,
    field_to_var: &CheckTimeAvgFields,
) {
    let expected_ds =
        netcdf::open(expected_file).expect("cannot open file with expected time-averaged data");
    let expected_group = expected_ds
        .group(gas.output_group())
        .unwrap()
        .expect("missing group for gas");
    let test_ds =
        netcdf::open(test_file).expect("cannot open file with test time-averaged data data");
    let test_group = test_ds
        .group(gas.output_group())
        .unwrap()
        .expect("missing group for gas");
    let data = TimeAvgData::read_netcdf(&test_group).unwrap();
    check_timeavg_data(&expected_group, data, field_to_var);
}

pub(crate) fn check_timeavg_data(
    expected_ds: &netcdf::Group,
    timeavg_data: TimeAvgData,
    field_to_var: &CheckTimeAvgFields,
) {
    let expected_solar_times = read_time(expected_ds, field_to_var.solar_time);
    let expected_solar_ts = expected_solar_times.mapv(|t| t.timestamp_millis());
    let actual_solar_ts = timeavg_data
        .solar_time_bin
        .mapv(|t| t.and_utc().timestamp_millis());
    // The solar time is stored in the Python 'truth' file as an int, so strict equality should be safe
    assert_eq!(expected_solar_ts, actual_solar_ts);

    let expected_zpd_times = read_time(expected_ds, field_to_var.cdate);
    let expected_zpd_ts = expected_zpd_times.mapv(|t| t.timestamp_millis());
    let actual_zpd_ts = timeavg_data.cdate.mapv(|t| t.timestamp_millis());
    // Allow ~2 second difference in the ZPD times; since we use the solar times for the obs IDs, the ZPD times don't matter as much.
    assert!(
        approx::abs_diff_eq!(expected_zpd_ts, actual_zpd_ts, epsilon = 2000),
        "Bin mean ZPD times do not match.\nExpected = {expected_zpd_times:?}\nActual = {:?}",
        timeavg_data.cdate
    );

    // obs_id and station_id aren't in the original Python "truth" file, so the best we can do right now
    // is check that the station_id is correct for Park Falls: 29 as of 31 Aug 2026
    let expected_obs_id = ndarray::Array1::from_elem(timeavg_data.station_id.dim(), 29);
    assert_eq!(timeavg_data.station_id, expected_obs_id);

    // Most of the remaining fields can be checked with a bit looser tolerance than in the L2, since there is some math.
    compare_to_netcdf_values_eq::<u64, _, _>(
        expected_ds,
        field_to_var.num_spec,
        &timeavg_data.num_soundings,
    );
    compare_to_netcdf_values_eq::<i8, _, _>(expected_ds, field_to_var.public, &timeavg_data.public);
    compare_to_netcdf_quantities_with_tolerance::<f32, _, _, _>(
        expected_ds,
        field_to_var.longitude,
        &timeavg_data.longitude,
        1e-6,
    );
    compare_to_netcdf_quantities_with_tolerance::<f32, _, _, _>(
        expected_ds,
        field_to_var.latitude,
        &timeavg_data.latitude,
        1e-6,
    );
    compare_to_netcdf_quantities_with_tolerance::<f32, _, _, _>(
        expected_ds,
        field_to_var.sza,
        &timeavg_data.sza,
        1e-6,
    );
    compare_to_netcdf_quantities_with_tolerance::<f32, _, _, _>(
        expected_ds,
        field_to_var.pout,
        &timeavg_data.p_surf,
        1e-6,
    );
    compare_to_netcdf_quantities_with_tolerance::<f32, _, _, _>(
        expected_ds,
        field_to_var.pres_prior,
        &timeavg_data.p_levels_prior,
        1e-6,
    );
    compare_to_netcdf_quantities_with_tolerance::<f32, _, _, _>(
        expected_ds,
        field_to_var.ak_pres,
        &timeavg_data.p_levels_ak,
        1e-6,
    );
    compare_to_netcdf_quantities_with_tolerance::<f32, _, _, _>(
        expected_ds,
        field_to_var.h2o_prior,
        &timeavg_data.prior_h2o,
        1e-6,
    );
    // The Python 'truth' files only have the WMF prior. Checking the DMF prior will have to
    // wait until we replace the Python files for the per-site case. The daily files can compare
    // the DMF prior
    compare_to_netcdf_quantities_with_tolerance::<f32, _, _, _>(
        expected_ds,
        field_to_var.wet_gas_prior,
        &timeavg_data.prior_mixing_tccon,
        1e-6,
    );
    if let Some(dmf_prior_var) = field_to_var.dry_gas_prior {
        compare_to_netcdf_quantities_with_tolerance::<f32, _, _, _>(
            expected_ds,
            dmf_prior_var,
            &timeavg_data.prior_mixing,
            1e-6,
        );
    }
    compare_to_netcdf_values_approx_with_tolerance::<f32, _, _>(
        expected_ds,
        field_to_var.ak,
        &timeavg_data.avg_kernel,
        1e-6,
    );
    compare_to_netcdf_quantities_with_tolerance::<f32, _, _, _>(
        expected_ds,
        field_to_var.xgas,
        &timeavg_data.column_mixing,
        1e-6,
    );

    // The uncertainty needs a bit looser tolerance to match the Python, because it involves more math
    // where slight implementation differences can add up.
    compare_to_netcdf_quantities_with_tolerance::<f32, _, _, _>(
        expected_ds,
        field_to_var.xgas_std,
        &timeavg_data.sigma_column_mixing,
        4e-5,
    );
    // Assume we are using the X2019 CO2 scale to check that this propagates correctly.
    assert_eq!(timeavg_data.wmo_or_analagous_scale, "WMO CO2 X2019");

    // Fields which are not in the original Python 'truth' files, so we can only check
    // when we get to the daily files. In the future, these should always be checked.
    if let Some(station_id_var) = field_to_var.station_id {
        compare_to_netcdf_values_eq(expected_ds, station_id_var, &timeavg_data.station_id);
    }

    if let Some(obs_id_var) = field_to_var.obs_id {
        compare_to_netcdf_values_eq(expected_ds, obs_id_var, &timeavg_data.obs_id);
    }
}

pub(crate) fn read_time<O: GetNcVar>(
    ds: O,
    varname: &str,
) -> ndarray::Array1<chrono::DateTime<chrono::Utc>> {
    nc_utils::get_var_cftime::<i64, Ix1, _>(ds, varname).expect("reading time should succeed")
}
