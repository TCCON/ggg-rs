use ggg_rs::{
    nc_utils,
    test_utils::{
        compare_to_netcdf_quantities, compare_to_netcdf_quantities_with_tolerance,
        compare_to_netcdf_values_approx, compare_to_netcdf_values_approx_with_tolerance,
        compare_to_netcdf_values_eq,
    },
};
use ndarray::Ix1;

use crate::data_structures::{Level2Data, TimeAvgData};

pub(crate) fn check_l2_data(expected_ds: &netcdf::File, l2_data: Level2Data) {
    // The UTC times are best done by converting the expected values to DateTime types and checking
    // equality, though this does assume millisecond precision between the netCDF file and the test.
    // Unfortunately, the test files from python all have different epochs (thanks, xarray) so we
    // do have to parse it.
    let expected_utc_times = read_time(&expected_ds, "time");
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
    compare_to_netcdf_values_eq::<i32, _>(&expected_ds, "flag", &l2_data.flag);
    compare_to_netcdf_values_eq::<i8, _>(&expected_ds, "public", &l2_data.is_public);
    compare_to_netcdf_quantities::<f32, _, _>(&expected_ds, "xco2_x2019", &l2_data.xgas);
    compare_to_netcdf_quantities::<f32, _, _>(
        &expected_ds,
        "xco2_error_x2019",
        &l2_data.xgas_error,
    );
    compare_to_netcdf_values_approx::<f32, _>(&expected_ds, "ak_xco2", &l2_data.avg_kernel);
    compare_to_netcdf_quantities::<f32, _, _>(&expected_ds, "ak_pressure", &l2_data.p_levels_ak);
    compare_to_netcdf_quantities::<f32, _, _>(&expected_ds, "prior_co2", &l2_data.prior_wet);
    compare_to_netcdf_quantities::<f32, _, _>(&expected_ds, "prior_h2o", &l2_data.prior_h2o_wet);
    compare_to_netcdf_quantities::<f32, _, _>(
        &expected_ds,
        "prior_pressure",
        &l2_data.p_levels_prior,
    );
}

pub(crate) fn check_timeavg_data(expected_ds: &netcdf::File, timeavg_data: TimeAvgData) {
    let expected_solar_times = read_time(&expected_ds, "bin_center_time");
    let expected_solar_ts = expected_solar_times.mapv(|t| t.timestamp_millis());
    let actual_solar_ts = timeavg_data
        .solar_mid_time
        .mapv(|t| t.and_utc().timestamp_millis());
    // The solar time is stored in the Python 'truth' file as an int, so strict equality should be safe
    assert_eq!(expected_solar_ts, actual_solar_ts);

    let expected_zpd_times = read_time(&expected_ds, "binned_xco2_time");
    let expected_zpd_ts = expected_zpd_times.mapv(|t| t.timestamp_millis());
    let actual_zpd_ts = timeavg_data.utc_mean_time.mapv(|t| t.timestamp_millis());
    // Allow ~2 second difference in the ZPD times; since we use the solar times for the obs IDs, the ZPD times don't matter as much.
    assert!(
        approx::abs_diff_eq!(expected_zpd_ts, actual_zpd_ts, epsilon = 2000),
        "Bin mean ZPD times do not match.\nExpected = {expected_zpd_times:?}\nActual = {:?}",
        timeavg_data.utc_mean_time
    );

    // obs_id and station_id aren't in the original Python "truth" file, so the best we can do right now
    // is check that the station_id is correct for Park Falls: 29 as of 31 Aug 2026
    let expected_obs_id = ndarray::Array1::from_elem(timeavg_data.station_id.dim(), 29);
    assert_eq!(timeavg_data.station_id, expected_obs_id);

    // Most of the remaining fields can be checked with a bit looser tolerance than in the L2, since there is some math.
    compare_to_netcdf_values_eq::<u64, _>(
        &expected_ds,
        "binned_xco2_num_spec",
        &timeavg_data.num_soundings,
    );
    compare_to_netcdf_values_eq::<i8, _>(&expected_ds, "public", &timeavg_data.public);
    compare_to_netcdf_quantities_with_tolerance::<f32, _, _>(
        &expected_ds,
        "long",
        &timeavg_data.longitude,
        1e-6,
    );
    compare_to_netcdf_quantities_with_tolerance::<f32, _, _>(
        &expected_ds,
        "lat",
        &timeavg_data.latitude,
        1e-6,
    );
    compare_to_netcdf_quantities_with_tolerance::<f32, _, _>(
        &expected_ds,
        "binned_xco2_sza",
        &timeavg_data.sza,
        1e-6,
    );
    compare_to_netcdf_quantities_with_tolerance::<f32, _, _>(
        &expected_ds,
        "binned_xco2_pout",
        &timeavg_data.p_surf,
        1e-6,
    );
    compare_to_netcdf_quantities_with_tolerance::<f32, _, _>(
        &expected_ds,
        "binned_co2_pres_prior",
        &timeavg_data.p_levels_prior,
        1e-6,
    );
    compare_to_netcdf_quantities_with_tolerance::<f32, _, _>(
        &expected_ds,
        "ak_pressure",
        &timeavg_data.p_levels_ak,
        1e-6,
    );
    compare_to_netcdf_quantities_with_tolerance::<f32, _, _>(
        &expected_ds,
        "binned_co2_h2o_prior",
        &timeavg_data.prior_h2o,
        1e-6,
    );
    // The Python 'truth' files only have the WMF prior. Checking the DMF prior will have to
    // wait until we replace the Python files.
    compare_to_netcdf_quantities_with_tolerance::<f32, _, _>(
        &expected_ds,
        "binned_co2_prior",
        &timeavg_data.prior_mixing_tccon,
        1e-6,
    );
    compare_to_netcdf_values_approx_with_tolerance::<f32, _>(
        &expected_ds,
        "binned_xco2_ak",
        &timeavg_data.avg_kernel,
        1e-6,
    );
    compare_to_netcdf_quantities_with_tolerance::<f32, _, _>(
        &expected_ds,
        "binned_xco2",
        &timeavg_data.column_mixing,
        1e-6,
    );

    // The uncertainty needs a bit looser tolerance to match the Python, because it involves more math
    // where slight implementation differences can add up.
    compare_to_netcdf_quantities_with_tolerance::<f32, _, _>(
        &expected_ds,
        "binned_xco2_std",
        &timeavg_data.sigma_column_mixing,
        4e-5,
    );
    // Assume we are using the X2019 CO2 scale to check that this propagates correctly.
    assert_eq!(timeavg_data.wmo_or_analagous_scale, "WMO CO2 X2019");
}

pub(crate) fn read_time(
    ds: &netcdf::File,
    varname: &str,
) -> ndarray::Array1<chrono::DateTime<chrono::Utc>> {
    nc_utils::get_var_cftime::<i64, Ix1>(ds, varname).expect("reading time should succeed")
}
