use ggg_rs::{
    nc_utils,
    test_utils::{
        compare_to_netcdf_quantities, compare_to_netcdf_values_approx, compare_to_netcdf_values_eq,
    },
};
use ndarray::Ix1;

use crate::data_structures::Level2Data;

pub(crate) fn check_l2_data(expected_ds: &netcdf::File, l2_data: Level2Data) {
    // The UTC times are best done by converting the expected values to DateTime types and checking
    // equality, though this does assume nanosecond precision between the netCDF file and the test.
    // Unfortunately, the test files from python all have different epochs (thanks, xarray) so we
    // do have to parse it.
    let time_var = expected_ds
        .variable("time")
        .expect("expected netCDF file should have 'time' variable");
    let epoch = get_expected_time_epoch(&time_var);
    let expected_utc_times = nc_utils::get_var_data::<i64, Ix1>(&expected_ds, "time")
        .expect("should be able to read 'time' from the file with the expected values")
        .mapv(|ts| epoch + chrono::Duration::nanoseconds(ts));
    let expected_ts = expected_utc_times.mapv(|t| t.timestamp_millis());
    let actual_ts = l2_data.utc_time.mapv(|t| t.timestamp_millis());
    assert!(
        approx::abs_diff_eq!(expected_ts, actual_ts, epsilon = 2),
        "UTC times do not match.\nExpected = {expected_utc_times:?}\nActual = {:?}",
        l2_data.utc_time
    );

    // The rest of the values can be directly compared to the expected ones in the netCDF file.
    // Note that it does not currently have the solar time, that will need added in the future.
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

fn get_expected_time_epoch(time_var: &netcdf::Variable) -> chrono::DateTime<chrono::Utc> {
    let units =
        nc_utils::get_string_attr(time_var, "units").expect("should be able to get time units");
    let (dur_str, epoch_str) = units
        .split_once(" since ")
        .expect("time units should have ' since ' in it");
    assert_eq!(dur_str, "nanoseconds");
    let epoch = chrono::NaiveDateTime::parse_from_str(epoch_str, "%Y-%m-%d %H:%M:%S%.f")
        .expect("should be able to parse the time units epoch");
    epoch.and_utc()
}
