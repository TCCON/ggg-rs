use std::path::Path;

use error_stack::ResultExt;

use crate::CliError;

mod assign_l2_time_bins;
mod average_time_bins;
mod filter_l2;
mod filter_time_bins;
mod read_l2;

pub(super) fn average_site_driver(
    l2_file: &Path,
    output_file: &Path,
    bin_width: chrono::Duration,
    min_obs_per_bin: Option<u64>,
) -> error_stack::Result<(), CliError> {
    let site_id = ggg_rs::utils::site_id_from_filename(l2_file)
        .change_context_lazy(|| CliError::context("Error getting site ID from filename"))?;

    for var_def in [read_l2::co2_bin_def(), read_l2::ch4_bin_def()] {
        let l2_filterer = filter_l2::get_l2_filterer_for_site(&site_id);
        let l2_binner = assign_l2_time_bins::get_l2_time_binner_for_site(&site_id);
        let bin_averager = average_time_bins::get_time_bin_averager_for_site(&site_id);
        let bin_filterer = filter_time_bins::get_bin_filterer_for_site(
            &site_id,
            var_def.stddev_limit,
            min_obs_per_bin,
        );

        let l2_data = read_l2::read_file(l2_file, &var_def).change_context_lazy(|| {
            CliError::context(format!("Error reading file {}", l2_file.display()))
        })?;
        let filtered_l2_data = l2_filterer.subset_l2_data(l2_data);
        let l2_time_bins =
            l2_binner.assign_solar_time_bin(filtered_l2_data.solar_time.view(), bin_width);
        let binned_data = bin_averager.compute_bins(&filtered_l2_data, &l2_time_bins, 1.0);
        let binned_data = bin_filterer.filter_bins(binned_data);
    }
    Ok(())
}
