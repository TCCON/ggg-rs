use std::path::Path;

use error_stack::ResultExt;

use crate::{average_site::read_l2::SourceBinDef, CliError};

mod assign_l2_time_bins;
mod average_time_bins;
mod filter_l2;
mod filter_time_bins;
pub(super) mod read_l2;

pub(super) fn average_site_driver(
    l2_file: &Path,
    output_file: &Path,
    gas_defs: &[SourceBinDef],
    bin_width: chrono::Duration,
    min_obs_per_bin: Option<u64>,
) -> error_stack::Result<(), CliError> {
    let site_id = ggg_rs::utils::site_id_from_filename(l2_file)
        .change_context_lazy(|| CliError::context("Error getting site ID from filename"))?;

    let mut output_ds = netcdf::create(output_file).change_context_lazy(|| {
        CliError::Context(format!(
            "Error creating output file {}",
            output_file.display()
        ))
    })?;
    for var_def in gas_defs {
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

        let mut grp = output_ds
            .add_group(var_def.output_group)
            .change_context_lazy(|| {
                CliError::Custom(format!(
                    "Error creating group '{}' in output file {}",
                    var_def.output_group,
                    output_file.display()
                ))
            })?;
        binned_data.write_netcdf(&mut grp).change_context_lazy(|| {
            CliError::Context(format!(
                "Error writing data to the '{}' group in the output file {}",
                var_def.output_group,
                output_file.display()
            ))
        })?;

        // If this errors, it's a bug, because this variable should be written.
        // The most likely mistake is the variable's name changed.
        let mut bin_flag_var = grp
            .variable_mut("bin_flag")
            .expect("Could not get 'bin_flag' to add flag attributes after writing to the group.");
        bin_filterer
            .add_flag_attrs(&mut bin_flag_var)
            .change_context_lazy(|| {
                CliError::context("Error writing the additional bin_flag attributes")
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ggg_rs::test_utils::init_test_logging;
    use read_l2::co2_bin_def;

    #[test]
    fn test_site_avg_driver() {
        // This test actually is an end-to-end test, so even in the future if we make it possible
        // for the other tests to start from the previous step's expected file, this one should
        // always start from the private file.

        // Use tracing to see any errors from the WriteNetcdf implementation.
        init_test_logging(Some(log::LevelFilter::Trace));
        let test_dir = ggg_rs::test_utils::test_data_dir();
        let input_file =
            test_dir.join("inputs/write-timeavg-netcdf/averaging/pa_input_test.private.qc.nc");
        let output_file =
            test_dir.join("outputs/write-timeavg-netcdf/averaging/pa_output_test.private.qc.nc");
        average_site_driver(
            &input_file,
            &output_file,
            &[co2_bin_def()],
            chrono::Duration::minutes(30),
            None,
        )
        .expect("End-to-end run should not error");

        todo!("Output file still needs compared to 'truth'");
    }
}
