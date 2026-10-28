use std::path::Path;

use error_stack::ResultExt;

use crate::{average_site::read_l2::SourceBinDef, data_structures::AllSitesMetaAttrs, CliError};

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
    let site_meta = read_l2::read_tccon_metadata(l2_file).change_context_lazy(|| {
        CliError::Context(format!("Error reading metadata from {}", l2_file.display()))
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

        // Creating this every time through the loop is a little wasteful, but I
        // decided that it's worth the cleaner interface of the L2 reader handling
        // the site_id -> station_id mapping rather than passing the station ID to
        // the L2 reader function stack.
        let meta_for_write = AllSitesMetaAttrs::from_one(
            site_meta.clone(),
            site_id.clone(),
            filtered_l2_data.station_id,
        );

        binned_data
            .write_netcdf_context(&mut grp, &meta_for_write)
            .change_context_lazy(|| {
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

    let mut out_root = output_ds.root_mut().ok_or_else(|| {
        CliError::Custom(format!(
            "Could not get root group in output file {}",
            output_file.display()
        ))
    })?;

    // Lastly, create the history and creation time attributes.
    let create_time = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%z").to_string();
    let history = format!(
        "{}: Per-site time average file {} created by write_timeavg_netcdf from v{} of ggg-rs",
        create_time,
        output_file
            .file_name()
            .map(|n| n.to_string_lossy())
            .unwrap_or_else(|| "?".to_string().into()),
        clap::crate_version!()
    );
    out_root
        .add_attribute("history", history)
        .change_context_lazy(|| {
            CliError::Context(format!(
                "error writing history attribute to {}",
                output_file.display()
            ))
        })?;
    out_root
        .add_attribute("creation_time", create_time)
        .change_context_lazy(|| {
            CliError::Context(format!(
                "error writing creation_time attribute to {}",
                output_file.display()
            ))
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        common_test::{check_station_meta_attrs, check_timeavg_data, CheckTimeAvgFields},
        data_structures::{Gas, TimeAvgData},
    };
    use ggg_rs::test_utils::{compare_nc_attrs, init_test_logging, NCDF_F32_DEFAULT_FILL};
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
        let expected_file =
            test_dir.join("expected/write-timeavg-netcdf/averaging/06_saved_output.nc");
        average_site_driver(
            &input_file,
            &output_file,
            &[co2_bin_def()],
            chrono::Duration::minutes(30),
            None,
        )
        .expect("End-to-end run should not error");

        let output_ds = netcdf::open(output_file).unwrap();
        let mut timeavg_data =
            TimeAvgData::read_netcdf(&output_ds.group("CO2").unwrap().unwrap()).unwrap();

        // Since the fill values do not have physical meaning, construct the quantity
        // directly to preserve the value.
        let col_avg_fill = uom::si::f32::Ratio {
            dimension: std::marker::PhantomData,
            units: std::marker::PhantomData,
            value: NCDF_F32_DEFAULT_FILL,
        };
        ndarray::azip!((x in &mut timeavg_data.column_mixing, flg in &timeavg_data.bin_flag) {
            if *flg > 0 {
                *x = col_avg_fill;
            }
        });
        let expected_ds = netcdf::open(expected_file).unwrap();
        let test_grp = output_ds.group(Gas::CO2.output_group()).unwrap().unwrap();

        let fields = CheckTimeAvgFields {
            solar_time: "solar_time_bin",
            ..CheckTimeAvgFields::default_vs_python_for_gas(&Gas::CO2)
        };

        check_timeavg_data(&expected_ds.root().unwrap(), timeavg_data, &fields);

        // Last, check the values of the key metadata attributes that we should copy over
        // The test uses Park Falls, which is station ID 29 as of 27 Oct 2029
        check_station_meta_attrs(&expected_ds, &test_grp, 29);
    }
}
