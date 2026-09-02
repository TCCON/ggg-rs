use uom::si::f32::Ratio;

use crate::data_structures::TimeAvgData;

pub(crate) trait BinFilterer {
    fn filter_bins(&self, data: TimeAvgData) -> TimeAvgData;
    fn add_flag_attrs(
        &self,
        bin_flag_var: &mut netcdf::VariableMut<'_>,
    ) -> Result<(), netcdf::Error>;
}

pub(crate) fn get_bin_filterer_for_site(
    site_id: &str,
    stddev_limit: Ratio,
    min_obs_per_bin: Option<u64>,
) -> Box<dyn BinFilterer> {
    let filterer = match site_id {
        _ => BaseBinFilterer::new(stddev_limit, min_obs_per_bin),
    };
    Box::new(filterer)
}

pub(crate) struct BaseBinFilterer {
    min_obs_per_bin: u64,
    stddev_limit: Ratio,
}

impl BaseBinFilterer {
    pub(crate) fn new(stddev_limit: Ratio, min_obs_per_bin: Option<u64>) -> Self {
        Self {
            min_obs_per_bin: min_obs_per_bin.unwrap_or(5),
            stddev_limit,
        }
    }
}

impl BinFilterer for BaseBinFilterer {
    fn filter_bins(&self, mut data: TimeAvgData) -> TimeAvgData {
        // Compared to the Python version, this object does not accept a parameter
        // to set a cutoff date. I decided that it doesn't make sense to do that when
        // generating the per-site files; that should be done when making the daily
        // files.
        for i in 0..data.bin_flag.len() {
            if data.num_soundings[i] < self.min_obs_per_bin {
                data.bin_flag[i] |= 1;
            }
            // Exclude data where the column value or standard deviation is not a valid number
            if !data.column_mixing[i].is_finite() || !data.sigma_column_mixing[i].is_finite() {
                data.bin_flag[i] |= 2;
            }
            // Exclude data where the bin's standard deviation exceeds the allowed limit
            if data.sigma_column_mixing[i] > self.stddev_limit {
                data.bin_flag[i] |= 4;
            }
        }
        data
    }

    fn add_flag_attrs(
        &self,
        bin_flag_var: &mut netcdf::VariableMut<'_>,
    ) -> Result<(), netcdf::Error> {
        bin_flag_var.put_attribute("flag_masks", vec![1_u64, 2_u64, 4_u64])?;
        bin_flag_var.put_attribute(
            "flag_meanings",
            "too_few_soundings non_finite_column_mean_or_std column_std_exceeds_limit",
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use ggg_rs::{nc_utils, test_utils::init_test_logging};
    use ndarray::Ix1;

    use crate::{
        average_site::{
            assign_l2_time_bins::{BaseTimeBinAssigner, TimeBinAssigner},
            average_time_bins::{BaseTimeBinAverager, TimeBinAverager},
            filter_l2::{BaseDataFilterer, L2DataFilterer},
            filter_time_bins::{BaseBinFilterer, BinFilterer},
            read_l2::{co2_bin_def, read_file},
        },
        common_test::read_time,
    };

    #[test]
    fn test_bin_filtering() {
        // This test actually could read in the expected file for the previous step,
        // but since that came from Python, I don't want to deal with potential NaNs.
        // So we'll keep the integration-like test for now.
        init_test_logging(None);
        let test_dir = ggg_rs::test_utils::test_data_dir();
        // For now, it's easier to start from the private file. This does mean that this is more
        // of an integration test. In the future, I should have the read_l2 test be able to save
        // its output, though that may be easier in GGG2020.2 when the private and public files
        // are a bit more unified, so the result of reading it can be treated like a private file.
        let input_file =
            test_dir.join("inputs/write-timeavg-netcdf/averaging/pa_input_test.private.qc.nc");
        let expected_file =
            test_dir.join("expected/write-timeavg-netcdf/averaging/05_bins_filtered.nc");

        let l2_data =
            read_file(&input_file, &co2_bin_def()).expect("Reading test input fail should succeed");
        let filterer = BaseDataFilterer::default();
        let filtered_l2_data = filterer.subset_l2_data(l2_data);
        let solar_time_bins = BaseTimeBinAssigner.assign_solar_time_bin(
            filtered_l2_data.solar_time.view(),
            chrono::Duration::minutes(30),
        );

        // The Python test 'truth' file used sigma = 1, so we do so here even if we normally want to use 2
        let binned_data =
            BaseTimeBinAverager.compute_bins(&filtered_l2_data, &solar_time_bins, 1.0);
        let binned_data =
            BaseBinFilterer::new(co2_bin_def().stddev_limit, None).filter_bins(binned_data);

        // In the Python code, we just set the binned Xgas to NaN for bins to ignore, but in Rust we
        // have a proper bin flag, so we need to check that the same bins have the flag set here as
        // are NaNs (or fill values) in the 'truth' file.
        let expected_ds =
            netcdf::open(expected_file).expect("Should be able to open the expected file");
        let expected_solar_times =
            read_time(&expected_ds, "bin_center_time").mapv(|t| t.naive_utc());
        assert_eq!(
            expected_solar_times, binned_data.solar_mid_time,
            "Solar times for the bins must match between the files"
        );

        let expected_xco2 = nc_utils::get_var_data::<f32, Ix1>(&expected_ds, "binned_xco2")
            .expect("Should be able to read 'binned_xco2' from the expected file");
        let expected_bad = expected_xco2.mapv(|x| x.is_nan());
        let actual_bad = binned_data.bin_flag.mapv(|f| f > 0);
        assert_eq!(
            expected_bad, actual_bad,
            "Bins marked as bad do not agree between expected and actual"
        );
    }
}
