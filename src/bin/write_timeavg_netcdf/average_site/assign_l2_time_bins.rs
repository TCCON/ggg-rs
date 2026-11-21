use chrono::{DurationRound, NaiveDateTime};
use indexmap::IndexMap;
use ndarray::ArrayView1;

/// A map of the midpoint solar times of the bins to the indices of the level 2
/// data that go into that bin.
pub(crate) type TimeBins = IndexMap<NaiveDateTime, Vec<usize>>;

pub(crate) trait TimeBinAssigner {
    fn assign_solar_time_bin(
        &self,
        solar_times: ArrayView1<NaiveDateTime>,
        bin_width: chrono::Duration,
    ) -> TimeBins;
}

pub(crate) fn get_l2_time_binner_for_site(site_id: &str) -> Box<dyn TimeBinAssigner> {
    let binner = match site_id {
        _ => BaseTimeBinAssigner,
    };
    Box::new(binner)
}

pub(crate) struct BaseTimeBinAssigner;

impl TimeBinAssigner for BaseTimeBinAssigner {
    fn assign_solar_time_bin(
        &self,
        solar_times: ArrayView1<NaiveDateTime>,
        bin_width: chrono::Duration,
    ) -> TimeBins {
        let mut bins: TimeBins = IndexMap::new();

        let bin_half_width = bin_width / 2;
        for (itime, time) in solar_times.into_iter().enumerate() {
            // As far as I can tell, the only errors we get are if the rounded time goes outside
            // the valid range to represent a unix timestamp in nanoseconds or the duration itself
            // can't be represented as signed nanoseconds. We should not encounter either problem.
            let bin_time = time.duration_trunc(bin_width)
                .expect("Bin width should not cause a time to round outside the range of values representable in nanoseconds");
            bins.entry(bin_time + bin_half_width)
                .or_default()
                .push(itime);
        }

        bins
    }
}

#[cfg(test)]
mod tests {
    use indexmap::IndexMap;

    use super::{BaseTimeBinAssigner, TimeBinAssigner};
    use crate::{
        average_site::{
            filter_l2::{BaseDataFilterer, L2DataFilterer},
            read_l2::{co2_bin_def, read_file},
        },
        common_test::read_time,
    };

    #[test]
    fn test_solar_time_bins() {
        let test_dir = ggg_rs::test_utils::test_data_dir();
        // For now, it's easier to start from the private file. This does mean that this is more
        // of an integration test. In the future, I should have the read_l2 test be able to save
        // its output, though that may be easier in GGG2020.2 when the private and public files
        // are a bit more unified, so the result of reading it can be treated like a private file.
        let input_file =
            test_dir.join("inputs/write-timeavg-netcdf/averaging/pa_input_test.private.qc.nc");
        let expected_file =
            test_dir.join("expected/write-timeavg-netcdf/averaging/03_time_binned.nc");

        let l2_data =
            read_file(&input_file, &co2_bin_def()).expect("Reading test input fail should succeed");
        let filterer = BaseDataFilterer::default();
        let filtered_l2_data = filterer.subset_l2_data(l2_data);
        let solar_time_bins = BaseTimeBinAssigner.assign_solar_time_bin(
            filtered_l2_data.solar_time.view(),
            chrono::Duration::minutes(30),
        );

        let expected_ds = netcdf::open(expected_file)
            .expect("Should be able to open the file with expected values");
        let expected_solar_times = read_time(&expected_ds, "solar_time_bin");

        // In the test file (derived from Python), the solar_time_bin variable has one entry per spectrum
        // giving the solar time for that bin. In Rust, we map solar times to the indices of the spectra they
        // include, so we need to construct the expected Rust value from the Python "truth".
        let mut expected_solar_time_bins: IndexMap<chrono::NaiveDateTime, Vec<usize>> =
            IndexMap::new();
        for (iexpected, expected_stime) in expected_solar_times.into_iter().enumerate() {
            expected_solar_time_bins
                .entry(expected_stime.naive_utc())
                .or_default()
                .push(iexpected);
        }
        assert_eq!(expected_solar_time_bins, solar_time_bins);
    }
}
