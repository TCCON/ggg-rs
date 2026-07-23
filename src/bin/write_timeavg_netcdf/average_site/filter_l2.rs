use itertools::izip;
use uom::si::{angle::degree, f32::Angle};

use crate::data_structures::Level2Data;

pub(crate) trait L2DataFilterer {
    fn get_good_spectra(&self, data: &Level2Data) -> Vec<usize>;
    fn subset_l2_data(&self, data: Level2Data) -> Level2Data {
        let ii_good = self.get_good_spectra(&data);
        data.subset(&ii_good)
    }
}

pub(crate) fn get_l2_filterer_for_site(site_id: &str) -> Box<dyn L2DataFilterer> {
    let filterer = match site_id {
        _ => BaseDataFilterer::default(),
    };
    Box::new(filterer)
}

pub(crate) struct BaseDataFilterer {
    sza_upper_limit: Angle,
}

impl Default for BaseDataFilterer {
    fn default() -> Self {
        Self {
            sza_upper_limit: Angle::new::<degree>(80.0),
        }
    }
}

impl L2DataFilterer for BaseDataFilterer {
    fn get_good_spectra(&self, data: &Level2Data) -> Vec<usize> {
        let mut indices = vec![];
        for (i, &flag, &sza, &xgas, &xgas_error) in izip!(
            0..data.utc_time.len(),
            data.flag.iter(),
            data.sza.iter(),
            data.xgas.iter(),
            data.xgas_error.iter()
        ) {
            // Excluding NaN and infinities in xgas and xgas_error should help avoid
            // issues with averaging down the road.
            if flag == 0
                && sza <= self.sza_upper_limit
                && xgas.is_finite()
                && xgas_error.is_finite()
            {
                indices.push(i);
            }
        }
        indices
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        average_site::{
            filter_l2::{BaseDataFilterer, L2DataFilterer},
            read_l2::{read_file, CO2_VAR_DEF},
        },
        common_test::check_l2_data,
    };

    #[test]
    fn test_read_and_filter_pa() {
        let test_dir = ggg_rs::test_utils::test_data_dir();
        // For now, it's easier to start from the private file. This does mean that this is more
        // of an integration test. In the future, I should have the read_l2 test be able to save
        // its output, though that may be easier in GGG2020.2 when the private and public files
        // are a bit more unified, so the result of reading it can be treated like a private file.
        let input_file =
            test_dir.join("inputs/write-timeavg-netcdf/averaging/pa_input_test.private.qc.nc");
        let expected_file =
            test_dir.join("expected/write-timeavg-netcdf/averaging/02_raw_filtered.nc");

        let l2_data =
            read_file(&input_file, &CO2_VAR_DEF).expect("Reading test input fail should succeed");
        let filterer = BaseDataFilterer::default();
        let filtered_l2_data = filterer.subset_l2_data(l2_data);

        let expected_ds = netcdf::open(expected_file)
            .expect("Should be able to open the file with expected values");

        check_l2_data(&expected_ds, filtered_l2_data);
    }
}
