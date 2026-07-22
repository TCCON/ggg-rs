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
