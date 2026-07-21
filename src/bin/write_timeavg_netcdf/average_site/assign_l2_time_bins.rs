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
