use error_stack::ResultExt;
use indexmap::IndexMap;
use itertools::Itertools;

use crate::{
    data_structures::{AllGasAvgData, Gas, TimeAvgData},
    CliError,
};

pub(super) struct DateRanges {
    pub(super) overall_start: chrono::NaiveDate,
    pub(super) overall_end: chrono::NaiveDate,
    pub(super) gas_ranges: IndexMap<Gas, (chrono::NaiveDate, chrono::NaiveDate)>,
}

impl DateRanges {
    pub(super) fn gas_has_date(&self, gas: &Gas, date: chrono::NaiveDate) -> bool {
        self.gas_ranges
            .get(gas)
            .is_some_and(|(d1, d2)| (date >= *d1) & (date <= *d2))
    }
}

pub(super) fn get_date_range_all_gases(
    all_site_data: &IndexMap<String, AllGasAvgData>,
    gases: &[Gas],
    start: Option<chrono::NaiveDate>,
    end: Option<chrono::NaiveDate>,
) -> error_stack::Result<DateRanges, CliError> {
    let mut gas_ranges = IndexMap::new();

    if gases.is_empty() {
        return Err(CliError::custom(
            "no gases provided, so no dates to identify",
        ))?;
    }

    for gas in gases {
        let it = all_site_data.values().map(|inner| {
            inner.get(gas)
            .expect("all_site_data should include all gases for all sites - this should be enforced by load_site_data")
        });
        let dates = get_date_range(it, start, end).change_context_lazy(|| {
            CliError::Context(format!("Error getting start/end dates for gas {gas}"))
        })?;
        gas_ranges.insert(*gas, dates);
    }
    let overall_start = gas_ranges
        .values()
        .map(|(d, _)| *d)
        .min()
        .expect("gases was not empty, so should get an overall start date");
    let overall_end = gas_ranges
        .values()
        .map(|(d, _)| *d)
        .max()
        .expect("gases was not empty, so should get an overall end date");

    Ok(DateRanges {
        overall_start,
        overall_end,
        gas_ranges,
    })
}

fn get_date_range<'i, I: Iterator<Item = &'i TimeAvgData>>(
    gas_data: I,
    start: Option<chrono::NaiveDate>,
    end: Option<chrono::NaiveDate>,
) -> error_stack::Result<(chrono::NaiveDate, chrono::NaiveDate), CliError> {
    let mut min_date: Option<chrono::NaiveDate> = None;
    let mut max_date: Option<chrono::NaiveDate> = None;
    for file in gas_data {
        let this_min = file.utc_mean_time.iter().min().map(|d| d.date_naive());
        let this_max = file.utc_mean_time.iter().max().map(|d| d.date_naive());
        if let Some(fmin) = this_min {
            min_date = min_date.and_then(|d| Some(d.min(fmin))).or(Some(fmin));
        }
        if let Some(fmax) = this_max {
            max_date = max_date.and_then(|d| Some(d.max(fmax))).or(Some(fmax));
        }
    }

    // The min/max logic is reverse from the above block; there, we wanted to find the maximum extend
    // of the input data. Here, we want the smaller of the extent requested and the available data.
    if let (Some(d1), Some(d2)) = (min_date, max_date) {
        let final_start = start.map(|ds| ds.max(d1)).unwrap_or(d1);
        let final_end = end.map(|de| de.min(d2)).unwrap_or(d2);
        Ok((final_start, final_end))
    } else {
        Err(CliError::custom(
            "Could not get start and end dates of the data, likely because no input files were provided",
        )
        .into())
    }
}

pub(super) fn extract_date_subset<'i, I: Iterator<Item = &'i TimeAvgData>>(
    all_site_data: I,
    date: chrono::NaiveDate,
) -> error_stack::Result<TimeAvgData, CliError> {
    let mut subsets = vec![];

    for data in all_site_data {
        let inds = data
            .utc_mean_time
            .iter()
            .enumerate()
            .filter_map(|(i, d)| {
                if d.date_naive() == date {
                    Some(i)
                } else {
                    None
                }
            })
            .collect_vec();
        let this_subset = data.subset(&inds);
        subsets.push(this_subset);
    }

    let day_data = TimeAvgData::concat(&subsets).change_context_lazy(|| {
        CliError::Context(format!("error combining site data for date {date}"))
    })?;

    // I ended up not adding a sorting macro to ggg-rs-derive because it doesn't look like
    // we can efficiently sort ndarrays in place - it might depend on the memory layout.
    // If this becomes a bottleneck in the future, we can revisit, but for now treat
    // sorting like a special case of subsetting.
    let sort_inds = make_sort_indices(&day_data);
    let day_data = day_data.subset(&sort_inds);

    Ok(day_data)
}

fn make_sort_indices(data: &TimeAvgData) -> Vec<usize> {
    let mut indices = (0..data.utc_mean_time.len()).collect_vec();
    // To maintain equivalence with the Python code, we want to sort by time and then station ID.
    // The original reasoning was: "it looks like Sourish's files sort obs by time, interleaving
    // sites if needed. I'll also sort by station ID to produce a repeatable order in case two bins
    // happen to have identical binned mean times."
    // I did not switch to sorting by the `obs_id` field, because that uses the _solar bin time_,
    // not the UTC time, so the ordering would not necessarily be the same as the Python code,
    // which does sort by UTC time then station ID.

    indices.sort_by_key(|&i| {
        let time = data.utc_mean_time[i];
        let mip_id = data.station_id[i];
        (time, mip_id)
    });
    indices
}

#[cfg(test)]
mod tests {
    use super::*;
    use ggg_rs::test_utils::{utc_dt_ymd, utc_dt_ymdhms};
    use ndarray::array;

    #[test]
    fn test_get_date_range() {
        // Create a vector of time-averaged data structures with UTC times, but nothing else -
        // those are all we need.
        let data = vec![
            TimeAvgData {
                utc_mean_time: array![
                    utc_dt_ymdhms(2026, 1, 1, 12, 30, 0),
                    utc_dt_ymdhms(2026, 2, 1, 13, 30, 0),
                    utc_dt_ymdhms(2026, 3, 1, 11, 30, 0)
                ],
                ..Default::default()
            },
            TimeAvgData {
                utc_mean_time: array![
                    utc_dt_ymdhms(2025, 12, 1, 12, 30, 0),
                    utc_dt_ymdhms(2026, 1, 1, 13, 30, 0),
                    utc_dt_ymdhms(2026, 2, 1, 11, 30, 0)
                ],
                ..Default::default()
            },
            TimeAvgData {
                utc_mean_time: array![
                    utc_dt_ymdhms(2026, 3, 1, 13, 30, 0),
                    utc_dt_ymdhms(2026, 4, 1, 11, 30, 0)
                ],
                ..Default::default()
            },
        ];

        // No manual limits, infer dates from a list of data
        let (a, b) = get_date_range(data.iter(), None, None).unwrap();
        assert_eq!(a, utc_dt_ymd(2025, 12, 1).date_naive());
        assert_eq!(b, utc_dt_ymd(2026, 4, 1).date_naive());

        // Single file
        let (a, b) = get_date_range(data[..1].iter(), None, None).unwrap();
        assert_eq!(a, utc_dt_ymd(2026, 1, 1).date_naive());
        assert_eq!(b, utc_dt_ymd(2026, 3, 1).date_naive());

        // Specified start & end outside the real data range;
        // range must equal the data's range
        let (a, b) = get_date_range(
            data.iter(),
            Some(utc_dt_ymd(2024, 1, 1).date_naive()),
            Some(utc_dt_ymd(2027, 1, 1).date_naive()),
        )
        .unwrap();
        assert_eq!(a, utc_dt_ymd(2025, 12, 1).date_naive());
        assert_eq!(b, utc_dt_ymd(2026, 4, 1).date_naive());

        // Specified start & end inside the real data range;
        // range limited to the specified range.
        let (a, b) = get_date_range(
            data.iter(),
            Some(utc_dt_ymd(2026, 1, 1).date_naive()),
            Some(utc_dt_ymd(2026, 3, 1).date_naive()),
        )
        .unwrap();
        assert_eq!(a, utc_dt_ymd(2026, 1, 1).date_naive());
        assert_eq!(b, utc_dt_ymd(2026, 3, 1).date_naive());

        // Only start or end specified (both in and out of range)
        let (a, b) =
            get_date_range(data.iter(), Some(utc_dt_ymd(2024, 1, 1).date_naive()), None).unwrap();
        assert_eq!(a, utc_dt_ymd(2025, 12, 1).date_naive());
        assert_eq!(b, utc_dt_ymd(2026, 4, 1).date_naive());

        let (a, b) =
            get_date_range(data.iter(), Some(utc_dt_ymd(2026, 1, 1).date_naive()), None).unwrap();
        assert_eq!(a, utc_dt_ymd(2026, 1, 1).date_naive());
        assert_eq!(b, utc_dt_ymd(2026, 4, 1).date_naive());

        let (a, b) =
            get_date_range(data.iter(), None, Some(utc_dt_ymd(2027, 1, 1).date_naive())).unwrap();
        assert_eq!(a, utc_dt_ymd(2025, 12, 1).date_naive());
        assert_eq!(b, utc_dt_ymd(2026, 4, 1).date_naive());

        let (a, b) =
            get_date_range(data.iter(), None, Some(utc_dt_ymd(2026, 2, 1).date_naive())).unwrap();
        assert_eq!(a, utc_dt_ymd(2025, 12, 1).date_naive());
        assert_eq!(b, utc_dt_ymd(2026, 2, 1).date_naive());
    }
}
