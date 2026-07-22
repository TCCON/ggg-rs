use std::{unimplemented, unreachable};

use chrono::{Datelike, NaiveDateTime, Timelike};
use ggg_rs::array_ops::weighted_mean;
use ndarray::{Array1, ArrayView1, Axis};
use ndarray_stats::{QuantileExt, SummaryStatisticsExt};
use uom::si::f32::Ratio;

use crate::{
    average_site::assign_l2_time_bins::TimeBins,
    data_structures::{Level2Data, TimeAvgData},
};

pub(crate) trait TimeBinAverager {
    fn compute_bins(&self, l2_data: &Level2Data, time_bins: &TimeBins, sigma: f64) -> TimeAvgData;
}

pub(crate) fn get_time_bin_averager_for_site(site_id: &str) -> Box<dyn TimeBinAverager> {
    let averager = match site_id {
        _ => BaseTimeBinAverager,
    };
    Box::new(averager)
}

pub(crate) struct BaseTimeBinAverager;

impl TimeBinAverager for BaseTimeBinAverager {
    fn compute_bins(&self, l2_data: &Level2Data, time_bins: &TimeBins, sigma: f64) -> TimeAvgData {
        let solar_times = get_ordered_solar_times(time_bins);
        let nlev = l2_data.p_levels_prior.dim().1;
        let mut avg_data =
            TimeAvgData::new_with_bins(solar_times.clone(), nlev, l2_data.xgas_wmo_scale.clone());

        // AK pressures (for now) are a constant vector, so no binning needed
        avg_data.p_levels_ak = l2_data.p_levels_ak.clone();

        // We need the median error to scale the individual errors
        let median_xgas_error = get_median_xgas_error(l2_data.xgas_error.view());

        for (ibin, bin_time) in solar_times.iter().enumerate() {
            let l2_indices = time_bins.get(bin_time)
                .expect("Since the list of solar times was constructed from `time_bins`, all solar times should be in `time_bins`!");
            assign_one_bin(
                &mut avg_data,
                ibin,
                *bin_time,
                l2_data,
                l2_indices,
                median_xgas_error,
                sigma,
            );
        }
        avg_data
    }
}

fn get_ordered_solar_times(time_bins: &TimeBins) -> Array1<NaiveDateTime> {
    let mut times = Vec::from_iter(time_bins.keys().copied());
    times.sort_unstable();
    Array1::from_vec(times)
}

fn get_median_xgas_error(xgas_error: ArrayView1<Ratio>) -> Ratio {
    let mut xgas_error_base_units = xgas_error.mapv(|v| v.get::<uom::si::ratio::ratio>());
    let median_res = xgas_error_base_units.quantile_axis_skipnan_mut(
        ndarray::Axis(0),
        noisy_float::types::n64(0.5),
        &ndarray_stats::interpolate::Nearest,
    );

    let median = match median_res {
        Ok(m) => {
            if m.len() != 1 {
                unimplemented!("Should only get a scalar median from a 1D array");
            }
            m.into_scalar()
        }
        Err(ndarray_stats::errors::QuantileError::EmptyInput) => {
            log::warn!("Got an empty bin, returning a median Xgas error of 0");
            0.0
        }
        Err(ndarray_stats::errors::QuantileError::InvalidQuantile(q)) => {
            unreachable!("Invalid quantile {q} - should not happen!")
        }
    };
    Ratio::new::<uom::si::ratio::ratio>(median)
}

fn assign_one_bin(
    avg_data: &mut TimeAvgData,
    avg_index: usize,
    solar_mid_time: NaiveDateTime,
    l2_data: &Level2Data,
    l2_indices: &[usize],
    median_xgas_error: Ratio,
    xgas_error_sigma: f64,
) {
    if l2_indices.is_empty() {
        // Leave the things that depend on the L2 indices as fill values
        return;
    }
    let i1d = ndarray::s![avg_index];
    let i2d = ndarray::s![avg_index, ..];
    let bin_xgas_errors = l2_data.xgas_error.select(Axis(0), l2_indices);
    let weights = bin_xgas_errors.mapv(|v| 1.0 / v.get::<uom::si::ratio::ratio>() as f64);

    // For the UTC times, we need to convert to timestamps and back for the averaging to work.
    let utc_timestamps = l2_data
        .utc_time
        .select(Axis(0), l2_indices)
        .mapv(|t| t.timestamp() as f64);
    let mean_timestamps = weighted_mean(&utc_timestamps, &weights).unwrap().mapv(|v| {
        chrono::DateTime::from_timestamp(v as i64, 0)
            .expect("mean timestamp should not be out of range")
    });
    avg_data
        .utc_mean_time
        .slice_mut(i1d)
        .assign(&mean_timestamps);

    // The obs ID is constructed from the bin center time and the station ID
    // The station ID can just be copied
    avg_data
        .obs_id
        .slice_mut(i1d)
        .fill(construct_obs_id(solar_mid_time, l2_data.station_id));
    avg_data.station_id.slice_mut(i1d).fill(l2_data.station_id);

    let all_public = l2_data
        .is_public
        .fold(1, |acc, &el| if acc == 0 { 0 } else { el });
    avg_data.public.slice_mut(i1d).fill(all_public);

    // The rest of the numerical quantities are straightforward
    let v = weighted_mean(&l2_data.latitude.select(Axis(0), l2_indices), &weights).unwrap();
    avg_data.latitude.slice_mut(i1d).assign(&v);

    let v = weighted_mean(&l2_data.longitude.select(Axis(0), l2_indices), &weights).unwrap();
    avg_data.longitude.slice_mut(i1d).assign(&v);

    let v = weighted_mean(&l2_data.sza.select(Axis(0), l2_indices), &weights).unwrap();
    avg_data.sza.slice_mut(i1d).assign(&v);

    let v = weighted_mean(&l2_data.p_surf.select(Axis(0), l2_indices), &weights).unwrap();
    avg_data.p_surf.slice_mut(i1d).assign(&v);

    let v = weighted_mean(
        &l2_data.p_levels_prior.select(Axis(0), l2_indices),
        &weights,
    )
    .unwrap();
    avg_data.p_levels_prior.slice_mut(i2d).assign(&v);

    let v = weighted_mean(&l2_data.prior_h2o_wet.select(Axis(0), l2_indices), &weights).unwrap();
    avg_data.prior_h2o.slice_mut(i2d).assign(&v);

    let v = weighted_mean(&l2_data.prior_dry.select(Axis(0), l2_indices), &weights).unwrap();
    avg_data.prior_mixing.slice_mut(i2d).assign(&v);

    let v = weighted_mean(&l2_data.prior_wet.select(Axis(0), l2_indices), &weights).unwrap();
    avg_data.prior_mixing_tccon.slice_mut(i2d).assign(&v);

    let v = weighted_mean(&l2_data.avg_kernel.select(Axis(0), l2_indices), &weights).unwrap();
    avg_data.avg_kernel.slice_mut(i2d).assign(&v);

    let v = weighted_mean(&l2_data.xgas.select(Axis(0), l2_indices), &weights).unwrap();
    avg_data.column_mixing.slice_mut(i1d).assign(&v);

    // The bin uncertainty is a little different. If we only had
    // one observation, just use its uncertainty. But if we have
    // more than one, calculate a value based on the standard deviation
    // of the Xgas values.
    let v = compute_bin_uncertainty(
        l2_data.xgas.select(Axis(0), l2_indices).view(),
        l2_data.xgas_error.select(Axis(0), l2_indices).view(),
        weights.view(),
        median_xgas_error,
        xgas_error_sigma,
    );
    avg_data.sigma_column_mixing.slice_mut(i1d).fill(v);
}

fn compute_bin_uncertainty(
    xgas: ArrayView1<uom::si::f32::Ratio>,
    xgas_errors: ArrayView1<uom::si::f32::Ratio>,
    weights: ArrayView1<f64>,
    median_xgas_error: Ratio,
    sigma: f64,
) -> uom::si::f32::Ratio {
    if xgas_errors.is_empty() {
        return uom::si::f32::Ratio::new::<uom::si::ratio::ratio>(0.0);
    }
    if xgas_errors.len() == 1 {
        return *xgas_errors.get(0).unwrap();
    }

    // If there is more than one observation, then take the bin error as
    // the weighted standard deviation of the bin scaled by the ratio
    // of the mean of the errors to the median Xgas ratio across all
    // the data, and reduced by sqrt(n).
    let nobs = xgas_errors.len() as f64;
    let plain_xgas = xgas.mapv(|q| q.get::<uom::si::ratio::ratio>() as f64);
    let plain_errors = xgas_errors.mapv(|q| q.get::<uom::si::ratio::ratio>() as f64);
    let w_std = plain_xgas.weighted_std(&weights, 0.0).expect(
        "weighted std. dev. should not error as we confirmed the array had at least 2 elements",
    );
    let mean_error = plain_errors
        .mean()
        .expect("mean should not fail since we confirmed the array as at least two elements");
    let median_xgas_error = median_xgas_error.get::<uom::si::ratio::ratio>() as f64;
    let scale = mean_error / median_xgas_error / nobs.sqrt();
    let plain_bin_error = w_std * scale * sigma;
    uom::si::f32::Ratio::new::<uom::si::ratio::ratio>(plain_bin_error as f32)
}

fn construct_obs_id(solar_mid_time: NaiveDateTime, station_id: i8) -> i64 {
    let year = solar_mid_time.year() as i64 * 1_000_000_000_000;
    let month = solar_mid_time.month() as i64 * 10_000_000_000;
    let day = solar_mid_time.day() as i64 * 100_000_000;
    let hour = solar_mid_time.hour() as i64 * 1_000_000;
    let minute = solar_mid_time.minute() as i64 * 10_000;
    let second = solar_mid_time.second() as i64 * 100;
    let station = station_id as i64;

    year + month + day + hour + minute + second + station
}
