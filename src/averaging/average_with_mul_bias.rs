use indexmap::IndexMap;
use log::warn;
use ndarray::Array1;

use crate::utils::GggError;

use super::Averager;

/// Average windows assumed to have a multiplicative bias w.r.t. each other that must be calculated.
pub struct MulBiasAverager;

impl Averager for MulBiasAverager {
    fn average_windows<S: AsRef<str>>(
        &self,
        window_values: ndarray::ArrayView2<f64>,
        error_values: ndarray::ArrayView2<f64>,
        window_names: &[S],
        missing_value: f64,
    ) -> Result<super::AveragingResult, crate::utils::GggError> {
        let (_, nwin) = window_values.dim();
        let scale_factors = Array1::from_elem(nwin, 1.0);
        let scale_factor_errors = Array1::from_elem(nwin, 5.0);
        Ok(iterative_mul_bias(
            window_values,
            error_values,
            scale_factors,
            scale_factor_errors,
            window_names,
            missing_value,
            25,
        ))
    }
}

/// Average windows assumed to have a multiplicative bias w.r.t. each other that has already been calculated.
pub struct PresetMulBiasAverager {
    scale_factors: IndexMap<String, f64>,
}

impl PresetMulBiasAverager {
    pub fn new(scale_factors: IndexMap<String, f64>) -> Self {
        Self { scale_factors }
    }

    fn get_window_scale_factors<S: AsRef<str>>(
        &self,
        window_names: &[S],
    ) -> Result<Array1<f64>, GggError> {
        let mut sf = Vec::with_capacity(window_names.len());

        for name in window_names {
            let this_sf = self.scale_factors.get(name.as_ref()).ok_or_else(|| {
                GggError::custom(format!(
                    "Unable to find preset scale factor for window '{}'",
                    name.as_ref()
                ))
            })?;
            sf.push(*this_sf);
        }

        Ok(Array1::from_vec(sf))
    }
}

impl Averager for PresetMulBiasAverager {
    fn average_windows<S: AsRef<str>>(
        &self,
        window_values: ndarray::ArrayView2<f64>,
        error_values: ndarray::ArrayView2<f64>,
        window_names: &[S],
        missing_value: f64,
    ) -> Result<super::AveragingResult, crate::utils::GggError> {
        let scale_factors = self.get_window_scale_factors(window_names)?;
        let scale_factor_errors = Array1::from_elem(scale_factors.dim(), 5.0);
        Ok(iterative_mul_bias(
            window_values,
            error_values,
            scale_factors,
            scale_factor_errors,
            window_names,
            missing_value,
            1,
        ))
    }
}

fn iterative_mul_bias<S: AsRef<str>>(
    window_values: ndarray::ArrayView2<f64>,
    error_values: ndarray::ArrayView2<f64>,
    mut scale_factors: ndarray::Array1<f64>,
    mut scale_factor_errors: ndarray::Array1<f64>,
    window_names: &[S],
    missing_value: f64,
    max_num_iter: usize,
) -> super::AveragingResult {
    let (n_spec, n_win) = window_values.dim();
    let mut mean_values = Array1::from_elem(n_spec, missing_value);
    let mut mean_errors = Array1::from_elem(n_spec, missing_value);

    let mut win_has_vals = Array1::from_elem(n_win, false);
    let a_priori_scale_factor_errors = scale_factor_errors.clone();
    let mut numerator_all = Array1::from_iter(
        a_priori_scale_factor_errors
            .iter()
            .map(|aperr| 1.0 / aperr.powi(2)),
    );
    let mut denominator_all = Array1::from_iter(
        a_priori_scale_factor_errors
            .iter()
            .map(|aperr| 1.0 / aperr.powi(2)),
    );

    for _ in 0..max_num_iter {
        for ispec in 0..n_spec {
            let mut n_obs_this_spec = 0;
            let mut numerator_spec = 0.0;
            let mut denominator_spec = 0.0;
            for iwin in 0..n_win {
                // let mut numerator_win = 1.0 / a_priori_scale_factor_errors[iwin].powi(2);
                // let mut denominator_win = 1.0 / a_priori_scale_factor_errors[iwin].powi(2);

                if approx::abs_diff_ne!(window_values[(ispec, iwin)], missing_value) {
                    let sf = scale_factors[iwin];
                    let y = window_values[(ispec, iwin)];
                    let yerr_sq = error_values[(ispec, iwin)].powi(2);
                    n_obs_this_spec += 1;
                    numerator_spec += sf * y / yerr_sq;
                    denominator_spec += sf.powi(2) / yerr_sq;
                }
            }

            if n_obs_this_spec > 0 {
                mean_values[ispec] = numerator_spec / denominator_spec;
                mean_errors[ispec] = 1.0 / denominator_spec.sqrt();
            }

            for iwin in 0..n_win {
                if approx::abs_diff_ne!(window_values[(ispec, iwin)], missing_value) {
                    numerator_all += mean_values[ispec] * window_values[(ispec, iwin)]
                        / error_values[(ispec, iwin)].powi(2);
                    denominator_all += (mean_values[ispec] / error_values[(ispec, iwin)]).powi(2);
                    win_has_vals[iwin] = true;
                }
            }
        }

        for iwin in 0..n_win {
            if win_has_vals[iwin] {
                scale_factors[iwin] = numerator_all[iwin] / denominator_all[iwin];
                scale_factor_errors[iwin] = 1.0 / denominator_all[iwin].powi(2);
            } else {
                warn!("No data for window {}", window_names[iwin].as_ref());
            }
        }
    }

    super::AveragingResult {
        values: mean_values,
        errors: mean_errors,
        adjustment_factors: scale_factors,
    }
}

#[cfg(test)]
mod tests {
    use core::f64;
    use std::hash::RandomState;

    use indexmap::IndexMap;
    use ndarray::array;

    use crate::averaging::{
        average_with_mul_bias::{MulBiasAverager, PresetMulBiasAverager},
        Averager,
    };

    #[test]
    fn test_preset_scale_mul_avg() {
        // Data from the 3 CH4 windows (5938, 6002, 6076)
        let col_vals = array!(
            [3.53152E19, 3.5241E19, 3.49562E19],
            [3.55762E19, 3.54595E19, 3.51951E19],
            [3.62114E19, 3.60018E19, 3.5926E19],
            [3.6256E19, 3.60751E19, 3.59989E19]
        );

        let err_vals = array![
            [2.63685E17, 2.70717E17, 2.4259E17],
            [2.81296E17, 3.12942E17, 2.70747E17],
            [2.61072E17, 2.97332E17, 2.50194E17],
            [2.53876E17, 2.90144E17, 2.42996E17]
        ];

        let sfs = IndexMap::<String, f64, RandomState>::from_iter([
            ("ch4_5938".to_string(), 1.005),
            ("ch4_6002".to_string(), 1.000),
            ("ch4_6076".to_string(), 0.995),
        ]);

        let averager = PresetMulBiasAverager::new(sfs);
        let results = averager
            .average_windows(
                col_vals.view(),
                err_vals.view(),
                &["ch4_5938", "ch4_6002", "ch4_6076"],
                f64::MAX,
            )
            .unwrap();

        let expected_vals = array![3.51674E+19, 3.54060E+19, 3.60517E+19, 3.61149E+19];
        let expected_errs = array![1.49081E+17, 1.65563E+17, 1.54403E+17, 1.50217E+17];
        // Chose the epsilon value to the comparison at the last decimal place of the expected arrays.
        approx::assert_abs_diff_eq!(results.values, expected_vals, epsilon = 1e14);
        approx::assert_abs_diff_eq!(results.errors, &expected_errs, epsilon = 1e12);
    }

    #[test]
    fn test_iterative_scale_mul_avg() {
        // Data from the 3 CH4 windows (5938, 6002, 6076)
        let col_vals = array!(
            [3.53152E19, 3.5241E19, 3.49562E19],
            [3.55762E19, 3.54595E19, 3.51951E19],
            [3.62114E19, 3.60018E19, 3.5926E19],
            [3.6256E19, 3.60751E19, 3.59989E19]
        );

        let err_vals = array![
            [2.63685E17, 2.70717E17, 2.4259E17],
            [2.81296E17, 3.12942E17, 2.70747E17],
            [2.61072E17, 2.97332E17, 2.50194E17],
            [2.53876E17, 2.90144E17, 2.42996E17]
        ];

        let averager = MulBiasAverager;
        let results = averager
            .average_windows(
                col_vals.view(),
                err_vals.view(),
                &["ch4_5938", "ch4_6002", "ch4_6076"],
                f64::MAX,
            )
            .unwrap();

        let expected_vals = array![3.51601E19, 3.53996E19, 3.60453E19, 3.61085E19];
        let expected_errs = array![1.4905E17, 1.65534E17, 1.54375E17, 1.5019E17];
        let expected_sfs = array![0.9994, 0.9989, 0.9994];
        approx::assert_abs_diff_eq!(results.adjustment_factors, expected_sfs);
        approx::assert_abs_diff_eq!(results.values, expected_vals, epsilon = 1e14);
        approx::assert_abs_diff_eq!(results.errors, expected_errs, epsilon = 1e12);
    }
}
