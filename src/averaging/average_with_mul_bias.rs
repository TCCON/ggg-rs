use indexmap::IndexMap;

use super::Averager;

/// Average windows assumed to have a multiplicative bias w.r.t. each other that must be calculated.
pub struct MulBiasAverager;

impl Averager for MulBiasAverager {
    fn average_windows<S: AsRef<str>>(
        &self,
        window_values: ndarray::ArrayView2<f64>,
        error_values: ndarray::ArrayView2<f64>,
        _window_names: &[S],
    ) -> Result<super::AveragingResult, crate::utils::GggError> {
        todo!()
    }
}

/// Average windows assumed to have a multiplicative bias w.r.t. each other that has already been calculated.
pub struct PresetMulBiasAverager {
    scale_factors: IndexMap<String, f64>,
}

impl Averager for PresetMulBiasAverager {
    fn average_windows<S: AsRef<str>>(
        &self,
        window_values: ndarray::ArrayView2<f64>,
        error_values: ndarray::ArrayView2<f64>,
        window_names: &[S],
    ) -> Result<super::AveragingResult, crate::utils::GggError> {
        todo!()
    }
}
