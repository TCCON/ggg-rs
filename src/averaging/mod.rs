use indexmap::IndexMap;

use crate::{readers::postproc_files::PostprocFileHeader, utils::GggError};

pub mod average_with_mul_bias;
pub mod grouping;

pub type AverageGroup<'c> = IndexMap<String, Vec<&'c str>>;

pub trait WindowGrouper {
    /// Given the header of a .?sw file, map groups of input windows to output mean gases
    ///
    /// The returned value is a map where the keys are the output column names and the
    /// value is a vector of input column names. This must only contain the mapping for
    /// the gas amount columns; the error columns will be inferred. That is, the map:
    ///
    /// ```text
    /// "xco2" -> vec!["xco2_6220", "xco2_6339"]
    /// ```
    ///
    /// means that the input columns "xco2_6220" and "xco2_6339" will be averaged to
    /// compute the output column "xco2" _and_ that "xco2_6220_error" and "xco2_6339_error"
    /// will be combined to compute "xco2_error".
    ///
    /// The output key need not only be the gas name. This may (and should) add a suffix
    /// if needed to differentiate gases from different spectra bands, e.g. "xco" for the
    /// near-IR and "xco_mir" for the mid-IR.
    fn group_windows<'c>(
        &self,
        header: &'c PostprocFileHeader,
    ) -> Result<AverageGroup<'c>, GggError>;

    /// Return a list of additional lines to add to the header to record how windows were averaged.
    fn header_lines(&self) -> Vec<String>;
}

pub trait Averager {
    /// Compute the averaged values and combined errors for the given windows.
    fn average_windows<S: AsRef<str>>(
        &self,
        window_values: ndarray::ArrayView2<f64>,
        error_values: ndarray::ArrayView2<f64>,
        window_names: &[S],
    ) -> Result<AveragingResult, GggError>;
}

pub struct AveragingResult {
    /// The average values across the input windows, length of `n_spectra`.
    values: ndarray::Array1<f64>,
    /// The combined error values across the input windows, length of `n_spectra`.
    errors: ndarray::Array1<f64>,
    /// Any adjustment factor made to the windows to account for biases.
    /// Length of `n_windows`.
    adjustment_factors: ndarray::Array1<f64>,
}
