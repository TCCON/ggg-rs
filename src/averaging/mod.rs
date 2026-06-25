use std::path::{Path, PathBuf};

use error_stack::ResultExt;
use indexmap::IndexMap;
use itertools::Itertools;
use ndarray::{s, Array2};

use crate::{
    collation::CollationMode,
    readers::{
        postproc_files::{
            open_and_read_postproc_file, PostprocArray, PostprocFileHeader, RetrievedDataArrays,
        },
        ProgramVersion,
    },
    utils::GggError,
    writers::postproc_files::{FortranPostprocWriter, PostprocWriter},
};

pub mod average_with_mul_bias;
pub mod grouping;

pub type AverageGroup<'c> = IndexMap<String, Vec<&'c str>>;

static SW_TO_AV_REGEX: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();

pub fn average_results<G: WindowGrouper>(
    per_window_file: &Path,
    grouper: &G,
    averaging_version: ProgramVersion,
    output_dir: Option<&Path>,
) -> error_stack::Result<(), GggError> {
    let output_file = output_file_path(per_window_file, output_dir)?;

    let (mut header, mut data) =
        open_and_read_postproc_file(per_window_file).change_context_lazy(|| {
            GggError::context(format!(
                "error occurred while reading file {} into arrays",
                per_window_file.display()
            ))
        })?;

    // Which averaging method we use depends on what quantity we are working with and whether or not
    // the window-to-window scale factors have been fixed.
    let preset_adj_factors = extract_scale_factors(&mut header).change_context_lazy(|| {
        GggError::context(format!(
            "error occurred while searching for present window scale factors in file {}",
            per_window_file.display()
        ))
    })?;
    let col_mode = CollationMode::try_from(per_window_file).change_context_lazy(|| {
        GggError::context(format!(
            "error getting collation mode from extension of file {}",
            per_window_file.display(),
        ))
    })?;
    let averager = averager_for_col_mode(col_mode, preset_adj_factors);

    // Next section handles getting the window groups and doing the averaging
    let window_groups = grouper.group_windows(&header).change_context_lazy(|| {
        GggError::context(format!(
            "error occurred while grouping windows in {}",
            per_window_file.display(),
        ))
    })?;
    let mut averaged_data = IndexMap::new();
    for (group_name, windows) in window_groups {
        let window_averages = average_one_group(&windows, &data, header.missing_value, &averager)?;
        averaged_data.insert(group_name.clone(), window_averages.values);
        averaged_data.insert(format!("{group_name}_error"), window_averages.errors);
    }

    // Update the metadata and data
    let mut new_column_names = header.column_names[0..header.naux].to_vec();
    new_column_names.extend(averaged_data.keys().cloned());
    header.update_colnames(new_column_names);
    header.program_versions.insert(0, averaging_version);
    header.extra_lines.extend(grouper.header_lines());
    data.set_retrieved(RetrievedDataArrays::try_from(averaged_data)?);

    let writer = FortranPostprocWriter::new(output_file, false);
    writer.write_postproc_file(&header, data.iter_rows().map(|r| Ok(r)))?;
    Ok(())
}

fn average_one_group(
    windows: &[&str],
    data: &PostprocArray,
    missing: f64,
    averager: &AveragingMethod,
) -> error_stack::Result<AveragingResult, GggError> {
    let mut values = Array2::from_elem((data.num_spec(), windows.len()), missing);
    let mut errors = Array2::from_elem((data.num_spec(), windows.len()), missing);
    for (iwin, win) in windows.iter().enumerate() {
        let win_values = data
            .retrieved_column(win)
            .expect(&format!("Could not find values for {win}"));
        values.slice_mut(s![.., iwin]).assign(win_values);
        let win_errors = data
            .retrieved_column(&format!("{win}_error"))
            .expect(&format!("Could not find error values for {win}"));
        errors.slice_mut(s![.., iwin]).assign(win_errors);
    }

    let averaged_data = averager
        .average_windows(values.view(), errors.view(), windows, missing)
        .change_context_lazy(|| {
            GggError::context(format!(
                "error while averaging windows {}",
                windows.join(", ")
            ))
        })?;

    Ok(averaged_data)
}

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

pub enum AveragingMethod {
    /// Average windows assumed to have a multiplicative bias w.r.t. each other that must be calculated.
    IterMulBias,
    /// Average windows assumed to have a multiplicative bias w.r.t. each other that has already been calculated.
    PresetMulBias(IndexMap<String, f64>),
    AddBias,
}

impl AveragingMethod {
    fn average_windows<S: AsRef<str>>(
        &self,
        window_values: ndarray::ArrayView2<f64>,
        error_values: ndarray::ArrayView2<f64>,
        window_names: &[S],
        missing_value: f64,
    ) -> Result<AveragingResult, GggError> {
        match self {
            AveragingMethod::IterMulBias => average_with_mul_bias::average_with_mul_bias_iter(
                window_values,
                error_values,
                window_names,
                missing_value,
            ),
            AveragingMethod::PresetMulBias(scale_factors) => {
                average_with_mul_bias::average_with_mul_bias_preset(
                    window_values,
                    error_values,
                    scale_factors,
                    window_names,
                    missing_value,
                )
            }
            AveragingMethod::AddBias => todo!(),
        }
    }
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

/// Extract precalculated scale factors from the header of a post-processing file.
/// If no "sf=" line is present, returns `None`, meaning scale factors will need
/// to be computed dynamically when averaging windows. Otherwise, the returned map
/// will have the gas windows as keys, e.g. `"co2_6220"`. Error column names are
/// not included.
fn extract_scale_factors(
    header: &mut PostprocFileHeader,
) -> Result<Option<IndexMap<String, f64>>, GggError> {
    for (iline, line) in header.extra_lines.iter().enumerate() {
        if line.trim_start().starts_with("sf=") {
            let sf_map = parse_scale_factors(line, header.gas_varnames())?;
            header.extra_lines.remove(iline);
            return Ok(Some(sf_map));
        }
    }

    Ok(None)
}

fn parse_scale_factors<S: ToString>(
    sf_line: &str,
    gas_colnames: &[S],
) -> Result<IndexMap<String, f64>, GggError> {
    let scale_factors: Vec<f64> = sf_line
        .replace("sf=", "")
        .trim()
        .split_ascii_whitespace()
        .map(|s| s.parse::<f64>())
        .try_collect()
        .map_err(|e| {
            GggError::custom(format!(
                "Could not parse one of the entries in the sf= header line as a number (error was: {e})"
            ))
        })?;

    // The gas columns includes the errors
    if scale_factors.len() != gas_colnames.len() / 2 {
        return Err(GggError::custom(format!(
            "Number of scale factors in the sf= header line ({}) was not equal to the number of gas columns ({})",
            scale_factors.len(),
            gas_colnames.len(),
        )));
    }

    let sf_map: IndexMap<String, f64, _> = IndexMap::from_iter(
        gas_colnames
            .iter()
            .step_by(2) // skip over the error columns
            .zip(scale_factors)
            .map(|(gas, sf)| (gas.to_string(), sf)),
    );
    Ok(sf_map)
}

fn averager_for_col_mode(
    mode: CollationMode,
    adjustment_factors: Option<IndexMap<String, f64>>,
) -> AveragingMethod {
    match (mode, adjustment_factors) {
        (CollationMode::VerticalColumns | CollationMode::VmrScaleFactors, Some(sfs)) => {
            AveragingMethod::PresetMulBias(sfs)
        }
        (CollationMode::VerticalColumns | CollationMode::VmrScaleFactors, None) => {
            AveragingMethod::IterMulBias
        }
    }
}

fn output_file_path(
    per_window_file: &Path,
    output_dir: Option<&Path>,
) -> Result<PathBuf, GggError> {
    let output_dir = output_dir
        .or_else(|| per_window_file.parent())
        .ok_or_else(|| {
            GggError::custom(format!(
                "Could not determine parent directory of upstream per-window file {}",
                per_window_file.display()
            ))
        })?;

    let orig_base_name = per_window_file
        .file_name()
        .ok_or_else(|| {
            GggError::custom(format!(
                "Could not get file name of upstream per-window file {}",
                per_window_file.display()
            ))
        })?
        .to_str()
        .ok_or_else(|| {
            GggError::custom(format!(
                "Could not interpret base name of {} in UTF-8 encoding",
                per_window_file.display()
            ))
        })?;

    let re = SW_TO_AV_REGEX.get_or_init(|| {
        regex::Regex::new(r"\.([a-z])sw(.|$)")
            .expect("Failed to compile regex for changing file extension")
    });
    let new_base_name = re.replace(orig_base_name, ".${1}av${2}");
    let out_file = output_dir.join(new_base_name.as_ref());

    Ok(out_file)
}

// TODO: write the .?av file along with any diagnostic files.
// In particular, write out the groups and window scale factors into their own TOML file.
// In the .?av file header, include a summary of the grouping and a checksum of that TOML file.
