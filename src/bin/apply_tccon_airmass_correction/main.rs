use std::{collections::HashMap, path::PathBuf, process::ExitCode};

use clap::Parser;

use error_stack::{Report, ResultExt};
use fortformat::FortFormat;
use ggg_rs::{output_files::{self, ProgramVersion, POSTPROC_FILL_VALUE}, tccon::input_config::{self, AdcfRow}};
use indexmap::IndexMap;
use o2_dmf::{O2DmfProvider, O2DmfError};

mod o2_dmf;
mod adcf;

const DEFAULT_G: f64 = 0.0;
const DEFAULT_P: f64 = 0.0;


fn main() -> ExitCode {
    let clargs = AirmassCorrCli::parse();
    if let Err(e) = driver(clargs) {
        eprintln!("ERROR: {e:?}");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }

}

#[derive(Debug, Parser)]
struct AirmassCorrCli {
    /// What file to read the airmass corrections from.
    correction_file: PathBuf,
    
    /// Path the to post processing file containing column densities
    /// to airmass correct and convert to column averages. In most
    /// cases, this will be a `.vsw` or `.vav` file.
    upstream_file: PathBuf,

    /// If time-varying O2 mean mole fractions are not available in the
    /// .vmr files or as a list file in --o2-dmf-file, provide a fixed
    /// mole fraction to use for all spectra here. Before GGG2020.1, this
    /// was 0.2095.
    #[clap(long, conflicts_with = "o2_dmf_file")]
    fixed_o2_dmf: Option<f64>,

    /// If time-varying O2 mean mole fractions are not present in the
    /// .vmr files, you can instead provide them as a list file generated
    /// by ginput. This must be a space-separated file that has two columns: the UTC
    /// datetime on the time resolution of the priors (e.g. 3 hours) and
    /// the O2 mean dry mole fraction.
    #[clap(long)]
    o2_dmf_file: Option<PathBuf>,
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error("Error getting the mean O2 dry mole fractions")]
    O2Dmf,
    #[error("Error reading {}", .0.display())]
    ReadError(PathBuf),
    #[error("Error reading line {line} of {}", .file.display())]
    ReadErrorAtLine{file: PathBuf, line: usize},
    #[error("Error writing output {}, {cause}", .path.display())]
    WriteError{path: PathBuf, cause: String},
    #[error("{0}")]
    Custom(String),
}

impl CliError {
    fn custom<S: ToString>(msg: S) -> Self {
        Self::Custom(msg.to_string())
    }
}

impl<E> From<Report<E>> for CliError
where E: ToOwned + Send + Sync + 'static, CliError: From<<E as ToOwned>::Owned> {
    fn from(value: Report<E>) -> Self {
        value.current_context().to_owned().into()
    }
}

fn driver(clargs: AirmassCorrCli) -> error_stack::Result<(), CliError> {
    let mut new_name = clargs.upstream_file
        .file_name()
        .expect("upstream file should have a base name")
        .to_os_string();
    new_name.push(".ada");

    let out_file = clargs.upstream_file.parent()
        .expect("upstream file should have a parent directory")
        .join(new_name);

    let extension = clargs.upstream_file.extension().unwrap_or_default().to_str().unwrap_or_default();
    let input_is_averaged = if extension.ends_with("sw") {
        false
    } else if extension.ends_with("av") {
        true
    } else {
        return Err(CliError::custom(format!(
            "apply_tccon_airmass_correction received a file with unknown extension: {extension}"
        )).into())
    };

    // Read in the appropriate airmass correction file
    let adcfs = input_config::read_adcf_file(&clargs.correction_file)
        .change_context_lazy(|| CliError::ReadError(clargs.correction_file.clone()))?;

    // Read in the O2 DMF information
    let o2_provider = make_boxed_o2_dmf_provider(&clargs)
        .change_context_lazy(|| CliError::O2Dmf)?;

    // Read in the header of the previous postproc file, add the airmass correction factors
    // to it. Write out to a temporary file to avoid confusion with a completed *.ada file.
    let (mut header, rows) = output_files::open_and_iter_postproc_file(&clargs.upstream_file)
        .change_context_lazy(|| CliError::ReadError(clargs.upstream_file.to_path_buf()))?;

    // Make sure we found a number of auxiliary column, if so, add 1 to account for the new
    // O2 DMF column.
    let naux = 1 + header.nauxcol.ok_or_else(|| CliError::custom(
        format!("first line of upstream file {} did not include the number of auxiliary columns", clargs.upstream_file.display())
    ))?;
    let nrow = header.nrow.ok_or_else(|| CliError::custom(
        format!("first line of upstream file {} did not include the number of data rows", clargs.upstream_file.display())
    ))?;


    let missing_value = header.missing.unwrap_or(POSTPROC_FILL_VALUE);
    
    // We aneed to insert an extra auxiliary column for the O2 DMF - both in the column names
    // and the format spec
    let mut format_spec = header.format;
    let mut col_names = header.column_names;

    // Before we edit the column names, find the O2 window. This looks complicated, but all it's doing
    // is finding the first window name that starts with "O2" and is not a column error amount.
    let o2_window = col_names[naux-1..].iter()
        .fold(None, |acc, name| {
            if !name.starts_with("o2") | name.ends_with("_error") {
                acc
            } else if name.starts_with("o2") && acc.is_none() {
                Some(Ok(name.to_string()))
            } else if acc.as_ref().is_some_and(|r| r.is_ok()) {
                let other = acc.unwrap().unwrap();
                Some(Err(CliError::custom(format!(
                    "multiple O2 windows found: '{name}' and '{other}'"
                ))))
            } else {
                acc
            }
        }).ok_or_else(|| CliError::custom("could not find O2 window"))??;

    // Because we added 1 to the naux value and because the index here is 0-based,
    // we need to subtract 1 to get back to the right index. If there were 25 aux columns
    // before, we'd want the O2 column to be inserted at index 25 (to be the 26th) column,
    // but we already added that 1 to naux, so naux would = 26 and naux - 1 = 25.
    col_names.insert(naux-1, "o2dmf".to_string());

    let o2_dmf_spec = fortformat::FortField::Real { 
        width: 10,
        precision: Some(7),
        fmt: fortformat::format_specs::RealFmt::F,
        scale: 0
    };
    // This inserts at naux because of the extra 1x spacer
    format_spec.insert_field(naux, o2_dmf_spec).map_err(|e| CliError::custom(
        format!("could not add the 'o2dmf' format specifier: {e}")
    ))?;

    // Also change the column names to prepend an "x" to all of the retrieved columns.
    for name in col_names[naux..].iter_mut() {
        name.insert(0, 'x');
    }

    // Handle replacing the "a1" column that we retain for backwards compatibility with
    // older runlog formats - this can't go in the format string because it represents a
    // commenting-out character that we don't have a field for.
    let format_str = format_spec.fmt_string(1).replacen("1x", "a1", 1);

    // Add the airmass corrections and the O2 mole fraction source to the file header
    header.extra_lines.push(format!(
        " Mean O2 DMF source: {}", o2_provider.header_line()
    ));
    add_adcf_header_lines(&mut header.extra_lines, &adcfs)
        .change_context_lazy(|| CliError::WriteError {
            path: out_file.clone(),
            cause: "writing the ADCF values in the header failed.".to_string()
        })?;

    // Write out the file header, this way we can do one row at a time and not have to
    // load the whole file into memory.
    let fw = std::fs::File::create(&out_file)
        .change_context_lazy(|| CliError::WriteError { path: out_file.to_path_buf(), cause: "creating file failed".to_string() })?;
    let mut fw = std::io::BufWriter::new(fw);

    output_files::write_postproc_header(
        &mut fw,
        col_names.len(),
        nrow,
        naux,
        &[program_version()],
        &header.extra_lines,
        missing_value,
        &format_str,
        &col_names
    ).change_context_lazy(|| 
        CliError::WriteError { path: out_file.clone(), cause: "error occurred while writing the file header".to_string() }
    )?;

    // Read each row, apply airmass corrections, and write out the Xgas values. Include the
    // O2 DMF as a new auxiliary column.
    let settings = fortformat::ser::SerSettings::default().align_left_str(true);
    let missing_value = header.missing.unwrap_or(POSTPROC_FILL_VALUE);

    for (irow, row) in rows.enumerate() {
        let mut row = row.change_context_lazy(|| CliError::ReadErrorAtLine {
            file: clargs.upstream_file.clone(),
            line: header.nhead + irow + 1,
        })?;

        let this_o2_dmf = o2_provider.o2_dmf(&row.auxiliary.spectrum)
            .change_context_lazy(|| CliError::O2Dmf)?;
        row.auxiliary.o2dmf = Some(this_o2_dmf);
        row.retrieved = apply_correction(&row.retrieved, &adcfs, &o2_window, this_o2_dmf, row.auxiliary.solzen, missing_value, input_is_averaged)?;

        fortformat::ser::to_writer_custom(row, &format_spec, Some(&col_names), &settings, &mut fw)
            .change_context_lazy(|| CliError::WriteError { 
                path: out_file.clone(),
                cause: format!("error serializing data line {}", irow+1)
            })?;
    }

    Ok(())
}

fn make_boxed_o2_dmf_provider(clargs: &AirmassCorrCli) -> error_stack::Result<Box<dyn O2DmfProvider>, O2DmfError> {
    if let Some(dmf) = clargs.fixed_o2_dmf {
        let provider = o2_dmf::FixedO2Dmf::new(dmf);
        return Ok(Box::new(provider));
    }

    if let Some(o2_file) = &clargs.o2_dmf_file {
        let run_dir = clargs.upstream_file.parent().ok_or_else(|| O2DmfError::custom(format!(
            "could not get parent of upstream file, {}", clargs.upstream_file.display()
        )))?;
        let provider = o2_dmf::O2DmfTimeseries::new(o2_file.to_path_buf(), run_dir)?;
        return Ok(Box::new(provider));
    }
    
    todo!()
}

fn add_adcf_header_lines(lines_out: &mut Vec<String>, adcfs: &IndexMap<String, AdcfRow>) -> Result<(), fortformat::SError> {
    let nrow = adcfs.len();
    lines_out.push(format!(" Airmass-Dependent Correction Factors: {nrow} 5"));

    let ff = FortFormat::parse("(a22,2(1x,f9.5),2(1x,f6.2))")
        .unwrap();
    let settings = fortformat::ser::SerSettings::default().align_left_str(true);
    for corr in adcfs.values() {
        let values = (
            &corr.gas_or_window,
            corr.adcf,
            corr.adcf_error,
            corr.g.unwrap_or(DEFAULT_G),
            corr.p.unwrap_or(DEFAULT_P)
        );
        let s = fortformat::ser::to_string_custom::<_, &str>(values, &ff, None, &settings)?;
        lines_out.push(s);
    }

    Ok(())
}

fn apply_correction(row: &HashMap<String, f64>, adcfs: &IndexMap<String, AdcfRow>, o2_window: &str, o2_dmf: f64, sza: f64, missing_value: f64, is_avg: bool) -> Result<HashMap<String, f64>, CliError> {
    let o2_window_error = format!("{o2_window}_error");

    let o2_col = *row.get(o2_window).ok_or_else(|| {
        CliError::custom(format!("row does not contain the {o2_window} window for O2"))
    })?;
    let o2_col_err = *row.get(&o2_window_error).ok_or_else(|| {
        CliError::custom(format!("row does not contain the O2 column error, '{o2_window_error}'"))
    })?;

    let all_windows = row.keys().filter(|k| !k.ends_with("_error"));

    let mut new_row = HashMap::new();
    let col_dry_air = o2_col / o2_dmf;

    for window in all_windows {
        let window_error = format!("{window}_error");
        let col_val = *row.get(window).unwrap(); // we know this will be in the row, b/c we're iterating over the row's keys
        let col_err_val = *row.get(&window_error).ok_or_else(|| {
            CliError::custom(format!("row does not contain the column '{window_error}' for the error value corresponding to '{window}'"))
        })?;

        // For most gases, if we're doing individual windows (opposed to window averages), we don't
        // want to add in the O2 uncertainty to the Xgas value just yet. If we did, then it would
        // get counted multiple times when average_results operates on the Xgas values. However,
        // we want to calculate the proper XO2 error here, so that average_results can use it.
        let gas_frac_uncert = if window == o2_window || !is_avg {
            col_err_val
        } else {
            let v = col_err_val.powi(2) + (col_val * o2_col_err / o2_col).powi(2);
            f64::sqrt(v)
        };

        let xgas_key = format!("x{window}");
        let xgas_error_key = format!("x{window}_error");

        let xgas_adcf = adcfs.get(&xgas_key);
        let cf = xgas_adcf.map(|a| a.adcf).unwrap_or(0.0);
        let p = xgas_adcf.map(|a| a.p).flatten().unwrap_or(DEFAULT_P);
        let g = xgas_adcf.map(|a| a.g).flatten().unwrap_or(DEFAULT_G);
        let sbf = symmetric_basis_function(sza, p, g);
        
        if approx::abs_diff_eq!(col_val, missing_value) {
            new_row.insert(xgas_key, missing_value);
        } else {
            let xgas = col_val / col_dry_air / (1.0 + cf*sbf);
            new_row.insert(xgas_key, xgas);
        }

        if approx::abs_diff_eq!(col_err_val, missing_value) {
            new_row.insert(xgas_error_key, missing_value);
        } else {
            let xgas_error = gas_frac_uncert / col_dry_air / (1.0 + cf*sbf);
            new_row.insert(xgas_error_key, xgas_error);
        }
    }

    Ok(new_row)
}

fn symmetric_basis_function(sza: f64, p: f64, g: f64) -> f64 {
    ((sza + g) / (90.0 + g)).powf(p) - ((45.0 + g) / (90.0 + g)).powf(p)
}

fn program_version() -> ProgramVersion {
    ProgramVersion {
        program: "apply_tccon_airmass_correction".to_string(),
        version: "Version 1.0".to_string(),
        date: "2024-09-30".to_string(),
        authors: "JLL".to_string(),
    }
}