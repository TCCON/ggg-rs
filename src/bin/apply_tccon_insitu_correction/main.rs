use std::{collections::HashMap, path::PathBuf, process::ExitCode};

use clap::Parser;
use error_stack::ResultExt;
use fortformat::FortFormat;
use ggg_rs::{readers::{postproc_files::open_and_iter_postproc_file, ProgramVersion}, tccon::input_config::{self, AicfRow}, writers::postproc_files::write_postproc_header};
use indexmap::IndexMap;

fn main() -> ExitCode {
    let clargs = InsituCorrCli::parse();
    if let Err(e) = driver(clargs) {
        eprintln!("ERROR: {e:?}");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

#[derive(Debug, Parser)]
struct InsituCorrCli {
    /// What file to read the in situ corrections from.
    correction_file: PathBuf,
    
    /// Path the to post processing file containing column densities
    /// to airmass correct and convert to column averages. In most
    /// cases, this will be a `.vsw` or `.vav` file.
    upstream_file: PathBuf,

    /// Directory in which to save the output file. If omitted, the output
    /// file will be saved to the same directory as the upstream file.
    #[clap(short='o', long)]
    output_dir: Option<PathBuf>,
}

#[derive(Debug, thiserror::Error)]
enum CliError {
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

fn driver(clargs: InsituCorrCli) -> error_stack::Result<(), CliError> {
    let mut new_name = clargs.upstream_file
        .file_name()
        .expect("upstream file should have a base name")
        .to_os_string();
    new_name.push(".aia");

    let out_dir = clargs.output_dir
        .as_deref()
        .unwrap_or_else(|| {
            clargs.upstream_file.parent()
            .expect("upstream file should have a parent directory")
        });
    let out_file = out_dir.join(new_name);

    // Read in the appropriate in situ correction file
    let aicfs = input_config::read_aicf_file(&clargs.correction_file)
        .change_context_lazy(|| CliError::ReadError(clargs.correction_file.clone()))?;

    // Read in the header of the previous postproc file, add the in situ correction factors
    let (mut header, rows) = open_and_iter_postproc_file(&clargs.upstream_file)
        .change_context_lazy(|| CliError::ReadError(clargs.upstream_file.to_path_buf()))?;
    add_aicf_header_lines(&mut header.extra_lines, &aicfs)
        .change_context_lazy(|| CliError::WriteError {
            path: out_file.clone(),
            cause: "writing the AICF values in the header failed.".to_string()
        })?;

    // Go ahead and start writing to the output
    let fw = std::fs::File::create(&out_file)
        .change_context_lazy(|| CliError::WriteError { path: out_file.to_path_buf(), cause: "creating file failed".to_string() })?;
    let mut fw = std::io::BufWriter::new(fw);

    let format_str = header.fformat_without_comment().fmt_string(1);
    let mut program_versions = Vec::from_iter(header.program_versions.values().cloned());
    program_versions.insert(0, program_version());

    write_postproc_header(
        &mut fw,
        header.column_names.len(),
        header.nrec,
        header.naux,
        &program_versions,
        &header.extra_lines,
        header.missing_value,
        &format_str,
        &header.column_names
    ).change_context_lazy(|| 
        CliError::WriteError { path: out_file.clone(), cause: "error occurred while writing the file header".to_string() }
    )?;

    // Read each row, apply airmass corrections, and write out the Xgas values.
    let settings = fortformat::ser::SerSettings::default().align_left_str(true);
    let missing_value = header.missing_value;

    // The original apply_insitu_correction switches the second field from "a1" to "1x".
    // We'll keep that behavior for backwards compatibility for now. Eventually we could
    // probably make that change back in collate_tccon_results, once all the post processing
    // programs can support that.
    let writer_fformat = header.fformat_without_comment();
    for (irow, row) in rows.enumerate() {
        let mut row = row.change_context_lazy(|| CliError::ReadErrorAtLine {
            file: clargs.upstream_file.clone(),
            line: header.nhead + irow + 1,
        })?;

        row.retrieved = apply_correction(&row.retrieved, &aicfs, missing_value)?;

        fortformat::ser::to_writer_custom(row, &writer_fformat, Some(&header.column_names), &settings, &mut fw)
            .change_context_lazy(|| CliError::WriteError { 
                path: out_file.clone(),
                cause: format!("error serializing data line {}", irow+1)
            })?;
    }

    Ok(())
}

fn add_aicf_header_lines(lines_out: &mut Vec<String>, aicfs: &IndexMap<String, AicfRow>) -> Result<(), fortformat::SError> {
    let nrow = aicfs.len();
    lines_out.push(format!(" Airmass-Independent/In-Situ Correction Factors: {nrow} 4"));
    let ff = FortFormat::parse("(a23,2f9.4,1x,a1,a,a1)")
        .unwrap();
    let settings = fortformat::ser::SerSettings::default().align_left_str(true);
    for corr in aicfs.values() {
        let values = (
            &corr.gas,
            corr.aicf,
            corr.aicf_error,
            '"',
            &corr.wmo_scale,
            '"'
        );
        let s = fortformat::ser::to_string_custom::<_, &str>(values, &ff, None, &settings)?;
        lines_out.push(s);
    }

    Ok(())
}

fn apply_correction(row: &HashMap<String, f64>, aicfs: &IndexMap<String, AicfRow>, missing_value: f64) -> Result<HashMap<String, f64>, CliError> {
    let all_xgases = row.keys().filter(|k| !k.ends_with("_error"));

    let mut new_row = HashMap::new();
    for xgas in all_xgases {
        let xgas_error = format!("{xgas}_error");
        let col_val = *row.get(xgas).unwrap(); // we know this will be in the row, b/c we're iterating over the row's keys
        let col_err_val = *row.get(&xgas_error).ok_or_else(|| {
            CliError::custom(format!("row does not contain the column '{xgas_error}' for the error value corresponding to '{xgas}'"))
        })?;
        
        let cf = aicfs.get(xgas).map(|r| r.aicf).unwrap_or(1.0);
        if approx::abs_diff_eq!(col_val, missing_value) {
            new_row.insert(xgas.to_owned(), missing_value);
        } else {
            new_row.insert(xgas.to_owned(), col_val / cf);
        }

        if approx::abs_diff_eq!(col_err_val, missing_value) {
            new_row.insert(xgas_error, missing_value);
        } else {
            new_row.insert(xgas_error, col_err_val / cf);
        }
    }
    Ok(new_row)
}

fn program_version() -> ProgramVersion {
    ProgramVersion {
        program: "apply_tccon_insitu_correction".to_string(),
        version: "Version 1.0".to_string(),
        date: "2025-03-31".to_string(),
        authors: "JLL".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use ggg_rs::utils::test_utils::compare_output_text_files;
    use crate::{driver, InsituCorrCli};

    #[test]
    fn test_insitu_correct_pa_benchmark() {
        let crate_root = env!("CARGO_MANIFEST_DIR");
        let input_dir = PathBuf::from(crate_root).join("test-data").join("inputs").join("apply-tccon-insitu-correction");
        let expected_dir = PathBuf::from(crate_root).join("test-data").join("expected").join("apply-tccon-insitu-correction");
        let output_dir = PathBuf::from(crate_root).join("test-data").join("outputs").join("apply-tccon-insitu-correction");

        let clargs = InsituCorrCli {
            correction_file: input_dir.join("corrections_insitu_postavg.dat"),
            upstream_file: input_dir.join("pa_ggg_benchmark.vav.ada"),
            output_dir: Some(output_dir.clone())
        };

        driver(clargs).expect("Running the airmass correction should not fail.");

        compare_output_text_files(&expected_dir, &output_dir, "pa_ggg_benchmark.vav.ada.aia");
    }
}