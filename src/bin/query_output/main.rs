use std::{fmt::Display, path::PathBuf, process::ExitCode};

use clap::Parser;
use error_stack::ResultExt;
use ggg_rs::readers::postproc_files::open_and_iter_postproc_file;

fn main() -> ExitCode {
    if let Err(e) = main_inner() {
        eprintln!("ERROR: {e:?}");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

fn main_inner() -> error_stack::Result<(), CliError> {
    let clargs = Cli::parse();
    let (_, it) = open_and_iter_postproc_file(&clargs.file)
        .change_context_lazy(|| "Error opening file".into())?;

    for col in clargs.columns.iter() {
        print!("{col:15}");
    }
    println!("");

    let mut missing_cols: Vec<&str> = vec![];
    for (irow, row) in it.enumerate() {
        let row = row.change_context_lazy(|| {
            format!("Error reading data row {} from file", irow + 1).into()
        })?;
        for col in clargs.columns.iter() {
            if let Some(val) = row.get_numeric_field(col) {
                if val.abs() < 1e-3 || val.abs() > 1e4 {
                    print!("{val:<12E}   ");
                } else {
                    // "{val:<12}" can produce strings longer than 12
                    // characters; 12 is only a minimum. So we have to
                    // make the string and then slice it to restrict floats
                    // to 12 characters in width. This doesn't seem to
                    // affect scientific notation.
                    let tmp = format!("{val:<12}");
                    print!("{}   ", &tmp[..12]);
                }
            } else {
                if !missing_cols.contains(&col.as_str()) {
                    missing_cols.push(col.as_str());
                }
                print!("{:15}", "N/A");
            }
        }
        println!("");
    }

    if !missing_cols.is_empty() {
        eprintln!(
            "Warning: {} columns were absent from the header: {}",
            missing_cols.len(),
            missing_cols.join(", ")
        );
    }

    Ok(())
}

/// Print specific numeric columns from a GGG output file.
#[derive(Debug, Parser)]
struct Cli {
    /// Path to the file to read from
    file: PathBuf,
    /// Columns from the data in the file to read from.
    /// May be repeated to show multiple columns
    columns: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
struct CliError(String);

impl Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for CliError {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for CliError {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}
