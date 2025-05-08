use std::{collections::HashMap, fmt::Display, path::PathBuf, process::ExitCode, str::FromStr};

use clap::Parser;
use clap_verbosity_flag::{InfoLevel, Verbosity};
use error_stack::ResultExt;
use ggg_rs::logging::init_logging;

mod calc_aks;
mod naming;
mod read_aks;
mod write_aks;

fn main() -> ExitCode {
    let clargs = Cli::parse();
    init_logging(clargs.verbosity.log_level_filter());
    log::info!("INFO level logging active");
    log::debug!("DEBUG level logging active");
    log::trace!("TRACE level logging active");
    match driver(clargs) {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("An error occurred:\n{e:?}");
            ExitCode::FAILURE
        }
    }
}
// TODO: interpolate to existing pressure levels, either in the file being appended to
// or another file.

#[derive(Debug, clap::Parser)]
struct Cli {
    /// A sequence of paths to files named "k0_GAS_*.all", that is, the
    /// result of running `avg_ker` on the concatenated Jacobian files.
    ak_files: Vec<PathBuf>,

    /// File to write the AK table to.
    /// Overwritten by default, use --append to change that behavior.
    #[clap(short, long, default_value = "ak_tables.nc")]
    output: PathBuf,

    /// Controls how the output file is appended to. By default, it
    /// is overwritten. Setting this to "keep" or "error" will allow
    /// adding new gases. The value controls what happens if one of
    /// the input AK files defines AKs for a gas already present in
    /// the file. "keep" will keep the table already in the netCDF file,
    /// "error" will exit with an error.
    #[clap(short, long, default_value_t = AppendMode::No)]
    append: AppendMode,

    #[command(flatten)]
    verbosity: Verbosity<InfoLevel>,
}

#[derive(Debug, Clone, Copy)]
enum AppendMode {
    No,
    Keep,
    Error,
}

impl Default for AppendMode {
    fn default() -> Self {
        Self::No
    }
}

impl FromStr for AppendMode {
    type Err = CliError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "no" => Ok(Self::No),
            "keep" => Ok(Self::Keep),
            "error" => Ok(Self::Error),
            _ => Err(CliError::Input(format!(
                "unknown value for --append: '{s}'"
            ))),
        }
    }
}

impl Display for AppendMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppendMode::No => write!(f, "no"),
            AppendMode::Keep => write!(f, "keep"),
            AppendMode::Error => write!(f, "error"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error("Invalid input: {0}")]
    Input(String),
    #[error("An error occurred while opening the output file for writing: {0}")]
    OpenOutput(#[from] netcdf::Error),
    #[error("An averaging kernel table for {gas} already exists in the output file {}", .output_file.display())]
    ExistingAk { output_file: PathBuf, gas: String },
    #[error("An error occurred while checking if the new AKs exist in the output file")]
    AkCheck,
    #[error("An error occurred while reading an AK .all file")]
    ReadError,
    #[error("An error occurred while calculating the AK tables")]
    CalcError,
    #[error("An error occurred while writing the AK tables to disk")]
    WriteError,
}

fn driver(clargs: Cli) -> error_stack::Result<(), CliError> {
    let mut table_ds = match clargs.append {
        AppendMode::No => {
            log::info!(
                "Creating AK output file, {} (overwriting if present)",
                clargs.output.display()
            );
            netcdf::create(&clargs.output).map_err(|e| CliError::OpenOutput(e))?
        }
        AppendMode::Keep | AppendMode::Error => {
            log::info!("Appending AKs to output file, {}", clargs.output.display());
            netcdf::append(&clargs.output).map_err(|e| CliError::OpenOutput(e))?
        }
    };

    if let AppendMode::Error = clargs.append {
        log::info!(
            "Checking if new gases conflict with existing AKs in {}",
            clargs.output.display()
        );
        if let Err(e) = read_aks::check_existing_gases(&table_ds, &clargs.ak_files) {
            if let Some(gas) = e.current_context().existing_ak_gas() {
                let err = CliError::ExistingAk {
                    output_file: clargs.output.clone(),
                    gas: gas.to_string(),
                };
                return Err(err.into());
            } else {
                return Err(e).change_context(CliError::AkCheck);
            }
        }
        log::info!("No conflicts found between new and existing AKs");
    }

    let mut all_gas_aks = HashMap::new();
    for akall_file in clargs.ak_files.iter() {
        log::info!("Reading AKs from {}", akall_file.display());
        let gas = read_aks::gas_name_from_path(&akall_file).change_context(CliError::ReadError)?;
        let aks = read_aks::read_akall_file(&akall_file).change_context(CliError::ReadError)?;
        all_gas_aks.insert(gas, aks);
    }
    let all_gas_tables =
        calc_aks::make_ak_tables(all_gas_aks).change_context(CliError::CalcError)?;
    log::info!("Writing AK tables to {}", clargs.output.display());
    write_aks::write_aks_to_dset(&mut table_ds, &all_gas_tables, &clargs.append)
        .change_context(CliError::WriteError)?;
    Ok(())
}
