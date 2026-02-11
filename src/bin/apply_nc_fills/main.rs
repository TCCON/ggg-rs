use std::{io::Read, path::Path, process::ExitCode};

use clap::Parser;
use error_stack::ResultExt;
use ggg_rs::logging::init_logging;

mod cli;
mod fills;

fn main() -> ExitCode {
    let clargs = cli::Cli::parse();
    init_logging(clargs.verbosity.log_level_filter());
    if let Err(e) = main_inner(clargs) {
        eprintln!("ERROR: {e:?}");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

fn main_inner(clargs: cli::Cli) -> error_stack::Result<(), CliError> {
    match clargs.command {
        cli::Commands::Toml(toml_cli) => fill_driver(toml_cli),
        cli::Commands::TomlTemplate(template_cli) => {
            fills::FilterConfig::write_template_example(&template_cli.template_file)
        }
        cli::Commands::Debug(debug_cli) => {
            let config = load_config(&debug_cli.template_file)?;
            println!("Configuration:\n\n{config:#?}");
            Ok(())
        }
    }
}

/// Main function that replaces specified data with fill values.
fn fill_driver(cli: cli::TomlCli) -> error_stack::Result<(), CliError> {
    let mut ds = setup_output_file(&cli.nc_file, &cli.output)?;
    let config = load_config(&cli.toml_file)?;
    fills::apply_filters(&mut ds, &config)?;
    Ok(())
}

/// Setup function that handles copying the output file (if necessary) and
/// opening the netCDF dataset.
fn setup_output_file(
    input_file: &Path,
    output_cli: &cli::OutputCli,
) -> error_stack::Result<netcdf::FileMut, CliError> {
    if let Some(output_file) = output_cli.output.as_deref() {
        log::info!(
            "Copying {} to {}",
            input_file.display(),
            output_file.display()
        );
        std::fs::copy(input_file, output_file).change_context_lazy(|| {
            CliError::context(format!(
                "Error copying {} to {}",
                input_file.display(),
                output_file.display()
            ))
        })?;

        let ds = netcdf::append(output_file).change_context_lazy(|| {
            CliError::context(format!(
                "Error opening output file ({}) for writing",
                output_file.display()
            ))
        })?;

        return Ok(ds);
    }

    if output_cli.in_place {
        log::info!("Modifying {} in-place", input_file.display());
        let ds = netcdf::append(input_file).change_context_lazy(|| {
            CliError::context(format!(
                "Error opening input file ({}) for modification",
                input_file.display()
            ))
        })?;

        return Ok(ds);
    }

    unreachable!("Command line arguments should require --output or --in-place")
}

/// Load the configuration of replacement filters from a TOML file.
fn load_config(toml_file: &Path) -> error_stack::Result<fills::FilterConfig, CliError> {
    let mut f = std::fs::File::open(toml_file).change_context_lazy(|| {
        CliError::context(format!(
            "Error opening configuration file: {}",
            toml_file.display()
        ))
    })?;
    let mut buf = String::new();
    f.read_to_string(&mut buf).change_context_lazy(|| {
        CliError::context(format!(
            "Error reading configuration file: {}",
            toml_file.display()
        ))
    })?;
    let config: fills::FilterConfig = toml::from_str(&buf).change_context_lazy(|| {
        CliError::context(format!(
            "Error parsing configuration file: {}",
            toml_file.display()
        ))
    })?;
    Ok(config)
}

/// Program error type
#[derive(Debug, thiserror::Error)]
enum CliError {
    /// Indicates a required variable is missing from the netCDF file.
    #[error("Missing expected variable: {0}")]
    MissingVariable(String),

    /// Indicates a variable does not have a fill value defined.
    #[error("No fill value defined for variable: {0}")]
    NoFillDef(String),

    /// Wrapper type used to add information to an inner error.
    #[error("{0}")]
    Context(String),
}

impl CliError {
    fn missing_variable<S: ToString>(varname: S) -> Self {
        Self::MissingVariable(varname.to_string())
    }

    fn no_fill_def<S: ToString>(varname: S) -> Self {
        Self::NoFillDef(varname.to_string())
    }

    fn context<S: ToString>(ctx: S) -> Self {
        Self::Context(ctx.to_string())
    }
}
