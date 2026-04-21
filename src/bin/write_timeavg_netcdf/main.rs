use std::{path::PathBuf, process::ExitCode};

use clap::Parser;
use clap_verbosity_flag::{InfoLevel, Verbosity};
use ggg_rs::logging::init_logging;

mod average_site;
mod data_structures;

fn main() -> ExitCode {
    let clargs = Cli::parse();

    init_logging(clargs.verbosity.log_level_filter());

    let res = match clargs.command {
        Subcommand::AvgSite(avg_site_cli) => avg_site_cli.driver(),
    };

    match res {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("write_timeavg_netcdf did not complete successfully:\n{e:?}");
            ExitCode::FAILURE
        }
    }
}

#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    command: Subcommand,

    #[command(flatten)]
    verbosity: Verbosity<InfoLevel>,
}

#[derive(Debug, Clone, clap::Subcommand)]
enum Subcommand {
    AvgSite(AvgSiteCli),
}

#[derive(Debug, Clone, clap::Args)]
struct AvgSiteCli {
    /// The netCDF file containing the data from individual
    /// spectra for one TCCON site
    level2_file: PathBuf,

    /// Where to write the time averaged file. If omitted, the
    /// file will be written in the same directory as the level 2
    /// file with an inferred name. If this is a directory, the
    /// file will be written into that directory with an inferred
    /// name.
    timeavg_file: Option<PathBuf>,

    /// The width of the time bins in minutes.
    #[clap(short = 'm', long, default_value_t = 30)]
    time_bin_minutes: u32,
}

impl AvgSiteCli {
    fn driver(&self) -> error_stack::Result<(), CliError> {
        Ok(())
    }

    fn get_timeavg_file(&self) -> Result<PathBuf, CliError> {
        let output_dir = if let Some(output) = &self.timeavg_file {
            if output.is_file() {
                return Ok(output.to_path_buf());
            }

            output
        } else {
            self.level2_file.parent().ok_or_else(|| {
                CliError::user_error(format!(
                    "Could not get parent directory of L2 file, {}",
                    self.level2_file.display()
                ))
            })?
        };

        let base_name = self.level2_file.file_name().ok_or_else(|| {
            CliError::user_error(format!(
                "Could not get base name of level 2 file, {}",
                self.level2_file.display()
            ))
        })?;
        let output_file = output_dir
            .join(base_name)
            .with_extension(format!("avg{}min.nc", self.time_bin_minutes));
        Ok(output_file)
    }
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error("Bad argument: {0}")]
    UserError(String),
}

impl CliError {
    fn user_error<S: ToString>(msg: S) -> Self {
        Self::UserError(msg.to_string())
    }
}
