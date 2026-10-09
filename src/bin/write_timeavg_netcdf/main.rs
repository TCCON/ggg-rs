use std::{path::PathBuf, process::ExitCode};

use clap::Parser;
use clap_verbosity_flag::{InfoLevel, Verbosity};
use ggg_rs::logging::init_logging;

use crate::data_structures::Gas;
use average_site::read_l2::{ch4_bin_def, co2_bin_def};

mod average_site;
#[cfg(test)]
mod common_test;
mod daily_files;
mod data_structures;
mod station_ids;

fn main() -> ExitCode {
    let clargs = Cli::parse();

    init_logging(clargs.verbosity.log_level_filter());

    let res = match clargs.command {
        Subcommand::AvgSite(avg_site_cli) => avg_site_cli.driver(),
        Subcommand::Daily(daily_cli) => daily_cli.driver(),
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
    Daily(DailyFileCli),
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
        average_site::average_site_driver(
            &self.level2_file,
            self.get_timeavg_file()?.as_path(),
            &[co2_bin_def(), ch4_bin_def()],
            self.get_time_bin(),
            None,
        )
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

    fn get_time_bin(&self) -> chrono::Duration {
        chrono::Duration::minutes(self.time_bin_minutes as i64)
    }
}

#[derive(Debug, Clone, clap::Args)]
struct DailyFileCli {
    /// The per-site averaged files; the result of the avg-site subcommand.
    average_files: Vec<PathBuf>,

    /// Which directory to write the daily files into
    #[clap(long, short = 'o', default_value = ".")]
    output_dir: PathBuf,

    /// Which gases to process. If any input file is missing the requisite
    /// group, that is an error.
    #[clap(long, short = 'g', default_values_t = vec![Gas::CO2, Gas::CH4])]
    gases: Vec<Gas>,

    /// Date to start processing on or after. If the earliest date in the files
    /// is after this date, processing will start on that date.
    start_date: Option<chrono::NaiveDate>,

    /// Date to stop processing on or before. If the latest date in the files
    /// is before this date, processing will stop on that date.
    end_date: Option<chrono::NaiveDate>,
}

impl DailyFileCli {
    fn driver(&self) -> error_stack::Result<(), CliError> {
        daily_files::daily_files_driver(
            &self.average_files,
            &self.output_dir,
            &self.gases,
            self.start_date,
            self.end_date,
        )
    }
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error("Bad argument: {0}")]
    UserError(String),
    #[error("{0}")]
    Custom(String),
    #[error("{0}")]
    Context(String),
}

impl CliError {
    fn user_error<S: ToString>(msg: S) -> Self {
        Self::UserError(msg.to_string())
    }

    fn custom<S: ToString>(msg: S) -> Self {
        Self::Custom(msg.to_string())
    }

    fn context<S: ToString>(ctx: S) -> Self {
        Self::Context(ctx.to_string())
    }
}
