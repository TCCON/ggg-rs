//! Command line interface definitions
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use clap_verbosity_flag::{InfoLevel, Verbosity};

#[derive(Debug, Parser)]
pub(crate) struct Cli {
    #[clap(subcommand)]
    pub(crate) command: Commands,

    #[command(flatten)]
    pub(crate) verbosity: Verbosity<InfoLevel>,
}

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum Commands {
    /// Convert fill values to netCDF fills based on a TOML file
    Toml(TomlCli),
    /// Create an example of the fill definition file
    TomlTemplate(TemplateCli),
    /// Print out the parsed representation of a TOML configuration file.
    Debug(DebugCli),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct TomlCli {
    #[clap(flatten)]
    pub(crate) output: OutputCli,

    /// Path to a TOML file containing the filter settings.
    pub(crate) toml_file: PathBuf,

    /// The path to the input netCDF file to add flags to
    #[clap(long)]
    pub(crate) nc_file: PathBuf,
}

/// Create an example of the fill definition file
#[derive(Debug, Clone, Args)]
pub(crate) struct TemplateCli {
    /// Path to write the template to
    pub(crate) template_file: PathBuf,
}

/// Print out the parsed representation of a TOML configuration file.
/// This can help confirm you've created the right configuration, or
/// debug the code doing something unexpected.
#[derive(Debug, Clone, Args)]
pub(crate) struct DebugCli {
    /// Path to write the template to
    pub(crate) template_file: PathBuf,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct OutputCli {
    /// Modify the given netCDF file in place. Either this or --output must
    /// be given. Use --output if you prefer not to modify your original netCDF
    /// file.
    #[clap(short = 'i', long, conflicts_with = "output", required = true)]
    // conflicts_with take precedence over required, that's how we defined one of in_place and output is required
    pub(crate) in_place: bool,

    /// Path to write out the modified netCDF file. Either this or --in-place
    /// must be given. Note that if no flags are changed, the output file
    #[clap(short = 'o', long, required = true)]
    pub(crate) output: Option<PathBuf>,
}
