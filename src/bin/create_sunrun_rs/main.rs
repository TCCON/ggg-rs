use std::{
    dbg,
    io::BufRead,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    process::ExitCode,
};

use clap::Parser;
use clap_verbosity_flag::{InfoLevel, Verbosity};
use error_stack::ResultExt;
use ggg_rs::{logging::init_logging, sunrun, utils::RunType};

mod site_config;

fn main() -> ExitCode {
    let clargs = Cli::parse();
    init_logging(clargs.verbosity.log_level_filter());
    if let Err(e) = driver(clargs) {
        eprintln!("{e:?}");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

fn driver(clargs: Cli) -> error_stack::Result<(), CliError> {
    // For now, keep the behavior that we use the same site ID as the list file.
    // to find the config file to load. We may want the ability to go by spectrum,
    // but that's not needed now.
    let list_site_id = ggg_rs::utils::site_id_from_filename(&clargs.spec_list_file)
        .change_context_lazy(|| {
            CliError::custom(format!(
                "Error occurred while extracting the site ID from the list file name"
            ))
        })?;

    let gggpath = ggg_rs::utils::get_ggg_path()
        .change_context_lazy(|| CliError::custom("Error occurred while getting the GGGPATH"))?;
    let run_type =
        RunType::try_from(clargs.spec_list_file.as_path()).change_context_lazy(|| {
            CliError::custom(format!(
                "Could not map the extension of {} to a run type (e.g., gnd, lab, etc.)",
                clargs.spec_list_file.display()
            ))
        })?;
    let sunrun_file = gggpath
        .join("tccon")
        .join(format!("{list_site_id}_sunrun.toml"));
    log::info!("Reading configuration from {}", sunrun_file.display());
    let sunrun_cfg: site_config::SiteConfig = ggg_rs::utils::read_toml_file(&sunrun_file)
        .change_context_lazy(|| CliError::custom("Error while reading the sunrun.toml file"))?;

    if clargs.debug_config {
        dbg!(sunrun_cfg);
        return Ok(());
    }

    // Set up the pieces needed to find the spectra
    let data_part = clargs
        .data_part_args
        .get_data_partition()
        .change_context_lazy(|| {
            CliError::custom("Unable to set up data partition for spectrum paths")
        })?;

    let list_rdr = ggg_rs::utils::FileBuf::open(&clargs.spec_list_file)
        .change_context_lazy(|| CliError::custom("Error opening the spectrum list file"))?;

    let output_sunrun = determine_sunrun_name(&clargs.spec_list_file, run_type, &gggpath)?;
    let mut output_f = std::fs::File::create(&output_sunrun).change_context_lazy(|| {
        CliError::custom(format!(
            "Error opening output sunrun file, {}",
            output_sunrun.display()
        ))
    })?;
    log::info!("Writing sunrun to {}", output_sunrun.display());
    sunrun::write_header(&mut output_f).change_context_lazy(|| {
        CliError::Custom(format!(
            "Error writing header to sunrun file, {}",
            output_sunrun.display()
        ))
    })?;

    let mut nspec = 0;
    for (ispec, line) in list_rdr.lines().enumerate() {
        if ispec > 0 && ispec % 1000 == 0 {
            log::info!("Processed {ispec} spectra");
        }
        let line = line.change_context_lazy(|| {
            CliError::custom(format!("Error reading line of spectrum list"))
        })?;
        let specname = line.trim();
        let specpath = data_part
            .find_spectrum(specname)
            .ok_or_else(|| CliError::custom(format!("Spectrum not found: {specname}")))?;
        let (nus, nue) = sunrun_cfg
            .get_nus_nue(specname)
            .change_context_lazy(|| CliError::custom("Error getting nus & nue for spectrum"))?;
        let row = sunrun::SunrunRow::build_from_spectrum(
            &specpath,
            sunrun_cfg.constants.instrument,
            run_type.is_lamp(),
            &sunrun_cfg.constants.defaults,
            sunrun_cfg.constants.object,
            nus,
            nue,
        )
        .change_context_lazy(|| {
            CliError::custom(format!(
                "Error getting header values from spectrum {}",
                specpath.display()
            ))
        })?;

        // Everything else is handled in the `build_from_spectrum` function,
        // but I kept nus & nue

        log::debug!("Writing {row:?}");
        row.write(&mut output_f).change_context_lazy(|| {
            CliError::custom("Error during writing of line to the sunrun file")
        })?;
        nspec += 1;
    }

    log::info!(
        "Completed writing {} with {nspec} spectra",
        output_sunrun.display()
    );
    Ok(())
}

fn determine_sunrun_name(
    list_file: &Path,
    run_type: RunType,
    gggpath: &Path,
) -> error_stack::Result<PathBuf, CliError> {
    let subdir = run_type.subdir().change_context_lazy(|| {
        CliError::custom(format!(
            "Could not map the extension of {} to a sunruns subdirectory",
            list_file.display()
        ))
    })?;
    let basename = list_file.file_prefix().ok_or_else(|| {
        CliError::custom(format!(
            "Could not get base name of list file, {}",
            list_file.display()
        ))
    })?;
    let e_char = list_file
        .extension()
        .ok_or_else(|| {
            CliError::custom(format!(
                "Could not get extension of the list file, {}",
                list_file.display()
            ))
        })?
        .as_bytes()
        .get(0)
        .map(|b| char::from(*b))
        .ok_or_else(|| {
            CliError::custom(format!(
                "Could not get first character of the list file's ({}) extension",
                list_file.display()
            ))
        })?;

    Ok(gggpath
        .join("sunruns")
        .join(subdir)
        .join(basename)
        .with_added_extension(format!("{e_char}op")))
}

#[derive(Debug, Parser)]
struct Cli {
    /// Path to the file containing an ordered list of spectra.
    spec_list_file: PathBuf,

    /// If this flag is given, the program will only parse the
    /// appropriate xx_sunrun.toml file, report an error if one
    /// is encountered, otherwise it will print out a debugging
    /// version of the parsed configuration. In either case, it
    /// stops before writing the sunrun itself.
    #[clap(long)]
    debug_config: bool,

    #[clap(flatten)]
    data_part_args: ggg_rs::utils::DataPartArgs,

    #[command(flatten)]
    verbosity: Verbosity<InfoLevel>,
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error("{0}")]
    Custom(String),
}

impl CliError {
    fn custom<S: ToString>(msg: S) -> Self {
        Self::Custom(msg.to_string())
    }
}
