use std::{collections::HashMap, path::PathBuf, process::ExitCode};

use clap::Parser;
use clap_verbosity_flag::{InfoLevel, Verbosity};
use error_stack::ResultExt;
use ggg_rs::{
    cit_spectrum_name::{CitDetector, CitSpectrumName, NoDetectorSpecName},
    collation::{
        collate_results, CollationError, CollationIndexer, CollationMode, CollationResult,
    },
    logging::init_logging,
    o2_dmf::{make_boxed_o2_dmf_provider, O2DmfCli},
    readers::{
        runlogs::{FallibleRunlog, RunlogDataRec},
        ProgramVersion,
    },
    tccon::input_config::TcconWindowPrefixes,
    utils::GggCompatibilityCli,
};

fn main() -> ExitCode {
    let clargs = CollateCli::parse();
    // This moved outside main_inner() for now to avoid panics during tests
    // calling main_inner and trying to initialize logging more than once.
    init_logging(clargs.verbosity.log_level_filter());
    if let Err(e) = main_inner(clargs) {
        eprintln!("ERROR: {e:?}");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

// TODO:
// - Test the mid-IR windows (visible seems ok)

fn main_inner(clargs: CollateCli) -> error_stack::Result<(), CollationError> {
    let multiggg_file = PathBuf::from(&clargs.multiggg_file);
    let collate_version = ProgramVersion {
        program: "collate_tccon_results".to_string(),
        version: "Version 1.0".to_string(),
        date: "2024-04-28".to_string(),
        authors: "JLL".to_string(),
    };
    let indexer = TcconColIndexer::new(clargs.primary_detector);

    // I think eventually we will require a prefix file. But for now, I want to be able to use
    // this without needing a prefix file.
    let prefixer = if let Some(p) = clargs.prefix_file {
        Some(TcconWindowPrefixes::new(&p).change_context_lazy(|| {
            CollationError::custom("Error getting the detector prefixes")
        })?)
    } else {
        TcconWindowPrefixes::new_standard_opt().change_context_lazy(|| {
            CollationError::custom("Error getting the detector prefixes from the standard file")
        })?
    };

    let run_dir = clargs.multiggg_file.parent().ok_or_else(|| {
        CollationError::CouldNotFind("parent directory of the multiggg.sh file".to_string())
    })?;
    let o2_provider = make_boxed_o2_dmf_provider(&clargs.o2_dmf_args, run_dir)
        .change_context_lazy(|| {
            CollationError::custom(
                "An error occurred while setting up the O2 mean mole fraction provider",
            )
        })?;

    collate_results(
        &multiggg_file,
        indexer,
        prefixer,
        o2_provider,
        clargs.mode,
        collate_version,
        clargs.output_dir.as_deref(),
        clargs.write_nts,
        clargs.compatibility.into(),
    )
}

#[derive(Debug, clap::Parser)]
struct CollateCli {
    /// What quantity to collate: 'v' will compute vertical
    /// column densities, 't' will extract the VSFs. (Other
    /// modes not yet implemented.)
    mode: CollationMode,

    /// Which multiggg.sh file that defines the windows to process.
    /// .col files will be read from the same directory as this file,
    /// and any relative paths needed in the .col headers will be interpreted
    /// as relative to that directory
    #[clap(short = 'm', long, default_value = "./multiggg.sh")]
    multiggg_file: PathBuf,

    /// Which detector is considered the "primary" detector; this will affect
    /// which auxiliary values (year, day, hour, zmin, met data, etc.) are written.
    /// For such values, those associated with the primary detector will take precedence
    /// over those with the secondary detector (though the secondary detector's values
    /// will be used if no primary detector is present). That is, by default, if
    /// the "a" spectrum has a `zmin` value of 0.100 and the "c" spectrum has a `zmin` value
    /// of "0.120", then the output `.Xsw` file will have `zmin = 0.100` for this entry.
    /// But, passing --primary-detector=c in this example would make `zmin = 0.120`.
    /// This option takes any single character (usually "a", "b", or "c") or recognized
    /// detector long names - see documentation for [`CitDetector`] for a list.
    #[clap(short='p', long, default_value_t = CitDetector::InGaAs)]
    primary_detector: CitDetector,

    /// Write out "collate_results.nts" listing spectra with a ZPD time earlier than
    /// the preceding spectrum in the runlog. This is not written by default, because
    /// collate_tccon_results does not rely on the runlog to be time-ordered.
    #[clap(short = 'n', long)]
    write_nts: bool,

    /// Path to the file that defines the specie's prefixes for different frequency
    /// ranges. If not given, will use the file at $GGGPATH/tccon/secondary_prefixes.dat
    /// if it exists. Giving a path to this argument that does not exist is an error.
    #[clap(long)]
    prefix_file: Option<PathBuf>,

    #[command(flatten)]
    o2_dmf_args: O2DmfCli,

    /// What directory to write the output into. If not given, then output will be written
    /// in the same directory as the multiggg.sh file.
    #[clap(short = 'o', long)]
    output_dir: Option<PathBuf>,

    #[command(flatten)]
    compatibility: GggCompatibilityCli,

    #[command(flatten)]
    verbosity: Verbosity<InfoLevel>,
}

#[derive(Debug)]
struct TcconColIndexer {
    index_map: HashMap<NoDetectorSpecName, usize>,
    runlog_data: Vec<RunlogDataRec>,
    neg_timesteps: Vec<(RunlogDataRec, RunlogDataRec)>,
    primary_detector: CitDetector,
}

impl TcconColIndexer {
    fn new(primary_detector: CitDetector) -> Self {
        Self {
            primary_detector,
            index_map: HashMap::new(),
            neg_timesteps: vec![],
            runlog_data: vec![],
        }
    }
}

impl CollationIndexer for TcconColIndexer {
    fn parse_runlog(&mut self, runlog: &std::path::Path) -> CollationResult<()> {
        let runlog_iter = FallibleRunlog::open(runlog)
            .map_err(|e| CollationError::could_not_read_file(e.to_string(), runlog))?;

        let mut last_spec = None;
        let mut prev_rec: Option<RunlogDataRec> = None;
        let mut idx = 0;

        for rec in runlog_iter {
            let rec = rec.map_err(|e| {
                CollationError::could_not_read_file(
                    format!("error occurred while reading one line of the runlog: {e}"),
                    runlog,
                )
            })?;

            if let Some(was) = prev_rec {
                let time_was = was.zpd_time();
                let time_is = rec.zpd_time();
                match (time_was, time_is) {
                    (Some(t_was), Some(t_is)) => {
                        if t_is < t_was {
                            self.neg_timesteps.push((was.clone(), rec.clone()));
                        }
                    },
                    (None, Some(_)) => log::warn!("Could not convert time for spectrum {}, cannot check for negative time steps", rec.spectrum_name),
                    // the last two arms are empty so we don't repeat the warning
                    (Some(_), None) => (),
                    (None, None) => (),
                }
            }

            let nd_spec = NoDetectorSpecName::new(&rec.spectrum_name).map_err(|e| {
                CollationError::custom(format!(
                    "Could not parse spectrum name '{}': {e}",
                    rec.spectrum_name
                ))
            })?;

            if Some(&nd_spec) == last_spec.as_ref() {
                // ignore this spectrum; it's a second detector for the same observation as the last one
            } else if self.index_map.contains_key(&nd_spec) {
                return Err(CollationError::custom(format!(
                    "Spectrum '{}' (ignoring the detector) shows up in two separate places in the runlog, this is not allowed.",
                    rec.spectrum_name
                )));
            } else {
                self.index_map.insert(nd_spec.clone(), idx);
                self.runlog_data.push(rec.clone());
                idx += 1;
                last_spec = Some(nd_spec);
            }

            prev_rec = Some(rec);
        }

        Ok(())
    }

    fn get_row_index(&self, spectrum: &str) -> CollationResult<usize> {
        let nd_spec = NoDetectorSpecName::new(spectrum).map_err(|e| {
            CollationError::custom(format!("Could not parse spectrum name '{}': {e}", spectrum))
        })?;

        self.index_map
            .get(&nd_spec)
            .ok_or_else(|| {
                CollationError::custom(format!(
                    "Cannot find spectrum '{}' in the runlog (ignoring the detector).",
                    spectrum
                ))
            })
            .map(|i| *i)
    }

    fn get_runlog_data(&self) -> CollationResult<&[ggg_rs::readers::runlogs::RunlogDataRec]> {
        Ok(&self.runlog_data)
    }

    fn get_negative_runlog_timesteps(&self) -> CollationResult<&[(RunlogDataRec, RunlogDataRec)]> {
        Ok(&self.neg_timesteps)
    }

    fn do_replace_value(&self, new_spectrum: &str, column_name: &str) -> CollationResult<bool> {
        // For standard TCCON use, we want auxiliary data like the time, met, zmin, etc. to come from
        // the primary detector (usually InGaAs) because that detector provides the key CO2 and CH4
        // products.
        // As in the default implementation, we assume that using the current compatibility is
        // reasonable; i.e., we want to check against the biggest list of auxiliary fields.
        if ggg_rs::readers::postproc_files::AuxData::postproc_fields_str(
            ggg_rs::utils::GggCompatibility::Current,
        )
        .contains(&column_name)
        {
            let new_spectrum: CitSpectrumName = new_spectrum.parse().map_err(|e| {
                CollationError::parsing_error(format!(
                    "could not parse spectrum name '{new_spectrum}': {e}"
                ))
            })?;
            if new_spectrum.detector() == self.primary_detector {
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Err(CollationError::duplicate_value(new_spectrum, column_name))
        }
    }
}

#[cfg(test)]
mod tests {
    use ggg_rs::o2_dmf::DEFAULT_O2_DMF;
    use ggg_rs::test_utils::compare_output_text_files;
    use ggg_rs::utils::GggCompatibilityInput;

    use super::*;

    #[test]
    fn test_collate_pa_benchmark_vsw() {
        test_inner(
            CollationMode::VerticalColumns,
            GggCompatibilityInput::Current,
            "pa_ggg_benchmark.vsw",
        );
    }

    #[test]
    fn test_collate_pa_benchmark_vsw_stable() {
        // This will test that we correctly produce a .vsw file compatible
        // with the previous release of GGG
        test_inner(
            CollationMode::VerticalColumns,
            GggCompatibilityInput::Stable,
            "pa_ggg_benchmark.vsw",
        );
    }

    #[test]
    fn test_collate_pa_benchmark_tsw() {
        test_inner(
            CollationMode::VmrScaleFactors,
            GggCompatibilityInput::Current,
            "pa_ggg_benchmark.tsw",
        );
    }

    #[test]
    fn test_collate_pa_benchmark_tsw_stable() {
        // This will test that we correctly produce a .vsw file compatible
        // with the previous release of GGG
        test_inner(
            CollationMode::VmrScaleFactors,
            GggCompatibilityInput::Stable,
            "pa_ggg_benchmark.tsw",
        );
    }

    fn test_inner(mode: CollationMode, compat: GggCompatibilityInput, out_file_name: &str) {
        let subdir = match compat {
            GggCompatibilityInput::Current => "collate-tccon-results",
            GggCompatibilityInput::Stable => "collate-tccon-results-stable",
            GggCompatibilityInput::GGG2020 => "collate-tccon-results-ggg2020",
        };

        let crate_root = env!("CARGO_MANIFEST_DIR");
        let input_dir = PathBuf::from(crate_root)
            .join("test-data")
            .join("inputs")
            .join("collate-tccon-results");
        let expected_dir = PathBuf::from(crate_root)
            .join("test-data")
            .join("expected")
            .join(subdir);
        let output_dir = PathBuf::from(crate_root)
            .join("test-data")
            .join("outputs")
            .join(subdir);
        let clargs = CollateCli {
            mode,
            multiggg_file: input_dir.join("multiggg.sh"),
            primary_detector: CitDetector::InGaAs,
            write_nts: false,
            prefix_file: Some(input_dir.join("secondary_prefixes.dat")),
            o2_dmf_args: O2DmfCli {
                fixed_o2_dmf: Some(DEFAULT_O2_DMF),
                o2_dmf_file: None,
            },
            output_dir: Some(output_dir.clone()),
            compatibility: GggCompatibilityCli::new(compat),
            verbosity: Verbosity::new(0, 0),
        };
        main_inner(clargs).expect("running collation should succeed");

        compare_output_text_files(&expected_dir, &output_dir, out_file_name);
    }
}
