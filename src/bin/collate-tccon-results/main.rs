use std::{collections::HashMap, path::PathBuf, process::ExitCode};

use clap::Parser;
use ggg_rs::{cit_spectrum_name::NoDetectorSpecName, collation::{collate_results, CollationError, CollationIndexer, CollationMode, CollationResult}, output_files::ProgramVersion, runlogs::{FallibleRunlog, RunlogDataRec}};
use log4rs::{encode::pattern::PatternEncoder, append::console::{ConsoleAppender, Target}, Config, config::{Appender, Root}};

fn main() -> ExitCode {
    if let Err(e) = main_inner() {
        eprintln!("ERROR: {e:?}");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

// TODO:
// - Verbosity CL flag
// - After fortformat alignment issue fixed, use that SerSettings::left_align_str instead of padding
//   the string ourselves.
// - Handle the mid-IR and visible windows

fn main_inner() -> error_stack::Result<(), CollationError> {
    let clargs = CollateCli::parse();
    init_logging(log::LevelFilter::Info);
    let multiggg_file = PathBuf::from(&clargs.multiggg_file);
    let collate_version = ProgramVersion { 
        program: "collate_tccon_results".to_string(),
        version: "Version 1.0".to_string(),
        date: "2024-04-28".to_string(),
        authors: "JLL".to_string()
    };
    let indexer = TcconColIndexer::default();
    collate_results(&multiggg_file, indexer, clargs.mode, collate_version)
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
    #[clap(short='m', long, default_value = "./multiggg.sh")]
    multiggg_file: PathBuf,
}

fn init_logging(level: log::LevelFilter) {
    // Eventually it might make sense to log to a file as well, so that
    // ALL of the issues that happened during post processing are captured.
    let stderr = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{h({d(%Y-%m-%d %H:%M:%S)} [{l}] from line {L} in {M})} - {m}{n}")))
        .target(Target::Stderr)
        .build();

    let config = Config::builder()
        .appender(Appender::builder().build("stderr", Box::new(stderr)))
        .build(
            Root::builder()
                .appender("stderr")
                .build(level)
        ).expect("Failed to configure logger");

    log4rs::init_config(config).expect("Failed to initialize logger");
}

#[derive(Debug, Default)]
struct TcconColIndexer {
    index_map: HashMap<NoDetectorSpecName, usize>,
    runlog_data: Vec<RunlogDataRec>,
}

impl CollationIndexer for TcconColIndexer {
    fn parse_runlog(&mut self, runlog: &std::path::Path) -> CollationResult<()> {
        let runlog_iter = FallibleRunlog::open(runlog)
            .map_err(|e| CollationError::could_not_read_file(e.to_string(), runlog))?;

        let mut last_spec = None;
        let mut idx = 0;

        for rec in runlog_iter {
            let mut rec = rec.map_err(|e| CollationError::could_not_read_file(
                format!("error occurred while reading one line of the runlog: {e}"), runlog
            ))?;

            let nd_spec = NoDetectorSpecName::new(&rec.spectrum_name)
                .map_err(|e| CollationError::custom(
                    format!("Could not parse spectrum name '{}': {e}", rec.spectrum_name)
                ))?;

            if Some(&nd_spec) == last_spec.as_ref() {
                // ignore this spectrum; it's a second detector for the same observation as the last one
            } else if self.index_map.contains_key(&nd_spec) {
                return Err(CollationError::custom(format!(
                    "Spectrum '{}' (ignoring the detector) shows up in two separate places in the runlog, this is not allowed.",
                    rec.spectrum_name
                )));
            } else {
                // TODO: fix fortran format to handle left-aligning - there's something broken that's not propagating 
                // settings through when serializing a structure it seems. For now this is a workaround to make sure
                // the spectrum names are left aligned.
                rec.spectrum_name = format!("{:57}", rec.spectrum_name);
                self.index_map.insert(nd_spec.clone(), idx);
                self.runlog_data.push(rec);
                idx += 1;
                last_spec = Some(nd_spec);
            }

        }
        
        Ok(())
    }

    fn get_row_index(&self, spectrum: &str) -> CollationResult<usize> {
        let nd_spec = NoDetectorSpecName::new(spectrum)
            .map_err(|e| CollationError::custom(
                format!("Could not parse spectrum name '{}': {e}", spectrum)
            ))?;

        self.index_map.get(&nd_spec).ok_or_else(|| CollationError::custom(
            format!("Cannot find spectrum '{}' in the runlog (ignoring the detector).", spectrum)
        )).map(|i| *i)
    }

    fn get_runlog_data(&self) -> CollationResult<&[ggg_rs::runlogs::RunlogDataRec]> {
        Ok(&self.runlog_data)
    }
}