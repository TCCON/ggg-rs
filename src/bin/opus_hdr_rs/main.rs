use std::{
    collections::HashSet,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    println,
    process::ExitCode,
};

use clap::Parser;

use clap_verbosity_flag::{InfoLevel, Verbosity};
use error_stack::ResultExt;
use ggg_rs::{
    logging::init_logging,
    opus::{
        constants::bruker::{BpvExportSerialization, BrukerParValue},
        IgramHeader,
    },
};
use indexmap::IndexMap;
use itertools::Itertools;

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

/// Print quantities from an Opus interferogram or spectrum header.
///
/// This is similar to the OpusHdr Perl program, but the block labels
/// will match the [`ggg_rs::opus::constants::bruker::BrukerBlockType`]
/// enum and it has more options for output.
#[derive(Debug, Parser)]
struct Cli {
    /// Path to the spectrum or interferogram.
    /// You can provide multiple spectra/interferograms/slices
    /// list their header values sequentially.
    spec_or_igm: Vec<PathBuf>,

    /// Give a path to a file to write the output. The file
    /// must have ".csv" or ".json" as its extension; this
    /// will determine which type of file is written. JSON
    /// files will be maps of file -> block -> parameter.
    /// CSV files will have one row per file, with the block &
    /// parameter as the column name.
    #[clap(short, long)]
    output: Option<PathBuf>,

    /// Which parameters to output. Repeat to include multiple
    /// parameters. If there are no -p options, then all parameters
    /// are included. This must be a three letter acronym for a parameter,
    /// e.g. "DAT" for the date. There is no way currently to select
    /// which block to read from.
    #[clap(short, long)]
    params: Vec<String>,

    /// When writing a CSV file, this option will omit the block name
    /// from the column headers, but this also means that if the same
    /// parameter appears in multiple blocks, only the last block read
    /// will be represented for that parameter. Use with care!
    #[clap(long)]
    no_block: bool,

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

fn driver(clargs: Cli) -> error_stack::Result<(), CliError> {
    let output_method = OutputMethod::try_from(clargs.output)?;
    output_method.output(&clargs.spec_or_igm, &clargs.params, clargs.no_block)?;
    Ok(())
}

enum OutputMethod {
    Stdout,
    Json(PathBuf),
    Csv(PathBuf),
}

impl TryFrom<Option<PathBuf>> for OutputMethod {
    type Error = CliError;

    fn try_from(value: Option<PathBuf>) -> Result<Self, Self::Error> {
        if let Some(p) = value {
            match p.extension().map(|ext| ext.as_bytes()) {
                Some(b"json") => Ok(Self::Json(p)),
                Some(b"csv") => Ok(Self::Csv(p)),
                Some(ext) => {
                    let ext = String::from_utf8_lossy(ext);
                    Err(CliError::Custom(format!(
                        "Output of type '{ext}' not implemented"
                    )))
                }
                None => Err(CliError::Custom(format!(
                    "Could not infer output type from path {} with no extension",
                    p.display()
                ))),
            }
        } else {
            Ok(Self::Stdout)
        }
    }
}

impl OutputMethod {
    fn output(
        &self,
        spec_or_igms: &[PathBuf],
        only_params: &[String],
        no_block: bool,
    ) -> error_stack::Result<(), CliError> {
        match self {
            OutputMethod::Stdout => Self::output_stdout(spec_or_igms, only_params),
            OutputMethod::Json(path_buf) => Self::output_json(path_buf, spec_or_igms, only_params),
            OutputMethod::Csv(path_buf) => {
                Self::output_csv(path_buf, spec_or_igms, only_params, no_block)
            }
        }
    }

    fn output_stdout(
        spec_or_igms: &[PathBuf],
        only_params: &[String],
    ) -> error_stack::Result<(), CliError> {
        for (pos, spec_or_igm) in spec_or_igms.into_iter().with_position() {
            let header = IgramHeader::read_full_igram_header(&spec_or_igm)
                .change_context_lazy(|| CliError::custom("Error reading file header"))?;

            println!("{}:", spec_or_igm.display());
            for (block_type, block) in header.parameter_blocks {
                let mut printed_block = false;
                for (param_key, param_val) in block {
                    if only_params.is_empty() || only_params.contains(&param_key) {
                        if !printed_block {
                            println!("{block_type}");
                            printed_block = true;
                        }
                        println!("  {param_key}: {}", param_val.display_with_type())
                    }
                }
            }
            let is_last = match pos {
                itertools::Position::First => false,
                itertools::Position::Middle => false,
                itertools::Position::Last => true,
                itertools::Position::Only => true,
            };

            if !is_last {
                println!("");
            }
        }
        Ok(())
    }

    fn output_json(
        output_path: &Path,
        spec_or_igms: &[PathBuf],
        only_params: &[String],
    ) -> error_stack::Result<(), CliError> {
        let mut output_map: IndexMap<
            String,
            IndexMap<String, IndexMap<String, BpvExportSerialization>>,
        > = IndexMap::new();
        for spec_or_igm in spec_or_igms {
            let file_name = spec_or_igm
                .file_name()
                .ok_or_else(|| {
                    CliError::custom(format!("Cannot get file name of {}", spec_or_igm.display()))
                })?
                .to_string_lossy();
            let header = IgramHeader::read_full_igram_header(&spec_or_igm)
                .change_context_lazy(|| CliError::custom("Error reading file header"))?;
            for (block_type, block) in header.parameter_blocks {
                for (param_key, param_val) in block {
                    if only_params.is_empty() || only_params.contains(&param_key) {
                        output_map
                            .entry(file_name.to_string())
                            .or_default()
                            .entry(block_type.to_string())
                            .or_default()
                            .insert(param_key, param_val.ser_export());
                    }
                }
            }
        }

        let f = std::fs::File::create(output_path).change_context_lazy(|| {
            CliError::custom(format!(
                "Cannot create output file {}",
                output_path.display()
            ))
        })?;
        serde_json::to_writer_pretty(f, &output_map).change_context_lazy(|| {
            CliError::custom(format!(
                "Cannot write to output file {}",
                output_path.display()
            ))
        })?;
        Ok(())
    }

    fn output_csv(
        output_path: &Path,
        spec_or_igms: &[PathBuf],
        only_params: &[String],
        no_block: bool,
    ) -> error_stack::Result<(), CliError> {
        let mut colnames = HashSet::new();
        colnames.insert("spectrum".to_string());
        let mut rows = vec![];
        for spec_or_igm in spec_or_igms {
            let file_name = spec_or_igm
                .file_name()
                .ok_or_else(|| {
                    CliError::custom(format!("Cannot get file name of {}", spec_or_igm.display()))
                })?
                .to_string_lossy();
            let header = IgramHeader::read_full_igram_header(&spec_or_igm)
                .change_context_lazy(|| CliError::custom("Error reading file header"))?;

            let mut this_row = IndexMap::new();
            this_row.insert(
                "spectrum".to_string(),
                BpvExportSerialization::new(BrukerParValue::String(file_name.to_string())),
            );
            for (block_type, block) in header.parameter_blocks {
                for (param_key, param_val) in block {
                    if only_params.is_empty() || only_params.contains(&param_key) {
                        let full_colname = if no_block {
                            param_key
                        } else {
                            format!("{block_type}: {param_key}")
                        };
                        colnames.insert(full_colname.clone());
                        let replaced = this_row
                            .insert(full_colname.clone(), param_val.ser_export())
                            .is_some();
                        if replaced {
                            log::warn!("For file {file_name}, parameter {full_colname} exists in multiple blocks, only the last block's value will be included");
                        }
                    }
                }
            }
            rows.push(this_row);
        }

        let mut writer = csv::Writer::from_path(output_path).change_context_lazy(|| {
            CliError::custom(format!(
                "Cannot create file {} for writing",
                output_path.display()
            ))
        })?;

        let mut colnames = Vec::from_iter(colnames);
        colnames.sort_unstable_by(csv_sort);
        writer
            .write_record(&colnames)
            .change_context_lazy(|| CliError::custom("Error writing header row"))?;
        for row in rows {
            let csv_row: Vec<Option<&BpvExportSerialization>> =
                Vec::from_iter(colnames.iter().map(|c| row.get(c)));
            writer
                .serialize(csv_row)
                .change_context_lazy(|| CliError::custom("Error writing data row"))?;
        }
        Ok(())
    }
}

fn csv_sort(a: &String, b: &String) -> std::cmp::Ordering {
    if a == "spectrum" {
        std::cmp::Ordering::Less
    } else if b == "spectrum" {
        std::cmp::Ordering::Greater
    } else {
        a.cmp(b)
    }
}
