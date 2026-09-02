use std::{
    collections::{HashMap, HashSet},
    eprintln,
    fmt::Display,
    hash::RandomState,
    io::{BufRead, Write},
    path::{Path, PathBuf},
    process::ExitCode,
};

use clap::Parser;
use clap_verbosity_flag::{InfoLevel, Verbosity};
use error_stack::ResultExt;
use ggg_rs::{
    logging::init_logging,
    utils::{get_ggg_path, FileBuf, GggError},
};
use itertools::Itertools;

use crate::read_jac_files::{JacBlockLine, JacFileLines};

mod read_jac_files;

fn main() -> ExitCode {
    let clargs = AlignJacCli::parse();
    init_logging(clargs.verbosity.log_level_filter());
    match driver(clargs) {
        Ok(ecode) => ExitCode::from(ecode),
        Err(e) => {
            eprintln!("ERROR: {e:?}");
            ExitCode::FAILURE
        }
    }
}

fn driver(clargs: AlignJacCli) -> error_stack::Result<u8, CliError> {
    clargs.validate()?;

    let jac_files_by_gas = clargs.get_jac_files().change_context_lazy(|| {
        CliError::custom("Error occurred getting the list of Jacobian files")
    })?;

    let mut ecode = 0;
    for (gas, jac_files) in jac_files_by_gas {
        if clargs.gases.is_empty() || clargs.gases.contains(&gas) {
            log::info!(
                "Will align {} files for {gas}: {jac_files:?}",
                jac_files.len()
            );
            let gas_ecode = align_files_for_gas(
                &gas,
                &jac_files,
                &clargs.output_dir,
                clargs.clobber,
                clargs.dry_run,
            )
            .change_context_lazy(|| {
                CliError::Custom(format!("Error aligning Jacobian files for gas '{gas}'"))
            })?;
            ecode |= gas_ecode;
        }
    }

    Ok(ecode)
}

fn get_spectra_in_jac_file(jac_file: &Path) -> error_stack::Result<Vec<String>, CliError> {
    let mut spectra = vec![];
    let lines = read_jac_files::JacFileLines::open(jac_file)?;
    for (iline, line) in lines.enumerate() {
        let line = line.change_context_lazy(|| {
            CliError::Custom(format!(
                "Error occurred while trying to read line {} of {}",
                iline + 1,
                jac_file.display()
            ))
        })?;
        if let JacBlockLine::Spectrum(specname) = line {
            spectra.push(specname.trim().to_string());
        }
    }
    Ok(spectra)
}

fn align_files_for_gas(
    gas: &str,
    jac_files: &[PathBuf],
    out_dir: &Path,
    clobber: bool,
    dry_run: bool,
) -> error_stack::Result<u8, CliError> {
    let spectra: Vec<Vec<String>> = jac_files
        .iter()
        .map(|p| get_spectra_in_jac_file(p))
        .try_collect()?;

    // First, just get the set of spectra common to keep. If this returns None, the input list was
    // empty, so there's nothing to do.
    let common_spectra = if let Some(common) = get_common_spectra(&spectra) {
        common
    } else {
        return Ok(0);
    };

    let mut ecode = 0;
    if common_spectra.is_empty() {
        log::warn!(
            "No common spectra found amoung the {} Jacobian files for gas {}",
            jac_files.len(),
            gas
        );
        ecode |= 4;
    } else {
        log::info!(
            "{} common spectra found among the {} Jacobian files for gas {}",
            common_spectra.len(),
            jac_files.len(),
            gas
        );
    }

    // Check that the common spectra are in the same order. For now at least, we won't support
    // arbitrary reordering, just filtering out spectra missing from some Jacobian files.
    let res = check_spectrum_order(&spectra, &common_spectra, jac_files);
    match res {
        Ok(_) => log::info!("Spectrum order matches among Jacobian files for {gas}"),
        Err(e) => {
            ecode |= 2;
            log::error!("{e}; will not write new files for {gas}")
        }
    }

    if !dry_run {
        for jac_file in jac_files {
            log::info!(
                "Copying {} blocks from {}",
                common_spectra.len(),
                jac_file.display()
            );
            copy_common_blocks(jac_file, &common_spectra, out_dir, clobber)?;
        }
    }

    Ok(ecode)
}

fn get_common_spectra(spectra: &[Vec<String>]) -> Option<HashSet<&str>> {
    let (first_spec_list, other_spec_lists) = spectra.split_first()?;
    let mut common: HashSet<&str, RandomState> =
        HashSet::from_iter(first_spec_list.iter().map(|s| s.as_str()));
    for spec_list in other_spec_lists {
        let tmp: HashSet<&str, _> = HashSet::from_iter(spec_list.iter().map(|s| s.as_str()));
        common = common.intersection(&tmp).map(|s| *s).collect();
    }

    Some(common)
}

fn check_spectrum_order(
    spectra: &[Vec<String>],
    common: &HashSet<&str>,
    jac_files: &[PathBuf],
) -> Result<(), OutOfOrderSpectra> {
    let (first_spec_list, other_spec_lists) = if let Some(t) = spectra.split_first() {
        t
    } else {
        return Ok(());
    };

    let canonical_order = first_spec_list
        .iter()
        .filter_map(|spec| {
            if common.contains(spec.as_str()) {
                Some(spec.as_str())
            } else {
                None
            }
        })
        .collect_vec();

    // This will require allocating Vecs for the other lists, but it makes the logic a lot clearer
    for (iother, spec_list) in other_spec_lists.iter().enumerate() {
        let spec_subset = spec_list
            .iter()
            .filter_map(|spec| {
                if common.contains(spec.as_str()) {
                    Some(spec.as_str())
                } else {
                    None
                }
            })
            .collect_vec();

        // canonical_order and spec_subset must have the same length, because `common` should only
        // have spectra present in both. If not, that is a bug, and a panic is appropriate.
        // We do the iteration to check equality ourselves rather than using == because we want
        // to know where the spectrum mismatch is anyway.
        for (&s1, s2) in canonical_order.iter().zip_eq(spec_subset) {
            if s1 != s2 {
                let s1 = s1.to_string();
                let p1 = jac_files
                    .get(0)
                    .map(|p| p.clone())
                    .unwrap_or_else(|| PathBuf::from("?"));
                let s2 = s2.to_string();
                let p2 = jac_files
                    .get(iother + 1)
                    .map(|p| p.clone())
                    .unwrap_or_else(|| PathBuf::from("?"));
                return Err(OutOfOrderSpectra { p1, s1, p2, s2 });
            }
        }
    }

    Ok(())
}

fn copy_common_blocks(
    jac_file: &Path,
    common: &HashSet<&str>,
    out_dir: &Path,
    clobber: bool,
) -> error_stack::Result<(), CliError> {
    let jac_name = jac_file.file_name().ok_or_else(|| {
        CliError::Custom(format!(
            "Cannot get file name of Jacobian file {}",
            jac_file.display()
        ))
    })?;
    let out_file = out_dir.join(jac_name);
    if out_file.exists() && !clobber {
        return Err(CliError::Custom(format!(
            "Output file {} already exists, use --clobber to overwrite",
            out_file.display()
        ))
        .into());
    }

    let lines = JacFileLines::open(jac_file).change_context_lazy(|| {
        CliError::Custom(format!(
            "Failed to open input Jacobian file {} for copying",
            jac_file.display()
        ))
    })?;

    let mut f_out = std::fs::File::create(&out_file).change_context_lazy(|| {
        CliError::Custom(format!(
            "Error opening output file {} for writing",
            out_file.display()
        ))
    })?;

    let mut copy_block = false;
    for (iline, line) in lines.enumerate() {
        let line = line.change_context_lazy(|| {
            CliError::Custom(format!(
                "Error reading line {} of file {}",
                iline + 1,
                jac_file.display()
            ))
        })?;

        let res = match line {
            JacBlockLine::GlobalHeader(s) => write!(f_out, "{s}"),
            JacBlockLine::Spectrum(s) => {
                let spec = s.trim();
                copy_block = common.contains(spec);
                if copy_block {
                    log::debug!("Copying block for spectrum {spec}");
                    write!(f_out, "{s}")
                } else {
                    log::debug!("NOT copying block for spectrum {spec}");
                    Ok(())
                }
            }
            JacBlockLine::BlockSize(s) | JacBlockLine::Other(s) => {
                if copy_block {
                    write!(f_out, "{s}")
                } else {
                    Ok(())
                }
            }
        };

        res.change_context_lazy(|| {
            CliError::Custom(format!(
                "Error writing line {} from input file {} to output file {}",
                iline + 1,
                jac_file.display(),
                out_file.display()
            ))
        })?;
    }

    Ok(())
}

/// Reduce window Jacobian files to a set of spectra common among them.
///
/// Occasionally, different windows for the same gas will produce Jacobian
/// files that do not have the same spectra (i.e., one or more will be
/// missing certain spectra). This creates a problem for `concat_jac`,
/// which expects the spectra to be aligned. This program will create
/// new Jacobian files which remove spectra not present in all of the
/// files for a given gas.
///
/// There are two limitations. First, the spectra must be in the same
/// order. That is, if one file has spectra 001, 002, and 003, and another
/// has 001 and 003, that is okay, but if the files contain spectra
/// 001, 002, 003 and 001, 003, 002, this code does not handle that.
/// Second, it does not handle spectra that differ only in the detector.
///
/// If your Jacobian files are in `$GGGPATH/jac`, begin with `j_`,
/// and you call this program from the directory containing a `multiggg.sh`
/// file that lists the windows you want to align, then you only need to
/// provide the output directory. Otherwise, you will need to specify
/// what options are different with the optional command line arguments.
///
/// An exit code of 1 means the program encountered a hard error. If the
/// 2 bit is set (i.e., ECODE & 2 > 0), this means that at least one gas's
/// files had spectra in inconsistent order. If the 4 bit is set, this
/// means at least one gas had no common spectra among the input files.
#[derive(Debug, clap::Parser)]
struct AlignJacCli {
    /// Where to output the modified per-window Jacobian files.
    /// This is required and must be different than the directory
    /// containing the input Jacobian files, as the modified files
    /// have the same names as the input files.
    output_dir: PathBuf,

    /// A list of Jacobian files to align.
    ///
    /// These must be full paths to the Jacobian files, but the paths
    /// can be absolute or relative to the current directory.
    /// This cannot be used with the --multiggg-file option and
    /// vice versa. If you do not specify any Jacobian files through
    /// these argument, align_jac will infer them from a multiggg file.
    #[clap(
        conflicts_with = "multiggg_file",
        conflicts_with = "jac_file_prefix",
        conflicts_with = "jac_file_dir"
    )]
    jac_files: Vec<PathBuf>,

    /// Path to the multiggg.sh to determine which windows to align.
    ///
    /// If using a multiggg file, align_jac will assume that each
    /// line of the multiggg file has the form `/path/to/gfit gas_window.runlog.ggg>/dev/null`
    /// in order to parse what windows there are to align. Specifcally,
    /// splitting line first on the string "gfit" and then on ".ggg" must
    /// yield the stem of the Jacobian file, without the --jac-file-prefix.
    /// That is, if there is a line `/home/user/ggg/bin/gfit co2_6220.example.ggg>/dev/null`
    /// in the multiggg file and --jac-file-prefix is the default "j_",
    /// then this implies that there is a Jacobian file named
    /// "j_co2_6220.example"
    #[clap(short = 'm', long)]
    multiggg_file: Option<PathBuf>,

    /// String prepended to the window names from a multiggg file to yield Jacobian file names.
    ///
    /// See --multiggg-file for details. Has no effect if the Jacobian
    /// files are directly specified as positional arguments.
    #[clap(short = 'p', long, default_value = "j_")]
    jac_file_prefix: String,

    /// Directory where the Jacobian files are stored.
    ///
    /// Has no effect if the Jacobian files are specified as positional
    /// arguments (which requires full paths to the files). This is only
    /// used if inferring the Jacobian file names from a multiggg file.
    /// If this argument is not given, then Jacobian files inferred from
    /// a multiggg file are assumed to be under $GGGPATH/jac.
    #[clap(short = 'i', long)]
    jac_file_dir: Option<PathBuf>,

    /// Limit which gases' Jacobian files will be aligned.
    ///
    /// The gas name must match that in the Jacobian file name,
    /// i.e., the part between the prefix and the next underscore,
    /// and this includes case. This argument can be repeated to
    /// limit to multiple gases. For example, --gas hcl --gas n2o
    /// would align both HCl and N2O windows.
    #[clap(short, long = "gas")]
    gases: Vec<String>,

    /// Allow overwriting output files.
    ///
    /// This does not relax the requirement for the output directory
    /// to be different from the input one. align_jac cannot directly
    /// overwrite the input Jacobian files as it does not read them
    /// entirely into memory before beginning to write the output.
    #[clap(short, long)]
    clobber: bool,

    /// Check the input files but do not write the output files.
    ///
    /// This will parse the input Jacobian files but will not write
    /// the output files. Logging will report on things like the
    /// number of common spectra and if the spectra are in order,
    /// so you will normally want to pair this with -v to see that
    /// information. The exit code will still indicate issues with
    /// the input files.
    #[clap(short = 'd', long)]
    dry_run: bool,

    #[command(flatten)]
    verbosity: Verbosity<InfoLevel>,
}

impl AlignJacCli {
    fn validate(&self) -> error_stack::Result<(), CliError> {
        let out = self.output_dir.canonicalize().change_context_lazy(|| {
            CliError::custom("Could not canonicalize the output directory while verifying input/output locations")
        })?;
        let inp = self.get_jac_dir()?.canonicalize().change_context_lazy(|| {
            CliError::custom("Could not canonicalize the input Jacobian directory while verifying input/output locations")
        })?;
        if out == inp {
            return Err(CliError::Custom(format!(
                "Input ({}) and output ({}) directories cannot be the same",
                inp.display(),
                out.display()
            ))
            .into());
        }
        Ok(())
    }
    fn get_jac_files(&self) -> error_stack::Result<HashMap<String, Vec<PathBuf>>, CliError> {
        // For now, we'll mimic concat_jac and read from a multiggg.sh.
        // In the future, we might add a command line option to specify
        // which Jacobian files to read.
        let jac_files = if !self.jac_files.is_empty() {
            log::info!("Using command line list of Jacobian files to align");
            self.jac_files.clone()
        } else if let Some(p) = self.multiggg_file.as_ref() {
            log::info!(
                "Using user-specified multiggg file ({}) to infer list of Jacobian files",
                p.display(),
            );
            self.jac_files_from_multiggg(p).change_context_lazy(|| {
                CliError::Custom(format!(
                    "error reading multiggg file provided by the user ({})",
                    p.display()
                ))
            })?
        } else {
            log::info!("Assume the multiggg.sh file in the current directory has the list of windows to align");
            self.jac_files_from_multiggg(&PathBuf::from("multiggg.sh"))
                .change_context_lazy(|| {
                    CliError::Custom("error reading multiggg file in current directory".to_string())
                })?
        };

        let mut files_by_gas: HashMap<String, Vec<PathBuf>> = HashMap::new();

        let i_gas = self.jac_file_prefix.len();
        for jac_file in jac_files {
            let jac_name = jac_file
                .file_name()
                .ok_or_else(|| {
                    CliError::Custom(format!(
                        "Cannot get base name of input file {}",
                        jac_file.display()
                    ))
                })?
                .to_string_lossy();
            let gas = jac_name[i_gas..]
                .split(|c| c == '_')
                .next()
                .ok_or_else(|| {
                    CliError::Custom(format!(
                        "Could not extract gas name from Jacobian file name: {jac_name}"
                    ))
                })?
                .to_string();
            files_by_gas.entry(gas).or_default().push(jac_file);
        }

        Ok(files_by_gas)
    }

    fn get_jac_dir(&self) -> error_stack::Result<PathBuf, CliError> {
        if let Some(p) = &self.jac_file_dir {
            Ok(p.clone())
        } else {
            let gggdir = get_ggg_path().change_context_lazy(|| {
                CliError::custom(
                    "Error getting GGGPATH environment for default Jacobian file location",
                )
            })?;
            Ok(gggdir.join("jac"))
        }
    }

    fn jac_files_from_multiggg(
        &self,
        multiggg: &Path,
    ) -> error_stack::Result<Vec<PathBuf>, CliError> {
        let f_multiggg = FileBuf::open(multiggg).map_err(|e| CliError::GggError(e))?;
        let mut filenames = vec![];
        let jac_dir = self.get_jac_dir()?;
        for (iline, line) in f_multiggg.lines().enumerate() {
            let line = line.map_err(|e| CliError::IoError(e))?;
            let (_, ggg_file) = line.split_once("gfit").ok_or_else(|| {
                CliError::Custom(format!(
                    "Line {} of the multiggg.sh file could not be split on whitespace",
                    iline + 1
                ))
            })?;

            let (basename, _) = ggg_file.trim().split_once(".ggg").ok_or_else(|| {
                CliError::Custom(format!(
                    "In line {} of the multiggg.file, could not find the '.ggg' extension",
                    iline + 1
                ))
            })?;
            let jac_name = format!("{}{basename}", self.jac_file_prefix);
            filenames.push(jac_dir.join(jac_name));
        }
        Ok(filenames)
    }
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("{0}")]
    GggError(#[from] GggError),
    #[error("{0}")]
    Custom(String),
}

impl CliError {
    pub(crate) fn custom<S: ToString>(msg: S) -> Self {
        Self::Custom(msg.to_string())
    }
}

#[derive(Debug, thiserror::Error)]
struct OutOfOrderSpectra {
    p1: PathBuf,
    s1: String,
    p2: PathBuf,
    s2: String,
}

impl Display for OutOfOrderSpectra {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Spectra are in different order, expected {} given the order in {}, but got {} in {}",
            self.s1,
            self.p1.display(),
            self.s2,
            self.p2.display()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ggg_rs::test_utils::{
        compare_output_text_files, init_test_logging, remove_file_if_exists, test_data_dir,
    };

    #[test]
    fn test_align_jac() {
        init_test_logging(None);
        let jac_in_dir = test_data_dir()
            .join("inputs")
            .join("averaging-kernels")
            .join("jac");
        let jac_exp_dir = test_data_dir()
            .join("expected")
            .join("averaging-kernels")
            .join("jac");
        let jac_out_dir = test_data_dir()
            .join("outputs")
            .join("averaging-kernels")
            .join("jac_simple");
        let multiggg = jac_in_dir.join("multiggg.sh");
        let clargs = AlignJacCli {
            output_dir: jac_out_dir.clone(),
            jac_files: vec![],
            multiggg_file: Some(multiggg),
            jac_file_prefix: "j_".to_string(),
            jac_file_dir: Some(jac_in_dir),
            gases: vec![],
            dry_run: false,
            clobber: true,
            verbosity: Verbosity::default(),
        };
        let files_to_omit = [
            jac_out_dir.join("j_hcl_5702.test"), // not in the multiggg file
        ];
        for f in files_to_omit.iter() {
            remove_file_if_exists(f).expect("should be able to remove extra Jacobian files");
        }
        driver(clargs).expect("Running Jacobian alignment should succeed");
        compare_output_text_files(&jac_exp_dir, &jac_out_dir, "j_hcl_5625.test");
        compare_output_text_files(&jac_exp_dir, &jac_out_dir, "j_hcl_5687.test");
        for f in files_to_omit.iter() {
            assert!(!f.exists(), "{} should not have been created", f.display());
        }
    }

    #[test]
    fn test_align_jac_filter_gases() {
        init_test_logging(None);
        let jac_in_dir = test_data_dir()
            .join("inputs")
            .join("averaging-kernels")
            .join("jac");
        let jac_exp_dir = test_data_dir()
            .join("expected")
            .join("averaging-kernels")
            .join("jac");
        let jac_out_dir = test_data_dir()
            .join("outputs")
            .join("averaging-kernels")
            .join("jac_filter_gases");
        let multiggg = jac_in_dir.join("multiggg_extra_windows.sh");
        let clargs = AlignJacCli {
            output_dir: jac_out_dir.clone(),
            jac_files: vec![],
            multiggg_file: Some(multiggg),
            jac_file_prefix: "j_".to_string(),
            jac_file_dir: Some(jac_in_dir),
            gases: vec!["hcl".to_string()],
            dry_run: false,
            clobber: true,
            verbosity: Verbosity::default(),
        };

        let files_to_omit = [
            jac_out_dir.join("j_hcl_5702.test"), // not in the multiggg file
            jac_out_dir.join("j_co2_6220.test"), // excluded by gases arg
            jac_out_dir.join("j_co2_6339.test"), // excluded by gases arg
        ];
        for f in files_to_omit.iter() {
            remove_file_if_exists(f).expect("should be able to remove extra Jacobian files");
        }

        driver(clargs).expect("Running Jacobian alignment should succeed");
        compare_output_text_files(&jac_exp_dir, &jac_out_dir, "j_hcl_5625.test");
        compare_output_text_files(&jac_exp_dir, &jac_out_dir, "j_hcl_5687.test");
        for f in files_to_omit.iter() {
            assert!(!f.exists(), "{} should not have been created", f.display());
        }
    }

    #[test]
    fn test_align_jac_specify_files() {
        init_test_logging(None);
        let jac_in_dir = test_data_dir()
            .join("inputs")
            .join("averaging-kernels")
            .join("jac");
        let jac_exp_dir = test_data_dir()
            .join("expected")
            .join("averaging-kernels")
            .join("jac");
        let jac_out_dir = test_data_dir()
            .join("outputs")
            .join("averaging-kernels")
            .join("jac_specify_files");
        let clargs = AlignJacCli {
            output_dir: jac_out_dir.clone(),
            jac_files: vec![
                jac_in_dir.join("j_hcl_5625.test"),
                jac_in_dir.join("j_hcl_5687.test"),
            ],
            multiggg_file: None,
            jac_file_prefix: "j_".to_string(),
            jac_file_dir: Some(jac_in_dir),
            gases: vec!["hcl".to_string()],
            dry_run: false,
            clobber: true,
            verbosity: Verbosity::default(),
        };

        let files_to_omit = [
            jac_out_dir.join("j_hcl_5702.test"), // not in the list of files
        ];
        for f in files_to_omit.iter() {
            remove_file_if_exists(f).expect("should be able to remove extra Jacobian files");
        }

        driver(clargs).expect("Running Jacobian alignment should succeed");
        compare_output_text_files(&jac_exp_dir, &jac_out_dir, "j_hcl_5625.test");
        compare_output_text_files(&jac_exp_dir, &jac_out_dir, "j_hcl_5687.test");
        for f in files_to_omit.iter() {
            assert!(!f.exists(), "{} should not have been created", f.display());
        }
    }

    #[test]
    fn test_align_jac_dry_run() {
        init_test_logging(None);
        let jac_files = ["j_hcl_5625.test", "j_hcl_5687.test"];

        let jac_in_dir = test_data_dir()
            .join("inputs")
            .join("averaging-kernels")
            .join("jac");
        let jac_out_dir = test_data_dir()
            .join("outputs")
            .join("averaging-kernels")
            .join("jac_dry_run");
        let jac_in_files = jac_files.iter().map(|f| jac_in_dir.join(f)).collect_vec();
        let jac_out_files = jac_files.iter().map(|f| jac_out_dir.join(f)).collect_vec();
        let clargs = AlignJacCli {
            output_dir: jac_out_dir.clone(),
            jac_files: jac_in_files,
            multiggg_file: None,
            jac_file_prefix: "j_".to_string(),
            jac_file_dir: Some(jac_in_dir),
            gases: vec!["hcl".to_string()],
            dry_run: true,
            clobber: true,
            verbosity: Verbosity::default(),
        };

        // No files should be produced in a dry run
        for f in jac_out_files.iter() {
            remove_file_if_exists(f).expect("should be able to remove extra Jacobian files");
        }

        driver(clargs).expect("Running Jacobian alignment should succeed");

        for f in jac_out_files.iter() {
            assert!(!f.exists(), "{} should not have been created", f.display());
        }
    }

    #[test]
    fn test_align_jac_exit_code_out_of_order() {
        test_exit_codes_inner("jac_misordered", 2);
    }

    #[test]
    fn test_align_jac_exit_no_common() {
        test_exit_codes_inner("jac_no_common", 4);
    }

    fn test_exit_codes_inner(input_subdir: &str, expected_ecode: u8) {
        init_test_logging(None);
        let jac_in_dir = test_data_dir()
            .join("inputs")
            .join("averaging-kernels")
            .join(input_subdir);
        let jac_out_dir = test_data_dir().join("outputs").join("averaging-kernels"); // shouldn't output anything because we set dry_run = true
        let multiggg = jac_in_dir.join("multiggg.sh");
        let clargs = AlignJacCli {
            output_dir: jac_out_dir.clone(),
            jac_files: vec![],
            multiggg_file: Some(multiggg),
            jac_file_prefix: "j_".to_string(),
            jac_file_dir: Some(jac_in_dir),
            gases: vec![],
            dry_run: true,
            clobber: true,
            verbosity: Verbosity::default(),
        };

        let ecode = driver(clargs).expect("Running Jacobian alignment should succeed");
        assert_eq!(
            ecode, expected_ecode,
            "Got a different exit code than expected"
        );
    }
}
