use std::{
    ffi::OsString,
    io::{BufRead, BufReader, Write},
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    process::ExitCode,
};

use clap::Parser;
use error_stack::ResultExt;
use ggg_rs::utils;
use itertools::Itertools;

fn main() -> ExitCode {
    let args = Cli::parse();
    if args.no_op() {
        eprintln!("No operation requested, aborting.");
        return ExitCode::FAILURE;
    }

    if let Err(e) = driver(args) {
        eprintln!("ERROR: {e:?}");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

fn driver(args: Cli) -> error_stack::Result<(), CliError> {
    let files_to_change = args.expand_change_targets()?;

    let start_time = chrono::Local::now();
    let backup_suffix = OsString::from(format!(".bak.{}", start_time.format("%Y%m%dT%H%M%S")));

    for file in files_to_change {
        if !args.no_backup {
            utils::make_backup(&file, &backup_suffix, false)
                .change_context_lazy(|| CliError::IoError)?;
        }
        modify_ggg_file(&file, &args)?;
    }

    Ok(())
}

/// Modify .ggg files to change the paths or maximum number of AK files or SPT files output
#[derive(Debug, clap::Parser)]
struct Cli {
    /// .ggg files or directories containing .ggg files to change
    change_targets: Vec<PathBuf>,

    /// Directory, or directory pattern, to save the spectral fit files
    /// under. The substring {WINDOW} will be replaced with the window name
    /// for the current .ggg file, e.g. "co2_6220".
    #[clap(short = 's', long)]
    spt_output_pattern: Option<String>,

    /// The maximum number of spectral fit files to allow GGG to write.
    #[clap(long, visible_alias = "spt-limit")]
    spt_output_limit: Option<u64>,

    /// Directory, or directory pattern, to save the averaging kernel files
    /// under. The substring {WINDOW} will be replaced with the window name
    /// for the current .ggg file, e.g. "co2_6220"
    #[clap(short = 'a', long)]
    ak_output_pattern: Option<String>,

    /// The maximum number of averaging kernel files to allow GGG to write.
    #[clap(long, visible_alias = "ak_limit")]
    ak_output_limit: Option<u64>,

    /// Set this flag to create the spectral fit and averaging kernel output
    /// directories if they don't exist.
    #[clap(short = 'm', long)]
    make_output_dirs: bool,

    /// Don't backup files before changing them
    #[clap(short = 'n', long)]
    no_backup: bool,
}

impl Cli {
    fn expand_change_targets(&self) -> error_stack::Result<Vec<PathBuf>, CliError> {
        if self.change_targets.is_empty() {
            return Err(CliError::UserError(
                "Pass at least one file or directory to change".to_string(),
            )
            .into());
        }
        let mut out = vec![];
        for path in self.change_targets.iter() {
            if path.is_file() {
                // Assume it is a .ggg file, even if the extension doesn't match
                out.push(path.clone());
            } else if path.is_dir() {
                // Get .ggg files in this directory
                for entry in std::fs::read_dir(path).change_context_lazy(|| CliError::IoError)? {
                    let entry = entry.change_context_lazy(|| CliError::IoError)?;
                    let path = entry.path();
                    let extension = path
                        .extension()
                        .map(|ext| ext.to_str())
                        .flatten()
                        .unwrap_or_default();
                    if extension == "ggg" {
                        out.push(path);
                    }
                }
            } else {
                eprintln!("WARNING: {} does not exist", path.display());
            }
        }

        Ok(out)
    }

    fn no_op(&self) -> bool {
        if self.ak_output_pattern.is_some() {
            return false;
        }
        if self.ak_output_limit.is_some() {
            return false;
        }
        if self.spt_output_pattern.is_some() {
            return false;
        }
        if self.spt_output_limit.is_some() {
            return false;
        }

        true
    }
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error("Problem with path {}: {1}", .0.display())]
    PathError(PathBuf, String),
    #[error("Unexpected format: {0}")]
    FileFormatError(String),
    #[error("Problem occurred in file {}", .0.display())]
    InFile(PathBuf),
    #[error("There was a problem with an I/O operation")]
    IoError,
    #[error("{0}")]
    UserError(String),
}

fn modify_ggg_file(ggg_file: &Path, args: &Cli) -> error_stack::Result<(), CliError> {
    let f = std::fs::File::open(ggg_file).change_context_lazy(|| CliError::IoError)?;
    let f = BufReader::new(f);
    let lines: Vec<String> = f
        .lines()
        .try_collect()
        .change_context_lazy(|| CliError::IoError)
        .attach_printable_lazy(|| format!("Could not read lines from {}", ggg_file.display()))?;

    let mut out = std::fs::File::create(ggg_file).change_context_lazy(|| CliError::IoError)?;

    // This uses an unsafe operation, but since we only split the bytes of the file name on an ASCII .,
    // there is no reason that the slice of bytes leading up to that should be an invalid OsStr.
    let window = ggg_file
        .file_name()
        .ok_or_else(|| {
            CliError::PathError(
                ggg_file.to_path_buf(),
                "Could not get file basename".to_string(),
            )
        })?
        .as_bytes()
        .split(|b| b == &b'.')
        .next()
        .ok_or_else(|| {
            CliError::PathError(
                ggg_file.to_path_buf(),
                "Cannot get window from .ggg file name".to_string(),
            )
        })?;
    let window = String::from_utf8(window.to_owned()).change_context_lazy(|| {
        CliError::PathError(
            ggg_file.to_path_buf(),
            "Window name in .ggg file contains invalid unicode".to_string(),
        )
    })?;

    for (i, line) in lines.into_iter().enumerate() {
        let new_line = if i == 14 {
            // AK line
            make_output_line(
                &window,
                &line,
                args.ak_output_pattern.as_deref(),
                args.ak_output_limit,
                args.make_output_dirs,
            )
            .change_context_lazy(|| CliError::InFile(ggg_file.to_path_buf()))?
        } else if i == 15 {
            // Spectral fit line
            make_output_line(
                &window,
                &line,
                args.spt_output_pattern.as_deref(),
                args.spt_output_limit,
                args.make_output_dirs,
            )
            .change_context_lazy(|| CliError::InFile(ggg_file.to_path_buf()))?
        } else {
            line
        };

        writeln!(&mut out, "{new_line}")
            .change_context_lazy(|| CliError::IoError)
            .attach_printable_lazy(|| {
                format!(
                    "Failed while writing line {} of {}",
                    i + 1,
                    ggg_file.display()
                )
            })?;
    }

    Ok(())
}

fn make_output_line(
    window: &str,
    orig_line: &str,
    output_pattern: Option<&str>,
    max_num_file: Option<u64>,
    mkdir: bool,
) -> error_stack::Result<String, CliError> {
    let mut orig_parts = orig_line.split_ascii_whitespace();
    let orig_path = orig_parts.next().ok_or_else(|| {
        CliError::FileFormatError(
            "expected the original AK/SPT line to have an output path in it".to_string(),
        )
    })?;
    let orig_limit = orig_parts.next().unwrap_or_default();

    let mut new_line = if let Some(pattern) = output_pattern {
        pattern.replace("{WINDOW}", window)
    } else {
        orig_path.to_string()
    };

    if mkdir {
        // We need this check because GGG allows you to input a string like "./spt/co2_6220/z" to mean
        // "write the SPT files to ./spt/co2_6220 with a prefix of z for each file."  But because GGG
        // doesn't automatically insert a trailing /, we know that if the path ends in a /, then there
        // is no file prefix. If not, we need to remove the file prefix from the path to avoid creating
        // a directory named e.g. "z".
        let dir = if new_line.trim().ends_with(std::path::MAIN_SEPARATOR_STR) {
            PathBuf::from(&new_line)
        } else {
            PathBuf::from(&new_line).parent()
                .ok_or_else(|| CliError::UserError("Could not determine SPT/AK output directory - do not pass an empty string as the directory".to_string()))?
                .to_path_buf()
        };
        if !dir.exists() {
            std::fs::create_dir_all(&dir).change_context_lazy(|| CliError::IoError)?;
        }
    }

    if let Some(n) = max_num_file {
        let s = format!(" {n}");
        new_line.push_str(&s);
    } else if !orig_limit.is_empty() {
        new_line.push(' ');
        new_line.push_str(orig_limit);
    }

    Ok(new_line)
}
