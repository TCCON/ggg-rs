use std::{
    ffi::OsString,
    fmt::Write as _,
    io::{BufRead, Seek, SeekFrom, Write},
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    process::ExitCode,
};

use clap::{ArgAction, Parser};
use error_stack::ResultExt;
use ggg_rs::utils;

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
    #[clap(long, visible_alias = "spt-pattern")]
    spt_output_pattern: Option<String>,

    /// The maximum number of spectral fit files to allow GGG to write.
    #[clap(long, visible_alias = "spt-limit")]
    spt_output_limit: Option<u64>,

    /// Directory, or directory pattern, to save the averaging kernel files
    /// under. The substring {WINDOW} will be replaced with the window name
    /// for the current .ggg file, e.g. "co2_6220"
    #[clap(long, visible_alias = "ak-pattern")]
    ak_output_pattern: Option<String>,

    /// The maximum number of averaging kernel files to allow GGG to write.
    #[clap(long, visible_alias = "ak-limit")]
    ak_output_limit: Option<u64>,

    /// Add "ak" to the .ggg file(s)' command line to tell GFIT to output
    /// Jacobian files. This is implied if either --ak-output-pattern or
    /// --ak-output-limit are given.
    #[clap(short = 'a', long, action = ArgAction::SetTrue)]
    add_ak_cmd: bool,

    /// Strictly do not add "ak" to the .ggg file(s)' command line so GFIT
    /// will not output Jacobian files. This overrides both a previous
    /// --add-ak-cmd and any implicit intent to do this if --ak-output-pattern
    /// or --ak-output-limit are given.
    #[clap(long, overrides_with = "add_ak_cmd", action = ArgAction::SetTrue)]
    no_add_ak_cmd: bool,

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

    /// Handles the interaction of the various AK-related flags to
    /// determine if we add "ak" to the .ggg file(s) command line.
    fn do_add_ak_cmd(&self) -> bool {
        if self.add_ak_cmd {
            return true;
        }
        if self.no_add_ak_cmd {
            return false;
        }
        if self.ak_output_pattern.is_some() || self.ak_output_limit.is_some() {
            return true;
        }

        false
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
        if self.do_add_ak_cmd() {
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

    let mut f = ggg_rs::utils::FileBuf::open(ggg_file).change_context_lazy(|| CliError::IoError)?;

    let nhead = ggg_rs::utils::get_nhead(&mut f).change_context_lazy(|| CliError::IoError)?;
    f.seek(SeekFrom::Start(0))
        .change_context_lazy(|| CliError::IoError)
        .attach_printable_lazy(|| {
            "Failed to rewrite .ggg file to start after reading number of lines"
        })?;
    let mut out_lines = vec![];

    for (i, line) in f.lines().enumerate() {
        let line = line.change_context_lazy(|| CliError::IoError)?;
        let line_num = i + 1;
        let new_line = if line_num == 15 {
            // AK line
            make_output_line(
                &window,
                &line,
                args.ak_output_pattern.as_deref(),
                args.ak_output_limit,
                args.make_output_dirs,
            )
            .change_context_lazy(|| CliError::InFile(ggg_file.to_path_buf()))?
        } else if line_num == 16 {
            // Spectral fit line
            make_output_line(
                &window,
                &line,
                args.spt_output_pattern.as_deref(),
                args.spt_output_limit,
                args.make_output_dirs,
            )
            .change_context_lazy(|| CliError::InFile(ggg_file.to_path_buf()))?
        } else if line_num == nhead && args.do_add_ak_cmd() {
            add_to_cmd_line(&line, &["ak"])
                .change_context_lazy(|| CliError::InFile(ggg_file.to_path_buf()))?
        } else {
            line
        };

        out_lines.push(new_line);
    }

    let mut out = std::fs::File::create(ggg_file).change_context_lazy(|| CliError::IoError)?;
    for (i, new_line) in out_lines.into_iter().enumerate() {
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

fn add_to_cmd_line(cmd_line: &str, new_cmds: &[&str]) -> error_stack::Result<String, CliError> {
    let delim = if cmd_line.contains("sf=") {
        "sf="
    } else if cmd_line.contains(":") {
        ":"
    } else {
        return Err(CliError::FileFormatError(
            "Expected command line of .ggg file to contain 'sf=' or ':', but neither found"
                .to_string(),
        )
        .into());
    };

    // We know that cmd_line was the delimiter in it, so safe to unwrap.
    let (pre, post) = cmd_line.split_once(delim).unwrap();
    let mut buf = String::new();
    write!(&mut buf, "{}", pre.trim_end()).expect("Should be able to write to an in-memory string");
    for cmd in new_cmds {
        write!(&mut buf, " {cmd}").expect("Should be able to write to an in-memory string");
    }
    write!(&mut buf, " sf={post}").expect("Should be able to write to an in-memory string");
    Ok(buf)
}
