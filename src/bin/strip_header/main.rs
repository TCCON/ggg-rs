use std::{io::{BufRead, Seek}, path::PathBuf, process::ExitCode};

use clap::Parser;
use error_stack::ResultExt;
use ggg_rs::utils;

fn main() -> ExitCode {
    let clargs = Cli::parse();
    if let Err(e) = driver(clargs.file, clargs.invert) {
        eprintln!("{e:?}");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

fn driver(file: PathBuf, invert: bool) -> error_stack::Result<(), CliError> {
    let mut fh = utils::FileBuf::open(&file)
        .change_context_lazy(|| CliError::ReadError(file.clone()))?;
    let nhead = utils::get_nhead(&mut fh)
        .change_context_lazy(|| CliError::ReadError(file.clone()))?;

    if invert {
        let mut rdr = fh.into_reader();
        rdr.seek(std::io::SeekFrom::Start(0))
            .change_context_lazy(|| CliError::ReadError(file.clone()))?;
        for _ in 0..nhead {
            let mut buf = String::new();
            rdr.read_line(&mut buf)
                .change_context_lazy(|| CliError::ReadError(file.clone()))?;
            // Unlike the lines() method, this keeps the original newline, so we
            // use print! instead of println!
            print!("{buf}");
        }
    } else {
        for _ in 1..nhead {
            fh.read_header_line()
                .change_context_lazy(|| CliError::ReadError(file.clone()))?;
        }

        for line in fh.into_reader().lines() {
            let line = line.change_context_lazy(|| CliError::ReadError(file.clone()))?;
            println!("{line}");
        }
    }

    Ok(())
}

/// Print the contents of a GGG file without its header, or only the header.
/// Useful as part of a CLI pipeline for concatentating files. The file given
/// must include the number of lines in the header as part of the first line.
#[derive(Debug, Parser)]
struct Cli {
    /// The file to print without its header
    file: PathBuf,

    /// Use this flag to print only the header, instead of everything after
    /// the header.
    #[clap(short, long)]
    invert: bool,
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error("Error opening/reading file: {0}")]
    ReadError(PathBuf)
}
