use std::path::PathBuf;

use clap::Parser;

use ggg_rs::tccon::sort_spectra;
use glob::glob;
use itertools::Itertools;

fn main() {
    let args = Cli::parse();
    let spectra = if args.expand_globs {
        expand_glob_patterns(&args.spectra).expect("Could not parse or match glob patterns")
    } else {
        args.spectra
            .into_iter()
            .map(|s| PathBuf::from(s))
            .collect_vec()
    };
    let spectra_names = sort_spectra::sort_spectrum_names(&spectra)
        .expect("Was not able to extract the base names of all given spectra");

    for name in spectra_names {
        println!("{name}");
    }
}

/// Print spectrum names in the correct order to pass to create_sunrun
///
/// This will print just the spectrum names (no leading paths) in alphanumeric
/// order, except that the detector character is considered last. The result
/// is a list of spectra where all detectors for scan .0001 are printed first,
/// then .0002, and so on.
///
/// Example:
///
/// ```bash
/// list_spectra /data/spectra/pa200501* > pa200501.gnd
/// ```
///
/// The spectra list is printed to stdout, so the example uses a redirect to write
/// it to a list file.
///
/// Note that this relies on your shell expanding any glob pattern used, such that
/// each spectrum is one command line argument. If you need to order a large number of
/// spectra, you may run into the limit for the maximum number of command line arguments
/// allowed on your system. In that case, make smaller individual lists and concatenate
/// them.
#[derive(Debug, Parser)]
struct Cli {
    /// The spectra to print in order. May be full paths to spectra, only the names
    /// will be printed.
    spectra: Vec<String>,

    /// If glob patterns (.e.g, *, ?) are given as arguments, expand them before
    /// trying to list the files. If not given, then it is assumed the shell will
    /// have expanded such wildcards. If you need to avoid the shell expanding the
    /// glob patterns (because you are reaching the limit of the number of command
    /// line arguments allowed), you will need to quote any glob patterns.
    #[clap(short = 'g', long)]
    expand_globs: bool,
}

fn expand_glob_patterns(patterns: &[String]) -> Result<Vec<PathBuf>, String> {
    let mut spectra = vec![];

    for pat in patterns {
        let matches = glob(pat).map_err(|e| e.to_string())?;
        for p in matches {
            match p {
                Ok(p) => spectra.push(p),
                Err(e) => return Err(e.to_string()),
            }
        }
    }

    Ok(spectra)
}
