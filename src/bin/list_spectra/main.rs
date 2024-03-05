use std::{path::{PathBuf, Path}, fmt::Display};

use clap::Parser;

fn main() {
    let args = Cli::parse();
    let mut spectra_names = get_spectrum_names(&args.spectra)
        .expect("Was not able to extract the base names of all given spectra");

    spectra_names.sort_unstable();
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
    spectra: Vec<PathBuf>
}

#[derive(Debug, thiserror::Error)]
enum NameError<'p> {
    #[error("Spectrum {} has no base name", .0.display())]
    NoBaseName(&'p Path),
    #[error("Spectrum {} has a base name with invalid unicode", .0.display())]
    NonUnicodeName(&'p Path),
    #[error("Spectrum {0} has a name that is too short")]
    TooShort(&'p str)
}

fn get_spectrum_names(paths: &[PathBuf]) -> Result<Vec<SortingSpec>, NameError> {
    let mut names = vec![];

    for path in paths {
        let this_name = path.file_name()
            .ok_or_else(|| NameError::NoBaseName(&path))?
            .to_str()
            .ok_or_else(|| NameError::NonUnicodeName(&path))?;
        names.push(SortingSpec::new(this_name)?);
    }

    Ok(names)
}

#[derive(Debug, PartialEq, Eq)]
struct SortingSpec<'s> {
    head: &'s str,
    detector: char,
    tail: &'s str,
}

impl<'s> SortingSpec<'s> {
    fn new(spectrum_name: &'s str) -> Result<Self, NameError> {
        let (i, detector) = spectrum_name.char_indices().nth(15)
            .ok_or_else(|| NameError::TooShort(spectrum_name))?;
        // The detector should be an ASCII character, so we assume it is one byte in the string
        let head = &spectrum_name[..i];
        let tail = &spectrum_name[i+1..];

        Ok(Self { head, detector, tail })
    }
}

impl<'s> Display for SortingSpec<'s> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}{}", self.head, self.detector, self.tail)
    }
}

impl<'s> PartialOrd for SortingSpec<'s> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.head.partial_cmp(&other.head) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }

        match self.tail.partial_cmp(&other.tail) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        
        self.detector.partial_cmp(&other.detector)
    }
}

impl<'s> Ord for SortingSpec<'s> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Because we're most often dealing with spectra from the same site, which
        // will have the same head, we can do a small optimization by comparing the
        // tail first (which will be the run number). 

        match self.tail.cmp(&other.tail) {
            core::cmp::Ordering::Equal => {},
            ord => return ord
        }

        match self.head.cmp(&other.head) {
            core::cmp::Ordering::Equal => {},
            ord => return ord
        }

        self.detector.cmp(&other.detector)
    }
}