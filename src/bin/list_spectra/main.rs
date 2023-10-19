use std::{path::{PathBuf, Path}, collections::HashMap, hash::Hash, fmt::Display};

use clap::Parser;

fn main() {
    let args = Cli::parse();
    let spectra_names = get_spectrum_names(&args.spectra)
        .expect("Was not able to extract the base names of all given spectra");

    let grouped_spectra = SpectraLists::from_names(&spectra_names)
        .expect("Was not able to group spectra by detector");
    grouped_spectra.print_list();
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
    /// The spectra to pr
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

fn get_spectrum_names(paths: &[PathBuf]) -> Result<Vec<&str>, NameError> {
    let mut names = vec![];

    for path in paths {
        let this_name = path.file_name()
            .ok_or_else(|| NameError::NoBaseName(&path))?
            .to_str()
            .ok_or_else(|| NameError::NonUnicodeName(&path))?;
        names.push(this_name);
    }

    Ok(names)
}

struct SpectrumKey<'n>(&'n str);

impl<'n> SpectrumKey<'n> {
    fn new(name: &'n str) -> Result<Self, NameError> {
        if name.len() < 20 {
            Err(NameError::TooShort(name))
        } else {
            Ok(Self(name))
        }
    }

    fn detector(&self) -> char {
        self.0.chars().nth(15).unwrap()
    }

    fn display_with_detector(&self, detector: char) -> SpecKeyDisplay {
        SpecKeyDisplay { key: self, det: detector }
    }
}


impl<'n> Hash for SpectrumKey<'n> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for (i, c) in self.0.chars().enumerate() {
            // Don't include the detector in the hash so that different spectra for the same detector
            // map to the same key
            if i != 15 {
                c.hash(state);
            }
        }
    }
}

impl<'n> PartialEq for SpectrumKey<'n> {
    fn eq(&self, other: &Self) -> bool {
        for (i, (c1, c2)) in self.0.chars().zip(other.0.chars()).enumerate() {
            if i != 15 && c1 != c2 {
                return false;
            }
        }

        true
    }
}

impl<'n> Eq for SpectrumKey<'n> {}

struct SpecKeyDisplay<'k> {
    key: &'k SpectrumKey<'k>,
    det: char
}

impl<'k> Display for SpecKeyDisplay<'k> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Because the SpectrumKey that this comes from checks the length of the spectrum
        // name, we know we're safe to index the first 15 characters.
        write!(f, "{}", &self.key.0[..15])?;
        write!(f, "{}", self.det)?;
        write!(f, "{}", &self.key.0[16..])?;
        Ok(())
    }
}

struct SpectraLists<'k> {
    detectors: HashMap<SpectrumKey<'k>, Vec<char>>,
    primary_spectra_ordered: Vec<&'k str>
}

impl<'k> SpectraLists<'k> {
    fn from_names(names: &[&'k str]) -> Result<Self, NameError<'k>> {
        let mut detectors: HashMap<SpectrumKey<'_>, Vec<char>> = HashMap::new();
        let mut primary = Vec::new();

        for name in names {
            let key = SpectrumKey::new(name)?;
            let det = key.detector();

            detectors.entry(key)
                .and_modify(|v| v.push(det))
                .or_insert(vec![det]);

            if det == 'a' {
                primary.push(*name);
            }
        }

        primary.sort_unstable();
        Ok(Self { detectors, primary_spectra_ordered: primary })
    }

    fn print_list(mut self) {
        for spectrum in self.primary_spectra_ordered {
            let key = SpectrumKey::new(spectrum)
                .expect("Only valid spectrum names should be retained at this point");
            let mut detectors = self.detectors.remove(&key)
                .expect("All spectra listed in the ordered vector should be a key in the detector map");
            detectors.sort_unstable();

            for det in detectors {
                println!("{}", key.display_with_detector(det));
            }
        }

        if !self.detectors.is_empty() {
            eprintln!("WARNING: there were {} scans that had spectra from one or more secondary detectors but not a primary detector", self.detectors.len());
        }
    }
}