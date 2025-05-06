use std::{
    fmt::Display,
    path::{Path, PathBuf},
};

#[derive(Debug, thiserror::Error)]
pub enum NameError {
    #[error("Spectrum {} has no base name", .0.display())]
    NoBaseName(PathBuf),
    #[error("Spectrum {} has a base name with invalid unicode", .0.display())]
    NonUnicodeName(PathBuf),
    #[error("Spectrum {0} has a name that is too short")]
    TooShort(String),
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
}

pub fn sort_spectrum_names<P: AsRef<Path>>(paths: &[P]) -> Result<Vec<String>, NameError> {
    let mut spectra_names = get_spectrum_names(paths)?;
    spectra_names.sort_unstable();
    Ok(spectra_names.into_iter().map(|s| s.to_string()).collect())
}

pub fn sort_spectra_in_dirs<P: AsRef<Path>>(dirs: &[P]) -> Result<Vec<String>, NameError> {
    let mut paths = vec![];
    for dir in dirs {
        let contents = std::fs::read_dir(dir.as_ref())?;
        for c in contents {
            let p = c?.path();
            if p.is_file() {
                paths.push(p);
            }
        }
    }
    sort_spectrum_names(&paths)
}

fn get_spectrum_names<P: AsRef<Path>>(paths: &[P]) -> Result<Vec<SortingSpec>, NameError> {
    let mut names = vec![];

    for path in paths {
        let this_name = path
            .as_ref()
            .file_name()
            .ok_or_else(|| NameError::NoBaseName(path.as_ref().to_path_buf()))?
            .to_str()
            .ok_or_else(|| NameError::NonUnicodeName(path.as_ref().to_path_buf()))?;
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
        let (i, detector) = spectrum_name
            .char_indices()
            .nth(15)
            .ok_or_else(|| NameError::TooShort(spectrum_name.to_string()))?;
        // The detector should be an ASCII character, so we assume it is one byte in the string
        let head = &spectrum_name[..i];
        let tail = &spectrum_name[i + 1..];

        Ok(Self {
            head,
            detector,
            tail,
        })
    }
}

impl<'s> Display for SortingSpec<'s> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}{}", self.head, self.detector, self.tail)
    }
}

impl<'s> PartialOrd for SortingSpec<'s> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.head.partial_cmp(other.head) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }

        match self.tail.partial_cmp(other.tail) {
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
            core::cmp::Ordering::Equal => {}
            ord => return ord,
        }

        match self.head.cmp(&other.head) {
            core::cmp::Ordering::Equal => {}
            ord => return ord,
        }

        self.detector.cmp(&other.detector)
    }
}
