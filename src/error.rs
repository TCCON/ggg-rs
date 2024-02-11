//! Common errors across the ggg-rs crate
use std::{path::{PathBuf, Path}, fmt::Display, error::Error};

#[derive(Debug, Default, Clone)]
pub struct FileLocation {
    path: Option<PathBuf>,
    line_num: Option<usize>,
    line_value: Option<String>,
}

impl FileLocation {
    pub fn new<P: AsRef<Path>, S: AsRef<str>>(path: Option<P>, line_num: Option<usize>, line_value: Option<S>) -> Self {
        Self { 
            path: path.map(|p| p.as_ref().to_owned()), 
            line_num,
            line_value: line_value.map(|s| s.as_ref().to_owned()) 
        }
    }
}


impl From<&Path> for FileLocation {
    fn from(value: &Path) -> Self {
        Self::new::<_, &'static str>(Some(value), None, None)
    }
}

impl From<PathBuf> for FileLocation {
    fn from(value: PathBuf) -> Self {
        Self::new::<_, &'static str>(Some(value), None, None)
    }
}

impl From<&str> for FileLocation {
    fn from(value: &str) -> Self {
        Self::new::<PathBuf, _>(None, None, Some(value))
    }
}

impl From<String> for FileLocation {
    fn from(value: String) -> Self {
        Self::new::<PathBuf, _>(None, None, Some(value))
    }
}

impl Display for FileLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(p) = self.path.as_deref() {
            write!(f, "in file '{}'", p.display())?;
        }

        if let Some(num) = self.line_num {
            write!(f, "at line {num}")?;
        }

        if let Some(value) = self.line_value.as_deref() {
            if self.path.is_some() || self.line_num.is_some() {
                write!(f, "(line = '{value}')")?;
            } else {
                write!(f, "in line '{value}'")?;
            }
        }

        Ok(())
    }
}


#[derive(Debug, Clone)]
pub enum HeaderError {
    ParseError{location: FileLocation, cause: String},
    NumLinesMismatch{expected: usize, got: usize},
    NumColMismatch{location: FileLocation, expected: usize, got: usize},
    CouldNotRead{location: FileLocation, cause: String},
}

impl Display for HeaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ParseError { location, cause } => {
                write!(f, "Could not parse header line {location}: {cause}")
            },
            Self::NumLinesMismatch { expected, got } => {
                write!(f, "Expected {expected} header lines, nhead indicates {got}")
            },
            Self::NumColMismatch { location, expected, got } => {
                write!(f, "Number of data columns ({got}) does not match that defined in the first line ({expected}) {location}")
            },
            Self::CouldNotRead{location, cause} => {
                write!(f, "Could not read {location}: {cause}")
            }
        }
    }
}

impl Error for HeaderError {}

/// Errors related to working with datetimes
#[derive(Debug, thiserror::Error)]
pub enum DateTimeError {
    #[error("Year {0}, month {1}, day {2} is not a valid date")]
    InvalidYearMonthDay(i32, u32, u32),
    #[error("Year {year} month {month} does not have {n} {weekday}s")]
    NoNthWeekday{year: i32, month: u32, n: u8, weekday: chrono::Weekday},
    #[error("{0} falls in the repeated hour of the DST -> standard transition, cannot determine the timezone")]
    AmbiguousDst(chrono::NaiveDateTime),
    #[error("Error adding timezone to naive datetime: {0}")]
    InvalidTimezone(String),
}