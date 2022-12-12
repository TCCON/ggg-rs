//! General GGG utilities, not particular to any program or I/O step.
use std::env;
use std::error::Error;
use std::fmt::Display;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::ops::{Deref, DerefMut};
use std::path::{PathBuf, Path};
use std::str::FromStr;


/// Standard error type for all GGG functions
#[derive(Debug)]
pub enum GggError {
    /// A problem occurred getting the GGGPATH environmental variable. See [`GggPathErrorKind`]
    GggPathError(GggPathErrorKind),
    /// Used when the string representation of an apodization function is not recognized.
    /// The contained value must be the unrecognized string.
    UnknownApodization(String),
    /// Used when a file could not be opened, e.g. it does not exist or there was a file system error.
    /// For the inner fields, `descr` must be a short description of the file type, `path` the path to the file
    /// attempted to open, and `reason` the root cause of being unable to open the file.
    CouldNotOpen{descr: String, path: PathBuf, reason: String},
    /// Used when a problem occurred while reading from a file. This means that the file exists and could
    /// be opened, but the filesystem gave some error while trying to read the contents. `path` must be the path 
    /// to the problematic file and `cause` a description of the problem (often the string representation of another
    /// error type).
    CouldNotRead{path: PathBuf, reason: String},
    /// Used for problems with the header format in file, meaning it could be read in, but not interpreted
    /// properly *or* there is some inconsistency (e.g. different number of columns given in the first line of the
    /// file from the number of columns actually in the file). `path` must be the path to the problematic file and
    /// `cause` a desciption of the problem.
    HeaderError{path: PathBuf, cause: String},
    /// Used for problems with the format of the data in a file, usually meaning that it could not be converted
    /// to the proper type. `path` must be the path to the problematic file and `cause` a description of the problem.
    DataError{path: PathBuf, cause: String}
}

impl Display for GggError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GggPathError(inner) => {
                write!(f, "Error getting GGGPATH: {inner}")
            },
            Self::UnknownApodization(a) => {
                write!(f, "Unknown apodization type: '{a}'")
            },
            Self::CouldNotOpen { descr, path, reason} => {
                write!(f, "Could not open {descr} at {} because: {reason}", path.display())
            },
            Self::CouldNotRead {path, reason} => {
                write!(f, "Could not read from {} because: {reason}", path.display())
            },
            Self::HeaderError { path, cause } => {
                write!(f, "Error in header format of {}: {cause}", path.display())
            },
            Self::DataError { path, cause } => {
                write!(f, "Error in data format of {}: {cause}", path.display())
            },
        }
    }
}

impl Error for GggError {}

/// An interior error type for the `GggPathError` variant of [`GggError`]
#[derive(Debug)]
pub enum GggPathErrorKind {
    /// Indicates that no GGGPATH environmental variable was set in the current environment.
    NotSet,
    /// Indicates that the path taken from the environment points to a directory that 
    /// doesn't exist at all. The contained [`PathBuf`] will be the path it expected.
    DoesNotExist(PathBuf),
    /// Indicated that the path taken from the environment points to *something* but that
    /// something is not a directory. The contained [`PathBuf`] will be the path it checked.
    IsNotDir(PathBuf),
}

impl Display for GggPathErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotSet => {
                write!(f, "GGGPATH/gggpath environmental variables not set")
            },
            Self::DoesNotExist(p) => {
                write!(f, "Current GGGPATH ({}) does not exist", p.display())
            },
            Self::IsNotDir(p) => {
                write!(f, "Current GGGPATH ({}) is not a directory", p.display())
            },
        }
    }
}

/// The various apodization functions allowed by GGG
/// 
/// [`FromStr`] and [`ToString`] are implemented to convert
/// this into and from the following strings:
/// 
/// * "BX" => `BoxCar`
/// * "N1" => `WeakNortonBeer
/// * "N2" => `MediumNortonBeer`
/// * "N3" => `StrongNortonBeer`
/// * "TR" => `Triangular`
/// 
/// For [`FromStr`], the conversion ignores case.
#[derive(Debug)]
pub enum ApodizationFxn {
    BoxCar,
    WeakNortonBeer,
    MediumNortonBeer,
    StrongNortonBeer,
    Triangular
}

impl FromStr for ApodizationFxn {
    type Err = GggError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "bx" => Ok(Self::BoxCar),
            "n1" => Ok(Self::WeakNortonBeer),
            "n2" => Ok(Self::MediumNortonBeer),
            "n3" => Ok(Self::StrongNortonBeer),
            "tr" => Ok(Self::Triangular),
            _ => Err(GggError::UnknownApodization(s.to_owned()))
        }
    }
}

impl Display for ApodizationFxn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ApodizationFxn::BoxCar => "BX",
            ApodizationFxn::WeakNortonBeer => "N1",
            ApodizationFxn::MediumNortonBeer => "N2",
            ApodizationFxn::StrongNortonBeer => "N3",
            ApodizationFxn::Triangular => "TR",
        };
        write!(f, "{s}")
    }
}

/// Get the GGG path as defined in the environment
/// 
/// This will check for the environmental variables "GGGPATH" and "gggpath"
/// in that order, the first one found is used.
/// 
/// # Returns
/// A [`Result`] containing the GGG path as a [`PathBuf`]. It returns an `Err`
/// if:
/// 
/// * no GGGPATH variable is set in the current environment, or
/// * the GGGPATH points to a nonexistant directory, or
/// * the GGGPATH points to a file and not a directory.
/// 
/// Which error occurred is communicated by the inner [`GggPathErrorKind`] enum.
pub fn get_ggg_path() -> Result<PathBuf, GggError> {
    let env_path = env::var_os("GGGPATH")
        .or_else(|| env::var_os("gggpath"))
        .ok_or_else(|| GggError::GggPathError(GggPathErrorKind::NotSet))
        .and_then(|p| Ok(PathBuf::from(p)))?;

    if !env_path.exists() {
        return Err(GggError::GggPathError(GggPathErrorKind::DoesNotExist(env_path)));
    }

    if !env_path.is_dir() {
        return Err(GggError::GggPathError(GggPathErrorKind::IsNotDir(env_path)));
    }

    Ok(env_path)
}

/// A wrapper around another struct implementing the [`BufRead`] trait that provides some convenience methods.
/// 
/// Generally, you should prefer this struct over a plain [`BufReader`] throughout GGG Rust code. It has
/// two helper methods (`read_header_line` and `read_data_line`) to help with reading GGG files more ergonomically.
/// It also stores the path of the file opened so that you can reference it more readily in error messages. 
/// 
/// This struct also implements dereferencing to the contained [`BufRead`] object. This means that you can call
/// any [`BufRead`] methods, such as `read_line` directly on this struct if desired.
pub struct FileBuf<'p, F: BufRead> {
    reader: F,
    pub path: &'p Path
}

impl<'p> FileBuf<'p, BufReader<File>> {
    /// Open a file in buffered mode.
    /// 
    /// This will create a `FileBuf` that uses a [`BufReader<File>`] internally.
    /// 
    /// # Returns
    /// A [`Result`] with the `FileBuf` instance. An error is returned if the file could
    /// not be opened by `std::fs::File::open`. The error from that method will be displayed
    /// as the `cause` string in the returned [`GggError::CouldNotOpen`].
    pub fn open(file: &'p Path) -> Result<Self, GggError> {
        let f = File::open(file)
            .or_else(|e|  Err(GggError::CouldNotOpen { descr: "file".to_owned(), path: file.to_owned(), reason: e.to_string() }))?;
        let r = BufReader::new(f);
        Ok(Self { reader: r, path: file })
    }
}

impl <'p, F: BufRead> FileBuf<'p, F> {
    /// Read and return one line from the header of a GGG file.
    /// 
    /// # Returns
    /// A [`Result`] with the line as an owned [`String`]. If an error occured, the error
    /// message will indicate that it occurred while reading a header line.
    pub fn read_header_line(&mut self) -> Result<String, GggError> {
        let mut buf = String::new();
        self.read_line(&mut buf)
            .or_else(|e| Err(GggError::CouldNotRead { path: self.path.to_owned(), reason: format!("{e} (while reading the header)") }))?;
        Ok(buf)
    }

    /// Read and return one line from the data block of a GGG file.
    /// 
    /// # Returns
    /// A [`Result`] with the line as an owned [`String`]. The only difference between this
    /// method and `read_header_line` is that the error message in this function indicates that
    /// the error occurred while reading part of the data.
    pub fn read_data_line(&mut self) -> Result<String, GggError> {
        let mut buf = String::new();
        self.read_line(&mut buf)
            .or_else(|e| Err(GggError::CouldNotRead { path: self.path.to_owned(), reason: format!("{e} (while reading the data)") }))?;
        Ok(buf)
    }
}

impl<'p, F: BufRead> Deref for FileBuf<'p, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

impl <'p, F: BufRead> DerefMut for FileBuf<'p, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.reader
    }
}

/// A structure representing some common elements contained in GGG file headers
/// 
/// This is meant for files that have at least two numbers in the first line of the file,
/// representing the number of header lines and number of data columns, respectively, and
/// has the column names as the last line in the header.
pub struct CommonHeader {
    /// The number of header lines in the file, according to the first line
    pub nhead: usize,
    /// The number of data columns in the file, according to the first line
    pub ncol: usize,
    /// The value used to indicate missing/invalid values in the data. Will be
    /// `None` if not found in the header.
    pub missing: Option<f64>,
    /// The Fortran format string that describes the format of each line of data in the file.
    /// Will be `None` if not found in the header.
    pub format_str: Option<String>,
    /// The data column names
    pub column_names: Vec<String>
}

/// Read the "shape" information from the first line of a GGG file
/// 
/// Almost all GGG files follow the convention that the first line contains a sequence
/// of numbers that describe the file. In many cases, the first number is the number
/// of header lines, and the second is the number of data columns. Some files however,
/// will have additional numbers or may use these first two numbers for other things.
/// This function is agnostic over how many numbers are in the first line, and will just
/// return them all.
/// 
/// # Parameters
/// 
/// * `f` - the just-opened `FileBuf` instance pointing to the file to query, the next `read_line` call on it must
///   return the line with the numbers to parse. After this function returns, the reader will be pointing to the
///   second line of the file.
/// * `min_numbers` - the minimum count of numbers expected from the first line of the file. This function will ensure
///   that the returned vector has at least that many, so you can safely index it up to `min_numbers - 1`.
/// 
/// # Returns
/// A [`Result`] containing a vector of the numbers parsed from the first line of `f`. An `Err` will be returned in a
/// number of cases.
/// 
/// * The file could not be read (error variant = [`GggError::CouldNotRead`])
/// * The first line could not be parsed entirely as space-separated numbers (error variant = [`GggError::HeaderError`])
/// * The count of numbers parsed is fewer than `min_numbers` (error variant = [`GggError::HeaderError`])
/// 
/// # See also
/// * [`get_nhead_ncol`] - shortcut to get the first two numbers (number of header lines and number of data columns)
/// * [`get_nhead`] - shortcut to get the first numbers (number of header lines)
pub fn get_file_shape_info<'p, F: BufRead>(f: &mut FileBuf<'p, F>, min_numbers: usize) -> Result<Vec<usize>, GggError> {
    let mut buf = String::new();
    f.read_line(&mut buf)
        .or_else(|e| Err(GggError::CouldNotRead { path: f.path.to_owned(), reason: e.to_string() }))?;

    let mut numbers = vec![];
    for (i, s) in buf.trim().split_whitespace().enumerate() {
        numbers.push(
            s.parse::<usize>().or_else(|_| Err(GggError::HeaderError { path: f.path.to_owned(), cause: format!("Could not parse number at position {}: {s}", i+1) }))?
        )
    }

    if numbers.len() < min_numbers {
        return Err(GggError::HeaderError { path: f.path.to_owned(), cause: format!("Expected at least {min_numbers} numbers, found {}", numbers.len()) })
    }

    Ok(numbers)
}

/// Return the number of header lines and number of data columns in a GGG file.
/// 
/// # Parameters
/// 
/// * `f` - the just-opened `FileBuf` instance pointing to the file to query, the next `read_line` call on it must
///   return the line with the numbers to parse. After this function returns, the reader will be pointing to the
///   second line of the file.
/// 
/// # Returns
/// A [`Result`] containing the number of header lines and number of data columns as a two-elemet tuple.
/// An `Err` will be returned in a number of cases.
/// 
/// * The file could not be read (error variant = [`GggError::CouldNotRead`])
/// * The first line could not be parsed entirely as space-separated numbers (error variant = [`GggError::HeaderError`])
/// * The first line did not contain at least two numbers (error variant = [`GggError::HeaderError`])
/// 
/// # See also
/// * [`get_file_shape_info`] - get an arbitrary count of numbers parsed from the first line of a file
/// * [`get_nhead`] - shortcut to get the first numbers (number of header lines)
pub fn get_nhead_ncol<'p, F: BufRead>(f: &mut FileBuf<'p, F>) -> Result<(usize, usize), GggError> {
    let nums = get_file_shape_info(f, 2)?;
    // Because get_file_shape_info checks the length of nums, we know there's at least two values
    Ok((nums[0], nums[1]))
}

/// Return the number of header lines in a GGG file.
/// 
/// # Parameters
/// 
/// * `f` - the just-opened `FileBuf` instance pointing to the file to query, the next `read_line` call on it must
///   return the line with the numbers to parse. After this function returns, the reader will be pointing to the
///   second line of the file.
/// 
/// # Returns
/// A [`Result`] containing the number of header lines. An `Err` will be returned in a number of cases.
/// 
/// * The file could not be read (error variant = [`GggError::CouldNotRead`])
/// * The first line could not be parsed entirely as space-separated numbers (error variant = [`GggError::HeaderError`])
/// * The first line did not contain at least one number (error variant = [`GggError::HeaderError`])
/// 
/// # See also
/// * [`get_file_shape_info`] - get an arbitrary count of numbers parsed from the first line of a file
/// * [`get_nhead_ncol`] - shortcut to get the first two numbers (number of header lines and number of data columns)
pub fn get_nhead<'p, F: BufRead>(f: &mut FileBuf<'p, F>) -> Result<usize, GggError> {
    let nums = get_file_shape_info(f, 1)?;
    // Because get_file_shape_info checks the length of nums, we know there's at least one value
    Ok(nums[0])
}


/// Read the common elements found in GGG file header blocks.
/// 
/// See the documentation for [`CommonHeader`] for which elements of GGG file headers can be read in.
/// 
/// # Parameters
/// 
/// * `f` - the just-opened `FileBuf` instance pointing to the file to query, the next `read_line` call on it must
///   return the line with the numbers to parse. After this function returns, the reader will be pointing to the
///   first non-header line of the file (i.e. will be ready to return data.)
/// 
/// # Returns
/// A [`Result`] containing the [`CommonHeader`] with what information was found in the file header. An error can 
/// be returned for a number of reasons:
/// 
/// * The number of header lines and data columns could not be parsed from the first line of the file (error variant
/// = [`GggError::CouldNotRead`] or [`GggError::HeaderError`])
/// * Any line of the header could not be read (error variant = [`GggError::CouldNotRead`])
/// * A missing value line was identified, but the value after the colon could not be interpreted as a float
/// (error variant = [`GggError::HeaderError`])
/// * The number of column names does not match the number of columns listed in the first line (error variant = 
/// [`GggError::HeaderError`])
pub fn read_common_header<'p, F: BufRead>(f: &mut FileBuf<'p, F>) -> Result<CommonHeader, GggError> {
    let (mut nhead, ncol) = get_nhead_ncol(f)?;
    // We've already read one header line
    nhead -= 1;
    let mut format_str = None;
    let mut missing = None;
    while nhead > 1 {
        let line = f.read_header_line()?;
        if line.starts_with("format=") {
            format_str = Some(line.replace("format=", ""));
        }
        if line.starts_with("missing:") {
            let missing_str = line.replace("missing:", "");
            let missing_str = missing_str.trim();
            missing = Some(
                missing_str.parse::<f64>()
                .or_else(|_| Err(GggError::HeaderError { path: f.path.to_owned(), cause: format!("Expecting a real number following 'missing:', got {missing_str}") }))?
            );
        }
        nhead -= 1;
    }

    // Last line should be the column names
    let line = f.read_header_line()?;
    let column_names: Vec<String> = line.split_whitespace()
        .map(|name| name.trim().to_owned())
        .collect();

    if column_names.len() != ncol {
        let nnames = column_names.len();
        let reason = format!("number of column names ({nnames}) does not equal the number of columns listed in the first line of the header ({ncol})");
        return Err(GggError::HeaderError { path: f.path.to_owned(), cause: reason });
    }

    Ok(CommonHeader { nhead, ncol, missing, format_str, column_names })
}