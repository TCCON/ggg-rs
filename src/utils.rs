//! General GGG utilities, not particular to any program or I/O step.
use std::ffi::OsString;
use std::num::NonZeroU8;
use std::{env, f64};
use std::error::Error;
use std::fmt::Display;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::ops::{Deref, DerefMut};
use std::path::{PathBuf, Path};
use std::str::FromStr;

use chrono::{Datelike, TimeZone};
use fortformat::format_specs::FortFormat;
use serde::{Deserialize, Deserializer, de::Error as DeserError};

use crate::error::DateTimeError;


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
    CouldNotWrite{path: PathBuf, reason: String},
    /// Used for problems with the header format in file, meaning it could be read in, but not interpreted
    /// properly *or* there is some inconsistency (e.g. different number of columns given in the first line of the
    /// file from the number of columns actually in the file). `path` must be the path to the problematic file and
    /// `cause` a desciption of the problem.
    HeaderError{path: PathBuf, cause: String},
    /// Used for problems with the format of the data in a file, usually meaning that it could not be converted
    /// to the proper type. `path` must be the path to the problematic file and `cause` a description of the problem.
    DataError{path: PathBuf, cause: String},
    /// A generic error for an unimplemented case in the code
    NotImplemented(String)
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
            Self::CouldNotWrite {path, reason} => {
                write!(f, "Could not write to {} because: {reason}", path.display())
            },
            Self::HeaderError { path, cause } => {
                write!(f, "Error in header format of {}: {cause}", path.display())
            },
            Self::DataError { path, cause } => {
                write!(f, "Error in data format of {}: {cause}", path.display())
            },
            Self::NotImplemented(case) => {
                write!(f, "Not implemented: {case}")
            }
        }
    }
}

impl Error for GggError {}

impl GggError {
    /// For variants with `path` field, return a new instance with the `path` replaced with a new value. Other variants are returned unchanged.
    /// 
    /// This is useful when an inner error may normally need to refer to a path, but it doesn't actually know the path
    /// of the file that relates to the error. When the outer function that knows the path receives the path, it can
    /// use this to replace it:
    /// 
    /// ```
    /// fn throw() -> Result<(), GggError> {
    ///     Err(GggError::CouldNotRead{path: PathBuf::new(), "demo".to_owned()})
    /// }
    /// 
    /// let path = PathBuf::from_str("~/Documents").unwrap();
    /// throw().or_else(
    ///     |e| Err(e.with_path(path))
    /// ).unwrap_err();
    /// ```
    pub fn with_path(self, new_path: PathBuf) -> Self {
        match self {
            Self::CouldNotOpen { descr, path: _, reason } => {
                Self::CouldNotOpen { descr, path: new_path, reason }
            },
            Self::CouldNotRead { path: _, reason } => {
                Self::CouldNotRead { path: new_path, reason }
            },
            Self::HeaderError { path: _, cause } => {
                Self::HeaderError { path: new_path, cause }
            },
            Self::DataError { path: _, cause } => {
                Self::DataError { path: new_path, cause }
            },
            _ => {
                self
            }
        }
    }
}

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApodizationFxn {
    BoxCar,
    WeakNortonBeer,
    MediumNortonBeer,
    StrongNortonBeer,
    Triangular
}

impl ApodizationFxn {
    pub fn as_int(&self) -> i8 {
        match self {
            ApodizationFxn::BoxCar => 0,
            ApodizationFxn::WeakNortonBeer => 1,
            ApodizationFxn::MediumNortonBeer => 2,
            ApodizationFxn::StrongNortonBeer => 3,
            ApodizationFxn::Triangular => 4,
        }
    }

    pub fn int_map_string() -> &'static str {
        "0 = Boxcar, 1 = Weak Norton-Beer, 2 = Medium Norton-Beer, 3 = Strong Norton-Beer, 4 = Triangular"
    }
}

impl ApodizationFxn {
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Self, D::Error> 
    where D: Deserializer<'de>
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(
            |e| D::Error::custom(e.to_string())
        )
    }
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

    /// Consume the FileBuf, returning the contained reader
    /// 
    /// Useful when you know you do not need the extra functionality of the `FileBuf`
    /// anymore but do want to call a method on the [`BufRead`] reader that requires 
    /// a move, e.g. the `lines` method:
    /// 
    /// ```no_run
    /// let f = FileBuf("./list.txt");
    /// for line in f.into_reader().lines() {
    ///     ...
    /// }
    /// ```
    pub fn into_reader(self) -> F {
        self.reader
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


/// A structure to use in command line interfaces when output may be to a new file or modifying one in place
/// 
/// The intention is you would incorporate this into a [`clap`] Derive-based parsers
/// like so:
/// 
/// ```no_run
/// # use std::path::PathBuf;
/// # use ggg_rs::utils::OutputOptCli;
/// #[derive(Debug, clap::Parser)]
/// struct Cli {
///     input_file: PathBuf,
///     #[clap(flatten)]
///     output: OutputOptCli
/// }
/// ```
/// 
/// This will put the arguments `--in-place` and `-o`/`--output-file` into your CLI.
/// Then, you can use the `setup_output` method to get an [`OptInplaceWriter`] which
/// will help with writing to the correct output file.
#[derive(Debug, clap::Args)]
#[group(required=true)]
pub struct OutputOptCli {
    /// Provide this flag to modify the output/destination file directly.
    /// Mutually exclusive with --output-file, but one of this and --output-file
    /// must be given.
    #[clap(long)]
    in_place: bool,
    /// Provide this argument with the path to write the output to. Mutually
    /// exclusive with --in-place, but one of this and --in-place must be given.
    #[clap(short, long)]
    output_file: Option<PathBuf>,
}

impl OutputOptCli {
    /// Given the user input, return an [`OptInplaceWriter`] to handle modifying a file in place or writing to a new file.
    pub fn setup_output<'a>(&'a self, input_file: &'a Path) -> std::io::Result<OptInplaceWriter> {
        if self.in_place && self.output_file.is_some() {
            panic!("Incorrect use of OptOutputCli - in_place and output_file should never both be given");
        }

        let writer = if let Some(ref out_file) = self.output_file {
            OptInplaceWriter::new_separate(out_file.to_path_buf())?
        } else {
            OptInplaceWriter::new_in_place(input_file.to_path_buf())?
        };

        Ok(writer)
    }
}

/// A structure that helps with optionally writing to a new file or modifying one in place.
/// 
/// This will typically be created from the `setup_output` method of `OutputOptCli`. If needed,
/// you can create it diretly with its `new_in_place` and `new_separate` methods, which create
/// a writer configured to help with modifying a file in place vs. writing a separate file.
/// 
/// The difference is that the "in-place" modification assumes that the file given as the output
/// path exists and is being read from, so it creates a temporary file. When you call `finalize`,
/// it renames the temporary file to its final location. This way, you can read from the original
/// file to copy and modify its contents into the new file, then overwrite the original only if
/// the new file is successfully completed:
/// 
/// ```no_run
/// # use std::path::PathBuf;
/// # use ggg_rs::utils::OptInplaceWriter;
/// use std::io::BufRead;
/// use std::io::Write;
/// 
/// let p = PathBuf::from("./example.txt");
/// let file = std::fs::File::open(&p).unwrap();
/// let mut reader = std::io::BufReader::new(file);
/// let mut writer = OptInplaceWriter::new_in_place(p).unwrap();
/// 
/// write!(&mut writer, "new line").unwrap();
/// let mut line = String::new();
/// reader.read_line(&mut line).unwrap();
/// write!(&mut writer, "{line}").unwrap();
/// // At this point, "example.txt" is unchanged.
/// writer.finalize().unwrap();
/// // Now "example.txt" has "new line" as its first line
/// // and its original first line was moved down one.
/// ```
/// 
/// If not doing an in-place modification, then this writes directly to the output file
/// as if you used `std::fs::File`.
/// 
/// **Note: you *must* call `finalize` for the in-place modification to complete! Otherwise
/// the changes will only be in a hidden file.**
/// 
/// As shown in the above example, this implements `std::io::Write`, so you can use it with
/// the `write!` and `writeln!` macros, as well as other standard methods to write to files.
pub struct OptInplaceWriter {
    in_place: bool,
    out_path: PathBuf,
    final_path: PathBuf,
    file: std::fs::File,
}

impl OptInplaceWriter {
    /// Create a new writer to defer writing to `path` until `finalize()` is called.
    pub fn new_in_place(path: PathBuf) -> std::io::Result<Self> {
        let curr_name = path.file_name()
            .ok_or_else(|| std::io::Error::other(format!("Could not get base name of {}", path.display())))?;

        let tmp_name = if curr_name.to_string_lossy().starts_with(".") {
            let mut n = OsString::new();
            n.push(curr_name);
            n.push(".tmp");
            n
        } else {
            let mut n = OsString::new();
            n.push(".");
            n.push(curr_name);
            n.push(".tmp");
            n
        };

        let out_path = path.with_file_name(tmp_name);
        let file = std::fs::File::create(&out_path)?;
        Ok(Self { in_place: true, out_path, final_path: path, file })
    }

    /// Create a new writer that writes directly to `path`.
    pub fn new_separate(path: PathBuf) -> std::io::Result<Self> {
        let file = std::fs::File::create(&path)?;
        Ok(Self { in_place: false, out_path: path, final_path: PathBuf::new(), file })
    }

    /// Perform any finalization. 
    /// 
    /// Consumes the writer, since after this call, no further data should be written.
    /// For all writers, this flushes any remaining data to disk. For in-place writers,
    /// this moves the temporary file into the final output location.
    pub fn finalize(mut self) -> std::io::Result<()> {
        self.file.flush()?;
        if self.in_place {
            std::fs::rename(self.out_path, self.final_path)
        } else {
            Ok(())
        }
    }

    /// Get a reference to the final output path.
    pub fn output_path(&self) -> &Path {
        &self.out_path
    }
}

impl Write for OptInplaceWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

/// Read the contents of input from a file or stdin.
/// 
/// If `input_path` is just "-", then this reads from stdin. Otherwise, it
/// reads the contents of `input_path`. Note that the stdin read is blocking;
/// if the user provides no input, the program may hang indefinitely.
pub fn read_input_file_or_stdin(input_path: &Path) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    if input_path.to_string_lossy() == "-" {
        let mut stdin = std::io::stdin().lock();
        stdin.read_to_end(&mut buf)?;
    } else {
        let mut file = std::fs::File::open(input_path)?;
        file.read_to_end(&mut buf)?;
    }
    Ok(buf)
}

/// A structure representing some common elements contained in GGG file headers
/// 
/// This is meant for files that have at least two numbers in the first line of the file,
/// representing the number of header lines and number of data columns, respectively, and
/// has the column names as the last line in the header.
#[derive(Debug, Clone)]
pub struct CommonHeader {
    /// The number of header lines in the file, according to the first line
    pub nhead: usize,
    /// The number of data columns in the file, according to the first line
    pub ncol: usize,
    /// The value used to indicate missing/invalid values in the data. Will be
    /// `None` if not found in the header.
    pub missing: Option<f64>,
    /// The Fortran format that describes the format of each line of data in the file.
    /// Will be `None` if not found in the header.
    pub fformat: Option<FortFormat>,
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
    let mut fformat = None;
    let mut missing = None;
    while nhead > 1 {
        let line = f.read_header_line()?;
        if line.starts_with("format=") {
            let format_str = line.replace("format=", "");
            fformat = Some(
                FortFormat::parse(&format_str)
                .map_err(|e| GggError::HeaderError { 
                    path: f.path.to_path_buf(), 
                    cause: format!("Error parsing format line: {e}") 
                })?
            );
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

    Ok(CommonHeader { nhead, ncol, missing, fformat, column_names })
}


/// Find a spectrum in one of the directories listed in the data_part.lst file.
/// 
/// This searches each (uncommented) directory in `$GGGPATH/config/data_part.lst`
/// until it finds a spectrum with file name `specname` or it runs out of directories.
/// 
/// # Returns
/// If the spectrum was found, then an `Ok(Some(p))` is returned, where `p` is the path
/// to the spectrum. If the spectrum was *not* found, `Ok(None)` is returned. An `Err`
/// is returned if:
/// 
/// * `$GGGPATH` is not set,
/// * `$GGGPATH/config/data_part.lst` does not exist, or
/// * a line of `data_part.lst` could not be read.
/// 
/// The final condition returns an `Err` rather than silently skipping the unreadable line
/// to avoid accidentally reading a spectrum from a later directory than it should if the
/// `data_part.lst` file was formatted correctly.
/// 
/// # Difference to Fortran
/// Unlike the Fortran subroutine that performs this task, this function does not require
/// that the paths in `data_part.lst` end in a path separator.
/// 
/// # See also
/// * [`find_spectrum_result`] - a version of this function that returns a `Result<PathBuf>`
///   instead of `Result<Option<PathBuf>>`, making a missing spectrum an error.
pub fn find_spectrum(specname: &str) -> Result<Option<PathBuf>, GggError> {
    let gggpath = get_ggg_path()?;
    let data_partition_file = gggpath.join("config/data_part.lst");
    if !data_partition_file.exists() {
        return Err(GggError::CouldNotOpen { descr: "data_part.lst".to_owned(), path: data_partition_file, reason: "does not exist".to_owned() });
    }

    let data_part = FileBuf::open(&data_partition_file)?;
    for (iline, line) in data_part.into_reader().lines().enumerate() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                return Err(GggError::CouldNotRead { 
                    path: data_partition_file, 
                    reason: format!("Error reading line {} was: {}", iline+1, e)
                });
            }
        };

        // GGG convention is that lines beginning with ":" are comments
        if line.starts_with(":") { continue; }

        // Apparently from_str cannot error, so okay to unwrap.
        let search_path = PathBuf::from_str(&line.trim()).unwrap();
        let spec_path = search_path.join(specname);
        if spec_path.exists() {
            return Ok(Some(spec_path));
        }
    }

    return Ok(None);
}

/// Find a spectrum in one of the directories listed in the data_part.lst file.
/// 
/// This searches each (uncommented) directory in `$GGGPATH/config/data_part.lst`
/// until it finds a spectrum with file name `specname` or it runs out of directories.
/// 
/// # Returns
/// If the spectrum was found, then an `Ok(p)` is returned, where `p` is the path
/// to the spectrum. An `Err` is returned if the spectrum is not found or if any of
/// the error conditions in [`find_spectrum`] occur.
/// 
/// # See also
/// * [`find_spectrum`] - a similar function that returns an `Ok(None)` if a spectrum
///   could not be found, rather than an error.
pub fn find_spectrum_result(spectrum_name: &str) -> Result<PathBuf, GggError> {
    if let Some(f) = find_spectrum(spectrum_name)? {
        Ok(f)
    }else{
        Err(GggError::CouldNotOpen { 
            descr: "spectrum".to_owned(), 
            path: PathBuf::from_str(spectrum_name).unwrap(), 
            reason: "spectrum not found".to_owned()
        })
    }
}

/// Convert a runlog year, day, and hour value to a proper UTC datetime
/// 
/// GGG runlogs store the ZPD time of a spectrum as a year, day of year, and UTC hour value where the
/// day of year accounts for leap years (i.e. Mar 1 is DOY 60 on non-leap years and DOY 61 on leap years)
/// and the UTC hour has a decimal component that provides the minutes and seconds. This function converts
/// those values into a [`chrono::DateTime`] with the UTC timezone.
pub fn runlog_ydh_to_datetime(year: i32, day_of_year: i32, utc_hour: f64) -> chrono::DateTime<chrono::Utc> {
    let ihours = utc_hour.floor();
    let iminutes = ((utc_hour - ihours) * 60.0).floor();
    let iseconds = (((utc_hour - ihours) * 60.0 - iminutes) * 60.0).floor();

    chrono::Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0).unwrap()
    + chrono::Duration::days((day_of_year - 1).into())
    + chrono::Duration::hours(ihours as i64)
    + chrono::Duration::minutes(iminutes as i64)
    + chrono::Duration::seconds(iseconds as i64)
}


#[derive(Debug, thiserror::Error)]
pub enum EncodingError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Could not convert file contents: {0}")]
    ConversionError(String),
}


pub fn read_unknown_encoding_file<P: AsRef<Path>>(filepath: P) -> Result<String, EncodingError> {
    let mut f = std::fs::File::open(filepath)?;
    let mut buf = vec![];
    f.read_to_end(&mut buf)?;
    let (content_result, _) = encoding::types::decode(
        &buf,
        encoding::types::DecoderTrap::Strict,
        encoding::all::UTF_8
    );

    content_result.map_err(|e| EncodingError::ConversionError(e.into_owned()))
}

/// Remove a comment from a line
/// 
/// GGG often (though not always) considers anything after a
/// colon in a line to be a comment. This function will return
/// everything in `value` up to the first colon. If there is not
/// a colon in `value`, the the full string is returned.
pub fn remove_comment(value: &str) -> &str {
    value.split_once(":")
        .map(|(a, _)| a)
        .unwrap_or(value)
}

pub fn remove_comment_multiple_lines(value: &str) -> String {
    let mut out = String::new();
    for line in value.split("\n") {
        let value = remove_comment(line);
        if !value.trim().is_empty() {
            out.push_str(value);
        }
    }
    out
}


pub fn is_usa_dst(datetime: chrono::NaiveDateTime) -> Result<bool, DateTimeError> {
    // Based on the rules listed on Wikipedia as of 2023-01-23 (https://en.wikipedia.org/wiki/Daylight_saving_time_in_the_United_States#1975%E2%80%931986:_Extension_of_daylight_saving_time),
    // 1987 to 2006 use daylight savings time between the first Sunday of April to the last Sunday of October
    // Starting in 2007, it became second Sunday of March to first Sunday of November
    if datetime.year() < 1987 {
        unimplemented!("USA daylight savings time before 1987 not implemented");
    }

    let (start, end) = if datetime.year() < 2007 {
        let start = nth_day_of_week(datetime.year(), 4, chrono::Weekday::Sun, 1.into())
            .expect("Should be able to find the first Sunday in April");
        let end = nth_day_of_week(datetime.year(), 10, chrono::Weekday::Sun, Nth::Last)
            .expect("Should be able to find the last Sunday in October");
        (start, end)
    } else {
        let start = nth_day_of_week(datetime.year(), 3, chrono::Weekday::Sun, 2.into())
            .expect("Should be able to get the second Sunday of March");
        let end = nth_day_of_week(datetime.year(), 11, chrono::Weekday::Sun, 1.into())
            .expect("Should be able to get the first Sunday in November");
        (start, end)
    };

    let start = start.and_hms_opt(2, 59, 59).unwrap();
    let end_ambiguous = end.and_hms_opt(2, 0, 0).unwrap();
    let end = end.and_hms_opt(1, 0, 0).unwrap();
    // The case we can't tell is if the time is between 1a and 2a on the date when
    // we "fall back", so anything in that time range is an error
    if datetime >= end && datetime <= end_ambiguous {
        return Err(DateTimeError::AmbiguousDst(datetime));
    }

    let is_dst = datetime >= start && datetime <= end;
    Ok(is_dst)
}

enum Nth {
    N(NonZeroU8),
    Last
}

impl From<u8> for Nth {
    fn from(value: u8) -> Self {
        Self::N(NonZeroU8::new(value).unwrap())
    }
}

fn nth_day_of_week(year: i32, month: u32, weekday: chrono::Weekday, n: Nth) -> Result<chrono::NaiveDate, DateTimeError> {
    let mut date = chrono::NaiveDate::from_ymd_opt(year, month, 1)
        .ok_or_else(|| DateTimeError::InvalidYearMonthDay(year, month, 1))?;

    let n: u8 = match n {
        Nth::N(n) => n.into(),
        Nth::Last => 0,
    };

    let mut m: u8 = 0;
    loop {
        if date.weekday() == weekday {
            m += 1;
        }

        if n > 0 && m == n {
            return Ok(date)
        }

        date += chrono::Duration::days(1);
        if date.month() != month && n != 0 {
            return Err(DateTimeError::NoNthWeekday { year, month, n, weekday })
        } else if date.month() != month {
            // back up to the last instance of the requested weekday in the month
            while date.weekday() != weekday {
                date -= chrono::Duration::days(1);
            }
            return Ok(date)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nth_day_of_week() {
        let first_sunday_apr = nth_day_of_week(2023, 4, chrono::Weekday::Sun, 1.into()).unwrap();
        assert_eq!(first_sunday_apr, chrono::NaiveDate::from_ymd_opt(2023, 4, 2).unwrap());

        let last_sunday_oct = nth_day_of_week(2023, 10, chrono::Weekday::Sun, Nth::Last).unwrap();
        assert_eq!(last_sunday_oct, chrono::NaiveDate::from_ymd_opt(2023, 10, 29).unwrap());
    }

    #[test]
    fn test_is_usa_dst() {
        // Post-2007 rules
        let b = is_usa_dst(datetime(2023, 1, 1, 0, 0)).unwrap();
        assert_eq!(b, false);
        let b = is_usa_dst(datetime(2023, 3, 11, 23, 59)).unwrap();
        assert_eq!(b, false);
        let b = is_usa_dst(datetime(2023, 3, 12, 3, 0)).unwrap();
        assert_eq!(b, true);
        let b = is_usa_dst(datetime(2023, 6, 1, 0, 0)).unwrap();
        assert_eq!(b, true);
        let b = is_usa_dst(datetime(2023, 11, 5, 0, 0)).unwrap();
        assert_eq!(b, true);
        let b = is_usa_dst(datetime(2023, 11, 5, 2, 1)).unwrap();
        assert_eq!(b, false);
        let b = is_usa_dst(datetime(2023, 12, 31, 23, 59)).unwrap();
        assert_eq!(b, false);

        let e = is_usa_dst(datetime(2023, 11, 5, 1, 30));
        assert!(e.is_err());

        // 1987 to 2006 rules
        let b = is_usa_dst(datetime(2000, 1, 1, 0, 0)).unwrap();
        assert_eq!(b, false);
        let b = is_usa_dst(datetime(2000, 4, 1, 23, 59)).unwrap();
        assert_eq!(b, false);
        let b = is_usa_dst(datetime(2000, 4, 2, 3, 0)).unwrap();
        assert_eq!(b, true);
        let b = is_usa_dst(datetime(2000, 6, 1, 0, 0)).unwrap();
        assert_eq!(b, true);
        let b = is_usa_dst(datetime(2000, 10, 29, 0, 0)).unwrap();
        assert_eq!(b, true);
        let b = is_usa_dst(datetime(2000, 10, 29, 2, 1)).unwrap();
        assert_eq!(b, false);
        let b = is_usa_dst(datetime(2000, 12, 31, 23, 59)).unwrap();
        assert_eq!(b, false);

        let e = is_usa_dst(datetime(2000, 10, 29, 1, 30));
        assert!(e.is_err());
    }

    fn datetime(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> chrono::NaiveDateTime {
        let d = chrono::NaiveDate::from_ymd_opt(year, month, day).unwrap();
        d.and_hms_opt(hour, minute, 0).unwrap()
    }
}