use std::collections::HashMap;
use std::fmt::Display;
use std::io::{BufReader, Write};
use std::path::Path;
use std::{path::PathBuf, str::FromStr, sync::OnceLock, io::BufRead};

use error_stack::ResultExt;
use fortformat::FortFormat;
use itertools::Itertools;

use crate::error::{BodyError, HeaderError, WriteError};
use crate::runlogs::RunlogDataRec;
use crate::utils::{self, get_file_shape_info, get_nhead_ncol, FileBuf, GggError};

pub const POSTPROC_FILL_VALUE: f64 = 9.8765e35;
static PROGRAM_VERSION_REGEX: OnceLock<regex::Regex> = OnceLock::new();
static INPUT_MD5_REGEX: OnceLock<regex::Regex> = OnceLock::new();

pub struct ColInputData {
    pub path: PathBuf,
    pub md5sum: String,
}

impl ColInputData {
    pub fn from_str_opt(s: &str) -> Result<Option<Self>, HeaderError> {
        let me = Self::from_str(s)?;
        if me.path.to_string_lossy() == "-" {
            Ok(None)
        } else {
            Ok(Some(me))
        }
    }
}

impl FromStr for ColInputData {
    type Err = HeaderError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let re = INPUT_MD5_REGEX.get_or_init(|| {
            regex::Regex::new("(?<md5>[0-9a-fA-F]{32})  (?<path>.+)").unwrap()
        });

        let s = s.trim();
        let caps = re.captures(s)
            .ok_or_else(|| HeaderError::ParseError { 
                location: s.into(), 
                cause: "Did not match expected format of 32 hex digit checksum, two spaces, then a path".to_string()
            })?;

        let md5sum = caps["md5"].to_string();
        let path = PathBuf::from(&caps["path"]);

        Ok(Self { path, md5sum })
    }
}

/// A struct representing the components of a complete GGG program version string
/// as found in the post processing file headers.
#[derive(Debug, PartialEq, Eq)]
pub struct ProgramVersion {
    /// The program name, e.g. "GFIT" or "collate_results"
    pub program: String,
    /// The program version, usually including the word "Version", e.g. "Version 1.0"
    pub version: String,
    /// The date this version was finalized in YYYY-MM-DD format
    pub date: String,
    /// The initials of individuals who contributed to this program, e.g. "GCT" or "GCT,JLL"
    pub authors: String,
}

impl Display for ProgramVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:24} {:15} {:10} {}", self.program, self.version, self.date, self.authors)
    }
}

impl FromStr for ProgramVersion {
    type Err = HeaderError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let re = PROGRAM_VERSION_REGEX.get_or_init(|| 
            regex::Regex::new(r"(?<program>\w+)\s+(?<version>[Vv][Ee][Rr][Ss][Ii][Oo][Nn]\s+[\d\.]+)\s+(?<date>[\d\-]+)\s+(?<authors>[\w\,]+)")
                .expect("Could not compile program version regex")
        );

        let s = s.trim();

        let caps = re.captures(s)
            .ok_or_else(|| HeaderError::ParseError { 
                location: s.into(), 
                cause: "Did not match expected format of program name, version, date, and authors".to_string()
            })?;

        Ok(Self { 
            program: caps["program"].to_string(),
            version: caps["version"].to_owned(), 
            date: caps["date"].to_string(),
            authors: caps["authors"].to_string()
        })
    }
}

pub struct ColFileHeader {
    pub nhead: usize,
    pub ncol: usize,
    pub gfit_version: ProgramVersion,
    pub gsetup_version: ProgramVersion,
    pub data_partition_file: ColInputData,
    pub apriori_file: ColInputData,
    pub runlog_file: ColInputData,
    pub levels_file: ColInputData,
    pub models_dir: PathBuf,
    pub vmrs_dir: PathBuf,
    pub mav_file: ColInputData,
    pub ray_file: ColInputData,
    pub isotopologs_file: ColInputData,
    pub windows_file: ColInputData,
    pub telluric_linelists_md5_file: ColInputData,
    pub solar_linelist_file: Option<ColInputData>,
    pub ak_prefix: PathBuf,
    pub spt_prefix: PathBuf,
    pub col_file: PathBuf,
    pub format: String,
    pub command_line: String,
    pub column_names: Vec<String>
}

/// Return a vector of paths to the `.col` files to read.
/// 
/// The windows will be inferred from the `multiggg_file` and the `.col` files
/// must exist in `run_dir`.
pub fn get_col_files(multiggg_file: &Path, run_dir: &Path) -> Result<Vec<PathBuf>, HeaderError> {
    let col_file_basenames = utils::get_windows_from_multiggg(multiggg_file, true)
        .map_err(|e| HeaderError::custom(format!(
            "could not get windows from multiggg file: {e}"
        )))?;
    let nwin = col_file_basenames.len();

    let mut col_files = vec![];
    let mut missing_files = vec![];
    for basename in col_file_basenames {
        let cf_path = run_dir.join(format!("{basename}.col"));
        if cf_path.exists() {
            col_files.push(cf_path);
        } else {
            missing_files.push(basename);
        }
    }

    if missing_files.is_empty() {
        Ok(col_files)
    } else {
        let missing_str = missing_files.join(", ");
        let msg = format!("Missing {} of {} expected .col files, missing windows were: {missing_str}", missing_files.len(), nwin);
        Err(HeaderError::custom(msg))
    }
}

/// Get a path to one file from the `.col` file headers, error if it differs across files.
/// 
/// `get_file` is a function that takes ownership of a [`ColFileHeader`] and returns the
/// desired path as a [`PathBuf`].
pub fn get_file_from_col_header<F>(col_files: &[PathBuf], run_dir: &Path, get_file: F) -> Result<PathBuf, HeaderError> 
where F: Fn(ColFileHeader) -> PathBuf
{
    if col_files.is_empty() {
        return Err(HeaderError::custom("no .col files found"));
    }

    let mut fbuf = FileBuf::open(&col_files[0])
        .map_err(|e| HeaderError::CouldNotRead { location: col_files[0].clone().into(), cause: e.to_string() })?;

    let first_header = read_col_file_header(&mut fbuf)
        .map_err(|e| HeaderError::CouldNotRead {
            location: col_files[0].clone().into(),
            cause: format!("error reading header: {e}")
        })?;
    let expected_file = get_file(first_header);

    for cfile in &col_files[1..] {
        let mut fbuf = FileBuf::open(cfile)
            .map_err(|e| HeaderError::CouldNotRead {
                location: cfile.to_path_buf().into(),
                cause: format!("could not open this .col file: {e}")
            })?;
        let header = read_col_file_header(&mut fbuf)
            .map_err(|e| HeaderError::CouldNotRead {
                location: cfile.to_path_buf().into(),
                cause: format!("error reading .col file header: {e}")
            })?;
        let new_file = get_file(header);

        if new_file != expected_file {
            return Err(HeaderError::custom(
                format!("mismatched files in .col header: {} gave {}, while {} gave {}",
                (&col_files[0]).display(), expected_file.display(), cfile.display(), new_file.display())
            ))?;
        }
    }

    if expected_file.is_absolute() {
        Ok(expected_file)
    } else {
        Ok(run_dir.join(expected_file))
    }
}

pub fn get_runlog_from_col_files(multiggg_file: &Path, run_dir: &Path) -> Result<PathBuf, HeaderError> {
    let col_files = get_col_files(multiggg_file, run_dir)?;
    let runlog = get_file_from_col_header(&col_files, run_dir, |h| h.runlog_file.path)?;
    Ok(runlog)
}

/// Parse a "format=(...)" or "format:(...)" line from a post processing file header into a [`FortFormat`]
/// 
/// This takes the full line, including the "format:" or "format="; it will split the line on the
/// ":" or "=" and parse the part after that delimiter as the format string.
/// 
/// If `ignore_a1` is `true`, then the first instance of "a1" in the format string will be replaced
/// with a "1x" before parsing. This assumes a string like "(a57,a1,f13.8,23f13.5,0114(1pe13.5))",
/// where the "a1" is present to support some legacy formatting that ggg-rs generally ignores.
/// For deserializing into ggg-rs structures, this almost always needs removed.
fn parse_postproc_format_str(fmt: String, ignore_a1: bool) -> Result<FortFormat, String> {
    let fmt = if ignore_a1 && fmt.contains(",a1,") {
        fmt.replacen("a1,", "1x,", 1)
    } else {
        fmt
    };

    let s = if fmt.contains("=") {
        fmt.split_once("=").unwrap().1
    } else if fmt.contains(":") {
        fmt.split_once(":").unwrap().1
    } else {
        return Err("Expected the format line in the header to contain a '=' or ':'".to_string());
    };

    FortFormat::parse(s).map_err(|e| {
        format!("Could not parse the format string '{s}': {e}")
    })
}
/// Read the header of a tabular file and return the column names.
/// 
/// Given an open file reader that points to the start of a tabular file,
/// this reads the header (and advances the pointer to after the header).
/// The column names are parsed from the final line of the header and returned.
pub fn read_postproc_file_header<F: BufRead>(file: &mut FileBuf<F>) -> error_stack::Result<PostprocFileHeader, HeaderError> {
    let sizes = get_file_shape_info(file, 2)?;
    let nhead = sizes[0];
    let ncol = sizes[1];
    let nrow = sizes.get(2).map(|v| *v);
    let nauxcol = sizes.get(3).map(|v| *v);

    let mut missing_val_line = None;
    let mut format_line = None;
    let mut extra_lines = vec![];
    let mut line = String::new();
    for ihead in 1..nhead {
        line = file.read_header_line()?;
        if line.trim().starts_with("format") {
            format_line = Some(line.clone());
        } else if line.trim().starts_with("missing:") {
            missing_val_line = Some(line.clone());
        } else if ihead < nhead - 1 {
            extra_lines.push(line.clone());
        }
    }

    let format = if let Some(fmt) = format_line {
        parse_postproc_format_str(fmt, true).map_err(|e| {
            HeaderError::ParseError { 
                location: file.path.as_path().into(),
                cause: e }
        })?
    } else {
        return Err(HeaderError::ParseError { location: file.path.as_path().into(), cause: "Could not find format line in header".to_string() }.into());
    };

    let missing = missing_val_line.map(|line| {
        let (_, val) = line.split_once(":").unwrap();
        val.trim().parse::<f64>().map_err(|e| HeaderError::ParseError { 
            location: file.path.clone().into(),
            cause: format!("Missing value could not be parsed as a float ({e})")
        })
    }).transpose()?;

    let column_names = line.trim()
        .split_ascii_whitespace()
        .map(|s| s.to_string())
        .collect_vec();
    Ok(PostprocFileHeader { nhead, ncol, nrow, nauxcol, missing, format, column_names, extra_lines })
}

#[derive(Debug)]
pub struct PostprocFileHeader {
    pub nhead: usize,
    pub ncol: usize,
    pub nrow: Option<usize>,
    pub nauxcol: Option<usize>,
    pub missing: Option<f64>,
    pub format: FortFormat,
    pub column_names: Vec<String>,
    pub extra_lines: Vec<String>,
}

pub fn read_col_file_header<F: BufRead>(file: &mut FileBuf<F>) -> error_stack::Result<ColFileHeader, HeaderError> {
    let (nhead, ncol) = get_nhead_ncol(file)
        .change_context_lazy(|| HeaderError::ParseError {
            location: file.path.as_path().into(), 
            cause: "Could not parse number of header lines and data columns".to_string() 
        })?;

    if nhead != 21 {
        error_stack::bail!(HeaderError::NumLinesMismatch { expected: 21, got: nhead });
    }

    Ok(ColFileHeader { 
        nhead, 
        ncol,
        gfit_version: ProgramVersion::from_str(&file.read_header_line()?)?, 
        gsetup_version: ProgramVersion::from_str(&file.read_header_line()?)?,
        data_partition_file: ColInputData::from_str(&file.read_header_line()?)?,
        apriori_file: ColInputData::from_str(&file.read_header_line()?)?,
        runlog_file: ColInputData::from_str(&file.read_header_line()?)?,
        levels_file: ColInputData::from_str(&file.read_header_line()?)?,
        models_dir: PathBuf::from(&file.read_header_line()?),
        vmrs_dir: PathBuf::from(&file.read_header_line()?),
        mav_file: ColInputData::from_str(&file.read_header_line()?)?,
        ray_file: ColInputData::from_str(&file.read_header_line()?)?,
        isotopologs_file: ColInputData::from_str(&file.read_header_line()?)?,
        windows_file: ColInputData::from_str(&file.read_header_line()?)?,
        telluric_linelists_md5_file: ColInputData::from_str(&file.read_header_line()?)?,
        solar_linelist_file: ColInputData::from_str_opt(&file.read_header_line()?)?,
        ak_prefix: PathBuf::from(&file.read_header_line()?),
        spt_prefix: PathBuf::from(&file.read_header_line()?),
        col_file: PathBuf::from(&file.read_header_line()?),
        format: file.read_header_line()?.trim().to_string(),
        command_line: file.read_header_line()?.trim().to_string(),
        column_names: file.read_header_line()?.split_whitespace().map(|s| s.to_string()).collect_vec()
    })
}

/// The different retrieved quantity types in `.col` files
#[derive(Debug, Clone, Copy)]
pub enum ColRetQuantity {
    /// Airmass
    Am,
    /// Original vertical column
    Ovc,
    /// VMR scale factor
    Vsf,
    /// VMR scale factor uncertainty
    VsfError
}

impl ColRetQuantity {
    /// The name of the data column in the `.col` file for this
    /// quantity and a given gas.
    fn column_name(&self, gas: &str) -> String {
        match self {
            ColRetQuantity::Am => format!("AM_{gas}"),
            ColRetQuantity::Ovc => format!("OVC_{gas}"),
            ColRetQuantity::Vsf => format!("VSF_{gas}"),
            ColRetQuantity::VsfError => format!("VSF_{gas}_error"),
        }
    }
}

/// Data from one row of a `.col` file
#[derive(Debug, serde::Deserialize)]
pub struct ColRow {
    /// Spectrum name
    #[serde(rename = "Spectrum")]
    pub spectrum: String,

    /// Number of iterations
    #[serde(rename = "Nit")]
    pub num_iter: u32,

    /// Continuum level
    #[serde(rename = "CL")]
    pub cont_level: f64,

    /// Continuum tilt (i.e. slope)
    #[serde(rename = "CT")]
    pub cont_tilt: f64,

    /// Continuum curvature (i.e. the quadratic term)
    #[serde(rename = "CC")]
    pub cont_curve: f64,

    /// Frequency shift
    #[serde(rename = "FS")]
    pub freq_shift: f64,

    /// Solar gas stretch
    #[serde(rename = "SG")]
    pub sg_stretch: f64,

    /// Zero level offset
    #[serde(rename = "ZO")]
    pub zlo: f64,

    /// Spectral root mean square residual (observed vs. simulated) divided by the continuum level
    #[serde(rename = "RMS/CL")]
    pub rms_over_cl: f64,

    /// Altitude of the measurement determined by outside pressure
    #[serde(rename = "Zpres")]
    pub zpres: f64,

    /// The retrieved fields for this window; it will have the four fields
    /// of [`ColRetQuantity`] per retrieved gas.
    #[serde(flatten)]
    pub ret_fields: HashMap<String, f64>,

    /// The target gas for this window (i.e. not one retrieved only as an interferent)
    #[serde(skip)]
    primary_gas: String,
}

impl ColRow {
    /// Get one of the retrieved quantities for this window's primary gas.
    /// 
    /// Returns `None` if the required column is not found in the `.col` file.
    /// This shouldn't happen unless something is very wrong, so it can be acceptable
    /// to treat this with an `.expect("...")`.
    pub fn get_primary_gas_quantity(&self, quantity: ColRetQuantity) -> Option<f64> {
        let key = quantity.column_name(&self.primary_gas);
        self.ret_fields.get(&key).map(|v| *v)
    }

    /// Get one of the retrieved quantities for any gas retrieved in this window.
    /// 
    /// Returns `None` if it cannot find the needed column in the `.col` file.
    pub fn get_gas_quanity(&self, gas: &str, quantity: ColRetQuantity) -> Option<f64> {
        let key = quantity.column_name(gas);
        self.ret_fields.get(&key).map(|v| *v)
    }
}

/// An iterator over data rows in a `.col` file; holds the
/// `.col` file open for the duration of the iterator's life.
pub struct ColRowIter {
    lines: std::io::Lines<FileBuf<BufReader<std::fs::File>>>,
    fmt: fortformat::FortFormat,
    colnames: Vec<String>,
    primary_gas: String,
    src_path: PathBuf,
}

impl Iterator for ColRowIter {
    type Item = Result<ColRow, GggError>;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.lines.next()?
            .map_err(|e| GggError::CouldNotRead { path: self.src_path.clone(), reason: e.to_string() });

        let line = match res {
            Ok(s) => s,
            Err(e) => return Some(Err(e)),
        };

        let mut row: ColRow = match fortformat::from_str_with_fields(&line, &self.fmt, &self.colnames) {
            Ok(r) => r,
            Err(e) => return Some(Err(GggError::DataError { path: self.src_path.clone(), cause: e.to_string() })),
        };

        row.primary_gas = self.primary_gas.clone();
        Some(Ok(row))
    }
}

/// Convenience function to open a `.col` file at `path` and return an iterator over
/// its data rows.
pub fn open_and_iter_col_file(path: &Path) -> error_stack::Result<ColRowIter, BodyError> {
    let mut fbuf = FileBuf::open(path)
        .change_context_lazy(|| BodyError::could_not_read("error opening .col file", Some(path.into()), None, None))?;
    let header = read_col_file_header(&mut fbuf).change_context_lazy(|| {
        BodyError::could_not_read("error getting information from .col file header", Some(path.into()), None, None)
    })?;

    let fmt = fortformat::FortFormat::parse(&header.format)
        .map_err(|e| BodyError::unexpected_format(
            format!("unable to parse Fortran format spec: {e}"), Some(path.into()), None, None
        ))?;

    let (_, ret_gases) = header.command_line.split_once(':').ok_or_else(|| {
        BodyError::unexpected_format("command line is expected to have one colon", Some(path.into()), None, Some(header.command_line.clone()))
    })?;

    let primary_gas = ret_gases.split_whitespace()
        .next()
        .ok_or_else(|| BodyError::unexpected_format(
            "command line is expected to have at least one gas after the colon",
            Some(path.into()), None, Some(header.command_line.clone())))?
        .to_string();

    Ok(ColRowIter {
        lines: fbuf.lines(),
        fmt,
        colnames: header.column_names,
        primary_gas,
        src_path: path.to_path_buf()
    })
}

/// Auxiliary (i.e. non-retrieved) data stored in post-processing files.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct AuxData {
    pub spectrum: String,
    pub year: f64,
    pub day: f64,
    pub hour: f64,
    pub run: f64,
    pub lat: f64,
    pub long: f64,
    pub zobs: f64,
    pub zmin: f64,
    pub solzen: f64,
    pub azim: f64,
    pub osds: f64,
    pub opd: f64,
    pub fovi: f64,
    pub amal: f64,
    pub graw: f64,
    pub tins: f64,
    pub pins: f64,
    pub tout: f64,
    pub pout: f64,
    pub hout: f64,
    pub sia: f64,
    pub fvsi: f64,
    pub wspd: f64,
    pub wdir: f64,
    pub o2dmf: Option<f64>,
}

impl AuxData {
    /// The names of the fields for this struct.
    /// 
    /// Developers *must* update this when fields are added unless those
    /// fields are not to be serialized or deserialized.
    pub fn postproc_fields_str() -> &'static[&'static str] {
        &["spectrum", "year", "day", "hour", "run", "lat", "long", "zobs", "zmin",
          "solzen", "azim", "osds", "opd", "fovi", "amal", "graw", "tins", "pins",
          "tout", "pout", "hout", "sia", "fvsi", "wspd", "wdir", "o2dmf"]
    }

    /// A fully-owned version of `postproc_fields_str`.
    pub fn postproc_fields_vec() -> Vec<String> {
        Vec::from_iter(Self::postproc_fields_str().into_iter().map(|s| s.to_string()))
    }

    pub fn get_numeric_field(&self, field: &str) -> Option<f64> {
        match field {
            "year" => Some(self.year),
            "day" => Some(self.day),
            "hour" => Some(self.hour),
            "run" => Some(self.run),
            "lat" => Some(self.lat),
            "long" => Some(self.long),
            "zobs" => Some(self.zobs),
            "zmin" => Some(self.zmin),
            "solzen" => Some(self.solzen),
            "azim" => Some(self.azim),
            "osds" => Some(self.osds),
            "opd" => Some(self.opd),
            "fovi" => Some(self.fovi),
            "amal" => Some(self.amal),
            "graw" => Some(self.graw),
            "tins" => Some(self.tins),
            "pins" => Some(self.pins),
            "tout" => Some(self.tout),
            "pout" => Some(self.pout),
            "hout" => Some(self.hout),
            "sia" => Some(self.sia),
            "fvsi" => Some(self.fvsi),
            "wspd" => Some(self.wspd),
            "wdir" => Some(self.wdir),
            _ => None
        }
    }
}

impl From<&RunlogDataRec> for AuxData {
    /// Create an `AuxData` instance from a reference to a [`RunlogDataRec`].
    /// Most values in `AuxData` come from a runlogs; the exceptions are the `run`
    /// value and the `zmin` value. `run` is usually just the 1-based row index in
    /// the output file, and `zmin` is the "Zpres" field of the `.ray` file. These
    /// will be initialized as the postprocessing fill value and should be replaced
    /// before serializing the returned instance.
    fn from(value: &RunlogDataRec) -> Self {
        let (dec_year, dec_doy, dec_hour) = utils::to_decimal_year_day_hour(value.year, value.day, value.hour);
        Self {
            spectrum: value.spectrum_name.to_string(),
            year: dec_year,
            day: dec_doy,
            hour: dec_hour,
            run: POSTPROC_FILL_VALUE,
            lat: value.obs_lat,
            long: value.obs_lon,
            zobs: value.obs_alt,
            zmin: POSTPROC_FILL_VALUE,
            solzen: value.asza,
            azim: value.azim,
            osds: value.osds,
            opd: value.opd,
            fovi: value.fovi,
            amal: value.amal,
            graw: value.delta_nu,
            tins: value.tins,
            pins: value.pins,
            tout: value.tout,
            pout: value.pout,
            hout: value.hout,
            sia: value.sia,
            fvsi: value.fvsi,
            wspd: value.wspd,
            wdir: value.wdir,
            o2dmf: None
        }
    }
}


pub enum PostprocType {
    Vsw,
    Tsw,
    Vav,
    Tav,
    VswAda,
    VavAda,
    VavAdaAia,
    Other(String)
}

impl PostprocType {
    pub fn from_path(path: &Path) -> Option<Self> {
        let name = path.file_name()?.to_string_lossy();
        let (_, ext) = name.split_once(".")?;
        match ext {
            "vsw" => Some(Self::Vsw),
            "tsw" => Some(Self::Tsw),
            "vav" => Some(Self::Vav),
            "tav" => Some(Self::Tav),
            "vsw.ada" => Some(Self::VswAda),
            "vav.ada" => Some(Self::VavAda),
            "vav.ada.aia" => Some(Self::VavAdaAia),
            _ => Some(Self::Other(ext.to_string()))
        }
    }
}

/// One row of data in a postprocessing file.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct PostprocRow {
    /// Auxiliary (i.e. non-retrieved) data.
    #[serde(flatten)]
    pub auxiliary: AuxData,
    /// Retrieved data (e.g. gas columns or VSFs and their associated errors).
    #[serde(flatten)]
    pub retrieved: HashMap<String, f64>
}

impl PostprocRow {
    /// Initialize a new row with the given auxiliary data and an
    /// empty `retrieved` map.
    pub fn new(aux_data: AuxData) -> Self {
        Self { auxiliary: aux_data, retrieved: HashMap::new() }
    }

    /// Get the value of one of the numeric fields from the row
    pub fn get_numeric_field(&self, field: &str) -> Option<f64> {
        if let Some(value) = self.auxiliary.get_numeric_field(field) {
            Some(value)
        } else {
            self.retrieved.get(field).map(|v| *v)
        }
    }
}

/// An iterator over data rows in a postprocessing text file; holds the
/// file open for the duration of the iterator's life.
pub struct PostprocRowIter {
    lines: std::io::Lines<FileBuf<BufReader<std::fs::File>>>,
    fmt: fortformat::FortFormat,
    colnames: Vec<String>,
    src_path: PathBuf,
}

impl Iterator for PostprocRowIter {
    type Item = Result<PostprocRow, GggError>;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.lines.next()?
            .map_err(|e| GggError::CouldNotRead { path: self.src_path.clone(), reason: e.to_string() });

        let line = match res {
            Ok(s) => s,
            Err(e) => return Some(Err(e)),
        };

        let row: PostprocRow = match fortformat::from_str_with_fields(&line, &self.fmt, &self.colnames) {
            Ok(r) => r,
            Err(e) => return Some(Err(GggError::DataError { path: self.src_path.clone(), cause: e.to_string() })),
        };

        Some(Ok(row))
    }
}

/// Convenience function to open a postprocessing output file at `path` and
/// return an iterator over its data rows.
pub fn open_and_iter_postproc_file(path: &Path) -> error_stack::Result<(PostprocFileHeader, PostprocRowIter), BodyError> {
    let mut fbuf = FileBuf::open(path)
        .change_context_lazy(|| BodyError::could_not_read("error opening .col file", Some(path.into()), None, None))?;

    let header = read_postproc_file_header(&mut fbuf).change_context_lazy(|| {
        BodyError::could_not_read("error getting information from postprocessing file header", Some(path.into()), None, None)
    })?;

    let it = PostprocRowIter {
        lines: fbuf.lines(),
        fmt: header.format.clone(),
        colnames: header.column_names.clone(),
        src_path: path.to_path_buf()
    };

    Ok((header, it))
}

/// Represents data from a row of any tabular GGG file that has the spectrum name,
/// followed by zero or more floating point values. A column containing the spectrum
/// name will be recognized if it is first and contains "spectrum" (case insensitive)
/// in the column name.
/// 
/// See [`iter_tabular_file`] for how to iterate over rows of such a file.
#[derive(Debug, serde::Deserialize)]
pub struct GenericRow {
    spectrum: String, // would like this to be Option<String>, but that causes the deserialization to fail as of fortformat commit 60d687db8
    #[serde(flatten)]
    data: HashMap<String, f64>,
}

impl GenericRow {
    /// Return the spectrum name for this row. 
    /// Note that this may return an `Option<&str>` in the future,
    /// to accomodate files that do not begin with a spectrum name.
    pub fn spectrum(&self) -> &str {
        &self.spectrum
    }

    /// Get the value of one of the data fields (other than the spectrum).
    /// Returns `None` if that field does not exist.
    pub fn get(&self, field: &str) -> Option<f64> {
        self.data.get(field).map(|v| *v)
    } 
}

/// An iterator over rows of a generic tabular GGG file.
pub struct GenericRowIter {
    lines: std::io::Lines<FileBuf<BufReader<std::fs::File>>>,
    colnames: Vec<String>,
    fmt: fortformat::FortFormat,
    src_path: PathBuf,
}

impl Iterator for GenericRowIter {
    type Item = Result<GenericRow, GggError>;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.lines.next()?
            .map_err(|e| GggError::CouldNotRead { path: self.src_path.clone(), reason: e.to_string() });

        let line = match res {
            Ok(s) => s,
            Err(e) => return Some(Err(e)),
        };

        let row: GenericRow = match fortformat::from_str_with_fields(&line, &self.fmt, &self.colnames) {
            Ok(r) => r,
            Err(e) => return Some(Err(GggError::DataError { path: self.src_path.clone(), cause: e.to_string() })),
        };

        Some(Ok(row))
    }
}

/// Iterate over a tabular GGG file.
/// 
/// A tabular GGG file must contain:
/// 
/// 1. a first line that specifies the number of lines in the header,
/// 2. a line in the header beginning with "format=" that gives the Fortran
///    format of the data,
/// 3. a list of column names as the last line of the header, and
/// 4. a table of values written in a Fortran format fixed space format, 
///    where the first column is the spectrum name and the remaining columns
///    are numeric.
/// 
/// The name for the spectrum column must contain "spectrum" in it (ignoring
/// case).
pub fn iter_tabular_file(file: &Path) -> Result<GenericRowIter, GggError> {
    let mut fbuf = utils::FileBuf::open(file)?;
    let nhead = utils::get_nhead(&mut fbuf)?;
    let mut fmt_str = None;
    let mut col_str = None;
    for i in 1..nhead {
        let line = fbuf.read_header_line()?;
        let line = line.trim_start();
        if line.starts_with("format=") {
            fmt_str = Some(line.replace("format=", "").to_string());
        }
        if i == nhead - 1 {
            col_str = Some(line.to_string());
        }
    }

    let fmt = if let Some(s) = fmt_str {
        fortformat::FortFormat::parse(&s).map_err(|e| {
            GggError::HeaderError(HeaderError::ParseError { 
                location: file.into(), cause: format!("invalid Fortran format string: {e}")
            })
        })?
    } else {
        return Err(GggError::HeaderError(HeaderError::ParseError { 
            location: file.into(), cause: "could not find format line".into()
        }))
    };

    let colnames = if let Some(s) = col_str {
        s.split_whitespace()
            .enumerate()
            .map(|(i,s)| {
                if i == 0 && s.to_ascii_lowercase().contains("spectrum") {
                    // Standardize different names for the spectrum column so that
                    // serde can work
                    "spectrum".to_string()
                } else {
                    s.to_string()
                }
            }).collect_vec()
    } else {
        // This shouldn't happen (because it'll always be set as the last line of the header),
        // but just in case...
        return Err(GggError::HeaderError(HeaderError::ParseError {
            location: file.into(), cause: "could not find column name line".into()
        }))
    };

    Ok(GenericRowIter { 
        lines: fbuf.lines(),
        colnames,
        fmt,
        src_path: file.to_path_buf()
    })
}


/// Write the header of a postprocessing file.
/// 
/// # Inputs
/// - `f`: the handle to write to, usually a mutable [`std::io::BufWriter`] or similar.
/// - `ncol`: the number of columns in the file (including the spectrum name).
/// - `naux`: the number of columns containing auxiliary data (i.e not retrieved quantities).
/// - `program_versions`: the list of programs that generated this file to add to the header.
///   If using this to write the first post processing file, make sure to include GSETUP and GFIT
///   from the `.col` files, as well as the program generating the current file. If using this to
///   write a later post processing file, then usually previous program versions will be included
///   in the `extra_lines` read from the previous file's header, and this will only include the
///   new program.
/// - `extra_lines`: additional lines to include in the header, e.g. AICF or ADCF values.
/// - `missing_value`: the value to use as a fill value for missing data. Should be *significantly*
///   larger than any real value, [`POSTPROC_FILL_VALUE`] is a good default.
/// - `format_str`: the Fortran format string which the output follows.
/// - `column_names`: a slice of all the data columns' names.
/// 
/// A note on `format_str` regarding compatibility with Fortran GGG programs: many of these programs
/// expect a 1-character-wide column just after the spectrum name which is kept for compatibility with
/// older runlog formats. Since the Rust code does not serialize that, the `format_str` value you pass
/// here should include that if needed, even if that means it differs from the string used by [`fortformat`]
/// to actually write the output. (That is, usually you will remove the "a1" column for the string given
/// to [`fortformat`] and add one to the width of the spectrum name column.)
pub fn write_postproc_header<W: Write>(mut f: W, ncol: usize, nrow: usize, naux: usize, program_versions: &[ProgramVersion],
                                       extra_lines: &[String], missing_value: f64, format_str: &str, column_names: &[String])
-> error_stack::Result<(), WriteError>
{
    // Skip single-character fields; those seem to be a holdover to allow a : or ; to follow
    // the spectrum name?
    let col_width = fortformat::FortFormat::parse(format_str)
        .map_err(|e| WriteError::convert_error(
            format!("Could not interpret widths in format string: {e}")
        ))?.into_fields()
        .expect("Fortran format string should contain fixed width fields, not list-directed input (i.e. must not be '*')")
        .into_iter()
        .filter_map(|field| {
            let width = field.width().expect("write_postproc_header should not receive a format string with non-fixed width fields");
            if width > 1 { 
                Some(width)
            } else {
                None
            }
        });

    // 4 = line with nhead etc. + missing + format + colnames
    let nhead = program_versions.len() + extra_lines.len() + 4;
    writeln!(f, " {nhead}  {ncol}  {nrow}  {naux}").change_context_lazy(|| WriteError::IoError)?;

    for pver in program_versions.iter() {
        writeln!(f, " {pver}").change_context_lazy(|| WriteError::IoError)?;
    }

    for line in extra_lines {
        // The trim_end protects against newlines being accidentally doubled from lines read in
        // from a previous file.
        writeln!(f, "{}", line.trim_end()).change_context_lazy(|| WriteError::IoError)?;
    }

    let mvfmt = fortformat::FortFormat::parse("(1pe11.4)").unwrap();
    let mvstr = fortformat::to_string(missing_value, &mvfmt).unwrap();
    writeln!(f, "missing: {mvstr}").change_context_lazy(|| WriteError::IoError)?;

    writeln!(f, "format:{format_str}").change_context_lazy(|| WriteError::IoError)?;

    for (width, name) in col_width.zip(column_names) {
        let width = width as usize;
        let n = if name.len() >= width - 1 { 0 } else { width - 1 - name.len() };
        write!(f, " {name}{}", " ".repeat(n)).change_context_lazy(|| WriteError::IoError)?;
    }
    writeln!(f, "").change_context_lazy(|| WriteError::IoError)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::{rstest,fixture};
    use super::*;

    #[fixture]
    fn benchmark_ray_path() -> PathBuf {
        let test_data_dir = PathBuf::from(file!())
            .parent().unwrap()
            .parent().unwrap()
            .join("test-data");
        test_data_dir.join("pa_ggg_benchmark.ray")
    }

    #[rstest]
    fn test_generic_iter(benchmark_ray_path: PathBuf) {
        let mut it = iter_tabular_file(&benchmark_ray_path).unwrap();
        let data = it.next().unwrap().unwrap();
        assert_eq!(data.spectrum(), "pa20040721saaaaa.043");
        approx::assert_abs_diff_eq!(data.get("Pobs").unwrap(), 950.7);
        approx::assert_abs_diff_eq!(data.get("ASZA").unwrap(), 39.684);
        approx::assert_abs_diff_eq!(data.get("Zmin").unwrap(), 0.46083);

        let data = it.next().unwrap().unwrap();
        assert_eq!(data.spectrum(), "pa20040721saaaab.043");

        let data = it.next().unwrap().unwrap();
        assert_eq!(data.spectrum(), "pa20040721saaaaa.119");
        approx::assert_abs_diff_eq!(data.get("Pobs").unwrap(), 950.6);
        approx::assert_abs_diff_eq!(data.get("ASZA").unwrap(), 63.799);
        approx::assert_abs_diff_eq!(data.get("Zmin").unwrap(), 0.46742);
    }
}