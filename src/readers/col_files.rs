use std::{collections::HashMap, io::{BufRead, BufReader}, path::{Path, PathBuf}, str::FromStr, sync::OnceLock};

use error_stack::ResultExt;
use itertools::Itertools;

use crate::{error::{BodyError, HeaderError}, utils::{self, FileBuf, GggError}};

use super::ProgramVersion;

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


pub fn read_col_file_header<F: BufRead>(file: &mut FileBuf<F>) -> error_stack::Result<ColFileHeader, HeaderError> {
    let (nhead, ncol) = utils::get_nhead_ncol(file)
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