use std::{path::PathBuf, str::FromStr, sync::OnceLock, io::BufRead, fmt::Display};

use chrono::NaiveDate;
use error_stack::{IntoReport, ResultExt, Report};
use itertools::Itertools;

use crate::utils::{get_nhead_ncol, FileBuf, GggError};


static INPUT_MD5_REGEX: OnceLock<regex::Regex> = OnceLock::new();

#[derive(Debug, thiserror::Error)]
pub enum HeaderError {
    ParseError{header_line: Option<String>, cause: String},
    NumLinesMismatch{expected: usize, got: usize},
}

impl Display for HeaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ParseError { header_line: Some(line), cause } => {
                write!(f, "Could not parse header line '{line}': {cause}")
            },
            Self::ParseError { header_line: None, cause } => {
                write!(f, "Error parsing header line: {cause}")
            },
            Self::NumLinesMismatch { expected, got } => {
                write!(f, "Expected {expected} header lines, nhead indicates {got}")
            }
        }
    }
}

pub struct ColInputData {
    path: PathBuf,
    md5sum: String,
}

impl FromStr for ColInputData {
    type Err = HeaderError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let re = INPUT_MD5_REGEX.get_or_init(|| {
            regex::Regex::new("(?<md5>0-9a-fA-F{32})  (?<path>.+)").unwrap()
        });

        let caps = re.captures(s)
            .ok_or_else(|| HeaderError::ParseError { 
                header_line: Some(s.to_string()), 
                cause: "Did not match expected format of 32 hex digit checksum, two spaces, then a path".to_string()
            })?;

        let md5sum = caps["md5"].to_string();
        let path = PathBuf::from(&caps["path"]);

        Ok(Self { path, md5sum })
    }
}

pub struct ProgramVersion {
    program: String,
    version: String,
    date: String,
    authors: String,
}

impl FromStr for ProgramVersion {
    type Err = HeaderError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (program, version, date, authors) = s.split_whitespace()
            .collect_tuple()
            .ok_or_else(|| HeaderError::ParseError {header_line: Some(s.to_string()), cause: "Expected 4 space-delimited strings".to_string()})?;

        // This takes something like "Version 5.26" and converts to just "5.26"
        let version = if version.to_ascii_lowercase().contains("version") {
            version.to_ascii_lowercase().replace("version", "")
        } else {
            version.to_string()
        }.trim().to_string();

        Ok(Self { 
            program: program.trim().to_string(),
            version, 
            date: date.to_string(),
            authors: authors.to_string() 
        })
    }
}

pub struct ColFileHeader {
    nhead: usize,
    ncol: usize,
    gfit_version: ProgramVersion,
    gsetup_version: ProgramVersion,
    data_partition_file: ColInputData,
    apriori_file: ColInputData,
    runlog_file: ColInputData,
    levels_file: ColInputData,
    models_dir: PathBuf,
    vmrs_dir: PathBuf,
    mav_file: ColInputData,
    ray_file: ColInputData,
    isotopologs_file: ColInputData,
    windows_file: ColInputData,
    telluric_linelists_md5_file: ColInputData,
    solar_linelist_file: Option<ColInputData>,
    ak_prefix: PathBuf,
    spt_prefix: PathBuf,
    col_file: PathBuf,
    format: String,
    command_line: String,
    column_names: Vec<String>
}

pub fn read_col_file_header<F: BufRead>(file: &mut FileBuf<F>) -> error_stack::Result<ColFileHeader, HeaderError> {
    let (nhead, ncol) = get_nhead_ncol(file)
        .into_report()
        .change_context_lazy(|| HeaderError::ParseError {
            header_line: None, 
            cause: "Could not parse number of header lines and data columns".to_string() 
        })?;

    if nhead != 21 {
        error_stack::bail!(HeaderError::NumLinesMismatch { expected: 21, got: nhead });
    }

    let gfit_version = ProgramVersion::from_str(&file.read_header_line()?)?;

    todo!()
}