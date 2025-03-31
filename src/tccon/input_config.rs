use std::{io::BufRead, path::Path};

use error_stack::ResultExt;
use fortformat::FortFormat;
use indexmap::IndexMap;
use itertools::Itertools;

use crate::{collation::{CollationError, CollationPrefixer}, error::BodyError, utils::{self, get_ggg_path, parse_window_name, FileBuf, GggError}};


pub struct PrefixEntry {
    pub start_wn: f32,
    pub end_wn: f32,
    pub prefix: Option<String>,
    pub nc_suffix: Option<String>,
    pub nc_group: Option<String>,
}

pub struct TcconWindowPrefixes {
    pub ranges: Vec<PrefixEntry>,
    pub all_prefixes: Vec<String>,
}

impl TcconWindowPrefixes {
    pub fn new_standard_opt() -> error_stack::Result<Option<Self>, BodyError> {
        let gggpath = get_ggg_path().change_context_lazy(|| BodyError::custom("could not get GGGPATH"))?;
        let default_file = gggpath.join("tccon").join("secondary_prefixes.dat");
        if default_file.exists() {
            Ok(Some(
                TcconWindowPrefixes::new(&default_file)?
            ))
        } else {
            log::warn!("No prefix file specified and default file ({}) not found, will not use any prefixes for secondary detector windows", default_file.display());
            Ok(None)
        }
    }

    pub fn new(prefix_file: &Path) -> error_stack::Result<Self, BodyError> {
        let f = std::fs::File::open(prefix_file)
            .change_context_lazy(|| BodyError::could_not_read(
                "opening file failed", 
                Some(prefix_file.to_path_buf()),
                None, None))?;
        let rdr = std::io::BufReader::new(f);
        let mut ranges = vec![];
        let mut all_prefixes = vec![];
        for (iline, line) in rdr.lines().enumerate() {
            let line = line.change_context_lazy(|| BodyError::could_not_read(
                "reading line failed", Some(prefix_file.to_path_buf()), Some(iline+1), None)
            )?;
            let line = line.trim();

            if line.starts_with(":") || line.is_empty() {
                // comment or empty line
                continue;
            }

            let mut parts = line.split_ascii_whitespace();
            let start_wn = parts.next().ok_or_else(|| BodyError::could_not_read(
                "line did not include a starting wavenumber", Some(prefix_file.to_path_buf()), Some(iline+1), None
            ))?;
            let end_wn = parts.next().ok_or_else(|| BodyError::could_not_read(
                "line did not include an ending wavenumber", Some(prefix_file.to_path_buf()), Some(iline+1), None
            ))?;
            let prefix = parts.next().map(|s| s.to_string());
            let nc_suffix = parts.next().map(|s| s.to_string());
            let nc_group = parts.next().map(|s| s.to_string());

            let start_wn = start_wn.parse::<f32>().change_context_lazy(|| BodyError::could_not_read(
                "starting wavenumber is not a valid number", Some(prefix_file.to_path_buf()), Some(iline+1), None
            ))?;
            let end_wn = end_wn.parse::<f32>().change_context_lazy(|| BodyError::could_not_read(
                "ending wavenumber on is not a valid number", Some(prefix_file.to_path_buf()), Some(iline+1), None
            ))?;
            
            if let Some(ref p) = prefix {
                all_prefixes.push(p.to_string());
            }
            ranges.push(PrefixEntry{start_wn, end_wn, prefix, nc_suffix, nc_group})
        }
        
        Ok(Self { ranges, all_prefixes })
    }

    pub fn get_entry(&self, window: &str) -> Result<&PrefixEntry, BodyError> {
        let (_, center) = parse_window_name(window)?;

        for entry in self.ranges.iter() {
            if entry.start_wn <= center && entry.end_wn > center {
                return Ok(entry)
            }
        }


        Err(BodyError::custom(
            format!("Window {window} does not have a prefix defined; frequency center ({center}) is outside all defined ranges")
        ))
    }
}


impl CollationPrefixer for TcconWindowPrefixes {
    fn set_provided_windows<P: AsRef<Path>>(&mut self, _col_files: &[P]) {}

    fn get_prefix(&self, window: &str) -> Result<&str, CollationError> {
        let entry = self.get_entry(window)
        .map_err(|e| CollationError::custom(format!("Could not get entry for window '{window}': {e}")))?;

        if !entry.prefix.as_ref().is_some_and(|p| window.starts_with(p)) {
            log::warn!("Window {window} already begins with {}. Please update your post processing to avoid adding this prefix yourself.", entry.prefix.as_ref().unwrap());
            Ok("")
        } else if self.all_prefixes.iter().any(|p| window.starts_with(p)) {
            Err(CollationError::custom(
                format!("Window {window} begins with a prefix it should not.")
            ))
        } else {
            Ok(entry.prefix.as_deref().unwrap_or(""))
        }
    }
}


// Correction files

trait RowWithKey {
    fn key(&self) -> String;
}

#[derive(Debug, serde::Deserialize)]
pub struct AdcfRow {
    #[serde(rename = "Gas")]
    pub gas_or_window: String,
    #[serde(rename = "ADCF")]
    pub adcf: f64,
    #[serde(rename = "ADCF_Err")]
    pub adcf_error: f64,
    pub g: Option<f64>,
    pub p: Option<f64>
}

impl RowWithKey for AdcfRow {
    fn key(&self) -> String {
        self.gas_or_window.clone()
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct AicfRow {
    #[serde(rename = "Gas")]
    pub gas: String,
    #[serde(rename = "AICF")]
    pub aicf: f64,
    #[serde(rename = "AICF_Err")]
    pub aicf_error: f64,
    #[serde(rename = "WMO_Scale")]
    pub wmo_scale: String,
}

impl RowWithKey for AicfRow {
    fn key(&self) -> String {
        self.gas.clone()
    }
}

pub fn read_adcf_file(corr_file: &Path) -> Result<IndexMap<String, AdcfRow>, GggError> {
    read_correction_file(corr_file)
}

pub fn read_aicf_file(corr_file: &Path) -> Result<IndexMap<String, AicfRow>, GggError> {
    read_correction_file(corr_file)
}

fn read_correction_file<'de, T: RowWithKey + serde::de::DeserializeOwned>(corr_file: &Path) -> Result<IndexMap<String, T>, GggError> {
    let mut f = FileBuf::open(corr_file)?;
    let nhead = utils::get_nhead(&mut f)?;
    for _ in 1..nhead-1 {
        f.read_header_line()?;
    }

    let colname_line = f.read_header_line()?;
    let colnames = colname_line.split_ascii_whitespace().collect_vec();
    let mut corrections = IndexMap::new();

    for line in f.lines() {
        let line = line.map_err(|e| GggError::CouldNotRead { path: corr_file.to_path_buf(), reason: e.to_string() })?;
        let row: T = fortformat::from_str_with_fields(&line, &FortFormat::ListDirected, &colnames)
            .map_err(|e| GggError::DataError {
                path: corr_file.to_path_buf(),
                cause: format!("While deserializing the line '{line}', error was: {e}")
            })?;
        let key = row.key();
        corrections.insert(key, row);
    }
    
    Ok(corrections)
}