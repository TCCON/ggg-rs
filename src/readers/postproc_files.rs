use std::{collections::HashMap, fmt::Display, io::{BufRead, BufReader}, path::{Path, PathBuf}, str::FromStr};

use error_stack::ResultExt;
use fortformat::FortFormat;
use itertools::Itertools;
use serde::Deserialize;

use crate::{error::{BodyError, FileLocation, HeaderError}, readers::runlogs::RunlogDataRec, utils::{self, FileBuf, GggError}};

use super::{ProgramVersion, POSTPROC_FILL_VALUE};

/// Return `true` if the given value is a fill (assuming it came from a post-processing file)
pub fn is_postproc_fill(v: f64) -> bool {
    v > POSTPROC_FILL_VALUE * 0.99
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
            "o2dmf" => self.o2dmf,
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


#[derive(Debug, Clone)]
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

impl Display for PostprocType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Vsw => write!(f, ".vsw file"),
            Self::Tsw => write!(f, ".tsw file"),
            Self::Vav => write!(f, ".vav file"),
            Self::Tav => write!(f, ".tav file"),
            Self::VswAda => write!(f, ".vsw.ada file"),
            Self::VavAda => write!(f, ".vav.ada file"),
            Self::VavAdaAia => write!(f, ".vav.ada.aia file"),
            Self::Other(ext) => write!(f, "{ext} file")
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

    let header = PostprocFileHeader::read_postproc_file_header(&mut fbuf).change_context_lazy(|| {
        BodyError::could_not_read("error getting information from postprocessing file header", Some(path.into()), None, None)
    })?;

    // We don't deserialize the comment character which can come after the spectrum name - if
    // there's an a1 format string in the second spot, that will cause a problem so convert it
    // to a skip.
    let mut fformat = header.fformat.clone();
    if let Some(fortformat::FortField::Char { width }) = fformat.get_field(1) {
        if width.is_none() || width.is_some_and(|w| w == 1) {
            fformat.set_field(1, fortformat::FortField::Skip)
                .expect("should be able to override the second format field");
        }
    }

    let it = PostprocRowIter {
        lines: fbuf.lines(),
        fmt: fformat,
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

#[derive(Debug, Clone)]
pub struct PostprocFileHeader {
    pub nhead: usize,
    pub ncol: usize,
    pub nrec: usize,
    pub naux: usize,
    pub program_versions: HashMap<String, ProgramVersion>,
    pub extra_lines: Vec<String>,
    pub missing_value: f64,
    pub fformat: FortFormat,
    pub column_names: Vec<String>,
}

impl PostprocFileHeader {
    pub fn read_postproc_file_header<F: BufRead>(file: &mut FileBuf<F>) -> error_stack::Result<Self, HeaderError> {
        let sizes = utils::get_file_shape_info(file, 4)?;
        // Since get_file_shape_info guarantees the length of sizes, this is safe.
        let nhead = sizes[0];
        let ncol = sizes[1];
        let nrec = sizes[2];
        let naux = sizes[3];

        let mut program_versions = HashMap::new();
        let mut extra_lines = vec![];
        let mut missing_value = None;
        let mut fformat = None;
        let mut column_names = None;

        let mut iline = 1;
        while iline < nhead {
            iline += 1;
            let line = file.read_header_line()?;
            if line.starts_with("missing:") {
                let (_, v) = line.split_once(":").unwrap();
                missing_value = Some(
                    v.trim().parse::<f64>().change_context_lazy(|| HeaderError::ParseError { 
                        location: FileLocation::new(Some(file.path.clone()), Some(iline+1), Some(line.clone())),
                        cause: "Could not parse missing value into a float".into()
                    })?
                );
            } else if line.starts_with("format:") {
                let (_, fmt) = line.split_once(":").unwrap();
                fformat = Some(
                    FortFormat::parse(fmt).map_err(|e| HeaderError::ParseError { 
                        location: FileLocation::new(Some(file.path.clone()), Some(iline+1), Some(line.clone())),
                        cause: format!("Could not parse the format line: {e}")
                    })?
                );
            } else if iline == nhead {
                column_names = Some(line.split_whitespace().map(|s| s.to_string()).collect_vec());
            } else {
                if let Ok(pv) = ProgramVersion::from_str(&line) {
                    program_versions.insert(pv.program.clone(), pv);
                } else {
                    extra_lines.push(line.trim_end().to_string());
                }
            }
        }
        
        let missing_value = missing_value.ok_or_else(|| HeaderError::ParseError { 
            location: file.path.as_path().into(), 
            cause: "The 'missing:' line was not found".into()
        })?;

        let fformat = fformat.ok_or_else(|| HeaderError::ParseError { 
            location: file.path.as_path().into(), 
            cause: "The 'format:' line was not found".into()
        })?;

        let column_names = column_names.ok_or_else(|| HeaderError::ParseError { 
            location: file.path.as_path().into(),
            cause: "The column names were not found".into()
        })?;

        Ok(Self { nhead, ncol, nrec, naux, program_versions, missing_value, extra_lines, fformat, column_names })
    }

    fn aux_varnames(&self) -> &[String] {
        &self.column_names[..self.naux]
    }

    fn gas_varnames(&self) -> &[String] {
        &self.column_names[self.naux..]
    }
}


pub struct PostprocFile {
    buffer: FileBuf<BufReader<std::fs::File>>,
    header: PostprocFileHeader
}

impl PostprocFile {
    pub fn open(p: &Path) -> Result<Self, GggError> {
        let mut buffer = FileBuf::open(p)?;
        let header = PostprocFileHeader::read_postproc_file_header(&mut buffer)
            .map_err(|e| GggError::HeaderError(e.current_context().to_owned()))?;
        Ok(Self { buffer, header })
    }

    pub fn next_data_record(&mut self) -> Result<PostprocData, GggError> {
        let line = self.buffer.read_data_line()?;
        
        // If I try to directly deserialize to PostprocData, even with #[serde(borrow)]
        // on the data field, I can't return the record because Serde thinks the borrowed
        // value comes from `line`, even though it's the column names from this struct.
        // So we go through an intermediate struct to work around that and clearly define
        // that the borrowed fields are the header's column names.
        let field_names = self.header.column_names.iter().map(|s| s.as_str()).collect_vec();
        let rec: PpDataTmp = fortformat::de::from_str_with_fields(
            &line,
            &self.header.fformat,
            &field_names
        ).map_err(|e| GggError::DataError { 
            path: self.buffer.path.clone(), 
            cause: format!("Could not deserialize next line: {e}")
        })?;

        let aux_it = self.header.aux_varnames()[1..].iter()
            .map(|k| (k.as_str(), *rec.data.get(k.as_str()).unwrap()));
        let aux_data = HashMap::from_iter(aux_it);

        let gas_it = self.header.gas_varnames().iter()
            .map(|k| (k.as_str(), *rec.data.get(k.as_str()).unwrap()));
        let gas_data = HashMap::from_iter(gas_it);
        Ok(PostprocData { spectrum: rec.spectrum, aux_data, gas_data })
    }

    pub fn aux_varnames(&self) -> &[String] {
        self.header.aux_varnames()
    }

    pub fn gas_varnames(&self) -> &[String] {
        self.header.gas_varnames()
    }
}

#[derive(Debug, Deserialize)]
struct PpDataTmp<'f> {
    spectrum: String,
    #[serde(flatten, borrow)]
    data: HashMap<&'f str, f64>,
}

#[derive(Debug, PartialEq)]
pub struct PostprocData<'f> {
    spectrum: String,
    aux_data: HashMap<&'f str, f64>,
    gas_data: HashMap<&'f str, f64>,
}

impl<'f> approx::AbsDiffEq for PostprocData<'f> {
    type Epsilon = f64;

    fn default_epsilon() -> Self::Epsilon {
        f64::default_epsilon()
    }

    fn abs_diff_eq(&self, other: &Self, epsilon: Self::Epsilon) -> bool {
        if self.spectrum != other.spectrum { return false; }
        if self.aux_data.keys().any(|&k| !other.aux_data.contains_key(k)) { return false; }
        if other.aux_data.keys().any(|&k| !self.aux_data.contains_key(k)) { return false; }
        if self.gas_data.keys().any(|&k| !other.gas_data.contains_key(k)) { return false; }
        if other.gas_data.keys().any(|&k| !self.gas_data.contains_key(k)) { return false; }

        for (&k, v) in self.aux_data.iter() {
            let v2 = other.aux_data.get(k).unwrap();
            if f64::abs_diff_ne(v, v2, epsilon) { return false; }
        }

        for (&k, v) in self.gas_data.iter() {
            let v2 = other.gas_data.get(k).unwrap();
            if f64::abs_diff_ne(v, v2, epsilon) { return false; }
        }

        true
    }
}



#[cfg(test)]
mod tests {
    use rstest::{rstest,fixture};
    use crate::test_utils::test_data_dir;
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

    #[fixture]
    fn benchmark_aia_file() -> PathBuf {
        test_data_dir().join("pa_ggg_benchmark.vav.ada.aia")
    }

    #[rstest]
    fn test_read_aia_header(benchmark_aia_file: PathBuf) {
        fn test_correction(dict: &HashMap<String, (f64, f64)>, key: &str, value: f64, uncertainty: f64) {
            let v = dict.get(key);
            assert!(v.is_some(), "{key} missing from corrections hash map");
            let (fval, func) = v.unwrap();
            approx::assert_abs_diff_eq!(fval, &value);
            approx::assert_abs_diff_eq!(func, &uncertainty);
        }

        let f = PostprocFile::open(&benchmark_aia_file).unwrap();
        // Only need to test the shape, program versions, corrections, and missing value. The column names
        // and fortran format will be implicitly tested by the data read test.
        assert_eq!(f.header.nhead, 32);
        assert_eq!(f.header.ncol, 55);
        assert_eq!(f.header.nrec, 4);
        assert_eq!(f.header.naux, 25);

        let ex_pgrm_vers = HashMap::from([
            ("apply_insitu_correction".to_string(), ProgramVersion{
                program: "apply_insitu_correction".to_string(),
                version: "Version 1.38".to_string(),
                date: "2020-03-20".to_string(),
                authors: "GCT,JLL".to_string()
            }),
            ("average_results".to_string(), ProgramVersion{
                program: "average_results".to_string(),
                version: "Version 1.36".to_string(),
                date: "2020-06-04".to_string(),
                authors: "GCT,JLL".to_string()
            }),
            ("apply_airmass_correction".to_string(), ProgramVersion{
                program: "apply_airmass_correction".to_string(),
                version: "Version 1.36".to_string(),
                date: "2020-06-08".to_string(),
                authors: "GCT,JLL".to_string()
            }),
            ("collate_results".to_string(), ProgramVersion{
                program: "collate_results".to_string(),
                version: "Version 2.07".to_string(),
                date: "2020-04-09".to_string(),
                authors: "GCT,JLL".to_string()
            }),
            ("GFIT".to_string(), ProgramVersion{
                program: "GFIT".to_string(),
                version: "Version 5.28".to_string(),
                date: "2020-04-24".to_string(),
                authors: "GCT".to_string()
            }),
            ("GSETUP".to_string(), ProgramVersion{
                program: "GSETUP".to_string(),
                version: "Version 4.60".to_string(),
                date: "2020-04-03".to_string(),
                authors: "GCT".to_string()
            }),
        ]);

        assert_eq!(f.header.program_versions, ex_pgrm_vers);

        // test_correction(&f.header.correction_factors["Airmass-Independent/In-Situ"], "xco2", 0.9898, 0.0010);
        // test_correction(&f.header.correction_factors["Airmass-Independent/In-Situ"], "xch4", 0.9765, 0.0020);
        // test_correction(&f.header.correction_factors["Airmass-Independent/In-Situ"], "xn2o", 0.9638, 0.0100);
        // test_correction(&f.header.correction_factors["Airmass-Independent/In-Situ"], "xco",  1.0672, 0.0200);
        // test_correction(&f.header.correction_factors["Airmass-Independent/In-Situ"], "xh2o", 1.0183, 0.0100);
        // test_correction(&f.header.correction_factors["Airmass-Independent/In-Situ"], "xluft", 1.000, 0.0000);

        // test_correction(&f.header.correction_factors["Airmass-Dependent"], "xco2_6220", -0.0068, 0.0050);
        // test_correction(&f.header.correction_factors["Airmass-Dependent"], "xco2_6339", -0.0068, 0.0050);
        // test_correction(&f.header.correction_factors["Airmass-Dependent"], "xlco2_4852", 0.0000, 0.0000);
        // test_correction(&f.header.correction_factors["Airmass-Dependent"], "xwco2_6073", 0.0000, 0.0000);
        // test_correction(&f.header.correction_factors["Airmass-Dependent"], "xwco2_6500", 0.0000, 0.0000);
        // test_correction(&f.header.correction_factors["Airmass-Dependent"], "xch4_5938", 0.0053, 0.0080);
        // test_correction(&f.header.correction_factors["Airmass-Dependent"], "xch4_6002", 0.0053, 0.0080);
        // test_correction(&f.header.correction_factors["Airmass-Dependent"], "xch4_6076", 0.0053, 0.0080);
        // test_correction(&f.header.correction_factors["Airmass-Dependent"], "xn2o_4395", 0.0039, 0.0100);
        // test_correction(&f.header.correction_factors["Airmass-Dependent"], "xn2o_4430", 0.0039, 0.0100);
        // test_correction(&f.header.correction_factors["Airmass-Dependent"], "xn2o_4719", 0.0039, 0.0100);
        // test_correction(&f.header.correction_factors["Airmass-Dependent"], "xco_4233", -0.0483, 0.1000);
        // test_correction(&f.header.correction_factors["Airmass-Dependent"], "xco_4290", -0.0483, 0.1000);
        // test_correction(&f.header.correction_factors["Airmass-Dependent"], "xluft_6146", -0.0000, 0.0000);

        approx::assert_abs_diff_eq!(f.header.missing_value, 9.8755E+35);

    }

    #[rstest]
    fn test_read_aia_data(benchmark_aia_file: PathBuf) {
        let ex_rec_1 = PostprocData {
            spectrum: "pa20040721saaaaa.043".to_string(),
            aux_data: HashMap::from([("year", 2004.55698948), ("day", 203.85815), ("hour", 20.59560), ("run", 1.0), ("lat", 45.945), ("long", -90.27300), ("zobs", 0.44200),
                                     ("zmin", 0.46100), ("asza", 39.68400), ("azim", 242.28101), ("osds", 0.13800), ("opd", 45.02000), ("fovi", 0.00240), ("amal", 0.0),
                                     ("graw", 0.00753), ("tins", 30.3), ("pins", 0.9), ("tout", 29.1), ("pout", 950.70001), ("hout", 62.8), ("sia", 207.5), ("fvsi", 0.00720),
                                     ("wspd", 1.7), ("wdir", 125.0)]),
            gas_data: HashMap::from([("xluft", 9.9472E-01), ("xluft_error", 7.9155E-03), ("xhf", 6.5608E-11), ("xhf_error", 9.0112E-12), ("xh2o", 6.2161E-03), ("xh2o_error", 5.1153E-05),
                                     ("xth2o", 6.2822E-03), ("xth2o_error", 5.6500E-05), ("xhdo", 5.3816E-03), ("xhdo_error", 5.1541E-05), ("xco", 8.4321E-08), ("xco_error", 1.5600E-09),
                                     ("xn2o", 3.0876E-07), ("xn2o_error", 3.0009E-09), ("xch4", 1.7782E-06), ("xch4_error", 1.6033E-08), ("xlco2", 3.7259E-04), ("xlco2_error", 4.4156E-06),
                                     ("xzco2", 3.7233E-04), ("xzco2_error", 3.7511E-06), ("xfco2", 3.7546E-04), ("xfco2_error", 4.6550E-06), ("xwco2", 3.7637E-04), ("xwco2_error", 3.8073E-06),
                                     ("xco2", 3.8072E-04), ("xco2_error", 3.4998E-06), ("xo2", 2.0950E-01), ("xo2_error", 1.6671E-03), ("xhcl", 1.6840E-10), ("xhcl_error", 1.4365E-12)])
        };
        let mut f = PostprocFile::open(&benchmark_aia_file).unwrap();
        let rec = f.next_data_record().unwrap();
        approx::assert_abs_diff_eq!(rec, ex_rec_1)
    }
}