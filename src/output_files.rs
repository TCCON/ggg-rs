use std::collections::HashMap;
use std::io::BufReader;
use std::path::Path;
use std::{path::PathBuf, str::FromStr, sync::OnceLock, io::BufRead};

use approx::AbsDiffEq;
use error_stack::ResultExt;
use fortformat::format_specs::FortFormat;
use itertools::Itertools;
use serde::Deserialize;

use crate::error::{FileLocation, HeaderError};
use crate::utils::{get_file_shape_info, get_nhead_ncol, CommonHeader, FileBuf, GggError};


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

#[derive(Debug, Clone, PartialEq)]
pub struct ProgramVersion {
    program: String,
    version: String,
    date: String,
    authors: String,
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


#[derive(Debug, Clone)]
pub struct PostprocFileHeader {
    nhead: usize,
    ncol: usize,
    nrec: usize,
    naux: usize,
    program_versions: HashMap<String, ProgramVersion>,
    correction_factors: HashMap<String, HashMap<String, (f64, f64)>>,
    missing_value: f64,
    fformat: FortFormat,
    column_names: Vec<String>,
}

impl PostprocFileHeader {
    pub fn read_postproc_file_header<F: BufRead>(file: &mut FileBuf<F>) -> error_stack::Result<Self, HeaderError> {
        let sizes = get_file_shape_info(file, 4)?;
        // Since get_file_shape_info guarantees the length of sizes, this is safe.
        let nhead = sizes[0];
        let ncol = sizes[1];
        let nrec = sizes[2];
        let naux = sizes[3];

        let mut program_versions = HashMap::new();
        let mut correction_factors = HashMap::new();
        let mut missing_value = None;
        let mut fformat = None;
        let mut column_names = None;

        let mut iline = 1;
        while iline < nhead {
            iline += 1;
            let line = file.read_header_line()?;
            if line.contains("Correction Factors") {
                let (key, cf_map) = parse_corr_fac_block(file, line, &mut iline)?;
                correction_factors.insert(key, cf_map);
            } else if line.starts_with("missing:") {
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
                dbg!((iline, nhead));
                let pv = ProgramVersion::from_str(&line).change_context_lazy(|| HeaderError::ParseError {
                    location: FileLocation::new(Some(file.path.clone()), Some(iline+1), Some(line.clone())),
                    cause: "Could not parse program version".into(),
                })?;

                program_versions.insert(pv.program.clone(), pv);
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

        Ok(Self { nhead, ncol, nrec, naux, program_versions, correction_factors, missing_value, fformat, column_names })
    }

    fn aux_varnames(&self) -> &[String] {
        &self.column_names[..self.naux]
    }

    fn data_varnames(&self) -> &[String] {
        &self.column_names[self.naux..]
    }
}

fn parse_corr_fac_block<F: BufRead>(file: &mut FileBuf<F>, first_line: String, iline: &mut usize) 
-> error_stack::Result<(String, HashMap<String, (f64, f64)>), HeaderError> {
    let (cf_name, cf_nums) = first_line.split_once(":")
        .ok_or_else(|| HeaderError::ParseError { 
            location: FileLocation::new(Some(file.path.clone()), Some(*iline+1), Some(first_line.clone())), 
            cause: "Line containing 'Correction Factors' must have a colon in it".to_string()
        })?;

    let s = cf_nums.split_whitespace().nth(0)
        .ok_or_else(|| HeaderError::ParseError { 
            location: FileLocation::new(Some(file.path.clone()), Some(*iline+1), Some(first_line.clone())),
            cause: "A corrections file line did not have at least one number after the colon".into()
        })?;

    let nfactor = s.parse::<usize>()
        .change_context_lazy(|| HeaderError::ParseError {
            location: FileLocation::new(Some(file.path.clone()), Some(*iline+1), Some(first_line.clone())),
            cause: "Could not parse first value after colon in correction factor line as an unsiged integer".into()
        })?;

    let mut cf_map = HashMap::new();
    for _ in 0..nfactor {
        let line = file.read_header_line()?;
        *iline += 1;
        if let Some((key, value, uncertainty)) = line.split_whitespace().collect_tuple() {
            let value = value.parse::<f64>()
            .change_context_lazy(|| HeaderError::ParseError { 
                location: FileLocation::new(Some(file.path.clone()), Some(*iline+1), Some(line.clone())),
                cause: format!("Could not parse the {key} value into a float"),
            })?;

            let uncertainty = uncertainty.parse::<f64>()
            .change_context_lazy(|| HeaderError::ParseError { 
                location: FileLocation::new(Some(file.path.clone()), Some(*iline+1), Some(line.clone())),
                cause: format!("Could not parse the {key} uncertainty into a float"),
            })?;

            let key = key.to_string();
            cf_map.insert(key, (value, uncertainty));
        } else {
            let n = line.split_whitespace().count();
            return Err(HeaderError::ParseError {
                location: FileLocation::new(Some(file.path.clone()), Some(*iline+1), Some(line)),
                cause: format!("A line with correction factor values should have 3 whitespace separated values, this one had {n}.")
            }.into())
        }
    }

    let cf_name = cf_name.split("Correction").nth(0)
        .expect("Correction factor header line should have 'Correction' in it")
        .trim()
        .to_string();
    Ok((cf_name, cf_map))
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

        let gas_it = self.header.data_varnames().iter()
            .map(|k| (k.as_str(), *rec.data.get(k.as_str()).unwrap()));
        let gas_data = HashMap::from_iter(gas_it);
        Ok(PostprocData { spectrum: rec.spectrum, aux_data, gas_data })
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
    use rstest::{fixture, rstest};
    use crate::test_utils::test_data_dir;
    use super::*;

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

        test_correction(&f.header.correction_factors["Airmass-Independent/In-Situ"], "xco2", 0.9898, 0.0010);
        test_correction(&f.header.correction_factors["Airmass-Independent/In-Situ"], "xch4", 0.9765, 0.0020);
        test_correction(&f.header.correction_factors["Airmass-Independent/In-Situ"], "xn2o", 0.9638, 0.0100);
        test_correction(&f.header.correction_factors["Airmass-Independent/In-Situ"], "xco",  1.0672, 0.0200);
        test_correction(&f.header.correction_factors["Airmass-Independent/In-Situ"], "xh2o", 1.0183, 0.0100);
        test_correction(&f.header.correction_factors["Airmass-Independent/In-Situ"], "xluft", 1.000, 0.0000);

        test_correction(&f.header.correction_factors["Airmass-Dependent"], "xco2_6220", -0.0068, 0.0050);
        test_correction(&f.header.correction_factors["Airmass-Dependent"], "xco2_6339", -0.0068, 0.0050);
        test_correction(&f.header.correction_factors["Airmass-Dependent"], "xlco2_4852", 0.0000, 0.0000);
        test_correction(&f.header.correction_factors["Airmass-Dependent"], "xwco2_6073", 0.0000, 0.0000);
        test_correction(&f.header.correction_factors["Airmass-Dependent"], "xwco2_6500", 0.0000, 0.0000);
        test_correction(&f.header.correction_factors["Airmass-Dependent"], "xch4_5938", 0.0053, 0.0080);
        test_correction(&f.header.correction_factors["Airmass-Dependent"], "xch4_6002", 0.0053, 0.0080);
        test_correction(&f.header.correction_factors["Airmass-Dependent"], "xch4_6076", 0.0053, 0.0080);
        test_correction(&f.header.correction_factors["Airmass-Dependent"], "xn2o_4395", 0.0039, 0.0100);
        test_correction(&f.header.correction_factors["Airmass-Dependent"], "xn2o_4430", 0.0039, 0.0100);
        test_correction(&f.header.correction_factors["Airmass-Dependent"], "xn2o_4719", 0.0039, 0.0100);
        test_correction(&f.header.correction_factors["Airmass-Dependent"], "xco_4233", -0.0483, 0.1000);
        test_correction(&f.header.correction_factors["Airmass-Dependent"], "xco_4290", -0.0483, 0.1000);
        test_correction(&f.header.correction_factors["Airmass-Dependent"], "xluft_6146", -0.0000, 0.0000);

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