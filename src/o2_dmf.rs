use std::{collections::HashMap, fmt::Debug, io::BufRead, path::{Path, PathBuf}};

use chrono::Datelike;
use clap::Args;
use error_stack::ResultExt;
use crate::readers::col_files::get_runlog_from_col_files;
use crate::readers::runlogs::FallibleRunlog;
use itertools::Itertools;
use nalgebra::{self, OMatrix, OVector};

pub const DEFAULT_O2_DMF: f64 = 0.2095;

// ----------------- //
// Generic interface //
// ----------------- //

#[derive(Debug, Clone, thiserror::Error)]
pub enum O2DmfError {
    #[error("Could not find O2 DMF for spectrum {specname}: {reason}")]
    SpectrumNotFound{specname: String, reason: String},
    #[error("Could not find required input file {}", .0.display())]
    InputNotFound(PathBuf),
    #[error("{0}")]
    Custom(String),
}

impl O2DmfError {
    pub(crate) fn spectrum_not_found<S: ToString, R: ToString>(specname: S, reason: R) -> Self {
        Self::SpectrumNotFound { specname: specname.to_string(), reason: reason.to_string() }
    }

    pub(crate) fn input_not_found<P: ToOwned<Owned = PathBuf>>(file: P) -> Self {
        Self::InputNotFound(file.to_owned())
    }

    pub(crate) fn custom<S: ToString>(msg: S) -> Self {
        Self::Custom(msg.to_string())
    }
}

pub trait O2DmfProvider: Debug {
    fn header_line(&self) -> String;
    fn o2_dmf(&self, spectrum_name: &str) -> error_stack::Result<f64, O2DmfError>;
}

// ----------------------------------------------------------------- //
// GGG2020 and before approach: single O2 mole fraction for all data //
// ----------------------------------------------------------------- //

#[derive(Debug)]
pub(crate) struct FixedO2Dmf {
    o2_dmf: f64
}

impl FixedO2Dmf {
    pub(crate) fn new(o2_dmf: f64) -> Self {
        Self { o2_dmf }
    }
}

impl O2DmfProvider for FixedO2Dmf {
    fn header_line(&self) -> String {
        format!("O2 DMF source: fixed {:.6}", self.o2_dmf)
    }

    fn o2_dmf(&self, _spectrum_name: &str) -> error_stack::Result<f64, O2DmfError> {
        Ok(self.o2_dmf)
    }
}

// --------------------------------------------------------------------------- //
// GGG2020.1 AICF support: read and interpolate f(O2) from a single input file //
// --------------------------------------------------------------------------- //

#[derive(Debug)]
pub(crate) struct O2DmfTimeseries {
    o2_file: PathBuf,
    years: Vec<i32>,
    o2_dmfs: Vec<f64>,
    runlog_timestamps: HashMap<String, chrono::DateTime<chrono::Utc>>,
    delay_years: i32,
    extrap_basis_years: i32,
}

impl O2DmfTimeseries {
    pub(crate) fn new(o2_file: PathBuf, run_dir: &Path) -> error_stack::Result<Self, O2DmfError> {
        // Handle reading the O2 file first - this will go quickly, so if there's an error here,
        // no sense in making the user wait until the runlog finishes being read.
        let (years, o2_dmfs) = Self::read_o2_dmf_file(&o2_file)?;

        // Now handle reading the runlog - all we need is the mapping of spectrum names to their times.
        let runlog_timestamps = Self::read_runlog(run_dir)?;
        
        Ok(Self { o2_file, years, o2_dmfs, runlog_timestamps, delay_years: 2, extrap_basis_years: 5 })
    }

    fn read_o2_dmf_file(o2_file: &Path) -> error_stack::Result<(Vec<i32>, Vec<f64>), O2DmfError> {
        let f = std::fs::File::open(o2_file).change_context_lazy(|| O2DmfError::input_not_found(o2_file.to_path_buf()))?;
        let f = std::io::BufReader::new(f);
        let mut years = vec![];
        let mut o2_dmfs = vec![];

        // This file won't be that lon#g, just read the non-comment lines into memory.
        // It'll make the rest of the logic easier.
        let o2_lines: Vec<String> = f.lines().filter_map(|line| {
            if line.as_ref().is_ok_and(|l| l.trim().starts_with("#")) {
                None
            } else {
                Some(line)
            }
        }).try_collect()
        .change_context_lazy(|| O2DmfError::custom(
            format!("error reading {}", o2_file.display())
        ))?;

        let colname_line = o2_lines.get(0).ok_or_else(|| O2DmfError::custom(format!(
            "O2 file {} has no non-comment lines", o2_file.display()
        )))?;
        let colnames = colname_line.trim().split_ascii_whitespace().map(|s| s.to_string()).collect_vec();
        
        let year_idx = colnames.iter().position(|s| s == "year")
            .ok_or_else(|| O2DmfError::custom(format!(
                "could not find 'year' column in {}", o2_file.display()
            )))?;

        let o2_idx = colnames.iter().position(|s| s == "fo2")
            .ok_or_else(|| O2DmfError::custom(format!(
                "could not find 'fo2' column in {}", o2_file.display()
            )))?;

        for (iline, line) in o2_lines[1..].into_iter().enumerate() {
            let parts = line.split_ascii_whitespace().collect_vec();

            // Parse the year into a datetime - because the file gives year averages, we treat them
            // as midpoints in the year (July 1). This must match the logic that ginput uses.
            let year_str = *parts.get(year_idx).ok_or_else(|| O2DmfError::custom(format!(
                "data line {} in {} does not contain year", iline+1, o2_file.display()
            )))?;
            let year = year_str.parse::<i32>().map_err(|_| O2DmfError::custom(format!(
                "could not parse year value in data line {} of {}, got the string '{year_str}' for year", iline+1, o2_file.display()
            )))?;
            // let dt = chrono::NaiveDate::from_ymd_opt(year, 7, 1).unwrap()
            //     .and_hms_opt(0, 0, 0).unwrap()
            //     .and_utc();
            years.push(year);

            // Parse the O2 DMF - this should be much easier
            let dmf_str = *parts.get(o2_idx).ok_or_else(|| O2DmfError::custom(format!(
                "data line {} in {} does not contain fo2", iline+1, o2_file.display()
            )))?;
            let dmf = dmf_str.parse::<f64>().map_err(|_| O2DmfError::custom(format!(
                "could not parse fo2 value in data line {} of {}, got the string '{dmf_str}' for fo2", iline+1, o2_file.display()
            )))?;
            o2_dmfs.push(dmf);
        }

        Ok((years, o2_dmfs))
    }

    fn read_runlog(run_dir: &Path) -> error_stack::Result<HashMap<String, chrono::DateTime<chrono::Utc>>, O2DmfError> {
        let multiggg_file = run_dir.join("multiggg.sh");
        if !multiggg_file.exists() {
            return Err(O2DmfError::input_not_found(multiggg_file).into());
        }

        let runlog_path = get_runlog_from_col_files(&multiggg_file, run_dir)
            .change_context_lazy(|| O2DmfError::custom("could not get a consistent runlog from the .col file headers"))?;

        let runlog = FallibleRunlog::open(&runlog_path)
            .change_context_lazy(|| O2DmfError::custom("error opening runlog"))?;

        let mut runlog_timestamps = HashMap::new();

        for (irec, record) in runlog.into_iter().enumerate() {
            let record = record.change_context_lazy(|| O2DmfError::custom(format!(
                "error occurred reading data record {} in runlog {}", irec+1, runlog_path.display()
            )))?;

            let specname = record.spectrum_name.clone();
            let ts = record.zpd_time().ok_or_else(|| O2DmfError::custom(format!(
                "could not get timestamp for spectrum {specname}"
            )))?;

            runlog_timestamps.insert(specname, ts);
        }

        Ok(runlog_timestamps)
    }

    fn interpolate_o2(&self, dt: &chrono::DateTime<chrono::Utc>) -> error_stack::Result<f64, O2DmfError> {
        let last_year_to_keep = dt.year() - self.delay_years;
        let ilast = self.years.iter().positions(|&y| y <= last_year_to_keep).last()
            .ok_or_else(|| O2DmfError::custom(format!(
                "need at least one year <= {last_year_to_keep} in the O2 mole fraction input file"
            )))?;
        
        let first_basis_year = last_year_to_keep - self.extrap_basis_years + 1;
        let ifirst = self.years.iter().positions(|&y| y >= first_basis_year).min()
            .ok_or_else(|| O2DmfError::custom(format!(
                "need at least one year >= {first_basis_year} in the O2 mole fraction input file"
            )))?;

        if ifirst > ilast {
            return Err(O2DmfError::custom(format!(
                "no years between {first_basis_year} and {last_year_to_keep} in the O2 mole fraction input file"
            )).into());
        }

        if self.years[ilast] != last_year_to_keep {
            return Err(O2DmfError::custom(format!(
                "must have {last_year_to_keep} in the O2 mole fraction input file"
            )).into());
        }

        // Error checking done now, so convert the years and datetimes to timestamps and fit a
        // line to the last N years to predict the O2 mole fraction. 
        let nyear = ilast - ifirst + 1;

        // This should produce a vector with [ts, 1, ts, 1, ts, 1, ...] which can be used to
        // make a Nx2 matrix with the timestamps in the first column and 1 in the second (to
        // fit the intercept).
        let input_ts = self.years[ifirst..=ilast].iter()
            .map(|&y| {
                chrono::NaiveDate::from_ymd_opt(y, 7, 1).unwrap()
                    .and_hms_opt(0, 0, 0).unwrap()
                    .and_utc()
                    .timestamp() as f64
            }).interleave_shortest(std::iter::repeat(1.0));

        let ts_matrix = OMatrix::<f64, nalgebra::Dyn, nalgebra::U2>::from_row_iterator(nyear, input_ts);
        let dmf_vec = OVector::<f64, nalgebra::Dyn>::from_row_slice(&self.o2_dmfs[ifirst..=ilast]);

        let epsilon = 1e-15; // since the lstsq example used 1e-14 for values of order 1 and the O2 DMFs are of order 0.1, I went one OoM down
        let res = lstsq::lstsq(&ts_matrix, &dmf_vec, epsilon)
            .map_err(|e| O2DmfError::custom(format!(
                "error fitting O2 DMF trend: {e}"
            )))?;

        let tgt_ts = dt.timestamp() as f64;
        let tgt_dmf = res.solution[0] * tgt_ts + res.solution[1];
        Ok(tgt_dmf)
    }
}

impl O2DmfProvider for O2DmfTimeseries {
    fn header_line(&self) -> String {
        format!("O2 DMF source: interpolated from file {}", self.o2_file.display())    
    }

    fn o2_dmf(&self, spectrum_name: &str) -> error_stack::Result<f64, O2DmfError> {
        let dt = self.runlog_timestamps.get(spectrum_name)
            .ok_or_else(|| O2DmfError::spectrum_not_found(spectrum_name, "spectrum not found in the runlog"))?;

        self.interpolate_o2(dt)
    }
}

#[derive(Debug, Args)]
pub struct O2DmfCli {
    /// If time-varying O2 mean mole fractions are not available in the
    /// .vmr files or as a list file in --o2-dmf-file, provide a fixed
    /// mole fraction to use for all spectra here. The default, if none
    /// of the option to specify O2 DMFs are given, is 0.2095.
    #[clap(long, conflicts_with = "o2_dmf_file")]
    pub fixed_o2_dmf: Option<f64>,

    /// If time-varying O2 mean mole fractions are not present in the
    /// .vmr files, you can instead provide them as a list file generated
    /// by ginput. This must be a space-separated file that has two columns: the UTC
    /// datetime on the time resolution of the priors (e.g. 3 hours) and
    /// the O2 mean dry mole fraction.
    #[clap(long)]
    pub o2_dmf_file: Option<PathBuf>,
}

pub fn make_boxed_o2_dmf_provider(clargs: &O2DmfCli, run_dir: &Path) -> error_stack::Result<Box<dyn O2DmfProvider>, O2DmfError> {
    if let Some(o2_file) = &clargs.o2_dmf_file {
        let provider = O2DmfTimeseries::new(o2_file.to_path_buf(), &run_dir)?;
        return Ok(Box::new(provider));
    }

    // If no time varying information is provided, fall back to a static
    // DMF, and if the user didn't give that, use the old GGG2020 and earlier
    // default.
    let dmf = clargs.fixed_o2_dmf.unwrap_or(DEFAULT_O2_DMF);
    let provider = FixedO2Dmf::new(dmf);
    Ok(Box::new(provider))    
}