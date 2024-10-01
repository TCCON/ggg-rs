use std::{collections::HashMap, fmt::Debug, io::BufRead, path::{Path, PathBuf}};

use error_stack::ResultExt;
use ggg_rs::output_files::get_runlog_from_col_files;
use ggg_rs::runlogs::FallibleRunlog;
use itertools::Itertools;

// ----------------- //
// Generic interface //
// ----------------- //

#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum O2DmfError {
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

pub(crate) trait O2DmfProvider: Debug {
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
        format!("fixed {:.6}", self.o2_dmf)
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
    timestamps: Vec<chrono::DateTime<chrono::Utc>>,
    o2_dmfs: Vec<f64>,
    runlog_timestamps: HashMap<String, chrono::DateTime<chrono::Utc>>
}

impl O2DmfTimeseries {
    pub(crate) fn new(o2_file: PathBuf, run_dir: &Path) -> error_stack::Result<Self, O2DmfError> {
        // Handle reading the O2 file first - this will go quickly, so if there's an error here,
        // no sense in making the user wait until the runlog finishes being read.
        let (timestamps, o2_dmfs) = Self::read_o2_dmf_file(&o2_file)?;

        // Now handle reading the runlog - all we need is the mapping of spectrum names to their times.
        let runlog_timestamps = Self::read_runlog(run_dir)?;
        
        Ok(Self { o2_file, timestamps, o2_dmfs, runlog_timestamps })
    }

    fn read_o2_dmf_file(o2_file: &Path) -> error_stack::Result<(Vec<chrono::DateTime<chrono::Utc>>, Vec<f64>), O2DmfError> {
        let f = std::fs::File::open(o2_file).change_context_lazy(|| O2DmfError::input_not_found(o2_file.to_path_buf()))?;
        let f = std::io::BufReader::new(f);
        let mut timestamps = vec![];
        let mut o2_dmfs = vec![];

        // This file won't be that long, just read the non-comment lines into memory.
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
            let dt = chrono::NaiveDate::from_ymd_opt(year, 7, 1).unwrap()
                .and_hms_opt(0, 0, 0).unwrap()
                .and_utc();
            timestamps.push(dt);

            // Parse the O2 DMF - this should be much easier
            let dmf_str = *parts.get(o2_idx).ok_or_else(|| O2DmfError::custom(format!(
                "data line {} in {} does not contain fo2", iline+1, o2_file.display()
            )))?;
            let dmf = dmf_str.parse::<f64>().map_err(|_| O2DmfError::custom(format!(
                "could not parse fo2 value in data line {} of {}, got the string '{dmf_str}' for fo2", iline+1, o2_file.display()
            )))?;
            o2_dmfs.push(dmf);
        }

        Ok((timestamps, o2_dmfs))
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

    fn interpolate_o2(&self, ifirst: usize, dt: &chrono::DateTime<chrono::Utc>, from_second_term: bool) -> f64 {
        let ts = dt.timestamp() as f64;
        let ts1 = self.timestamps[ifirst].timestamp() as f64;
        let ts2 = self.timestamps[ifirst+1].timestamp() as f64;
        let dmf1 = self.o2_dmfs[ifirst];
        let dmf2 = self.o2_dmfs[ifirst+1];
        let slope = (dmf2 - dmf1) / (ts2 - ts1);

        if from_second_term {
            dmf2 + slope * (ts - ts2)
        } else {
            dmf1 + slope * (ts - ts1)
        }
    }
}

impl O2DmfProvider for O2DmfTimeseries {
    fn header_line(&self) -> String {
        format!("interpolated from file {}", self.o2_file.display())    
    }

    fn o2_dmf(&self, spectrum_name: &str) -> error_stack::Result<f64, O2DmfError> {
        let dt = self.runlog_timestamps.get(spectrum_name)
            .ok_or_else(|| O2DmfError::spectrum_not_found(spectrum_name, "spectrum not found in the runlog"))?;

        let imax = self.timestamps.len() - 1;
        let idx_before = self.timestamps.iter().positions(|v| v <= dt).last();
        let interp_dmf = if idx_before.is_some_and(|i| i == imax) {
            // dt is after the last O2 timestamp, so need to extrapolate
            self.interpolate_o2(idx_before.unwrap()-1, dt, true)
        } else if idx_before.is_some() {
            // dt is between two timestamps
            self.interpolate_o2(idx_before.unwrap(), dt, false)
        } else {
            // dt is before the first timestamp
            self.interpolate_o2(0, dt, false)
        };
        
        Ok(interp_dmf)
    }
}