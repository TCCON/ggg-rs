use std::{collections::HashMap, fmt::Display, path::{Path, PathBuf}, str::FromStr};

use chrono::{DateTime, Utc};
use error_stack::ResultExt;
use ggg_rs::{cit_spectrum_name::{CitSpectrumName, NoDetectorSpecName}, readers::runlogs::FallibleRunlog};
use indicatif::ProgressBar;
use ndarray::Array1;

use crate::{dimensions::TIME_DIM_NAME, errors::{InputError, WriteError}, interface::{ConcreteVarToBe, DataProvider, SpectrumIndexer, StdDataGroup}};

static DIMS_REQ: [&'static str; 1] = [TIME_DIM_NAME];

pub(crate) struct RunlogProvider {
    runlog_path: PathBuf,
    times: Array1<DateTime<Utc>>,
}

impl RunlogProvider {
    pub(crate) fn new(runlog: PathBuf) -> error_stack::Result<(Self, SpectrumIndexer), InputError> {
        if !runlog.exists() {
            return Err(InputError::file_not_found(runlog).into())
        }

        let (spectrum_indexer, times) = Self::get_times_and_indexer(&runlog)?;
        Ok((Self{ runlog_path: runlog, times }, spectrum_indexer))
    }

    fn get_times_and_indexer(runlog: &Path) -> error_stack::Result<(SpectrumIndexer, Array1<DateTime<Utc>>), InputError> {
        let runlog_handle = FallibleRunlog::open(runlog)
            .change_context_lazy(|| InputError::error_reading_file(runlog))?;

        // Get the number of lines in the header so that we can give the correct line number in errors
        let nhead = runlog_handle.header().nhead;

        let mut curr_spec_index: usize = 0;
        let mut spec_inds = HashMap::new();
        let mut times = vec![];
        let mut last_nodet_spec = None;

        for (i_data_line, res) in runlog_handle.into_line_iter() {
            let line_num = i_data_line + nhead + 1; // want line number in reports to be 1-based

            // Handle the case where reading & parsing the next line of the runlog fails
            let rl_rec = res.change_context_lazy(|| InputError::error_reading_at_line(runlog, line_num))?;

            // If the spectrum was already encountered, this is an invalid runlog. Spectrum names must be unique.
            if spec_inds.contains_key(&rl_rec.spectrum_name) {
                return Err(InputError::custom(format!(
                    "Spectrum '{}' at line {line_num} of runlog {} is a duplicate of a spectrum earlier in the file. Duplicate spectra are not allowed.",
                    rl_rec.spectrum_name,
                    runlog.display()
                )).into())
            }

            // We need information about the spectrum and ZPD time - make sure we can get that successfully
            let nodet_spectrum = CitSpectrumName::from_str(&rl_rec.spectrum_name)
                .map(NoDetectorSpecName::from)
                .change_context_lazy(|| InputError::error_reading_at_line(runlog, line_num))?;

            let zpd_time = rl_rec.zpd_time()
                .ok_or_else(|| InputError::custom(format!(
                    "Invalid ZPD time on line {line_num} of runlog {}", runlog.display()
                )))?;

            // We need to increment the spectrum index if this is a new observation. For TCCON, that means the
            // spectrum name differs from the previous, ignoring the detector character.
            if last_nodet_spec.as_ref().is_some_and(|last| last != &nodet_spectrum) {
                curr_spec_index += 1;
                last_nodet_spec = Some(nodet_spectrum);
                times.push(zpd_time);
            } else if last_nodet_spec.is_none() {
                last_nodet_spec = Some(nodet_spectrum);
                times.push(zpd_time);
            }

            // We always record the index for a spectrum; that way we can just use the string version of the
            // spectrum name and don't have to create no-detector spectrum names in the rest of the providers.
            spec_inds.insert(rl_rec.spectrum_name, curr_spec_index);
        }
        
        let indexer = SpectrumIndexer::new(spec_inds);
        let times = Array1::from_iter(times);
        Ok((indexer, times))
    }
}

impl DataProvider for RunlogProvider {
    fn dimension_lengths(&self) -> std::borrow::Cow<[(&'static str, usize)]> {
        let lengths = vec![(TIME_DIM_NAME, self.times.len())];
        std::borrow::Cow::Owned(lengths)
    }

    fn dimensions_required(&self) -> std::borrow::Cow<[&'static str]> {
        std::borrow::Cow::Borrowed(&DIMS_REQ)
    }
    
    fn write_data_to_nc(&self, _spec_indexer: &SpectrumIndexer, writer: &dyn crate::interface::GroupWriter, _pb: ProgressBar) -> error_stack::Result<(), WriteError> {
        // Unlike other providers, since the runlog sets the order of data, it doesn't need to use the
        // spectrum indexer to make sure the data are in the correct order.
        // Also, since we only have one variable, there's no benefit to using the "write multiple vars" writer method.
        let data = self.times.mapv(|dt| dt.timestamp());
        let mut times_var = ConcreteVarToBe::new(
            TIME_DIM_NAME, DIMS_REQ.to_vec(), data, "time", "seconds since 1970-01-01 00:00:00", &self.runlog_path
        ).map_err(|e| WriteError::from(e))?;
        times_var.add_attribute("calendar", "gregorian");
        writer.write_variable(&times_var, &StdDataGroup::InGaAs)?;
        Ok(())
    }
}

impl Display for RunlogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "runlog")
    }
}