use indexmap::IndexMap;

use crate::utils::GggError;
use std::fmt::Write;

use super::WindowGrouper;

pub struct FrequencyWindowGrouper {
    ranges: Vec<FrequencyRange>,
}

static WINDOW_RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();

impl FrequencyWindowGrouper {
    pub fn new(ranges: Vec<FrequencyRange>) -> Self {
        Self { ranges }
    }

    fn find_group(&self, window_freq: f64) -> Result<&FrequencyRange, GggError> {
        for r in self.ranges.iter() {
            if r.min_freq <= window_freq && window_freq < r.max_freq {
                return Ok(r);
            }
        }
        return Err(self.error_no_range(window_freq));
    }

    fn error_no_range(&self, window_freq: f64) -> GggError {
        let mut msg = format!("Window centered on {window_freq:.2} did not match any of the defined frequency ranges: ");
        for (i, r) in self.ranges.iter().enumerate() {
            if i > 0 {
                write!(&mut msg, ", ").expect("Should be able to write to a string");
            }
            write!(&mut msg, "{:.2} to {:.2}", r.min_freq, r.max_freq)
                .expect("Should be able to write to a string");
        }

        GggError::Custom(msg)
    }
}

impl WindowGrouper for FrequencyWindowGrouper {
    fn group_windows<'c>(
        &self,
        header: &'c crate::readers::postproc_files::PostprocFileHeader,
    ) -> Result<super::AverageGroup<'c>, GggError> {
        let window_re = WINDOW_RE
            .get_or_init(|| regex::Regex::new(r"^\d+").expect("Window regex failed to compile"));

        let mut groups: IndexMap<String, Vec<&str>> = IndexMap::new();

        for colname in header.gas_varnames() {
            if colname.ends_with("error") {
                continue;
            }

            let (gas, window_str) = if let Some((g, w)) = colname.split_once("_") {
                // Some columns may have an "a", "b", etc. appended, just get the frequency.
                // This does mean that, e.g. "zco2_4852" and "zco2_4852a" would get averaged,
                // but that should be what the Fortran version does anyway.
                let m = window_re.find(w).ok_or_else(|| {
                    GggError::custom(format!(
                        "Failed to extract window frequency from '{w}' in column '{colname}'"
                    ))
                })?;
                (g, m.as_str())
            } else {
                return Err(GggError::custom(format!("Error parsing column '{colname}': expected gas and window separated by a single underscore")));
            };

            let window_freq: f64 = window_str.parse().map_err(|e| {
                GggError::custom(format!(
                    "Error parsing window '{window_str}' of column '{colname}': {e}"
                ))
            })?;

            let range = self.find_group(window_freq)?;
            let out_name = if let Some(suf) = &range.suffix {
                format!("{gas}_{suf}")
            } else {
                gas.to_string()
            };

            groups.entry(out_name).or_default().push(&colname);
        }

        Ok(groups)
    }

    fn header_lines(&self) -> Vec<String> {
        todo!()
    }
}

pub struct FrequencyRange {
    min_freq: f64,
    max_freq: f64,
    suffix: Option<String>,
}

impl FrequencyRange {
    pub fn new<S: ToString>(min_freq: f64, max_freq: f64, suffix: Option<S>) -> Self {
        if max_freq < min_freq {
            log::warn!("max_freq was less than min_freq, reversing");
            Self {
                min_freq: max_freq,
                max_freq: min_freq,
                suffix: suffix.map(|s| s.to_string()),
            }
        } else {
            Self {
                min_freq,
                max_freq,
                suffix: suffix.map(|s| s.to_string()),
            }
        }
    }
}
