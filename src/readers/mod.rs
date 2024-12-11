use std::{fmt::Display, str::FromStr, sync::OnceLock};

use crate::error::HeaderError;

pub mod runlogs;
pub mod mav_files;
pub mod col_files;
pub mod postproc_files;

pub const POSTPROC_FILL_VALUE: f64 = 9.8765e35;
static PROGRAM_VERSION_REGEX: OnceLock<regex::Regex> = OnceLock::new();


#[derive(Debug, Clone, PartialEq, Eq)]
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
            regex::Regex::new(r"(?<program>\w+)\s+(?<version>[Vv][Ee][Rr][Ss][Ii][Oo][Nn]\s+[\w\.\-]+)\s+(?<date>[\d\-]+)(\s+(?<authors>[\w\,]+))?")
                .expect("Could not compile program version regex")
        );

        let s = s.trim();

        let caps = re.captures(s)
            .ok_or_else(|| HeaderError::ParseError { 
                location: s.into(), 
                cause: "Did not match expected format of program name, version, date, and authors".to_string()
            })?;

        // JLL: I allow authors to be missing because it was in one of the program lines for
        // the AICF work. Might revert to this being required in the future.
        let program = caps["program"].to_string();
        let authors = if let Some(m) = caps.name("authors") {
            m.as_str().to_string()
        } else {
            log::warn!("authors not found in the {program} program version line");
            "".to_string()
        };
        
        Ok(Self { 
            program,
            version: caps["version"].to_owned(), 
            date: caps["date"].to_string(),
            authors
        })
    }
}