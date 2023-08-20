use std::{str::FromStr, fmt::Display, hash::Hash};

use itertools::Itertools;
use log::warn;

#[derive(Debug)]
pub struct CitFormatError {
    specname: String,
    not_letters: Vec<usize>,
    not_numbers: Vec<usize>,
    missing_period: Option<usize>,
    bad_date: bool,
    too_short: bool
}

impl CitFormatError {
    fn new(specname: String) -> Self {
        Self { specname, not_letters: vec![], not_numbers: vec![], missing_period: None, bad_date: false, too_short: false }
    }

    fn spec_too_short(specname: String) -> Self {
        let mut err = Self::new(specname);
        err.too_short = true;
        err
    }

    fn has_problem(&self) -> bool {
        !self.not_letters.is_empty()
            || !self.not_numbers.is_empty()
            || self.missing_period.is_some()
            || self.bad_date
            || self.too_short
    }
}

impl Display for CitFormatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.too_short {
            return write!(f, "Spectrum name '{}' to too short (minimum 20 characters, actual length was {} characters)", self.specname, self.specname.len());
        }

        let mut causes: Vec<String> = vec![];
        if !self.not_letters.is_empty() {
            let pos = self.not_letters
                .iter()
                .map(|i| (i+1).to_string())
                .join(", ");
            causes.push(format!("Character(s) at position(s) {pos} must be ASCII letters"));
        }

        if !self.not_numbers.is_empty() {
            let pos = self.not_numbers
                .iter()
                .map(|i| (i+1).to_string())
                .join(", ");
            causes.push(format!("Character(s) at position(s) {pos} must be ASCII numbers"));
        }

        if let Some(i) = self.missing_period {
            causes.push(format!("Character at position {} must be a period", i+1));
        }

        if self.bad_date {
            let dstr = &self.specname[2..=9];
            causes.push(format!("Substring '{dstr}' is not a valid date"));
        }

        write!(f, "Spectrum name '{}' does not follow the CIT naming convention: ", self.specname)?;
        let multiple_causes = causes.len() > 1;
        for (idx, cause) in causes.into_iter().enumerate() {
            if multiple_causes && idx == 0 {
                write!(f, "({}) ", idx + 1)?;
            } else if multiple_causes {
                write!(f, ", ({}) ", idx + 1)?;
            }

            write!(f, "{cause}")?;
        }



        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CitSource {
    Sun,
    Moon,
    Lamp,
    ScatteredSky,
    Other(char)
}

impl From<char> for CitSource {
    fn from(value: char) -> Self {
        match value {
            's' => Self::Sun,
            'm' => Self::Moon,
            'l' => Self::Lamp,
            'a' => Self::ScatteredSky,
            _ => Self::Other(value)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CitInternalCell {
    None,
    TcconHCl10cm,
    AdditionalHCl10cm,
    NdaccHBr5cm,
    N2O20cm,
    Other(char)
}

impl From<char> for CitInternalCell {
    fn from(value: char) -> Self {
        match value {
            '0' => Self::None,
            'a' | 'b' | 'c' | 'd' | 'e' | 'f' => Self::TcconHCl10cm,
            'g' => Self::AdditionalHCl10cm,
            'h' => Self::NdaccHBr5cm,
            'i' => Self::N2O20cm,
            _ => Self::Other(value)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CitBeamsplitter {
    CaltechCaF2,
    LauderCaF2,
    DarwinCaF2,
    LamontCaF2,
    Other(char)
}

impl From<char> for CitBeamsplitter {
    fn from(value: char) -> Self {
        match value {
            'a' => Self::CaltechCaF2,
            'b' => Self::LauderCaF2,
            'c' => Self::DarwinCaF2,
            'd' => Self::LamontCaF2,
            _ => Self::Other(value)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CitDichroic {
    None,
    Caltech,
    Lauder,
    Darwin,
    Lamont,
    Other(char)
}

impl From<char> for CitDichroic {
    fn from(value: char) -> Self {
        match value {
            '0' => Self::None,
            'a' => Self::Caltech,
            'b' => Self::Lauder,
            'c' => Self::Darwin,
            'd' => Self::Lamont,
            _ => Self::Other(value)
        }
    }
}

pub enum CitOpticalFilter {
    None,
    RedGlass,
    Germanium,
    TcconGhostFilter,
    Other(char)
}

impl From<char> for CitOpticalFilter {
    fn from(value: char) -> Self {
        match value {
            '0' => Self::None,
            'a' => Self::RedGlass,
            'b' => Self::Germanium,
            'g' => Self::TcconGhostFilter,
            _ => Self::Other(value)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CitDetector {
    InGaAs,
    Si,
    InSb,
    Em27Ext,
    DualChannel,
    Other(char)
}

impl From<char> for CitDetector {
    fn from(value: char) -> Self {
        match value {
            'a' => Self::InGaAs,
            'b' => Self::Si,
            'c' => Self::InSb,
            'd' => Self::Em27Ext,
            'x' | 'X' => Self::DualChannel,
            _ => Self::Other(value)
        }
    }
}

impl Display for CitDetector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let c: char = self.into();
        write!(f, "{c}")
    }
}

impl From<&CitDetector> for char {
    fn from(value: &CitDetector) -> Self {
        match value {
            CitDetector::InGaAs => 'a',
            CitDetector::Si => 'b',
            CitDetector::InSb => 'c',
            CitDetector::Em27Ext => 'd',
            CitDetector::DualChannel => 'x',
            CitDetector::Other(c) => *c,
        }
    }
}

pub struct CitSpectrumName {
    spectrum_name: String,
    date: chrono::NaiveDate,
    spec_num: u64,
}

impl CitSpectrumName {
    pub fn site_id(&self) -> &str {
        &self.spectrum_name[..2]
    }

    pub fn date(&self) -> chrono::NaiveDate {
        self.date
    }

    // For these, since from_str verifies the length, we know we can
    // get these characters
    pub fn source(&self) -> CitSource {
        let c = self.spectrum_name.chars().nth(10)
            .expect("Spectrum name had no 11th character");
        CitSource::from(c)
    }

    pub fn internal_cell(&self) -> CitInternalCell {
        let c: char = self.spectrum_name.chars().nth(11)
            .expect("Spectrum name had no 12th character");
        CitInternalCell::from(c)
    }

    pub fn beamsplitter(&self) -> CitBeamsplitter {
        let c = self.spectrum_name.chars().nth(12)
            .expect("Spectrum name had no 13th character");
        CitBeamsplitter::from(c)
    }

    pub fn dichroic(&self) -> CitDichroic {
        let c = self.spectrum_name.chars().nth(13)
            .expect("Spectrum name had no 14th character");
        CitDichroic::from(c)
    }

    pub fn optical_filter(&self) -> CitOpticalFilter {
        let c = self.spectrum_name.chars().nth(14)
            .expect("Spectrum name had no 15th character");
        CitOpticalFilter::from(c)
    }

    pub fn detector(&self) -> CitDetector {
        let c = self.spectrum_name.chars().nth(15)
            .expect("Spectrum name had no 16th character");
        CitDetector::from(c)
    }

    pub fn spectrum_number(&self) -> u64 {
        self.spec_num
    }

    pub fn spectrum_name_with_detector(&self, detector: CitDetector) -> String {
        format!("{}{}{}", &self.spectrum_name[..=14], detector, &self.spectrum_name[16..])
    }
}

impl FromStr for CitSpectrumName {
    type Err = CitFormatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() < 20 {
            return Err(CitFormatError::spec_too_short(s.to_string()));
        } else if s.len() > 21 {
            warn!("Spectrum name '{s}' is too long: CIT convention expects at most 21 characters (a 4 digit extension)");
        }

        let mut err = CitFormatError::new(s.to_string());

        for (i, c) in s.char_indices() {
            match i {
                0..=1 => {
                    // The first two characters are expected to be the site ID
                    if !c.is_ascii_alphabetic() {
                        err.not_letters.push(i);
                    }
                },
                2..=9 => {
                    // This is the date, we'll check that it's a valid date later
                    if !c.is_ascii_digit() {
                        err.not_numbers.push(i);
                    }
                }
                10..=15 => {
                    // These represent instrument characteristics, we'll allow them
                    // to be anything, since their enums have wildcard variants
                },
                16 => {
                    // This is the period separating the spectrum number from the rest
                    // of the name
                    if c != '.' {
                        err.missing_period = Some(i);
                    }
                },
                _ => {
                    // The extension after the period must be numbers
                    if !c.is_ascii_digit() {
                        err.not_numbers.push(i);
                    }
                }
            }
        }

        if err.has_problem() {
            return Err(err);
        }

        let date = if let Ok(d) = chrono::NaiveDate::parse_from_str(&s[2..=9], "%Y%m%d") {
            d
        } else {
            err.bad_date = true;
            return Err(err);
        };

        // Since we checked that the extension was all digits, this should not fail
        let spec_num: u64 = (&s[17..]).parse()
            .expect("Tried to parse a non-numeric spectrum extension");

        Ok(Self { spectrum_name: s.to_string(), date, spec_num })
    }
}

impl Display for CitSpectrumName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.spectrum_name)
    }
}

impl Hash for CitSpectrumName {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.spectrum_name.hash(state);
    }
}