use std::{io::{BufReader, BufRead}, fs::File, path::Path, str::FromStr, fmt::Display};

use chrono::NaiveDate;
use tabled::Tabled;

use crate::utils::{FileBuf, remove_comment, GggError, remove_comment_multiple_lines};

pub type CatalogueEntryResult<T> = Result<T, CatalogueEntryError>;

/// Number of header parameters in an I2S 2014 file
pub const I2S2014_NUM_HEADER_PARAMS: usize = 27;
/// Number of header parameters in an I2S 2020 file
pub const I2S2020_NUM_HEADER_PARAMS: usize = 28;


/// Error indicating an unknown I2S version
#[derive(Debug)]
pub struct I2SVersionError {
    given: String,
}

impl Display for I2SVersionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unknown I2S version, '{}'. Available versions are: '2014', '2020'.", self.given)
    }
}

impl std::error::Error for I2SVersionError {}

/// A representation of which I2S version is in use.
#[derive(Debug, Clone, Copy)]
pub enum I2SVersion {
    I2S2014,
    I2S2020,
}

impl I2SVersion {
    /// Return the number of header parameters in an I2S input file for this version of I2S.
    pub fn num_header_params(&self) -> usize {
        match self {
            I2SVersion::I2S2014 => I2S2014_NUM_HEADER_PARAMS,
            I2SVersion::I2S2020 => I2S2020_NUM_HEADER_PARAMS,
        }
    }

    pub fn num_lines_for_param(&self, param_num: usize) -> usize {
        match self {
            I2SVersion::I2S2014 => 1,

            // In GGG2020 parameter 17 is the min/max check which is written like so:
            //  0.005 0.005   Min allowed igram values (Master, Slave)
            //  1.000 1.000   Max allowed igram values (Master, Slave)
            // so it must be read as two lines.
            I2SVersion::I2S2020 => match param_num {
                17 => 2,
                _ => 1
            },
        }
    }
}

impl Display for I2SVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            I2SVersion::I2S2014 => write!(f, "2014"),
            I2SVersion::I2S2020 => write!(f, "2020"),
        }
    }
}

/// The default I2S version, currently I2S2020.
impl Default for I2SVersion {
    fn default() -> Self {
        Self::I2S2020
    }
}

impl FromStr for I2SVersion {
    type Err = I2SVersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "2014" => Ok(Self::I2S2014),
            "2020" => Ok(Self::I2S2020),
            _ => Err(I2SVersionError { given: s.to_string() })
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CatalogueEntryError {
    #[error("Invalid value for field {field}: {reason}")]
    InvalidValue{field: &'static str, reason: String},
    #[error("Field {0} is required for a catalogue entry")]
    MissingRequiredField(&'static str)
}

// ------------------------ //
// I2S INPUT FILE ITERATORS //
// ------------------------ //

/// Given a path to an I2S input file, provide an iterator over the header (top) values.
/// 
/// Each parameter will be returned as a string with any inline comment, trailing newline,
/// or trailing carriage return removed. The I2S version must be passed as `i2s_version`
/// to indicate how many header parameters there are. For more control over the iteration, 
/// construct the [`I2SParamIter`] directly.
/// 
/// # Errors
/// 
/// This function itself will return an error if it cannot open `i2s_input_file`.
/// The iterator may also return an error if reading a line from that file fails.
/// 
/// # See also
/// - [`iter_i2s_header_params_with_number`] if parameter numbers are required.
/// - [`iter_i2s_lines`] if all lines (including comments) from an input file are required.
pub fn iter_i2s_header_params(i2s_input_file: &Path, i2s_version: I2SVersion) -> Result<I2SParamIter, GggError> {
    let file = FileBuf::open(i2s_input_file)?;
    Ok(I2SParamIter::new(file, i2s_version, false))
}

/// Given a path to an I2S input file, provide an iterator over the header (top) values and their parameter numbers.
/// 
/// Like [`iter_i2s_header_params`], the iterator will return each top value as a string with
/// any inline comment removed and trailing newlines/carriage returns trimmed. This iterator will
/// also include the I2S parameter number (1-based) as the first element of each iterated value.
/// For more control of the iteration, construct the [`I2SParamIterWithIndex`] directly.
/// 
/// # Errors
/// 
/// Same as [`iter_i2s_header_params`].
/// 
/// # See also
/// - [`iter_i2s_header_params`] if parameter numbers are not required.
/// - [`iter_i2s_lines`] if all lines (including comments) from an input file are required.
pub fn iter_i2s_header_params_with_number(i2s_input_file: &Path, i2s_version: I2SVersion) -> Result<I2SParamIterWithIndex, GggError> {
    let inner = iter_i2s_header_params(i2s_input_file, i2s_version)?;
    Ok(I2SParamIterWithIndex(inner))
}

/// Given a path to an I2S input file, iterate over all lines, indicating whether each one is a top line.
/// 
/// This is essentially a wrapper around a [`BufReader`] that also indicates whether each line returned
/// is an top) line, i.e. one in the header. Each iteration returns a tuple `(I2SLineType, String)` where
/// the first value indicates the role of the line in the input file. To determine if the line is in the
/// header, call the `is_header_line` method on that value.
/// 
/// **Note:** unlike [`iter_i2s_header_params`] and [`iter_i2s_header_params_with_number`], trailing newlines
/// and carriage returns are *not* removed from the lines yielded by this iterator.
/// 
/// # Errors
/// 
/// Same as [`iter_i2s_header_params`].
/// 
/// # See also
/// - [`iter_i2s_header_params`] to iterate only over header parameter values.
/// - [`iter_i2s_header_params_with_number`] to iterate over header parameter values with the parameter numbers included.
pub fn iter_i2s_lines(i2s_input_file: &Path, i2s_version: I2SVersion) -> Result<I2SLineIter, GggError> {
    let file = FileBuf::open(i2s_input_file)?;
    Ok(I2SLineIter::new(file, i2s_version))
}

/// An iterator over I2S parameters.
/// 
/// This is normally created by calling the [`iter_i2s_header_params`] function, but can be constructed
/// directly for more control. Note that this iterator considers any line of an I2S input file with
/// non-whitespace and characters before a colon (i.e. not commented) to be a parameter. Thus, uncommented
/// catalog rows will be yielded as "parameters". The values yielded by this iterator will have any
/// inline comments, trailing newlines, and trailing carriage returns stripped.
pub struct I2SParamIter<'a> {
    file: FileBuf<'a, BufReader<File>>,
    curr_param: usize,
    i2s_version: I2SVersion,
    all_params: bool,
}

impl<'a> I2SParamIter<'a> {
    /// Create a new instance of the iterator.
    /// 
    /// Pass a [`FileBuf`] reader around an I2S input file and the maximum number of parameters
    /// to read before the iterator stops. If `max_n_param` is `None`, then the iterator will continue
    /// until all lines in the reader are exhausted. Otherwise, it will stop after returning that
    /// many parameters. (This is usually used to get only the top parameters.) 
    pub fn new(i2s_reader: FileBuf<'a, BufReader<File>>, i2s_version: I2SVersion, all_params: bool) -> Self {
        Self { file: i2s_reader, curr_param: 0, i2s_version, all_params }
    }

    fn max_n_param(&self) -> Option<usize> {
        if self.all_params {
            None
        } else {
            Some(self.i2s_version.num_header_params())
        }
    }
}


impl<'a> Iterator for I2SParamIter<'a> {
    type Item = std::io::Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        let max_n_params = self.max_n_param();
        loop {
            let (is_param, param) = match iter_i2s_lines_inner(
                    &mut self.file, 
                    self.curr_param, 
                    self.i2s_version.num_lines_for_param(self.curr_param),
                    max_n_params
            ) {
                Some(Ok(line)) => line,
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            };

            if is_param {
                self.curr_param += 1;
                let value = remove_comment_multiple_lines(&param)
                    .trim_end_matches("\n")
                    .trim_end_matches("\r")
                    .to_string();
                return Some(Ok(value))
            }
        }
    }
}

/// An iterator over I2S parameters that includes the parameter number.
/// 
/// This is almost identical to [`I2SParamIter`] except that the values
/// yielded by the iterator will be tuples of `(usize, String)` that are
/// the 1-based parameter number and its value, rather than just the value.
/// 
/// This iterator is normally constructed by calling the [`iter_i2s_header_params_with_number`];
/// only construct it directly if you need more control over the iteration.
pub struct I2SParamIterWithIndex<'a>(I2SParamIter<'a>);

impl<'a> I2SParamIterWithIndex<'a> {
    /// Create a new instance of the iterator.
    /// 
    /// Pass a [`FileBuf`] reader around an I2S input file and the maximum number of parameters
    /// to read before the iterator stops. If `max_n_param` is `None`, then the iterator will continue
    /// until all lines in the reader are exhausted. Otherwise, it will stop after returning that
    /// many parameters. (This is usually used to get only the top parameters.) 
    pub fn new(i2s_reader: FileBuf<'a, BufReader<File>>, i2s_version: I2SVersion, all_params: bool) -> Self {
        let inner = I2SParamIter::new(i2s_reader, i2s_version, all_params);
        Self(inner)
    }
}

impl<'a> Iterator for I2SParamIterWithIndex<'a> {
    type Item = std::io::Result<(usize, String)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next() {
            Some(Ok(v)) => Some(Ok((self.0.curr_param, v))),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

/// An enum indicating the role of a line in an I2S input file
#[derive(Debug, Clone, Copy)]
pub enum I2SLineType {
    /// The line is a parameter in the file header (i.e. top). The
    /// contained value is the 1-based parameter number.
    HeaderParam(usize),

    /// Thie line is from the header but is not a parameter. This
    /// is usually a comment or blank line.
    HeaderLine,

    /// This line is a row from the catalog that contains at least
    /// some information. It does not guarantee that the row is 
    /// complete.
    CatalogRow,

    /// This line is from the catalog, but does not contain any 
    /// information for I2S. This may be a comment or blank line.
    Other,
}

impl I2SLineType {
    /// Returns `true` if the line is in the header of the input file.
    /// 
    /// All lines up to the first row of the catalog are considered in
    /// the header, thus any comment/blank lines between the last header
    /// parameter and first catalog row will return `true`. This cannot
    /// distinguish between a real comment and a commented-out catalog
    /// row before any real catalog rows.
    pub fn is_header_line(&self) -> bool {
        match self {
            I2SLineType::HeaderParam(_) => true,
            I2SLineType::HeaderLine => true,
            I2SLineType::CatalogRow => false,
            I2SLineType::Other => false,
        }
    }

    /// Get the header parameter number for this line. If this line is
    /// not a header parameter, returns `None`.
    pub fn header_param(&self) -> Option<usize> {
        if let Self::HeaderParam(i) = self {
            Some(*i)
        } else {
            None
        }
    }
}

/// An iterator over all lines in an I2S input file.
/// 
/// This is normally constructed by calling [`iter_i2s_lines`], only construct
/// this directly if you need more control over the iteration. **Note:** unlike
/// [`I2SParamIter`] and [`I2SParamIterWithIndex`], trailing newlines/carriage returns
/// are *not* stripped from the lines yielded by this iterator. The iterator will
/// yield tuples of `(I2SLineType, String)`, where the first value indicates what
/// role the line plays in the input file.
pub struct I2SLineIter<'a> {
    file: FileBuf<'a, BufReader<File>>,
    curr_param: usize,
    i2s_version: I2SVersion,
}

impl<'a> I2SLineIter<'a> {
    /// Create a new instance of the iterator.
    /// 
    /// Pass a [`FileBuf`] reader around an I2S input file and the number of parameters in 
    /// the header. The number of header parameters determines when the lines yielded
    /// are no longer in the header.
    pub fn new(i2s_reader: FileBuf<'a, BufReader<File>>, i2s_version: I2SVersion) -> Self {
        Self { file: i2s_reader, curr_param: 0, i2s_version }
    }
}

impl<'a> Iterator for I2SLineIter<'a> {
    type Item = std::io::Result<(I2SLineType, String)>;

    fn next(&mut self) -> Option<Self::Item> {
        let (is_param, line) = match iter_i2s_lines_inner(
                &mut self.file,
                self.curr_param, 
                self.i2s_version.num_lines_for_param(self.curr_param),
                None
        ) {
            Some(Ok(v)) => v,
            Some(Err(e)) => return Some(Err(e)),
            None => return None,
        };

        if is_param {
            self.curr_param += 1;
        }

        let is_header_line = self.curr_param <= self.i2s_version.num_header_params();
        let line_type = if is_param && is_header_line {
            I2SLineType::HeaderParam(self.curr_param)
        } else if is_header_line {
            I2SLineType::HeaderLine
        } else if is_param {
            I2SLineType::CatalogRow
        } else {
            I2SLineType::Other
        };

        Some(Ok((line_type, line)))
    }
}

fn iter_i2s_lines_inner(file: &mut FileBuf<'_, BufReader<File>>, curr_param: usize, param_num_lines: usize, max_n_param: Option<usize>) -> Option<std::io::Result<(bool, String)>> {
    if let Some(max) = max_n_param {
        if curr_param >= max {
            return None;
        }
    }

    // The I2S input format is that each line that has any non-whitespace and non-commented
    // characters is a parameter. As of 2023-12-22, there is only one input in I2S 2020 that
    // has two lines (#17, min/max values). I2S's parse_input_top calls read_input_line twice
    // to get that, so we mimic that sort of. Here we keep all the line contents, so that if
    // there is a comment between the lines for some reason, it can be retained. Outer iterators
    // will have to remove the comments themselves if they only want the value.
    //
    // Seriously, can we make I2S use a different input format? Please? At least for the top?
    let mut full_buf = String::new();
    let mut is_param = false;
    let mut n_param_lines_read = 0;
    while n_param_lines_read < param_num_lines {   
        let mut buf = String::new();
        match file.read_line(&mut buf) {
            Ok(0) => {
                if n_param_lines_read == 0 {
                    return None;
                } else {
                    return Some(Err(std::io::Error::other("File ended midway through reading a parameter.")))
                }
            } 
            Err(e) => return Some(Err(e)),
            Ok(_) => {},
        }

        let value = remove_comment(&buf);
        full_buf.push_str(&buf);
        let is_param_line = !value.trim().is_empty();
        if is_param_line {
            is_param = true;
            n_param_lines_read += 1;
        } else if n_param_lines_read == 0 {
            // If this is the first line we read and it wasn't a parameter,
            // then this is a comment and should be returned as it's own line.
            // We only need to return multi-line parameters as a unit to allow them
            // to be copied or replaced correctly.
            break;
        }
    }
    Some(Ok((is_param, full_buf)))
}


// ------------------ //
// INPUT MODIFICATION //
// ------------------ //

/// A structure representing edits to make to an I2S input file
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct I2SInputModifcations {
    /// The list of changes to make to header parameters
    pub header: Vec<I2SHeaderEdit>,
}

impl I2SInputModifcations {
    /// Given a line type, return `Some(_)` with the new line if that line should
    /// be replaced, `None` otherwise.
    /// 
    /// This is intended for use with [`iter_i2s_lines`], such that you can use
    /// the iterator over lines to get `line_type`, pass in the `line_type` and
    /// use the optional return of this function to decide whether to write
    /// the new line or existing line.
    pub fn change_line_opt(&self, line_type: I2SLineType) -> Option<&str> {
        match line_type {
            I2SLineType::HeaderParam(param) => {
                for edit in self.header.iter() {
                    if edit.parameter == param {
                        return Some(&edit.value)
                    }
                }

                None
            },
            I2SLineType::HeaderLine => None,
            I2SLineType::CatalogRow => None,
            I2SLineType::Other => None,
        }
    }

    /// Create an example JSON string.
    /// 
    /// Usually used to print out as an example for users to learn from.
    pub fn example_json_string(pretty: bool) -> String {
        let example = Self {
            header: vec![
                I2SHeaderEdit{ parameter: 1, value: "./igms".to_string() },
                I2SHeaderEdit{ parameter: 2, value: "./spectra".to_string() },
                I2SHeaderEdit{ parameter: 7, value: "1 1".to_string() },
            ]
        };

        if pretty {
            serde_json::to_string_pretty(&example).unwrap()
        } else {
            serde_json::to_string(&example).unwrap()
        }
    }
}

/// A structure representing a single change to an I2S input file header
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct I2SHeaderEdit {
    /// The 1-based parameter number that this corresponds to
    pub parameter: usize,

    /// The new value for this parameter
    pub value: String
}

// ----------------- //
// CATALOG FUNCTIONS //
// ----------------- //

/// Write out a list of catalogue entries plus the header to a stream
/// 
/// # Inputs
/// - `writer`: any object that implements the [`std::io::Write`] trait, such as [`std::io::Stdout`] or [`std::fs::File`].
///   The catalogue will be written to that stream.
/// - `entries`: the list of catalogue entries to write out
/// - `no_colon:` if `true`, then the header line will be printed without a leading colon. This should be `false` in
///   most cases where writing out a catalogue for use by I2S, as the header must be commented out for I2S.
pub fn write_opus_catalogue_table<W: std::io::Write>(writer: &mut W, entries: &[OpusCatalogueEntry], no_colon: bool) -> std::io::Result<()> {
    // While we can use the Tabled trait to make the table directly, we use a builder to control whether there is a colon
    // at the start of the header line
    let mut builder = tabled::builder::Builder::new();
    if no_colon {
        builder.set_header(OpusCatalogueEntry::headers());
    } else {
        let mut headers = OpusCatalogueEntry::headers();
        headers[0] = format!(": {}", headers[0]).into();
        builder.set_header(headers);
    }

    for entry in entries {
        builder.push_record(entry.fields());
    }

    let mut catalogue = builder.build();
    catalogue.with(tabled::settings::style::Style::blank())
        .with(tabled::settings::Alignment::left());
    
    write!(writer, "{catalogue}")
}


pub struct OpusCatalogueEntry {
    spectrum_name: String,
    year: i32,
    month: u32,
    day: u32,
    run: u32,
    latitude: f32,
    longitude: f32,
    altitude: f32,
    instrument_temperature: f32,
    instrument_pressure: f32,
    instrument_humidity: f32,
    outside_temperature: f32,
    outside_pressure: f32,
    outside_humidity: f32,
    solar_intensity_average: f32,
    fractional_variation_solar_intensity: f32,
    wind_speed: f32,
    wind_direction: f32,
}

impl OpusCatalogueEntry {
    pub fn build(spectrum_name: String) -> OpusCatalogueEntryBuilder {
        let mut builder = OpusCatalogueEntryBuilder::default();
        builder.spectrum_name = spectrum_name;
        builder
    }

    pub fn write_headers<W: std::io::Write>(writer: &mut W, no_colon: bool) -> std::io::Result<()> {
        if !no_colon {
            write!(writer, ":")?
        }

        for header in Self::headers() {
            write!(writer, "  {header}")?;
        }
        writeln!(writer, "")?;

        Ok(())
    }

    pub fn write<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let name = self.quote_spec_name();

        // Make variables for all of these so we can use them directly in the formatting string
        let y = self.year;
        let m = self.month;
        let d = self.day;
        let r = self.run;
        let lat = self.latitude;
        let lon = self.longitude;
        let alt = self.altitude;
        let tins = self.instrument_temperature;
        let pins = self.instrument_pressure;
        let hins = self.instrument_humidity;
        let tout = self.outside_temperature;
        let pout = self.outside_pressure;
        let hout = self.outside_humidity;
        let sia = self.solar_intensity_average;
        let fvsi = self.fractional_variation_solar_intensity;
        let wspd = self.wind_speed;
        let wdir = self.wind_direction;

        writeln!(writer, "{name}  {y}  {m}  {d}  {r}  {lat:.4}  {lon:.4}  {alt:.4}  {tins:.1}  {pins:.1}  {hins:.1}  {tout:.1}  {pout:.1}  {hout:.1}  {sia:.1}  {fvsi:.4}  {wspd:.1}  {wdir:.1}")
    }

    fn quote_spec_name(&self) -> String {
        // If we are by chance given a spectrum that has spaces in it, quote the name to ensure I2S doesn't split on them
        // (note, have not tested this works for I2S as of 2023-09-26)
        if self.spectrum_name.contains(' ') {
            format!("\"{}\"", self.spectrum_name)
        } else {
            self.spectrum_name.clone()
        }
    }
}

impl tabled::Tabled for OpusCatalogueEntry {
    const LENGTH: usize = 18;

    fn fields(&self) -> Vec<std::borrow::Cow<'_, str>> {
        vec![
            self.quote_spec_name().into(),
            format!("{}", self.year).into(),
            format!("{}", self.month).into(),
            format!("{}", self.day).into(),
            format!("{}", self.run).into(),
            format!("{:.4}", self.latitude).into(),
            format!("{:.4}", self.longitude).into(),
            format!("{:.4}", self.altitude).into(),
            format!("{:.1}", self.instrument_temperature).into(),
            format!("{:.1}", self.instrument_pressure).into(),
            format!("{:.1}", self.instrument_humidity).into(),
            format!("{:.1}", self.outside_temperature).into(),
            format!("{:.1}", self.outside_pressure).into(),
            format!("{:.1}", self.outside_humidity).into(),
            format!("{:.1}", self.solar_intensity_average).into(),
            format!("{:.4}", self.fractional_variation_solar_intensity).into(),
            format!("{:.1}", self.wind_speed).into(),
            format!("{:.1}", self.wind_direction).into(),
        ]
    }

    fn headers() -> Vec<std::borrow::Cow<'static, str>> {
        vec![
            "Spectrum_Name".into(),
            "year".into(),
            "mon".into(),
            "day".into(),
            "run".into(),
            "lat".into(),
            "lon".into(),
            "alt".into(),
            "Tins".into(),
            "Pins".into(),
            "Hins".into(),
            "Tout".into(),
            "Pout".into(),
            "Hout".into(),
            "SIA".into(),
            "FVSI".into(),
            "WSPD".into(),
            "WDIR".into(),
        ]
    }
}

#[derive(Debug, Default)]
pub struct OpusCatalogueEntryBuilder {
    spectrum_name: String,
    year: Option<i32>,
    month: Option<u32>,
    day: Option<u32>,
    run: Option<u32>,
    latitude: Option<f32>,
    longitude: Option<f32>,
    altitude: Option<f32>,
    instrument_temperature: Option<f32>,
    instrument_pressure: Option<f32>,
    instrument_humidity: Option<f32>,
    outside_temperature: Option<f32>,
    outside_pressure: Option<f32>,
    outside_humidity: Option<f32>,
    solar_intensity_average: Option<f32>,
    fractional_variation_solar_intensity: Option<f32>,
    wind_speed: Option<f32>,
    wind_direction: Option<f32>,
}

impl OpusCatalogueEntryBuilder {
    pub fn finalize(self, fill_value: f32) -> CatalogueEntryResult<OpusCatalogueEntry> {
        Ok(OpusCatalogueEntry { 
            spectrum_name: self.spectrum_name, 
            year: self.year.ok_or_else(|| CatalogueEntryError::MissingRequiredField("year"))?, 
            month: self.month.ok_or_else(|| CatalogueEntryError::MissingRequiredField("month"))?, 
            day: self.day.ok_or_else(|| CatalogueEntryError::MissingRequiredField("day"))?, 
            run: self.run.ok_or_else(|| CatalogueEntryError::MissingRequiredField("run"))?, 
            latitude: self.latitude.unwrap_or(fill_value), 
            longitude: self.longitude.unwrap_or(fill_value), 
            altitude: self.altitude.unwrap_or(fill_value), 
            instrument_temperature: self.instrument_temperature.unwrap_or(fill_value), 
            instrument_pressure: self.instrument_pressure.unwrap_or(fill_value), 
            instrument_humidity: self.instrument_humidity.unwrap_or(fill_value), 
            outside_temperature: self.outside_temperature.unwrap_or(fill_value), 
            outside_pressure: self.outside_pressure.unwrap_or(fill_value), 
            outside_humidity: self.outside_humidity.unwrap_or(fill_value), 
            solar_intensity_average: self.solar_intensity_average.unwrap_or(fill_value), 
            fractional_variation_solar_intensity: self.fractional_variation_solar_intensity.unwrap_or(fill_value), 
            wind_speed: self.wind_speed.unwrap_or(fill_value), 
            wind_direction: self.wind_direction.unwrap_or(fill_value) 
        })
    }

    pub fn with_time(mut self, year: i32, month: u32, day: u32, run: u32) -> CatalogueEntryResult<Self> {
        if NaiveDate::from_ymd_opt(year, month, day).is_none() {
            return Err(CatalogueEntryError::InvalidValue { field: "year/month/day", reason: format!("{year:04}-{month:02}-{day:02} is not a valid date") })
        }

        self.year = Some(year);
        self.month = Some(month);
        self.day = Some(day);
        self.run = Some(run);

        Ok(self)
    }

    pub fn with_coordinates(mut self, latitude: f32, longitude: f32, altitude_meters: f32) -> CatalogueEntryResult<Self> {
        if latitude < -90.0 || latitude > 90.0 {
            return Err(CatalogueEntryError::InvalidValue { field: "latitude", reason: format!("Latitude must be between -90 and +90, {latitude} is invalid") })
        } else {
            self.latitude = Some(latitude);
        }

        if longitude < -180.0 || longitude > 360.0 {
            return Err(CatalogueEntryError::InvalidValue { field: "longitude", reason: format!("Longitude must be between -180 and +360, {longitude} is invalid") })
        } else if longitude > 180.0 {
            self.longitude = Some(longitude - 360.0);
        } else {
            self.longitude = Some(longitude);
        }

        self.altitude = Some(altitude_meters);

        Ok(self)
    }

    #[allow(non_snake_case)]
    pub fn with_instrument(mut self, instr_temperature_degC: f32, instr_pressure_hPa: f32, instr_humidity_percent: f32) -> Self {
        self.instrument_temperature = Some(instr_temperature_degC);
        self.instrument_pressure = Some(instr_pressure_hPa);
        self.instrument_humidity = Some(instr_humidity_percent);
        self
    }


    #[allow(non_snake_case)]
    pub fn with_outside_met(mut self, temperature_degC: f32, pressure_hPa: f32, humidity_percent: f32) -> Self {
        self.outside_temperature = Some(temperature_degC);
        self.outside_pressure = Some(pressure_hPa);
        self.outside_humidity = Some(humidity_percent);
        self
    }

    pub fn with_solar(mut self, sia: f32, fvsi: f32) -> Self {
        self.solar_intensity_average = Some(sia);
        self.fractional_variation_solar_intensity = Some(fvsi);
        self
    }

    pub fn with_wind(mut self, wind_speed: f32, wind_direction: f32) -> Self {
        self.wind_speed = Some(wind_speed);
        self.wind_direction = Some(wind_direction);
        self
    }
}
