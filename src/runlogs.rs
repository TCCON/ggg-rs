//! Utilities for reading runlogs and iterating over their data records.
use std::{path::Path, io::BufReader, fs::File};

use fortformat::de::from_str_with_fields;
use itertools::Itertools;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{error::{FileLocation, HeaderError}, utils::{self, GggError}};

pub const NUM_RUNLOG_COLS: usize = 36;

/// A struct representing one line of a GGG2020 runlog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunlogDataRec {
    /// Whether this line was commented out in the runlog
    #[serde(deserialize_with = "deser_comment", serialize_with = "ser_comment")]
    pub commented: bool,
    /// The name of the spectrum
    #[serde(rename = "Spectrum_File_Name")]
    pub spectrum_name: String,
    /// The year the data is from
    #[serde(rename = "Year")]
    pub year: i32,
    /// The day-of-year the data is from
    #[serde(rename = "Day")]
    pub day: i32,
    /// The fractional UTC hour when zero-path-difference occured
    #[serde(rename = "Hour")]
    pub hour: f64,
    /// The observation latitude (south is negative)
    #[serde(rename = "oblat")]
    pub obs_lat: f64,
    /// The observation longitude (west is negative)
    #[serde(rename = "oblon")]
    pub obs_lon: f64,
    /// The observation altitude in kilometers
    #[serde(rename = "obalt")]
    pub obs_alt: f64,
    /// The astronomical solar zenith angle in degrees
    #[serde(rename = "ASZA")]
    pub asza: f64,
    /// The pointing offset in degrees
    #[serde(rename = "POFF")]
    pub poff: f64,
    /// The azimuth angle in degrees
    #[serde(rename = "AZIM")]
    pub azim: f64,
    /// Observer-sun doppler stretch
    #[serde(rename = "OSDS")]
    pub osds: f64,
    /// Optical path difference
    #[serde(rename = "OPD")]
    pub opd: f64,
    /// Internal FOV diameter in radians
    #[serde(rename = "FOVI")]
    pub fovi: f64,
    /// External FOV diameter in radians
    #[serde(rename = "FOVO")]
    pub fovo: f64,
    /// Angular misalignment
    #[serde(rename = "AMAL")]
    pub amal: f64,
    /// Index of first spectral point (lowest frequency/point spacing)
    #[serde(rename = "IFIRST")]
    pub ifirst: usize,
    /// Index of last spectral point (highest frequency/point spacing)
    #[serde(rename = "ILAST")]
    pub ilast: usize,
    /// Spectral point sampling
    #[serde(rename = "DELTA_NU")]
    pub delta_nu: f64,
    /// Byte offset of the first spectral point (length of header in bytes)
    #[serde(rename = "POINTER")]
    pub pointer: i32,
    /// Bytes per data word (i.e. intensity value, big endian is positive, little endian is negative)
    #[serde(rename = "BPW")]
    pub bpw: i8,
    /// Zero level offset as a fraction
    #[serde(rename = "ZOFF")]
    pub zoff: f64,
    /// Signal to noise ratio
    #[serde(rename = "SNR")]
    pub snr: i32,
    /// Apodization function
    #[serde(rename = "APF", deserialize_with = "utils::ApodizationFxn::deserialize")]
    pub apf: utils::ApodizationFxn,
    /// Instrument internal temperature in deg. C
    pub tins: f64,
    /// Instrument internal pressure in mbar
    pub pins: f64,
    /// Instrument internal relative humidity in percent
    pub hins: f64,
    /// Atmospheric temperature in deg. C
    pub tout: f64,
    /// Atmospheric pressure in mbar
    pub pout: f64,
    /// Atmospheric relative humidity in percent
    pub hout: f64,
    /// Average solar intensity
    pub sia: f64,
    /// Fractional variation in solar intensity
    pub fvsi: f64,
    /// Wind speed in meters/second
    pub wspd: f64,
    /// Wind direction (deg)
    pub wdir: f64,
    /// Laser frequency in cm-1
    pub lasf: f64,
    /// Tracker frequency in cm-1
    pub wavtkr: f64,
    /// Airmass-independent path length in kilometers
    pub aipl: f64
}

impl approx::AbsDiffEq for RunlogDataRec {
    type Epsilon = f64;

    fn default_epsilon() -> Self::Epsilon {
        // Since all floating point fields are f64, we use f64. 
        // If an f32 is added in the future, that should probably
        // be used instead
        f64::EPSILON
    }

    fn abs_diff_eq(&self, other: &Self, epsilon: Self::Epsilon) -> bool {
        if self.commented != other.commented { return false; }
        if self.spectrum_name != other.spectrum_name { return false; }
        if self.year != other.year { return false; }
        if self.day != other.day { return false; }
        if f64::abs_diff_ne(&self.hour, &other.hour, epsilon) { return false; }
        if f64::abs_diff_ne(&self.obs_lat, &other.obs_lat, epsilon) { return false; }
        if f64::abs_diff_ne(&self.obs_lon, &other.obs_lon, epsilon) { return false; }
        if f64::abs_diff_ne(&self.obs_alt, &other.obs_alt, epsilon) { return false; }
        if f64::abs_diff_ne(&self.hour, &other.hour, epsilon) { return false; }
        if f64::abs_diff_ne(&self.asza, &other.asza, epsilon) { return false; }
        if f64::abs_diff_ne(&self.poff, &other.poff, epsilon) { return false; }
        if f64::abs_diff_ne(&self.azim, &other.azim, epsilon) { return false; }
        if f64::abs_diff_ne(&self.osds, &other.osds, epsilon) { return false; }
        if f64::abs_diff_ne(&self.opd, &other.opd, epsilon) { return false; }
        if f64::abs_diff_ne(&self.fovi, &other.fovi, epsilon) { return false; }
        if f64::abs_diff_ne(&self.fovo, &other.fovo, epsilon) { return false; }
        if f64::abs_diff_ne(&self.amal, &other.amal, epsilon) { return false; }
        if self.ifirst != other.ifirst { return false; }
        if self.ilast != other.ilast { return false; }
        if f64::abs_diff_ne(&self.delta_nu, &other.delta_nu, epsilon) { return false; }
        if self.pointer != other.pointer { return false; }
        if self.bpw != other.bpw { return false; }
        if self.snr != other.snr { return false; }
        if self.apf != other.apf { return false; }
        if f64::abs_diff_ne(&self.tins, &other.tins, epsilon) { return false; }
        if f64::abs_diff_ne(&self.pins, &other.pins, epsilon) { return false; }
        if f64::abs_diff_ne(&self.hins, &other.hins, epsilon) { return false; }
        if f64::abs_diff_ne(&self.tout, &other.tout, epsilon) { return false; }
        if f64::abs_diff_ne(&self.pout, &other.pout, epsilon) { return false; }
        if f64::abs_diff_ne(&self.hout, &other.hout, epsilon) { return false; }
        if f64::abs_diff_ne(&self.sia, &other.sia, epsilon) { return false; }
        if f64::abs_diff_ne(&self.fvsi, &other.fvsi, epsilon) { return false; }
        if f64::abs_diff_ne(&self.wspd, &other.wspd, epsilon) { return false; }
        if f64::abs_diff_ne(&self.wdir, &other.wdir, epsilon) { return false; }
        if f64::abs_diff_ne(&self.lasf, &other.lasf, epsilon) { return false; }
        if f64::abs_diff_ne(&self.wavtkr, &other.wavtkr, epsilon) { return false; }
        if f64::abs_diff_ne(&self.aipl, &other.aipl, epsilon) { return false; }
        true
    }
}

fn deser_comment<'de, D>(deserializer: D) -> Result<bool, D::Error>
where D: Deserializer<'de>
{
    let s = String::deserialize(deserializer)?;
    Ok(s == ":")
}

fn ser_comment<S>(value: &bool, serializer: S) -> Result<S::Ok, S::Error>
where S: Serializer
{
    if *value {
        serializer.serialize_char(':')
    } else {
        serializer.serialize_char(' ')
    }
}

impl RunlogDataRec {
    pub fn zpd_time(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        let h = self.hour.floor();
        let m = (self.hour.fract() * 60.0).floor();
        let s = (self.hour.fract() * 60.0 - m).floor() * 60.0;
        let dt = chrono::NaiveDate::from_yo_opt(self.year, self.day as u32)?
            .and_hms_opt(h as u32, m as u32, s as u32)?;
        Some(chrono::DateTime::from_naive_utc_and_offset(dt, chrono::Utc))
    }
}

/// An iterator over lines in a runlog.
/// 
/// Use the `open` method to create an instance of this struct. The common
/// header information will be stored in the `header` field. You can then
/// access each line of the runlog in sequence either by converting this
/// into an iterator with `into_iter` or using the `next_data_record` method.
/// The former would look like:
/// 
/// ```no_run
/// let runlog = Runlog::open("~/ggg/runlogs/gnd/pa_ggg_benchmark.grl").unwrap();
/// for data_rec in runlog.into_iter() {
///     ...
/// }
/// ```
/// 
/// This is the most convenient way to deal with runlogs, but has the disadvantage that
/// if parsing a line of the runlog fails, it will cause the program to panic. If you
/// need the ability to recover from errors, use `next_data_record` instead:
/// 
/// ```no_run
/// let runlog = Runlog::open("~/ggg/runlogs/gnd/pa_ggg_benchmark.grl").unwrap();
/// loop {
///     // First we check if there was an error reading the data record.
///     let opt_data_rec = if let Ok(rec) = runlog.next_data_record() {
///         rec
///     }else{
///         println!("Error reading line {} of the runlog, skipping", runlog.curr_line());
///         continue
///     }
/// 
///     // If not, we also have to check that we actually have a data record
///     let data_rec = if let Some(rec) = opt_data_rec {
///         rec
///         // do whatever work needs done
///     }else{
///         // No further records
///         break
///     }
/// }
/// ```
/// 
/// Alternatively, use a [`FallibleRunlog`] instead.
pub struct Runlog {
    pub header: utils::CommonHeader,
    rl_handle: utils::FileBuf<BufReader<File>>,
    data_line_index: usize
}

impl Runlog {
    /// Open a runlog file as a `Runlog` instance.
    /// 
    /// # Parameters
    /// * `runlog` - the path to the runlog file.
    /// 
    /// # Returns
    /// A [`Result`] containing runlog instance with the header lines parsed and ready to iterate over data records.
    /// An `Err` can be returned if:
    /// 
    /// * the file could not be opened,
    /// * the header could not be parsed,
    /// * the number of columns specified in the header does not match the expected number, [`NUM_RUNLOG_COLS`]
    pub fn open(runlog: &Path) -> Result<Runlog, GggError> {
        let mut rl = utils::FileBuf::open(runlog)?;
        let header = utils::read_common_header(&mut rl)?;
        if header.ncol != NUM_RUNLOG_COLS {
            return Err(HeaderError::ParseError { 
                location: runlog.into(), 
                cause: format!("Number of columns specified in the header of runlog {} is not the expected number, {}",
                               header.ncol, NUM_RUNLOG_COLS)
            }.into());
        }

        if header.fformat.is_none() {
            return Err(GggError::HeaderError(
                HeaderError::ParseError { 
                    location: FileLocation::new::<_, String>(Some(runlog.to_path_buf()), None, None),
                    cause: "No format line found in the header".to_string()
                }
            ))
        }
    
        // At this point, the file handle will be pointing to the first line of data in the runlog
        Ok(Runlog{ rl_handle: rl, header, data_line_index: 0 })
    }

    /// Return which line in the file was last read.
    /// 
    /// This is 1-based, and counts from the top of the file (including the header). It is meant
    /// for error messages to help the user identify where their runlog is ill-formatted.
    pub fn curr_line(&self) -> usize {
        self.header.nhead + self.data_line_index
    }

    /// Get the next data record from the runlog, if one exists.
    /// 
    /// # Returns
    /// A [`Result`] containing an `Option<RunlogDataRec>`. If the end of the file has been reached, then
    /// this will be `None`. An `Err` is returned in several cases:
    /// 
    /// * the next data line could not be read,
    /// * the number of elements in the line does not match the number of columns,
    /// * any of the elements in the line could not be converted to the proper Rust type
    pub fn next_data_record(&mut self, keep_commented: bool) -> Result<Option<RunlogDataRec>, GggError> {
        let fformat = self.header.fformat.as_ref().expect("Runlog must have a valid fortran format line in the header.");
        let mut fields = self.header.column_names.iter()
            .map(|s| s.as_str())
            .collect_vec();

        // GGG runlogs don't include a header for the comment marker column, 
        // but we need it for proper deserialization.
        fields.insert(0, "commented");

        loop {
            let line = self.rl_handle.read_data_line()?;
            self.data_line_index += 1;

            if line.len() == 0 {
                // End of file
                return Ok(None)
            }

            if line.chars().nth(0) == Some(':') && !keep_commented {
                continue;
            }

            let data_rec: RunlogDataRec = from_str_with_fields(&line,fformat, &fields)
                .map_err(|e| GggError::DataError { 
                    path: self.rl_handle.path.to_path_buf(),
                    cause: format!("Error deserializing line #{}: {e}", self.curr_line())
                })?;

            
            return Ok(Some(data_rec));
        }
    }
}

impl Iterator for Runlog {
    type Item = RunlogDataRec;

    fn next(&mut self) -> Option<Self::Item> {
        // I don't like this, but because iterators use None to represent the end of iteration,
        // if we hit an error while iterating over the runlog, 
        match self.next_data_record(false) {
            Ok(rec) => rec,
            Err(e) => panic!(
                "Error while reading line {} of runlog at {}: {e}", 
                self.header.nhead + self.data_line_index, 
                self.rl_handle.path.display())
        }
    }
}

/// A alternative iterator for runlogs that will not panic if an error occurs.
/// 
/// When this is used as an iterator, it returns a `Result` instead of the [`RunlogDataRec`]
/// directly. This means that if an error occurs while reading the data line, the program can
/// recover:
/// 
/// ```no_run
/// let runlog = FallibleRunlog::open("~/ggg/runlogs/gnd/pa_ggg_benchmark.grl").unwrap();
/// for (irec, res_data_rec) in runlog.into_iter().enumerate() {
///     match res_data_rec {
///         Err(e) => {
///             println!("Error reading data line {irec} of the runlog, skipping. Error was: {e}")
///         },
///         Ok(rec) => {
///             // process the data record 
///         }
///     }
/// }
/// ```
pub struct FallibleRunlog {
    runlog: Runlog
}

impl FallibleRunlog {
    /// Open a runlog file as a `FallibleRunlog` iterator.
    /// 
    /// # Returns
    /// A [`Result`] containing the `FallibleRunlog` iterator. An error is returned for the same
    /// reasons as [`Runlog::open`].
    pub fn open(runlog: &Path) -> Result<FallibleRunlog, GggError> {
        let rl = Runlog::open(runlog)?;
        Ok(Self { runlog: rl })
    }

    pub fn into_line_iter(self) -> FallibleRunlogLineIter {
        FallibleRunlogLineIter { runlog: self.runlog }
    }
}

impl<'p> From<Runlog> for FallibleRunlog {
    fn from(rl: Runlog) -> Self {
        Self { runlog: rl }
    }
}

impl Iterator for FallibleRunlog {
    type Item = Result<RunlogDataRec, GggError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.runlog.next_data_record(false).transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rstest::{rstest, fixture};
    use super::*;

    #[fixture]
    fn benchmark_rl_path() -> PathBuf {
        let test_data_dir = PathBuf::from(file!())
            .parent().unwrap()
            .parent().unwrap()
            .join("test-data");
        test_data_dir.join("pa_ggg_benchmark.grl")
    }

    #[rstest]
    fn test_runlog_data(benchmark_rl_path: PathBuf) {
        let data_rec_1a = RunlogDataRec {
            commented: false,
            spectrum_name: "pa20040721saaaaa.043".to_string(), year: 2004, day: 203, hour: 20.5956, obs_lat: 45.945, obs_lon: -90.273, obs_alt: 0.442,
            asza: 39.684, poff: 0.0, azim: 242.281, osds: 0.138, opd: 45.02, fovi: 0.0024, fovo: 0.0024, amal: 0.0, ifirst: 530991, ilast: 1460226,
            delta_nu: 0.00753308262, pointer: 108232, bpw: -4, zoff: 0.000, snr: 117, apf: utils::ApodizationFxn::BoxCar, tins: 30.3, pins: 0.9, hins: 99.9,
            tout: 29.1, pout: 950.70, hout: 62.8, sia: 207.5, fvsi: 0.0072, wspd: 1.7, wdir: 125., lasf: 15798.014, wavtkr: 9900., aipl: 0.002
        };

        let mut data_rec_1b = data_rec_1a.clone();
        data_rec_1b.spectrum_name = "pa20040721saaaab.043".to_string();
        data_rec_1b.ifirst = 1460226;
        data_rec_1b.ilast = 1991217;
        data_rec_1b.pointer = 533028;
        data_rec_1b.snr = 147;

        let mut rl = Runlog::open(&benchmark_rl_path).unwrap();

        let test_rec = rl.next_data_record(false)
            .expect("Reading first data line should not error")
            .expect("First data line should not return None");
        approx::assert_abs_diff_eq!(test_rec, data_rec_1a);

        let test_rec = rl.next_data_record(false)
            .expect("Reading first data line should not error")
            .expect("First data line should not return None");
        approx::assert_abs_diff_eq!(test_rec, data_rec_1b);
    }
}


pub struct FallibleRunlogLineIter {
    runlog: Runlog
}

impl Iterator for FallibleRunlogLineIter {
    type Item = (usize, Result<RunlogDataRec, GggError>);

    fn next(&mut self) -> Option<Self::Item> {
        let rec = self.runlog.next_data_record(false).transpose()?;
        // TODO: verify that this returns the correct line (i.e. doesn't need to be called first)
        let line_num = self.runlog.curr_line();
        Some((line_num, rec))
    }
}