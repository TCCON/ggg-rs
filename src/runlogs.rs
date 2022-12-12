//! Utilities for reading runlogs and iterating over their data records.
use std::{path::Path, io::BufReader, fs::File, str::FromStr};

use crate::utils::{self, GggError};

pub const NUM_RUNLOG_COLS: usize = 36;

/// A struct representing one line of a GGG2020 runlog.
#[derive(Debug)]
pub struct RunlogDataRec {
    /// The name of the spectrum
    pub spectrum_name: String,
    /// The year the data is from
    pub year: i32,
    /// The day-of-year the data is from
    pub day: i32,
    /// The fractional UTC hour when zero-path-difference occured
    pub hour: f64,
    /// The observation latitude (south is negative)
    pub obs_lat: f64,
    /// The observation longitude (west is negative)
    pub obs_lon: f64,
    /// The observation altitude in kilometers
    pub obs_alt: f64,
    /// The astronomical solar zenith angle in degrees
    pub asza: f64,
    /// The pointing offset in degrees
    pub poff: f64,
    /// The azimuth angle in degrees
    pub azim: f64,
    /// Observer-sun doppler stretch
    pub osds: f64,
    /// Optical path difference
    pub opd: f64,
    /// Internal FOV diameter in radians
    pub fovi: f64,
    /// External FOV diameter in radians
    pub fovo: f64,
    /// Angular misalignment
    pub amal: f64,
    /// Index of first spectral point (lowest frequency/point spacing)
    pub ifirst: usize,
    /// Index of last spectral point (highest frequency/point spacing)
    pub ilast: usize,
    /// Spectral point sampling
    pub delta_nu: f64,
    /// Byte offset of the first spectral point (length of header in bytes)
    pub pointer: i32,
    /// Bytes per data word (i.e. intensity value, big endian is positive, little endian is negative)
    pub bpw: i8,
    /// Zero level offset as a fraction
    pub zoff: f64,
    /// Signal to noise ratio
    pub snr: f64,
    /// Apodization function
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
pub struct Runlog<'p> {
    pub header: utils::CommonHeader,
    rl_handle: utils::FileBuf<'p, BufReader<File>>,
    data_line_index: usize
}

impl<'p> Runlog<'p> {
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
    pub fn open(runlog: &'p Path) -> Result<Runlog<'p>, GggError> {
        let mut rl = utils::FileBuf::open(runlog)?;
        let header = utils::read_common_header(&mut rl)?;
        if header.ncol != NUM_RUNLOG_COLS {
            return Err(GggError::HeaderError { 
                path: runlog.to_owned(), 
                cause: format!("Number of columns specified in the header of runlog {} is not the expected number, {}",
                               header.ncol, NUM_RUNLOG_COLS)
            });
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
    pub fn next_data_record(&mut self) -> Result<Option<RunlogDataRec>, GggError> {
        fn parse<'r, T: FromStr>(rl: &mut Runlog<'r>, s: &str, field: &str) -> Result<T, GggError> {
            match s.parse::<T>() {
                Ok(v) => Ok(v),
                Err(_) => {
                    let path = rl.rl_handle.path.to_owned();
                    let cause = format!("Could not convert value {s} for {field} on line {}", rl.curr_line() );
                    Err(GggError::DataError { path, cause })
                }
            }
        }

        // TODO: skip "commented out" lines and blank lines?
        let line = self.rl_handle.read_data_line()?;
        self.data_line_index += 1;

        if line.len() == 0 {
            // End of file
            return Ok(None)
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() != self.header.ncol {
            return Err(GggError::DataError { 
                path: self.rl_handle.path.to_owned(), 
                cause: format!("Line {} of runlog at {} has a different number of columns ({}) than specified in the header ({})", 
                               self.curr_line(), self.rl_handle.path.display(), parts.len(), self.header.ncol)
            });
        }

        // TODO: implement reading GGG files as custom serde
        Ok(Some(RunlogDataRec { 
            spectrum_name: parts[0].to_owned(), 
            year: parse(self, parts[1], "year")?, 
            day: parse(self, parts[2], "day")?, 
            hour: parse(self, parts[3], "hour")?, 
            obs_lat: parse(self, parts[4], "obs_lat")?, 
            obs_lon: parse(self, parts[5], "obs_lon")?, 
            obs_alt: parse(self, parts[6], "obs_alt")?, 
            asza: parse(self, parts[7], "asza")?, 
            poff: parse(self, parts[8], "poff")?, 
            azim: parse(self, parts[9], "azim")?, 
            osds: parse(self, parts[10], "osds")?, 
            opd: parse(self, parts[11], "opd")?, 
            fovi: parse(self, parts[12], "fovi")?, 
            fovo: parse(self, parts[13], "fovo")?, 
            amal: parse(self, parts[14], "amal")?, 
            ifirst: parse(self, parts[15], "ifirst")?, 
            ilast: parse(self, parts[16], "ilast")?, 
            delta_nu: parse(self, parts[17], "delta_nu")?,
            pointer: parse(self, parts[18], "pointer")?, 
            bpw: parse(self, parts[19], "bpw")?,
            zoff: parse(self, parts[20], "zoff")?,
            snr: parse(self, parts[21], "snr")?, 
            apf: parse(self, parts[22], "apf")?, 
            tins: parse(self, parts[23], "tins")?, 
            pins: parse(self, parts[24], "pins")?, 
            hins: parse(self, parts[25], "hins")?, 
            tout: parse(self, parts[26], "tout")?, 
            pout: parse(self, parts[27], "pout")?, 
            hout: parse(self, parts[28], "hout")?, 
            sia: parse(self, parts[29], "sia")?,
            fvsi: parse(self, parts[30], "fvsi")?, 
            wspd: parse(self, parts[31], "wspd")?, 
            wdir: parse(self, parts[32], "wdir")?, 
            lasf: parse(self, parts[33], "lasf")?,
            wavtkr: parse(self, parts[34], "wavtkr")?, 
            aipl: parse(self, parts[35], "aipl")? 
        }))
    }
}

impl<'p> Iterator for Runlog<'p> {
    type Item = RunlogDataRec;

    fn next(&mut self) -> Option<Self::Item> {
        // I don't like this, but because iterators use None to represent the end of iteration,
        // if we hit an error while iterating over the runlog, 
        match self.next_data_record() {
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
pub struct FallibleRunlog<'p> {
    runlog: Runlog<'p>
}

impl<'p> FallibleRunlog<'p> {
    /// Open a runlog file as a `FallibleRunlog` iterator.
    /// 
    /// # Returns
    /// A [`Result`] containing the `FallibleRunlog` iterator. An error is returned for the same
    /// reasons as [`Runlog::open`].
    pub fn open(runlog: &'p Path) -> Result<FallibleRunlog<'p>, GggError> {
        let rl = Runlog::open(runlog)?;
        Ok(Self { runlog: rl })
    }

    // TODO: implement a non-consuming iteration so we can use curr_line()?
}

impl<'p> From<Runlog<'p>> for FallibleRunlog<'p> {
    fn from(rl: Runlog<'p>) -> Self {
        Self { runlog: rl }
    }
}

impl<'p> Iterator for FallibleRunlog<'p> {
    type Item = Result<RunlogDataRec, GggError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.runlog.next_data_record().transpose()
    }
}