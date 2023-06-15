use std::{path::{Path, PathBuf}, fs::File, io::{Seek, Read}, str::FromStr, slice::ChunksExact};

use ndarray::Array1;
use crate::{runlogs, utils::{self,GggError}};

pub struct Spectrum {
    pub freq: Array1<f32>,
    pub spec: Array1<f32>
}

/// Read the spectrum pointed to by a runlog data record
/// 
/// Aside from the input types, this differs from [`read_spectrum`] in that this uses [`utils::find_spectrum`]
/// to search for the spectrum named in the data record, rather than requiring the exact path to the spectrum
/// to be given.
/// 
/// In addition to the `Err` cases for [`read_spectrum`], this function will return an `Err` if:
/// 
/// * reading the `$GGGPATH/config/data_part.lst` file fails, or
/// * the spectrum named cannot be found in any of the directories listed in `$GGGPATH/config/data_part.lst`.
pub fn read_spectrum_from_runlog_rec(data_rec: &runlogs::RunlogDataRec) -> Result<Spectrum, GggError> {
    let spec_file = if let Some(f) = utils::find_spectrum(&data_rec.spectrum_name)? {
        f
    }else{
        return Err(GggError::CouldNotOpen { 
            descr: "spectrum".to_owned(), 
            path: PathBuf::from_str(&data_rec.spectrum_name).unwrap(), 
            reason: "spectrum not found".to_owned()
        })
    };

    read_spectrum(
        &spec_file,
        data_rec.bpw, 
        data_rec.ifirst,  
        data_rec.delta_nu, 
        data_rec.pointer
    )
}

/// Read an Opus-format binary spectrum.
/// 
/// # Parameters
/// * `spec_file` - path to the spectrum file
/// * `bpw` - the bytes-per-word value from the runlog/spectrum header. Used to interpret how the binary data is converted;
///   +/- 2 means it is interpreted as a series of `i16` values, +/- 4 means it is interpreted as a series of `f32` values.
///   Positive means big endian, negative means little endian. 
/// * `ifirst` - the number of spectral points between 0 and the first of the spectrum, i.e. the frequency of the first point
///   will be `ifirst * delta_nu`.
/// * `delta_nu` - the wavenumber spacing between adjacent spectral points.
/// * `pointer` - the number of bytes that make up the header in the spectrum, i.e. the address of the first spectral data in
///   the file.
/// 
/// # Returns
/// A [`Result`] containing the [`Spectrum`] structure with the frequencies and spectral values. An `Err` will be returned if:
/// 
/// * the `spec_file` could not be opened,
/// * moving the file pointer past the header fails,
/// * reading in the spectral data fails,
/// * the number of bytes of spectral data is not a multiple of `bpw.abs()`, or
/// * an unimplemented `bpw` value is passed,
/// 
/// # See also
/// * [`read_spectrum_from_runlog_rec`] - read the spectrum defined by a runlog data record
pub fn read_spectrum(spec_file: &Path, bpw: i8, ifirst: usize, delta_nu: f64, pointer: i32) -> Result<Spectrum, GggError> {
    let mut spec_h = File::open(spec_file)
        .or_else(|e| Err(GggError::CouldNotOpen { descr: "spectrum".to_owned(), path: spec_file.to_owned(), reason: e.to_string() }))?;

    // For now, just seek past the header because we're not reading it
    spec_h.seek(std::io::SeekFrom::Start(pointer as u64))
        .or_else(|e| Err(GggError::CouldNotRead { path: spec_file.to_owned(), reason: format!("{e} (while moving past header)") }))?;

    // Next just read in the rest of the file 
    let mut buf = vec![];
    spec_h.read_to_end(&mut buf)
        .or_else(|e| Err(GggError::CouldNotRead { path: spec_file.to_owned(), reason: format!("{e} (while reading spectrum data)") }))?;

    let spec = BytesToFloat::convert_spectrum(&buf, bpw)?;
    let npts = spec.len();
    
    let mut freq = ndarray::Array1::zeros(npts);
    
    for i in 0..npts{
        freq[i] = (delta_nu as f32) * (i + ifirst) as f32;
    }

    Ok(Spectrum { freq, spec })
}

/// A converter that handles the various Opus spectrum formats
/// 
/// To use: call `convert_spectrum` with the raw bytes read from the Opus spectrum.
enum BytesToFloat {
    IntBigEndian,
    IntLittleEndian,
    FloatBigEndian,
    FloatLittleEndian
}

impl BytesToFloat {
    /// Convert spectrum bytes into a float32 array
    /// 
    /// # Parameters
    /// * `buf` - the slice of bytes read from the spectrum
    /// * `bpw` - the number of bytes per spectrum point. Only +/- 2 and +/- 4 currently implemented.
    ///   Negative is treated as little endian, 2 BPW values are interpreted as i16s and 4 BPWs as f32s.
    fn convert_spectrum(buf: &[u8], bpw: i8) -> Result<ndarray::Array1<f32>, GggError> {
        let (me, chunks) = Self::new_from_buf(buf, bpw)?;
        let npts = buf.len() / (bpw.abs() as usize);
        let mut spec = ndarray::Array1::zeros(npts);
        for (i, bytes) in chunks.enumerate() {
            spec[i] = me.convert(bytes);
        }
        Ok(spec)
    }


    /// Create the appropriate variant for the given bytes per word and sets up the correct chunk iterator
    fn new_from_buf<'b>(buf: &'b [u8], bpw: i8) -> Result<(Self, ChunksExact<'b, u8>), GggError> {
        let abs_bpw = bpw.abs() as usize;
        if buf.len() % abs_bpw != 0 {
            // Maybe this could just be a warning?
            return Err(GggError::DataError { path: PathBuf::new(), cause: format!("Spectrum did not have a number of bytes evenly divisible by the bpw, {abs_bpw}") });
        }

        let me = match bpw {
            2 => Self::IntBigEndian,
            -2 => Self::IntLittleEndian,
            4 => Self::FloatBigEndian,
            -4 => Self::FloatLittleEndian,
            _ => return Err(GggError::NotImplemented(format!("reading spectra with bpw = {bpw}")))
        };

        let iter = buf.chunks_exact(abs_bpw);
        Ok((me, iter))
    }
    
    /// Convert one set of bytes to an f32 value
    /// 
    /// # Panics
    /// Will panic if given a slice of bytes with a different length than the variant is expecting (2 for the ints and 4 for the floats).
    /// It is not recommended to call this method directly but instead to use `convert_spectrum`, which ensures the correct chunking is done.
    fn convert(&self, bytes: &[u8]) -> f32 {
        match self {
            BytesToFloat::IntBigEndian => {
                let i = i16::from_be_bytes(bytes.try_into().expect(&format!("Passed the wrong number of bytes to BytesToFloat::convert, expected 2, got {}", bytes.len())));
                (i as f32) / 15000.0
            },
            BytesToFloat::IntLittleEndian => {
                let i = i16::from_le_bytes(bytes.try_into().expect(&format!("Passed the wrong number of bytes to BytesToFloat::convert, expected 2, got {}", bytes.len())));
                (i as f32) / 15000.0
            },
            BytesToFloat::FloatBigEndian => {
                f32::from_be_bytes(bytes.try_into().expect(&format!("Passed the wrong number of bytes to BytesToFloat::convert, expected 4, got {}", bytes.len())))
            },
            BytesToFloat::FloatLittleEndian => {
                f32::from_le_bytes(bytes.try_into().expect(&format!("Passed the wrong number of bytes to BytesToFloat::convert, expected 4, got {}", bytes.len())))
            },

        }
    }
}

/// Calculates the number of points in an Opus binary spectrum
/// 
/// # Parameters
/// * `spec_name` - the file name of the spectrum. The path is not needed, the paths
///   configued in `$GGGPATH/config/data_part.lst` are searched to find this spectrum.
/// * `pointer` - the pointer value from the runlog data record for this spectrum.
/// * `bytes_per_word` - the BPW value from the runlog data record for this spectrum.
/// 
/// # Returns
/// The number of data points expected from the spectrum. It will return an error if:
/// * it cannot find the spectrum,
/// * it cannot open the spectrum file, or
/// * it cannot get the size of the spectrum file.
pub fn get_spectrum_num_points(spec_name: &str, pointer: i32, bytes_per_word: i8) -> Result<u64, std::io::Error> {
    let p = utils::find_spectrum_result(spec_name)
        .map_err(|_| std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Unable to find spectrum {spec_name}")
        ))?;
    let f = std::fs::File::open(p)?;
    let meta = f.metadata()?;
    let file_length = meta.len();
    let pointer = pointer as u64;
    let abpw = bytes_per_word.abs() as u64;
    Ok((file_length - pointer) / abpw)
}
