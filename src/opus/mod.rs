use std::{path::{Path, PathBuf}, fs::File, io::{Seek, Read}, str::FromStr, slice::ChunksExact, fmt::{Debug, Display}, collections::HashMap};

use ndarray::Array1;
use crate::{runlogs, utils::{self,GggError}, opus::constants::bruker::BrukerParType};

use self::constants::bruker::{BrukerParValue, BrukerBlockType};

pub mod constants;

pub type OpusResult<T> = Result<T, OpusError>;

#[derive(Debug, thiserror::Error)]
pub enum OpusError {
    #[error("Error reading from Opus file: {0}")]
    ReadError(#[from] std::io::Error),
    #[error("{descr} value did not match expected: expected {expected}, got {actual}")]
    StaticValueMismatch{descr: &'static str, expected: f64, actual: f64},
    #[error("Invalid {pointer_descr} pointer: {inner}")]
    InvalidPointer{pointer_descr: &'static str, inner: OpusPointerError},
    #[error("Could not decode bytes as a UTF-8/ASCII string: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
    #[error("Length of header parameter ({actual}) does not match expected for the type ({expected})")]
    ParamLengthMismatch{expected: usize, actual: usize},
}

#[derive(Debug, thiserror::Error)]
pub enum OpusPointerError {
    #[error("directory pointer is negative")]
    Negative,
    #[error("directory pointer overlaps header")]
    OverlapsHeader,
    #[error("directory pointer is not on a word boundary")]
    NotOnWord
}

#[derive(Debug, thiserror::Error)]
pub enum OpusTypeError {
    #[error("Could not convert Opus generic value into {expected}, was {actual}")]
    ValueIntoError{expected: String, actual: String}
}

#[derive(Debug)]
pub struct MissingOpusParameterError {
    block: BrukerBlockType,
    parameter: String,
    block_missing: bool
}

impl Display for MissingOpusParameterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.block_missing {
            write!(f, "Requested block {:?} missing from Opus header", self.block)
        } else {
            write!(f, "Requested parameter {} from block {:?} missing from Opus header", self.parameter, self.block)
        }
    }
}

impl std::error::Error for MissingOpusParameterError {}

pub struct Spectrum {
    pub path: PathBuf,
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
pub fn read_spectrum_from_runlog_rec(data_rec: &runlogs::RunlogDataRec, data_part: &utils::DataPartition) -> Result<Spectrum, GggError> {
    let spec_file = if let Some(f) = data_part.find_spectrum(&data_rec.spectrum_name) {
        f
    }else{
        return Err(GggError::CouldNotOpen { 
            descr: "spectrum".to_owned(), 
            path: PathBuf::from_str(&data_rec.spectrum_name).unwrap(), 
            reason: "spectrum not found".to_owned()
        })
    };

    read_spectrum(
        spec_file,
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
pub fn read_spectrum(spec_file: PathBuf, bpw: i8, ifirst: usize, delta_nu: f64, pointer: i32) -> Result<Spectrum, GggError> {
    let mut spec_h = File::open(&spec_file)
        .or_else(|e| Err(GggError::CouldNotOpen { descr: "spectrum".to_owned(), path: spec_file.to_owned(), reason: e.to_string() }))?;

    // For now, just seek past the header because we're not reading it
    spec_h.seek(std::io::SeekFrom::Start(pointer as u64))
        .or_else(|e| Err(GggError::CouldNotRead { path: spec_file.to_owned(), reason: format!("{e} (while moving past header)") }))?;

    // Next just read in the rest of the file 
    let mut buf = vec![];
    spec_h.read_to_end(&mut buf)
        .or_else(|e| Err(GggError::CouldNotRead { path: spec_file.to_owned(), reason: format!("{e} (while reading spectrum data)") }))?;

    let spec = SpecBytesToFloat::convert_spectrum(&buf, bpw)?;
    let npts = spec.len();
    
    let mut freq = ndarray::Array1::zeros(npts);
    
    for i in 0..npts{
        freq[i] = (delta_nu as f32) * (i + ifirst) as f32;
    }

    Ok(Spectrum { path: spec_file, freq, spec })
}

/// A converter that handles the various Opus spectrum formats
/// 
/// To use: call `convert_spectrum` with the raw bytes read from the Opus spectrum.
enum SpecBytesToFloat {
    IntBigEndian,
    IntLittleEndian,
    FloatBigEndian,
    FloatLittleEndian
}

impl SpecBytesToFloat {
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
            SpecBytesToFloat::IntBigEndian => {
                let i = i16::from_be_bytes(bytes.try_into().expect(&format!("Passed the wrong number of bytes to BytesToFloat::convert, expected 2, got {}", bytes.len())));
                (i as f32) / 15000.0
            },
            SpecBytesToFloat::IntLittleEndian => {
                let i = i16::from_le_bytes(bytes.try_into().expect(&format!("Passed the wrong number of bytes to BytesToFloat::convert, expected 2, got {}", bytes.len())));
                (i as f32) / 15000.0
            },
            SpecBytesToFloat::FloatBigEndian => {
                f32::from_be_bytes(bytes.try_into().expect(&format!("Passed the wrong number of bytes to BytesToFloat::convert, expected 4, got {}", bytes.len())))
            },
            SpecBytesToFloat::FloatLittleEndian => {
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
pub fn get_spectrum_num_points(spec_name: &str, data_part: &utils::DataPartition, pointer: i32, bytes_per_word: i8) -> Result<u64, std::io::Error> {
    let p = data_part.find_spectrum(spec_name)
        .ok_or_else(|| std::io::Error::new(
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



fn read_spectrum_header(spec_name: &str) {
    
}

struct HeaderByteReader {
    is_big_endian: bool,
}

impl Default for HeaderByteReader {
    fn default() -> Self {
        // It looks like Opus headers always use little-endian?
        Self { is_big_endian: false }
    }
}

impl HeaderByteReader {
    /// Read two bytes and interpret them as an i16
    fn read_i16(&self, f: &mut std::fs::File) -> OpusResult<i16> {
        let mut buf = [0; 2];
        f.read_exact(&mut buf)?;

        if self.is_big_endian {
            Ok(i16::from_be_bytes(buf))
        } else {
            Ok(i16::from_le_bytes(buf))
        }
    }

    /// Read four bytes and intepret them as an i32
    fn read_i32(&self, f: &mut std::fs::File) -> OpusResult<i32> {
        let mut buf = [0; 4];
        f.read_exact(&mut buf)?;

        if self.is_big_endian {
            Ok(i32::from_be_bytes(buf))
        } else {
            Ok(i32::from_le_bytes(buf))
        }
    }

    /// Read eight bytes and interpret them as an f64
    fn read_f64(&self, f: &mut std::fs::File) -> OpusResult<f64> {
        let mut buf = [0; 8];
        f.read_exact(&mut buf)?;

        if self.is_big_endian {
            Ok(f64::from_be_bytes(buf))
        } else {
            Ok(f64::from_le_bytes(buf))
        }
    }

    /// Read `string_length` bytes and interpret them as a UTF-8 string
    /// 
    /// Note that:
    /// - this may return an error if the bytes are not valid UTF-8 (which includes ASCII as a subset)
    /// - the returned string will have any trailing 0 bytes removed
    fn read_string(&self, f: &mut std::fs::File, string_length: usize) -> OpusResult<String> {
        let mut buf = Vec::with_capacity(string_length);
        buf.resize(string_length, 0);
        f.read_exact(&mut buf)?;
        // Big/little endian should only affect the order of the bytes in the word, not the bits
        // in the byte. Since we're dealing with 1 byte characters, endianness shouldn't matter.
        // However, these are null-terminated strings so we need to cut them off at the first
        // byte == 0
        let inull = buf.iter().position(|&b| b == 0).unwrap_or_else(|| buf.len());
        let s = String::from_utf8(buf[..inull].to_vec())?;
        Ok(s.trim_end_matches(char::from(0)).to_string())
    }

    fn read_bytes(&self, f: &mut std::fs::File, nbytes: usize) -> OpusResult<Vec<u8>> {
        let mut buf = Vec::with_capacity(nbytes);
        buf.resize(nbytes, 0);
        f.read_exact(&mut buf)?;
        // Big/little endian should only affect the order of the bytes in the word, not the bits
        // in the byte. Since we're dealing with 1 byte characters, endianness shouldn't matter.
        Ok(buf)
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
struct HeaderBlockDef<T: Copy + Clone> {
    itype: T,
    ilen: usize,
    ipoint: usize,
}


#[derive(Debug, PartialEq)]
pub struct IgramHeaderMetadata {
    pointer: usize,
    dir_max_size: usize,
    num_dirs: usize,
    blocks: Vec<HeaderBlockDef<constants::bruker::BrukerBlockType>>
}

impl IgramHeaderMetadata {
    fn read_from_file(igram_file: &mut std::fs::File, reader: &HeaderByteReader, include_unknown_blocks: bool) -> OpusResult<Self> {
        // Check the magic value
        let igram_magic = reader.read_i32(igram_file)?;
        if igram_magic != constants::bruker::MAGIC {
            return Err(OpusError::StaticValueMismatch { descr: "magic", expected: constants::bruker::MAGIC.into(), actual: igram_magic.into() })
        }

        let version = reader.read_f64(igram_file)?;
        if (version - constants::bruker::PRGM_VERS).abs() > 0.01 {
            return Err(OpusError::StaticValueMismatch { descr: "program_version", expected: constants::bruker::PRGM_VERS, actual: version })
        }

        // Now get the directory pointer and check it is valid
        let dir_pointer = reader.read_i32(igram_file)?;
        if dir_pointer < 0 {
            return Err(OpusError::InvalidPointer { 
                pointer_descr: "directory", inner: OpusPointerError::Negative
            });
        }
        if dir_pointer < 24 {
            return Err(OpusError::InvalidPointer { 
                pointer_descr: "directory", inner: OpusPointerError::OverlapsHeader
            });
        }
        if dir_pointer % 4 != 0 {
            return Err(OpusError::InvalidPointer { 
                pointer_descr: "directory", inner: OpusPointerError::NotOnWord
            });
        }

        // Next the maximum and actual number of directory entries
        let max_dir_size = reader.read_i32(igram_file)?;
        if max_dir_size < 0 {
            return Err(OpusError::InvalidPointer { 
                pointer_descr: "directory max size", inner: OpusPointerError::Negative
            })
        }

        let curr_num_dir = reader.read_i32(igram_file)?;
        if curr_num_dir < 0 {
            return Err(OpusError::InvalidPointer { 
                pointer_descr: "directory current size", inner: OpusPointerError::Negative
            });
        }

        // Advance the file to the start of the directory block and read in the list of such blocks
        igram_file.seek(std::io::SeekFrom::Start(dir_pointer as u64))?;
        let mut blocks = vec![];
        for _ in 0..curr_num_dir {
            let block_type = reader.read_i32(igram_file)? % 2_i32.pow(30);
            let block_length = reader.read_i32(igram_file)?;
            let block_pointer = reader.read_i32(igram_file)?;

            let block = HeaderBlockDef{
                itype: constants::bruker::BrukerBlockType::from(block_type),
                ilen: block_length as usize,
                ipoint: block_pointer as usize,
            };

            if !block.itype.is_unknown() || include_unknown_blocks {
                blocks.push(block);
            }
        }

        Ok(Self { pointer: dir_pointer as usize, dir_max_size: max_dir_size as usize, num_dirs: curr_num_dir as usize, blocks })
    }
}


#[derive(Debug)]
pub struct IgramHeader {
    metadata: IgramHeaderMetadata,
    parameter_blocks: HashMap<BrukerBlockType, HashMap<String, BrukerParValue>>
}

impl IgramHeader {
    pub fn read_full_igram_header(inteferogram: &Path) -> OpusResult<IgramHeader> {
        let mut igm = std::fs::File::open(inteferogram)?;
        // This assumes that Opus igrams are always little endian - need to confirm that.
        let byte_reader = HeaderByteReader::default();
        let header_metadata = IgramHeaderMetadata::read_from_file(&mut igm, & byte_reader, false)?;
        let mut block_values = HashMap::new();

        for block_definition in header_metadata.blocks.iter() {
            if block_definition.itype.is_data_block() || block_definition.itype.is_directory_block() {
                // skip the data blocks for now, we just want header information
                // also skip directory block; we don't use it and it's a lot of data that clutters up printing the struct.
                continue;
            }

            let bv = Self::read_param_block(block_definition, &byte_reader, &mut igm)?;
            block_values.insert(block_definition.itype, bv);
        }

        Ok(IgramHeader {
            metadata: header_metadata,
            parameter_blocks: block_values
        })
    }

    fn read_param_block(header_def: &HeaderBlockDef<constants::bruker::BrukerBlockType>, reader: &HeaderByteReader, f: &mut std::fs::File) -> OpusResult<HashMap<String, BrukerParValue>> {
        let mut parameters = HashMap::new();
        f.seek(std::io::SeekFrom::Start(header_def.ipoint as u64))?;
        loop {
            // One would think that the length parameter given in the directory entry for each block is how many bytes
            // that block consists of, but comparing with the Perl OpusHdr, stopping after reading that many bytes resulted
            // in fewer parameters from this function than the Perl output. Looking back at i2s, it only stops searching for
            // parameters when the next *parameter length* is 0, so that's what we do here.
            match Self::read_param(reader, f)? {
                Some((param_key, param_val, _)) => { parameters.insert(param_key, param_val); },
                None => break,
            }
        }

        Ok(parameters)
    }

    /// Read the next parameter from the Opus header
    /// 
    /// # Inputs
    /// - `reader`: the `HeaderByteReader` configured to correctly interpret the bytes in the header
    /// - `f`: handle to the Opus file/slice, it must be positioned so that the next byte read is the first
    /// byte of the parameter name.
    /// 
    /// # Outputs
    /// - The parameter name (any null characters are trimmed)
    /// - The parameter value
    /// - The number of bytes read
    /// 
    /// If the parameter size was 0, then this returns `Ok(None)` to indicate that we've reached the
    /// end of valid parameters in an Opus header block.
    fn read_param(reader: &HeaderByteReader, f: &mut std::fs::File) -> OpusResult<Option<(String, BrukerParValue, usize)>> {
        // Each Bruker parameter should consist of:
        //  - 4 bytes for the parameter name
        //  - 2 bytes for the parameter type
        //  - 2 bytes for the number of two-byte units making up the value
        //  - the value itself

        let param_key = reader.read_string(f, 4)?;
        let param_type = reader.read_i16(f)?;
        let param_nbytes = reader.read_i16(f)? as usize * 2;
        if param_nbytes == 0 {
            // This function may be called on a parameter that doesn't actually exist, which is when the number of bytes = 0.
            // Indicate that by returning a None.
            return Ok(None)
        }

        // TODO: Enums might still need debugging; in the IgramSecondaryStatus block, which I think matches up to the 
        // "Data Parameters IgSm/2.Chn." output of OpusHdr, my code has DXU = WN, but OpusHdr has DXU2 = PNT. Except - 
        // there's a *second* "Data Parameters IgSm/2.Chn." block output from OpusHdr which *does* have DXU2 = WN - what!?
        let param_type: BrukerParType = param_type.into();
        param_type.check_par_length(param_nbytes)?;
        let param_value = match param_type {
            BrukerParType::Integer => BrukerParValue::Integer(reader.read_i32(f)?),
            BrukerParType::Float => BrukerParValue::Float(reader.read_f64(f)?),
            BrukerParType::String => BrukerParValue::String(reader.read_string(f, param_nbytes)?),
            BrukerParType::Enum => BrukerParValue::Enum(reader.read_bytes(f, param_nbytes)?),
            BrukerParType::Senum => BrukerParValue::Senum(reader.read_bytes(f, param_nbytes)?),
            BrukerParType::Unknown(i) => BrukerParValue::Unknown(reader.read_bytes(f, param_nbytes)?, i),
        };
        
        Ok(Some((param_key, param_value, 4 + 2 + 2 + param_nbytes)))
    }

    pub fn read_slices_header(slices: &[&Path]) {
        // From I2S: all slices but the last one should have data blocks for each channel, data status
        // blocks for each channel, and an acqisition parameter block. The final slice has the instrument
        // status, optics, and sample origin parameters, but no data/data status blocks. The exception is
        // a single slice scan, which looks more like a regular igram in that it has all 6 parameter blocks.
        //
        // In get_opus_xx.f's get_opus_i4 documentation, it states that it checks the parameter value matches
        // all following slices (if `slicecnt` is > 1). So this should also scan all slices.
        todo!()
    }

    pub fn get_value(&self, block: constants::bruker::BrukerBlockType, parameter: &str) -> Result<&BrukerParValue, MissingOpusParameterError> {
        self.parameter_blocks
            .get(&block)
            .ok_or_else(|| MissingOpusParameterError{ block, parameter: parameter.to_string(), block_missing: true})?
            .get(parameter)
            .ok_or_else(|| MissingOpusParameterError { block, parameter: parameter.to_string(), block_missing: false })
    }
}

#[derive(Debug, PartialEq)]
struct SpectrumHeaderMetadata {
    magic: i32,
    prog: f64,
    pointer: usize,
    max_size: usize,
    curr_size: usize,
    blocks: Vec<HeaderBlockDef<constants::i2s::I2sSpectrumHeaderBlockType>>
}




impl SpectrumHeaderMetadata {
    fn read_from_file(f: &mut std::fs::File, byte_reader: &mut HeaderByteReader) -> OpusResult<Self> {
        // TODO: test with (correct) spectrum instead of (wrong) igram
        let magic = byte_reader.read_i32(f)?;
        let prog = byte_reader.read_f64(f)?; // TODO: this is a 4-byte int in `read_opus_header` but 8 byte float in `get_opusigram_param` - need to handle both cases or check that read_opus_header isn't actually taking 8 bytes
        let pointer = byte_reader.read_i32(f)? as usize;
        let max_size = byte_reader.read_i32(f)? as usize;
        let curr_size = byte_reader.read_i32(f)? as usize;

        let mut blocks = vec![];
        for _ in 0..curr_size {
            let itype = byte_reader.read_i32(f)?;
            let ilen = byte_reader.read_i32(f)? as usize;
            let ipoint = byte_reader.read_i32(f)? as usize;
            blocks.push(HeaderBlockDef{itype: itype.into(), ilen, ipoint});
        }

        Ok(Self { magic, prog, pointer, max_size, curr_size, blocks })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_igram_header_metadata() {
        let gggpath = utils::get_ggg_path().unwrap();
        let mut wg = std::fs::File::open(gggpath.join("src/i2s/raw_data/wg20090206_1640NIR_DC.0")).unwrap();
        let mut br = HeaderByteReader::default();
        let meta = IgramHeaderMetadata::read_from_file(&mut wg, &mut br, false).unwrap();
        // TODO: vet this against I2S or something and fill in the actual expected values.
        // let expected_meta = IgramHeaderMetadata{ pointer: 0, dir_max_size: 0, num_dirs: 0, blocks: vec![] };
        println!("{meta:#?}");
        assert!(false, "This test is not complete yet");
    }

    #[test]
    fn test_slice_header_metadata() {
        let gggpath = utils::get_ggg_path().unwrap();
        // With slices, all but the last slice have data, the last slice has remaining metadata.
        let mut slice = std::fs::File::open(gggpath.join("src/i2s/raw_data/040721.1/scan/b211127.0")).unwrap();
        // let mut slice = std::fs::File::open(gggpath.join("src/i2s/raw_data/040721.1/scan/b211880.0")).unwrap();
        let mut br = HeaderByteReader::default();
        let meta = IgramHeaderMetadata::read_from_file(&mut slice, &mut br, false).unwrap();
        println!("{meta:#?}");
        assert!(false, "This test is not complete yet");
    }

    #[test]
    fn test_igram_header() {
        let gggpath = utils::get_ggg_path().unwrap();
        let wg = gggpath.join("src/i2s/raw_data/wg20090206_1640NIR_DC.0");
        // TODO: need context for header errors (which block/parameter). Need to decide if going to use error-stack/eyre 
        //  or build this into the OpusError type. Could do a struct which holds an OpusError plus the block and param name.
        let header = IgramHeader::read_full_igram_header(&wg).unwrap();
        println!("{:#?}", header);
        assert!(false, "This test is not complete yet");
    }
}