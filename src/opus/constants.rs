pub mod bruker {
    use std::fmt::Display;

    use crate::opus::{OpusResult, OpusTypeError, OpusError};

    /// Maximum number of directory blocks
    pub const MDB: i32 = 32;

    /// Maximum header length in bytes
    pub const MHL: i32 = 4096;

    /// Magic number for OPUS format
    pub const MAGIC: i32 = -16905718;

    /// Version number (1992-06-22 - yowza)
    pub const PRGM_VERS: f64 = 920622.0;

    /// Bruker part of block type for amplitude data
    pub const DBB_AMPL: i32 = 3 * 2_i32.pow(0);

    /// Bruker part of block type for sample data
    pub const DBB_SAMP: i32 = 1 * 2_i32.pow(2);

    /// Bruker part of block type for data status parameters
    pub const DBB_DSTAT: i32 = 1 * 2_i32.pow(4);

    /// Bruker part of block type for instrument status parameters
    pub const DBB_INSTR: i32 = 2 * 2_i32.pow(4);

    /// Bruker part of block type for standard aquisition parameters
    pub const DBB_AQPAR: i32 = 3 * 2_i32.pow(4);

    /// Bruker part of block type for Fourier transform parameters
    pub const DBB_FTPAR: i32 = 4 * 2_i32.pow(4);

    /// Made-up part of block type for optics parameters
    pub const DBB_OPTPAR: i32 = 6 * 2_i32.pow(4);

    /// Bruker part of block type for sample origin parameters
    pub const DBB_ORGPAR: i32 = 10 * 2_i32.pow(4);

    /// Bruker part of block type for spectrum data
    pub const DBB_SPEC: i32 = 1 * 2_i32.pow(10);

    /// Bruker part of block type for interferogram data
    pub const DBB_IGRAM: i32 = 2 * 2_i32.pow(10);

    /// Bruker part of block type for directory
    pub const DBB_DIR: i32 = 13 * 2_i32.pow(10);

    /// Made-up part of block type for secondary detector ("slave") data
    pub const DBB_SLAV: i32 = 32 * 2_i32.pow(10);

    /// Parameter type of 4-byte integer
    pub const TYPE_I4: i32 = 0;

    /// Parameter type of 8-byte float
    pub const TYPE_R8: i32 = 1;

    /// Parameter type of string
    pub const TYPE_STRING: i32 = 2;

    /// Parameter type of string "enum"
    pub const TYPE_ENUM: i32 = 3;

    /// Parameter type of string "senum"
    pub const TYPE_SENUM: i32 = 4;

    // The following are derived constants used to identify master/slave (primary/secondary)
    // data and status blocks in the header. The component constants probably act like bit flags.
    const DER_IGRAM_PRI_DATA: i32 = DBB_AMPL + DBB_SAMP + DBB_IGRAM;
    const DER_IGRAM_SEC_DATA: i32 = DER_IGRAM_PRI_DATA + DBB_SLAV;
    const DER_IGRAM_PRI_STAT: i32 = DER_IGRAM_PRI_DATA + DBB_DSTAT;
    const DER_IGRAM_SEC_STAT: i32 = DER_IGRAM_SEC_DATA + DBB_DSTAT;

    // NB: these I'm guessing on, since I2S doesn't use them. -JLL
    const DER_SPEC_PRI_DATA: i32 = DBB_AMPL + DBB_SAMP + DBB_SPEC;
    const DER_SPEC_SEC_DATA: i32 = DER_SPEC_PRI_DATA + DBB_SLAV;
    const DER_SPEC_PRI_STAT: i32 = DER_SPEC_PRI_DATA + DBB_DSTAT;
    const DER_SPEC_SEC_STAT: i32 = DER_SPEC_SEC_DATA + DBB_DSTAT;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display)]
    pub enum BrukerBlockType {
        InstrumentStatus,
        AquisitionParameters,
        FourierTransParameters,
        OpticsParameters,
        SampleOriginParameters,
        Directory,
        IgramPrimaryData,
        IgramSecondaryData,
        SpectrumPrimaryData,
        SpectrumSecondaryData,
        IgramPrimaryStatus,
        IgramSecondaryStatus,
        SpectrumPrimaryStatus,
        SpectrumSecondaryStatus,
        Unknown(i32)
    }

    impl From<i32> for BrukerBlockType {
        fn from(value: i32) -> Self {
            match value {
                // These are blocks identified by a single constant
                DBB_INSTR => Self::InstrumentStatus,
                DBB_AQPAR => Self::AquisitionParameters,
                DBB_FTPAR => Self::FourierTransParameters,
                DBB_OPTPAR => Self::OpticsParameters,
                DBB_ORGPAR => Self::SampleOriginParameters,
                DBB_DIR => Self::Directory,

                // These seem to be acting like bitflags
                DER_IGRAM_PRI_DATA => Self::IgramPrimaryData,
                DER_IGRAM_SEC_DATA => Self::IgramSecondaryData,
                DER_IGRAM_PRI_STAT => Self::IgramPrimaryStatus,
                DER_IGRAM_SEC_STAT => Self::IgramSecondaryStatus,

                DER_SPEC_PRI_DATA => Self::SpectrumPrimaryData,
                DER_SPEC_SEC_DATA => Self::SpectrumSecondaryData,
                DER_SPEC_PRI_STAT => Self::SpectrumPrimaryStatus,
                DER_SPEC_SEC_STAT => Self::SpectrumSecondaryStatus,
                
                _ => Self::Unknown(value)
            }
        }
    }

    impl BrukerBlockType {
        pub fn is_unknown(&self) -> bool {
            if let Self::Unknown(_) = self {
                true
            } else {
                false
            }
        }

        pub fn is_data_block(&self) -> bool {
            match self {
                Self::IgramPrimaryData => true,
                Self::IgramSecondaryData => true,
                Self::SpectrumPrimaryData => true,
                Self::SpectrumSecondaryData => true,
                _ => false
            }
        }

        pub fn is_directory_block(&self) -> bool {
            if let Self::Directory = self {
                true
            } else {
                false
            }
        }
    }

    pub enum BrukerParType {
        Integer,
        Float,
        String,
        Enum,
        Senum,
        Unknown(i32)
    }

    impl BrukerParType {
        pub fn check_par_length(&self, nbytes: usize) -> OpusResult<()> {
            match (self, nbytes) {
                (BrukerParType::Integer, 4) => Ok(()),
                (BrukerParType::Integer, _) => Err(OpusError::ParamLengthMismatch { expected: 4, actual: nbytes }),
                (BrukerParType::Float, 8) => Ok(()),
                (BrukerParType::Float, _) => Err(OpusError::ParamLengthMismatch { expected: 8, actual: nbytes }),
                (BrukerParType::String, _) => Ok(()),
                (BrukerParType::Enum, _) => Ok(()),
                (BrukerParType::Senum, _) => Ok(()),
                (BrukerParType::Unknown(_), _) => Ok(()),
            }
        }
    }

    impl From<i16> for BrukerParType {
        fn from(value: i16) -> Self {
            Self::from(value as i32)
        }
    }

    impl From<i32> for BrukerParType {
        fn from(value: i32) -> Self {
            match value {
                TYPE_I4 => Self::Integer,
                TYPE_R8 => Self::Float,
                TYPE_STRING => Self::String,
                TYPE_ENUM => Self::Enum,
                TYPE_SENUM => Self::Senum,
                _ => Self::Unknown(value)
            }

        }
    }

    impl Display for BrukerParType {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                BrukerParType::Integer => write!(f, "integer"),
                BrukerParType::Float => write!(f, "float"),
                BrukerParType::String => write!(f, "string"),
                BrukerParType::Enum => write!(f, "enum"),
                BrukerParType::Senum => write!(f, "senum"),
                BrukerParType::Unknown(i) => write!(f, "unknown({i})"),
            }
        }
    }

    #[derive(Debug)]
    pub enum BrukerParValue {
        Integer(i32),
        Float(f64),
        String(String),
        Enum(Vec<u8>),
        Senum(Vec<u8>),
        Unknown(Vec<u8>, i32),
    }

    impl BrukerParValue {
        pub fn as_integer(&self) -> Result<i32, OpusTypeError> {
            if let Self::Integer(i) = self {
                Ok(*i)
            } else {
                Err(OpusTypeError::ValueIntoError { expected: BrukerParType::Integer.to_string(), actual: self.opus_type().to_string() })
            }
        }

        pub fn as_float(&self) -> Result<f64, OpusTypeError> {
            if let Self::Float(f) = self {
                Ok(*f)
            } else {
                Err(OpusTypeError::ValueIntoError { expected: BrukerParType::Float.to_string(), actual: self.opus_type().to_string() })
            }
        }

        pub fn into_string(self) -> Result<String, OpusTypeError> {
            if let Self::String(s) = self {
                Ok(s)
            } else {
                Err(OpusTypeError::ValueIntoError { expected: BrukerParType::String.to_string(), actual: self.opus_type().to_string() })
            }
        }

        pub fn as_str(&self) -> Result<&str, OpusTypeError> {
            if let Self::String(s) = self {
                Ok(s)
            } else {
                Err(OpusTypeError::ValueIntoError { expected: BrukerParType::String.to_string(), actual: self.opus_type().to_string() })
            }
        }

        pub fn into_bytes(self) -> Result<Vec<u8>, OpusTypeError> {
            match self {
                BrukerParValue::Integer(_) => Err(OpusTypeError::ValueIntoError { expected: "String, Enum, or Senum".to_string(), actual: self.opus_type().to_string() }),
                BrukerParValue::Float(_) => Err(OpusTypeError::ValueIntoError { expected: "String, Enum, or Senum".to_string(), actual: self.opus_type().to_string() }),
                BrukerParValue::String(s) => Ok(s.into_bytes()),
                BrukerParValue::Enum(b) => Ok(b),
                BrukerParValue::Senum(b) => Ok(b),
                BrukerParValue::Unknown(b, _) => Ok(b)
            }
        }


        pub fn opus_type(&self) -> BrukerParType {
            match self {
                BrukerParValue::Integer(_) => BrukerParType::Integer,
                BrukerParValue::Float(_) => BrukerParType::Float,
                BrukerParValue::String(_) => BrukerParType::String,
                BrukerParValue::Enum(_) => BrukerParType::Enum,
                BrukerParValue::Senum(_) => BrukerParType::Senum,
                BrukerParValue::Unknown(_, i) => BrukerParType::Unknown(*i)
            }
        }
    }
}

pub mod i2s {
    #[derive(Debug, PartialEq, Clone, Copy)]
    pub enum I2sSpectrumHeaderBlockType {
        DstatSpec,
        DstatIgram,
        OrgPar,
        OptPar,
        FtPar,
        AqPar,
        Instr,
        Data,
        Unknown(i32)
    }

    impl From<i32> for I2sSpectrumHeaderBlockType {
        fn from(value: i32) -> Self {
            let btype = value % 2_i32.pow(30);
            if btype % 32768 == 1047 || btype % 32768 == 5151 {
                Self::DstatSpec
            } else if btype % 32768 == 2071 {
                Self::DstatIgram
            } else if btype == 160 {
                Self::OrgPar
            } else if btype == 96 {
                Self::OptPar
            } else if btype == 64 {
                Self::FtPar
            } else if btype == 48 {
                Self::AqPar
            } else if btype == 32 {
                Self::Instr
            } else if btype % 32768 == 1031 || btype % 32768 == 2055 || btype % 32768 == 5135 {
                Self::Data
            } else {
                Self::Unknown(btype)
            }
        }
    }
}