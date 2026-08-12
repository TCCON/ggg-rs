use std::{io::Write, path::Path, unimplemented};

use chrono::{Datelike, Timelike, Utc};
use error_stack::ResultExt;
use fortformat::FortFormat;
use serde::{Deserialize, Serialize};

use crate::{
    opus::{constants::bruker::BrukerBlockType, IgramHeader},
    utils::GggError,
};

const SUNRUN_MISSING: f64 = -999.0;
static SUNRUN_FMT_STR: &'static str = "(1x,a57,1x,i2,f8.0,f9.4,f10.4,f7.3,f6.1,f8.2,f6.1,f6.1,f8.2,f6.1,f7.1,f7.4,f6.1,f6.0,1x,2f7.0,f11.8,f11.3,f7.0,f7.3,f6.2)";
static SUNRUN_FMT: std::sync::OnceLock<FortFormat> = std::sync::OnceLock::new();

/// Needed for serde default
fn sunrun_missing() -> f64 {
    SUNRUN_MISSING
}

/// A struct representing values found in a row of a sunrun file
#[derive(Debug, Clone, Serialize, Deserialize, field_names::FieldNames)]
pub struct SunrunRow {
    pub spectrum_name: String,
    pub object: Object,
    pub tcorr: f64,
    pub oblat: f64,
    pub oblon: f64,
    pub obalt: f64,
    pub tins: f64,
    pub pins: f64,
    pub hins: f64,
    pub tout: f64,
    pub pout: f64,
    pub hout: f64,
    pub sia: f64,
    pub fvsi: f64,
    pub wspd: f64,
    pub wdir: f64,
    pub nus: f64,
    pub nue: f64,
    pub fsf: f64,
    pub lasf: f64,
    pub wavtkr: f64,
    pub aipl: f64,
    pub tm: f64,
}

impl SunrunRow {
    /// Serialize the row and write it out.
    ///
    /// Usually `f` will be a file handle (from [`std::fs::File::create`]),
    /// but any writable object will do.
    pub fn write<W: std::io::Write>(&self, f: W) -> error_stack::Result<(), GggError> {
        let fmt = SUNRUN_FMT.get_or_init(|| {
            FortFormat::parse(SUNRUN_FMT_STR)
                .expect("The predefined sunrun format string must be a valid fortran format")
        });
        let settings = fortformat::ser::SerSettings::default().align_left_str(true);
        fortformat::ser::to_writer_custom(self, fmt, Some(&SunrunRow::FIELDS), &settings, f)
            .change_context_lazy(|| {
                GggError::context(format!(
                    "Error writing or serializing the sunrun row for spectrum {}",
                    self.spectrum_name
                ))
            })?;
        Ok(())
    }
}

impl From<ExpandedSunrunRow> for SunrunRow {
    fn from(value: ExpandedSunrunRow) -> Self {
        Self {
            spectrum_name: value.spectrum_file_name,
            object: value.obj,
            tcorr: value.tcorr,
            oblat: value.oblat,
            oblon: value.oblon,
            obalt: value.obalt,
            tins: value.tins,
            pins: value.pins,
            hins: value.hins,
            tout: value.tout,
            pout: value.pout,
            hout: value.hout,
            sia: value.sia,
            fvsi: value.fvsi,
            wspd: value.wspd,
            wdir: value.wdir,
            nus: value.nus,
            nue: value.nue,
            fsf: value.fsf,
            lasf: value.lasf,
            wavtkr: value.wavtkr,
            aipl: value.aipl,
            tm: value.tm,
        }
    }
}

/// A struct containing values of a sunrun row plus extra information useful for editing the rows.
///
/// Currently, this structure adds the ZPD time as both a [`chrono::DateTime`]
/// and with the individual parts (year, month, day, etc.) as fields. The latter
/// is intended to make it easier to write simple Lua lines that depend on the date & time.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExpandedSunrunRow {
    pub spectrum_file_name: String,

    /// The ZPD time of the spectrum.
    pub zpd_time: chrono::DateTime<Utc>,
    pub year: i32,
    pub month: i32,
    pub day: i32,
    pub hour: i32,
    pub minute: i32,
    pub second: i32,
    pub obj: Object,
    pub tcorr: f64,
    pub oblat: f64,
    pub oblon: f64,
    pub obalt: f64,
    pub tins: f64,
    pub pins: f64,
    pub hins: f64,
    pub tout: f64,
    pub pout: f64,
    pub hout: f64,
    pub sia: f64,
    pub fvsi: f64,
    pub wspd: f64,
    pub wdir: f64,
    pub nus: f64,
    pub nue: f64,
    pub fsf: f64,
    pub lasf: f64,
    pub wavtkr: f64,
    pub aipl: f64,
    pub tm: f64,
}

impl ExpandedSunrunRow {
    /// Create an instance from a spectrum and ancillary data.
    ///
    /// # Parameters
    /// - `spectrum`: path to the binary spectrum file to read.
    /// - `instrument`: used to determine how to read the headers.
    /// - `is_lamp`: `true` is this is for a lamp (a.k.a. lab) run.
    ///   Determines how FVSI is calculated.
    /// - `defaults`: the set of default values to use in the row
    ///   if that value is not available from the spectrum header.
    /// - `object`: what celestial object the instrument was pointed
    ///   towards.
    /// - `nus`: lower wavenumber bound for this spectrum, in cm-1.
    /// - `nue`: upper wavenumber bound for the spectrum, in cm-1.
    pub fn build_from_spectrum(
        spectrum: &Path,
        instrument: Instrument,
        is_lamp: bool,
        defaults: &SunrunDefaults,
        object: Object,
        nus: f64,
        nue: f64,
    ) -> Result<ExpandedSunrunRow, GggError> {
        // TODO: switch to spectrum types? Or make the `opus` module names reflect that this can be a spectrum or interferogram?
        let spec_header =
            IgramHeader::read_full_igram_header(&spectrum).map_err(|e| GggError::CouldNotRead {
                path: spectrum.to_path_buf(),
                reason: e.to_string(),
            })?;
        let name = spectrum
            .file_name()
            .ok_or_else(|| {
                GggError::Custom(format!(
                    "Could not get base name of spectrum, {}",
                    spectrum.display()
                ))
            })?
            .to_string_lossy()
            .to_string();
        let row = match instrument {
            Instrument::MkIV => unimplemented!("MkIV headers not implemented yet"),
            Instrument::TCCON | Instrument::Other => assign_tccon_spectrum_header_info(
                &name,
                &spec_header,
                is_lamp,
                defaults,
                object,
                nus,
                nue,
            ),
        };

        row
    }
}

/// A set of default values to use in a sunrun if the information is not present in a spectrum header.
///
/// For configuration, `oblat`, `oblon`, `obalt`, and `aipl` are required. The others
/// will default to the [`SUNRUN_MISSING`] constant, though note that this
/// value is too wide for some of the fields, and several really cannot be left
/// as fill values.
#[derive(Debug, Deserialize)]
pub struct SunrunDefaults {
    #[serde(default)]
    pub tcorr: f64,
    #[serde(default = "sunrun_missing")]
    pub oblat: f64,
    #[serde(default = "sunrun_missing")]
    pub oblon: f64,
    #[serde(default = "sunrun_missing")]
    pub obalt: f64,
    #[serde(default = "sunrun_missing")]
    pub tins: f64,
    #[serde(default = "sunrun_missing")]
    pub pins: f64,
    #[serde(default = "sunrun_missing")]
    pub hins: f64,
    #[serde(default = "sunrun_missing")]
    pub tout: f64,
    #[serde(default = "sunrun_missing")]
    pub pout: f64,
    #[serde(default = "sunrun_missing")]
    pub hout: f64,
    #[serde(default = "sunrun_missing")]
    pub sia: f64,
    #[serde(default = "sunrun_missing")]
    pub fvsi: f64,
    #[serde(default = "sunrun_missing")]
    pub wspd: f64,
    #[serde(default = "sunrun_missing")]
    pub wdir: f64,
    #[serde(default = "sunrun_missing")]
    pub nus: f64,
    #[serde(default = "sunrun_missing")]
    pub nue: f64,
    #[serde(default = "sunrun_missing")]
    pub fsf: f64,
    #[serde(default = "sunrun_missing")]
    pub lasf: f64,
    #[serde(default = "sunrun_missing")]
    pub wavtkr: f64,
    #[serde(default = "sunrun_missing")]
    pub aipl: f64,
    #[serde(default = "sunrun_missing")]
    pub tm: f64,
}

impl Default for SunrunDefaults {
    fn default() -> Self {
        Self {
            tcorr: 0.0,
            oblat: SUNRUN_MISSING,
            oblon: SUNRUN_MISSING,
            obalt: SUNRUN_MISSING,
            tins: SUNRUN_MISSING,
            pins: SUNRUN_MISSING,
            hins: SUNRUN_MISSING,
            tout: SUNRUN_MISSING,
            pout: SUNRUN_MISSING,
            hout: SUNRUN_MISSING,
            sia: SUNRUN_MISSING,
            fvsi: SUNRUN_MISSING,
            wspd: SUNRUN_MISSING,
            wdir: SUNRUN_MISSING,
            nus: SUNRUN_MISSING,
            nue: SUNRUN_MISSING,
            fsf: SUNRUN_MISSING,
            lasf: SUNRUN_MISSING,
            wavtkr: SUNRUN_MISSING,
            aipl: SUNRUN_MISSING,
            tm: SUNRUN_MISSING,
        }
    }
}

/// Write the sunrun file header.
pub fn write_header<W: Write>(mut f: W) -> error_stack::Result<(), std::io::Error> {
    // TODO: the replacement code uses these column names. Find a way
    // to link that to this function so that I don't have to maintain
    // the column names in two locations in the future.
    write!(f, "           3          23\n")?;
    write!(f, " create_sunrun_rs    Version 1.0   2026-07-28   JLL\n")?;
    write!(f, " Spectrum_File_Name                                        Obj  tcorr   oblat    oblon   obalt   tins   pins   hins  tout  pout   hout    sia    fvsi   wspd   wdir   Nus    Nue      FSF      lasf    wavtkr   AIPL   TM\n")?;
    Ok(())
}

/// Helper function that copies data from an Opus header into a sunrun row.
///
/// Assumes the layout of an Opus header for TCCON.
fn assign_tccon_spectrum_header_info(
    specname: &str,
    header: &IgramHeader,
    is_lamp: bool,
    defaults: &SunrunDefaults,
    object: Object,
    nus: f64,
    nue: f64,
) -> Result<ExpandedSunrunRow, GggError> {
    let zpd_time = header.get_zpd_time()?;

    let oblat = get_float(
        specname,
        header,
        BrukerBlockType::SampleOriginParameters,
        "LAT",
    )?
    .unwrap_or(defaults.oblat);

    let oblon = get_float(
        specname,
        header,
        BrukerBlockType::SampleOriginParameters,
        "LON",
    )?
    .unwrap_or(defaults.oblon);

    let obalt = get_float(
        specname,
        header,
        BrukerBlockType::SampleOriginParameters,
        "ALT",
    )?
    .map(|x| x / 1000.0)
    .unwrap_or(defaults.obalt);

    let tins = get_float(specname, header, BrukerBlockType::InstrumentStatus, "TSC")?
        .unwrap_or(defaults.tins);

    let pins = get_float_some_key(
        specname,
        header,
        &[
            (BrukerBlockType::InstrumentStatus, "PIM"),
            (BrukerBlockType::InstrumentStatus, "PRS"),
        ],
    )?
    .unwrap_or(defaults.pins);

    // Limit to a max 99.9% humidity
    let hins = get_float(specname, header, BrukerBlockType::InstrumentStatus, "HUM")?
        .map(|x| x.min(99.9))
        .unwrap_or(defaults.hins);

    let tout = get_float(
        specname,
        header,
        BrukerBlockType::SampleOriginParameters,
        "TOU",
    )?
    .unwrap_or(defaults.tout);

    let pout = get_float(
        specname,
        header,
        BrukerBlockType::SampleOriginParameters,
        "POU",
    )?
    .unwrap_or(defaults.pout);

    let hout = get_float(
        specname,
        header,
        BrukerBlockType::SampleOriginParameters,
        "HOU",
    )?
    .unwrap_or(defaults.hout);

    let sia = get_float(
        specname,
        header,
        BrukerBlockType::SampleOriginParameters,
        "SIA",
    )?
    .unwrap_or(defaults.sia);

    let fvsi = get_fvsi(specname, header, is_lamp)?.unwrap_or(defaults.fvsi);

    let wspd = get_float(
        specname,
        header,
        BrukerBlockType::SampleOriginParameters,
        "WSA",
    )?
    .unwrap_or(defaults.wspd);

    let wdir = get_float(
        specname,
        header,
        BrukerBlockType::SampleOriginParameters,
        "WDA",
    )?
    .unwrap_or(defaults.wdir);

    let lasf = get_float(specname, header, BrukerBlockType::InstrumentStatus, "LWN")?
        .unwrap_or(defaults.lasf);

    Ok(ExpandedSunrunRow {
        spectrum_file_name: specname.to_string(),
        zpd_time,
        year: zpd_time.year(),
        month: zpd_time.month() as i32,
        day: zpd_time.day() as i32,
        hour: zpd_time.hour() as i32,
        minute: zpd_time.minute() as i32,
        second: zpd_time.second() as i32,
        obj: object,
        tcorr: defaults.tcorr,
        oblat,
        oblon,
        obalt,
        tins,
        pins,
        hins,
        tout,
        pout,
        hout,
        sia,
        fvsi,
        wspd,
        wdir,
        nus: nus,
        nue: nue,
        fsf: defaults.fsf,
        lasf,
        wavtkr: defaults.wavtkr,
        aipl: defaults.aipl,
        tm: defaults.tm,
    })
}

/// Retrieve a float value from the header that might be
/// in one of several locations. Returns `Ok(None)` if none
/// of the block/parameter combinations in `keys` is present
/// in the header.
fn get_float_some_key(
    specname: &str,
    hdr: &IgramHeader,
    keys: &[(BrukerBlockType, &str)],
) -> Result<Option<f64>, GggError> {
    for (block, param) in keys {
        if let Some(val) = get_float(specname, hdr, *block, param)? {
            return Ok(Some(val));
        }
    }
    Ok(None)
}

/// Compute FVSI from the best available header quantities.
fn get_fvsi(specname: &str, hdr: &IgramHeader, is_lamp: bool) -> Result<Option<f64>, GggError> {
    let sia_opt = get_float(
        specname,
        hdr,
        BrukerBlockType::SampleOriginParameters,
        "SIA",
    )?;
    let sis_opt = get_float(
        specname,
        hdr,
        BrukerBlockType::SampleOriginParameters,
        "SIS",
    )?;
    let vdc_opt = get_float(
        specname,
        hdr,
        BrukerBlockType::FourierTransParameters,
        "VDC",
    )?;

    //  Calculate fvsi as sis/sia if sia is not zero or missing, and if
    //  it's not a lamp run:
    let fvsi_opt = if is_lamp {
        vdc_opt
    } else if let (Some(sis), Some(sia)) = (sis_opt, sia_opt) {
        // Just in case we sometimes get negative values written by the firmware,
        // instead of the Fortran reader using negative values as sentinels for
        // missing values.
        if sis < 0.0 {
            log::warn!("Negative SIS in {specname}, using VDC instead");
            vdc_opt
        } else if sia < 0.0 {
            log::warn!("Negative SIA in {specname}, using VDC instead");
            vdc_opt
        } else {
            Some(sis / sia)
        }
    } else {
        //  DW 20170809: The EM27/SUN community uses a computed SIA value from MXY
        //  and MNY to track mirror degredation. They do not compute SIS, so they
        //  depend on I2S to compute FVSI from the VDC parameter.
        vdc_opt
    };

    // Ensure that, if we took the value of VDC, we only use positive VDC values,
    // and that FVSI is never > 1.0
    if let Some(fvsi) = fvsi_opt {
        if fvsi < 0.0 {
            Ok(None)
        } else if fvsi > 1.0 {
            Ok(Some(1.0))
        } else {
            Ok(Some(fvsi))
        }
    } else {
        Ok(None)
    }
}

/// Get a float from a specific block in the header.
fn get_float(
    specname: &str,
    hdr: &IgramHeader,
    block: BrukerBlockType,
    param: &str,
) -> Result<Option<f64>, GggError> {
    let parval = if let Ok(p) = hdr.get_value(block, param) {
        p
    } else {
        return Ok(None);
    };

    let val = match parval.as_float() {
        Ok(v) => v,
        Err(e) => {
            return Err(GggError::Custom(format!("In spectrum {specname}, could not convert '{param}' parameter in '{block}' block to a float: {e}")));
        }
    };

    Ok(Some(val))
}

/// An enum indicating which instrument this sunrun is for.
///
/// This is used to determine how to read the spectrum headers.
#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(from = "i8")]
#[repr(i8)]
pub enum Instrument {
    MkIV = 1,
    TCCON = 2,
    Other = 3,
}

impl From<i8> for Instrument {
    fn from(value: i8) -> Self {
        match value {
            1 => Self::MkIV,
            2 => Self::TCCON,
            _ => Self::Other,
        }
    }
}

/// An enum indicating which celestial object the instrument was pointing at.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(try_from = "i8", into = "i8")]
#[repr(i8)]
pub enum Object {
    Moon = 1,
    Sun = 2,
}

impl Default for Object {
    fn default() -> Self {
        Self::Sun
    }
}

impl TryFrom<i8> for Object {
    type Error = GggError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Moon),
            2 => Ok(Self::Sun),
            _ => Err(GggError::custom(format!(
                "'{value}' is an invalid number for a sunrun object, must be 1 (Moon) or 2 (Sun)"
            ))),
        }
    }
}

impl From<Object> for i8 {
    fn from(value: Object) -> Self {
        match value {
            Object::Moon => 1,
            Object::Sun => 2,
        }
    }
}
