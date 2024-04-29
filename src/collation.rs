//! Functions for collating output from multiple .col files
//! 
//! GGG writes the results of retrieving individual windows to .col
//! files. These results are usually then combined into a single file
//! with a `.Xsw` extension, where the `X` is a single character representing
//! what quantity is stored in the file:
//! 
//! - "v" = retrieved vertical columns,
//! - "t" = VMR scale factors,
//! - "o" = original vertical columns,
//! - "l" = retrieved slant columns,
//! - "f" = frequency shift,
//! - "s" = SG stretch,
//! - "c" = continuum level,
//! - "m" = continuum tilt,
//! - "n" = continuum curvature,
//! - "r" = RMS divided by continuum level.
//! 
//! Note that not all of these options are implemented in this module yet,
//! see [`CollationMode`] for available options.
//! 
//! The original Fortran implementation of `collate_results` tried to handle
//! all use cases (MkIV, TCCON, EM27s, etc.), but this was frequently challenging 
//! because of the different idiosyncrasies of different instruments. For the Rust
//! version, we instead factor out the code that needs to be unique to each use
//! case, while keeping the common code here.
//! 
//! The main function if [`collate_results`]. This handles reading in the `.col`
//! and `.ray` files and writing out the data. It relies on a type implementing
//! the [`CollationIndexer`] trait to tell it how to align rows from different
//! `.col` files. For example, the TCCON implementation (in `bin/collate-tccon-results`)
//! iterates through the runlog first and assigns adjacent spectra with identical
//! names save the detector character to the same index.
use std::path::{Path, PathBuf};
use std::str::FromStr;

use error_stack::ResultExt;
use log::info;

use crate::error::FileLocation;
use crate::output_files::{iter_tabular_file, open_and_iter_col_file, read_col_file_header, write_postproc_header, AuxData, ColFileHeader, ColRetQuantity, PostprocRow, ProgramVersion, POSTPROC_FILL_VALUE};
use crate::runlogs::RunlogDataRec;
use crate::utils::{self, FileBuf};

pub type CollationResult<T> = Result<T, CollationError>;

static WINDOW_SF_REGEX: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();

/// Possible errors during data collation.
/// 
/// Each error type has a similarly named associated function
/// that creates it with some generics to make it more convenient
/// to use.
#[derive(Debug, thiserror::Error)]
pub enum CollationError {
    /// One or more input files could not be found
    #[error("Error gathering input files: {0}")]
    MissingInput(String),

    /// One of the necessary inputs could not be read
    #[error("Error reading {loc}: {reason}")]
    CouldNotRead{loc: FileLocation, reason: String},

    /// The output file could not be written
    #[error("Could not write to {}", .path.display())]
    CouldNotWrite{path: PathBuf},

    /// Some path could not be identified, either because something
    /// about the directory structure is odd (i.e. missing parent directory)
    /// or the path to this file differed across other input files' headers.
    #[error("Could not find the {0}")]
    CouldNotFind(String),

    /// An input value expected to be the same across multiple files was not
    #[error("Value from header of {} ({}) does not match value ({}) from the first file ({})", other_col_file.display(), other_value, first_value, first_col_file.display())]
    MismatchedInput{first_col_file: PathBuf, other_col_file: PathBuf, first_value: String, other_value: String},

    /// A data column in one of the input files was missing.
    #[error("Missing column '{column}' in {path}")]
    MissingColumn{path: PathBuf, column: String},

    /// Some value had a different format than expected and could not be parsed.
    #[error("{0}")]
    ParsingError(String),

    /// Reading data from a .col file failed
    #[error("Problem getting data from .col file {}", .0.display())]
    ColFileError(PathBuf),

    /// Error raised when converting a string to [`CollationMode`] failed
    /// because the string did not map to any of the allowed modes.
    #[error("Unknown collation mode '{0}'")]
    UnknownMode(String),

    /// A fallback error type to handle situations unique to specific implementations
    /// of data collation.
    #[error("{0}")]
    Custom(String),
}

impl CollationError {
    pub fn missing_input<S: Into<String>>(description: S) -> Self {
        Self::MissingInput(description.into())
    }

    pub fn could_not_read_file<S: Into<String>, P: AsRef<Path>>(reason: S, path: P) -> Self {
        Self::CouldNotRead {
            loc: FileLocation::new::<_, String>(Some(path), None, None),
            reason: reason.into()
        }
    }

    pub fn could_not_write<P: Into<PathBuf>>(p: P) -> Self {
        Self::CouldNotWrite { path: p.into() }
    }

    pub fn could_not_find<S: Into<String>>(file_descr: S) -> Self {
        Self::CouldNotFind(file_descr.into())
    }

    pub fn mismatched_input<P: Into<PathBuf>, S: Into<String>>(first_col_file: P, other_col_file: P, first_value: S, other_value: S) -> Self {
        Self::MismatchedInput { 
            first_col_file: first_col_file.into(),
            other_col_file: other_col_file.into(),
            first_value: first_value.into(),
            other_value: other_value.into()
        }
    }

    pub fn missing_column<S: Into<String>, P: Into<PathBuf>>(path: P, column: S) -> Self {
        Self::MissingColumn { path: path.into(), column: column.into() }
    }

    pub fn parsing_error<S: Into<String>>(reason: S) -> Self {
        Self::ParsingError(reason.into())
    }

    pub fn col_file_error<P: Into<PathBuf>>(col_file: P) -> Self {
        Self::ColFileError(col_file.into())
    }

    pub fn custom<S: Into<String>>(descr: S) -> Self {
        Self::Custom(descr.into())
    }
}

/// A trait implemented by types that align data from different `.col` files.
pub trait CollationIndexer: Sized {
    /// Create a new instance of this type given a path to the runlog.
    /// 
    /// Typically this method will iterate through the runlog and store
    /// the index for the row that each spectrum's values should be placed
    /// in in the output file. This will also likely need to store the runlog's
    /// data to return from the `get_runlog_data` method.
    fn new_from_runlog(runlog: &Path) -> CollationResult<Self>;

    /// Given a spectrum name from a `.col` file, return the row index where
    /// values from this `.col` row should be placed in the `.Xsw` file.
    fn get_row_index(&self, spectrum: &str) -> CollationResult<usize>;

    /// Return a slice of runlog data to write as (most) of the auxiliary
    /// data columns in the `.Xsw` file. Note that these *must* align with
    /// the indices returned by `get_row_index`, so that the record at index
    /// 0 of the slice aligns with any spectrum for which `get_row_index`
    /// returns a 0, the record at 1 aligns with any spectrum which `get_row_index`
    /// returns a 1, and so on.
    fn get_runlog_data(&self) -> CollationResult<&[RunlogDataRec]>;
}

/// What data to write to the `.Xsw` file.
#[derive(Debug, Clone, Copy)]
pub enum CollationMode {
    /// Write the retrieved vertical columns (i.e. VSF * OVC)
    VerticalColumns,
    /// Write the VMR scale factors only (i.e. VSF)
    VmrScaleFactors,
}

impl CollationMode {
    fn ext_char(&self) -> char {
        match self {
            CollationMode::VerticalColumns => 'v',
            CollationMode::VmrScaleFactors => 't',
        }
    }
}

impl FromStr for CollationMode {
    type Err = CollationError;

    /// Return the [`CollationMode`] that matches the given string.
    /// 
    /// For consistency with the original `collate_results`, the 
    /// single-character representations of these modes are recognized
    /// (i.e. "v" -> `VerticalColumns`, etc.). However, more complete
    /// strings are also recognized:
    /// 
    /// - "v" or "vertical-columns" returns `Self::VerticalColumns`,
    /// - "t" or "vmr-scale-factors" returns `Self::VmrScaleFactors`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "v" | "vertical-columns" => Ok(Self::VerticalColumns),
            "t" | "vmr-scale-factors" => Ok(Self::VmrScaleFactors),
            _ => Err(CollationError::UnknownMode(s.to_string()))
        }
    }
}


/// The primary entry point for this module.
/// 
/// Given a path to a multiggg.sh file, i.e. one with a series of calls to `gfit` such as:
/// 
/// ```text
/// /home/jlaugh/GGG/ggg-my-devel/bin/gfit luft_6146.pa_ggg_benchmark.ggg>/dev/null
/// /home/jlaugh/GGG/ggg-my-devel/bin/gfit hf_4038.pa_ggg_benchmark.ggg>/dev/null
/// /home/jlaugh/GGG/ggg-my-devel/bin/gfit h2o_4565.pa_ggg_benchmark.ggg>/dev/null
/// /home/jlaugh/GGG/ggg-my-devel/bin/gfit h2o_4570.pa_ggg_benchmark.ggg>/dev/null
/// ...
/// ```
/// 
/// this will find the `.col` files for each line of this file *not* beginning with
/// a colon and combine their data into a single `.Xsw` file. The `.col` files must
/// be present in the same directory, and must all reference the same `.ray` file
/// and runlog in their headers (and those files must exist as well). Other inputs:
/// 
/// -  `mode` controls what values are written from each `.col` file.
/// - `collate_version` specifies what program version to put in the header of the output file.
/// 
/// This function also needs a [`CollationIndexer`] as a type parameter. If you have
/// a struct `TcconIndexer` that implements [`CollationIndexer`], you would call
/// this function as:
/// 
/// ```ignore
/// collate_results::<TcconIndexer>(multiggg_file, mode, collate_version)
/// ``` 
pub fn collate_results<I: CollationIndexer>(multiggg_file: &Path, mode: CollationMode, collate_version: ProgramVersion) -> error_stack::Result<(), CollationError> {
    let run_dir = multiggg_file.parent().ok_or_else(
        || CollationError::could_not_find(
            format!("run directory (could not get parent directory of the given multiggg file, {})", multiggg_file.display())
        ))?;

    info!("Collating results in {}", run_dir.display());

    // Make sure we can get all the input files we need
    let col_files = get_col_files(multiggg_file, run_dir)?;
    let runlog = get_file_from_header(&col_files, run_dir, |h| h.runlog_file.path)
        .change_context_lazy(|| CollationError::could_not_find("runlog"))?;
    let ray_file = get_file_from_header(&col_files, run_dir, |h| h.ray_file.path)
        .change_context_lazy(|| CollationError::could_not_find(".ray file"))?;
    let runlog_name = runlog.file_stem().ok_or_else(|| CollationError::could_not_find(
        "file stem of the runlog"
    ))?.to_string_lossy();

    info!("{} .col files will be collated", col_files.len());
    info!("Spectrum order taken from {}", runlog.display());

    // Get the program versions and the scale factors from the .col files
    let (gsetup_version, gfit_version, window_sfs) = get_header_info(&col_files)?;

    // Gather the auxiliary data we can from the runlog
    let mut indexer = I::new_from_runlog(&runlog)?;
    let mut columns = AuxData::postproc_fields_vec();
    let mut rows: Vec<PostprocRow> = indexer.get_runlog_data()?
        .iter()
        .map(|rld| {
            let aux = AuxData::from(rld);
            PostprocRow::new(aux)
        }).collect();

    let naux = columns.len();

    // Get zmin from the .ray file and add "run" as the 1-based row index
    add_zmin(&mut rows, &mut indexer, &ray_file)?;
    add_run(&mut rows);

    // Get values from the .col files
    let ncol = col_files.len();
    for (idx, cfile) in col_files.into_iter().enumerate() {
        let window = cfile.file_name()
            .ok_or_else(|| CollationError::parsing_error(
                format!("Could not get base name of .col file {}", cfile.display())
            ))?.to_str()
            .ok_or_else(|| CollationError::parsing_error(
                format!("Could not convert base name of {} to valid UTF-8", cfile.display())
            ))?.split('.')
            .next()
            .ok_or_else(|| CollationError::parsing_error(
                format!("Could not find a '.' in base name of {} to mark the end of the window name", cfile.display())
            ))?;
        info!("Reading .col file {}/{ncol}: {window}", idx+1);
        
        let val_colname = window;
        let val_err_colname = format!("{window}_error");
        add_col_value(&mut rows, &mut indexer, &cfile, mode, val_colname, &val_err_colname)
            .change_context_lazy(|| CollationError::col_file_error(&cfile))?;
        columns.push(val_colname.to_string());
        columns.push(val_err_colname);
    }

    // Write the output file
    let extra_lines = if let Some(sfs) = window_sfs {
        vec![format!("sf=   {}", sfs.join("   "))]
    } else {
        vec![]
    };
    let xsw_file = run_dir.join(format!("{runlog_name}.{}sw", mode.ext_char()));
    let f = std::fs::File::create(&xsw_file).change_context_lazy(|| CollationError::could_not_write(&xsw_file))?;
    let mut writer = std::io::BufWriter::new(f);
    let format_str = format!("(a57,a1,f13.8,{}f13.5,{}(1pe13.5))", naux - 2, columns.len() - naux);
    write_postproc_header(&mut writer, columns.len(), rows.len(), naux,
    &[collate_version, gfit_version, gsetup_version], &extra_lines, POSTPROC_FILL_VALUE,
    &format_str, &columns).change_context_lazy(|| CollationError::could_not_write(&xsw_file))?;
    
    // We don't write the "a1" column that has the colon/semicolon
    let writer_format_str = format_str.replace("a1,", "1x");
    let write_format = fortformat::FortFormat::parse(&writer_format_str)
        .map_err(|e| CollationError::parsing_error(
            format!("Could not parse format .xsw format string '{writer_format_str}': {e}")
        ))?;
    
    let ser_settings = fortformat::ser::SerSettings::default().align_left_str(true);
    fortformat::ser::many_to_writer_custom(&rows, &write_format, Some(&columns), &ser_settings, &mut writer)
        .change_context_lazy(|| CollationError::could_not_write(&xsw_file))?;
    info!("Results written to {}.", xsw_file.display());

    Ok(())
}

/// Return a vector of paths to the `.col` files to read
fn get_col_files(multiggg_file: &Path, run_dir: &Path) -> error_stack::Result<Vec<PathBuf>, CollationError> {
    let col_file_basenames = utils::get_windows_from_multiggg(multiggg_file, true)
        .change_context_lazy(|| CollationError::missing_input("Error getting windows from multiggg.sh file"))?;
    let nwin = col_file_basenames.len();

    let mut col_files = vec![];
    let mut missing_files = vec![];
    for basename in col_file_basenames {
        let cf_path = run_dir.join(format!("{basename}.col"));
        if cf_path.exists() {
            col_files.push(cf_path);
        } else {
            missing_files.push(basename);
        }
    }

    if missing_files.is_empty() {
        Ok(col_files)
    } else {
        let missing_str = missing_files.join(", ");
        let msg = format!("Missing {} of {} expected .col files, missing windows were: {missing_str}", missing_files.len(), nwin);
        Err(CollationError::missing_input(msg).into())
    }
}

/// Get a path to one file from the `.col` file headers, error if it differs across files.
/// 
/// `get_file` is a function that takes ownership of a [`ColFileHeader`] and returns the
/// desired path as a [`PathBuf`].
fn get_file_from_header<F>(col_files: &[PathBuf], run_dir: &Path, get_file: F) -> error_stack::Result<PathBuf, CollationError> 
where F: Fn(ColFileHeader) -> PathBuf
{
    if col_files.is_empty() {
        return Err(CollationError::missing_input("no .col files found").into());
    }

    let mut fbuf = FileBuf::open(&col_files[0])
        .change_context_lazy(|| CollationError::could_not_read_file("could not open", &col_files[0]))?;
    let first_header = read_col_file_header(&mut fbuf)
        .change_context_lazy(|| CollationError::could_not_read_file("error reading header", &col_files[0]))?;
    let expected_file = get_file(first_header);

    for cfile in &col_files[1..] {
        let mut fbuf = FileBuf::open(cfile)
            .change_context_lazy(|| CollationError::could_not_read_file("could not open", cfile))?;
        let header = read_col_file_header(&mut fbuf)
            .change_context_lazy(|| CollationError::could_not_read_file("error reading header", cfile))?;
        let new_file = get_file(header);

        if new_file != expected_file {
            return Err(CollationError::mismatched_input(
                &col_files[0], cfile, expected_file.display().to_string(), new_file.display().to_string()
            ))?;
        }
    }

    if expected_file.is_absolute() {
        Ok(expected_file)
    } else {
        Ok(run_dir.join(expected_file))
    }
}

/// Return the gsetup and gfit versions, possibly along with a list of window scale factors.
/// 
/// The third return value will be `None` if none of the `.col` files recorded an `sf=` entry
/// in their command lines. In that case, the output file should not write an `sf=` line.
/// If any `.col` file contains an `sf=` value, then this return will be a `Some(_)` and
/// any `.col` file without an `sf=` value will use `sf=1.0`.
fn get_header_info(col_files: &[PathBuf]) -> error_stack::Result<(ProgramVersion, ProgramVersion, Option<Vec<String>>), CollationError> {
    if col_files.is_empty() {
        return Err(CollationError::missing_input("no .col files found").into());
    }

    let mut fbuf = FileBuf::open(&col_files[0])
        .change_context_lazy(|| CollationError::could_not_read_file("could not open", &col_files[0]))?;
    let first_header = read_col_file_header(&mut fbuf)
        .change_context_lazy(|| CollationError::could_not_read_file("error reading header", &col_files[0]))?;

    let first_sf = get_window_sf(&first_header);
    let mut sf_present = first_sf.is_some();
    let expected_gsetup_version = first_header.gsetup_version;
    let expected_gfit_version = first_header.gfit_version;
    let mut window_sfs = vec![first_sf.unwrap_or_else(|| "1.000".to_string())];

    for cfile in &col_files[1..] {
        let mut fbuf = FileBuf::open(cfile)
            .change_context_lazy(|| CollationError::could_not_read_file("could not open", &col_files[0]))?;
        let new_header = read_col_file_header(&mut fbuf)
            .change_context_lazy(|| CollationError::could_not_read_file("error reading header", &col_files[0]))?;

        let new_sf = get_window_sf(&new_header);
        sf_present = sf_present || new_sf.is_some();
        window_sfs.push(new_sf.unwrap_or_else(|| "1.000".to_string()));

        if new_header.gsetup_version != expected_gsetup_version {
            return Err(CollationError::mismatched_input(
                &col_files[0], cfile, expected_gsetup_version.to_string(), new_header.gsetup_version.to_string()
            ))?
        }

        if new_header.gfit_version != expected_gfit_version {
            return Err(CollationError::mismatched_input(
                &col_files[0], cfile, expected_gfit_version.to_string(), new_header.gfit_version.to_string()
            ))?
        }
    }

    let window_sfs = if sf_present { Some(window_sfs) } else { None };
    Ok((expected_gsetup_version, expected_gfit_version, window_sfs))
}

/// Get the `sf=` value from a `.col` file's header, if present (`None` returned if not).
fn get_window_sf(header: &ColFileHeader) -> Option<String> {
    let re = WINDOW_SF_REGEX.get_or_init(|| 
        regex::Regex::new(r"sf=([0-9\.]+)")
            .expect("Could not compile window scale factor regex")
    );

    let sf_match = re.captures(&header.command_line)
        .map(|m| m.get(1).expect("regex should return the SF value as group 1").as_str().to_string());
    sf_match
}

/// Add the zmin values from the `.ray` file to the `.Xsw` file rows.
/// [`PostprocRow`] instances created from runlog data records have
/// a fill value for `zmin`, so this overwrites that.
fn add_zmin<I: CollationIndexer>(rows: &mut Vec<PostprocRow>, indexer: &mut I, ray_file: &Path) -> error_stack::Result<(), CollationError> {
    let it = iter_tabular_file(ray_file)
        .change_context_lazy(|| CollationError::could_not_read_file("iteration of .ray file failed", ray_file))?;
    for (irow, row) in it.enumerate() {
        let ray_row = row.change_context_lazy(|| {
            CollationError::could_not_read_file(
                format!("error readling data line {} of .ray file", irow+1), ray_file 
            )})?;

        let sw_idx = indexer.get_row_index(&ray_row.spectrum())?;
        let sw_row = rows.get_mut(sw_idx)
            .expect("Index returned by the collation indexer should be a valid index for the rows created from the runlog");
        sw_row.auxiliary.zmin = ray_row.get("Zmin").ok_or_else(|| 
            CollationError::missing_column(ray_file, "Zmin")
        )?;
    }
    Ok(())
}

/// Add the "run" number to the output
fn add_run(rows: &mut Vec<PostprocRow>) {
    for (irun, row) in rows.iter_mut().enumerate() {
        row.auxiliary.run = (irun + 1) as f64;
    }
}

/// Add the value and its error from the `.col` file to the `.Xsw` file.
/// 
/// # Inputs
/// - `rows`: [`PostprocRow`] instances to modify, will add to the `retrieved` field.
/// - `indexer: the instance that tells us which index in `rows` to add a given value to.
/// - `col_file`: the `.col` file to read values from.
/// - `mode`: which values and errors to write.
/// - `val_colname`: the key the values will be under in the [`PostprocRow`] hash maps.
/// - `val_err_colname`: the key the error values will be under in the [`PostprocRow`] hash maps.
/// 
/// Note that `val_colname` and `val_err_colname` need to match their respective values in the list
/// of field names passed to the serializer.
fn add_col_value<I: CollationIndexer>(rows: &mut Vec<PostprocRow>, indexer: &mut I, col_file: &Path, mode: CollationMode, val_colname: &str, val_err_colname: &str)
-> error_stack::Result<(), CollationError> 
{
    let it = open_and_iter_col_file(col_file)
        .change_context_lazy(|| CollationError::could_not_read_file("error setting up .col file read", col_file))?;

    for (irow, row) in it.enumerate() {
        let col_row = row.change_context_lazy(|| {
            CollationError::could_not_read_file(
                format!("error readling data line {} of .col file", irow+1), col_file 
            )})?;

        let (val, val_err) = match mode {
            CollationMode::VerticalColumns => {
                let vsf = col_row.get_primary_gas_quantity(ColRetQuantity::Vsf)
                    .ok_or_else(|| CollationError::missing_column(col_file, "primary gas VSF"))?;
                let vsf_error = col_row.get_primary_gas_quantity(ColRetQuantity::VsfError)
                    .ok_or_else(|| CollationError::missing_column(col_file, "primary gas VSF error"))?;
                let ovc = col_row.get_primary_gas_quantity(ColRetQuantity::Ovc)
                    .ok_or_else(|| CollationError::missing_column(col_file, "primary gas OVC"))?;

                (vsf * ovc, vsf_error * ovc)
            },
            CollationMode::VmrScaleFactors => {
                let vsf = col_row.get_primary_gas_quantity(ColRetQuantity::Vsf)
                    .ok_or_else(|| CollationError::missing_column(col_file, "primary gas VSF"))?;
                let vsf_error = col_row.get_primary_gas_quantity(ColRetQuantity::VsfError)
                    .ok_or_else(|| CollationError::missing_column(col_file, "primary gas VSF error"))?;
                (vsf, vsf_error)
            },
        };

        let sw_idx = indexer.get_row_index(&col_row.spectrum)?;
        let sw_row = rows.get_mut(sw_idx)
            .expect("Index returned by the collation indexer should be a valid index for the rows created from the runlog");

        sw_row.retrieved.insert(val_colname.to_string(), val);
        sw_row.retrieved.insert(val_err_colname.to_string(), val_err);
    }

    Ok(())
}