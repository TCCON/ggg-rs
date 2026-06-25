use std::{io::Write, path::{PathBuf}};

use error_stack::ResultExt;

use crate::{error::WriteError, readers::{ProgramVersion, postproc_files::{PostprocFileHeader, PostprocRow}}, utils::GggError};

/// A trait that defines how to write a postprocessing file
/// 
/// For GGG2020 and GGG2020.1, the standard post processing files (`.?sw`, `.?av`, `.vsw.ada`, `.vav.ada`, and `.vav.ada.aia`)
/// are written as Fortran fixed-format text files with a header. This trait defined a general interface for that which we can
/// use in the future if we want the option to, e.g., change these files to netCDF files.
pub trait PostprocWriter {
    /// Write a common post processing file.
    /// 
    /// The `header` is used to write metadata about the file. `row_iter` must be an
    /// iterator that produces `Result<PostprocRow, GggError>`. While this might often
    /// be an infallible iterator (e.g., one derived from `Vec<PostprocRow>`), returning
    /// a result gives us the option to "stream" the conversion of previous data. In cases
    /// where the transformation from input to output can work on one row at a time, this
    /// could reduce memory usage.
    fn write_postproc_file<I>(&self, header: &PostprocFileHeader, row_iter: I) -> error_stack::Result<(), GggError>
        where I: Iterator<Item = Result<PostprocRow, GggError>>;
}

/// A concrete post-processing writer to generate fixed-format Fortran files.
pub struct FortranPostprocWriter {
    file: PathBuf,
    replace_comment_in_format: bool,
}

impl FortranPostprocWriter {
    /// Create a new Fortran fixed-format writer.
    /// 
    /// `file` is the path to the file to write; it must have the correct
    /// extension already. `replace_comment_in_format` determines whether
    /// the "a1" field in the format string is replaced with "1x" (`true`)
    /// in the file header, or kept as "a1" (false). This allows us to match
    /// previous file formats for compatibility with Fortran programs.
    pub fn new(file: PathBuf, replace_comment_in_format: bool) -> Self {
        Self { file, replace_comment_in_format }
    }
}

impl PostprocWriter for FortranPostprocWriter {
    fn write_postproc_file<I>(&self, header: &PostprocFileHeader, row_iter: I) -> error_stack::Result<(), GggError>
    where I: Iterator<Item = Result<PostprocRow, GggError>> {
        let fw = std::fs::File::create(&self.file).change_context_lazy(|| GggError::CouldNotWrite { 
            path: self.file.clone(),
            reason: "could not open output file for writing".to_string()
        })?;
        let mut fw = std::io::BufWriter::new(fw);

        let fmt_str = if self.replace_comment_in_format {
            header.fformat_without_comment().fmt_string(1)
        } else {
            header.fformat.fmt_string(1)
        };

        write_postproc_header(
            &mut fw,
            header.column_names.len(),
            header.nrec,
            header.naux,
            &header.program_versions,
            &header.extra_lines,
            header.missing_value,
            &fmt_str,
            &header.column_names,
        )
        .change_context_lazy(|| GggError::CouldNotWrite { 
            path: self.file.clone(),
            reason: "error occurred while writing the file header".to_string()
        })?;

        // We want to allow skipped fields in case we are reading a file that omitted
        // fields allowed to have defaults in the auxiliary columns.
        let settings = fortformat::ser::SerSettings::default()
            .align_left_str(true)
            .allow_skipped_fields(true);
        // Handle replacing the "a1" column that we retain for backwards compatibility with
        // older runlog formats - this can't go in the format string because it represents a
        // commenting-out character that we don't have a field for.
        let writer_format_spec = header.fformat_without_comment();

        for (irow, row_res) in row_iter.enumerate() {
            let row = row_res.change_context_lazy(|| GggError::CouldNotWrite { 
                path: self.file.clone(),
                reason: format!("error getting data for output row {}", irow + 1)
            })?;
            fortformat::ser::to_writer_custom(
                row,
                &writer_format_spec,
                Some(&header.column_names),
                &settings,
                &mut fw,
            )
            .change_context_lazy(|| GggError::CouldNotWrite { 
                path: self.file.clone(),
                reason: format!("error serializing data line {}", irow + 1)
            })?;
        }

        Ok(())
    }
}


/// Write the header of a postprocessing file.
///
/// # Inputs
/// - `f`: the handle to write to, usually a mutable [`std::io::BufWriter`] or similar.
/// - `ncol`: the number of columns in the file (including the spectrum name).
/// - `naux`: the number of columns containing auxiliary data (i.e not retrieved quantities).
/// - `program_versions`: the list of programs that generated this file to add to the header.
///   If using this to write the first post processing file, make sure to include GSETUP and GFIT
///   from the `.col` files, as well as the program generating the current file. If using this to
///   write a later post processing file, then usually previous program versions will be included
///   in the `extra_lines` read from the previous file's header, and this will only include the
///   new program.
/// - `extra_lines`: additional lines to include in the header, e.g. AICF or ADCF values.
/// - `missing_value`: the value to use as a fill value for missing data. Should be *significantly*
///   larger than any real value, [`POSTPROC_FILL_VALUE`] is a good default.
/// - `format_str`: the Fortran format string which the output follows.
/// - `column_names`: a slice of all the data columns' names.
///
/// A note on `format_str` regarding compatibility with Fortran GGG programs: many of these programs
/// expect a 1-character-wide column just after the spectrum name which is kept for compatibility with
/// older runlog formats. Since the Rust code does not serialize that, the `format_str` value you pass
/// here should include that if needed, even if that means it differs from the string used by [`fortformat`]
/// to actually write the output. (That is, usually you will remove the "a1" column for the string given
/// to [`fortformat`] and add one to the width of the spectrum name column.)
fn write_postproc_header<W: Write>(
    mut f: W,
    ncol: usize,
    nrow: usize,
    naux: usize,
    program_versions: &[ProgramVersion],
    extra_lines: &[String],
    missing_value: f64,
    format_str: &str,
    column_names: &[String],
) -> error_stack::Result<(), WriteError> {
    // Skip single-character fields; those seem to be a holdover to allow a : or ; to follow
    // the spectrum name?
    let col_width = fortformat::FortFormat::parse(format_str)
        .map_err(|e| WriteError::convert_error(
            format!("Could not interpret widths in format string: {e}")
        ))?.into_fields()
        .expect("Fortran format string should contain fixed width fields, not list-directed input (i.e. must not be '*')")
        .into_iter()
        .filter_map(|field| {
            let width = field.width().expect("write_postproc_header should not receive a format string with non-fixed width fields");
            if width > 1 { 
                Some(width)
            } else {
                None
            }
        });

    // The extra 4 = line with nhead etc. + missing + format + colnames
    let nhead = program_versions.len() + extra_lines.len() + 4;
    let first_line_format = fortformat::FortFormat::parse("(i2,i5,i7,i4)")
        .expect("The (hard coded) Fortran format for the first line of a post-processing output file should be valid");
    fortformat::to_writer((nhead, ncol, nrow, naux), &first_line_format, &mut f)
        .change_context_lazy(|| WriteError::IoError)?;

    for pver in program_versions.iter() {
        writeln!(f, " {pver}").change_context_lazy(|| WriteError::IoError)?;
    }

    for line in extra_lines {
        // The trim_end protects against newlines being accidentally doubled from lines read in
        // from a previous file.
        writeln!(f, "{}", line.trim_end()).change_context_lazy(|| WriteError::IoError)?;
    }

    let mvfmt = fortformat::FortFormat::parse("(1pe11.4)").unwrap();
    let mvstr = fortformat::to_string(missing_value, &mvfmt).unwrap();
    writeln!(f, "missing: {mvstr}").change_context_lazy(|| WriteError::IoError)?;

    writeln!(f, "format:{format_str}").change_context_lazy(|| WriteError::IoError)?;

    for (width, name) in col_width.zip(column_names) {
        let width = width as usize;
        let n = if name.len() >= width - 1 {
            0
        } else {
            width - 1 - name.len()
        };
        write!(f, " {name}{}", " ".repeat(n)).change_context_lazy(|| WriteError::IoError)?;
    }
    writeln!(f, "").change_context_lazy(|| WriteError::IoError)?;

    Ok(())
}
