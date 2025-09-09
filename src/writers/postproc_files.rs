use std::io::Write;

use error_stack::ResultExt;

use crate::{error::WriteError, readers::ProgramVersion};

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
pub fn write_postproc_header<W: Write>(
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
