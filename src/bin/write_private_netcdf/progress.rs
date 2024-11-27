//! Helper functions to set up consistent progress bars
use std::fmt::Display;

use indicatif::{ProgressBar, ProgressStyle};

/// Set up a progress bar for reading data from a GGG output file.
/// 
/// This will configure progress bar `pb` to have a prefix of "Reading {file_desc} file",
/// where `file_desc` is some short description of the file being read (such as its extension)
/// and a length of `nrec`, which is intended to be the number of records (i.e. data rows)
/// in the file, but which could be used to represent a different quantity.
pub(crate) fn setup_read_pb<D: Display>(pb: &ProgressBar, nrec: usize, file_desc: D) {
    pb.set_length(nrec as u64);
    pb.set_position(0);
    let style = ProgressStyle::with_template(
        "{prefix} {wide_bar} [{human_pos}/{human_len}]"
    ).unwrap();
    pb.set_style(style);
    pb.set_prefix(format!("Reading {file_desc} file"));
    pb.set_message("");
    pb.tick(); // force a redraw
}

/// Set up a progress bar for writing data to the netCDF file.
/// 
/// This will configure progress bar `pb` to have a prefix of "Writing {file_desc} variable"
/// followed by the bar's message (which is meant to be the variable name) and set it to
/// have length `nvar`, which is intended to be the number of variables to be written.
pub(crate) fn setup_write_pb<D: Display>(pb: &ProgressBar, nvar: usize, file_desc: D) {
    pb.set_length(nvar as u64);
    pb.set_position(0);
    let style = ProgressStyle::with_template(
        "{prefix} {msg} {wide_bar} [{human_pos}/{human_len}]"
    ).unwrap();
    pb.set_style(style);
    pb.set_prefix(format!("Writing {file_desc} variable"));
    pb.set_message("");
    pb.tick(); // force a redraw
}