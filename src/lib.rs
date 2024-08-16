/// Common error types
pub mod error;
/// General common utilities
pub mod utils;
/// Utilities related to interpolation
pub mod interpolation;
/// Reading runlogs
pub mod runlogs;
/// Reading OPUS interferograms or spectra
pub mod opus;
/// Utilities for handling I2S configuration
pub mod i2s;
/// Utilities for interpreting spectra named following the Caltech TCCON convention
pub mod cit_spectrum_name;
/// Utilities for reading and writing GGG output files
pub mod output_files;
/// Common code for `collate-*-results` programs.
pub mod collation;
/// Code supporting TCCON-focused programs
pub mod tccon;

#[cfg(feature = "python")]
mod python;