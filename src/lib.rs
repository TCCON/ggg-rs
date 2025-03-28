/// Common error types
pub mod error;
/// General common utilities
pub mod utils;
/// Utilities related to interpolation
pub mod interpolation;
/// Reading OPUS interferograms or spectra
pub mod opus;
/// Utilities for handling I2S configuration
pub mod i2s;
/// Utilities for interpreting spectra named following the Caltech TCCON convention
pub mod cit_spectrum_name;
/// Utilities for reading GGG files.
pub mod readers;
/// Utilities for writing GGG files
pub mod writers;
/// Interfaces for calculating the mean atmospheric O2 mole fraction
pub mod o2_dmf;
/// Common code for `collate-*-results` programs.
pub mod collation;
/// Code supporting TCCON-focused programs
pub mod tccon;

#[cfg(test)]
mod test_utils;
