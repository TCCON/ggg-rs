/// Utilities for interpreting spectra named following the Caltech TCCON convention
pub mod cit_spectrum_name;
/// Common code for `collate-*-results` programs.
pub mod collation;
/// Common error types
pub mod error;
/// Utilities for handling I2S configuration
pub mod i2s;
/// Utilities related to interpolation
pub mod interpolation;
/// Helper functions for consistent logging
pub mod logging;
/// Utilities for netCDF files
#[cfg(feature = "netcdf")]
pub mod nc_utils;
/// Interfaces for calculating the mean atmospheric O2 mole fraction
pub mod o2_dmf;
/// Reading OPUS interferograms or spectra
pub mod opus;
/// Utilities for reading GGG files.
pub mod readers;
/// Code supporting TCCON-focused programs
pub mod tccon;
/// Code used in multiple tests
pub mod test_utils;
/// Helper functions for unit conversions
pub mod units;
/// General common utilities
pub mod utils;
/// Utilities for writing GGG files
pub mod writers;
