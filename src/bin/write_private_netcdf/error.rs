use std::path::PathBuf;

use ggg_rs::error::FileLocation;

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("Error occurred during setup")]
    Setup,
    #[error("Error occurred while writing dimensions")]
    Dimension,
    #[error("An unexpected error occurred: {0}")]
    Unexpected(&'static str),
}


#[derive(Debug, thiserror::Error)]
pub enum SetupError {
    #[error("Error reading {description}")]
    FileReadError{description: String},
    #[error("File {} was not one of the expected kinds of {kind} file", .path.display())]
    FileKindError{path: PathBuf, kind: &'static str},
    #[error("Error parsing {location}: {cause}")]
    ParsingError{location: FileLocation, cause: String},
}