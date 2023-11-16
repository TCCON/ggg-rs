use ggg_rs::error::FileLocation;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CliError {
    #[error("Error occurred during setup")]
    Setup,
    #[error("Error occurred while writing dimensions")]
    Dimension,
    #[error("An unexpected error occurred: {0}")]
    Unexpected(&'static str),
}


#[derive(Debug, thiserror::Error)]
pub(crate) enum SetupError {
    #[error("Error reading {description}")]
    FileReadError{description: String},
    #[error("Error parsing {location}: {cause}")]
    ParsingError{location: FileLocation, cause: String},
}