use ggg_rs::error::FileLocation;

#[derive(Debug, thiserror::Error)]
pub(crate) enum SetupError {
    #[error("Error reading {description}")]
    FileReadError{description: String},
    #[error("Error parsing {location}: {cause}")]
    ParsingError{location: FileLocation, cause: String},
}