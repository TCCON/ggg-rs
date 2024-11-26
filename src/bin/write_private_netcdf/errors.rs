use std::{fmt::Display, path::PathBuf};

/// An extension trait to allow converting an error type into an
/// `error_stack::Report<CliError>`.
pub(crate) trait IntoCliReport {
    fn into_cli_report(self) -> error_stack::Report<CliError>;
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CliError {
    /// This error should only be used if the user called the program incorrectly
    UsageError(String),

    /// This error should be used if there is a problem with the input files, such as
    /// a file being missing, formatted incorrectly, has duplicate values for one
    /// observation, etc.
    InputError(String),

    /// This error should indicate a problem that may not necessarily be the user's
    /// fault, but is a problem with their system; for example, a file existing, but
    /// not being readable.
    RuntimeError(String),

    /// This error should indicate a problem with the design of the netCDF writer itself;
    /// that is, something which the user should not be expected to fix.
    InternalError(String),
}

impl CliError {
    /// This error should only be used if the user called the program incorrectly
    pub(crate) fn usage_error<S: ToString>(msg: S) -> Self {
        Self::UsageError(msg.to_string())
    }

    /// This error should be used if there is a problem with the input files, such as
    /// a file being missing, formatted incorrectly, has duplicate values for one
    /// observation, etc.
    pub(crate) fn input_error<S: ToString>(msg: S) -> Self {
        Self::InputError(msg.to_string())
    }

    /// This error should indicate a problem that may not necessarily be the user's
    /// fault, but is a problem with their system; for example, a file existing, but
    /// not being readable.
    pub(crate) fn runtime_error<S: ToString>(msg: S) -> Self {
        Self::RuntimeError(msg.to_string())
    }

    /// This error should indicate a problem with the design of the netCDF writer itself;
    /// that is, something which the user should not be expected to fix.
    pub(crate) fn internal_error<S: ToString>(msg: S) -> Self {
        Self::InternalError(msg.to_string())
    }

    /// This is not an error constructor, it returns a final help message to give
    /// the user if this kind of error happens. This should be the last thing printed
    /// before the program exits unsuccessfully.
    pub(crate) fn user_message(&self) -> &'static str {
        match self {
            CliError::UsageError(_) => {
                "Please double check that you are calling this program correctly. If you cannot find the mistake, reach out to the TCCON algorithm team."
            },
            CliError::InputError(_) => {
                "Please double check the file referenced in the above error message and investigate the root cause of the problem mentioned. If you cannot find the problem, reach out the the TCCON algorithm team and be prepared to provide your full set of input files (from both the run directory and under GGGPATH)."
            },
            CliError::RuntimeError(_) => {
                "This may indicate a temporary problem with you system, an incorrect system setup, or a system setup not well supported by the netCDF write. Wait a few minutes, then try the action that failed again. If the problem persists, please check if there is something on your system causing the problem (e.g. incorrect file permissions) before asking the TCCON algorithm team for assistance."
            },
            CliError::InternalError(_) => {
                "This likely indicates a problem with the netCDF writer. Please assemble a minimal working example of the problem and open an issue on the GGGRS GitHub page"
            },
        }
    }
}

impl Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (type_str, err_msg) = match self {
            CliError::UsageError(msg) => {
                let typestr = "Usage error";
                (typestr, msg)
            },
            CliError::InputError(msg) => {
                let typestr = "Input error";
                (typestr, msg)
            },
            CliError::RuntimeError(msg) => {
                let typestr = "Runtime error";
                (typestr, msg)
            },
            CliError::InternalError(msg) => {
                let typestr = "Internal error";
                (typestr, msg)
            },
        };

        writeln!(f, "{type_str}: {err_msg}")
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum InputError {
    #[error("A required input file was not present on disk: {}", .0.display())]
    FileNotFound(PathBuf),
    #[error("Error occurred while opening or reading file {}", .0.display())]
    ErrorReadingFile(PathBuf),
    #[error("Error occurred while reading line {1} of file {}", .0.display())]
    ErrorReadingAtLine(PathBuf, usize),
    #[error("{0}")]
    Custom(String),
}

impl InputError {
    pub(crate) fn file_not_found<P: Into<PathBuf>>(p: P) -> Self {
        Self::FileNotFound(p.into())
    }

    pub(crate) fn error_reading_file<P: Into<PathBuf>>(p: P) -> Self {
        Self::ErrorReadingFile(p.into())
    }

    pub(crate) fn error_reading_at_line<P: Into<PathBuf>>(p: P, line_num: usize) -> Self {
        Self::ErrorReadingAtLine(p.into(), line_num)
    }

    pub(crate) fn custom<S: ToString>(msg: S) -> Self {
        Self::Custom(msg.to_string())
    }
}

impl IntoCliReport for InputError {
    fn into_cli_report(self) -> error_stack::Report<CliError> {
        error_stack::Report::new(CliError::InputError(self.to_string()))
    }
}


/// Errors that occur when writing a netCDF variable.
#[derive(Debug, thiserror::Error)]
pub(crate) enum WriteError {
    /// Represents an error returned by the netCDF library.
    #[error(transparent)]
    Netcdf(#[from] netcdf::Error),

    /// Represents an error that occurs when creating the variable to be
    #[error(transparent)]
    VarCreation(#[from] VarError),

    /// Represents an error that occurs from reading a file, it is assumed that
    /// this will wrap a lower level error
    #[error("Error reading file {}", .0.display())]
    FileReadError(PathBuf),

    /// Similar to FileReadError but with the ability to provide more information
    /// as to the context of the error
    #[error("Error reading {}: {1}", .0.display())]
    DetailedReadError(PathBuf, String),

    /// Error to use if a dimension required by a provider was not created in the netCDF file
    #[error("Dimension '{dimname}', required by the {requiring_file} file, was not created properly")]
    MissingDimError{requiring_file: String, dimname: &'static str},

    /// General-purpose error
    #[error("{0}")]
    Custom(String),
}

impl WriteError {
    pub(crate) fn file_read_error<P: Into<PathBuf>>(p: P) -> Self {
        Self::FileReadError(p.into())
    }

    pub(crate) fn detailed_read_error<P: Into<PathBuf>, S: ToString>(p: P, reason: S) -> Self {
        Self::DetailedReadError(p.into(), reason.to_string())
    }

    pub(crate) fn missing_dim_error<S: ToString>(req_file: S, dimname: &'static str) -> Self {
        Self::MissingDimError { requiring_file: req_file.to_string(), dimname }
    }

    pub(crate) fn custom<S: ToString>(msg: S) -> Self {
        Self::Custom(msg.to_string())
    }
}


/// An error representing problems creating a variable to be
#[derive(Debug, thiserror::Error)]
pub(crate) enum VarError {
    /// Used if the number of dimension names given does not match the number of dimensions of the data array
    #[error("Variable {name}: array has {array_ndim} dimensions, {n_dim_names} dimension names were supplied")]
    DimMismatch{name: String, array_ndim: usize, n_dim_names: usize},

    /// Used if the source file does not exist on disk
    #[error("Variable {name}: source file {} is missing", .path.display())]
    SourceFileMissing{name: String, path: PathBuf},

    /// Used for miscellaneous problems accessing the source file (e.g. to compute the checksum)
    #[error("Variable {name}, source file {}: {problem}", .path.display())]
    SourceFileError{name: String, path: PathBuf, problem: String}
}