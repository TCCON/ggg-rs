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
    pub(crate) fn usage_error<S: ToString>(msg: S) -> Self {
        Self::UsageError(msg.to_string())
    }

    pub(crate) fn input_error<S: ToString>(msg: S) -> Self {
        Self::InputError(msg.to_string())
    }

    pub(crate) fn runtime_error<S: ToString>(msg: S) -> Self {
        Self::RuntimeError(msg.to_string())
    }

    pub(crate) fn internal_error<S: ToString>(msg: S) -> Self {
        Self::InternalError(msg.to_string())
    }
}

impl Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (type_str, err_msg, fix_msg) = match self {
            CliError::UsageError(msg) => {
                let typestr = "Usage error";
                let fix = "Please double check that you are calling this program correctly. If you cannot find the mistake, reach out to the TCCON algorithm team.";
                (typestr, msg, fix)
            },
            CliError::InputError(msg) => {
                let typestr = "Input error";
                let fix = "Please double check the file referenced in the above error message and investigate the root cause of the problem mentioned. If you cannot find the problem, reach out the the TCCON algorithm team and be prepared to provide your full set of input files (from both the run directory and under GGGPATH).";
                (typestr, msg, fix)
            },
            CliError::RuntimeError(msg) => {
                let typestr = "Runtime error";
                let fix = "This may indicate a temporary problem with you system, an incorrect system setup, or a system setup not well supported by the netCDF write. Wait a few minutes, then try the action that failed again. If the problem persists, please check if there is something on your system causing the problem (e.g. incorrect file permissions) before asking the TCCON algorithm team for assistance.";
                (typestr, msg, fix)
            },
            CliError::InternalError(msg) => {
                let typestr = "Internal error";
                let fix = "This likely indicates a problem with the netCDF writer. Please assemble a minimal working example of the problem and open an issue on the GGGRS GitHub page";
                (typestr, msg, fix)
            },
        };

        writeln!(f, "{type_str}: {err_msg}\n\n{fix_msg}")
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
        Self::custom(msg.to_string())
    }
}

impl IntoCliReport for InputError {
    fn into_cli_report(self) -> error_stack::Report<CliError> {
        error_stack::Report::new(CliError::InputError(self.to_string()))
    }
}