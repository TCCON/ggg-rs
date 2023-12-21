use std::fmt::Display;
use std::io::Write;
use std::path::{PathBuf, Path};
use std::str::FromStr;

use error_stack::ResultExt;
use ggg_rs::i2s::{I2SVersion, iter_i2s_header_params_with_number, iter_i2s_lines};
use itertools::Itertools;

use crate::CliError;

pub(crate) fn driver(
    input_files: &[PathBuf],
    output_file: &Path,
    i2s_version: I2SVersion,
    whitespace_method: ParamWhitespaceEq, 
    skip_check_params: &[usize]
) -> error_stack::Result<(), CliError> {
    check_inputs_match(input_files, i2s_version, whitespace_method, skip_check_params)?;

    let mut outf = std::fs::File::create(output_file)
        .change_context_lazy(|| CliError::WriteError(output_file.to_path_buf()))?;

    for (ifile, input_path) in input_files.into_iter().enumerate() {
        let line_iter = iter_i2s_lines(&input_path, i2s_version)
            .change_context_lazy(|| CliError::ReadError(input_path.to_path_buf()))?;

        for line in line_iter {
            let (is_header, line) = line.change_context_lazy(|| CliError::ReadError(input_path.to_path_buf()))?;
            if !is_header || ifile == 0 {
                write!(outf, "{line}").change_context_lazy(|| CliError::WriteError(output_file.to_path_buf()))?;
            }
        }
    }
    Ok(())
}

fn check_inputs_match(
    input_files: &[PathBuf],
    i2s_version: I2SVersion,
    whitespace_method: ParamWhitespaceEq, 
    skip_params: &[usize]
) -> error_stack::Result<(), CliError> {
    if input_files.len() < 2 {
        return Ok(())
    }

    for i in 1..input_files.len() {
        let first_file_it = iter_i2s_header_params_with_number(&input_files[0], i2s_version)
            .change_context_lazy(|| CliError::ReadError(input_files[0].clone()))?;
        let other_file_it = iter_i2s_header_params_with_number(&input_files[i], i2s_version)
            .change_context_lazy(|| CliError::ReadError(input_files[i].clone()))?;

        for value_pair in first_file_it.zip_longest(other_file_it) {
            match value_pair {
                itertools::EitherOrBoth::Both(value1, value2) => {
                    let (num1, val1) = value1.change_context_lazy(|| CliError::ReadError(input_files[0].clone()))?;
                    let (_, val2) = value2.change_context_lazy(|| CliError::ReadError(input_files[i].clone()))?;

                    if !skip_params.contains(&num1) && !whitespace_method.params_eq(&val1, &val2, num1, i2s_version) {
                        return Err(CliError::ParamMismatch { 
                            f1: input_files[0].clone(),
                            v1: val1,
                            f2: input_files[1].clone(),
                            v2: val2,
                            param: num1
                        }.into())
                    }
                },
                itertools::EitherOrBoth::Left(value) => {
                    let (num, val) = value.change_context_lazy(|| CliError::ReadError(input_files[0].clone()))?;
                    return Err(CliError::ParamMismatch { 
                        f1: input_files[0].clone(),
                        v1: val,
                        f2: input_files[i].clone(),
                        v2: "--MISSING--".to_string(),
                        param: num
                    }.into())
                },
                itertools::EitherOrBoth::Right(value) => {
                    let (num, val) = value.change_context_lazy(|| CliError::ReadError(input_files[i].clone()))?;
                    return Err(CliError::ParamMismatch { 
                        f1: input_files[0].clone(),
                        v1: "--MISSING--".to_string(),
                        f2: input_files[i].clone(),
                        v2: val,
                        param: num
                    }.into())
                },
            }
        }
    }

    Ok(())
}

/// Parameter equality check
/// 
/// The different modes control how it determines equality:
/// - `MatchAll` will require that all parameters be exactly
///   the same, including whitespace.
/// - `Default` will allow parameters for which it is safe
///   for whitespace to vary to do so, all others must match
///   exactly, including whitespace. This relies on the I2S
///   version.
/// - `IgnoreAll` will ignore whitespace differences in all
///   parameters.
/// 
/// To compare values, call the `params_eq` method on an instance.
/// **NOTE: any inline comments must be removed from the values before
/// passing them for comparison.**
#[derive(Debug, Clone, Copy)]
pub(crate) enum ParamWhitespaceEq {
    MatchAll,
    Default,
    IgnoreAll
}

impl Default for ParamWhitespaceEq {
    fn default() -> Self {
        Self::Default
    }
}

impl Display for ParamWhitespaceEq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParamWhitespaceEq::MatchAll => write!(f, "MatchAll"),
            ParamWhitespaceEq::Default => write!(f, "Default"),
            ParamWhitespaceEq::IgnoreAll => write!(f, "IgnoreAll"),
        }
    }
}

impl FromStr for ParamWhitespaceEq {
    type Err = CliError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "matchall" => Ok(Self::MatchAll),
            "default" => Ok(Self::Default),
            "ignoreall" => Ok(Self::IgnoreAll),
            _ => Err(CliError::BadInput(
                format!("invalid value '{s}' for whitespace equality, allowed values are 'matchall', 'default', or 'ignoreall' (case insensitive)")
            ))
        }
    }
}

impl ParamWhitespaceEq {
    pub(crate) fn params_eq(&self, value1: &str, value2: &str, param_num: usize, i2s_version: I2SVersion) -> bool {
        let ignore_whitespace = match self {
            ParamWhitespaceEq::MatchAll => false,
            ParamWhitespaceEq::Default => Self::param_ignore_whitespace(param_num, i2s_version),
            ParamWhitespaceEq::IgnoreAll => true,
        };

        if ignore_whitespace {
            Self::compare_without_whitespace(value1, value2)
        } else {
            value1 == value2
        }
    }

    fn compare_without_whitespace(value1: &str, value2: &str) -> bool {
        let mut value1 = value1.split_ascii_whitespace();
        let mut value2 = value2.split_ascii_whitespace();

        loop {
            match (value1.next(), value2.next()) {
                // Both iterators end at the same time, so the number of whitespace-separated elements match
                (None, None) => return true,

                // One iterator ended before the other, so the number of whitespace-separated elements differs
                (None, Some(_)) => return false,
                (Some(_), None) => return false,

                // Compare the non-whitespace elements, if they differ, the values do. If not, keep going.
                (Some(subv1), Some(subv2)) => {
                    if subv1 != subv2 {
                        return false;
                    }
                },
            }
        }
    }

    fn param_ignore_whitespace(param_num: usize, i2s_version: I2SVersion) -> bool {
        let unsafe_param_nums = match i2s_version {
            I2SVersion::I2S2014 => [1, 2, 4, 6, 8, 14].as_slice(),
            I2SVersion::I2S2020 => [1, 2, 4, 6, 8, 14].as_slice(),
        };

        return !unsafe_param_nums.contains(&param_num)
    }
}

