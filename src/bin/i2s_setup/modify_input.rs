use std::path::PathBuf;
use std::str::FromStr;
use std::{io::Write, path::Path};

use error_stack::ResultExt;
use ggg_rs::{
    i2s::{iter_i2s_lines, I2SInputModifcations, I2SVersion},
    utils::{read_input_file_or_stdin, OptInplaceWriter, OutputOptCli},
};

use crate::CliError;

pub(crate) fn driver(
    input_file: PathBuf,
    edits_json: Option<PathBuf>,
    cli_edits: Vec<HeaderEditCli>,
    output_cfg: OutputOptCli,
    i2s_version: I2SVersion,
) -> error_stack::Result<(), CliError> {
    let writer = output_cfg
        .setup_output(&input_file)
        .change_context_lazy(|| CliError::IoError)?;

    let edits =
        edits_from_json_and_cli(edits_json.as_deref(), cli_edits).change_context_lazy(|| {
            CliError::BadInput("Could not set up I2S input edits".to_string())
        })?;

    edit_i2s_input_file(&input_file, writer, i2s_version, edits)
}

pub(crate) fn edit_i2s_input_file(
    input_file: &Path,
    mut writer: OptInplaceWriter,
    i2s_version: I2SVersion,
    edits: I2SInputModifcations,
) -> error_stack::Result<(), CliError> {
    let input_iter = iter_i2s_lines(&input_file, i2s_version)
        .change_context_lazy(|| CliError::ReadError(input_file.to_path_buf()))?;

    let out_path = writer.output_path().to_path_buf();

    for line_res in input_iter {
        let (line_type, line) =
            line_res.change_context_lazy(|| CliError::ReadError(input_file.to_path_buf()))?;
        if let Some(new_line) = edits.change_line_opt(line_type) {
            writeln!(&mut writer, "{new_line}")
                .change_context_lazy(|| CliError::WriteError(out_path.clone()))?;
        } else {
            write!(&mut writer, "{line}")
                .change_context_lazy(|| CliError::WriteError(out_path.clone()))?;
        }
    }

    writer
        .finalize()
        .change_context_lazy(|| CliError::WriteError(out_path))?;

    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct HeaderEditCli {
    parameter: usize,
    value: String,
}

impl FromStr for HeaderEditCli {
    type Err = CliError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (param_str, value_str) = s.split_once(",").ok_or_else(|| {
            CliError::BadInput(format!(
                "Header edit '{s}' incorrect: must have at least one comma"
            ))
        })?;

        let param = param_str.parse::<usize>().map_err(|e| {
            CliError::BadInput(format!(
                "Header edit '{s}' incorrect: value before the comma must be a number ({e})"
            ))
        })?;

        if param < 1 {
            return Err(CliError::BadInput(format!(
                "Header edit '{s}' incorrect: parameter number cannot be < 1"
            )));
        }

        Ok(Self {
            parameter: param,
            value: value_str.to_string(),
        })
    }
}

pub(crate) fn edits_from_json_and_cli(
    json_path: Option<&Path>,
    cli_edits: Vec<HeaderEditCli>,
) -> error_stack::Result<I2SInputModifcations, CliError> {
    let mut edits: I2SInputModifcations = if let Some(p) = json_path {
        let json_bytes = read_input_file_or_stdin(p)
            .change_context_lazy(|| CliError::ReadError(p.to_path_buf()))?;
        serde_json::from_slice(&json_bytes).change_context_lazy(|| {
            CliError::BadInput("Could not parse JSON edits file".to_string())
        })?
    } else {
        I2SInputModifcations::default()
    };
    for ed in cli_edits {
        edits.set_parameter_change(ed.parameter, ed.value);
    }

    Ok(edits)
}
