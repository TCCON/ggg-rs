use std::{io::Write, path::Path};
use std::path::PathBuf;

use error_stack::ResultExt;
use ggg_rs::{utils::{OutputOptCli, read_input_file_or_stdin, OptInplaceWriter}, i2s::{I2SInputModifcations, iter_i2s_lines, I2SVersion}};

use crate::CliError;

pub(crate) fn driver(input_file: PathBuf, edits_json: PathBuf, output_cfg: OutputOptCli, i2s_version: I2SVersion) -> error_stack::Result<(), CliError> {
    let mut writer = output_cfg.setup_output(&input_file)
        .change_context_lazy(|| CliError::IoError)?;

    let json_bytes = read_input_file_or_stdin(&edits_json)
        .change_context_lazy(|| CliError::ReadError(edits_json))?;
    let edits: I2SInputModifcations = serde_json::from_slice(&json_bytes)
        .change_context_lazy(|| CliError::BadInput("Could not parse JSON edits file".to_string()))?;

    edit_i2s_input_file(&input_file, writer, i2s_version, edits)
}

pub(crate) fn edit_i2s_input_file(input_file: &Path, mut writer: OptInplaceWriter, i2s_version: I2SVersion, edits: I2SInputModifcations) -> error_stack::Result<(), CliError> {

    let input_iter = iter_i2s_lines(&input_file, i2s_version)
        .change_context_lazy(|| CliError::ReadError(input_file.to_path_buf()))?;

    let out_path = writer.output_path().to_path_buf();
    
    for line_res in input_iter {
        let (line_type, line) = line_res.change_context_lazy(|| CliError::ReadError(input_file.to_path_buf()))?;
        if let Some(new_line) = edits.change_line_opt(line_type) {
            writeln!(&mut writer, "{new_line}").change_context_lazy(|| CliError::WriteError(out_path.clone()))?;
        } else {
            write!(&mut writer, "{line}").change_context_lazy(|| CliError::WriteError(out_path.clone()))?;
        }
    }

    writer.finalize()
        .change_context_lazy(|| CliError::WriteError(out_path))?;

    Ok(())
}