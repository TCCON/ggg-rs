use std::{fmt::Display, io::Write, str::FromStr, path::Path, collections::HashMap};

use error_stack::ResultExt;
use ggg_rs::{utils, i2s::{I2SVersion, I2SLineType, iter_i2s_lines}};

use crate::CliError;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ParamMap {
    from: usize,
    to: usize
}

impl Display for ParamMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{},{}", self.from, self.to)
    }
}

impl FromStr for ParamMap {
    type Err = CliError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (from_str, to_str) = if let Some(x) = s.split_once(",") {
            (x.0, x.1)
        } else {
            return Err(CliError::BadInput(
                "Parameter map values must be two numbers separated by a comma (no space)".to_string()
            ));
        };

        let from = from_str.parse::<usize>()
            .map_err(|e| CliError::BadInput(
                format!("In parameter map '{s}', could not parse the left number ({from_str}). Reason was: {e}")
            ))?;

        if from < 1 {
            return Err(CliError::BadInput(
                format!("In parameter map '{s}', left number cannot be < 1")
            ));
        }

        let to = to_str.parse::<usize>()
            .map_err(|e| CliError::BadInput(
                format!("In parameter map '{s}', could not parse the right number ({to_str}). Reason was: {e}")
            ))?;

        if to < 1 {
            return Err(CliError::BadInput(
                format!("In parameter map '{s}', right number cannot be < 1")
            ));
        }

        Ok(Self { from, to })
    }
}

pub(crate) fn driver(src_file: &Path, dest_file: &Path, output_cfg: utils::OutputOptCli, top_params: &[ParamMap],
                     src_i2s_version: I2SVersion, dest_i2s_version: I2SVersion, copy_catalog: bool) -> error_stack::Result<(), CliError> {
    let mut writer = output_cfg.setup_output(dest_file)
        .change_context_lazy(|| CliError::WriteError(dest_file.to_path_buf()))?;
    let out_file = writer.output_path().to_path_buf();
    dbg!((dest_file, &out_file));
    let copy_params = load_params_to_copy(src_file, top_params, src_i2s_version, dest_i2s_version)?;

    let dest_iter = iter_i2s_lines(dest_file, dest_i2s_version)
        .change_context_lazy(|| CliError::ReadError(dest_file.to_path_buf()))?;

    for element in dest_iter {
        let (line_type, orig_line) = element.change_context_lazy(|| CliError::ReadError(dest_file.to_path_buf()))?;

        let new_line = match line_type {
            I2SLineType::HeaderParam(i) => copy_params.get(&i),
            I2SLineType::HeaderLine => None,
            I2SLineType::CatalogRow | I2SLineType::Other => {
                // If we need to copy the catalog from SRC to DEST, then we need to break out of this
                // loop, as it will copy the catalog of DEST
                if copy_catalog {
                    break;
                } else {
                    None
                }
            }
        };

        dbg!((line_type, new_line));

        if let Some(new_line) = new_line {
            write!(&mut writer, "{new_line}").change_context_lazy(|| CliError::WriteError(out_file.clone()))?;
        } else {
            println!("Writing original line");
            write!(&mut writer, "{orig_line}").change_context_lazy(|| CliError::WriteError(out_file.clone()))?;
        }
    }

    // This was cleaner to do as a separate loop; if we wanted to copy this in the main loop, we'd have to
    // have some way to change how much that loop iterates.
    if copy_catalog {
        let src_iter = iter_i2s_lines(src_file, src_i2s_version)
            .change_context_lazy(|| CliError::ReadError(src_file.to_path_buf()))?;

        for element in src_iter {
            let (src_line_type, src_line) = element.change_context_lazy(|| CliError::ReadError(src_file.to_path_buf()))?;
            match src_line_type {
                I2SLineType::HeaderParam(_) | I2SLineType::HeaderLine => (),
                I2SLineType::CatalogRow | I2SLineType::Other => {
                    write!(&mut writer, "{src_line}").change_context_lazy(|| CliError::WriteError(out_file.to_path_buf()))?;
                }
            }
        }
    }

    writer.finalize().change_context_lazy(|| CliError::WriteError(out_file))?;

    Ok(())
}


fn load_params_to_copy(src_file: &Path, top_params: &[ParamMap], src_i2s_version: I2SVersion, dest_i2s_version: I2SVersion) -> error_stack::Result<HashMap<usize, String>, CliError> {
    if top_params.iter().any(|m| m.to > dest_i2s_version.num_header_params()) {
        return Err(CliError::BadInput(
            format!("Cannot copy a top parameter into a new parameter. (For destination I2S version {dest_i2s_version}, TO values cannot be > {})", dest_i2s_version.num_header_params())
        ).into());
    }

    let mut copy_params = HashMap::new();

    let src_iter = iter_i2s_lines(src_file, src_i2s_version)
        .change_context_lazy(|| CliError::ReadError(src_file.to_path_buf()))?;

    for element in src_iter {
        let (line_type, line) = element.change_context_lazy(|| CliError::ReadError(src_file.to_path_buf()))?;
        if let Some(output_param_num) = get_output_param(top_params, line_type) {
            copy_params.insert(output_param_num, line);
        }
    }

    Ok(copy_params)
}

fn get_output_param(top_params: &[ParamMap], line_type: I2SLineType) -> Option<usize> {
    let input_param_num = if let I2SLineType::HeaderParam(i) = line_type {
        i
    } else {
        return None;
    };

    for param in top_params {
        if param.from == input_param_num {
            return Some(param.to);
        }
    }

    None
}