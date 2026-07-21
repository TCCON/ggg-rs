use std::path::Path;

use error_stack::ResultExt;

use crate::CliError;

mod read_l2;

pub(super) fn average_site_driver(
    l2_file: &Path,
    output_file: &Path,
    bin_width: chrono::Duration,
) -> error_stack::Result<(), CliError> {
    for var_def in [read_l2::CO2_VAR_DEF, read_l2::CH4_VAR_DEF] {
        let l2_data = read_l2::read_file(l2_file, &var_def).change_context_lazy(|| {
            CliError::context(format!("Error reading file {}", l2_file.display()))
        })?;
        dbg!(l2_data);
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AveragingError {}
