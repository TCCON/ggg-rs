use std::path::Path;

use crate::CliError;

mod read_l2;

pub(super) fn average_site_driver(
    l2_file: &Path,
    output_file: &Path,
    bin_width: chrono::Duration,
) -> error_stack::Result<(), CliError> {
    Ok(())
}
