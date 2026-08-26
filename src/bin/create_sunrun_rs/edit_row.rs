use error_stack::ResultExt;
use ggg_rs::{sunrun::ExpandedSunrunRow, utils::GggError};
use mlua::Lua;

use crate::{site_config::ModRow, CliError};

/// Wrapper struct that helps apply the configured edits to a sunrun row.
pub(crate) struct RowEditor {
    lua: Lua,
}

impl RowEditor {
    pub(crate) fn new_default() -> error_stack::Result<Self, CliError> {
        let lua = ggg_rs::lua::setup_ggg_lua()
            .change_context_lazy(|| CliError::custom("Error while setting up the Lua engine"))?;
        Ok(Self { lua })
    }
}

impl RowEditor {
    pub(crate) fn edit_row(
        &self,
        mut row: ExpandedSunrunRow,
        edits: &[ModRow],
    ) -> Result<ExpandedSunrunRow, GggError> {
        for edit in edits {
            row = edit.apply(row, &self.lua)?;
        }
        Ok(row)
    }
}
