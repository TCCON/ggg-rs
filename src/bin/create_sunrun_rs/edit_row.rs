use ggg_rs::{sunrun::ExpandedSunrunRow, utils::GggError};
use mlua::Lua;

use crate::site_config::ModRow;

/// Wrapper struct that helps apply the configured edits to a sunrun row.
pub(crate) struct RowEditor {
    lua: Lua,
}

impl Default for RowEditor {
    fn default() -> Self {
        let lua = Lua::new();
        Self { lua }
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
