//! Code to read the qc.dat files
use std::{collections::HashMap, io::BufRead, path::Path};

use error_stack::ResultExt;
use ggg_rs::utils::{get_nhead_ncol, FileBuf};
use itertools::Itertools;
use serde::Deserialize;

use crate::errors::WriteError;

/// Represents one row in a qc.dat file
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct QcRow {
    pub(crate) variable: String,
    pub(crate) output: u8,
    pub(crate) scale: f64,
    #[allow(unused)]
    pub(crate) format: String,
    pub(crate) unit: String,
    pub(crate) vmin: f64,
    pub(crate) vmax: f64,
    pub(crate) description: String,
}

impl QcRow {
    pub(crate) fn do_output(&self) -> bool {
        self.output > 0
    }
}

/// Load a qc.dat file. The returned HashMap will have the variable names as its keys
/// (which will match) the variable name in the [`QcRow`]).
pub(crate) fn load_qc_file_hashmap(
    qc_file_path: &Path,
) -> error_stack::Result<HashMap<String, QcRow>, WriteError> {
    let mut hm = HashMap::new();
    for row in load_qc_file(qc_file_path)? {
        let key = row.variable.clone();
        hm.insert(key, row);
    }

    Ok(hm)
}

/// Load a qc.dat file as a vector of its rows. This is useful when you need the order
/// of the rows retained (e.g. when setting the flagged variable index).
pub(crate) fn load_qc_file(qc_file_path: &Path) -> error_stack::Result<Vec<QcRow>, WriteError> {
    let mut rdr = FileBuf::open(qc_file_path)
        .change_context_lazy(|| WriteError::file_read_error(qc_file_path))?;
    let (nhead, _) = get_nhead_ncol(&mut rdr)
        .change_context_lazy(|| WriteError::file_read_error(qc_file_path))?;

    // We've read the first header line, and we want to get the column names from the last header line
    for _ in 1..nhead - 1 {
        rdr.read_header_line()
            .change_context_lazy(|| WriteError::file_read_error(qc_file_path))?;
    }

    let colnames = rdr
        .read_header_line()
        .change_context_lazy(|| WriteError::file_read_error(qc_file_path))?;
    let colnames = colnames.split_ascii_whitespace().collect_vec();

    let mut qc_rows = vec![];
    let ff = fortformat::FortFormat::ListDirected;
    for (iline, line) in rdr.lines().enumerate() {
        let line_num = iline + nhead + 1;
        let line = line.change_context_lazy(|| {
            WriteError::detailed_read_error(qc_file_path, format!("failed to read line {line_num}"))
        })?;

        let this_row: QcRow = fortformat::from_str_with_fields(&line, &ff, &colnames)
            .change_context_lazy(|| {
                WriteError::detailed_read_error(
                    qc_file_path,
                    format!("error deserializing line {line_num}"),
                )
            })?;
        qc_rows.push(this_row);
    }

    Ok(qc_rows)
}
