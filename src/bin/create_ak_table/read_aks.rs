use std::{io::BufRead, path::{Path, PathBuf}};

use error_stack::ResultExt;
use itertools::Itertools;
use serde::Deserialize;

use ggg_rs::utils::{get_nhead, FileBuf};

use crate::naming;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReadError {
    #[error("{0}")]
    FileName(String),
    #[error("An averaging kernel table for {0} already exists in the output file")]
    ExistingAk(String),
    #[error("An error occurred while {0}")]
    Context(String)
}

impl ReadError {
    fn context<C: ToString>(ctx: C) -> Self {
        Self::Context(ctx.to_string())
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct AkInfo {
    pub(crate) ispec: usize,
    pub(crate) zmin: f64,
    pub(crate) sza: f64,
    pub(crate) airmass: f64,
    pub(crate) z: f64,
    pub(crate) ak: f64,
    pub(crate) p: f64
}

pub(crate) fn read_akall_file(path: &Path) -> error_stack::Result<Vec<AkInfo>, ReadError> {
    let mut f = FileBuf::open(path)
        .change_context_lazy(|| ReadError::context(format!("opening AK .all file, {}", path.display())))?;
    let nhead = get_nhead(&mut f)
        .change_context_lazy(|| ReadError::context(format!("getting the number of header lines in AK .all file, {}", path.display())))?;

    // We're on the second line of the file, and we want to get to the last line to read the column names.
    for _ in 1..nhead-1 {
        f.read_header_line().change_context_lazy(|| ReadError::context(format!("reading header of AK .all file, {}", path.display())))?;
    }
    let col_name_line = f.read_header_line().change_context_lazy(|| ReadError::context(format!("reading header of AK .all file, {}", path.display())))?;
    let col_names = col_name_line.trim().split_whitespace().collect_vec();
    let mut aks = vec![];
    let mut iline = nhead;
    for line in f.lines() {
        iline += 1;
        let line = line.change_context_lazy(|| ReadError::context(format!("reading line {iline} of AK .all file, {}", path.display())))?;
        let this_ak_row: AkInfo = fortformat::from_str_with_fields(&line, &fortformat::FortFormat::ListDirected, &col_names)
            .change_context_lazy(|| ReadError::context(format!("deserializing line {iline} of AK .all file, {}", path.display())))?;
        aks.push(this_ak_row);
    }
    Ok(aks)
}

pub(crate) fn gas_name_from_path(path: &Path) -> error_stack::Result<String, ReadError> {
    let base_name = path.file_name()
        .ok_or_else(|| ReadError::FileName(format!("Could not get the base name of input file, {}", path.display())))?
        .to_string_lossy();

    // Assume that it is named something like "k0_GAS_...". We don't actually care about the k0 part,
    // just that we need to split on underscores and take the second part.
    let gas = base_name.split('_').take(2).last()
        .ok_or_else(|| ReadError::FileName(format!("AK .all file {base_name} has an unexpected naming pattern: there should be at least two parts when split on an underscore, but there were not.")))?;
    Ok(gas.to_string())
}

pub(crate) fn check_existing_gases<P: AsRef<Path>>(ds: &netcdf::File, ak_all_files: &[P]) -> error_stack::Result<(), ReadError> {
    for p in ak_all_files {
        let gas = gas_name_from_path(p.as_ref())?;
        let ak_varname = naming::ak_varname(&gas);
        if ds.variable(&ak_varname).is_some() {
            return Err(ReadError::ExistingAk(gas).into());
        }
    }
    Ok(())
}