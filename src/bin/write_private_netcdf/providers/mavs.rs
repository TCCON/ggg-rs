use core::f32;
use std::{collections::HashMap, fmt::Display, path::PathBuf};

use error_stack::ResultExt;
use ggg_rs::readers::mav_files::open_and_iter_mav_file;
use itertools::Itertools;
use ndarray::{s, Array1, Array2};

use crate::{dimensions::{CELL_INDEX_DIM_NAME, PRIOR_ALT_DIM_NAME, TIME_DIM_NAME}, errors::{CliError, WriteError}, interface::{ConcreteVarToBe, DataProvider, GroupWriter, StdDataGroup, VarToBe}};

static DIMS_REQ: [&'static str; 3] = [TIME_DIM_NAME, PRIOR_ALT_DIM_NAME, CELL_INDEX_DIM_NAME];


pub(crate) struct MavFile {
    mav_file_path: PathBuf,
    mav_columns: Vec<String>,
    ncell: usize,
    nlev: usize
}


impl MavFile {
    pub(crate) fn new(mav_file_path: PathBuf) -> error_stack::Result<Self, CliError> {
        let first_mav_block = open_and_iter_mav_file(mav_file_path.clone())
            .change_context_lazy(|| CliError::input_error("failed to open the .mav file"))?
            .next()
            .ok_or_else(|| CliError::input_error(".mav file had no blocks!"))?
            .change_context_lazy(|| CliError::input_error("failed to read the first block from the .mav file"))?;

        let height = first_mav_block.data.get("Height")
            .ok_or_else(|| CliError::input_error("did not find a column named 'Height' in the first .mav block"))?;
        let ncell = height.mapv(|z| if z < -1.0 { 1 } else { 0 }).sum() as usize;
        let nlev = height.len() - ncell;
        Ok(Self { mav_file_path, mav_columns: first_mav_block.column_order, ncell, nlev })
    }


    fn build_mav_arrays(&self, spec_indexer: &crate::interface::SpectrumIndexer, writer: &dyn crate::interface::GroupWriter)
        -> error_stack::Result<MavArrays, WriteError>
    {
        let ntimes = writer.get_dim_length(&TIME_DIM_NAME)
            .ok_or_else(|| WriteError::missing_dim_error(".mav file", TIME_DIM_NAME))?;
        let mut atmosphere = HashMap::from_iter(self.mav_columns.iter().map(|c| {
            (c.to_string(), Array2::from_elem((ntimes, self.nlev), f32::NAN))
        }));
        let mut cells = HashMap::from_iter(self.mav_columns.iter().map(|c| {
            (c.to_string(), Array2::from_elem((ntimes, self.ncell), f32::NAN))
        }));

        let mut mav_iter = open_and_iter_mav_file(self.mav_file_path.clone())
            .change_context_lazy(|| WriteError::file_read_error(&self.mav_file_path))?;
        let mut curr_block = mav_iter.next()
            .ok_or_else(|| WriteError::custom(".mav file is empty"))?
            .change_context_lazy(|| WriteError::file_read_error(&self.mav_file_path))?;
        let mut next_block_opt = mav_iter.next()
            .transpose()
            .change_context_lazy(|| WriteError::file_read_error(&self.mav_file_path))?;

        // The altitude vector should not change, so we just get it from the first block
        let atmosphere_alts = curr_block.data.get("Height")
            .ok_or_else(|| WriteError::custom("Expected the first .mav block to contain a 'Height' column"))?
            .slice(s![self.ncell..])
            .to_owned();

        loop {

            let i_start_time = spec_indexer.get_index_for_spectrum(&curr_block.header.next_spectrum)
                .ok_or_else(|| WriteError::custom(format!(
                    "Spectrum '{}' from the .mav file not found in the list of spectra from the runlog. This usually happens when the runlog and output files are filtered for bad spectra after running gfit and before running post processing. That processing approach is not supported.",
                    curr_block.header.next_spectrum
                )))?;


            let i_end_time = if let Some(next_block) = &next_block_opt {
                let next_spec = &next_block.header.next_spectrum;
                spec_indexer.get_index_for_spectrum(&next_block.header.next_spectrum)
                    .ok_or_else(|| WriteError::custom(format!(
                        "Spectrum '{next_spec}' from the .mav file not found in the list of spectra from the runlog. This usually happens when the runlog and output files are filtered for bad spectra after running gfit and before running post processing. That processing approach is not supported.",
                    )))?
            } else {
                ntimes
            };

            for (colname, arr) in curr_block.data.iter() {
                if colname == "Height" {
                    continue;
                }

                let cell_values = arr.slice(s![0..self.ncell]);
                let atm_values = arr.slice(s![self.ncell..]);
                for i_time in i_start_time..i_end_time {
                    cells.get_mut(colname)
                        .expect("all .mav file columns should have an array in the cells hashmap")
                        .slice_mut(s![i_time, ..]).assign(&cell_values);
                    atmosphere.get_mut(colname)
                        .expect("all .mav file columns should have an array in the atmosphere hashmap")
                        .slice_mut(s![i_time, ..]).assign(&atm_values);
                }
            }

            if let Some(next_block) = next_block_opt {
                curr_block = next_block;
                next_block_opt = mav_iter.next()
                    .transpose()
                    .change_context_lazy(|| WriteError::file_read_error(&self.mav_file_path))?;
            } else {
                return Ok(MavArrays { height: atmosphere_alts, cell_profs: cells, atm_profs: atmosphere, col_order: curr_block.column_order })
            }
        }
    }
}

impl Display for MavFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, ".mav file")
    }
}

impl DataProvider for MavFile {
    fn dimension_lengths(&self) -> std::borrow::Cow<[(&'static str, usize)]> {
        let lengths = vec![
            (PRIOR_ALT_DIM_NAME, self.nlev),
            (CELL_INDEX_DIM_NAME, self.ncell)
        ];
        std::borrow::Cow::Owned(lengths)
    }

    fn dimensions_required(&self) -> std::borrow::Cow<[&'static str]> {
        std::borrow::Cow::Borrowed(&DIMS_REQ)
    }

    fn write_data_to_nc(&self, spec_indexer: &crate::interface::SpectrumIndexer, writer: &dyn crate::interface::GroupWriter, pb: indicatif::ProgressBar) -> error_stack::Result<(), crate::errors::WriteError> {
        let mav_data = self.build_mav_arrays(spec_indexer, writer)?;

        let mav_basename = self.mav_file_path.file_name().expect("Couldn't get the basename of the .mav file")
            .to_string_lossy()
            .to_string();
        let mav_checksum = ggg_rs::utils::file_sha256_hexdigest(&self.mav_file_path)
            .change_context_lazy(|| WriteError::detailed_read_error(
                self.mav_file_path.clone(), "failed to compute the SHA256 checksum"
            ))?;

        let mut variables: Vec<Box<dyn VarToBe>> = vec![Box::new(ConcreteVarToBe::new_with_checksum(
            "prior_altitude",
            vec![PRIOR_ALT_DIM_NAME],
            mav_data.height.into_dyn(),
            "prior altitude",
            "km",
            mav_basename.clone(),
            mav_checksum.clone()
        ))];

        for colname in mav_data.col_order {
            if colname == "Height" {
                continue;
            }
            let col_lower = colname.to_ascii_lowercase();
            let (units, varname) = if col_lower == "pres" {
                ("atm", "pressure")
            } else if col_lower == "density" {
                ("molec cm^-3", "density")
            } else if col_lower == "temp" {
                ("K", "temperature")
            } else {
                ("mol mol^-1", col_lower.as_str())
            };

            let cell_var = ConcreteVarToBe::new_with_checksum(
                format!("cell_{varname}"),
                vec![TIME_DIM_NAME, CELL_INDEX_DIM_NAME],
                mav_data.cell_profs.get(&colname).expect("All .mav columns should have a corresponding entry in the cell array hash map").clone().into_dyn(),
                format!("cell {varname}"),
                units,
                mav_basename.clone(),
                mav_checksum.clone()
            );
            
            let atm_var = ConcreteVarToBe::new_with_checksum(
                format!("prior_{varname}"),
                vec![TIME_DIM_NAME, PRIOR_ALT_DIM_NAME],
                mav_data.atm_profs.get(&colname).expect("All .mav columns should have a corresponding entry in the atmoshpere array hash map").clone().into_dyn(),
                format!("prior {varname}"),
                units,
                mav_basename.clone(),
                mav_checksum.clone()
            );

            variables.push(Box::new(cell_var));
            variables.push(Box::new(atm_var));
        }

        let tmp = variables.iter().map(|v| v.as_ref()).collect_vec();
        Ok(writer.write_many_variables(&tmp, &StdDataGroup::InGaAs, Some(&pb))?)
    }
}


struct MavArrays {
    height: Array1<f32>,
    cell_profs: HashMap<String, Array2<f32>>,
    atm_profs: HashMap<String, Array2<f32>>,
    col_order: Vec<String>
}
