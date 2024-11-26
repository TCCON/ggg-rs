use std::{borrow::Cow, collections::HashMap, fmt::Display, hash::RandomState, io::BufRead, path::{Path, PathBuf}};

use error_stack::ResultExt;
use ggg_rs::{output_files::{open_and_iter_postproc_file, POSTPROC_FILL_VALUE}, utils::{get_nhead_ncol, FileBuf}};
use indexmap::IndexMap;
use itertools::Itertools;
use ndarray::Array1;
use serde::Deserialize;
use tracing::instrument;

use crate::{dimensions::TIME_DIM_NAME, errors::WriteError, interface::{ConcreteVarToBe, DataProvider, GroupWriter, SpectrumIndexer, StdDataGroup, VarToBe}};

/// Provider for .vav.ada.aia files
/// 
/// This is distinct from the other post processing file providers because it needs to
/// use the qc.dat file to scale and flag its values.
#[derive(Debug)]
pub(crate) struct AiaFile {
    aia_path: PathBuf,
    qc_path: PathBuf,
}

impl AiaFile {
    pub(crate) fn new(aia_path: PathBuf, qc_path: PathBuf) -> Self {
        Self { aia_path, qc_path }
    }

    fn read_variables(postproc_file: &Path, ntimes: usize, spec_indexer: &SpectrumIndexer, qc_rows: HashMap<String, QcRow>)
    -> error_stack::Result<Vec<ConcreteVarToBe<f64>>, WriteError> {

        let (header, row_iter) = open_and_iter_postproc_file(postproc_file)
            .change_context_lazy(|| WriteError::file_read_error(postproc_file))?;

        // Create arrays for all of the numeric variables
        let it = header.column_names.iter()
            .filter_map(|colname| {
                if colname != "spectrum" {
                    Some((colname.to_string(), Array1::from_elem((ntimes,), POSTPROC_FILL_VALUE)))
                } else {
                    None
                }
            });
        let mut data_arrays = IndexMap::<_,_,RandomState>::from_iter(it);

        // Every variable will need the file basename and checksum for the .aia file, so we get those
        // now to save recomputing the checksum every time.
        let aia_basename = postproc_file.file_name().expect("Couldn't get the basename of the .vav.ada.aia file");
        let aia_checksum = ggg_rs::utils::file_sha256_hexdigest(postproc_file)
            .change_context_lazy(|| WriteError::detailed_read_error(
                postproc_file, "failed to compute the SHA256 checksum"
            ))?;

        // For each row, find the correct index along the time dimension given the spectrum name,
        // and apply the scale from the QC file. Since this is the .aia file, we will get the numeric
        // auxiliary variables as well as the xgas ones.
        for (irow, row) in row_iter.enumerate() {
            let line_num = header.nhead + irow + 1;
            let row = row.change_context_lazy(|| WriteError::detailed_read_error(
                postproc_file, format!("could not read line {line_num}")
            ))?;
            let itime = spec_indexer.get_index_for_spectrum(&row.auxiliary.spectrum)
                .ok_or_else(|| WriteError::detailed_read_error(
                    postproc_file, format!("spectrum {} on line {line_num} of the .aia file was not in the runlog!", row.auxiliary.spectrum)
                ))?;

            // Aux variables first
            for &varname in ggg_rs::output_files::AuxData::postproc_fields_str() {
                if let Some(value) = row.auxiliary.get_numeric_field(varname) {
                    let arr = data_arrays.get_mut(varname)
                        .ok_or_else(|| WriteError::custom(format!(
                            "in the .aia file, {varname} is in line {line_num} but was not in the column names"
                        )))?;

                    let scale = qc_rows.get(varname)
                        .map(|row| row.scale)
                        .ok_or_else(|| WriteError::custom(format!(
                            "auxiliary variable {varname} from the .aia file was not included in the qc.dat file"
                        )))?;
                    arr[itime] = value * scale;
                }
            }

            // Now the xgas variables.
            for (varname, value) in row.retrieved.iter() {
                let arr = data_arrays.get_mut(varname)
                    .ok_or_else(|| WriteError::custom(format!(
                        "in the .aia file, {varname} is in line {line_num} but was not in the column names"
                    )))?;

                let scale = qc_rows.get(varname)
                    .map(|row| row.scale)
                    .ok_or_else(|| WriteError::custom(format!(
                        "retrieved variable {varname} from the .aia file was not included in the qc.dat file"
                    )))?;
                
                arr[itime] = value * scale;
            }
        }

        // Almost there, now we go through all the data arrays and construct the variables with metadata for them.
        let mut variables = vec![];
        for (varname, array) in data_arrays.into_iter() {
            // We've gotten the QcRows for every variable to this point, so we can safely unwrap
            let qc_row = qc_rows.get(&varname).unwrap();
            if !qc_row.do_output() {
                tracing::debug!("variable {varname} will not be included in the netCDF file because its output is disabled in the qc.dat file");
                continue;
            }

            let mut this_var = ConcreteVarToBe::new_with_checksum(
                varname.to_string(),
                vec![TIME_DIM_NAME],
                array.into_dyn(),
                varname.replace("_", " "),
                // The qc.dat files by convention often put the units in parentheses, which we don't want.
                qc_row.unit.trim_start_matches('(').trim_end_matches(')').to_string(),
                aia_basename.to_string_lossy().to_string(),
                aia_checksum.clone()
            );
            this_var.add_attribute("description", qc_row.description.clone());
            this_var.add_attribute("vmin", qc_row.vmin);
            this_var.add_attribute("vmax", qc_row.vmax);
            variables.push(this_var);
        }

        Ok(variables)
    }
}

impl Display for AiaFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, ".vav.ada.aia file")
    }
}

impl DataProvider for AiaFile {
    fn dimension_lengths(&self) -> std::borrow::Cow<[(&'static str, usize)]> {
        Cow::Borrowed(&[])
    }

    fn dimensions_required(&self) -> std::borrow::Cow<[&'static str]> {
        Cow::Owned(vec![TIME_DIM_NAME])
    }

    #[instrument(name = "aia_file_writer", skip_all)]
    fn write_data_to_nc(&self, spec_indexer: &crate::interface::SpectrumIndexer, writer: &dyn GroupWriter) -> error_stack::Result<(), WriteError> {
        let qc_rows = load_qc_file(&self.qc_path)?;
        let ntimes = writer.get_dim_length(TIME_DIM_NAME)
            .ok_or_else(|| WriteError::missing_dim_error(".aia", TIME_DIM_NAME))?;
        // TODO: compute flag variable
        let variables = Self::read_variables(&self.aia_path, ntimes, spec_indexer, qc_rows)?;
        let grouped_variables = split_ret_vars_to_groups(variables)?;
        for (group, vars) in grouped_variables {
            let tmp = vars.iter().map(|v| v.as_ref()).collect_vec();
            writer.write_many_variables(&tmp, &group)?;
        }
        Ok(())
    }
}

/// Represents one row in a qc.dat file
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct QcRow {
    variable: String,
    output: u8,
    scale: f64,
    format: String,
    unit: String,
    vmin: f64,
    vmax: f64,
    description: String,
}

impl QcRow {
    fn do_output(&self) -> bool {
        self.output > 0
    }
}

/// Load a qc.dat file. The returned HashMap will have the variable names as its keys
/// (which will match) the variable name in the [`QcRow`]).
fn load_qc_file(qc_file_path: &Path) -> error_stack::Result<HashMap<String, QcRow>, WriteError> {
    let mut rdr = FileBuf::open(qc_file_path)
        .change_context_lazy(|| WriteError::file_read_error(qc_file_path))?;
    let (nhead, _) = get_nhead_ncol(&mut rdr)
        .change_context_lazy(|| WriteError::file_read_error(qc_file_path))?;
    
    // We've read the first header line, and we want to get the column names from the last header line
    for _ in 1..nhead-1 {
        rdr.read_header_line().change_context_lazy(|| WriteError::file_read_error(qc_file_path))?;
    }

    let colnames = rdr.read_header_line()
        .change_context_lazy(|| WriteError::file_read_error(qc_file_path))?;
    let colnames = colnames.split_ascii_whitespace()
        .collect_vec();

    let mut qc_rows = HashMap::new();
    let ff = fortformat::FortFormat::ListDirected;
    for (iline, line) in rdr.lines().enumerate() {
        let line_num = iline + nhead + 1;
        let line = line.change_context_lazy(|| 
            WriteError::detailed_read_error(qc_file_path, format!("failed to read line {line_num}"))
        )?;

        let this_row: QcRow = fortformat::from_str_with_fields(&line, &ff, &colnames)
            .change_context_lazy(|| WriteError::detailed_read_error(
                qc_file_path, format!("error deserializing line {line_num}")
            ))?;
        let varname = this_row.variable.clone();
        qc_rows.insert(varname, this_row);
    }

    Ok(qc_rows)
}

/// Given a list of variables, split them into the standard TCCON groups.
/// 
/// This might be better done in the read_variables function, since that knows
/// which variables are retrieved vs. auxiliary.
fn split_ret_vars_to_groups(variables: Vec<ConcreteVarToBe<f64>>) 
    -> error_stack::Result<
        HashMap<StdDataGroup, Vec<Box<dyn VarToBe>>>,
        WriteError
    > {
    let mut grouped_vars = HashMap::new();

    // TODO: parse the group definition file in $GGGPATH/tccon instead of hardcoding these groups
    for var in variables {
        let group = if var.name().starts_with("xv") {
            StdDataGroup::Si
        } else if var.name().starts_with("xm") {
            StdDataGroup::InSb
        } else {
            StdDataGroup::InGaAsExperimental
        };

        if !grouped_vars.contains_key(&group) {
            grouped_vars.insert(group, vec![]);
        }
        let boxed_var: Box<dyn VarToBe> = Box::new(var);
        grouped_vars.get_mut(&group).unwrap().push(boxed_var);
    }

    Ok(grouped_vars)
}

// TODO: update this to handle ADCFs that may have three or five values, and use it to write the ADCFs
// fn parse_corr_fac_block<F: BufRead>(file: &mut FileBuf<F>, first_line: String, iline: &mut usize) 
// -> error_stack::Result<(String, HashMap<String, (f64, f64)>), HeaderError> {
//     
//     // and AICFs to the netCDF file
//     let (cf_name, cf_nums) = first_line.split_once(":")
//         .ok_or_else(|| HeaderError::ParseError { 
//             location: FileLocation::new(Some(file.path.clone()), Some(*iline+1), Some(first_line.clone())), 
//             cause: "Line containing 'Correction Factors' must have a colon in it".to_string()
//         })?;

//     let s = cf_nums.split_whitespace().nth(0)
//         .ok_or_else(|| HeaderError::ParseError { 
//             location: FileLocation::new(Some(file.path.clone()), Some(*iline+1), Some(first_line.clone())),
//             cause: "A corrections file line did not have at least one number after the colon".into()
//         })?;

//     let nfactor = s.parse::<usize>()
//         .change_context_lazy(|| HeaderError::ParseError {
//             location: FileLocation::new(Some(file.path.clone()), Some(*iline+1), Some(first_line.clone())),
//             cause: "Could not parse first value after colon in correction factor line as an unsiged integer".into()
//         })?;

//     let mut cf_map = HashMap::new();
//     for _ in 0..nfactor {
//         let line = file.read_header_line()?;
//         *iline += 1;
//         if let Some((key, value, uncertainty)) = line.split_whitespace().collect_tuple() {
//             let value = value.parse::<f64>()
//             .change_context_lazy(|| HeaderError::ParseError { 
//                 location: FileLocation::new(Some(file.path.clone()), Some(*iline+1), Some(line.clone())),
//                 cause: format!("Could not parse the {key} value into a float"),
//             })?;

//             let uncertainty = uncertainty.parse::<f64>()
//             .change_context_lazy(|| HeaderError::ParseError { 
//                 location: FileLocation::new(Some(file.path.clone()), Some(*iline+1), Some(line.clone())),
//                 cause: format!("Could not parse the {key} uncertainty into a float"),
//             })?;

//             let key = key.to_string();
//             cf_map.insert(key, (value, uncertainty));
//         } else {
//             let n = line.split_whitespace().count();
//             return Err(HeaderError::ParseError {
//                 location: FileLocation::new(Some(file.path.clone()), Some(*iline+1), Some(line)),
//                 cause: format!("A line with correction factor values should have 3 whitespace separated values, this one had {n}.")
//             }.into())
//         }
//     }

//     let cf_name = cf_name.split("Correction").nth(0)
//         .expect("Correction factor header line should have 'Correction' in it")
//         .trim()
//         .to_string();
//     Ok((cf_name, cf_map))
// }