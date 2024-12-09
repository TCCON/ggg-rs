use std::{borrow::Cow, collections::HashMap, fmt::Display, hash::RandomState, path::{Path, PathBuf}};

use error_stack::ResultExt;
use ggg_rs::readers::{postproc_files::{open_and_iter_postproc_file, PostprocType}, POSTPROC_FILL_VALUE};
use indexmap::IndexMap;
use indicatif::ProgressBar;
use itertools::Itertools;
use ndarray::Array1;
use tracing::instrument;

use crate::{
    dimensions::TIME_DIM_NAME,
    errors::{CliError, WriteError},
    interface::{ConcreteVarToBe, DataProvider, GroupWriter, SpectrumIndexer, StdDataGroup, VarToBe},
    progress::{setup_read_pb, setup_write_pb},
    qc::{load_qc_file_hashmap, QcRow}
};

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

    fn read_variables(postproc_file: &Path, ntimes: usize, spec_indexer: &SpectrumIndexer, qc_rows: HashMap<String, QcRow>, pb: &ProgressBar)
    -> error_stack::Result<Vec<ConcreteVarToBe<f32>>, WriteError> {

        let (header, row_iter) = open_and_iter_postproc_file(postproc_file)
            .change_context_lazy(|| WriteError::file_read_error(postproc_file))?;
        setup_read_pb(pb, header.nrec, ".vav.ada.aia");

        // Create arrays for all of the numeric variables
        let it = header.column_names.iter()
            .filter_map(|colname| {
                if colname != "spectrum" {
                    Some((colname.to_string(), Array1::from_elem((ntimes,), POSTPROC_FILL_VALUE as f32)))
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
            pb.inc(1);

            let line_num = header.nhead + irow + 1;
            let row = row.change_context_lazy(|| WriteError::detailed_read_error(
                postproc_file, format!("could not read line {line_num}")
            ))?;
            let itime = spec_indexer.get_index_for_spectrum(&row.auxiliary.spectrum)
                .ok_or_else(|| WriteError::detailed_read_error(
                    postproc_file, format!("spectrum {} on line {line_num} of the .aia file was not in the runlog!", row.auxiliary.spectrum)
                ))?;

            // Aux variables first
            for &varname in ggg_rs::readers::postproc_files::AuxData::postproc_fields_str() {
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
                    arr[itime] = (value * scale) as f32;
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
                
                if !ggg_rs::readers::postproc_files::is_postproc_fill(*value) {
                    arr[itime] = (value * scale) as f32;
                } else {
                    arr[itime] = *value as f32;
                }
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
    fn write_data_to_nc(&self, spec_indexer: &crate::interface::SpectrumIndexer, writer: &dyn GroupWriter, pb: ProgressBar) -> error_stack::Result<(), WriteError> {
        let qc_rows = load_qc_file_hashmap(&self.qc_path)?;
        let ntimes = writer.get_dim_length(TIME_DIM_NAME)
            .ok_or_else(|| WriteError::missing_dim_error(".aia", TIME_DIM_NAME))?;
        // TODO: compute flag variable
        let variables = Self::read_variables(&self.aia_path, ntimes, spec_indexer, qc_rows, &pb)?;
        let nvar = variables.len();
        let grouped_variables = split_ret_vars_to_groups(variables)?;
        setup_write_pb(&pb, nvar, ".vav.ada.aia");
        for (group, vars) in grouped_variables {
            let tmp = vars.iter().map(|v| v.as_ref()).collect_vec();
            writer.write_many_variables(&tmp, &group, Some(&pb))?;
        }
        Ok(())
    }
}

/// Provider for any of the post processing files created downstream of collate results,
/// _except_ the `.aia` file. The `.aia` must be use the [`AiaFile`] provider to include
/// the auxiliary variables and correctly apply the QC file scaling.
#[derive(Debug)]
pub(crate) struct PostprocFile {
    file_path: PathBuf,
    postproc_type: PostprocType
}

impl PostprocFile {
    pub(crate) fn new(file_path: PathBuf) -> Result<Self, CliError> {
        let postproc_type = PostprocType::from_path(&file_path)
            .ok_or_else(|| CliError::internal_error("Tried to construct a PostprocFile provider with an unrecognized post-processing file"))?;
        if let PostprocType::VavAdaAia = postproc_type {
            // Maybe we will want to use this for a .aia file in unusual cases, so this is just a warning.
            tracing::warn!("Using a generic PostprocFile provider for a .vav.ada.aia file - normally .vav.ada.aia files are provided with the AiaFile type");
        }

        Ok(Self { file_path, postproc_type })
    }

    fn read_variables(&self, ntimes: usize, spec_indexer: &SpectrumIndexer, pb: &ProgressBar)
    -> error_stack::Result<Vec<ConcreteVarToBe<f32>>, WriteError> {
        // Every variable will need the file basename and checksum for the source file, so we get those
        // now to save recomputing the checksum every time.
        let file_basename = self.file_path.file_name().expect("Couldn't get the basename of the .vav.ada.aia file");
        let file_checksum = ggg_rs::utils::file_sha256_hexdigest(&self.file_path)
            .change_context_lazy(|| WriteError::detailed_read_error(
                &self.file_path, "failed to compute the SHA256 checksum"
            ))?;
        
        let (header, row_iter) = open_and_iter_postproc_file(&self.file_path)
            .change_context_lazy(|| WriteError::file_read_error(&self.file_path))?;

        setup_read_pb(pb, header.nrec, &self.postproc_type);

        // Create arrays for all of the retrieved variables. We deliberately skip the auxiliary
        // variables; those should only be provided by the AiaFile provider.
        let aux_fields = ggg_rs::readers::postproc_files::AuxData::postproc_fields_str();
        let it = header.column_names.iter()
            .filter_map(|colname| {
                if !aux_fields.contains(&colname.as_str()) {
                    Some((colname.to_string(), Array1::from_elem((ntimes,), POSTPROC_FILL_VALUE as f32)))
                } else {
                    None
                }
            });
        let mut data_arrays = IndexMap::<_,_,RandomState>::from_iter(it);

        // For each row, find the correct index along the time dimension given the spectrum name.
        // Unlike the AiaFile provider, we don't apply any scaling - just keep the values as they are.
        for (irow, row) in row_iter.enumerate() {
            pb.inc(1);

            let line_num = header.nhead + irow + 1;
            let row = row.change_context_lazy(|| WriteError::detailed_read_error(
                &self.file_path, format!("could not read line {line_num}")
            ))?;
            let itime = spec_indexer.get_index_for_spectrum(&row.auxiliary.spectrum)
                .ok_or_else(|| WriteError::detailed_read_error(
                    &self.file_path, format!("spectrum {} on line {line_num} of the .aia file was not in the runlog!", row.auxiliary.spectrum)
                ))?;

            // We only do the retrieved variables.
            for (varname, value) in row.retrieved.iter() {
                let arr = data_arrays.get_mut(varname)
                    .ok_or_else(|| WriteError::custom(format!(
                        "in the .aia file, {varname} is in line {line_num} but was not in the column names"
                    )))?;
                
                arr[itime] = *value as f32;
            }
        }

        // As in AiaFile, now we go through all the data arrays and construct the variables with metadata for them.
        // Unlike AiaFile, we define the attributes ourselves rather than relying on the qc.dat file.
        let mut variables = vec![];
        for (input_varname, array) in data_arrays.into_iter() {
            let varname = self.ret_var_name(&input_varname);
            let long_name = varname.replace("_", " ");
            let mut this_var = ConcreteVarToBe::new_with_checksum(
                varname,
                vec![TIME_DIM_NAME],
                array.into_dyn(),
                long_name,
                self.ret_var_units().to_string(),
                file_basename.to_string_lossy().to_string(),
                file_checksum.clone()
            );
            this_var.add_attribute("description", self.ret_var_descr(&input_varname));
            variables.push(this_var);
        }

        Ok(variables)
    }

    fn ret_var_name(&self, gas_or_window: &str) -> String {
        match &self.postproc_type {
            PostprocType::Vsw => format!("vsw_{gas_or_window}"),
            PostprocType::Tsw => format!("tsw_{gas_or_window}"),
            PostprocType::Vav => format!("column_{gas_or_window}"),
            PostprocType::Tav => format!("vsf_{gas_or_window}"),
            PostprocType::VswAda => format!("vsw_ada_{gas_or_window}"),
            PostprocType::VavAda => format!("ada_{gas_or_window}"),
            PostprocType::VavAdaAia => format!("aia_{gas_or_window}"),
            PostprocType::Other(ext) => {
                let ext = ext.replace(".", "_");
                format!("{ext}_{gas_or_window}")
            },
        }
    }

    fn ret_var_units(&self) -> &'static str {
        match &self.postproc_type {
            PostprocType::Vsw | PostprocType::Vav => "molecules.cm^-2",
            PostprocType::Tsw | PostprocType::Tav => "1",
            PostprocType::VswAda | PostprocType::VavAda | PostprocType::VavAdaAia => "1",
            PostprocType::Other(_) => "?",
        }
    }

    fn ret_var_descr(&self, gas_or_window: &str) -> String {
        let (gas, window) = if let Some((g, w)) = ggg_rs::utils::split_gas_and_window(gas_or_window) {
            let win = format!("the window centered at {w} cm-1");
            (g, win)
        } else {
            // Either this is after average_results, or we got a weird column name, so
            // just keep the gas as is and make window be this to fit grammatically in
            // where the specific string used if we know the window goes.
            // We still want to split the input on the first underscore to remove "_error"
            // from the gas name.
            let g = gas_or_window.split("_").next().unwrap_or(gas_or_window);
            (g, "its window".to_string())
        };

        if gas_or_window.contains("error") {
            match &self.postproc_type {
                PostprocType::Vsw => format!("one-sigma precision for {gas} column density retrieved in {window}"),
                PostprocType::Tsw => format!("one-sigma precision for the {gas} VMR scale factor retrieved in {window}"),
                PostprocType::Vav => format!("one-sigma precision for {gas} column density across all windows"),
                PostprocType::Tav => format!("one-sigma precision for the {gas} VMR scale factor across all windows"),
                PostprocType::VswAda => format!("one-sigma precision for the {gas} column-average mole fraction retrieved in {window}, with airmass correction applied but without the in situ scaling applied"),
                PostprocType::VavAda => format!("one-sigma precision for the {gas} column-average mole fraction across all windows, with airmass correction applied, but without the in situ scaling applied"),
                // NB: this usually should not get used, normally .aia files should use the AiaFile provider
                PostprocType::VavAdaAia => format!("one-sigma precision for the {gas} column-average mole fraction across all windows with both the airmass correction and in situ scaling applied"),
                PostprocType::Other(ext) => format!("one-sigma precision for {gas_or_window} from the {ext} file"),
            }
        } else {
            match &self.postproc_type {
                PostprocType::Vsw => format!("{gas} total column density from {window}"),
                PostprocType::Tsw => format!("VMR scale factor for {gas} retrieved in {window}"),
                PostprocType::Vav => format!("{gas} total column density averaged across all windows"),
                PostprocType::Tav => format!("VMR scale factor for {gas} averaged across all windows"),
                PostprocType::VswAda => format!("{gas} column-average mole fraction from {window} with the airmass correction applied but without the in situ scaling applied"),
                PostprocType::VavAda => format!("{gas} column-average mole fraction averaged across all windows with the airmass correction applied but without the in situ scaling applied"),
                // NB: this usually should not get used, normally .aia files should use the AiaFile provider
                PostprocType::VavAdaAia => format!("{gas} column-average mole fraction averaged across all windows with the airmass correction and in situ scaling applied"),
                PostprocType::Other(ext) => format!("{gas_or_window} from the {ext} file"),
            }
        }
    }
}


impl Display for PostprocFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.postproc_type {
            PostprocType::Vsw => write!(f, ".vsw file"),
            PostprocType::Tsw => write!(f, ".tsw file"),
            PostprocType::Vav => write!(f, ".vav file"),
            PostprocType::Tav => write!(f, ".tav file"),
            PostprocType::VswAda => write!(f, ".vsw.ada file"),
            PostprocType::VavAda => write!(f, ".vav.ada file"),
            PostprocType::VavAdaAia => write!(f, ".vav.ada.aia file"),
            PostprocType::Other(ext) => write!(f, "{ext} file"),
        }
    }
}

impl DataProvider for PostprocFile {
    fn dimension_lengths(&self) -> std::borrow::Cow<[(&'static str, usize)]> {
        Cow::Borrowed(&[])
    }

    fn dimensions_required(&self) -> std::borrow::Cow<[&'static str]> {
        Cow::Owned(vec![TIME_DIM_NAME])
    }

    #[instrument(name = "postproc_file_writer", skip(spec_indexer, writer))]
    fn write_data_to_nc(&self, spec_indexer: &crate::interface::SpectrumIndexer, writer: &dyn GroupWriter, pb: ProgressBar) -> error_stack::Result<(), WriteError> {
        let ntimes = writer.get_dim_length(TIME_DIM_NAME)
            .ok_or_else(|| WriteError::missing_dim_error(".aia", TIME_DIM_NAME))?;
        let variables = self.read_variables(ntimes, spec_indexer, &pb)?;
        let nvar = variables.len();
        let grouped_variables = split_ret_vars_to_groups(variables)?;

        setup_write_pb(&pb, nvar, &self.postproc_type);
        for (group, vars) in grouped_variables {
            let tmp = vars.iter().map(|v| v.as_ref()).collect_vec();
            writer.write_many_variables(&tmp, &group, Some(&pb))?;
        }
        Ok(())
    }
}


/// Given a list of variables, split them into the standard TCCON groups.
fn split_ret_vars_to_groups(variables: Vec<ConcreteVarToBe<f32>>) 
    -> error_stack::Result<
        HashMap<StdDataGroup, Vec<Box<dyn VarToBe>>>,
        WriteError
    > {
    let mut grouped_vars = HashMap::new();

    // TODO: parse the group definition file in $GGGPATH/tccon instead of hardcoding these groups.
    // until that's done, this won't properly divide things into groups unless they duplicate InGaAs
    // names.
    let aux_vars = ggg_rs::readers::postproc_files::AuxData::postproc_fields_str();
    for var in variables {
        let group = if aux_vars.contains(&var.name()) {
            // Auxiliary variables (e.g. solzen, lat, lon, etc.) should all go in the regular
            // group.
            StdDataGroup::InGaAs
        } else if var.name().starts_with("xv") || var.name().starts_with("v") {
            StdDataGroup::Si
        } else if var.name().starts_with("xm") || var.name().starts_with("m") {
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