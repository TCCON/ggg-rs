//! Data calculators for the flag-related variables.
use std::{collections::HashMap, path::Path};

use error_stack::ResultExt;
use indicatif::ProgressBar;
use itertools::Itertools;
use ndarray::{Array1, Ix1};

use crate::{dimensions::TIME_DIM_NAME, errors::{CliError, WriteError}, interface::{ConcreteVarToBe, DataCalculator, GroupAccessor, StdDataGroup, StrVarToBe}, progress, qc::{load_qc_file, QcRow}};

/// Data calculator for the flag variables (e.g. flag and flagged_var_name)
pub(crate) struct FlagCalculator {
    qc_limits: Vec<QcRow>
}

impl FlagCalculator {
    /// Construct a new `FlagCalculator`. Returns an error if reading the qc.dat file
    /// fails for any reason.
    pub(crate) fn new(qc_file_path: &Path) -> error_stack::Result<Self, CliError> {
        let qc_limits = load_qc_file(qc_file_path)
            .change_context_lazy(|| CliError::input_error("error getting the QC file"))?;
        Ok(Self { qc_limits })
    }

    /// Returns a hash map of the data variables from which we will determine the flags.
    /// 
    /// Only variables listed in the qc.dat file _and_ which are configured for output are returned.
    /// The intention is that if a variable is not being output then it should not be flagged on.
    fn load_vars_to_flag_on(&self, accessor: &dyn GroupAccessor) -> error_stack::Result<HashMap<String, Array1<f32>>, WriteError> {
        let mut vars = HashMap::new();
        for qc_row in self.qc_limits.iter() {
            if !qc_row.do_output() {
                // If we are supposed to flag on unwritten variables, all this flagging code
                // would need to move into the .aia writer.
                continue;
            }

            // TODO: get the correct data group for the variable. What we'll probably need to do is have an object that figures
            // that out and pass it around.
            let varname = qc_row.variable.clone();
            let data = accessor.read_f32_variable(&varname, &StdDataGroup::InGaAs)
                .map_err(|e| WriteError::NcReadError(e))?;
            let arr = data.data.into_dimensionality::<Ix1>()
                .map_err(|e| WriteError::custom(format!(
                    "expected variable '{varname}' to be a 1D array, but was not ({e})"
                )))?;
            vars.insert(varname, arr);
        }
        Ok(vars)
    }

    /// Construct the arrays of flags and flagged variable names along with a map of the
    /// number of times each flag appeared, which can be used to print the summary table.
    /// The first array will contain the 1-based index of the row in the qc.dat file for 
    /// the variable most out of range. The second array will include that variable's name.
    /// The values will be 0 and an empty, respectively, when no variable is out of range.
    fn make_flag_arrays(&self, ntime: usize, flag_vars: &HashMap<String, Array1<f32>>, pb: ProgressBar) -> (Array1<u32>, Array1<&str>, HashMap<&str, FlagCount>) {
        let mut flags = Array1::from_elem(ntime, 0);
        let mut flag_var_names = Array1::from_elem(ntime, "");
        let mut flag_var_counts = HashMap::new();

        progress::setup_generic_pb(&pb, ntime, "Generating flags");
        for i in 0..ntime {
            pb.inc(1);
            let (flag_index, flag_name) = self.get_flag_for_spectrum(flag_vars, i);
            flags[i] = flag_index;
            flag_var_names[i] = flag_name;
            if flag_index != 0 {
                if !flag_var_counts.contains_key(flag_name) {
                    flag_var_counts.insert(flag_name, FlagCount::new(flag_index, flag_name));
                }
                let tmp = flag_var_counts.get_mut(flag_name).unwrap();
                tmp.incr();
            }
        }

        (flags, flag_var_names, flag_var_counts)
    }

    /// Determine which variable is most out of range (if any) for a given spectrum.
    /// 
    /// Returns 0 and an empty string if none of `flag_vars` is outside its allowed
    /// range from the qc.dat file. Otherwise, returns the 1-based index and name of
    /// the variable most outside its allowed range (normalized to the width of said range).
    /// 
    /// # Panics
    /// Will panic if any of the variables needed by the qc.dat file (i.e. file listed and does
    /// not have output set to 0) is not in `flag_vars`.
    fn get_flag_for_spectrum<'a>(&'a self, flag_vars: &HashMap<String, Array1<f32>>, spec_index: usize) -> (u32, &'a str) {
        let mut i_max_var = 0;
        let mut max_var_name = "";
        let mut max_deviation = 0.0;
        for (i_var, qc_row) in self.qc_limits.iter().enumerate() {
            if !qc_row.do_output() {
                continue;
            }

            let varname = &qc_row.variable;
            let value = flag_vars.get(varname)
                .expect("All variables required for flagged should have been loaded")[spec_index] as f64;

            if ggg_rs::output_files::is_postproc_fill(value as f64) {
                // don't flag on fill values
                continue;
            }

            // If value == vmin, then (v - vmin)/(vmax - vmin) = 0, so deviation = 0.5.
            // If value == vmax, then (v - vmin)/(vmax - vmin) = 1, so deviation also = 0.5.
            // This is just a mathematically more efficient way of checking if the value is below vmin
            // or above vmax.
            // Note that this assumes the variable was already scaled by the scale amount in the QC file.
            let deviation = ((value - qc_row.vmin) / (qc_row.vmax - qc_row.vmin) - 0.5).abs();
            if deviation > max_deviation {
                i_max_var = i_var;
                max_var_name = varname;
                max_deviation = deviation;
            }
        }

        if max_deviation > 0.5 {
            (i_max_var as u32 + 1, max_var_name)
        } else {
            (0, "")
        }
    }

    /// Print a table summarizing the flags using `tracing::info!`.
    fn print_flag_table(flag_var_counts: HashMap<&str, FlagCount>, ntime: usize) {
        let mut flag_var_counts = flag_var_counts.into_values().collect_vec();
        flag_var_counts.sort_by_key(|count| count.flag_index);
        
        // Construct the table as a string so that we can ensure it is written as a unit
        let n = ntime as f64;
        let mut table = "  #  Parameter              N_flag      %\n".to_string();
        let mut total_num_flagged = 0;
        for flags in flag_var_counts {
            let percent = (flags.flag_count as f64) / n * 100.0;
            let line = format!("{:>3}  {:<20} {:>6}   {:>8.3}\n", flags.flag_index, flags.flag_name, flags.flag_count, percent);
            table.push_str(&line);
            total_num_flagged += flags.flag_count;
        }
        let total_percent = (total_num_flagged as f64) / n * 100.0;
        let line = format!("     {:<20} {:>6}   {:>8.3}", "TOTAL", total_num_flagged, total_percent);
        table.push_str(&line);
        tracing::info!("Report on number of spectra flagged followes:\n{table}");
    }
}

impl DataCalculator for FlagCalculator {
    fn write_data_to_nc(&self, _spec_indexer: &crate::interface::SpectrumIndexer, accessor: &dyn GroupAccessor, pb: ProgressBar) -> error_stack::Result<(), WriteError> {
        let flag_vars = self.load_vars_to_flag_on(accessor)?;

        let ntime = accessor.read_dim_length(TIME_DIM_NAME)
            .map_err(|e| WriteError::NcReadError(e))?;

        let (flags, flag_var_names, flag_var_counts) = self.make_flag_arrays(ntime, &flag_vars, pb);

        // TODO: add manual flags

        let flag_var = ConcreteVarToBe::new_calculated(
            "flag",
            vec![TIME_DIM_NAME],
            flags.into_dyn(),
            "flag",
            "",
            std::any::type_name::<Self>()
        );

        let flag_name_var = StrVarToBe::new_calculated(
            "flagged_var_name",
            TIME_DIM_NAME,
            flag_var_names,
            "flagged variable name",
            "",
            std::any::type_name::<Self>()
        );

        accessor.write_variable(&flag_var, &StdDataGroup::InGaAs)?;
        accessor.write_variable(&flag_name_var, &StdDataGroup::InGaAs)?;

        Self::print_flag_table(flag_var_counts, ntime);

        Ok(())
    }
}

/// A utility structure used to count the number of times a particular
/// flag appears in the file.
struct FlagCount<'a> {
    flag_index: u32,
    flag_name: &'a str,
    flag_count: u64,
}

impl<'a> FlagCount<'a> {
    fn new(flag_index: u32, flag_name: &'a str) -> Self {
        Self { flag_index, flag_name, flag_count: 0 }
    }

    fn incr(&mut self) {
        self.flag_count += 1;
    }
}