//!
use std::io::Write;
use std::path::Path;

use approx::ulps_ne;
use chrono::{NaiveDate, NaiveDateTime};
use error_stack::ResultExt;
use ggg_rs::nc_utils;
use ndarray::{ArrayD, ArrayViewD, Zip};
use netcdf::{Extents, NcTypeDescriptor};
use serde::{Deserialize, Serialize};

use crate::CliError;

/// Main function that applies the filters to replace fill values,
/// as defined in `config`, to the netCDF dataset `ds`. That dataset
/// is modified in-place.
pub(crate) fn apply_filters(
    ds: &mut netcdf::FileMut,
    config: &FilterConfig,
) -> error_stack::Result<(), CliError> {
    let times = ds
        .variable("time")
        .ok_or_else(|| CliError::missing_variable("time"))?
        .get(Extents::All)
        .change_context_lazy(|| CliError::context(format!("Error getting 'time' values")))?
        .mapv(|ts| nc_utils::convert_nc_timestamp(ts).naive_utc());

    let nfilter = config.replace.len();
    for (ifilter, filter) in config.replace.iter().enumerate() {
        log::info!("Applying filter {} of {nfilter}", ifilter + 1);
        match filter.matches {
            Matches::Approx { approx } => {
                filter_variable_approx(ds, &filter.varname, times.view(), approx, filter)?;
            }
            Matches::Between { gt, lt } => {
                filter_variable_between(ds, &filter.varname, times.view(), gt, lt, filter)?;
            }
            Matches::Equal { eq } => {
                filter_variable_equal(ds, &filter.varname, times.view(), eq, filter)?;
            }
        }
    }
    Ok(())
}

/// Overall configuration structure used to (de)serialize the TOML configuration.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct FilterConfig {
    replace: Vec<FillReplacement>,
}

impl FilterConfig {
    /// Create an example configuration file
    pub(crate) fn write_template_example(example_file: &Path) -> error_stack::Result<(), CliError> {
        let comments = [
            "This is an example TOML file to specify what value to change to netCDF fills.",
            "Each [[replace]] entry defines one rule for what to replace. A rule specifies",
            "what variable to replace values in, how to determine if a value should be replaced",
            "and, optionally, what time range it affects.",
            "",
            "Each entry will define one of several ways to select values to replace with fills.",
            "The fields present will determine which selection method is used:",
            "  - If the entry has the 'approx' field, then values within floating-point error of",
            "    that value are replaced with fills. The variable in the netCDF file must be a float.",
            "  - If the entry has the 'gt' and 'lt' fields, then values in that range (i.e., any",
            "    value x that satisfieds gt <= x <= lt) will be replaced. The netCDF variable must",
            "    be a float.",
            "  - If the entry has the 'eq' field, then values exactly equal to that value are replace.",
            "    The netCDF variable must be an integer",
            "You cannot mix fields, so having an entry with 'approx' and 'eq' will give an error.",
            "",
            r#"In the examples below, the section with varname = "tins" will look for any value in"#,
            "the tins variable that is within floating point error of 60.0 and replace that with the",
            "correct fill value for that netCDF variable. This will affect the whole file, since a",
            "time range is not specified.",
            "",
            r#"The second example with varname = "pout" shows an example of filtering by time. While"#,
            "this example specifies both ends of a time range, you can also specify just time_greater_than",
            "or time_less_than to limit to all times after or before, respectively, that time.",
            "",
            "The third example shows the use of 'gt' and 'lt'. Any value between those values",
            "will be replaced with fill values.",
        ];
        let template = Self::template_example();
        let mut f = std::fs::File::create(example_file).change_context_lazy(|| {
            CliError::context(
                "Error occurred while opening the output file for the filter template",
            )
        })?;

        for line in comments {
            writeln!(f, "# {line}").change_context_lazy(|| {
                CliError::context(format!(
                    "Error writing to template file: {}",
                    example_file.display()
                ))
            })?;
        }
        let s = toml::to_string_pretty(&template)
            .expect("The example template should be able to be serialized (this is a bug)");
        write!(f, "{s}").change_context_lazy(|| {
            CliError::context(format!(
                "Error writing to template file: {}",
                example_file.display()
            ))
        })?;

        Ok(())
    }

    /// Create an example configuration
    pub(crate) fn template_example() -> Self {
        let mut variables = vec![];
        let tins = FillReplacement {
            varname: "tins".to_string(),
            matches: Matches::Approx { approx: 60.0 },
            time_greater_than: None,
            time_less_than: None,
        };
        variables.push(tins);

        let pout = FillReplacement {
            varname: "pout".to_string(),
            matches: Matches::Approx { approx: -99.0 },
            time_greater_than: Some(
                NaiveDate::from_ymd_opt(2010, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            ),
            time_less_than: Some(
                NaiveDate::from_ymd_opt(2015, 6, 12)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            ),
        };
        variables.push(pout);

        let fvsi = FillReplacement {
            varname: "fvsi".to_string(),
            matches: Matches::Between { gt: -1.5, lt: -0.5 },
            time_greater_than: None,
            time_less_than: None,
        };
        variables.push(fvsi);

        Self { replace: variables }
    }
}

/// A single definition for data to be replaced with fill values.
///
/// Note, since the `matches` field is a flattened, untagged enum,
/// the fields from the enum must be directly in this table in
/// the TOML file.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct FillReplacement {
    varname: String,
    #[serde(flatten)]
    matches: Matches,
    time_greater_than: Option<NaiveDateTime>,
    time_less_than: Option<NaiveDateTime>,
}

/// Defines how to determine whether a value is a fill value
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum Matches {
    /// See if a floating point value is approximately equal to some known fill value.
    Approx { approx: f32 },

    /// See if a floating point value is in a range of values.
    Between { gt: f32, lt: f32 },

    /// See if an integer is equal to a particular value.
    Equal { eq: i64 },
}

/// Helper function that returns `true` if time `t` is outside the
/// time bounds specified and should be skipped.
fn outside_time_bounds(
    t: &NaiveDateTime,
    time_gt: Option<NaiveDateTime>,
    time_lt: Option<NaiveDateTime>,
) -> bool {
    if time_gt.is_some_and(|first| t < &first) {
        return true;
    }

    if time_lt.is_some_and(|last| t > &last) {
        return true;
    }

    false
}

/// Helper function that applies the "approximate" filtering to a variable.
///
/// This, [`filter_variable_between`], and [`filter_variable_equal`]
/// were written as separate functions because of the different combinations
/// of memory types and boolean checks needed.
fn filter_variable_approx(
    ds: &mut netcdf::FileMut,
    varname: &str,
    times: ArrayViewD<NaiveDateTime>,
    approx: f32,
    filter: &FillReplacement,
) -> error_stack::Result<(), CliError> {
    let opt_arr = get_var_values_opt::<f32>(ds, varname)
        .change_context_lazy(|| CliError::context("Error reading variable as float"))?;
    let mut values = if let Some(arr) = opt_arr {
        arr
    } else {
        log::warn!("Variable {varname} not found in file, skipping");
        return Ok(());
    };

    let nc_fill = get_var_fill::<f32>(ds, varname)?;
    let n_changed = filter_approx(
        &mut values,
        times,
        nc_fill,
        approx,
        filter.time_greater_than,
        filter.time_less_than,
    );
    if n_changed > 0 {
        put_var_values(ds, varname, values.view())?;
        log::info!("Replaced {n_changed} values in {varname} approximately equal to {approx}");
    } else {
        log::info!("No values in {varname} approximately equal to {approx}, variable not modified");
    }

    Ok(())
}

/// Helper function that applies the "approximate" filtering to an in-memory array.
fn filter_approx(
    values: &mut ArrayD<f32>,
    times: ArrayViewD<NaiveDateTime>,
    new_fill: f32,
    approx: f32,
    time_gt: Option<NaiveDateTime>,
    time_lt: Option<NaiveDateTime>,
) -> usize {
    let mut n_changed = 0;
    Zip::from(values).and(&times).for_each(|v, t| {
        if outside_time_bounds(t, time_gt, time_lt) {
            return;
        }

        if ulps_ne!(*v, approx) {
            return;
        }

        *v = new_fill;
        n_changed += 1;
    });
    n_changed
}

/// Helper function that applies the "between" filtering to a variable.
fn filter_variable_between(
    ds: &mut netcdf::FileMut,
    varname: &str,
    times: ArrayViewD<NaiveDateTime>,
    gt: f32,
    lt: f32,
    filter: &FillReplacement,
) -> error_stack::Result<(), CliError> {
    let opt_arr = get_var_values_opt::<f32>(ds, varname)
        .change_context_lazy(|| CliError::context("Error reading variable as float"))?;
    let mut values = if let Some(arr) = opt_arr {
        arr
    } else {
        log::warn!("Variable {varname} not found in file, skipping");
        return Ok(());
    };
    let nc_fill = get_var_fill::<f32>(ds, varname)?;
    let n_changed = filter_between(
        &mut values,
        times.view(),
        nc_fill,
        gt,
        lt,
        filter.time_greater_than,
        filter.time_less_than,
    );
    if n_changed > 0 {
        put_var_values(ds, varname, values.view())?;
        log::info!("Replaced {n_changed} values in {varname} between {gt} and {lt}");
    } else {
        log::info!("No values in {varname} between {gt} and {lt}, variable not modified");
    }

    Ok(())
}

/// Helper function that applies the "between" filtering to an in-memory array.
fn filter_between(
    values: &mut ArrayD<f32>,
    times: ArrayViewD<NaiveDateTime>,
    new_fill: f32,
    gt: f32,
    lt: f32,
    time_gt: Option<NaiveDateTime>,
    time_lt: Option<NaiveDateTime>,
) -> usize {
    let mut n_changed = 0;
    Zip::from(values).and(&times).for_each(|v, t| {
        if outside_time_bounds(t, time_gt, time_lt) {
            return;
        }

        if *v < gt {
            return;
        }

        if *v > lt {
            return;
        }

        *v = new_fill;
        n_changed += 1;
    });
    n_changed
}

/// Helper function that applies the "equal" filtering to a variable.
fn filter_variable_equal(
    ds: &mut netcdf::FileMut,
    varname: &str,
    times: ArrayViewD<NaiveDateTime>,
    eq: i64,
    filter: &FillReplacement,
) -> error_stack::Result<(), CliError> {
    let opt_arr = get_var_values_opt::<i64>(ds, varname)
        .change_context_lazy(|| CliError::context("Error reading variable as float"))?;
    let mut values = if let Some(arr) = opt_arr {
        arr
    } else {
        log::warn!("Variable {varname} not found in file, skipping");
        return Ok(());
    };
    let nc_fill = get_var_fill::<i64>(ds, varname)?;
    let n_changed = filter_equal(
        &mut values,
        times.view(),
        nc_fill,
        eq,
        filter.time_greater_than,
        filter.time_less_than,
    );
    if n_changed > 0 {
        put_var_values(ds, varname, values.view())?;
        log::info!("Replaced {n_changed} values in {varname} equal to {eq}");
    } else {
        log::info!("No values in {varname} equal to {eq}, variable not modified");
    }
    Ok(())
}

/// Helper function that applies the "equal" filtering to an in-memory array.
fn filter_equal(
    values: &mut ArrayD<i64>,
    times: ArrayViewD<NaiveDateTime>,
    new_fill: i64,
    eq: i64,
    time_gt: Option<NaiveDateTime>,
    time_lt: Option<NaiveDateTime>,
) -> usize {
    let mut n_changed = 0;
    Zip::from(values).and(&times).for_each(|v, t| {
        if outside_time_bounds(t, time_gt, time_lt) {
            return;
        }

        if *v != eq {
            return;
        }

        *v = new_fill;
        n_changed += 1;
    });
    n_changed
}

/// Helper function that returns the values array of a netCDF variable.
/// If the variable doesn't exist in the given dataset, it returns `None`.
fn get_var_values_opt<T: NcTypeDescriptor + Copy>(
    ds: &netcdf::File,
    varname: &str,
) -> error_stack::Result<Option<ndarray::ArrayD<T>>, CliError> {
    if let Some(var) = ds.variable(&varname) {
        var.get(Extents::All)
            .map(|v| Some(v))
            .change_context_lazy(|| {
                CliError::context(format!("Error getting values from variable: {varname}"))
            })
    } else {
        Ok(None)
    }
}

/// Helper function that writes an array of values to a netCDF variable.
fn put_var_values<T: NcTypeDescriptor + Copy>(
    ds: &mut netcdf::FileMut,
    varname: &str,
    values: ArrayViewD<T>,
) -> error_stack::Result<(), CliError> {
    let mut var = ds
        .variable_mut(varname)
        .ok_or_else(|| CliError::missing_variable(varname))?;

    var.put(values, Extents::All).change_context_lazy(|| {
        CliError::context(format!("Error writing new values to variable: {varname}"))
    })
}

/// Helper function that gets the defined fill value for a given netCDF variable.
fn get_var_fill<T: NcTypeDescriptor + Copy>(
    ds: &netcdf::File,
    varname: &str,
) -> error_stack::Result<T, CliError> {
    ds.variable(&varname)
        .ok_or_else(|| CliError::missing_variable(varname))?
        .fill_value::<T>()
        .transpose()
        .ok_or_else(|| CliError::no_fill_def(varname))?
        .change_context_lazy(|| {
            CliError::context(format!("Error reading fill value from variable: {varname}"))
        })
}
