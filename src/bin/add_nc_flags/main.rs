use std::{
    collections::HashMap,
    fmt::Display,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::ExitCode,
    str::FromStr,
};

use clap::{Args, Parser, Subcommand};
use error_stack::ResultExt;
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests;

fn main() -> ExitCode {
    if let Err(e) = main_inner() {
        eprintln!("ERROR: {e:?}");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

fn main_inner() -> error_stack::Result<(), CliError> {
    let args = Cli::parse();
    match args.command {
        Commands::Quick(quick_args) => {
            let no_filters_defined = quick_args.filtering.filter.no_filters();
            let output = quick_args.output.clone();
            let nc_file = quick_args.nc_file.clone();
            let nchanged = driver(output, quick_args.into(), &nc_file)?;
            if nchanged == 0 && no_filters_defined {
                println!("Note: nothing flagged because you gave neither the --less-than nor --greater-than argument.");
            }
        }
        Commands::Toml(toml_args) => {
            let filter_set = toml_args.load_filters()?;
            let no_filters_defined = filter_set.no_filters();
            let nchanged = driver(toml_args.output, filter_set, &toml_args.nc_file)?;
            if nchanged == 0 && no_filters_defined {
                println!(
                    "Note: nothing flagged because no filters were defined in the given JSON file"
                );
            }
        }
        Commands::TomlTemplate(template_args) => {
            FilterSet::write_template(&template_args.template_file)?;
        }
    }

    Ok(())
}

fn driver(
    output: OutputCli,
    filters: FilterSet,
    nc_file: &Path,
) -> error_stack::Result<u64, CliError> {
    // One check - if we are outputting to a new path, make sure that isn't a directory but
    // that its parent directory exists. This way we can give a clearer error message.
    if let Some(out_file) = &output.output {
        if out_file.is_dir() {
            return Err(CliError::UserError(
                "Path given to --output must not be a directory".to_string(),
            )
            .into());
        }

        // The second check is necessary because e.g. "test.nc" has a parent of "" which doesn't exist,
        // but means "." which better exist.
        if let Some(parent) = out_file.parent() {
            if !parent.exists() && !parent.as_os_str().is_empty() {
                return Err(CliError::UserError(format!(
                    "The directory part of --output ({}) must exist, i.e. for --output /home/me/test.nc, /home/me must exist.",
                    out_file.parent().map(|p| p.to_str()).flatten().unwrap_or("")
                )).into());
            }
        } else {
            // We really shouldn't get here (the is_dir check should catch these cases), but just in case
            return Err(CliError::UserError(format!("{} is not a valid path for --output (cannot be your root directory or a drive prefix)", out_file.display())).into());
        }
    }

    let data = load_flags_and_data(nc_file, &filters.filter_vars())?;
    let (new_flags, nchanged) = update_flags(data, &filters, &filters.flags)?;

    if nchanged == 0 {
        if output.in_place {
            println!(
                "No flags required update, file {} unchanged.",
                nc_file.display()
            );
        } else if output.always_copy {
            // Ok to unwrap args.output.output - Cli::parse ensures in_place or output is given
            let out_file = output.output.as_deref().unwrap();
            std::fs::copy(nc_file, out_file)
                .change_context(CliError::IoError)
                .attach_printable("Could not copy NC_FILE to the path given by --output")?;
            println!(
                "No flags required update, copied {} to {}",
                nc_file.display(),
                out_file.display()
            );
        } else {
            // Ok to unwrap args.output.output - Cli::parse ensures in_place or output is given
            println!(
                "No flags required update, did not produce/overwrite output file {}",
                output.output.unwrap().display()
            );
        }
        return Ok(nchanged);
    }

    let output_file = if let Some(f) = &output.output {
        std::fs::copy(nc_file, &f)
            .change_context(CliError::IoError)
            .attach_printable("Could not copy NC_FILE to the path given by --output")?;
        f
    } else if output.in_place {
        nc_file
    } else {
        panic!("One of --in-place or --output must be given");
    };

    let mut ds = netcdf::append(&output_file)
        .attach_printable(
            "Could not edit the original (if --in-place) or output (if --output) netCDF file",
        )
        .change_context(CliError::NcError)?;
    let mut flags = ds.variable_mut("flag")
        .ok_or_else(|| CliError::MissingReqVariable("flag"))
        .attach_printable("This occurred while trying to get the flag variable in either the new output file or (if --in-place given) the original file")?; // this really shouldn't happen, since we read that variable in already
    flags.put_values(new_flags.as_slice().unwrap(), netcdf::Extents::All)
        .change_context(CliError::NcError)
        .attach_printable("This occur write new flag values to either the new output file or (if --in-place given) the original file")?;

    println!("{nchanged} flag values updated");
    Ok(nchanged)
}

#[derive(Debug, thiserror::Error)]
enum CliError {
    #[error("Missing required variable '{0}' - is this a TCCON private file?")]
    MissingReqVariable(&'static str),
    #[error("The filter variable '{0}' is not present in this file")]
    MissingFilterVariable(String),
    #[error("The variable {0} was not {1}D")]
    WrongDimension(String, u8),
    #[error("The filter, timestamp, and flag variables are not all the same length")]
    MismatchedLengths,
    #[error("The flag at index {0} (0-based) already has the flag set you are trying to replace")]
    FlagReplaceError(usize),
    #[error("Unexpected netCDF error")]
    NcError,
    #[error("Unexpected I/O error")]
    IoError,
    #[error("{0}")]
    UserError(String),
}

fn load_flags_and_data<S: AsRef<str> + ToString>(
    nc_file: &Path,
    filter_varnames: &[S],
) -> error_stack::Result<TcconData, CliError> {
    let ds = netcdf::open(nc_file).change_context(CliError::NcError)?;

    let timestamps = ds
        .variable("time")
        .ok_or_else(|| CliError::MissingReqVariable("time"))?
        .get::<f64, _>(netcdf::Extents::All)
        .change_context(CliError::NcError)?
        .into_dimensionality::<ndarray::Ix1>()
        .change_context_lazy(|| CliError::WrongDimension("time".to_string(), 1))?;

    let flags = ds
        .variable("flag")
        .ok_or_else(|| CliError::MissingReqVariable("flag"))?
        .get::<i16, _>(netcdf::Extents::All)
        .change_context(CliError::NcError)?
        .into_dimensionality::<ndarray::Ix1>()
        .change_context_lazy(|| CliError::WrongDimension("flag".to_string(), 1))?;

    let mut filter_vars = HashMap::new();
    for varname in filter_varnames {
        let data = ds.variable(varname.as_ref())
            .ok_or_else(|| CliError::MissingFilterVariable(varname.to_string()))?
            .get::<f32, _>(netcdf::Extents::All)
            .change_context(CliError::NcError)
            .attach_printable("This error may be caused by trying to filter on a variable that is not of type 'float'")?
            .into_dimensionality::<ndarray::Ix1>()
            .change_context_lazy(|| CliError::WrongDimension(varname.to_string(), 1))?;
        filter_vars.insert(varname.to_string(), data);
    }

    Ok(TcconData {
        filter_vars,
        timestamps,
        flags,
    })
}

#[derive(Debug)]
struct TcconData {
    filter_vars: HashMap<String, ndarray::Array1<f32>>,
    timestamps: ndarray::Array1<f64>,
    flags: ndarray::Array1<i16>,
}

impl TcconData {
    fn into_parts(
        self,
    ) -> (
        ndarray::Array1<f64>,
        ndarray::Array1<i16>,
        HashMap<String, ndarray::Array1<f32>>,
    ) {
        (self.timestamps, self.flags, self.filter_vars)
    }
}

fn update_flags(
    data: TcconData,
    filtering: &FilterSet,
    flagging: &Flags,
) -> error_stack::Result<(ndarray::Array1<i16>, u64), CliError> {
    let timestamp_check = GreaterLess {
        greater_than: filtering
            .timespan
            .time_greater_than
            .map(|dt| dt.and_utc().timestamp() as f64),
        less_than: filtering
            .timespan
            .time_less_than
            .map(|dt| dt.and_utc().timestamp() as f64),
        combination: filtering.timespan.time_mode,
    };

    for data_arr in data.filter_vars.values() {
        if data_arr.len() != data.timestamps.len() || data_arr.len() != data.flags.len() {
            return Err(CliError::MismatchedLengths.into());
        }
    }

    let mut nchanged = 0;
    let (data_timestamps, mut data_flags, filter_data) = data.into_parts();

    for (i, f) in data_flags.iter_mut().enumerate() {
        // We checked the lengths before, so we are okay to unwrap here.
        let t = data_timestamps.get(i).unwrap();

        if !timestamp_check.no_limits() && !timestamp_check.do_flag(t) {
            // println!("Skipped for time");
            continue;
        }

        if filtering.do_flag(&filter_data, i) {
            let new = flagging.flag_type.update_flag(*f, flagging.flag, flagging.existing_flags)
                .change_context_lazy(|| CliError::FlagReplaceError(i))
                .attach_printable("Suggestion: this may be because part of the file's timespan already has a manual or release flag set. Either use the date limits to work around that, or allow skipping/overwriting existing flags with the --existing-flags option.")?;

            if &new != f {
                *f = new;
                nchanged += 1;
            }
        }
    }

    Ok((data_flags, nchanged))
}

/// Add manual or release flags in a TCCON private netCDF file.
///
/// This program allows you manually flag data in a TCCON private file
/// based on the timestamp and value of a variable in the file. This
/// is meant to allow you to flag data when a certain error metric not
/// normally considered is too large or small.
///
/// Although there are a large number of arguments to this program, only
/// a few are required: --nc-file, --filter-var, one of --in-place or
/// --output, and at least one of --less-than and/or --greater-than. Note
/// than forgetting to pass --less-than and --greater-than will not produce
/// an error, but will not add any flags.
#[derive(Debug, Parser)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Clone, Subcommand)]
enum Commands {
    Quick(QuickCli),
    Toml(TomlCli),
    TomlTemplate(TemplateCli),
}

/// Flag a netCDF file on a single variable with arguments given via the command line
#[derive(Debug, Clone, Args)]
struct QuickCli {
    #[command(flatten)]
    output: OutputCli,

    #[command(flatten)]
    flagging: Flags,

    #[command(flatten)]
    filtering: FilterCli,

    /// The path to the input netCDF file to add flags to.
    #[clap(long)]
    nc_file: PathBuf,
}

/// Flag a netCDF file based on a predefined set of filters in a TOML file
#[derive(Debug, Args, Clone)]
struct TomlCli {
    #[command(flatten)]
    output: OutputCli,

    /// Path to a TOML file containing the filter settings.
    toml_file: PathBuf,

    /// The path to the input netCDF file to add flags to
    #[clap(long)]
    nc_file: PathBuf,
}

impl TomlCli {
    fn load_filters(&self) -> error_stack::Result<FilterSet, CliError> {
        let mut f = std::fs::File::open(&self.toml_file).change_context(CliError::IoError)?;
        let mut buf = String::new();
        f.read_to_string(&mut buf)
            .change_context(CliError::IoError)?;
        let filter_set: FilterSet = toml::from_str(&buf).change_context(CliError::IoError)?;
        Ok(filter_set)
    }
}

/// Create an example of the filter file
#[derive(Debug, Clone, Args)]
struct TemplateCli {
    /// Path to write the template to
    template_file: PathBuf,
}

#[derive(Debug, Clone, Args)]
struct OutputCli {
    /// Modify the given netCDF file in place. Either this or --output must
    /// be given. Use --output if you prefer not to modify your original netCDF
    /// file.
    #[clap(short = 'i', long, conflicts_with = "output", required = true)]
    // conflicts_with take precedence over required, that's how we defined one of in_place and output is required
    in_place: bool,

    /// Path to write out the modified netCDF file. Either this or --in-place
    /// must be given. Note that if no flags are changed, the output file
    #[clap(short = 'o', long, required = true)]
    output: Option<PathBuf>,

    /// Set this flag so that the file specified by --output is always created,
    /// even if no changes to the flags are required.
    #[clap(long)]
    always_copy: bool,
}

#[derive(Debug, Clone, Args, Deserialize, Serialize)]
struct Flags {
    /// Value to use when flagging data. This must be a value between 1 and 9,
    /// it will be multiplied by 1000 and added to existing flags, i.e. this will
    /// be treated as a manual flag. See --existing-flags for how conflicts with
    /// existing manual flags are handled.
    #[clap(short = 'f', long, default_value_t = 9)]
    flag: u8,

    /// This controls what happens if you try to flag an observation that already
    /// has a manual flag. This can one of the following values: "error" (the default)
    /// will throw an error if we try to flag an observation that already has a manual
    /// flag; "skip" will leave the current manual flag in place; "overwrite" will replace
    /// the current manual flag.
    #[clap(short='e', long, default_value_t = ExistingFlag::default())]
    #[serde(default)]
    existing_flags: ExistingFlag,

    /// Which flag type ("manual" or "release") to set in the file. This controls which
    /// place in the flag integer is set; for "manual" it is the 1000s place, for "release"
    /// it is the 10000s place. IMPORTANT: most users should use the default of "manual".
    /// Release flags are intended ONLY for Caltech staff to use to flag data in response
    /// to QA/QC feedback.
    #[clap(long, default_value_t = FlagType::default())]
    #[serde(default)]
    flag_type: FlagType,
}

impl Default for Flags {
    fn default() -> Self {
        Self {
            flag: 9,
            existing_flags: Default::default(),
            flag_type: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Args)]
struct FilterCli {
    #[clap(flatten)]
    filter: Filter,
    #[clap(flatten)]
    timespan: Timespan,
}

#[derive(Debug, Clone, Args, Deserialize, Serialize)]
struct Filter {
    /// For numeric variables, flag observations less than this value.
    /// Negative values are allowed.
    #[clap(short = 'l', long, allow_negative_numbers = true)]
    #[serde(default)]
    less_than: Option<f32>,

    /// For numeric variables, flag observations greater than this value.
    /// Negative values are allowed.
    #[clap(short = 'g', long, allow_negative_numbers = true)]
    #[serde(default)]
    greater_than: Option<f32>,

    /// If both --less-than and --greater-than are given, this determines
    /// whether the observation is flagged if VARIABLE has a value between
    /// greater_than and less_than (i.e. greater_than <= v <= less_than)
    /// if the value is outside less_than and greater_than (i.e. v >= greater_than
    /// OR v <= less_than). Passing "in" or "inside" for this flag uses the first
    /// criterion, passing "out" or "outside" uses the second.
    /// If only one of --less-than  and --greater-than are given, then only the
    /// respective comparison is used; i.e. --less-than 0 will add a flag
    /// to all measurements where the filter variable is <= 0.
    #[clap(long, default_value_t = Combination::default())]
    #[serde(default)]
    value_mode: Combination,

    /// This is a required argument, it is the name of the variable to filter on.
    #[clap(short = 'x', long)]
    filter_var: String,
}

impl Filter {
    fn do_flag(&self, data: &HashMap<String, ndarray::Array1<f32>>, index: usize) -> bool {
        let comp = GreaterLess {
            less_than: self.less_than,
            greater_than: self.greater_than,
            combination: self.value_mode,
        };

        let value = data
            .get(&self.filter_var)
            .expect("All filter variables should be loaded before filtering")
            .get(index)
            .expect(
                "All filter variables should have the same number of elements as the flag variable",
            );
        comp.do_flag(value)
    }

    fn no_filters(&self) -> bool {
        self.less_than.is_none() && self.greater_than.is_none()
    }
}

#[derive(Debug, Clone, Default, Args, Deserialize, Serialize)]
struct Timespan {
    /// To limit the flags to only a specific time period, use this flag along with
    /// --time-greater-than to specify that time period. See --time-and-or for how these
    /// two arguments are interpreted together. These two arguments are
    /// compared to the timestamps in the netCDF file, so should be given in UTC.
    /// The datetimes may be given in the following formats: YYYY-MM-DD,
    /// YYYY-MM-DD HH:MM, or YYYY-MM-DD HH:MM:SS. Note that the last two contain
    /// spaces, so must be quoted. Alternatively, you may use a T in place of the
    /// space; that is "2004-07-01 12:00" and "2004-07-01T12:00" are both valid.
    #[clap(long, visible_alias="time-lt", value_parser = parse_cli_time_str)]
    #[serde(default)]
    time_less_than: Option<chrono::NaiveDateTime>,

    /// See --time-less-than and --time-mode.
    #[clap(long, visible_alias="time-gt", value_parser = parse_cli_time_str)]
    #[serde(default)]
    time_greater_than: Option<chrono::NaiveDateTime>,

    /// If both --time-less-than and --time-greater-than are given, this controls how
    /// they are combined. The default, "inside", flaggs a measurement only if its time
    /// time t is between time_greater_than and time_less_than.
    /// Set this to "outside" to instead flag if t >= time_greater_than OR t <= time_less_than,
    /// useful for flagging all points outside a given time period.
    #[clap(long, default_value_t = Combination::default())]
    #[serde(default)]
    time_mode: Combination,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct FilterAndGroup {
    filters: Vec<Filter>,
}

impl FilterAndGroup {
    fn do_flag(&self, data: &HashMap<String, ndarray::Array1<f32>>, index: usize) -> bool {
        // Only flag if all of the filters say we should flag.
        self.filters.iter().all(|f| f.do_flag(data, index))
    }

    fn no_filters(&self) -> bool {
        if self.filters.is_empty() {
            return true;
        }

        self.filters.iter().all(|f| f.no_filters())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct FilterSet {
    groups: Vec<FilterAndGroup>,
    #[serde(default)]
    timespan: Timespan,
    #[serde(default)]
    flags: Flags,
}

impl From<QuickCli> for FilterSet {
    fn from(value: QuickCli) -> Self {
        Self {
            groups: vec![FilterAndGroup {
                filters: vec![value.filtering.filter],
            }],
            timespan: value.filtering.timespan,
            flags: value.flagging,
        }
    }
}

impl FilterSet {
    fn template() -> Self {
        let cl_filter = Filter {
            less_than: Some(0.05),
            greater_than: None,
            value_mode: Combination::Inside,
            filter_var: "o2_7885_cl".to_string(),
        };
        let rms_filter = Filter {
            greater_than: Some(0.5),
            less_than: None,
            value_mode: Combination::Inside,
            filter_var: "o2_7885_rmsocl".to_string(),
        };
        let sg_filter = Filter {
            less_than: Some(-0.1),
            greater_than: Some(0.1),
            value_mode: Combination::Outside,
            filter_var: "o2_7885_sg".to_string(),
        };

        let group1 = FilterAndGroup {
            filters: vec![cl_filter, rms_filter],
        };
        let group2 = FilterAndGroup {
            filters: vec![sg_filter],
        };
        let timespan = Timespan {
            time_less_than: None,
            time_greater_than: Some(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            ),
            time_mode: Combination::Inside,
        };
        let flags = Flags::default();

        Self {
            groups: vec![group1, group2],
            timespan,
            flags,
        }
    }

    fn write_template(path: &Path) -> error_stack::Result<(), CliError> {
        let comments = [
            "This is an example filter TOML file.",
            "The top level field 'groups' is required, 'flags' and 'timespan' are not.",
            "Each entry in 'groups' represents one filter group, a value will be flagged",
            "if any of the filter groups returns true. A group returns true if all of the",
            "individual filters inside it return true.",
            "A filter must have 'filter_var' and one or both of 'less_than' and 'greater_than',",
            "value_mode is optional and defaults to 'inside'.",
            "For timespan, if given, it should have one or both of 'time_less_than' and/or",
            "'time_greater_than', 'time_mode' is optional.",
            "All fields in 'flags' are optional.",
            "The meaning of fields in each individual filter, timespan, and flags mirrors the 'quick' CLI,",
            "see the quick CLI --help for details.",
        ];
        let template = Self::template();
        let mut f = std::fs::File::create(path).change_context(CliError::IoError)?;
        for line in comments {
            writeln!(&mut f, "# {line}").change_context(CliError::IoError)?;
        }
        let s = toml::to_string_pretty(&template).change_context(CliError::IoError)?;
        write!(f, "{s}").change_context(CliError::IoError)?;

        Ok(())
    }

    fn do_flag(&self, data: &HashMap<String, ndarray::Array1<f32>>, index: usize) -> bool {
        // Flag if any of the groups says we should flag
        self.groups.iter().any(|g| g.do_flag(data, index))
    }

    fn filter_vars(&self) -> Vec<&str> {
        let mut varnames = vec![];
        for group in self.groups.iter() {
            for filter in group.filters.iter() {
                varnames.push(filter.filter_var.as_str());
            }
        }
        varnames
    }

    fn no_filters(&self) -> bool {
        if self.groups.is_empty() {
            return true;
        }

        self.groups.iter().all(|g| g.no_filters())
    }
}

fn parse_cli_time_str(s: &str) -> Result<chrono::NaiveDateTime, String> {
    match s.len() {
        10 => {
            // YYYY-MM-DD format
            let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|e| e.to_string())?;
            date.and_hms_opt(0, 0, 0).ok_or_else(|| format!("Cannot have midnight on {s} as a time."))
        },
        16 => {
            // YYYY-MM-DD HH:MM or YYYY-MM-DDTHH:MM format
            if s.chars().nth(10) == Some('T') {
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M").map_err(|e| e.to_string())
            } else {
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M").map_err(|e| e.to_string())
            }
        },
        19 => {
            // YYYY-MM-DD HH:MM:SS or YYYY-MM-DDTHH:MM:SS format
            if s.chars().nth(10) == Some('T') {
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").map_err(|e| e.to_string())
            } else {
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").map_err(|e| e.to_string())
            }
        },
        _ => {
            Err("Datetimes must be given in one of the following formats: YYYY-MM-DD, 'YYYY-MM-DD HH:MM', 'YYYY-MM-DD HH:MM:SS'. In the last two, the space may be replaced with a T if desired.".to_string())
        }
    }
}

// fn deserialize_time_str_opt<'de, D>(deserializer: D) -> Result<Option<chrono::NaiveDateTime>, D::Error>
// where D: Deserializer<'de> {
//     let value = Option<String>::deseria?;
//     let datetime = parse_cli_time_str(&value)
//         .map_err(|e| de::Error::custom(e.to_string()))?;
//     Ok(datetime)
// }

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(try_from = "String")]
enum ExistingFlag {
    Error,
    SkipEqual,
    Skip,
    Overwrite,
}

impl FromStr for ExistingFlag {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "error" => Ok(Self::Error),
            "skipeq" => Ok(Self::SkipEqual),
            "skip" => Ok(Self::Skip),
            "overwrite" => Ok(Self::Overwrite),
            _ => Err(format!("'{s}' is not a valid existing flag variant")),
        }
    }
}

impl TryFrom<String> for ExistingFlag {
    type Error = String;

    fn try_from(value: String) -> Result<Self, String> {
        value.parse()
    }
}

impl Display for ExistingFlag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExistingFlag::Error => write!(f, "error"),
            ExistingFlag::SkipEqual => write!(f, "skipeq"),
            ExistingFlag::Skip => write!(f, "skip"),
            ExistingFlag::Overwrite => write!(f, "overwrite"),
        }
    }
}

impl Default for ExistingFlag {
    fn default() -> Self {
        Self::Error
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(try_from = "String")]
enum Combination {
    Inside,
    Outside,
}

impl Default for Combination {
    fn default() -> Self {
        Self::Inside
    }
}

impl FromStr for Combination {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "in" | "inside" => Ok(Self::Inside),
            "and" => {
                log::warn!("'and' is deprecated as a keyword to combine limits, use 'in' or 'inside' instead");
                Ok(Self::Inside)
            }
            "out" | "outside" => Ok(Self::Outside),
            "or" => {
                log::warn!("'or' is deprecated as a keyword to combine limits, use 'out' or 'outside' instead");
                Ok(Self::Outside)
            }
            _ => Err(format!("'{s}' is not a valid combination variant")),
        }
    }
}

impl TryFrom<String> for Combination {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl Display for Combination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Combination::Inside => write!(f, "inside"),
            Combination::Outside => write!(f, "outside"),
        }
    }
}

#[derive(Debug)]
struct FlagReplaceError {
    place: i16,
}

impl Display for FlagReplaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "observation already has a flag in the {}s place",
            self.place
        )
    }
}

impl std::error::Error for FlagReplaceError {}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(try_from = "String")]
enum FlagType {
    Manual,
    Release,
}

impl Default for FlagType {
    fn default() -> Self {
        Self::Manual
    }
}

impl FromStr for FlagType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "manual" => Ok(Self::Manual),
            "release" => Ok(Self::Release),
            _ => Err(format!("'{s}' is not a valid flag type")),
        }
    }
}

impl TryFrom<String> for FlagType {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl FlagType {
    fn update_flag(
        &self,
        original_flag: i16,
        new_flag_place_value: u8,
        exists: ExistingFlag,
    ) -> Result<i16, FlagReplaceError> {
        let place_value = self.value_in_place(original_flag);
        let new_flag_place_value = new_flag_place_value as i16 * self.flag_place();
        match exists {
            ExistingFlag::Error => {
                if place_value == 0 {
                    return Ok(original_flag + new_flag_place_value);
                } else {
                    return Err(FlagReplaceError {
                        place: self.flag_place(),
                    });
                }
            }
            ExistingFlag::SkipEqual => {
                if place_value == 0 {
                    return Ok(original_flag + new_flag_place_value);
                } else if place_value == new_flag_place_value {
                    return Ok(original_flag);
                } else {
                    return Err(FlagReplaceError {
                        place: self.flag_place(),
                    });
                }
            }
            ExistingFlag::Skip => {
                if place_value == 0 {
                    return Ok(original_flag + new_flag_place_value);
                } else {
                    return Ok(original_flag);
                }
            }
            ExistingFlag::Overwrite => {
                return Ok(original_flag - place_value + new_flag_place_value)
            }
        }
    }

    fn flag_place(&self) -> i16 {
        match self {
            FlagType::Manual => 1000,
            FlagType::Release => 10_000,
        }
    }

    fn value_in_place(&self, original_flag: i16) -> i16 {
        let place = self.flag_place();
        if place != 1 && place != 10 && place != 100 && place != 1000 && place != 10_000 {
            panic!("place must be 1, 10, 100, 1000, or 10000")
        }

        let place_value = original_flag.div_euclid(place) % 10;
        place_value * place
    }
}

impl Display for FlagType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlagType::Manual => write!(f, "manual"),
            FlagType::Release => write!(f, "release"),
        }
    }
}

struct GreaterLess<T: PartialOrd> {
    less_than: Option<T>,
    greater_than: Option<T>,
    combination: Combination,
}

impl<T: PartialOrd> GreaterLess<T> {
    fn no_limits(&self) -> bool {
        self.greater_than.is_none() && self.less_than.is_none()
    }

    fn do_flag(&self, value: &T) -> bool {
        // println!("{value:?} in [{:?}, {:?}]", self.greater_than, self.less_than);
        match (&self.greater_than, &self.less_than) {
            (None, None) => false,
            (None, Some(lim)) => value <= lim,
            (Some(lim), None) => value >= lim,
            (Some(gt_lim), Some(lt_lim)) => match self.combination {
                Combination::Inside => value >= gt_lim && value <= lt_lim,
                Combination::Outside => value >= gt_lim || value <= lt_lim,
            },
        }
    }
}
