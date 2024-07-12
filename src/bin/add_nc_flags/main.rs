use std::{path::{PathBuf, Path}, str::FromStr, fmt::Display, process::ExitCode};

use clap::{Parser, Args};
use error_stack::ResultExt;

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

    // One check - if we are outputting to a new path, make sure that isn't a directory but
    // that its parent directory exists. This way we can give a clearer error message.
    if let Some(out_file) = &args.output.output {
        if out_file.is_dir() {
            return Err(CliError::UserError("Path given to --output must not be a directory".to_string()).into());
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


    let data = load_flags_and_data(&args.nc_file, &args.filtering.filter_var)?;
    let (data, nchanged) = update_flags(data, &args.filtering, &args.flagging)?;

    if nchanged == 0 {
        if args.output.in_place {
            println!("No flags required update, file {} unchanged.", args.nc_file.display());
        } else if args.output.always_copy {
            // Ok to unwrap args.output.output - Cli::parse ensures in_place or output is given
            let out_file = args.output.output.as_deref().unwrap();
            std::fs::copy(&args.nc_file, out_file)
                .change_context(CliError::IoError)
                .attach_printable("Could not copy NC_FILE to the path given by --output")?;
            println!("No flags required update, copied {} to {}", args.nc_file.display(), out_file.display());
        } else {
            // Ok to unwrap args.output.output - Cli::parse ensures in_place or output is given
            println!("No flags required update, did not produce/overwrite output file {}", args.output.output.unwrap().display());
        }

        if args.filtering.less_than.is_none() && args.filtering.greater_than.is_none() {
            println!("Note: nothing flagged because you gave neither the --less-than nor --greater-than argument.")
        }
        return Ok(());
    }

    let output_file = if let Some(f) = args.output.output {
        std::fs::copy(&args.nc_file, &f)
            .change_context(CliError::IoError)
            .attach_printable("Could not copy NC_FILE to the path given by --output")?;
        f
    } else if args.output.in_place {
        args.nc_file
    } else {
        panic!("One of --in-place or --output must be given");
    };

    let mut ds = netcdf::append(&output_file)
        .attach_printable("Could not edit the original (if --in-place) or output (if --output) netCDF file")
        .change_context(CliError::NcError)?;
    let mut flags = ds.variable_mut("flag")
        .ok_or_else(|| CliError::MissingReqVariable("flag"))
        .attach_printable("This occurred while trying to get the flag variable in either the new output file or (if --in-place given) the original file")?; // this really shouldn't happen, since we read that variable in already
    flags.put_values(data.flags.as_slice().unwrap(), netcdf::Extents::All)
        .change_context(CliError::NcError)
        .attach_printable("This occur write new flag values to either the new output file or (if --in-place given) the original file")?;

    println!("{nchanged} flag values updated");
    Ok(())
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

fn load_flags_and_data(nc_file: &Path, filter_varname: &str ) -> error_stack::Result<TcconData, CliError> {
    let ds = netcdf::open(nc_file)
        .change_context(CliError::NcError)?;

    let timestamps = ds.variable("time")
        .ok_or_else(|| CliError::MissingReqVariable("time"))?
        .get::<f64, _>(netcdf::Extents::All)
        .change_context(CliError::NcError)?
        .into_dimensionality::<ndarray::Ix1>()
        .change_context_lazy(|| CliError::WrongDimension("time".to_string(), 1))?;

    let flags = ds.variable("flag")
        .ok_or_else(|| CliError::MissingReqVariable("flag"))?
        .get::<i16, _>(netcdf::Extents::All)
        .change_context(CliError::NcError)?
        .into_dimensionality::<ndarray::Ix1>()
        .change_context_lazy(|| CliError::WrongDimension("flag".to_string(), 1))?;

    let filter_var = ds.variable(filter_varname)
        .ok_or_else(|| CliError::MissingFilterVariable(filter_varname.to_string()))?
        .get::<f32, _>(netcdf::Extents::All)
        .change_context(CliError::NcError)
        .attach_printable("This error may be caused by trying to filter on a variable that is not of type 'float'")?
        .into_dimensionality::<ndarray::Ix1>()
        .change_context_lazy(|| CliError::WrongDimension(filter_varname.to_string(), 1))?;

    Ok(TcconData { filter_var, timestamps, flags })
}


#[derive(Debug)]
struct TcconData {
    filter_var: ndarray::Array1<f32>,
    timestamps: ndarray::Array1<f64>,
    flags: ndarray::Array1<i16>,
}

fn update_flags(mut data: TcconData, filtering: &FilterCli, flagging: &FlagsCli) -> error_stack::Result<(TcconData, u64), CliError> {
    let value_check = GreaterLess { 
        greater_than: filtering.greater_than, 
        less_than: filtering.less_than, 
        combination: filtering.value_and_or 
    };

    let timestamp_check = GreaterLess {
        greater_than: filtering.time_greater_than.map(|dt| dt.timestamp() as f64),
        less_than: filtering.time_less_than.map(|dt| dt.timestamp() as f64),
        combination: filtering.time_and_or
    };

    if data.filter_var.len() != data.timestamps.len() || data.filter_var.len() != data.flags.len() {
        return Err(CliError::MismatchedLengths.into());
    }

    let mut nchanged = 0;

    for (i, f) in data.flags.iter_mut().enumerate() {
        // We checked the lengths before, so we are okay to unwrap here.
        let t = data.timestamps.get(i).unwrap();
        let v = data.filter_var.get(i).unwrap();

        if !timestamp_check.no_limits() && !timestamp_check.do_flag(t) {
            // println!("Skipped for time");
            continue;
        }

        if value_check.do_flag(v) {
            let new = flagging.flag_type.update_flag(*f, flagging.flag, flagging.existing_flags)
                .change_context_lazy(|| CliError::FlagReplaceError(i))
                .attach_printable("Suggestion: this may be because part of the file's timespan already has a manual or release flag set. Either use the date limits to work around that, or allow skipping/overwriting existing flags with the --existing-flags option.")?;

            if &new != f {
                *f = new;
                nchanged += 1;
            }
        }
    }

    
    Ok((data, nchanged))
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
    #[command(flatten)]
    output: OutputCli,

    #[command(flatten)]
    flagging: FlagsCli,

    #[command(flatten)]
    filtering: FilterCli,

    /// The path to the input netCDF file to add flags to.
    #[clap(long)]
    nc_file: PathBuf,
}

#[derive(Debug, Args)]
struct OutputCli {
    /// Modify the given netCDF file in place. Either this or --output must
    /// be given. Use --output if you prefer not to modify your original netCDF
    /// file.
    #[clap(short='i', long, conflicts_with = "output", required = true)]  // conflicts_with take precedence over required, that's how we defined one of in_place and output is required
    in_place: bool,

    /// Path to write out the modified netCDF file. Either this or --in-place 
    /// must be given. Note that if no flags are changed, the output file
    #[clap(short='o', long, required = true)]
    output: Option<PathBuf>,

    /// Set this flag so that the file specified by --output is always created,
    /// even if no changes to the flags are required.
    #[clap(long)]
    always_copy: bool,
}

#[derive(Debug, Args)]
struct FlagsCli {
    /// Value to use when flagging data. This must be a value between 1 and 9,
    /// it will be multiplied by 1000 and added to existing flags, i.e. this will 
    /// be treated as a manual flag. See --existing-flags for how conflicts with
    /// existing manual flags are handled.
    #[clap(short='f', long, default_value_t=9)]
    flag: u8,

    /// This controls what happens if you try to flag an observation that already
    /// has a manual flag. This can one of the following values: "error" (the default)
    /// will throw an error if we try to flag an observation that already has a manual
    /// flag; "skip" will leave the current manual flag in place; "overwrite" will replace
    /// the current manual flag.
    #[clap(short='e', long, default_value_t = ExistingFlag::default())]
    existing_flags: ExistingFlag,

    /// Which flag type ("manual" or "release") to set in the file. This controls which 
    /// place in the flag integer is set; for "manual" it is the 1000s place, for "release"
    /// it is the 10000s place. IMPORTANT: most users should use the default of "manual". 
    /// Release flags are intended ONLY for Caltech staff to use to flag data in response
    /// to QA/QC feedback.
    #[clap(long, default_value_t = FlagType::Manual)]
    flag_type: FlagType,
}

#[derive(Debug, Args)]
struct FilterCli {
    /// For numeric variables, flag observations less than this value.
    /// Negative values are allowed.
    #[clap(short='l', long, allow_negative_numbers=true)]
    less_than: Option<f32>,

    /// For numeric variables, flag observations greater than this value.
    /// Negative values are allowed.
    #[clap(short='g', long, allow_negative_numbers=true)]
    greater_than: Option<f32>,

    /// If both --less-than and --greater-than are given, this determines
    /// whether the observation is flagged if VARIABLE has v >= greater_than
    /// AND v <= less_than or v >= greater_than OR v <= less_than. If only
    /// one of --less-than  and --greater-than are given, then only the
    /// respective comparison is used; i.e. --less-than 0 will add a flag 
    /// to all measurements where the filter variable is <= 0.
    #[clap(long, default_value_t = Combination::And)]
    value_and_or: Combination,

    /// To limit the flags to only a specific time period, use this flag along with
    /// --time-greater-than to specify that time period. See --time-and-or for how these
    /// two arguments are interpreted together. These two arguments are
    /// compared to the timestamps in the netCDF file, so should be given in UTC.
    /// The datetimes may be given in the following formats: YYYY-MM-DD, 
    /// YYYY-MM-DD HH:MM, or YYYY-MM-DD HH:MM:SS. Note that the last two contain
    /// spaces, so must be quoted. Alternatively, you may use a T in place of the
    /// space; that is "2004-07-01 12:00" and "2004-07-01T12:00" are both valid.
    #[clap(long, visible_alias="time-lt", value_parser = parse_cli_time_str)]
    time_less_than: Option<chrono::NaiveDateTime>,

    /// See --time-less-than and --time-and-or.
    #[clap(long, visible_alias="time-gt", value_parser = parse_cli_time_str)]
    time_greater_than: Option<chrono::NaiveDateTime>,

    /// If both --time-less-than and --time-greater-than are given, this controls how
    /// they are combined. The default, "and", does a logical AND, i.e. a measurement
    /// is flagged only if its time t is time_greater_than <= t <= time_less_than.
    /// Set this to "or" to instead flag if t >= time_greater_than OR t <= time_less_than,
    /// useful for flagging all points outside a given time period.
    #[clap(long, default_value_t = Combination::And)]
    time_and_or: Combination,

    /// This is a required argument, it is the name of the variable to filter on.
    #[clap(short='x', long)]
    filter_var: String,
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


#[derive(Debug, Clone, Copy)]
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
            _ => Err(format!("'{s}' is not a valid existing flag variant"))
        }
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

#[derive(Debug, Clone, Copy)]
enum Combination {
    And,
    Or
}

impl FromStr for Combination {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "and" => Ok(Self::And),
            "or" => Ok(Self::Or),
            _ => Err(format!("'{s}' is not a valid combination variant"))
        }
    }
}

impl Display for Combination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Combination::And => write!(f, "and"),
            Combination::Or => write!(f, "or"),
        }
    }
}

#[derive(Debug)]
struct FlagReplaceError {
    place: i16
}

impl Display for FlagReplaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "observation already has a flag in the {}s place", self.place)
    }
}

impl std::error::Error for FlagReplaceError {}

#[derive(Debug, Clone, Copy)]
enum FlagType {
    Manual,
    Release
}

impl FromStr for FlagType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "manual" => Ok(Self::Manual),
            "release" => Ok(Self::Release),
            _ => Err(format!("'{s}' is not a valid flag type"))
        }
    }
}

impl FlagType {
    fn update_flag(&self, original_flag: i16, new_flag_place_value: u8, exists: ExistingFlag) -> Result<i16, FlagReplaceError> {
        let place_value = self.value_in_place(original_flag);
        let new_flag_place_value  = new_flag_place_value as i16 * self.flag_place();
        match exists {
            ExistingFlag::Error => {
                if place_value == 0 {
                    return Ok(original_flag + new_flag_place_value)
                } else {
                    return Err(FlagReplaceError { place: self.flag_place() })
                }
            },
            ExistingFlag::SkipEqual => {
                if place_value == 0 {
                    return Ok(original_flag + new_flag_place_value)
                } else if place_value == new_flag_place_value {
                    return Ok(original_flag)
                } else {
                    return Err(FlagReplaceError { place: self.flag_place() })
                }
            },
            ExistingFlag::Skip => {
                if place_value == 0 {
                    return Ok(original_flag + new_flag_place_value)
                } else {
                    return Ok(original_flag)
                }
            },
            ExistingFlag::Overwrite => {
                return Ok(original_flag - place_value + new_flag_place_value)
            },
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
    combination: Combination
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
            (Some(gt_lim), Some(lt_lim)) => {
                match self.combination {
                    Combination::And => value >= gt_lim && value <= lt_lim,
                    Combination::Or => value >= gt_lim || value <= lt_lim,
                }
            },
        }
    }
}