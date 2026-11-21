use std::{
    collections::HashSet,
    io::{BufWriter, Write},
    path::Path,
};

use error_stack::ResultExt;
use fortformat::{
    format_specs::{IntBase, RealFmt},
    ser::{many_to_writer_custom, SerSettings},
    serde_common::NoneFill,
    FortField, FortFormat,
};
use ggg_rs::{
    math_utils::{RunningExtrema, RunningMean},
    units::ratio::serialize_opt_in,
};
use log::debug;
use uom::si::f32::Ratio;
use uom::si::ratio::ratio;

use crate::{
    compare::{make_prev_and_new_list, PrevOrNew},
    data_structures::{Gas, TimeAvgData},
    CliError,
};

#[derive(Debug, serde::Serialize)]
struct TimeavgComparison<'f> {
    file_name: &'f str,
    obs_id: i64,
    prev_num_soundings: Option<u64>,
    new_num_soundings: Option<u64>,
    diff_num_soundings: Option<i64>,
    #[serde(serialize_with = "serialize_opt_in::<ratio, _, _, _>")]
    prev_col_mixing: Option<Ratio>,
    #[serde(serialize_with = "serialize_opt_in::<ratio, _, _, _>")]
    new_col_mixing: Option<Ratio>,
    #[serde(serialize_with = "serialize_opt_in::<ratio, _, _, _>")]
    diff_col_mixing: Option<Ratio>,
    #[serde(serialize_with = "serialize_opt_in::<ratio, _, _, _>")]
    prev_sigma_col: Option<Ratio>,
    #[serde(serialize_with = "serialize_opt_in::<ratio, _, _, _>")]
    new_sigma_col: Option<Ratio>,
    #[serde(serialize_with = "serialize_opt_in::<ratio, _, _, _>")]
    diff_sigma_col: Option<Ratio>,
}

impl<'f> TimeavgComparison<'f> {
    fn field_names() -> &'static [&'static str] {
        &[
            "file_name",
            "obs_id",
            "prev_num_soundings",
            "new_num_soundings",
            "diff_num_soundings",
            "prev_col_mixing",
            "new_col_mixing",
            "diff_col_mixing",
            "prev_sigma_col",
            "new_sigma_col",
            "diff_sigma_col",
        ]
    }

    fn fortran_format() -> FortFormat {
        let mut fields = Vec::with_capacity(Self::field_names().len() * 2 - 1);
        // file_name should be 26 characters long, e.g. "tccon_timeavg_20250604.nc4"
        fields.push(FortField::Char { width: Some(26) });
        fields.push(FortField::Skip);

        // obs_id will always be 16 digits, no need for padding because it starts with the year
        fields.push(FortField::Integer {
            width: 16,
            zeros: None,
            base: IntBase::Decimal,
        });

        // Add three fields for the numbers of soundings. The width of the header strings is
        // longer than we'd ever need here.
        for _ in 0..3 {
            fields.push(FortField::Skip);
            fields.push(FortField::Integer {
                width: 18,
                zeros: None,
                base: IntBase::Decimal,
            });
        }

        // Then the six fields for the column mixing and its uncertainty. For now, I'm writing
        // them as ratios (not ppm or anything) just because I haven't figured out how to serialize
        // uom `Quantity` instances in different units for different instances of the type.
        // As for the numbers of soundings, the widths are set to fit the column names.
        for _ in 0..6 {
            fields.push(FortField::Skip);
            fields.push(FortField::Real {
                width: 15,
                precision: Some(4),
                fmt: RealFmt::E,
                scale: 1,
            });
        }

        FortFormat::Fixed(fields)
    }

    fn header_fortran_format() -> FortFormat {
        let mut fields = vec![];
        for val_field in Self::fortran_format().iter_fields() {
            let head_field = match val_field {
                FortField::Char { width } => FortField::Char { width: *width },
                FortField::Logical { width } => FortField::Char {
                    width: Some(*width),
                },
                FortField::Integer {
                    width,
                    zeros: _,
                    base: _,
                } => FortField::Char {
                    width: Some(*width),
                },
                FortField::Real {
                    width,
                    precision: _,
                    fmt: _,
                    scale: _,
                } => FortField::Char {
                    width: Some(*width),
                },
                FortField::Skip => FortField::Skip,
                FortField::Any => FortField::Any,
            };
            fields.push(head_field);
        }
        FortFormat::Fixed(fields)
    }

    fn new_both(
        file_name: &'f str,
        obs_id: i64,
        prev_num_soundings: u64,
        new_num_soundings: u64,
        prev_col_mixing: Ratio,
        new_col_mixing: Ratio,
        prev_col_uncert: Ratio,
        new_col_uncert: Ratio,
    ) -> Self {
        Self {
            file_name,
            obs_id,
            prev_num_soundings: Some(prev_num_soundings),
            new_num_soundings: Some(new_num_soundings),
            diff_num_soundings: Some(new_num_soundings as i64 - prev_num_soundings as i64),
            prev_col_mixing: Some(prev_col_mixing),
            new_col_mixing: Some(new_col_mixing),
            diff_col_mixing: Some(new_col_mixing - prev_col_mixing),
            prev_sigma_col: Some(prev_col_uncert),
            new_sigma_col: Some(new_col_uncert),
            diff_sigma_col: Some(new_col_uncert - prev_col_uncert),
        }
    }
    fn new_one_only(
        prev: bool,
        file_name: &'f str,
        obs_id: i64,
        num_soundings: u64,
        col_mixing: Ratio,
        col_uncert: Ratio,
    ) -> Self {
        if prev {
            Self {
                file_name,
                obs_id,
                prev_num_soundings: Some(num_soundings),
                new_num_soundings: None,
                diff_num_soundings: None,
                prev_col_mixing: Some(col_mixing),
                new_col_mixing: None,
                diff_col_mixing: None,
                prev_sigma_col: Some(col_uncert),
                new_sigma_col: None,
                diff_sigma_col: None,
            }
        } else {
            Self {
                file_name,
                obs_id,
                prev_num_soundings: None,
                new_num_soundings: Some(num_soundings),
                diff_num_soundings: None,
                prev_col_mixing: None,
                new_col_mixing: Some(col_mixing),
                diff_col_mixing: None,
                prev_sigma_col: None,
                new_sigma_col: Some(col_uncert),
                diff_sigma_col: None,
            }
        }
    }
}

#[derive(Debug, Default)]
struct TimeavgDiffStats {
    num_prev_only: u64,
    num_new_only: u64,
    num_soundings_mean_abs_bias: RunningMean,
    num_soundings_diff_extrema: RunningExtrema,
    col_mixing_mean_abs_bias: RunningMean,
    col_mixing_diff_extrema: RunningExtrema,
    sigma_col_mean_abs_bias: RunningMean,
    sigma_col_diff_extrema: RunningExtrema,
}

impl TimeavgDiffStats {
    fn add_diff(&mut self, num_soundings_diff: f64, col_mixing_diff: Ratio, sigma_col_diff: Ratio) {
        let num_soundings_diff = num_soundings_diff as f64;
        let col_mixing_diff = col_mixing_diff.get::<ratio>();
        let sigma_col_diff = sigma_col_diff.get::<ratio>();

        self.num_soundings_mean_abs_bias
            .add_value(num_soundings_diff.abs());
        self.num_soundings_diff_extrema
            .add_value(num_soundings_diff);
        self.col_mixing_mean_abs_bias
            .add_value(col_mixing_diff.abs());
        self.col_mixing_diff_extrema.add_value(col_mixing_diff);
        self.sigma_col_mean_abs_bias.add_value(sigma_col_diff.abs());
        self.sigma_col_diff_extrema.add_value(sigma_col_diff);
    }

    fn add_prev_only(&mut self) {
        self.num_prev_only += 1;
    }

    fn add_new_only(&mut self) {
        self.num_new_only += 1;
    }

    fn write_summary<W: std::io::Write>(&self, mut f: W) -> std::io::Result<()> {
        writeln!(
            f,
            "# soundings only in previous across common files = {}",
            self.num_prev_only
        )?;
        writeln!(
            f,
            "# soundings only in new across common files = {}",
            self.num_new_only
        )?;
        writeln!(
            f,
            "# soundings mean bias = {:.2}",
            self.num_soundings_mean_abs_bias
                .current_mean()
                .unwrap_or(0.0)
        )?;
        writeln!(
            f,
            "# soundings min/max difference = {:.0}, {:.0}",
            self.num_soundings_diff_extrema.current_min().unwrap_or(0.0),
            self.num_soundings_diff_extrema.current_max().unwrap_or(0.0)
        )?;
        writeln!(
            f,
            "Column mixing mean bias = {:.4E} mol/mol",
            self.col_mixing_mean_abs_bias.current_mean().unwrap_or(0.0)
        )?;
        writeln!(
            f,
            "Column mixing min/max difference = {:.4E}, {:.4E} mol/mol",
            self.col_mixing_diff_extrema.current_min().unwrap_or(0.0),
            self.col_mixing_diff_extrema.current_max().unwrap_or(0.0)
        )?;
        writeln!(
            f,
            "Sigma column mixing mean bias = {:.4E} mol/mol",
            self.sigma_col_mean_abs_bias.current_mean().unwrap_or(0.0)
        )?;
        writeln!(
            f,
            "Sigma column mixing min/max difference = {:.4E}, {:.4E} mol/mol",
            self.sigma_col_diff_extrema.current_min().unwrap_or(0.0),
            self.sigma_col_diff_extrema.current_max().unwrap_or(0.0)
        )?;
        Ok(())
    }
}

pub(super) fn make_comparison(
    prev_dir: &Path,
    new_dir: &Path,
    report_dir: &Path,
    summary_writer: &mut BufWriter<std::fs::File>,
    files: &[PrevOrNew<String>],
    gases: &[Gas],
) -> error_stack::Result<(), CliError> {
    for gas in gases {
        make_comparison_one_gas(prev_dir, new_dir, report_dir, summary_writer, files, gas)?;
    }
    Ok(())
}

fn make_comparison_one_gas(
    prev_dir: &Path,
    new_dir: &Path,
    report_dir: &Path,
    summary_writer: &mut BufWriter<std::fs::File>,
    files: &[PrevOrNew<String>],
    gas: &Gas,
) -> error_stack::Result<(), CliError> {
    let mut stats = TimeavgDiffStats::default();

    let obs_report_file = report_dir.join(format!(
        "{}_comparison.dat",
        gas.output_group().to_lowercase()
    ));
    let f = std::fs::File::create(&obs_report_file).change_context_lazy(|| {
        CliError::Context(format!(
            "error creating report file {}",
            obs_report_file.display()
        ))
    })?;
    let mut rpt = BufWriter::new(f);
    let fmt = TimeavgComparison::fortran_format();
    let settings = SerSettings::default()
        .fill_method(NoneFill::new_string("NA"))
        .align_left_str(true);

    // Write a GGG-style first line (nhead, ncol)
    // followed by the column names. The format
    // should be right to keep the column names and
    // values aligned.
    let col_names = TimeavgComparison::field_names();
    let header_fmt = TimeavgComparison::header_fortran_format();
    writeln!(rpt, "2 {}", col_names.len()).change_context_lazy(|| {
        CliError::Context(format!(
            "Error writing header to {}",
            obs_report_file.display()
        ))
    })?;
    fortformat::ser::to_writer_custom::<_, _, &str>(
        &col_names,
        &header_fmt,
        None,
        &settings,
        &mut rpt,
    )
    .change_context_lazy(|| {
        CliError::Context(format!(
            "Error writing header to {}",
            obs_report_file.display()
        ))
    })?;

    // Reset the string alignment so that "NA" values are right aligned
    // like numbers
    let settings = settings.align_left_str(false);
    for file in files {
        debug!("Checking data in {}", file.as_str());
        extend_report(
            &mut rpt, &mut stats, &fmt, &settings, prev_dir, new_dir, file, gas,
        )?;
    }

    // Add to the summary file
    writeln!(summary_writer, "\n==== {} ====\n", gas.output_group()).change_context_lazy(|| {
        CliError::Context(format!(
            "error writing {} group header to summary file",
            gas.output_group()
        ))
    })?;
    stats
        .write_summary(summary_writer)
        .change_context_lazy(|| {
            CliError::Context(format!(
                "error writing {} group stats to summary file",
                gas.output_group()
            ))
        })?;
    Ok(())
}

fn extend_report(
    rpt: &mut BufWriter<std::fs::File>,
    stats: &mut TimeavgDiffStats,
    fmt: &FortFormat,
    settings: &SerSettings,
    prev_dir: &Path,
    new_dir: &Path,
    file: &PrevOrNew<String>,
    gas: &Gas,
) -> error_stack::Result<(), CliError> {
    let comparisons = match file {
        PrevOrNew::Both(name) => extend_with_match(stats, prev_dir, new_dir, name, gas)?,
        PrevOrNew::PrevOnly(name) => extend_no_match(true, prev_dir, name, gas)?,
        PrevOrNew::NewOnly(name) => extend_no_match(false, new_dir, name, gas)?,
    };

    many_to_writer_custom(
        &comparisons,
        fmt,
        Some(TimeavgComparison::field_names()),
        settings,
        rpt,
    )
    .change_context_lazy(|| {
        CliError::Context(format!(
            "error adding {} report rows for file {}",
            gas.output_group(),
            file.as_str()
        ))
    })?;
    Ok(())
}

fn extend_no_match<'f>(
    prev: bool,
    nc_dir: &Path,
    nc_file_name: &'f str,
    gas: &Gas,
) -> error_stack::Result<Vec<TimeavgComparison<'f>>, CliError> {
    let nc_file = nc_dir.join(nc_file_name);
    let data = TimeAvgData::read_gas_from_file(&nc_file, gas)?;
    let mut comparisons = vec![];
    for i in 0..data.obs_id.len() {
        comparisons.push(TimeavgComparison::new_one_only(
            prev,
            nc_file_name,
            data.obs_id[i],
            data.num_soundings[i],
            data.column_mixing[i],
            data.sigma_column_mixing[i],
        ));
    }
    Ok(comparisons)
}

fn extend_with_match<'f>(
    stats: &mut TimeavgDiffStats,
    prev_dir: &Path,
    new_dir: &Path,
    nc_file_name: &'f str,
    gas: &Gas,
) -> error_stack::Result<Vec<TimeavgComparison<'f>>, CliError> {
    let prev_file = prev_dir.join(nc_file_name);
    let new_file = new_dir.join(nc_file_name);
    let prev_data = TimeAvgData::read_gas_from_file(&prev_file, gas)?;
    let new_data = TimeAvgData::read_gas_from_file(&new_file, gas)?;

    let prev_obs_ids = HashSet::from_iter(prev_data.obs_id.iter().copied());
    let new_obs_ids = HashSet::from_iter(new_data.obs_id.iter().copied());
    let mut obs_ids = make_prev_and_new_list(prev_obs_ids, new_obs_ids);
    obs_ids.sort_unstable_by(|a, b| a.as_ref().cmp(b.as_ref()));

    let mut comparisons = vec![];
    for obs_id in obs_ids {
        match obs_id {
            PrevOrNew::Both(id) => {
                let (prev_ns, prev_col, prev_sigma) = val_for_obs_id(&prev_data, id)
                    .change_context_lazy(|| {
                        CliError::Context(format!(
                            "could not find obs ID {id} in previous file ({}) - this is a bug",
                            prev_file.display()
                        ))
                    })?;
                let (new_ns, new_col, new_sigma) = val_for_obs_id(&new_data, id)
                    .change_context_lazy(|| {
                        CliError::Context(format!(
                            "could not find obs ID {id} in new file ({}) - this is a bug",
                            new_file.display()
                        ))
                    })?;

                comparisons.push(TimeavgComparison::new_both(
                    nc_file_name,
                    id,
                    prev_ns,
                    new_ns,
                    prev_col,
                    new_col,
                    prev_sigma,
                    new_sigma,
                ));
                stats.add_diff(
                    new_ns as f64 - prev_ns as f64,
                    new_col - prev_col,
                    new_sigma - prev_sigma,
                );
            }
            PrevOrNew::PrevOnly(id) => {
                let (prev_ns, prev_col, prev_sigma) = val_for_obs_id(&prev_data, id)
                    .change_context_lazy(|| {
                        CliError::Context(format!(
                            "could not find obs ID {id} in previous file ({}) - this is a bug",
                            prev_file.display()
                        ))
                    })?;

                comparisons.push(TimeavgComparison::new_one_only(
                    true,
                    nc_file_name,
                    id,
                    prev_ns,
                    prev_col,
                    prev_sigma,
                ));
                stats.add_prev_only();
            }
            PrevOrNew::NewOnly(id) => {
                let (new_ns, new_col, new_sigma) = val_for_obs_id(&new_data, id)
                    .change_context_lazy(|| {
                        CliError::Context(format!(
                            "could not find obs ID {id} in new file ({}) - this is a bug",
                            new_file.display()
                        ))
                    })?;

                comparisons.push(TimeavgComparison::new_one_only(
                    false,
                    nc_file_name,
                    id,
                    new_ns,
                    new_col,
                    new_sigma,
                ));
                stats.add_new_only();
            }
        }
    }
    Ok(comparisons)
}

fn val_for_obs_id(
    data: &TimeAvgData,
    obs_id: i64,
) -> error_stack::Result<(u64, Ratio, Ratio), CliError> {
    let idx = data
        .obs_id
        .iter()
        .position(|id| *id == obs_id)
        .ok_or_else(|| CliError::Custom(format!("unable to find obs ID {obs_id} in data")))?;
    Ok((
        data.num_soundings[idx],
        data.column_mixing[idx],
        data.sigma_column_mixing[idx],
    ))
}
