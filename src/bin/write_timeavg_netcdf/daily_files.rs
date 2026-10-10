use std::path::Path;

use error_stack::ResultExt;
use ggg_rs::utils::{iter_dates, site_id_from_filename};
use indexmap::IndexMap;

use crate::{
    data_structures::{AllGasAvgData, Gas, TimeAvgData},
    CliError,
};

mod split_dates;

pub(super) fn daily_files_driver<F: AsRef<Path>>(
    average_files: &[F],
    output_dir: &Path,
    gases: &[Gas],
    start_date: Option<chrono::NaiveDate>,
    end_date: Option<chrono::NaiveDate>,
) -> error_stack::Result<(), CliError> {
    let all_data = load_site_files(average_files, gases)?;
    let gas_date_ranges =
        split_dates::get_date_range_all_gases(&all_data, gases, start_date, end_date)?;
    for date in iter_dates(gas_date_ranges.overall_start, gas_date_ranges.overall_end) {
        let filename = date.format("tccon_timeavg_%Y%m%d.nc4").to_string();
        let file = output_dir.join(filename);
        let mut ds = netcdf::create(&file).change_context_lazy(|| {
            CliError::Context(format!("error creating daily file {}", file.display()))
        })?;
        for gas in gases {
            if !gas_date_ranges.gas_has_date(gas, date) {
                continue;
            }

            let gas_data_iter = all_data.values().map(|v| {
                v.get(gas).expect("all sites' data must have all gases, this should be enforced by the loading function")
            });
            let day_data = split_dates::extract_date_subset(gas_data_iter, date)
                .change_context_lazy(|| {
                    CliError::Context(format!("error subsetting data for gas {gas} on {date}"))
                })?;

            let mut grp = ds.add_group(gas.output_group()).change_context_lazy(|| {
                CliError::Context(format!(
                    "error creating group '{}' in daily file {}",
                    gas.output_group(),
                    file.display()
                ))
            })?;
            day_data.write_netcdf(&mut grp).change_context_lazy(|| {
                CliError::Context(format!("error writing data for gas {gas} on {date}"))
            })?;
        }
    }

    Ok(())
}

fn load_site_files<F: AsRef<Path>>(
    average_files: &[F],
    gases: &[Gas],
) -> error_stack::Result<IndexMap<String, AllGasAvgData>, CliError> {
    let mut data = IndexMap::new();

    for file in average_files {
        let file = file.as_ref();

        let site_id = site_id_from_filename(file).change_context_lazy(|| {
            CliError::Context(format!(
                "error inferring site ID from average file {}",
                file.display()
            ))
        })?;

        if data.contains_key(&site_id) {
            return Err(CliError::Custom(format!(
                "Multiple files for site '{site_id}'; this is not supported"
            )))?;
        }

        let ds = netcdf::open(file).change_context_lazy(|| {
            CliError::Context(format!(
                "error opening site average file: {}",
                file.display()
            ))
        })?;

        let mut inner = AllGasAvgData::new();
        for gas in gases {
            let grp = ds
                .group(gas.output_group())
                .change_context_lazy(|| {
                    CliError::Context(format!(
                        "Error getting group '{}' from input file {}",
                        gas.output_group(),
                        file.display()
                    ))
                })?
                .ok_or_else(|| {
                    CliError::Custom(format!(
                        "No group '{}' in input file {}",
                        gas.output_group(),
                        file.display()
                    ))
                })?;
            let this_data = TimeAvgData::read_netcdf(&grp).change_context_lazy(|| {
                CliError::Context(format!(
                    "error reading data from site average file {}",
                    file.display()
                ))
            })?;
            inner.insert(*gas, this_data);
        }

        data.insert(site_id, inner);
    }

    Ok(data)
}
