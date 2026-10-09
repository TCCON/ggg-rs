use std::path::Path;

use error_stack::ResultExt;
use ggg_rs::utils::site_id_from_filename;
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
    let data = load_site_files(average_files, gases)?;
    let gas_date_ranges =
        split_dates::get_date_range_all_gases(&data, gases, start_date, end_date)?;
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
