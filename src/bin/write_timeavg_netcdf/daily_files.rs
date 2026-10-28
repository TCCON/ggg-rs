use std::path::Path;

use error_stack::ResultExt;
use ggg_rs::{
    nc_utils::get_string_attr,
    utils::{iter_dates, site_id_from_filename},
};
use indexmap::IndexMap;

use crate::{
    data_structures::{AllGasAvgData, AllSitesMetaAttrs, Gas, TimeAvgData},
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
    let all_metadata = AllSitesMetaAttrs::from_site_files(average_files, gases)?;
    let all_history = load_site_files_history(average_files)?;
    let gas_date_ranges =
        split_dates::get_date_range_all_gases(&all_data, gases, start_date, end_date)?;
    log::debug!(
        "Will process {} to {}",
        gas_date_ranges.overall_start,
        gas_date_ranges.overall_end
    );
    for date in iter_dates(gas_date_ranges.overall_start, gas_date_ranges.overall_end) {
        log::info!("Processing date {date}");
        let filename = date.format("tccon_timeavg_%Y%m%d.nc4").to_string();
        let file = output_dir.join(filename);
        let mut ds = netcdf::create(&file).change_context_lazy(|| {
            CliError::Context(format!("error creating daily file {}", file.display()))
        })?;

        add_global_dims(&mut ds)?;
        let mut pub_after: Option<chrono::NaiveDate> = None;

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

            let day_allpub_date = split_dates::extract_allpub_date(&day_data, &all_metadata)?;
            match (pub_after, day_allpub_date) {
                (None, None) => pub_after = None,
                (None, Some(d)) => pub_after = Some(d),
                (Some(d), None) => pub_after = Some(d),
                (Some(d1), Some(d2)) => pub_after = Some(d1.max(d2)),
            }

            let mut grp = ds.add_group(gas.output_group()).change_context_lazy(|| {
                CliError::Context(format!(
                    "error creating group '{}' in daily file {}",
                    gas.output_group(),
                    file.display()
                ))
            })?;
            day_data
                .write_netcdf_context(&mut grp, &all_metadata)
                .change_context_lazy(|| {
                    CliError::Context(format!("error writing data for gas {gas} on {date}"))
                })?;
        }

        add_global_attrs(&mut ds, pub_after, &all_history).change_context_lazy(|| {
            CliError::context("error occurred while adding the global attributes")
        })?;
    }

    Ok(())
}

fn add_global_dims(ds: &mut netcdf::FileMut) -> error_stack::Result<(), CliError> {
    // Write the idate dimension to the root
    ds.add_dimension("idate", 6).change_context_lazy(|| {
        CliError::context("error adding idate dimension to the root group")
    })?;
    Ok(())
}

fn add_global_attrs(
    ds: &mut netcdf::FileMut,
    all_public_date: Option<chrono::NaiveDate>,
    site_file_history: &[String],
) -> error_stack::Result<(), CliError> {
    let data_access_text = if all_public_date.is_none() {
        "All TCCON data used to produce this file were public at the time of its creation."
    } else {
        "Some TCCON data used to produce this file were PRIVATE at the time of its creation. \
        If your work using this data made use of it before the date given by the data_public_after attribute, \
        you have a particular obligation to offer the TCCON sites coauthorship on manuscripts or presentations \
        resulting from that work."
    };

    ds.add_attribute("data_public_accessibility", data_access_text)
        .change_context_lazy(|| {
            CliError::context("error adding the 'data_public_accessibility' attribute")
        })?;
    ds.add_attribute("contains_private_data", all_public_date.is_some() as i8)
        .change_context_lazy(|| {
            CliError::context("error adding the 'contains_private_data' attribute")
        })?;
    let all_pub_str = all_public_date
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "N/A".to_string());
    ds.add_attribute("data_public_after", all_pub_str)
        .change_context_lazy(|| {
            CliError::context("error adding the 'data_public_after' attribute")
        })?;

    ds.add_attribute("title", "Time-averaged TCCON data for model comparison")
        .change_context_lazy(|| CliError::context("error adding the 'title' attribute"))?;
    ds.add_attribute("institution", "California Institute of Technology")
        .change_context_lazy(|| CliError::context("error adding the 'institution' attribute"))?;
    ds.add_attribute("institution", "California Institute of Technology")
        .change_context_lazy(|| CliError::context("error adding the 'institution' attribute"))?;
    ds.add_attribute("source", "Total Carbon Column Observing Network (TCCON)")
        .change_context_lazy(|| CliError::context("error adding the 'source' attribute"))?;

    let refs = "Laughner, J. L., Toon, G. C., Mendonca, J., Petri, C., Roche, S., Wunch, D., Blavier, J.-F., Griffith, D. W. T., \
        Heikkinen, P., Keeling, R. F., Kiel, M., Kivi, R., Roehl, C. M., Stephens, B. B., Baier, B. C., Chen, H., Choi, Y., Deutscher, N. M., \
        DiGangi, J. P., Gross, J., Herkommer, B., Jeseck, P., Laemmel, T., Lan, X., McGee, E., McKain, K., Miller, J., Morino, I., Notholt, J., \
        Ohyama, H., Pollard, D. F., Rettinger, M., Riris, H., Rousogenous, C., Sha, M. K., Shiomi, K., Strong, K., Sussmann, R., Té, Y., Velazco, V. A., \
        Wofsy, S. C., Zhou, M., and Wennberg, P. O.: \
        The Total Carbon Column Observing Network\'s GGG2020 data version, Earth Syst. Sci. Data, 16, 2197-2260, https://doi.org/10.5194/essd-16-2197-2024, 2024.\n\
        Wunch, D., G. C. Toon, J.-F. L. Blavier, R. A. Washenfelder, J. Notholt, B. J. Connor, D. W. T. Griffith, V. Sherlock, and P. O. Wennberg: \
        The Total Carbon Column Observing Network, Philos. Trans. R. Soc. A Math. Phys. Eng. Sci., 369 (1943), 2087-2112, doi:10.1098/rsta.2010.0240, 2011.";
    ds.add_attribute("references", refs)
        .change_context_lazy(|| CliError::context("error adding the 'references' attribute"))?;
    ds.add_attribute(
        "comment",
        "TCCON data use policy: https://tccon-wiki.caltech.edu/Main/DataUseGuidelines",
    )
    .change_context_lazy(|| CliError::context("error adding the 'comment' attribute"))?;

    let create_time = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%z").to_string();
    let output_file = ds.path().change_context_lazy(|| {
        CliError::custom("unable to get file path of output file for history")
    })?;
    let mut history = site_file_history.join("\n");
    history.push_str(&format!(
        "\n{}: Daily file {} created by write_timeavg_netcdf from v{} of ggg-rs",
        create_time,
        output_file
            .file_name()
            .map(|n| n.to_string_lossy())
            .unwrap_or_else(|| "?".to_string().into()),
        clap::crate_version!()
    ));
    ds.add_attribute("history", history)
        .change_context_lazy(|| CliError::context("error adding the 'history' attribute"))?;
    ds.add_attribute("creation_time", create_time)
        .change_context_lazy(|| CliError::context("error adding the 'creation_time' attribute"))?;

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

fn load_site_files_history<F: AsRef<Path>>(
    files: &[F],
) -> error_stack::Result<Vec<String>, CliError> {
    let mut history = vec![];
    for file in files {
        let ds = netcdf::open(file).change_context_lazy(|| {
            CliError::Context(format!(
                "error opening {} to read the history",
                file.as_ref().display()
            ))
        })?;

        let this_history = get_string_attr(&ds, "history").change_context_lazy(|| {
            CliError::Context(format!(
                "error reading history attribute from {}",
                file.as_ref().display()
            ))
        })?;

        history.push(this_history);
    }
    Ok(history)
}

#[cfg(test)]
mod tests {
    use crate::common_test::{check_timeavg_files, CheckTimeAvgFields};

    use super::*;
    use ggg_rs::test_utils::{init_test_logging, utc_dt_ymd};

    #[test]
    fn test_daily_files_end2end() {
        init_test_logging(None);
        let test_dir = ggg_rs::test_utils::test_data_dir();

        // TODO: update to checked in input files once tests are working
        let site_files = [
            test_dir.join("inputs/write-timeavg-netcdf/daily/pa.private.qc.avg30min.nc"),
            test_dir.join("inputs/write-timeavg-netcdf/daily/ci.private.qc.avg30min.nc"),
        ];
        let expected_dir = test_dir.join("expected/write-timeavg-netcdf/daily");
        let output_dir = test_dir.join("outputs/write-timeavg-netcdf/daily");

        // We want to test at least two days to check that the data gets split across them appropriately.
        let start = utc_dt_ymd(2025, 6, 4).date_naive();
        let end = utc_dt_ymd(2025, 6, 5).date_naive();
        daily_files_driver(
            &site_files,
            &output_dir,
            &[Gas::CO2],
            Some(start),
            Some(end),
        )
        .unwrap();

        let field_to_var = CheckTimeAvgFields::default();
        let nccomp_def = nccompare::CompElements {
            no_values: true,
            no_shapes: true,
            no_att_values: false,
        };
        for date in iter_dates(start, end) {
            let fname = format!("tccon_timeavg_{}.nc4", date.format("%Y%m%d"));
            let expected_file = expected_dir.join(&fname);
            let test_file = output_dir.join(&fname);
            check_timeavg_files(&expected_file, &test_file, Gas::CO2, &field_to_var);

            // Use nccompare to check that all attributes and variables are present.
            // For now, we're handling value checks separately for more control over
            // required precision.
            let att_check = nccompare::compare_files(
                &expected_file,
                &test_file,
                nccompare::DetailLevel::Medium,
                &nccomp_def,
                false,
                true,
            )
            .unwrap();
            assert!(
                att_check,
                "Attribute or simple variable presence mismatch between {} and {}",
                expected_file.display(),
                test_file.display()
            );
        }
    }
}
