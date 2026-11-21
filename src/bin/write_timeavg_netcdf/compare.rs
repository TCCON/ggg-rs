use std::{
    collections::HashSet,
    hash::Hash,
    io::{BufWriter, Write},
    path::Path,
};

use error_stack::ResultExt;
use log::info;

use crate::{data_structures::Gas, CliError};

mod report_data_diffs;
mod report_file_diffs;

pub(crate) fn compare_driver(
    prev_dir: &Path,
    new_dir: &Path,
    report_dir: &Path,
    gases: &[Gas],
) -> error_stack::Result<(), CliError> {
    let mut summary_writer = init_summary_file(prev_dir, new_dir, report_dir)
        .change_context_lazy(|| CliError::context("error creating the summary report file"))?;
    info!("Gathering lists of time-averaged files");
    let files = report_file_diffs::list_prev_and_new_files(prev_dir, new_dir)?;
    info!("Checking for new or missing files");
    let file_report = report_dir.join("timeavg_file_comparison.txt");
    report_file_diffs::write_file_report(&files, &file_report, &mut summary_writer)
        .change_context_lazy(|| {
            CliError::Context(format!(
                "error while creating/writing the file comparison report at {}",
                file_report.display()
            ))
        })?;
    info!("Comparing data among the files");
    report_data_diffs::make_comparison(
        prev_dir,
        new_dir,
        report_dir,
        &mut summary_writer,
        &files,
        gases,
    )
    .change_context_lazy(|| {
        CliError::Context(format!(
            "error while creating/writing the data comparison reports under {}",
            report_dir.display()
        ))
    })?;
    Ok(())
}

enum PrevOrNew<T> {
    Both(T),
    PrevOnly(T),
    NewOnly(T),
}

impl<T> PrevOrNew<T> {
    fn as_ref(&self) -> &T {
        match self {
            PrevOrNew::Both(v) => v,
            PrevOrNew::PrevOnly(v) => v,
            PrevOrNew::NewOnly(v) => v,
        }
    }
}

impl PrevOrNew<String> {
    fn as_str(&self) -> &str {
        match self {
            PrevOrNew::Both(s) => s.as_str(),
            PrevOrNew::PrevOnly(s) => s.as_str(),
            PrevOrNew::NewOnly(s) => s.as_str(),
        }
    }
}

fn make_prev_and_new_list<T>(prev_items: HashSet<T>, mut new_items: HashSet<T>) -> Vec<PrevOrNew<T>>
where
    T: Eq + Hash,
{
    let mut out = vec![];
    for item in prev_items {
        if new_items.contains(&item) {
            new_items.remove(&item);
            out.push(PrevOrNew::Both(item));
        } else {
            out.push(PrevOrNew::PrevOnly(item));
        }
    }
    for item in new_items {
        out.push(PrevOrNew::NewOnly(item));
    }
    out
}

fn init_summary_file(
    prev_dir: &Path,
    new_dir: &Path,
    report_dir: &Path,
) -> std::io::Result<BufWriter<std::fs::File>> {
    let summary_report = report_dir.join("timeavg_summary_comparison.txt");
    let mut summary_writer = BufWriter::new(std::fs::File::create(&summary_report)?);
    writeln!(
        summary_writer,
        "Comparison of time-averaged files in between:"
    )?;
    writeln!(summary_writer, "  - {}", prev_dir.display())?;
    writeln!(summary_writer, "  - {}", new_dir.display())?;
    Ok(summary_writer)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::data_structures::Gas::CO2;

    use super::*;
    use ggg_rs::test_utils::{compare_output_text_files, init_test_logging};

    #[test]
    fn test_comparison_end2end() {
        init_test_logging(None);
        // These are DELIBERATELY relative; the summary file includes the prev and new directories
        // given, so they must be relative to work on different machines. NB: may not work on Windows
        // due to forward slash vs. backslash separators (thanks DOS) but will deal with that if
        // testing on Windows becomes necessary.
        let prev_dir = PathBuf::from("test-data/inputs/write-timeavg-netcdf/compare/prev");
        let new_dir = PathBuf::from("test-data/inputs/write-timeavg-netcdf/compare/new");
        let report_dir = PathBuf::from("test-data/outputs/write-timeavg-netcdf/compare");
        let expected_dir = PathBuf::from("test-data/expected/write-timeavg-netcdf/compare");

        compare_driver(&prev_dir, &new_dir, &report_dir, &[CO2]).unwrap();
        compare_output_text_files(&expected_dir, &report_dir, "timeavg_summary_comparison.txt");
        compare_output_text_files(&expected_dir, &report_dir, "timeavg_file_comparison.txt");
        compare_output_text_files(&expected_dir, &report_dir, "co2_comparison.dat");
    }
}
