use std::{collections::HashSet, io::Write, path::Path};

use error_stack::ResultExt;
use itertools::Itertools;

use super::{make_prev_and_new_list, PrevOrNew};
use crate::CliError;

pub(super) fn list_prev_and_new_files(
    prev_dir: &Path,
    new_dir: &Path,
) -> error_stack::Result<Vec<PrevOrNew<String>>, CliError> {
    let prev_files = list_timeavg_files(prev_dir)?;
    let new_files = list_timeavg_files(new_dir)?;

    let mut out = make_prev_and_new_list(prev_files, new_files);
    out.sort_unstable_by(|a, b| a.as_str().cmp(b.as_str()));
    Ok(out)
}

fn list_timeavg_files(dir: &Path) -> error_stack::Result<HashSet<String>, CliError> {
    let files: Vec<String> = std::fs::read_dir(dir)
        .change_context_lazy(|| {
            CliError::Context(format!("error listing files in {}", dir.display()))
        })?
        .into_iter()
        .filter_map(|res| {
            let name_res =
                res.and_then(|entry| Ok(entry.file_name().to_string_lossy().to_string()));
            if name_res
                .as_deref()
                .is_ok_and(|n| n.starts_with("tccon_timeavg_") && n.ends_with(".nc4"))
            {
                Some(name_res)
            } else if name_res.is_err() {
                Some(name_res)
            } else {
                None
            }
        })
        .try_collect()
        .change_context_lazy(|| {
            CliError::Context(format!("error getting file names in {}", dir.display()))
        })?;
    Ok(HashSet::from_iter(files))
}

pub(super) fn write_file_report<W: std::io::Write>(
    files: &[PrevOrNew<String>],
    report_file: &Path,
    mut summary_writer: W,
) -> std::io::Result<()> {
    let rpt = std::fs::File::create(report_file)?;
    let mut rpt = std::io::BufWriter::new(rpt);

    let n_both = files
        .iter()
        .fold(0, |n, f| if let PrevOrNew::Both(_) = f { n + 1 } else { n });

    let n_prev = files.iter().fold(0, |n, f| {
        if let PrevOrNew::PrevOnly(_) = f {
            n + 1
        } else {
            n
        }
    });

    let n_new = files.iter().fold(0, |n, f| {
        if let PrevOrNew::NewOnly(_) = f {
            n + 1
        } else {
            n
        }
    });

    writeln!(summary_writer, "\n==== FILES ====\n")?;
    writeln!(summary_writer, "{n_both} files present in both directories")?;
    writeln!(
        summary_writer,
        "{n_prev} files present only in the PREVIOUS directory"
    )?;
    writeln!(
        summary_writer,
        "{n_new} files present only in the NEW directory"
    )?;

    writeln!(rpt, "\n==== PREVIOUS ONLY ====\n")?;
    for f in files.iter() {
        if let PrevOrNew::PrevOnly(name) = f {
            writeln!(rpt, "- {name}")?;
        }
    }
    writeln!(rpt, "\n==== NEW ONLY ====\n")?;
    for f in files.iter() {
        if let PrevOrNew::NewOnly(name) = f {
            writeln!(rpt, "+ {name}")?;
        }
    }
    writeln!(rpt, "\n==== IN BOTH ====\n")?;
    for f in files.iter() {
        if let PrevOrNew::Both(name) = f {
            writeln!(rpt, "o {name}")?;
        }
    }
    Ok(())
}
