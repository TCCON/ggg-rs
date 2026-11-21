use std::path::Path;

use error_stack::ResultExt;
use indexmap::IndexMap;
use itertools::Itertools;

use crate::{data_structures::AllStationJsonCitation, CliError};

pub(crate) fn release_driver(
    source_dir: &Path,
    dest_dir: &Path,
    revision: Option<String>,
    doi: Option<String>,
) -> error_stack::Result<(), CliError> {
    let revision = if let Some(rev) = revision {
        if !rev.starts_with("R") {
            return Err(CliError::custom("Revision must start with 'R'").into());
        }
        rev
    } else {
        format!("R{}", chrono::Local::now().format("%Y%m%d"))
    };

    let doi = if let Some(doi) = doi {
        if !doi.starts_with("10.") {
            return Err(CliError::custom("DOI must start with '10.'").into());
        }
        doi
    } else {
        format!("10.14291/tccon.ggg2020.timeaverages.{revision}")
    };

    let out_file = dest_dir.join(format!("tccon_timeaverages_{revision}.tgz"));
    create_tarball(source_dir, &out_file, &revision, &doi)?;

    let json_cite_file = source_dir.join("citations.json");
    if json_cite_file.exists() {
        let metadata_file = dest_dir.join(format!("tccon_timeaverages_{revision}_metadata.json"));
        create_metadata_file(&json_cite_file, &metadata_file)?;
    } else {
        log::warn!(
            "JSON citations file ({}) does not exist, cannot create metadata file",
            json_cite_file.display()
        );
    }
    Ok(())
}

/// Create a tarball of the daily netCDF and the citations files.
/// Add attributes to the netCDF files describing the release revision and DOI.
fn create_tarball(
    source_dir: &Path,
    tgz_file: &Path,
    revision: &str,
    doi: &str,
) -> error_stack::Result<(), CliError> {
    let tmp = tempfile::tempdir().change_context_lazy(|| {
        CliError::context("error creating temporary directory for modified netCDF files")
    })?;

    // First we copy all of the daily files and add the time average collection metadata
    let it = std::fs::read_dir(source_dir).change_context_lazy(|| {
        CliError::Context(format!(
            "error listing contents of {} while creating the release",
            source_dir.display()
        ))
    })?;
    for p in it {
        let p = p
            .change_context_lazy(|| CliError::context("error while getting directory entry"))?
            .path();
        let name = p
            .file_name()
            .ok_or_else(|| {
                CliError::Context(format!("failed to get base name of {}", p.display()))
            })?
            .to_string_lossy();
        let tmpfile = tmp.path().join(name.as_ref());
        if name.starts_with("citations") {
            std::fs::copy(&p, &tmpfile).change_context_lazy(|| {
                CliError::Context(format!(
                    "error copying {} to {}",
                    p.display(),
                    tmpfile.display()
                ))
            })?;
        }
        if name.ends_with(".nc4") {
            std::fs::copy(&p, &tmpfile).change_context_lazy(|| {
                CliError::Context(format!(
                    "error copying {} to {}",
                    p.display(),
                    tmpfile.display()
                ))
            })?;
            let mut ds = netcdf::append(&tmpfile).change_context_lazy(|| {
                CliError::Context(format!("error opening {}", tmpfile.display()))
            })?;
            ds.add_attribute("timeaverage_collection_doi", doi)
                .change_context_lazy(|| {
                    CliError::Context(format!(
                        "error adding 'timeaverage_collection_doi' attribute to {}",
                        tmpfile.display()
                    ))
                })?;
            ds.add_attribute("timeaverage_collection_revision", revision)
                .change_context_lazy(|| {
                    CliError::Context(format!(
                        "error adding 'timeaverage_collection_revision' attribute to {}",
                        tmpfile.display()
                    ))
                })?;
        }
    }

    // Now we can actually make the tarball
    let f = std::fs::File::create(tgz_file).change_context_lazy(|| {
        CliError::Context(format!(
            "error creating the tgz file {}",
            tgz_file.display()
        ))
    })?;
    let encoder = flate2::write::GzEncoder::new(f, flate2::Compression::default());
    let mut archive = tar::Builder::new(encoder);
    archive.follow_symlinks(false);
    archive
        .append_dir_all("", tmp.path())
        .change_context_lazy(|| {
            CliError::Context(format!("error writing tgz file {}", tgz_file.display()))
        })?;
    Ok(())
}

/// Create a JSON file with the original TCCON DOI as related identifiers.
/// This should be able to be uploaded to caltechData to populate its metadata fields.
fn create_metadata_file(
    citations_file: &Path,
    json_file: &Path,
) -> error_stack::Result<(), CliError> {
    let f = std::fs::File::open(citations_file).change_context_lazy(|| {
        CliError::Context(format!("error opening {}", citations_file.display()))
    })?;
    let citations: AllStationJsonCitation =
        serde_json::from_reader(f).change_context_lazy(|| {
            CliError::Context(format!("error reading {}", citations_file.display()))
        })?;
    let related_ids = citations
        .citations
        .into_iter()
        .map(|c| RelatedId::new_doi(c.doi))
        .collect_vec();
    let output: IndexMap<&str, Vec<RelatedId>> =
        IndexMap::from_iter([("relatedIdentifiers", related_ids)]);

    let fout = std::fs::File::create(json_file).change_context_lazy(|| {
        CliError::Context(format!("error opening {}", json_file.display()))
    })?;
    serde_json::to_writer_pretty(fout, &output).change_context_lazy(|| {
        CliError::Context(format!("error writing {}", json_file.display()))
    })?;
    Ok(())
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct RelatedId {
    related_identifier: String,
    relation_type: String,
    related_identifier_type: String,
}

impl RelatedId {
    fn new_doi(doi: String) -> Self {
        Self {
            related_identifier: doi,
            relation_type: "HasPart".to_string(),
            related_identifier_type: "DOI".to_string(),
        }
    }
}
