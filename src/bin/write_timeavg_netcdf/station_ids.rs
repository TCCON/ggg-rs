use std::{io::Read, path::Path};

use error_stack::ResultExt;
use indexmap::IndexMap;

use crate::CliError;

/// TOML string mapping between the TCCON site IDs, TCCON names, and MIP station IDs
static DEFAULT_SITE_CONFIG_TOML: &'static str = include_str!("mip_site_mapping.toml");

/// Structure mapping between the TCCON site IDs, TCCON names, and MIP station IDs.
/// Should be parsed from [`DEFAULT_SITE_CONFIG_TOML`] the first time it is needed.
static DEFAULT_SITE_CONFIG: std::sync::OnceLock<MipIdMap> = std::sync::OnceLock::new();

/// Get the MIP station ID for a given TCCON site ID.
///
/// The MIP station ID is an integer, whereas the TCCON site ID is two characters, e.g. "pa".
/// If `custom_cfg_toml` is `Some(_)`, then it will be read and the site to station ID mapping
/// in it takes precedence over the default. That is, if both the custom and default configurations
/// specify a station ID for "pa", the custom one will be used, but if the custom configuration
/// does not specify a mapping for "pa", or no custom configuration is passed, the default
/// mapping is used.
///
/// Returns an error if reading or parsing the custom file fails, or if the given `site_id` does
/// not exist in either configuration.
///
/// # Panics
/// If the default configuration is an invalid TOML string.
pub(crate) fn get_mip_station_id(
    site_id: &str,
    custom_cfg_toml: Option<&Path>,
) -> error_stack::Result<i8, CliError> {
    if let Some(custom_path) = custom_cfg_toml {
        let custom_cfg = MipIdMap::from_file(custom_path).change_context_lazy(|| {
            CliError::context("Error reading the custom MIP station ID configuration")
        })?;
        if let Ok(station_id) = custom_cfg.get_station_id(site_id) {
            return Ok(station_id);
        }
    }

    let std_cfg = DEFAULT_SITE_CONFIG.get_or_init(|| {
        MipIdMap::from_str(DEFAULT_SITE_CONFIG_TOML)
            .expect("Deserializing the standard MIP station ID configuration should succeed")
    });
    Ok(std_cfg.get_station_id(site_id)?)
}

/// A structure representing the TCCON site ID -> MIP station mapping.
#[derive(Debug, serde::Deserialize)]
struct MipIdMap {
    site_mapping: IndexMap<String, MipId>,
}

impl MipIdMap {
    /// Return the MIP station ID for the given site ID.
    ///
    /// Returns an error if `site_id` is not included in the mapping.
    fn get_station_id(&self, site_id: &str) -> Result<i8, CliError> {
        let site_info = self.site_mapping.get(site_id).ok_or_else(|| {
            CliError::Custom(format!("No mapping defined for site ID '{site_id}"))
        })?;
        Ok(site_info.mip_site_id)
    }

    /// Parse the given TOML file into an instance of this struct.
    ///
    /// Returns an error if reading or parsing the file fails.
    fn from_file(file: &Path) -> error_stack::Result<Self, CliError> {
        let mut f = std::fs::File::open(file).change_context_lazy(|| {
            CliError::Context(format!(
                "Error opening MIP station ID file {}",
                file.display()
            ))
        })?;
        let mut buf = String::new();
        f.read_to_string(&mut buf).change_context_lazy(|| {
            CliError::Context(format!(
                "Error reading MIP station ID file {}",
                file.display()
            ))
        })?;
        Self::from_str(&buf)
    }

    /// Parse the given TOML string into an instance of this struct.
    ///
    /// Returns an error if the parsing fails.
    fn from_str(s: &str) -> error_stack::Result<Self, CliError> {
        toml::from_str(s).change_context_lazy(|| {
            CliError::context("Error deserializing the MIP station ID configuration")
        })
    }
}

/// A structure representing identifiers for a single TCCON site.
#[derive(Debug, serde::Deserialize)]
struct MipId {
    /// The long name for the site used by TCCON, e.g., "lauder01" or "parkfalls01".
    /// This must match that used at https://tccondata.org/metadata/siteinfo/.
    #[allow(unused)]
    tccon_name: String,

    /// A long name for the MIP users, e.g., "lauder125" or "parkfalls".
    /// For sites that have only had one instrument, this is usually the
    /// TCCON name without the instrument number suffix. For sites (like
    /// Lauder) that _have_ had multiple instrument, the MIP team prefers
    /// to append a descriptor that only changes if there are significant
    /// difference in the instrument. For example, "lh" maps to "lauder120",
    /// but "ll" and "lr" both map to "lauder125". In the latter case,
    /// since the first and second 125HR at Lauder perform similarly, they
    /// use the same name, while the 120 instrument gets special nomenclature
    /// since it does not have the same performance.
    #[allow(unused)]
    mip_name: String,

    /// An integer used by the MIP teams to identify the station in a daily file.
    /// This should not change across data versions, since it is used in
    /// bin IDs.
    mip_site_id: i8,
}
