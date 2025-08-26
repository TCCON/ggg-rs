pub(crate) static PROGRAM_NAME: &'static str = "write_public_netcdf";
pub(crate) static TIME_DIM_NAME: &'static str = "time";
pub(crate) static PRIOR_INDEX_VARNAME: &'static str = "prior_index";
pub(crate) static PRIOR_PRESSURE_VARNAME: &'static str = "prior_pressure";
pub(crate) static AK_PRESSURE_VARNAME: &'static str = "ak_pressure";
pub(crate) static DEFAULT_GAS_LONG_NAMES: &'static [(&'static str, &'static str)] = &[
    ("co2", "carbon dioxide"),
    ("ch4", "methane"),
    ("n2o", "nitrous oxide"),
    ("co", "carbon monoxide"),
    ("h2o", "water"),
    ("hdo", "semiheavy water"),
    ("hf", "hydrofluoric acid"),
    ("hcl", "hydrochloric acid"),
    ("o3", "ozone"),
    ("o2", "oxygen"),
];
