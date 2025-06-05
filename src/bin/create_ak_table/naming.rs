pub(crate) static Z_DIM_NAME: &'static str = "z";
pub(crate) static SZA_DIM_NAME: &'static str = "sza_bin";
pub(crate) static PRES_VAR_NAME: &'static str = "pressure";

pub(crate) fn ak_varname(gas: &str) -> String {
    format!("x{gas}_aks")
}
