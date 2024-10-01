use std::fmt::Debug;

#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum O2DmfError {
    #[error("Could not find O2 DMF for spectrum {specname}: {reason}")]
    SpectrumNotFound{specname: String, reason: String},
}

impl O2DmfError {
    fn spectrum_not_found<S: ToString, R: ToString>(specname: S, reason: R) -> Self {
        Self::SpectrumNotFound { specname: specname.to_string(), reason: reason.to_string() }
    }
}

pub(crate) trait O2DmfProvider: Debug {
    fn o2_dmf(&self, spectrum_name: &str) -> error_stack::Result<f64, O2DmfError>;
}

#[derive(Debug)]
pub(crate) struct FixedO2Dmf {
    o2_dmf: f64
}

impl FixedO2Dmf {
    pub(crate) fn new(o2_dmf: f64) -> Self {
        Self { o2_dmf }
    }
}

impl O2DmfProvider for FixedO2Dmf {
    fn o2_dmf(&self, spectrum_name: &str) -> error_stack::Result<f64, O2DmfError> {
        Ok(self.o2_dmf)
    }
}