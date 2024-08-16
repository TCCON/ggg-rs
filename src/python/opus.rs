use std::path::PathBuf;

use itertools::Itertools;
use pyo3::{exceptions::{PyIOError, PyValueError}, prelude::*, types::PyBytes};
use crate::opus::{self, constants::bruker::BrukerParValue};

#[pyfunction]
pub(super) fn read_igram_header(path: PathBuf) -> PyResult<PyIgramHeader> {
    let header = opus::IgramHeader::read_full_igram_header(&path)
        .map_err(|e| PyIOError::new_err(format!(
            "Error reading interferogram or spectrum at {}: {e}", path.display()
        )))?;
    Ok(PyIgramHeader(header))
}

#[pyclass]
pub(super) struct PyIgramHeader(opus::IgramHeader);

#[pymethods]
impl PyIgramHeader {
    /// Retrieve a parameter value from the interferogram header.
    /// 
    /// ``parameter`` is the 3-character identifier for that parameter. This returns any
    /// byte values as byte strings, including null bytes and subsequent bytes. For a more
    /// readable version, prefer ``get_value``.
    /// 
    /// Like ``get_value``, parameters present in more than one of the primary interferogram,
    /// secondary interferogram, primary spectrum, and secondary spectrum blocks can be disambiguated
    /// by appending '1' or '2' and 'I'/'i'/'S'/'s' to the parameter name, see ``get_value`` for
    /// details.
    fn get_value_raw<'py>(&self, py: Python<'py>, parameter: &str) -> PyResult<PyObject> {
        let value = self.0.get_value_any_block(parameter)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        match value {
            BrukerParValue::Enum(v) => Ok(PyBytes::new_bound(py, v.as_slice()).into()),
            BrukerParValue::Float(v) => Ok(v.to_object(py)),
            BrukerParValue::Integer(v) => Ok(v.to_object(py)),
            BrukerParValue::Senum(v) => Ok(PyBytes::new_bound(py, v.as_slice()).into()),
            BrukerParValue::String(v) => Ok(v.to_object(py)),
            BrukerParValue::Unknown(v, _) => Ok(PyBytes::new_bound(py, v.as_slice()).into())
        }
    }

    /// Retrieve a parameter value from the interferogram header.
    /// 
    /// ``parameter`` is the 3-character identifier for that parameter.
    /// 
    /// Parameters that show up in the interferogram and spectrum blocks for both channels can be
    /// differentiated by appending a '1' or '2' and an 'I', 'i', 'S', or 's' to the end of the parameter
    /// name. For example, to get the "NPT" parameter for the primary interferogram, pass "NPT1I" or "NPT1i"
    /// as the parameter name. 'I' and 'i' will give the interferogram, 'S' and 's' the spectrum.
    /// If the parameter is present in both channels for the interferogram or spectrum blocks (but not both),
    /// you only need to include the '1' or '2'.
    /// 
    /// Parameter values that are integers or floats are returned as such; all others are returned as strings.
    /// Some values are stored in the header as bytes; in that case, they will be truncated at the first null
    /// byte. If you need the full value of a byte parameter (including the null byte and any subsequent bytes),
    /// use ``get_value_raw`` instead.
    fn get_value<'py>(&self, py: Python<'py>, parameter: &str) -> PyResult<PyObject> {
        fn decode_bytes<'py>(bytes: &[u8], py: Python<'py>) -> PyObject {
            let v = bytes.into_iter()
                .take_while(|b| **b != 0)
                .map(|b| *b)
                .collect_vec();
            let s = String::from_utf8_lossy(&v);
            s.to_object(py)
        }

        let value = self.0.get_value_any_block(parameter)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        match value {
            BrukerParValue::Enum(v) => Ok(decode_bytes(v, py)),
            BrukerParValue::Float(v) => Ok(v.to_object(py)),
            BrukerParValue::Integer(v) => Ok(v.to_object(py)),
            BrukerParValue::Senum(v) => Ok(decode_bytes(v, py)),
            BrukerParValue::String(v) => Ok(v.to_object(py)),
            BrukerParValue::Unknown(v, _) => Ok(decode_bytes(v, py))
        }
    }
}