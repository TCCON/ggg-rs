use pyo3::prelude::*;

mod opus;

/// A Python module implemented in Rust.
#[pymodule]
fn gggrs_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(opus::read_igram_header, m)?)?;
    m.add_class::<opus::PyIgramHeader>()?;
    Ok(())
}
