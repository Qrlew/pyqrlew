pub mod error;
pub mod dataset;
pub mod relation;

use pyo3::prelude::*;


/// A Python module implemented in Rust.
#[pymodule]
fn pyqrlew(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<dataset::Dataset>()?;
    m.add_class::<relation::Relation>()?;
    Ok(())
}

