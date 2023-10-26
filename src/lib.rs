pub mod dataset;
pub mod error;
pub mod relation;

use crate::{dataset::Dataset, relation::Relation};
use pyo3::prelude::{pymodule, PyModule, PyResult, Python};

/// A Python module implemented in Rust.
#[pymodule]
fn pyqrlew(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Dataset>()?;
    m.add_class::<Relation>()?;
    Ok(())
}
