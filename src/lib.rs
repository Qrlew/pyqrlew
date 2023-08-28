pub mod error;
pub mod dataset;
pub mod relation;

use pyo3::prelude::{pymodule, Python, PyModule, PyResult};
use crate::{
    dataset::Dataset,
    relation::Relation,
};

/// A Python module implemented in Rust.
#[pymodule]
fn pyqrlew(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Dataset>()?;
    m.add_class::<Relation>()?;
    Ok(())
}

