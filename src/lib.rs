pub mod dataset;
pub mod error;
pub mod relation;
pub mod dialect;
pub mod dp_event;

pub use crate::{
    dataset::Dataset,
    relation::{Relation, Strategy},
    dialect::Dialect,
};
use pyo3::prelude::*;

/// A Python module implemented in Rust.
#[pymodule]
fn pyqrlew(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Dataset>()?;
    m.add_class::<Relation>()?;
    m.add_class::<Dialect>()?;
    m.add_class::<Strategy>()?;
    Ok(())
}
