pub mod dataset;
pub mod dialect;
pub mod dp_event;
pub mod error;
pub mod relation;

pub use crate::{
    dataset::Dataset,
    dialect::Dialect,
    relation::{Relation, Strategy},
};
use dp_event::RelationWithDpEvent;
use pyo3::prelude::*;

/// A Python module implemented in Rust.
#[pymodule]
fn pyqrlew(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Dataset>()?;
    m.add_class::<Relation>()?;
    m.add_class::<Dialect>()?;
    m.add_class::<Strategy>()?;
    m.add_class::<RelationWithDpEvent>()?;
    Ok(())
}
