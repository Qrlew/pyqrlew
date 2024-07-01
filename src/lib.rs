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

use std::panic;
use backtrace::Backtrace;

// Set a custom panic hook to print the backtrace
fn set_panic_hook() {
    panic::set_hook(Box::new(|info| {
        eprintln!("panic occurred: {:?}", info);
        let backtrace = Backtrace::new();
        eprintln!("{:?}", backtrace);
    }));
}

/// A Python module implemented in Rust.
#[pymodule]
fn pyqrlew(_py: Python, m: &PyModule) -> PyResult<()> {
    set_panic_hook();
    m.add_class::<Dataset>()?;
    m.add_class::<Relation>()?;
    m.add_class::<Dialect>()?;
    m.add_class::<Strategy>()?;
    m.add_class::<RelationWithDpEvent>()?;
    Ok(())
}
