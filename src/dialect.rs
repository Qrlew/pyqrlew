use pyo3::prelude::*;

#[pyclass]
#[derive(Clone)]
pub enum Dialect {
    PostgresSql,
    MsSql,
}