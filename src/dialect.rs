use pyo3::prelude::*;

/// SQL Datasets
#[pyclass]
#[derive(Clone)]
pub enum Dialect {
    PostgreSql,
    MsSql,
    BigQuery
}