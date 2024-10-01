use pyo3::prelude::*;

/// An Enum with supported SQL Datasets
#[pyclass]
#[derive(Clone)]
pub enum Dialect {
    PostgreSql,
    MsSql,
    BigQuery,
    MySql,
    Hive,
    Databricks,
    RedshiftSql,
}