use crate::{dialect::Dialect, error::Result};
use pyo3::prelude::*;
use qrlew::{dialect_translation::{bigquery::BigQueryTranslator, databricks::DatabricksTranslator, hive::HiveTranslator, mssql::MsSqlTranslator, mysql::MySqlTranslator, postgresql::PostgreSqlTranslator, redshiftsql::RedshiftSqlTranslator}, sql};



#[pyfunction]
/// Util to extract the prefix of fully qualified table names from a query.
/// Only base tables are considered. If table names are quoted, the quoting
/// must be coherent with the dialect.
pub fn tables_prefix(query: &str, dialect: Dialect) -> Result<Vec<String>> {
    match dialect {
        Dialect::PostgreSql => Ok(sql::tables_prefix(query, PostgreSqlTranslator)?),
        Dialect::MsSql => Ok(sql::tables_prefix(query, MsSqlTranslator)?),
        Dialect::BigQuery => Ok(sql::tables_prefix(query, BigQueryTranslator)?),
        Dialect::MySql => Ok(sql::tables_prefix(query, MySqlTranslator)?),
        Dialect::Hive => Ok(sql::tables_prefix(query, HiveTranslator)?),
        Dialect::Databricks => Ok(sql::tables_prefix(query, DatabricksTranslator)?),
        Dialect::RedshiftSql => Ok(sql::tables_prefix(query, RedshiftSqlTranslator)?),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_find_tables_name() {
        let query_str = "SELECT * FROM my_db.my_sch.my_tab";
        let tables = tables_prefix(query_str, Dialect::PostgreSql).unwrap();
        assert_eq!(
            tables, vec!["my_db".to_string()]
        )
    }
}