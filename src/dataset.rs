use pyo3::prelude::*;
use qrlew::{
    builder::With,
    hierarchy::Hierarchy,
    sql,
    relation,
    dialect_translation::{
        bigquery::BigQueryTranslator, mssql::MsSqlTranslator, postgresql::PostgreSqlTranslator, QueryToRelationTranslator
    },
};
use qrlew_sarus::{
    data_spec,
    protobuf::print_to_string,
};
use std::ops::Deref;
use std::sync::Arc;
use crate::{
    error::Result,
    relation::Relation,
    dialect::Dialect,
};
/// A Dataset is a set of SQL Tables
#[pyclass]
#[derive(Clone)]
pub struct Dataset(data_spec::Dataset);

impl Deref for Dataset {
    type Target = data_spec::Dataset;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Dataset> for data_spec::Dataset {
    fn from(value: Dataset) -> Self {
        value.0
    }
}

#[pymethods]
impl Dataset {
    #[new]
    pub fn new(dataset: &str, schema: &str, size: &str) -> Result<Self> {
        Ok(Dataset(data_spec::Dataset::parse_from_dataset_schema_size(
            dataset, schema, size,
        )?))
    }
    /// The schema of the Dataset
    #[getter]
    pub fn schema(&self) -> Result<String> {
        Ok(print_to_string(self.0.schema())?)
    }

    #[getter]
    pub fn size(&self) -> Option<String> {
        match self.0.size() {
            Some(size_proto) => print_to_string(size_proto).ok(),
            None => None,
        }
    }

    pub fn with_range(&self, schema_name: &str, table_name: &str, field_name: &str, min: f64, max: f64) -> Result<Self> {
        Ok(Dataset(self.0.with_range(schema_name, table_name, field_name, min, max)?))
    }

    pub fn with_possible_values(&self, schema_name: &str, table_name: &str, field_name: &str, possible_values: Vec<String>) -> Result<Self> {
        Ok(Dataset(self.0.with_possible_values(schema_name, table_name, field_name, &possible_values)?))
    }

    pub fn with_constraint(&self, schema_name: &str, table_name: &str, field_name: &str, constraint: Option<&str>) -> Result<Self> {
        Ok(Dataset(self.0.with_constraint(schema_name, table_name, field_name, constraint)?))
    }

    pub fn relations(&self) -> Vec<(Vec<String>, Relation)> {
        self.deref()
            .relations()
            .into_iter()
            .map(|(i, r)| (i, Relation::new(r)))
            .collect()
    }

    pub fn relation(&self, query: &str, dialect: Option<Dialect>) -> Result<Relation> {
        let dialect = dialect.unwrap_or(Dialect::PostgreSql);
        let relations = self.deref().relations();
        match dialect {
            Dialect::PostgreSql => {
                let translator = PostgreSqlTranslator;
                let query = sql::relation::parse_with_dialect(query, translator.dialect())?;
                let query_with_relations = query.with(&relations);
                Ok(Relation::new(Arc::new(relation::Relation::try_from((query_with_relations, translator))?)))
            },
            Dialect::MsSql => {
                let translator = MsSqlTranslator;
                let query = sql::relation::parse_with_dialect(query, translator.dialect())?;
                let query_with_relations = query.with(&relations);
                Ok(Relation::new(Arc::new(relation::Relation::try_from((query_with_relations, translator))?)))
            },
            Dialect::BigQuery => {
                let translator = BigQueryTranslator;
                let query = sql::relation::parse_with_dialect(query, translator.dialect())?;
                let query_with_relations = query.with(&relations);
                Ok(Relation::new(Arc::new(relation::Relation::try_from((query_with_relations, translator))?)))
            },
        }  
    }

    pub fn from_queries(&self, queries: Vec<(Vec<String>, String)>, dialect: Option<Dialect>) -> Result<Self> {
        let relations = self.deref().relations();
        let dialect = dialect.unwrap_or(Dialect::PostgreSql);

        let result_relations: Result<Hierarchy<Arc<relation::Relation>>> = queries
            .iter()
            .map(|(path, query)| {
                match dialect {
                    Dialect::PostgreSql => {
                        let translator = PostgreSqlTranslator;
                        let parsed = sql::relation::parse_with_dialect(query, translator.dialect())?;
                        let query_with_rel = parsed.with(&relations);
                        let rel = relation::Relation::try_from((query_with_rel, translator))?;
                        Ok((path.clone(), Arc::new(rel)))
                    },
                    Dialect::MsSql => {
                        let translator = MsSqlTranslator;
                        let parsed = sql::relation::parse_with_dialect(query, translator.dialect())?;
                        let query_with_rel = parsed.with(&relations);
                        let rel = relation::Relation::try_from((query_with_rel, translator))?;
                        Ok((path.clone(), Arc::new(rel)))
                    },
                    Dialect::BigQuery => {
                        let translator = BigQueryTranslator;
                        let parsed = sql::relation::parse_with_dialect(query, translator.dialect())?;
                        let query_with_rel = parsed.with(&relations);
                        let rel = relation::Relation::try_from((query_with_rel, translator))?;
                        Ok((path.clone(), Arc::new(rel)))
                    }
                }
            })
            .collect();
        let ds: data_spec::Dataset = (&result_relations?).try_into()?;
        Ok(Dataset(ds))
    }

    pub fn __str__(&self) -> String {
        format!("{}", self.0)
    }
}
