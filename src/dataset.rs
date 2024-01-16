use pyo3::{pyclass, pymethods};
use qrlew::{
    builder::With,
    hierarchy::Hierarchy,
    relation,
    sql, dialect_translation::{postgres::PostgresTranslator, QueryToRelationTranslator, mssql::MSSQLTranslator},
};
use qrlew_sarus::{data_spec, protobuf::print_to_string};
use std::ops::Deref;
use std::sync::Arc;

use crate::{error::Result, relation::{Relation, Dialect}};

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

    pub fn relations(&self) -> Vec<(Vec<String>, Relation)> {
        self.deref()
            .relations()
            .into_iter()
            .map(|(i, r)| (i, Relation::new(r)))
            .collect()
    }

    pub fn relation(&self, query: &str, dialect: Option<Dialect>) -> Result<Relation> {
        let dialect = dialect.unwrap_or(Dialect::Postgres);
        let relations = self.deref().relations();
        
        match dialect {
            Dialect::Postgres => {
                let translator = PostgresTranslator;
                let query = sql::relation::parse_with_dialect(query, translator.dialect())?;
                let query_with_relations = query.with(&relations);
                Ok(Relation::new(Arc::new(relation::Relation::try_from((query_with_relations, translator))?)))
            },
            Dialect::Mssql => {
                let translator = MSSQLTranslator;
                let query = sql::relation::parse_with_dialect(query, translator.dialect())?;
                let query_with_relations = query.with(&relations);
                Ok(Relation::new(Arc::new(relation::Relation::try_from((query_with_relations, translator))?)))
            }, 
        
        }  
    }

    pub fn from_queries(&self, queries: Vec<(Vec<String>, String)>, dialect: Option<Dialect>) -> Result<Self> {
        let relations = self.deref().relations();
        let dialect = dialect.unwrap_or(Dialect::Postgres);

        let result_relations: Hierarchy<Arc<relation::Relation>> = queries
            .iter()
            .map(|(path, query)| {
                match dialect {
                    Dialect::Postgres => {
                        let tranlator = PostgresTranslator;
                        let parsed = sql::relation::parse_with_dialect(query, tranlator.dialect())?;
                        let query_with_rel = parsed.with(&relations);
                        let rel = relation::Relation::try_from((query_with_rel, tranlator))?;
                        Ok((path.clone(), Arc::new(rel)))
                    },
                    Dialect::Mssql => {
                        let tranlator = MSSQLTranslator;
                        let parsed = sql::relation::parse_with_dialect(query, tranlator.dialect())?;
                        let query_with_rel = parsed.with(&relations);
                        let rel = relation::Relation::try_from((query_with_rel, tranlator))?;
                        Ok((path.clone(), Arc::new(rel)))
                    }
                }
            })
            .collect::<Result<Hierarchy<Arc<relation::Relation>>>>()?;
        let ds: data_spec::Dataset = (&result_relations).try_into()?;
        Ok(Dataset(ds))
    }

    pub fn __str__(&self) -> String {
        format!("{}", self.0)
    }
}
