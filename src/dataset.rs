use std::ops::Deref;
use qrlew::{builder::With, sql};
use qrlew_sarus::data_spec;
use std::sync::Arc;
use pyo3::{pyclass, pymethods};

use crate::{relation::Relation, error::Result};

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

    pub fn relations(&self) -> Vec<(Vec<String>, Relation)> {
        self.deref().relations()
            .into_iter()
            .map(|(i, r)| (i, Relation::new(r)))
            .collect()
    }

    pub fn sql(&self, query: &str) -> Result<Relation> {
        let query = sql::relation::parse(query)?;
        let relations = self.deref().relations();
        let query_with_relations = query.with(&relations);
        Ok(Relation::new(Arc::new(query_with_relations.try_into()?)))
    }

    pub fn __str__(&self) -> String {
        format!("{}", self.0)
    }
}