use pyo3::{pyclass, pymethods};
use qrlew::{
    builder::With,
    hierarchy::Hierarchy,
    relation::{self, Variant as _},
    sql::{self},
};
use qrlew_sarus::{data_spec, protobuf::print_to_string};
use std::ops::Deref;
use std::sync::Arc;

use crate::{error::Result, relation::Relation};

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

    pub fn sql(&self, query: &str) -> Result<Relation> {
        let query = sql::relation::parse(query)?;
        let relations = self.deref().relations();
        let query_with_relations = query.with(&relations);
        Ok(Relation::new(Arc::new(query_with_relations.try_into()?)))
    }

    pub fn from_queries(&self, queries: Vec<(Vec<String>, String)>) -> Result<Self> {
        let relations = self.deref().relations();

        let result_relations: Hierarchy<Arc<relation::Relation>> = queries
            .iter()
            .map(|(path, query)| {
                let parsed = sql::relation::parse(query)?;
                let query_with_rel = parsed.with(&relations);
                let rel = relation::Relation::try_from(query_with_rel)?;
                Ok((path.clone(), Arc::new(rel)))
            })
            .collect::<Result<Hierarchy<Arc<relation::Relation>>>>()?;
        let ds: data_spec::Dataset = (&result_relations).try_into()?;
        Ok(Dataset(ds))
    }

    pub fn __str__(&self) -> String {
        format!("{}", self.0)
    }
}
