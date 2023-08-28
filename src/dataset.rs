
use qrlew::{builder::With, sql};
use qrlew_sarus::data_spec;
use std::rc::Rc;
use pyo3::{pyclass, pymethods};

use crate::{relation::Relation, error};

#[pyclass]
pub struct Dataset(pub data_spec::Dataset);

#[pymethods]
impl Dataset {
    #[new]
    pub fn new(dataset: &str, schema: &str, size: &str) -> error::Result<Self> {
        Ok(Dataset(data_spec::Dataset::parse_from_dataset_schema_size(
            dataset, schema, size,
        )?))
    }

    pub fn relations(&self) -> Vec<(Vec<String>, Relation)> {
        self.0
            .relations()
            .into_iter()
            .map(|(i, r)| (i, Relation(r)))
            .collect()
    }

    pub fn sql(&self, query: &str) -> error::Result<Relation> {
        let query = sql::relation::parse(query)?;
        let relations = self.0.relations();
        let query_with_relations = query.with(&relations);
        Ok(Relation(Rc::new(query_with_relations.try_into()?)))
    }

    pub fn __str__(&self) -> String {
        format!("{}", self.0)
    }

    // fn schema_type_data(&self) -> String {
    //     (self.0).schema_type_data()
    // }
}