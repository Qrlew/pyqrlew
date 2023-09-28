use pyo3::prelude::*;
use qrlew::{
    ast,
    data_type::DataType,
    relation::{self, Variant},
    differential_privacy::DPRelation,
    protection::PEPRelation};
use serde_json::Value;
use std::{sync::Arc, ops::Deref};
use crate::{error::Result, dataset::Dataset};
use qrlew_sarus::protobuf::{type_, schema, print_to_string};
use std::str;

#[pyclass(unsendable)]
#[derive(Clone)]
pub struct Relation(Arc<relation::Relation>);

impl Deref for Relation {
    type Target = relation::Relation;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Relation {
    pub fn new(relation: Arc<relation::Relation>) -> Self {
        Relation(relation)
    }
}

#[pymethods]
impl Relation {
    #[staticmethod]
    pub fn parse(query: &str, dataset: &Dataset) -> Result<Self> {
        dataset.sql(query)
    }

    pub fn __str__(&self) -> String {
        let relation = self.0.as_ref();
        format!("{}", relation)
    }

    pub fn dot(&self) -> Result<String> {
        let mut out: Vec<u8> = vec![];
        self.0.as_ref().dot(&mut out, &[]).unwrap();
        Ok(String::from_utf8(out).unwrap())
    }

    pub fn schema(&self) -> String {
        (*self.0).schema().to_string()
    }

    pub fn protect<'a>(&'a self, dataset: &'a Dataset, protected_entity: Vec<(&'a str, Vec<(&'a str, &'a str, &'a str)>, &'a str)>) -> Result<Self> {
        let relations = dataset.deref().relations();
        Ok(Relation(Arc::new(self.deref().clone().force_protect_from_field_paths(&relations, protected_entity).into())))
    }

    pub fn dp_compile<'a>(&'a self, dataset: &'a Dataset, protected_entity: Vec<(&'a str, Vec<(&'a str, &'a str, &'a str)>, &'a str)>, epsilon: f64, delta: f64) -> Result<Self> {
        let relations = dataset.deref().relations();
        let pep_relation = self.deref().clone().force_protect_from_field_paths(&relations, protected_entity);
        let dp_relation = pep_relation.dp_compile(epsilon/2., delta/2., epsilon/2., delta/2.)?;
        Ok(Relation(Arc::new(dp_relation.into())))
    }

    pub fn render(&self) -> String {
        let relation = &*(self.0);
        let ast_query: ast::Query = relation.into();
        format!("{}", ast_query)
    }
}
