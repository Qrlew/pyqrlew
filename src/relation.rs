use crate::{
    dataset::Dataset,
    error::{MissingKeysError, Result},
};
use pyo3::prelude::*;
use qrlew::{
    ast,
    data_type::DataType,
    differential_privacy::{self, budget::Budget, private_query},
    expr::Identifier,
    hierarchy::Hierarchy,
    privacy_unit_tracking::PrivacyUnit,
    relation::{self, Variant},
    rewriting::rewriting_rule,
    synthetic_data::{self, SyntheticData},
};
use serde_json::Value;
use std::{collections::HashMap, ops::Deref, str, sync::Arc};

#[pyclass]
#[derive(Clone, Debug)]
pub struct PrivateQuery(Arc<private_query::PrivateQuery>);

impl Deref for PrivateQuery {
    type Target = private_query::PrivateQuery;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PrivateQuery {
    pub fn new(private_query: Arc<private_query::PrivateQuery>) -> Self {
        PrivateQuery(private_query)
    }
}

#[pymethods]
impl PrivateQuery {
    fn __str__(&self) -> String {
        format!("{:?}", self)
    }
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct RelationWithPrivateQuery(Arc<rewriting_rule::RelationWithPrivateQuery>);

impl Deref for RelationWithPrivateQuery {
    type Target = rewriting_rule::RelationWithPrivateQuery;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl RelationWithPrivateQuery {
    pub fn new(relation_with_private_query: Arc<rewriting_rule::RelationWithPrivateQuery>) -> Self {
        RelationWithPrivateQuery(relation_with_private_query)
    }
}

#[pymethods]
impl RelationWithPrivateQuery {
    pub fn __str__(&self) -> String {
        let relation_with_qiery = self.0.as_ref();
        format!(
            "Relation: {}\nPrivateQuery: {}",
            relation_with_qiery.relation(),
            relation_with_qiery.private_query()
        )
    }
    pub fn relation(&self) -> Relation {
        let relation_with_qiery = self.0.as_ref();
        Relation(Arc::new(relation_with_qiery.relation().clone()))
    }

    pub fn private_query(&self) -> PrivateQuery {
        let relation_with_qiery = self.0.as_ref();
        PrivateQuery::new(Arc::new(relation_with_qiery.private_query().clone()))
    }
}

#[pyclass]
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

    pub fn rewrite_as_privacy_unit_preserving<'a>(
        &'a self,
        dataset: &'a Dataset,
        synthetic_data: Vec<(Vec<&'a str>, Vec<&'a str>)>,
        privacy_unit: Vec<(&'a str, Vec<(&'a str, &'a str, &'a str)>, &'a str)>,
        epsilon_delta: HashMap<&'a str, f64>,
    ) -> Result<RelationWithPrivateQuery> {
        let relation = self.deref().clone();
        let relations = dataset.deref().relations();
        let synthetic_data = SyntheticData::new(synthetic_data
            .into_iter()
            .map(|(path, iden)| {
                let iden_as_vec_of_strings: Vec<String> = iden.iter().map(|s| s.to_string()).collect();
                (path, Identifier::from(iden_as_vec_of_strings))
            }).collect());
        let privacy_unit = PrivacyUnit::from(privacy_unit);
        let epsilon = epsilon_delta
            .get("epsilon")
            .ok_or(MissingKeysError("epsion".to_string()))?;
        let delta = epsilon_delta
            .get("delta")
            .ok_or(MissingKeysError("delta".to_string()))?;
        let budget = Budget::new(*epsilon, *delta);
        let relation_with_private_query = relation.rewrite_as_privacy_unit_preserving(
            &relations,
            synthetic_data,
            privacy_unit,
            budget,
        )?;
        Ok(RelationWithPrivateQuery(Arc::new(
            relation_with_private_query,
        )))
    }

    pub fn rewrite_with_differential_privacy<'a>(
        &'a self,
        dataset: &'a Dataset,
        synthetic_data: Vec<(Vec<&'a str>, Vec<&'a str>)>,
        privacy_unit: Vec<(&'a str, Vec<(&'a str, &'a str, &'a str)>, &'a str)>,
        epsilon_delta: HashMap<&'a str, f64>,
    ) -> Result<RelationWithPrivateQuery> {
        let relation = self.deref().clone();
        let relations = dataset.deref().relations();
        let synthetic_data = SyntheticData::new(synthetic_data
            .into_iter()
            .map(|(path, iden)| {
                let iden_as_vec_of_strings: Vec<String> = iden.iter().map(|s| s.to_string()).collect();
                (path, Identifier::from(iden_as_vec_of_strings))
            }).collect());
        let privacy_unit = PrivacyUnit::from(privacy_unit);
        let epsilon = epsilon_delta
            .get("epsilon")
            .ok_or(MissingKeysError("epsion".to_string()))?;
        let delta = epsilon_delta
            .get("delta")
            .ok_or(MissingKeysError("delta".to_string()))?;
        let budget = Budget::new(*epsilon, *delta);
        let relation_with_private_query = relation.rewrite_with_differential_privacy(
            &relations,
            synthetic_data,
            privacy_unit,
            budget,
        )?;
        Ok(RelationWithPrivateQuery(Arc::new(
            relation_with_private_query,
        )))
    }

    pub fn render(&self) -> String {
        let relation = &*(self.0);
        let ast_query: ast::Query = relation.into();
        format!("{}", ast_query)
    }
}
