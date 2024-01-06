use crate::{
    error::{MissingKeysError, Result},
    relation::Relation,
};
use pyo3::prelude::*;
use qrlew::{
    differential_privacy::dp_event,
    rewriting::rewriting_rule,
};
use std::{ops::Deref, str, sync::Arc};

#[pyclass]
#[derive(Clone, Debug)]
pub struct DpEvent(Arc<dp_event::DpEvent>);

impl DpEvent {
    pub fn new(dp_event: Arc<dp_event::DpEvent>) -> Self {
        DpEvent(dp_event)
    }
}

impl Deref for DpEvent {
    type Target = dp_event::DpEvent;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[pymethods]
impl DpEvent {
    fn __str__(&self) -> String {
        format!("{:?}", self)
    }
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct RelationWithDpEvent(Arc<rewriting_rule::RelationWithDpEvent>);

impl RelationWithDpEvent {
    pub fn new(relation_with_dp_event: Arc<rewriting_rule::RelationWithDpEvent>) -> Self {
        RelationWithDpEvent(relation_with_dp_event)
    }
}

impl Deref for RelationWithDpEvent {
    type Target = rewriting_rule::RelationWithDpEvent;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[pymethods]
impl RelationWithDpEvent {
    pub fn __str__(&self) -> String {
        let relation_with_qiery = self.0.as_ref();
        format!(
            "Relation: {}\nDpEvent: {}",
            relation_with_qiery.relation(),
            relation_with_qiery.dp_event()
        )
    }
    pub fn relation(&self) -> Relation {
        let relation_with_qiery = self.0.as_ref();
        Relation::new(Arc::new(relation_with_qiery.relation().clone()))
    }

    pub fn dp_event(&self) -> DpEvent {
        let relation_with_qiery = self.0.as_ref();
        DpEvent::new(Arc::new(relation_with_qiery.dp_event().clone()))
    }
}