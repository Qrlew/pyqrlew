use crate::{
    error::{MissingKeysError, Result},
    relation::Relation,
};
use pyo3::{prelude::*, types::{PyDict, PyList}};
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

    pub fn _to_named_tuple(event: &dp_event::DpEvent, py: Python) -> PyObject {
        let named_tuple = PyDict::new(py);
        let fields = PyList::empty(py);
        named_tuple.set_item("module_name", "dp_accounting.dp_event");
        fields.append("module_name");
        match event {
            dp_event::DpEvent::NoOp => {
                named_tuple.set_item("class_name", "NoOpDpEvent");
                fields.append("class_name");
            },
            dp_event::DpEvent::Gaussian { noise_multiplier } => {
                named_tuple.set_item("class_name", "GaussianDpEvent");
                fields.append("class_name");
                named_tuple.set_item("noise_multiplier", noise_multiplier);
                fields.append("noise_multiplier");
            },
            dp_event::DpEvent::Laplace { noise_multiplier } => {
                named_tuple.set_item("class_name", "LaplaceDpEvent");
                fields.append("class_name");
                named_tuple.set_item("noise_multiplier", noise_multiplier);
                fields.append("noise_multiplier");
            },
            dp_event::DpEvent::EpsilonDelta { epsilon, delta } => {
                named_tuple.set_item("class_name", "EpsilonDeltaDpEvent");
                fields.append("class_name");
                named_tuple.set_item("epsilon", epsilon);
                fields.append("epsilon");
                named_tuple.set_item("delta", delta);
                fields.append("delta");
            },
            dp_event::DpEvent::Composed { events } => {
                named_tuple.set_item("class_name", "ComposedDpEvent");
                fields.append("class_name");
                named_tuple.set_item("events", PyList::new(py, events.into_iter().map(|event| DpEvent::_to_named_tuple(event, py))));
                fields.append("events");
            },
            dp_event::DpEvent::PoissonSampled { sampling_probability, event } => {
                named_tuple.set_item("class_name", "PoissonSampledDpEvent");
                fields.append("class_name");
                named_tuple.set_item("sampling_probability", sampling_probability);
                fields.append("sampling_probability");
                named_tuple.set_item("event", DpEvent::_to_named_tuple(event, py));
                fields.append("event");
            },
            dp_event::DpEvent::SampledWithReplacement { source_dataset_size, sample_size, event } => {
                named_tuple.set_item("class_name", "SampledWithReplacementDpEvent");
                fields.append("class_name");
                named_tuple.set_item("source_dataset_size", source_dataset_size);
                fields.append("source_dataset_size");
                named_tuple.set_item("sample_size", sample_size);
                fields.append("sample_size");
                named_tuple.set_item("event", DpEvent::_to_named_tuple(event, py));
                fields.append("event");
            },
            dp_event::DpEvent::SampledWithoutReplacement { source_dataset_size, sample_size, event } => {
                named_tuple.set_item("class_name", "SampledWithoutReplacementDpEvent");
                fields.append("class_name");
                named_tuple.set_item("source_dataset_size", source_dataset_size);
                fields.append("source_dataset_size");
                named_tuple.set_item("sample_size", sample_size);
                fields.append("sample_size");
                named_tuple.set_item("event", DpEvent::_to_named_tuple(event, py));
                fields.append("event");
            },
        }
        named_tuple.set_item("_fields", fields);
        named_tuple.into_py(py)
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
    pub fn __str__(&self) -> String {
        format!("{:?}", self)
    }

    /// Generate a namedtuple-like usable with https://github.com/google/differential-privacy/blob/main/python/dp_accounting/dp_event.py
    pub fn to_named_tuple(&self, py: Python) -> PyObject {
        DpEvent::_to_named_tuple(self, py)
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