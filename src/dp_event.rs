use crate::relation::Relation;
use pyo3::{
    exceptions::PyAttributeError,
    prelude::*,
    types::{PyDict, PyList},
};
use qrlew::{differential_privacy::dp_event, rewriting::rewriting_rule};
use std::{ops::Deref, str, sync::Arc};

#[pyclass]
#[derive(Clone, Debug)]
pub struct DpEvent(Arc<dp_event::DpEvent>);

impl DpEvent {
    pub fn new(dp_event: Arc<dp_event::DpEvent>) -> Self {
        DpEvent(dp_event)
    }

    pub fn _to_dict<'py>(event: &dp_event::DpEvent, py: Python<'py>) -> &'py PyDict {
        let dict = PyDict::new(py);
        let fields = PyList::empty(py);
        dict.set_item("module_name", "dp_accounting.dp_event")
            .unwrap();
        fields.append("module_name").unwrap();
        match event {
            dp_event::DpEvent::NoOp => {
                dict.set_item("class_name", "NoOpDpEvent").unwrap();
                fields.append("class_name").unwrap();
            }
            dp_event::DpEvent::Gaussian { noise_multiplier } => {
                dict.set_item("class_name", "GaussianDpEvent").unwrap();
                fields.append("class_name").unwrap();
                dict.set_item("noise_multiplier", noise_multiplier).unwrap();
                fields.append("noise_multiplier").unwrap();
            }
            dp_event::DpEvent::Laplace { noise_multiplier } => {
                dict.set_item("class_name", "LaplaceDpEvent").unwrap();
                fields.append("class_name").unwrap();
                dict.set_item("noise_multiplier", noise_multiplier).unwrap();
                fields.append("noise_multiplier").unwrap();
            }
            dp_event::DpEvent::EpsilonDelta { epsilon, delta } => {
                dict.set_item("class_name", "EpsilonDeltaDpEvent").unwrap();
                fields.append("class_name").unwrap();
                dict.set_item("epsilon", epsilon).unwrap();
                fields.append("epsilon").unwrap();
                dict.set_item("delta", delta).unwrap();
                fields.append("delta").unwrap();
            }
            dp_event::DpEvent::Composed { events } => {
                dict.set_item("class_name", "ComposedDpEvent").unwrap();
                fields.append("class_name").unwrap();
                dict.set_item(
                    "events",
                    PyList::new(
                        py,
                        events.into_iter().map(|event| DpEvent::_to_dict(event, py)),
                    ),
                )
                .unwrap();
                fields.append("events").unwrap();
            }
            dp_event::DpEvent::PoissonSampled {
                sampling_probability,
                event,
            } => {
                dict.set_item("class_name", "PoissonSampledDpEvent")
                    .unwrap();
                fields.append("class_name").unwrap();
                dict.set_item("sampling_probability", sampling_probability)
                    .unwrap();
                fields.append("sampling_probability").unwrap();
                dict.set_item("event", DpEvent::_to_dict(event, py))
                    .unwrap();
                fields.append("event").unwrap();
            }
            dp_event::DpEvent::SampledWithReplacement {
                source_dataset_size,
                sample_size,
                event,
            } => {
                dict.set_item("class_name", "SampledWithReplacementDpEvent")
                    .unwrap();
                fields.append("class_name").unwrap();
                dict.set_item("source_dataset_size", source_dataset_size)
                    .unwrap();
                fields.append("source_dataset_size").unwrap();
                dict.set_item("sample_size", sample_size).unwrap();
                fields.append("sample_size").unwrap();
                dict.set_item("event", DpEvent::_to_dict(event, py))
                    .unwrap();
                fields.append("event").unwrap();
            }
            dp_event::DpEvent::SampledWithoutReplacement {
                source_dataset_size,
                sample_size,
                event,
            } => {
                dict.set_item("class_name", "SampledWitoutReplacement")
                    .unwrap();
                fields.append("class_name").unwrap();
                dict.set_item("source_dataset_size", source_dataset_size)
                    .unwrap();
                fields.append("source_dataset_size").unwrap();
                dict.set_item("sample_size", sample_size).unwrap();
                fields.append("sample_size").unwrap();
                dict.set_item("event", DpEvent::_to_dict(event, py))
                    .unwrap();
                fields.append("event").unwrap();
            }
        }
        dict.set_item("_fields", fields).unwrap();
        dict
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

    /// Generate a dict similar to https://github.com/google/differential-privacy/blob/main/python/dp_accounting/dp_event.py
    pub fn to_dict<'py>(&self, py: Python<'py>) -> &'py PyDict {
        DpEvent::_to_dict(self.deref(), py)
    }

    /// Generate a namedtuple-like usable with https://github.com/google/differential-privacy/blob/main/python/dp_accounting/dp_event.py
    pub fn to_named_tuple<'py>(&self, py: Python<'py>) -> Py<NamedTuple> {
        Py::new(py, NamedTuple::new(self.to_dict(py).into())).unwrap()
    }
}

#[pyclass(name = "_RelationWithDpEvent")]
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

#[pyclass]
#[derive(Clone, Debug)]
pub struct NamedTuple {
    dict: Py<PyDict>,
}

#[pymethods]
impl NamedTuple {
    #[new]
    fn new(dict: Py<PyDict>) -> Self {
        NamedTuple { dict }
    }

    fn __getattribute__<'py>(&'py self, name: &str, py: Python<'py>) -> PyResult<&'py PyAny> {
        self.dict
            .as_ref(py)
            .get_item(name)?
            .ok_or(PyAttributeError::new_err("Unknown attribute: {name}"))
    }
}

#[cfg(test)]
mod tests {
    use super::DpEvent;
    use pyo3::prelude::*;
    use qrlew::differential_privacy::dp_event;
    use std::sync::Arc;

    #[test]
    fn test_to_dict() {
        let gaussian_mechanism = DpEvent::new(Arc::new(dp_event::DpEvent::gaussian(1.5)));
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let gm = Py::new(py, gaussian_mechanism).unwrap();
            pyo3::py_run!(
                py,
                gm,
                r#"
                print(gm)
                print(gm.to_dict())
            "#
            );
        });
    }

    #[test]
    fn test_named_tuple() {
        let gaussian_mechanism = DpEvent::new(Arc::new(dp_event::DpEvent::laplace(1.5)));
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let named_tuple = gaussian_mechanism.to_named_tuple(py);
            pyo3::py_run!(
                py,
                named_tuple,
                r#"
                print(named_tuple._fields)
                print(named_tuple.module_name)
                print(named_tuple.class_name)
                print(named_tuple.noise_multiplier)
            "#
            );
        });
    }
}
