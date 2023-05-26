use std::{error, result, rc::Rc};
use pyo3::prelude::*;
use pyo3::exceptions::{PyException, PyRuntimeError};
use qrlew::{
    builder::With,
    expr,
    relation,
    sql,
    sqlparser::ast,
};
use qrlew_sarus::{
    data_spec,
    protobuf,
};
/*
Error management
 */

struct Error(Box<dyn error::Error>);

impl From<Error> for PyErr {
    fn from(err: Error) -> PyErr {
        PyRuntimeError::new_err(err.0.to_string())
    }
}

impl From<protobuf::ParseError> for Error { fn from(err: protobuf::ParseError) -> Error { Error(Box::new(err)) } }
impl From<protobuf::PrintError> for Error { fn from(err: protobuf::PrintError) -> Error { Error(Box::new(err)) } }
impl From<data_spec::Error> for Error { fn from(err: data_spec::Error) -> Error { Error(Box::new(err)) } }
impl From<relation::Error> for Error { fn from(err: relation::Error) -> Error { Error(Box::new(err)) } }
impl From<sql::Error> for Error { fn from(err: sql::Error) -> Error { Error(Box::new(err)) } }

type Result<T> = result::Result<T, Error>;

/*
Basic classes
 */

#[pyclass]
struct Dataset(data_spec::Dataset);

#[pymethods]
impl Dataset {
    #[new]
    fn new(dataset: &str, schema: &str, size: &str) -> Result<Self> {
        Ok(Dataset(data_spec::Dataset::parse_from_dataset_schema_size(dataset, schema, size)?))
    }

    fn relations(&self) -> Vec<(Vec<String>, Relation)> {
        self.0.relations().into_iter().map(|(i, r)| (i, Relation(r))).collect()
    }

    fn sql(&self, query: &str) -> Result<Relation> {
        let query = sql::relation::parse(query)?;
        let relations = self.0.relations();
        let query_with_relations = query.with(&relations);
        Ok(Relation(Rc::new(query_with_relations.try_into()?)))
    }

    fn __str__(&self) -> String {
        format!("{}", self.0)
    }
}

#[pyclass(unsendable)]
struct Relation(Rc<relation::Relation>);

#[pymethods]
impl Relation {
    fn __str__(&self) -> String {
        let relation = &*(self.0);
        format!("{}", relation)
    }

    fn dot(&self) -> Result<String> {
        Ok((*self.0).dot()?)
    }

    fn render(&self) -> String {
        let relation = &*(self.0);
        let ast_query: ast::Query = relation.into();
        format!("{}", ast_query)

    }

}

/// A Python module implemented in Rust.
#[pymodule]
fn pyqrlew(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Dataset>()?;
    m.add_class::<Relation>()?;
    Ok(())
}
