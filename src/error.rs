use pyo3::exceptions::{PyException, PyRuntimeError};
use pyo3::prelude::*;
use qrlew_sarus::{data_spec, protobuf};
use std::{error, result};
use qrlew::relation;
use qrlew::sql;

/*
Error management
 */

#[derive(Debug)]
pub struct Error(Box<dyn error::Error>);

impl From<Error> for PyErr {
    fn from(err: Error) -> PyErr {
        PyRuntimeError::new_err(err.0.to_string())
    }
}

impl From<protobuf::ParseError> for Error {
    fn from(err: protobuf::ParseError) -> Error {
        Error(Box::new(err))
    }
}
impl From<protobuf::PrintError> for Error {
    fn from(err: protobuf::PrintError) -> Error {
        Error(Box::new(err))
    }
}
impl From<data_spec::Error> for Error {
    fn from(err: data_spec::Error) -> Error {
        Error(Box::new(err))
    }
}
impl From<relation::Error> for Error {
    fn from(err: relation::Error) -> Error {
        Error(Box::new(err))
    }
}
impl From<sql::Error> for Error {
    fn from(err: sql::Error) -> Error {
        Error(Box::new(err))
    }
}

pub type Result<T> = result::Result<T, Error>;