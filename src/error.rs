use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::PyErr;
use qrlew::{differential_privacy, relation, rewriting, sql};
use qrlew_sarus::{data_spec, protobuf};
use std::{error, fmt, result};

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

impl From<differential_privacy::Error> for Error {
    fn from(err: differential_privacy::Error) -> Error {
        Error(Box::new(err))
    }
}

impl From<rewriting::Error> for Error {
    fn from(err: rewriting::Error) -> Error {
        Error(Box::new(err))
    }
}

#[derive(Debug, Clone)]
pub struct MissingKeyError(pub String);

impl error::Error for MissingKeyError {}

impl From<MissingKeyError> for Error {
    fn from(err: MissingKeyError) -> Error {
        Error(Box::new(err))
    }
}

impl fmt::Display for MissingKeyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Missing {} key", self.0)
    }
}

pub type Result<T> = result::Result<T, Error>;
