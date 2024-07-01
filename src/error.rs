use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::PyErr;
use qrlew::{differential_privacy, relation, rewriting, sql};
use qrlew_sarus::{data_spec, protobuf};
use std::{error, fmt};

#[derive(Debug)]
pub enum Error {
    ProtobufParse(protobuf::ParseError),
    ProtobufPrint(protobuf::PrintError),
    DataSpec(data_spec::Error),
    Relation(relation::Error),
    Sql(sql::Error),
    DifferentialPrivacy(differential_privacy::Error),
    Rewriting(rewriting::Error),
    MissingKey(MissingKeyError),
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::ProtobufParse(err) => write!(f, "Protobuf parse error: {}", err),
            Error::ProtobufPrint(err) => write!(f, "Protobuf print error: {}", err),
            Error::DataSpec(err) => write!(f, "Data spec error: {}", err),
            Error::Relation(err) => write!(f, "Relation error: {}", err),
            Error::Sql(err) => write!(f, "SQL error: {}", err),
            Error::DifferentialPrivacy(err) => write!(f, "Differential privacy error: {}", err),
            Error::Rewriting(err) => write!(f, "Rewriting error: {}", err),
            Error::MissingKey(err) => write!(f, "Missing key error: {}", err),
            Error::Other(err) => write!(f, "Other error: {}", err),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Error::ProtobufParse(err) => Some(err),
            Error::ProtobufPrint(err) => Some(err),
            Error::DataSpec(err) => Some(err),
            Error::Relation(err) => Some(err),
            Error::Sql(err) => Some(err),
            Error::DifferentialPrivacy(err) => Some(err),
            Error::Rewriting(err) => Some(err),
            Error::MissingKey(err) => Some(err),
            Error::Other(_) => None,
        }
    }
}

impl From<Error> for PyErr {
    fn from(err: Error) -> PyErr {
        PyRuntimeError::new_err(err.to_string())
    }
}

impl From<protobuf::ParseError> for Error {
    fn from(err: protobuf::ParseError) -> Error {
        Error::ProtobufParse(err)
    }
}

impl From<protobuf::PrintError> for Error {
    fn from(err: protobuf::PrintError) -> Error {
        Error::ProtobufPrint(err)
    }
}

impl From<data_spec::Error> for Error {
    fn from(err: data_spec::Error) -> Error {
        Error::DataSpec(err)
    }
}

impl From<relation::Error> for Error {
    fn from(err: relation::Error) -> Error {
        Error::Relation(err)
    }
}

impl From<sql::Error> for Error {
    fn from(err: sql::Error) -> Error {
        Error::Sql(err)
    }
}

impl From<differential_privacy::Error> for Error {
    fn from(err: differential_privacy::Error) -> Error {
        Error::DifferentialPrivacy(err)
    }
}

impl From<rewriting::Error> for Error {
    fn from(err: rewriting::Error) -> Error {
        Error::Rewriting(err)
    }
}
#[derive(Debug, Clone)]
pub struct MissingKeyError(pub String);

impl fmt::Display for MissingKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Missing {} key", self.0)
    }
}

impl error::Error for MissingKeyError {}

impl From<MissingKeyError> for Error {
    fn from(err: MissingKeyError) -> Error {
        Error::MissingKey(err)
    }
}

pub type Result<T> = std::result::Result<T, Error>;