//! What a compilation or a walk can end with, and how it reaches Python.

use casty_core::schema::{Malformed, SchemaError};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

#[derive(Debug)]
pub enum Failure {
    /// A type that cannot be serialized, or a value that does not match the type: `SchemaError`.
    Schema(SchemaError),
    /// An annotation that is not of the shape the decorator needs: `TypeError`.
    Shape(String),
    /// Something Python raised while the compiler was reading the annotation.
    Py(PyErr),
}

pub type Outcome<T> = Result<T, Failure>;

impl Failure {
    /// The same failure named under `role`, which is how the decorator says whether it is the state or the message.
    #[must_use]
    pub fn under(self, role: &str) -> Self {
        match self {
            Self::Schema(error) => Self::Schema(error.under(role)),
            other => other,
        }
    }
}

impl From<SchemaError> for Failure {
    fn from(error: SchemaError) -> Self {
        Self::Schema(error)
    }
}

impl From<Malformed> for Failure {
    fn from(malformed: Malformed) -> Self {
        Self::Schema(malformed.into())
    }
}

/// Building a small value cannot fail, and the walks say so with `?` all the same.
impl From<core::convert::Infallible> for Failure {
    fn from(never: core::convert::Infallible) -> Self {
        match never {}
    }
}

impl From<PyErr> for Failure {
    fn from(error: PyErr) -> Self {
        Self::Py(error)
    }
}

impl From<Failure> for PyErr {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Schema(error) => {
                crate::errors::SchemaError::new_err(error.message().to_owned())
            }
            Failure::Shape(message) => PyTypeError::new_err(message),
            Failure::Py(error) => error,
        }
    }
}
