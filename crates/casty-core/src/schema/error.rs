//! What the schema refuses, and where.

use core::fmt;

use super::msgpack::Malformed;

/// An annotation the schema does not accept, or a value that does not match the one it compiled.
///
/// The text is what `SchemaError` carries to Python, and it names the path of the field so that a type with a
/// container three levels down says which field is the problem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaError(String);

impl SchemaError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    /// The same message under `path`, which is how a field reports what its type refused.
    #[must_use]
    pub fn at(path: &[&str], message: impl Into<String>) -> Self {
        let message = message.into();
        if path.is_empty() {
            return Self(message);
        }
        Self(format!("{}: {message}", path.join(".")))
    }

    /// `expected X, got Y`, the shape every mismatch takes.
    #[must_use]
    pub fn mismatch(expected: &str, got: &str) -> Self {
        Self(format!("expected {expected}, got {got}"))
    }

    /// The same error one level further in, which is how a walk names the field it was reading.
    #[must_use]
    pub fn under(self, name: &str) -> Self {
        Self(format!("{name}: {}", self.0))
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SchemaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl core::error::Error for SchemaError {}

impl From<Malformed> for SchemaError {
    fn from(malformed: Malformed) -> Self {
        Self(malformed.to_string())
    }
}
