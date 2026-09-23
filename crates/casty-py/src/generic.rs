//! Subscripting the public types, so that `Context[S, M]` and `Ref[M]` read as annotations.

use pyo3::prelude::*;
use pyo3::types::{PyTuple, PyType};

/// `class[item]`, as the `types.GenericAlias` that `typing.get_origin` and `get_args` take apart again.
pub fn alias<'py>(
    class: &Bound<'py, PyType>,
    item: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    aliased(class, &subscript(item))
}

/// `types.GenericAlias(class, args)`.
pub fn aliased<'py>(
    class: &Bound<'py, PyType>,
    args: &[Bound<'py, PyAny>],
) -> PyResult<Bound<'py, PyAny>> {
    let py = class.py();
    py.import("types")?
        .getattr("GenericAlias")?
        .call1((class, PyTuple::new(py, args)?))
}

/// What was written between the brackets, as the one or more annotations it stands for.
#[must_use]
pub fn subscript<'py>(item: &Bound<'py, PyAny>) -> Vec<Bound<'py, PyAny>> {
    match item.cast::<PyTuple>() {
        Ok(tuple) => tuple.iter().collect(),
        Err(_) => vec![item.clone()],
    }
}
