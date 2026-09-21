//! The Python the compiler needs to read an annotation.
//!
//! Resolved once per compilation and held by the compiler, never by the module: a second interpreter in the same
//! process has its own `typing`, and a handle that outlived the compilation would be a handle to the wrong one.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple, PyType};

/// The modules and sentinels an annotation is read with.
// Nothing but borrowed handles to Python objects, which have no useful debug form.
#[allow(missing_debug_implementations)]
pub struct Introspect<'py> {
    py: Python<'py>,
    get_origin: Bound<'py, PyAny>,
    get_args: Bound<'py, PyAny>,
    get_type_hints: Bound<'py, PyAny>,
    fields: Bound<'py, PyAny>,
    is_dataclass: Bound<'py, PyAny>,
    missing: Bound<'py, PyAny>,
    pub literal: Bound<'py, PyAny>,
    pub never: Bound<'py, PyAny>,
    pub union: Bound<'py, PyAny>,
    pub union_type: Bound<'py, PyAny>,
    pub type_var: Bound<'py, PyType>,
    pub type_alias_type: Bound<'py, PyType>,
    pub mapping: Bound<'py, PyAny>,
    pub datetime: Bound<'py, PyType>,
    pub uuid: Bound<'py, PyType>,
    pub none_type: Bound<'py, PyType>,
    pub bool_type: Bound<'py, PyType>,
    pub int_type: Bound<'py, PyType>,
    pub float_type: Bound<'py, PyType>,
    pub str_type: Bound<'py, PyType>,
    pub bytes_type: Bound<'py, PyType>,
    pub tuple_type: Bound<'py, PyType>,
    pub frozenset_type: Bound<'py, PyType>,
    pub list_type: Bound<'py, PyType>,
    pub dict_type: Bound<'py, PyType>,
    pub set_type: Bound<'py, PyType>,
    pub ellipsis: Bound<'py, PyAny>,
    /// `casty.Ref`, whose argument is the messages a ref takes.
    pub reference: Bound<'py, PyAny>,
    signature: Bound<'py, PyAny>,
}

impl<'py> Introspect<'py> {
    pub fn new(py: Python<'py>, reference: Bound<'py, PyAny>) -> PyResult<Self> {
        let typing = py.import("typing")?;
        let dataclasses = py.import("dataclasses")?;
        let types = py.import("types")?;
        let builtins = py.import("builtins")?;
        Ok(Self {
            py,
            get_origin: typing.getattr("get_origin")?,
            get_args: typing.getattr("get_args")?,
            get_type_hints: typing.getattr("get_type_hints")?,
            fields: dataclasses.getattr("fields")?,
            is_dataclass: dataclasses.getattr("is_dataclass")?,
            missing: dataclasses.getattr("MISSING")?,
            literal: typing.getattr("Literal")?,
            never: typing.getattr("Never")?,
            union: typing.getattr("Union")?,
            union_type: types.getattr("UnionType")?,
            type_var: typing.getattr("TypeVar")?.cast_into()?,
            type_alias_type: typing.getattr("TypeAliasType")?.cast_into()?,
            mapping: py.import("collections.abc")?.getattr("Mapping")?,
            datetime: py.import("datetime")?.getattr("datetime")?.cast_into()?,
            uuid: py.import("uuid")?.getattr("UUID")?.cast_into()?,
            none_type: types.getattr("NoneType")?.cast_into()?,
            bool_type: builtins.getattr("bool")?.cast_into()?,
            int_type: builtins.getattr("int")?.cast_into()?,
            float_type: builtins.getattr("float")?.cast_into()?,
            str_type: builtins.getattr("str")?.cast_into()?,
            bytes_type: builtins.getattr("bytes")?.cast_into()?,
            tuple_type: builtins.getattr("tuple")?.cast_into()?,
            frozenset_type: builtins.getattr("frozenset")?.cast_into()?,
            list_type: builtins.getattr("list")?.cast_into()?,
            dict_type: builtins.getattr("dict")?.cast_into()?,
            set_type: builtins.getattr("set")?.cast_into()?,
            ellipsis: builtins.getattr("Ellipsis")?,
            reference,
            signature: py.import("inspect")?.getattr("signature")?,
        })
    }

    #[must_use]
    pub fn py(&self) -> Python<'py> {
        self.py
    }

    pub fn origin(&self, annotation: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        self.get_origin.call1((annotation,))
    }

    pub fn args(&self, annotation: &Bound<'py, PyAny>) -> PyResult<Vec<Bound<'py, PyAny>>> {
        Ok(self
            .get_args
            .call1((annotation,))?
            .cast_into::<PyTuple>()?
            .iter()
            .collect())
    }

    pub fn hints(&self, class: &Bound<'py, PyType>) -> PyResult<Bound<'py, PyDict>> {
        Ok(self.get_type_hints.call1((class,))?.cast_into()?)
    }

    pub fn is_dataclass(&self, value: &Bound<'py, PyAny>) -> PyResult<bool> {
        self.is_dataclass.call1((value,))?.is_truthy()
    }

    /// The fields of a dataclass, as name and whether it has no default of either kind.
    /// The annotations of the parameters of `body`, with the strings among them evaluated.
    pub fn parameters(&self, body: &Bound<'py, PyAny>) -> PyResult<Vec<Bound<'py, PyAny>>> {
        let named = PyDict::new(self.py);
        named.set_item("eval_str", true)?;
        let signature = self.signature.call((body,), Some(&named))?;
        signature
            .getattr("parameters")?
            .call_method0("values")?
            .try_iter()?
            .map(|parameter| parameter?.getattr("annotation"))
            .collect()
    }

    pub fn dataclass_fields(&self, class: &Bound<'py, PyType>) -> PyResult<Vec<(String, bool)>> {
        let mut found = Vec::new();
        for field in self.fields.call1((class,))?.try_iter()? {
            let field = field?;
            let name = field.getattr("name")?.extract::<String>()?;
            let default = field.getattr("default")?.is(&self.missing);
            let factory = field.getattr("default_factory")?.is(&self.missing);
            found.push((name, default && factory));
        }
        Ok(found)
    }
}
