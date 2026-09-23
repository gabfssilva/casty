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
    pub annotated: Bound<'py, PyAny>,
    /// `casty.Opaque`, the metadata of an `Annotated` that makes a value travel as the bytes of the caller's functions.
    pub opaque: Bound<'py, PyType>,
    pub never: Bound<'py, PyAny>,
    pub union: Bound<'py, PyAny>,
    pub union_type: Bound<'py, PyAny>,
    pub type_var: Bound<'py, PyType>,
    pub type_alias_type: Bound<'py, PyType>,
    pub mapping: Bound<'py, PyAny>,
    pub datetime: Bound<'py, PyType>,
    pub date: Bound<'py, PyType>,
    pub time: Bound<'py, PyType>,
    pub timedelta: Bound<'py, PyType>,
    pub decimal: Bound<'py, PyType>,
    pub uuid: Bound<'py, PyType>,
    /// `PurePosixPath`, `PureWindowsPath` and `Path`.
    pub paths: [Bound<'py, PyType>; 3],
    pub enumeration: Bound<'py, PyType>,
    pub flag: Bound<'py, PyType>,
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
        let moments = py.import("datetime")?;
        let enumerations = py.import("enum")?;
        let pathlib = py.import("pathlib")?;
        Ok(Self {
            py,
            get_origin: typing.getattr("get_origin")?,
            get_args: typing.getattr("get_args")?,
            get_type_hints: typing.getattr("get_type_hints")?,
            fields: dataclasses.getattr("fields")?,
            is_dataclass: dataclasses.getattr("is_dataclass")?,
            missing: dataclasses.getattr("MISSING")?,
            literal: typing.getattr("Literal")?,
            annotated: typing.getattr("Annotated")?,
            opaque: py.import("casty")?.getattr("Opaque")?.cast_into()?,
            never: typing.getattr("Never")?,
            union: typing.getattr("Union")?,
            union_type: types.getattr("UnionType")?,
            type_var: typing.getattr("TypeVar")?.cast_into()?,
            type_alias_type: typing.getattr("TypeAliasType")?.cast_into()?,
            mapping: py.import("collections.abc")?.getattr("Mapping")?,
            datetime: moments.getattr("datetime")?.cast_into()?,
            date: moments.getattr("date")?.cast_into()?,
            time: moments.getattr("time")?.cast_into()?,
            timedelta: moments.getattr("timedelta")?.cast_into()?,
            decimal: py.import("decimal")?.getattr("Decimal")?.cast_into()?,
            uuid: py.import("uuid")?.getattr("UUID")?.cast_into()?,
            paths: [
                pathlib.getattr("PurePosixPath")?.cast_into()?,
                pathlib.getattr("PureWindowsPath")?.cast_into()?,
                pathlib.getattr("Path")?.cast_into()?,
            ],
            enumeration: enumerations.getattr("Enum")?.cast_into()?,
            flag: enumerations.getattr("Flag")?.cast_into()?,
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

    /// The annotations of the fields of `class`, with their `Annotated` metadata kept: it is where `Opaque` is.
    pub fn hints(&self, class: &Bound<'py, PyType>) -> PyResult<Bound<'py, PyDict>> {
        let named = PyDict::new(self.py);
        named.set_item("include_extras", true)?;
        Ok(self
            .get_type_hints
            .call((class,), Some(&named))?
            .cast_into()?)
    }

    pub fn is_dataclass(&self, value: &Bound<'py, PyAny>) -> PyResult<bool> {
        self.is_dataclass.call1((value,))?.is_truthy()
    }

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

    /// The fields of a dataclass, as name and whether it has no default of either kind.
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
