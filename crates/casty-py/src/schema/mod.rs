//! The bridge between an annotation and the bytes it travels as.
//!
//! The tree an annotation compiles to lives in `casty-core`; what is here is everything that needs an interpreter:
//! reading the annotation, reading a value, and building one.

pub mod compile;
pub mod dump;
pub mod failure;
pub mod introspect;
pub mod load;
pub mod naming;
pub mod values;

use casty_core::schema::ir::{NodeRef, Tree};
use casty_core::schema::{Reader, SchemaError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyType};

use compile::compile;
use introspect::Introspect;

/// A compiled annotation: what a state, a message or a reply is written from and read into.
///
/// Compiling walks the annotation once. From then on nothing is reflected again: a value is written straight to
/// msgpack and read straight from it, with no structure in between.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
pub struct Schema {
    tree: Tree,
    classes: Vec<Py<PyType>>,
    values: values::Values,
}

#[pymethods]
impl Schema {
    /// Compile `annotation`, in the canonical order when the bytes of the value are compared to other bytes.
    #[new]
    #[pyo3(signature = (annotation, /, *, canonical = false))]
    fn new(py: Python<'_>, annotation: &Bound<'_, PyAny>, canonical: bool) -> PyResult<Self> {
        let introspect = Introspect::new(py, py.get_type::<crate::refs::Ref>().into_any())?;
        Ok(Self::compiled(py, &introspect, annotation, canonical)?)
    }

    /// `value` as the bytes it travels in, tagged when the annotation is a dataclass.
    fn dump<'py>(
        &self,
        py: Python<'py>,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let mut out = Vec::new();
        dump::dump(self, self.tree.sent(), value, &mut out)?;
        Ok(PyBytes::new(py, &out))
    }

    /// Each top level field of a dataclass as a page named after it, and any other value as the single page `"."`.
    fn pages<'py>(
        &self,
        py: Python<'py>,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let written = PyDict::new(py);
        match self.tree.pages() {
            None => {
                let mut out = Vec::new();
                dump::dump(self, self.tree.root(), value, &mut out)?;
                written.set_item(".", PyBytes::new(py, &out))?;
            }
            Some(dataclass) => {
                let class = self.classes[dataclass.class.0 as usize].bind(py);
                if !value.is_instance(class)? {
                    return Err(dump::wrong(&dataclass.qualname, value)?.into());
                }
                for field in &dataclass.fields {
                    let mut out = Vec::new();
                    dump::dump(
                        self,
                        field.node,
                        &value.getattr(field.name.as_str())?,
                        &mut out,
                    )?;
                    written.set_item(field.name.as_str(), PyBytes::new(py, &out))?;
                }
            }
        }
        Ok(written)
    }

    /// The value `data` holds, built as the annotation says.
    #[pyo3(name = "load")]
    fn load_value<'py>(slf: &Bound<'py, Self>, data: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        Ok(Self::read(slf, slf.get().tree.sent(), data, None)?)
    }

    /// The value the pages of a state hold, with a page this version does not know left out.
    fn from_pages<'py>(
        slf: &Bound<'py, Self>,
        pages: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        load::from_pages(slf, pages, None)
    }

    fn __repr__(&self) -> String {
        format!("Schema({} nodes)", self.nodes())
    }
}

impl Schema {
    /// Compile `annotation` with an interpreter already read: the decorator compiles two schemas from one reading.
    pub fn compiled(
        py: Python<'_>,
        introspect: &Introspect<'_>,
        annotation: &Bound<'_, PyAny>,
        canonical: bool,
    ) -> failure::Outcome<Self> {
        let compiled = compile(introspect, annotation, canonical)?;
        Ok(Self {
            tree: compiled.tree,
            classes: compiled.classes,
            values: values::Values::new(py)?,
        })
    }

    /// Compile `annotation` from Rust, which is what the decorator and the core do.
    pub fn of(
        py: Python<'_>,
        annotation: &Bound<'_, PyAny>,
        canonical: bool,
    ) -> failure::Outcome<Self> {
        let introspect = Introspect::new(py, py.get_type::<crate::refs::Ref>().into_any())?;
        Self::compiled(py, &introspect, annotation, canonical)
    }

    /// `value` as the bytes the node `at` writes it in.
    pub fn write(
        slf: &Bound<'_, Self>,
        at: NodeRef,
        value: &Bound<'_, PyAny>,
    ) -> failure::Outcome<Vec<u8>> {
        let mut out = Vec::new();
        dump::dump(slf.get(), at, value, &mut out)?;
        Ok(out)
    }

    /// The value `data` holds under the node `at`, with the refs in it bound to `node`.
    pub fn read<'py>(
        slf: &Bound<'py, Self>,
        at: NodeRef,
        data: &[u8],
        node: Option<&std::sync::Arc<crate::node::Node>>,
    ) -> failure::Outcome<Bound<'py, PyAny>> {
        let mut reader = Reader::new(data);
        let value = load::load(slf, at, &mut reader, node)?;
        reader.finish().map_err(SchemaError::from)?;
        Ok(value)
    }

    /// Each top level field of a dataclass as a page, and any other value as the single page `"."`.
    pub fn write_pages(
        slf: &Bound<'_, Self>,
        value: &Bound<'_, PyAny>,
    ) -> failure::Outcome<casty_core::store::Pages> {
        let tree = slf.get().tree();
        let mut written = casty_core::store::Pages::new();
        match tree.pages() {
            None => {
                written.insert(".".to_owned(), Self::write(slf, tree.root(), value)?);
            }
            Some(dataclass) => {
                let class = slf.get().classes[dataclass.class.0 as usize].bind(value.py());
                if !value.is_instance(class)? {
                    return Err(dump::wrong(&dataclass.qualname, value)?);
                }
                for field in &dataclass.fields {
                    let at = value.getattr(field.name.as_str())?;
                    written.insert(field.name.clone(), Self::write(slf, field.node, &at)?);
                }
            }
        }
        Ok(written)
    }

    /// The value the pages hold, with the refs in it bound to `node`.
    pub fn read_pages<'py>(
        slf: &Bound<'py, Self>,
        pages: &casty_core::store::Pages,
        node: Option<&std::sync::Arc<crate::node::Node>>,
    ) -> failure::Outcome<Bound<'py, PyAny>> {
        let holder = PyDict::new(slf.py());
        for (name, page) in pages {
            holder.set_item(name.as_str(), PyBytes::new(slf.py(), page))?;
        }
        Ok(load::from_pages(slf, holder.as_any(), node)?)
    }

    #[must_use]
    pub fn tree(&self) -> &Tree {
        &self.tree
    }

    #[must_use]
    pub fn class(&self, at: casty_core::schema::ClassRef) -> &Py<PyType> {
        &self.classes[at.0 as usize]
    }

    #[must_use]
    pub fn values(&self) -> &values::Values {
        &self.values
    }

    fn nodes(&self) -> usize {
        self.classes.len()
    }
}

/// The schema of the answer to `ask(build, ...)`, from the annotation of the first parameter of `build`.
pub fn reply_schema(py: Python<'_>, build: &Bound<'_, PyAny>) -> PyResult<Schema> {
    let reference = py.get_type::<crate::refs::Ref>();
    let introspect = Introspect::new(py, reference.clone().into_any())?;
    let shape = || {
        let written = build
            .repr()
            .map_or_else(|_| "the builder".to_owned(), |text| text.to_string());
        pyo3::exceptions::PyTypeError::new_err(format!(
            "the first parameter of {written} must be annotated as Ref[R]"
        ))
    };
    let parameters = introspect.parameters(build)?;
    let Some(first) = parameters.first() else {
        return Err(shape());
    };
    if !introspect.origin(first)?.is(&reference) {
        return Err(shape());
    }
    let args = introspect.args(first)?;
    let Some(answer) = args.first() else {
        return Err(shape());
    };
    Ok(Schema::compiled(py, &introspect, answer, false)?)
}
