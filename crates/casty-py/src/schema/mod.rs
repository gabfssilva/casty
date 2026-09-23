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

use casty_core::schema::Reader;
use casty_core::schema::ir::{NodeRef, Tree};
use casty_core::store::Pages;
use pyo3::prelude::*;
use pyo3::types::PyType;

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
    codecs: Vec<compile::Codec>,
    values: values::Values,
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
            codecs: compiled.codecs,
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
        reader.finish()?;
        Ok(value)
    }

    /// Each top level field of a dataclass as a page, and any other value as the single page `"."`.
    pub fn write_pages(slf: &Bound<'_, Self>, value: &Bound<'_, PyAny>) -> failure::Outcome<Pages> {
        let tree = slf.get().tree();
        let mut written = Pages::new();
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
                    let page = Self::write(slf, field.node, &at)
                        .map_err(|failure| failure.under(&field.name))?;
                    written.insert(field.name.clone(), page);
                }
            }
        }
        Ok(written)
    }

    /// The value the pages hold, with the refs in it bound to `node`.
    pub fn read_pages<'py>(
        slf: &Bound<'py, Self>,
        pages: &Pages,
        node: Option<&std::sync::Arc<crate::node::Node>>,
    ) -> failure::Outcome<Bound<'py, PyAny>> {
        load::from_pages(slf, pages, node)
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
    pub fn codec(&self, at: casty_core::schema::CodecRef) -> &compile::Codec {
        &self.codecs[at.0 as usize]
    }

    #[must_use]
    pub fn values(&self) -> &values::Values {
        &self.values
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
