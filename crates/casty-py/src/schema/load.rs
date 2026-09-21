//! msgpack straight into Python values, without a structure in between.
//!
//! Nothing is built that the schema did not ask for. A field this version of a type does not know is stepped over in
//! the bytes, and a field it knows is built once, in place.

use std::sync::Arc;

use casty_core::node::{NodeId, Target};
use casty_core::schema::ir::{Container, Dataclass, Literal, Native, Node, NodeRef};
use casty_core::schema::{Int, Kind, Reader, SchemaError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFrozenSet, PyString, PyTuple};

use super::Schema;
use super::failure::{Failure, Outcome};
use crate::node::Node as Host;
use crate::refs::Ref;

/// Read one value as `at` says, and give back what it built.
pub fn load<'py>(
    schema: &Bound<'py, Schema>,
    at: NodeRef,
    reader: &mut Reader<'_>,
    node: Option<&Arc<Host>>,
) -> Outcome<Bound<'py, PyAny>> {
    Loader { schema, node }.read(at, reader)
}

/// The value the pages of a state hold: each top level field from the page named after it.
pub fn from_pages<'py>(
    schema: &Bound<'py, Schema>,
    pages: &Bound<'py, PyAny>,
    node: Option<&Arc<Host>>,
) -> PyResult<Bound<'py, PyAny>> {
    let loader = Loader { schema, node };
    let tree = schema.get().tree();
    let Some(dataclass) = tree.pages() else {
        let Some(page) = pages.get_item(".").ok() else {
            return Err(Failure::Schema(SchemaError::new("missing page '.'")).into());
        };
        let raw: Vec<u8> = page.extract()?;
        let mut reader = Reader::new(&raw);
        let value = loader.read(tree.root(), &mut reader)?;
        reader
            .finish()
            .map_err(SchemaError::from)
            .map_err(Failure::from)?;
        return Ok(value);
    };
    let mut found: Vec<Option<Bound<'py, PyAny>>> = vec![None; dataclass.fields.len()];
    for (at, field) in dataclass.fields.iter().enumerate() {
        let Ok(page) = pages.get_item(field.name.as_str()) else {
            continue;
        };
        let raw: Vec<u8> = page.extract()?;
        let mut reader = Reader::new(&raw);
        found[at] = Some(loader.read(field.node, &mut reader)?);
        reader
            .finish()
            .map_err(SchemaError::from)
            .map_err(Failure::from)?;
    }
    Ok(loader.build(dataclass, found)?)
}

struct Loader<'a, 'py> {
    schema: &'a Bound<'py, Schema>,
    node: Option<&'a Arc<Host>>,
}

impl<'py> Loader<'_, 'py> {
    fn tree(&self) -> &casty_core::schema::ir::Tree {
        self.schema.get().tree()
    }

    fn read(&self, at: NodeRef, reader: &mut Reader<'_>) -> Outcome<Bound<'py, PyAny>> {
        let py = self.schema.py();
        let kind = reader.kind().map_err(SchemaError::from)?;
        Ok(match self.tree().node(at) {
            Node::Never => {
                return Err(SchemaError::new(format!(
                    "no value is expected, got {}",
                    kind.python()
                ))
                .into());
            }
            Node::Native(native) => self.native(*native, kind, reader)?,
            Node::Literal(values) => self.literal(values, kind, reader)?,
            Node::Datetime => self.datetime(kind, reader)?,
            Node::Uuid => {
                let Kind::Bytes = kind else {
                    return Err(wrong("UUID", kind));
                };
                let raw = reader.read_bin().map_err(SchemaError::from)?;
                if raw.len() != 16 {
                    return Err(wrong("UUID", kind));
                }
                let named = PyDict::new(py);
                named.set_item("bytes", PyBytes::new(py, raw))?;
                self.schema
                    .get()
                    .values()
                    .uuid
                    .bind(py)
                    .call((), Some(&named))?
            }
            Node::Items {
                item, container, ..
            } => self.items(*item, *container, kind, reader)?,
            Node::Tuple(items) => {
                let expected = format!("tuple of {}", items.len());
                let Kind::List = kind else {
                    return Err(wrong(&expected, kind));
                };
                let len = reader.read_array_len().map_err(SchemaError::from)?;
                if len != items.len() {
                    return Err(wrong(&expected, kind));
                }
                let mut built = Vec::with_capacity(len);
                for node in items {
                    built.push(self.read(*node, reader)?);
                }
                PyTuple::new(py, built)?.into_any()
            }
            Node::Mapping { key, value, .. } => self.mapping(*key, *value, kind, reader)?,
            Node::Dataclass(dataclass) => {
                let Kind::Map = kind else {
                    return Err(wrong(&dataclass.qualname, kind));
                };
                self.fields(at, reader)?
            }
            Node::Tagged(inner) => self.tagged(*inner, kind, reader)?,
            Node::Union(_) => self.union(at, kind, reader)?,
            Node::Ref(messages) => self.reference(*messages, kind, reader)?,
            Node::Alias(_) => unreachable!("aliases are rewritten away when the tree is built"),
        })
    }

    fn native(
        &self,
        native: Native,
        kind: Kind,
        reader: &mut Reader<'_>,
    ) -> Outcome<Bound<'py, PyAny>> {
        let py = self.schema.py();
        Ok(match (native, kind) {
            (Native::None, Kind::None) => {
                reader.read_nil().map_err(SchemaError::from)?;
                py.None().into_bound(py)
            }
            // A payload that holds a bool under an `int` annotation stays a bool, as it does when it is written.
            (Native::Bool | Native::Int, Kind::Bool) => {
                let value = reader.read_bool().map_err(SchemaError::from)?;
                PyBool::new(py, value).to_owned().into_any()
            }
            (Native::Int, Kind::Int) => match reader.read_int().map_err(SchemaError::from)? {
                Int::Unsigned(value) => value.into_pyobject(py)?.into_any(),
                Int::Signed(value) => value.into_pyobject(py)?.into_any(),
            },
            (Native::Float, Kind::Float) => {
                let value = reader.read_f64().map_err(SchemaError::from)?;
                value.into_pyobject(py)?.into_any()
            }
            (Native::Float, Kind::Int) => {
                let value = reader.read_int().map_err(SchemaError::from)?;
                value.as_f64().into_pyobject(py)?.into_any()
            }
            (Native::Float, Kind::Bool) => {
                let value = reader.read_bool().map_err(SchemaError::from)?;
                f64::from(u8::from(value)).into_pyobject(py)?.into_any()
            }
            (Native::Str, Kind::Str) => {
                let value = reader.read_str().map_err(SchemaError::from)?;
                PyString::new(py, value).into_any()
            }
            (Native::Bytes, Kind::Bytes) => {
                let value = reader.read_bin().map_err(SchemaError::from)?;
                PyBytes::new(py, value).into_any()
            }
            _ => return Err(wrong(native.name(), kind)),
        })
    }

    fn literal(
        &self,
        values: &[Literal],
        kind: Kind,
        reader: &mut Reader<'_>,
    ) -> Outcome<Bound<'py, PyAny>> {
        let py = self.schema.py();
        let read = match kind {
            Kind::Bool => Some(Literal::Bool(
                reader.read_bool().map_err(SchemaError::from)?,
            )),
            Kind::Int => match reader.read_int().map_err(SchemaError::from)? {
                Int::Signed(value) => Some(Literal::Int(value)),
                Int::Unsigned(value) => i64::try_from(value).ok().map(Literal::Int),
            },
            Kind::Str => Some(Literal::Str(
                reader.read_str().map_err(SchemaError::from)?.to_owned(),
            )),
            _ => {
                let mut skipped = reader.clone();
                skipped.skip().map_err(SchemaError::from)?;
                None
            }
        };
        if let Some(read) = &read {
            for literal in values {
                if literal == read {
                    return Ok(self.built(literal)?);
                }
            }
        }
        let expected = super::dump::literals(py, values)?;
        let got = match &read {
            Some(literal) => self.built(literal)?.repr()?.to_str()?.to_owned(),
            None => kind.python().to_owned(),
        };
        Err(SchemaError::new(format!("expected one of {expected}, got {got}")).into())
    }

    fn built(&self, literal: &Literal) -> PyResult<Bound<'py, PyAny>> {
        let py = self.schema.py();
        Ok(match literal {
            Literal::Bool(value) => PyBool::new(py, *value).to_owned().into_any(),
            Literal::Int(value) => value.into_pyobject(py)?.into_any(),
            Literal::Str(value) => PyString::new(py, value).into_any(),
        })
    }

    fn datetime(&self, kind: Kind, reader: &mut Reader<'_>) -> Outcome<Bound<'py, PyAny>> {
        let py = self.schema.py();
        let Kind::List = kind else {
            return Err(wrong("datetime", kind));
        };
        let mut pair = reader.clone();
        let len = pair.read_array_len().map_err(SchemaError::from)?;
        let two = len == 2 && matches!(pair.kind().map_err(SchemaError::from)?, Kind::Int) && {
            pair.read_int().map_err(SchemaError::from)?;
            matches!(pair.kind().map_err(SchemaError::from)?, Kind::Int)
        };
        if !two {
            reader.skip().map_err(SchemaError::from)?;
            return Err(wrong("datetime", kind));
        }
        reader.read_array_len().map_err(SchemaError::from)?;
        let micros = reader.read_int().map_err(SchemaError::from)?;
        let offset = reader.read_int().map_err(SchemaError::from)?;
        let micros = micros.as_i64().ok_or_else(|| wrong("datetime", kind))?;
        let offset = offset.as_i64().ok_or_else(|| wrong("datetime", kind))?;
        #[allow(clippy::cast_possible_truncation)]
        Ok(self
            .schema
            .get()
            .values()
            .moment(py, micros, offset as i32)?)
    }

    fn items(
        &self,
        item: NodeRef,
        container: Container,
        kind: Kind,
        reader: &mut Reader<'_>,
    ) -> Outcome<Bound<'py, PyAny>> {
        let py = self.schema.py();
        let Kind::List = kind else {
            return Err(wrong(container.name(), kind));
        };
        let len = reader.read_array_len().map_err(SchemaError::from)?;
        let mut built = Vec::with_capacity(len);
        for _ in 0..len {
            built.push(self.read(item, reader)?);
        }
        Ok(match container {
            Container::Tuple => PyTuple::new(py, built)?.into_any(),
            Container::FrozenSet => PyFrozenSet::new(py, &built)?.into_any(),
        })
    }

    fn mapping(
        &self,
        key: NodeRef,
        value: NodeRef,
        kind: Kind,
        reader: &mut Reader<'_>,
    ) -> Outcome<Bound<'py, PyAny>> {
        let py = self.schema.py();
        let Kind::List = kind else {
            return Err(wrong("Mapping", kind));
        };
        let len = reader.read_array_len().map_err(SchemaError::from)?;
        let built = PyDict::new(py);
        for _ in 0..len {
            let entry = reader.kind().map_err(SchemaError::from)?;
            let Kind::List = entry else {
                return Err(wrong("pair", entry));
            };
            let pair = reader.read_array_len().map_err(SchemaError::from)?;
            if pair != 2 {
                return Err(wrong("pair", entry));
            }
            built.set_item(self.read(key, reader)?, self.read(value, reader)?)?;
        }
        Ok(built.into_any())
    }

    /// The fields of a dataclass from a map, where a name this version does not know is stepped over.
    fn fields(&self, at: NodeRef, reader: &mut Reader<'_>) -> Outcome<Bound<'py, PyAny>> {
        let Node::Dataclass(dataclass) = self.tree().node(at) else {
            unreachable!("only a dataclass has fields");
        };
        let len = reader.read_map_len().map_err(SchemaError::from)?;
        let mut found: Vec<Option<Bound<'py, PyAny>>> = vec![None; dataclass.fields.len()];
        for _ in 0..len {
            let name = reader.read_str().map_err(SchemaError::from)?.to_owned();
            match dataclass.position(&name) {
                Some(at) => found[at] = Some(self.read(dataclass.fields[at].node, reader)?),
                None => reader.skip().map_err(SchemaError::from)?,
            }
        }
        self.build(dataclass, found)
    }

    fn build(
        &self,
        dataclass: &Dataclass,
        found: Vec<Option<Bound<'py, PyAny>>>,
    ) -> Outcome<Bound<'py, PyAny>> {
        let py = self.schema.py();
        let named = PyDict::new(py);
        for (field, value) in dataclass.fields.iter().zip(found) {
            match value {
                Some(value) => named.set_item(field.name.as_str(), value)?,
                None if field.required => {
                    let why = format!("{}.{} is missing", dataclass.qualname, field.name);
                    return Err(SchemaError::new(why).into());
                }
                None => {}
            }
        }
        let class = self.schema.get().class(dataclass.class).bind(py);
        Ok(class.call((), Some(&named))?)
    }

    fn tagged(
        &self,
        inner: NodeRef,
        kind: Kind,
        reader: &mut Reader<'_>,
    ) -> Outcome<Bound<'py, PyAny>> {
        let Node::Dataclass(dataclass) = self.tree().node(inner) else {
            unreachable!("only a dataclass is tagged");
        };
        let Some(name) = Self::tag(kind, reader)? else {
            return Err(wrong(&dataclass.qualname, kind));
        };
        if name != dataclass.qualname {
            return Err(wrong(&dataclass.qualname, kind));
        }
        self.fields(inner, reader)
    }

    fn union(
        &self,
        at: NodeRef,
        kind: Kind,
        reader: &mut Reader<'_>,
    ) -> Outcome<Bound<'py, PyAny>> {
        let Node::Union(union) = self.tree().node(at) else {
            unreachable!("only a union has alternatives");
        };
        if !union.tagged.is_empty()
            && let Some(name) = Self::tag(kind, reader)?
        {
            let Some(node) = union.tag(&name) else {
                return Err(SchemaError::new(format!("unknown alternative {name}")).into());
            };
            return self.fields(node, reader);
        }
        for node in &union.untagged {
            if self.tree().kinds(*node).has(kind) {
                return self.read(*node, reader);
            }
        }
        Err(SchemaError::new(format!(
            "{} is not an alternative of the union",
            kind.name()
        ))
        .into())
    }

    /// The tag of a `[qualname, fields]` payload, consumed only when the payload has that shape.
    fn tag(kind: Kind, reader: &mut Reader<'_>) -> Outcome<Option<String>> {
        if kind != Kind::List {
            return Ok(None);
        }
        let mut ahead = reader.clone();
        if ahead.read_array_len().map_err(SchemaError::from)? != 2 {
            return Ok(None);
        }
        if ahead.kind().map_err(SchemaError::from)? != Kind::Str {
            return Ok(None);
        }
        let name = ahead.read_str().map_err(SchemaError::from)?.to_owned();
        if ahead.kind().map_err(SchemaError::from)? != Kind::Map {
            return Ok(None);
        }
        *reader = ahead;
        Ok(Some(name))
    }

    fn reference(
        &self,
        messages: NodeRef,
        kind: Kind,
        reader: &mut Reader<'_>,
    ) -> Outcome<Bound<'py, PyAny>> {
        let py = self.schema.py();
        let Kind::List = kind else {
            return Err(wrong("ref", kind));
        };
        let len = reader.read_array_len().map_err(SchemaError::from)?;
        let tag = reader.read_str().map_err(SchemaError::from)?;
        let target = match (tag, len) {
            ("e", 3) => Target::Entity {
                actor: reader.read_str().map_err(SchemaError::from)?.to_owned(),
                key: reader.read_str().map_err(SchemaError::from)?.to_owned(),
            },
            ("r", 4) => {
                let address = match reader.kind().map_err(SchemaError::from)? {
                    Kind::None => {
                        reader.read_nil().map_err(SchemaError::from)?;
                        None
                    }
                    Kind::Str => Some(reader.read_str().map_err(SchemaError::from)?.to_owned()),
                    _ => return Err(wrong("ref", kind)),
                };
                let raw = reader.read_bin().map_err(SchemaError::from)?;
                let Ok(incarnation) = <[u8; 16]>::try_from(raw) else {
                    return Err(wrong("ref", kind));
                };
                let id = reader.read_int().map_err(SchemaError::from)?;
                let Some(id) = id.as_i64() else {
                    return Err(wrong("ref", kind));
                };
                Target::Reply {
                    node: NodeId {
                        address,
                        incarnation,
                    },
                    id,
                }
            }
            _ => return Err(wrong("ref", kind)),
        };
        let reference = Ref::new(
            target,
            self.schema.clone().unbind(),
            messages,
            self.node.cloned(),
        );
        Ok(Bound::new(py, reference)?.into_any())
    }
}

fn wrong(expected: &str, kind: Kind) -> super::failure::Failure {
    SchemaError::mismatch(expected, kind.python()).into()
}
