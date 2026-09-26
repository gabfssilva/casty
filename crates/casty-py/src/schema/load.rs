//! msgpack straight into Python values, without a structure in between.
//!
//! Nothing is built that the schema did not ask for. A field this version of a type does not know is stepped over in
//! the bytes, and a field it knows is built once, in place.

use std::sync::Arc;

use casty_core::node::{NodeId, Target};
use casty_core::schema::ir::{
    Container, Dataclass, Enum, Literal, Native, Node, NodeRef, Opaque, Union,
};
use casty_core::schema::{ClassRef, Int, Kind, Reader, SchemaError};
use casty_core::store::Pages;
use pyo3::exceptions::PyArithmeticError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFrozenSet, PyString, PyTuple};

use super::Schema;
use super::compile::qualname;
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
    pages: &Pages,
    node: Option<&Arc<Host>>,
) -> Outcome<Bound<'py, PyAny>> {
    let loader = Loader { schema, node };
    let tree = schema.get().tree();
    let Some(dataclass) = tree.pages() else {
        let page = pages
            .get(".")
            .ok_or_else(|| SchemaError::new("missing page '.'"))?;
        let mut reader = Reader::new(page);
        let value = loader.read(tree.root(), &mut reader)?;
        reader.finish()?;
        return Ok(value);
    };
    let mut found: Vec<Option<Bound<'py, PyAny>>> = vec![None; dataclass.fields.len()];
    for (at, field) in dataclass.fields.iter().enumerate() {
        let Some(page) = pages.get(&field.name) else {
            continue;
        };
        let mut reader = Reader::new(page);
        let read = loader
            .read(field.node, &mut reader)
            .map_err(|failure| failure.under(&field.name))?;
        found[at] = Some(read);
        reader.finish()?;
    }
    loader.build(dataclass, found)
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
        let kind = reader.kind()?;
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
            Node::Datetime => {
                let (micros, offset) = Self::pair("datetime", kind, reader)?;
                self.schema.get().values().moment(py, micros, offset)?
            }
            Node::Date => {
                let days = Self::int("date", kind, reader)?;
                self.schema.get().values().day(py, days)?
            }
            Node::Time => {
                let (micros, offset) = Self::pair("time", kind, reader)?;
                self.schema.get().values().clock(py, micros, offset)?
            }
            Node::Timedelta => {
                let micros = Self::int("timedelta", kind, reader)?;
                self.schema.get().values().duration(py, micros)?
            }
            Node::Decimal => self.decimal(kind, reader)?,
            Node::Enum(enumeration) => self.member(enumeration, kind, reader)?,
            Node::Path(class) => self.path(*class, kind, reader)?,
            Node::Opaque(opaque) => self.opaque(opaque, kind, reader)?,
            Node::Uuid => {
                let Kind::Bytes = kind else {
                    return Err(wrong("UUID", kind));
                };
                let raw = reader.read_bin()?;
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
                let len = reader.read_array_len()?;
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
                self.fields(dataclass, reader)?
            }
            Node::Tagged(dataclass) => self.tagged(dataclass, kind, reader)?,
            Node::Union(union) => self.union(union, kind, reader)?,
            Node::Ref(messages) => self.reference(*messages, kind, reader)?,
            Node::Alias(named) => self.read(*named, reader)?,
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
                reader.read_nil()?;
                py.None().into_bound(py)
            }
            // A payload that holds a bool under an `int` annotation stays a bool, as it does when it is written.
            (Native::Bool | Native::Int, Kind::Bool) => {
                let value = reader.read_bool()?;
                PyBool::new(py, value).to_owned().into_any()
            }
            (Native::Int, Kind::Int) => match reader.read_int()? {
                Int::Unsigned(value) => value.into_pyobject(py)?.into_any(),
                Int::Signed(value) => value.into_pyobject(py)?.into_any(),
            },
            (Native::Float, Kind::Float) => {
                let value = reader.read_f64()?;
                value.into_pyobject(py)?.into_any()
            }
            (Native::Float, Kind::Int) => {
                let value = reader.read_int()?;
                value.as_f64().into_pyobject(py)?.into_any()
            }
            (Native::Float, Kind::Bool) => {
                let value = reader.read_bool()?;
                f64::from(u8::from(value)).into_pyobject(py)?.into_any()
            }
            (Native::Str, Kind::Str) => {
                let value = reader.read_str()?;
                PyString::new(py, value).into_any()
            }
            (Native::Bytes, Kind::Bytes) => {
                let value = reader.read_bin()?;
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
            Kind::Bool => Some(Literal::Bool(reader.read_bool()?)),
            Kind::Int => match reader.read_int()? {
                Int::Signed(value) => Some(Literal::Int(value)),
                Int::Unsigned(value) => i64::try_from(value).ok().map(Literal::Int),
            },
            Kind::Str => Some(Literal::Str(reader.read_str()?.to_owned())),
            _ => {
                let mut skipped = reader.clone();
                skipped.skip()?;
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

    /// The `[int, int]` a `datetime` or a `time` travels as: its microseconds and the offset of its zone in seconds.
    fn pair(expected: &str, kind: Kind, reader: &mut Reader<'_>) -> Outcome<(i64, i32)> {
        let Kind::List = kind else {
            return Err(wrong(expected, kind));
        };
        let mut pair = reader.clone();
        let len = pair.read_array_len()?;
        let two = len == 2 && matches!(pair.kind()?, Kind::Int) && {
            pair.read_int()?;
            matches!(pair.kind()?, Kind::Int)
        };
        if !two {
            reader.skip()?;
            return Err(wrong(expected, kind));
        }
        reader.read_array_len()?;
        let micros = reader.read_int()?;
        let offset = reader.read_int()?;
        let micros = micros.as_i64().ok_or_else(|| wrong(expected, kind))?;
        let offset = offset.as_i64().ok_or_else(|| wrong(expected, kind))?;
        #[allow(clippy::cast_possible_truncation)]
        Ok((micros, offset as i32))
    }

    /// The single int a `date` or a `timedelta` travels as.
    fn int(expected: &str, kind: Kind, reader: &mut Reader<'_>) -> Outcome<i64> {
        let Kind::Int = kind else {
            return Err(wrong(expected, kind));
        };
        let read = reader.read_int()?;
        read.as_i64().ok_or_else(|| wrong(expected, kind))
    }

    fn decimal(&self, kind: Kind, reader: &mut Reader<'_>) -> Outcome<Bound<'py, PyAny>> {
        let py = self.schema.py();
        let Kind::Str = kind else {
            return Err(wrong("Decimal", kind));
        };
        let text = reader.read_str()?;
        match self.schema.get().values().decimal.bind(py).call1((text,)) {
            Ok(number) => Ok(number),
            // `InvalidOperation`, which is what a string that is not a number raises.
            Err(raised) if raised.is_instance_of::<PyArithmeticError>(py) => {
                Err(wrong("Decimal", kind))
            }
            Err(raised) => Err(raised.into()),
        }
    }

    /// The member of an enum by its name, where a name the enum no longer has is an error and not a default.
    fn member(
        &self,
        enumeration: &Enum,
        kind: Kind,
        reader: &mut Reader<'_>,
    ) -> Outcome<Bound<'py, PyAny>> {
        let Kind::Str = kind else {
            return Err(wrong(&enumeration.qualname, kind));
        };
        let name = reader.read_str()?;
        if !enumeration.has(name) {
            let why = format!("{} has no member {name}", enumeration.qualname);
            return Err(SchemaError::new(why).into());
        }
        let class = self
            .schema
            .get()
            .class(enumeration.class)
            .bind(self.schema.py());
        Ok(class.get_item(name)?)
    }

    /// A path as the class it was annotated with, whatever class wrote it.
    fn path(
        &self,
        class: ClassRef,
        kind: Kind,
        reader: &mut Reader<'_>,
    ) -> Outcome<Bound<'py, PyAny>> {
        let class = self.schema.get().class(class).bind(self.schema.py());
        let Kind::Str = kind else {
            return Err(wrong(&qualname(class)?, kind));
        };
        let text = reader.read_str()?;
        Ok(class.call1((text,))?)
    }

    /// The value the caller's `decode` makes of the bytes its `encode` wrote.
    fn opaque(
        &self,
        opaque: &Opaque,
        kind: Kind,
        reader: &mut Reader<'_>,
    ) -> Outcome<Bound<'py, PyAny>> {
        let py = self.schema.py();
        let Kind::Bytes = kind else {
            return Err(wrong(&opaque.name, kind));
        };
        let raw = reader.read_bin()?;
        let decode = self.schema.get().codec(opaque.codec).decode.bind(py);
        Ok(decode.call1((PyBytes::new(py, raw),))?)
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
        let len = reader.read_array_len()?;
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
        let len = reader.read_array_len()?;
        let built = PyDict::new(py);
        for _ in 0..len {
            let entry = reader.kind()?;
            let Kind::List = entry else {
                return Err(wrong("pair", entry));
            };
            let pair = reader.read_array_len()?;
            if pair != 2 {
                return Err(wrong("pair", entry));
            }
            built.set_item(self.read(key, reader)?, self.read(value, reader)?)?;
        }
        Ok(built.into_any())
    }

    /// The fields of a dataclass from a map, where a name this version does not know is stepped over.
    fn fields(&self, dataclass: &Dataclass, reader: &mut Reader<'_>) -> Outcome<Bound<'py, PyAny>> {
        let len = reader.read_map_len()?;
        let mut found: Vec<Option<Bound<'py, PyAny>>> = vec![None; dataclass.fields.len()];
        for _ in 0..len {
            let name = reader.read_str()?.to_owned();
            match dataclass.position(&name) {
                Some(at) => {
                    let read = self
                        .read(dataclass.fields[at].node, reader)
                        .map_err(|failure| failure.under(&name))?;
                    found[at] = Some(read);
                }
                None => reader.skip()?,
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
        dataclass: &Dataclass,
        kind: Kind,
        reader: &mut Reader<'_>,
    ) -> Outcome<Bound<'py, PyAny>> {
        let Some(name) = Self::tag(kind, reader)? else {
            return Err(wrong(&dataclass.qualname, kind));
        };
        if name != dataclass.qualname {
            return Err(wrong(&dataclass.qualname, kind));
        }
        self.fields(dataclass, reader)
    }

    fn union(
        &self,
        union: &Union,
        kind: Kind,
        reader: &mut Reader<'_>,
    ) -> Outcome<Bound<'py, PyAny>> {
        if !union.tagged.is_empty()
            && let Some(name) = Self::tag(kind, reader)?
        {
            let Some(dataclass) = self.tree().alternative(union, &name) else {
                return Err(SchemaError::new(format!("unknown alternative {name}")).into());
            };
            return self.fields(dataclass, reader);
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
        if ahead.read_array_len()? != 2 {
            return Ok(None);
        }
        if ahead.kind()? != Kind::Str {
            return Ok(None);
        }
        let name = ahead.read_str()?.to_owned();
        if ahead.kind()? != Kind::Map {
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
        if let Kind::None = kind {
            reader.read_nil()?;
            let nobody = Ref::nobody(self.schema.clone().unbind(), messages, self.node.cloned());
            return Ok(Bound::new(py, nobody)?.into_any());
        }
        let Kind::List = kind else {
            return Err(wrong("ref", kind));
        };
        let len = reader.read_array_len()?;
        let tag = reader.read_str()?;
        let target = match (tag, len) {
            ("e", 3) => Target::Entity {
                actor: reader.read_str()?.to_owned(),
                key: reader.read_str()?.to_owned(),
            },
            ("r", 4) => {
                let address = match reader.kind()? {
                    Kind::None => {
                        reader.read_nil()?;
                        None
                    }
                    Kind::Str => Some(reader.read_str()?.to_owned()),
                    _ => return Err(wrong("ref", kind)),
                };
                let raw = reader.read_bin()?;
                let Ok(incarnation) = <[u8; 16]>::try_from(raw) else {
                    return Err(wrong("ref", kind));
                };
                let id = reader.read_int()?;
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

fn wrong(expected: &str, kind: Kind) -> Failure {
    SchemaError::mismatch(expected, kind.python()).into()
}
