//! A Python value straight to msgpack, without a structure in between.

use core::cell::Cell;

use casty_core::node::Target;
use casty_core::schema::ir::{
    Container, Dataclass, Enum, Literal, Native, Node, NodeRef, Opaque, Union,
};
use casty_core::schema::{ClassRef, Int, SchemaError, msgpack};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyFloat, PyFrozenSet, PyInt, PyString, PyTuple};

use super::Schema;
use super::compile::qualname;
use super::failure::Outcome;
use super::values;
use crate::refs::Ref;

/// Write `value` as `at` says, appending to `out`.
pub fn dump(
    schema: &Schema,
    at: NodeRef,
    value: &Bound<'_, PyAny>,
    out: &mut Vec<u8>,
) -> Outcome<()> {
    Writer {
        schema,
        asking: None,
    }
    .write(at, value, out)
}

/// Write `value`, an `Askable`, as `dump` does, with `reply` in place of its `reply_to`, and give back the node of
/// that field: the type of what is told to it, which is how the answer to the `ask` is read. Nothing when the value
/// has no such field.
pub fn dump_asking(
    schema: &Schema,
    at: NodeRef,
    value: &Bound<'_, PyAny>,
    reply: &Target,
    out: &mut Vec<u8>,
) -> Outcome<Option<NodeRef>> {
    let writer = Writer {
        schema,
        asking: Some(Asking {
            reply,
            started: Cell::new(false),
            answers: Cell::new(None),
        }),
    };
    writer.write(at, value, out)?;
    Ok(writer.asking.and_then(|asking| asking.answers.get()))
}

struct Writer<'a> {
    schema: &'a Schema,
    asking: Option<Asking<'a>>,
}

/// The `reply_to` an `ask` gives the message it sends, which only the message itself takes, not a value inside it.
struct Asking<'a> {
    reply: &'a Target,
    /// Whether the message has been reached, which is the first dataclass written.
    started: Cell<bool>,
    answers: Cell<Option<NodeRef>>,
}

impl Writer<'_> {
    fn write(&self, at: NodeRef, value: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> Outcome<()> {
        let py = value.py();
        match self.schema.tree().node(at) {
            Node::Never => {
                return Err(SchemaError::new(format!(
                    "no value is expected, got {}",
                    named(value)?
                ))
                .into());
            }
            Node::Native(Native::None) => {
                if !value.is_none() {
                    return Err(wrong("none", value)?);
                }
                msgpack::write_nil(out);
            }
            Node::Native(Native::Bool) => match value.cast::<PyBool>() {
                Ok(native) => msgpack::write_bool(out, native.is_true()),
                Err(_) => return Err(wrong("bool", value)?),
            },
            // A `bool` is an `int` in Python, and the annotation `int` takes it as one; it stays a bool on the wire.
            Node::Native(Native::Int) => match value.cast::<PyBool>() {
                Ok(native) => msgpack::write_bool(out, native.is_true()),
                Err(_) => match value.cast::<PyInt>() {
                    Ok(native) => msgpack::write_int(out, integer(native)?),
                    Err(_) => return Err(wrong("int", value)?),
                },
            },
            Node::Native(Native::Float) => match value.extract::<f64>() {
                Ok(native)
                    if value.is_instance_of::<PyFloat>() || value.is_instance_of::<PyInt>() =>
                {
                    msgpack::write_f64(out, native);
                }
                _ => return Err(wrong("float", value)?),
            },
            Node::Native(Native::Str) => match value.cast::<PyString>() {
                Ok(native) => msgpack::write_str(out, native.to_str()?),
                Err(_) => return Err(wrong("str", value)?),
            },
            Node::Native(Native::Bytes) => match value.cast::<PyBytes>() {
                Ok(native) => msgpack::write_bin(out, native.as_bytes()),
                Err(_) => return Err(wrong("bytes", value)?),
            },
            Node::Literal(values) => Self::literal(values, value, out)?,
            Node::Datetime => self.datetime(value, out)?,
            Node::Date => self.date(value, out)?,
            Node::Time => self.time(value, out)?,
            Node::Timedelta => self.timedelta(value, out)?,
            Node::Decimal => self.decimal(value, out)?,
            Node::Enum(enumeration) => self.member(enumeration, value, out)?,
            Node::Path(class) => self.path(*class, value, out)?,
            Node::Opaque(opaque) => self.opaque(opaque, value, out)?,
            Node::Uuid => {
                if !value.is_instance(self.schema.values().uuid.bind(py))? {
                    return Err(wrong("UUID", value)?);
                }
                let raw: Vec<u8> = value.getattr("bytes")?.extract()?;
                msgpack::write_bin(out, &raw);
            }
            Node::Items {
                item,
                container,
                canonical,
            } => self.items(*item, *container, *canonical, value, out)?,
            Node::Tuple(items) => {
                let Ok(tuple) = value.cast::<PyTuple>() else {
                    return Err(wrong(&format!("tuple of {}", items.len()), value)?);
                };
                if tuple.len() != items.len() {
                    return Err(wrong(&format!("tuple of {}", items.len()), value)?);
                }
                msgpack::write_array_len(out, items.len());
                for (node, item) in items.iter().zip(tuple.iter()) {
                    self.write(*node, &item, out)?;
                }
            }
            Node::Mapping {
                key,
                value: item,
                canonical,
            } => self.mapping(*key, *item, *canonical, value, out)?,
            Node::Dataclass(dataclass) => self.fields(dataclass, value, out)?,
            Node::Tagged(dataclass) => {
                msgpack::write_array_len(out, 2);
                msgpack::write_str(out, &dataclass.qualname);
                self.fields(dataclass, value, out)?;
            }
            Node::Union(union) => self.union(union, value, out)?,
            Node::Ref(_) => Self::reference(value, out)?,
            Node::Alias(named) => self.write(*named, value, out)?,
        }
        Ok(())
    }

    fn literal(values: &[Literal], value: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> Outcome<()> {
        let py = value.py();
        for literal in values {
            // `type(literal) is type(value)`: a `True` is not the literal `1`, though the two compare equal.
            let same = match literal {
                Literal::Bool(_) => value.get_type().is(py.get_type::<PyBool>()),
                Literal::Int(_) => value.get_type().is(py.get_type::<PyInt>()),
                Literal::Str(_) => value.get_type().is(py.get_type::<PyString>()),
            };
            if !same {
                continue;
            }
            match literal {
                Literal::Bool(expected) if value.extract::<bool>()? == *expected => {
                    msgpack::write_bool(out, *expected);
                    return Ok(());
                }
                Literal::Int(expected)
                    if value.extract::<i64>().is_ok_and(|got| got == *expected) =>
                {
                    msgpack::write_int(out, Int::Signed(*expected));
                    return Ok(());
                }
                Literal::Str(expected)
                    if value.extract::<String>().is_ok_and(|got| got == *expected) =>
                {
                    msgpack::write_str(out, expected);
                    return Ok(());
                }
                _ => {}
            }
        }
        let expected = literals(py, values)?;
        let got = value.repr()?.to_str()?.to_owned();
        Err(SchemaError::new(format!("expected one of {expected}, got {got}")).into())
    }

    fn datetime(&self, value: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> Outcome<()> {
        let py = value.py();
        if !value.is_instance(self.schema.values().datetime.bind(py))? {
            return Err(wrong("datetime", value)?);
        }
        let offset = utc_offset("datetime", value)?;
        let since = value.sub(self.schema.values().epoch(py))?;
        msgpack::write_array_len(out, 2);
        msgpack::write_int(out, Int::Signed(microseconds(&since)?));
        msgpack::write_int(out, Int::Signed(offset));
        Ok(())
    }

    /// A `time` as the wall clock it shows and its zone, which is required for the reason a `datetime`'s is.
    fn time(&self, value: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> Outcome<()> {
        if !value.is_instance(self.schema.values().time.bind(value.py()))? {
            return Err(wrong("time", value)?);
        }
        let offset = utc_offset("time", value)?;
        let part = |name: &str| -> PyResult<i64> { value.getattr(name)?.extract() };
        let elapsed = (part("hour")? * 60 + part("minute")?) * 60 + part("second")?;
        msgpack::write_array_len(out, 2);
        msgpack::write_int(out, Int::Signed(elapsed * 1_000_000 + part("microsecond")?));
        msgpack::write_int(out, Int::Signed(offset));
        Ok(())
    }

    fn date(&self, value: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> Outcome<()> {
        if !self.schema.values().is_date(value)? {
            return Err(wrong("date", value)?);
        }
        msgpack::write_int(out, Int::Signed(values::days(value)?));
        Ok(())
    }

    fn timedelta(&self, value: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> Outcome<()> {
        if !value.is_instance(self.schema.values().timedelta.bind(value.py()))? {
            return Err(wrong("timedelta", value)?);
        }
        msgpack::write_int(out, Int::Signed(microseconds(value)?));
        Ok(())
    }

    fn decimal(&self, value: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> Outcome<()> {
        if !value.is_instance(self.schema.values().decimal.bind(value.py()))? {
            return Err(wrong("Decimal", value)?);
        }
        msgpack::write_str(out, value.str()?.to_str()?);
        Ok(())
    }

    /// A member of an enum by its name, so that changing its value is not a wire change.
    fn member(
        &self,
        enumeration: &Enum,
        value: &Bound<'_, PyAny>,
        out: &mut Vec<u8>,
    ) -> Outcome<()> {
        let class = self.schema.class(enumeration.class).bind(value.py());
        if !value.is_instance(class)? {
            return Err(wrong(&enumeration.qualname, value)?);
        }
        msgpack::write_str(out, &value.getattr("_name_")?.extract::<String>()?);
        Ok(())
    }

    fn path(&self, class: ClassRef, value: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> Outcome<()> {
        let class = self.schema.class(class).bind(value.py());
        if !value.is_instance(class)? {
            return Err(wrong(&qualname(class)?, value)?);
        }
        msgpack::write_str(out, value.str()?.to_str()?);
        Ok(())
    }

    /// The bytes the caller's `encode` makes of `value`, written without being read.
    fn opaque(&self, opaque: &Opaque, value: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> Outcome<()> {
        let encode = self.schema.codec(opaque.codec).encode.bind(value.py());
        let encoded = encode.call1((value,))?;
        let Ok(raw) = encoded.cast::<PyBytes>() else {
            let why = format!(
                "the encode of {} returned {}, not bytes",
                opaque.name,
                named(&encoded)?
            );
            return Err(SchemaError::new(why).into());
        };
        msgpack::write_bin(out, raw.as_bytes());
        Ok(())
    }

    fn items(
        &self,
        item: NodeRef,
        container: Container,
        canonical: bool,
        value: &Bound<'_, PyAny>,
        out: &mut Vec<u8>,
    ) -> Outcome<()> {
        let holds = match container {
            Container::Tuple => value.is_instance_of::<PyTuple>(),
            Container::FrozenSet => value.is_instance_of::<PyFrozenSet>(),
        };
        if !holds {
            return Err(wrong(container.name(), value)?);
        }
        if canonical && container == Container::FrozenSet {
            let mut written: Vec<Vec<u8>> = Vec::new();
            for element in value.try_iter()? {
                let mut one = Vec::new();
                self.write(item, &element?, &mut one)?;
                written.push(one);
            }
            written.sort();
            msgpack::write_array_len(out, written.len());
            for one in written {
                out.extend_from_slice(&one);
            }
            return Ok(());
        }
        msgpack::write_array_len(out, value.len()?);
        for element in value.try_iter()? {
            self.write(item, &element?, out)?;
        }
        Ok(())
    }

    fn mapping(
        &self,
        key: NodeRef,
        item: NodeRef,
        canonical: bool,
        value: &Bound<'_, PyAny>,
        out: &mut Vec<u8>,
    ) -> Outcome<()> {
        let py = value.py();
        if !value.is_instance(self.schema.values().mapping.bind(py))? {
            return Err(wrong("Mapping", value)?);
        }
        let mut written: Vec<Vec<u8>> = Vec::new();
        for pair in value.call_method0("items")?.try_iter()? {
            let pair = pair?;
            let mut one = Vec::new();
            msgpack::write_array_len(&mut one, 2);
            self.write(key, &pair.get_item(0)?, &mut one)?;
            self.write(item, &pair.get_item(1)?, &mut one)?;
            written.push(one);
        }
        if canonical {
            written.sort();
        }
        msgpack::write_array_len(out, written.len());
        for one in written {
            out.extend_from_slice(&one);
        }
        Ok(())
    }

    /// The fields of a dataclass as a map, which is what lets a version without a field still read the rest.
    fn fields(
        &self,
        dataclass: &Dataclass,
        value: &Bound<'_, PyAny>,
        out: &mut Vec<u8>,
    ) -> Outcome<()> {
        let class = self.schema.class(dataclass.class).bind(value.py());
        if !value.is_instance(class)? {
            return Err(wrong(&dataclass.qualname, value)?);
        }
        let asking = self
            .asking
            .as_ref()
            .filter(|asking| !asking.started.replace(true));
        msgpack::write_map_len(out, dataclass.fields.len());
        for field in &dataclass.fields {
            msgpack::write_str(out, &field.name);
            if let Some(asking) = asking
                && field.name == "reply_to"
                && let Node::Ref(answers) = self.schema.tree().node(field.node)
            {
                target(asking.reply, out);
                asking.answers.set(Some(*answers));
                continue;
            }
            self.write(field.node, &value.getattr(field.name.as_str())?, out)
                .map_err(|failure| failure.under(&field.name))?;
        }
        Ok(())
    }

    fn union(&self, union: &Union, value: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> Outcome<()> {
        let qualname = named(value)?;
        if let Some(dataclass) = self.schema.tree().alternative(union, &qualname) {
            msgpack::write_array_len(out, 2);
            msgpack::write_str(out, &qualname);
            return self.fields(dataclass, value, out);
        }
        // An `IntEnum` member is an `int` as well: the enum it belongs to is the alternative it means.
        let enumerations = union
            .untagged
            .iter()
            .filter(|node| matches!(self.schema.tree().node(**node), Node::Enum(_)));
        for node in enumerations.chain(&union.untagged) {
            if self.matches(*node, value)? {
                return self.write(*node, value, out);
            }
        }
        // Nothing tells a value of an opaque type apart, so the opaque alternative, which its kind keeps alone in the
        // union, takes what no other one did.
        if let Some(node) = union
            .untagged
            .iter()
            .find(|node| matches!(self.schema.tree().node(**node), Node::Opaque(_)))
        {
            return self.write(*node, value, out);
        }
        Err(SchemaError::new(format!("{qualname} is not an alternative of the union")).into())
    }

    fn reference(value: &Bound<'_, PyAny>, out: &mut Vec<u8>) -> Outcome<()> {
        let Ok(reference) = value.cast::<Ref>() else {
            let qualname = named(value)?;
            return Err(
                SchemaError::new(format!("{qualname} is not a ref created by a system")).into(),
            );
        };
        match reference.get().target() {
            Some(reached) => target(reached, out),
            None => msgpack::write_nil(out),
        }
        Ok(())
    }

    fn matches(&self, at: NodeRef, value: &Bound<'_, PyAny>) -> PyResult<bool> {
        let py = value.py();
        Ok(match self.schema.tree().node(at) {
            // `Opaque` is what the union falls back to, not what it recognises.
            Node::Never | Node::Opaque(_) => false,
            Node::Native(Native::None) => value.is_none(),
            Node::Native(Native::Bool) => value.is_instance_of::<PyBool>(),
            Node::Native(Native::Int) => value.is_instance_of::<PyInt>(),
            Node::Native(Native::Float) => {
                value.is_instance_of::<PyFloat>() || value.is_instance_of::<PyInt>()
            }
            Node::Native(Native::Str) => value.is_instance_of::<PyString>(),
            Node::Native(Native::Bytes) => value.is_instance_of::<PyBytes>(),
            Node::Literal(values) => values.iter().any(|literal| match literal {
                Literal::Bool(_) => value.is_instance_of::<PyBool>(),
                Literal::Int(_) => value.is_instance_of::<PyInt>(),
                Literal::Str(_) => value.is_instance_of::<PyString>(),
            }),
            Node::Datetime => value.is_instance(self.schema.values().datetime.bind(py))?,
            Node::Date => self.schema.values().is_date(value)?,
            Node::Time => value.is_instance(self.schema.values().time.bind(py))?,
            Node::Timedelta => value.is_instance(self.schema.values().timedelta.bind(py))?,
            Node::Decimal => value.is_instance(self.schema.values().decimal.bind(py))?,
            Node::Enum(enumeration) => {
                value.is_instance(self.schema.class(enumeration.class).bind(py))?
            }
            Node::Path(class) => value.is_instance(self.schema.class(*class).bind(py))?,
            Node::Uuid => value.is_instance(self.schema.values().uuid.bind(py))?,
            Node::Items { container, .. } => match container {
                Container::Tuple => value.is_instance_of::<PyTuple>(),
                Container::FrozenSet => value.is_instance_of::<PyFrozenSet>(),
            },
            Node::Tuple(_) => value.is_instance_of::<PyTuple>(),
            Node::Mapping { .. } => value.is_instance(self.schema.values().mapping.bind(py))?,
            Node::Dataclass(dataclass) | Node::Tagged(dataclass) => {
                value.is_instance(self.schema.class(dataclass.class).bind(py))?
            }
            Node::Union(union) => {
                let mut found = false;
                for node in union
                    .untagged
                    .iter()
                    .chain(union.tagged.iter().map(|(_, node)| node))
                {
                    found = found || self.matches(*node, value)?;
                }
                found
            }
            Node::Ref(_) => value.is_instance_of::<Ref>(),
            Node::Alias(named) => self.matches(*named, value)?,
        })
    }
}

fn named(value: &Bound<'_, PyAny>) -> PyResult<String> {
    value.get_type().getattr("__qualname__")?.extract()
}

pub fn wrong(expected: &str, value: &Bound<'_, PyAny>) -> PyResult<super::failure::Failure> {
    Ok(SchemaError::mismatch(expected, &named(value)?).into())
}

pub fn literals(py: Python<'_>, values: &[Literal]) -> PyResult<String> {
    let mut built: Vec<Bound<'_, PyAny>> = Vec::with_capacity(values.len());
    for literal in values {
        built.push(match literal {
            Literal::Bool(value) => PyBool::new(py, *value).to_owned().into_any(),
            Literal::Int(value) => PyInt::new(py, *value).into_any(),
            Literal::Str(value) => PyString::new(py, value).into_any(),
        });
    }
    Ok(PyTuple::new(py, built)?.repr()?.to_str()?.to_owned())
}

fn integer(value: &Bound<'_, PyInt>) -> PyResult<Int> {
    if let Ok(unsigned) = value.extract::<u64>() {
        return Ok(Int::Unsigned(unsigned));
    }
    Ok(Int::Signed(value.extract::<i64>()?))
}

/// The parts of a `timedelta`, read as attributes because the limited API has no date and time functions.
fn parts(delta: &Bound<'_, PyAny>) -> PyResult<(i64, i64, i64)> {
    Ok((
        delta.getattr("days")?.extract()?,
        delta.getattr("seconds")?.extract()?,
        delta.getattr("microseconds")?.extract()?,
    ))
}

/// `delta // timedelta(microseconds=1)`, which a normalized `timedelta` makes exact.
///
/// A `timedelta` reaches a billion days, which is more microseconds than 64 bits hold.
fn microseconds(delta: &Bound<'_, PyAny>) -> Outcome<i64> {
    let (days, seconds, micros) = parts(delta)?;
    let total = days
        .checked_mul(86_400_000_000)
        .and_then(|total| total.checked_add(seconds * 1_000_000 + micros));
    let Some(total) = total else {
        let written = delta.str()?.to_str()?.to_owned();
        return Err(SchemaError::new(format!(
            "timedelta does not fit in 64 bits of microseconds: {written}"
        ))
        .into());
    };
    Ok(total)
}

/// The offset from UTC of a `datetime` or a `time`, in seconds, which one without a zone does not have.
fn utc_offset(what: &str, value: &Bound<'_, PyAny>) -> Outcome<i64> {
    let offset = value.call_method0("utcoffset")?;
    if offset.is_none() {
        let written = value.str()?.to_str()?.to_owned();
        return Err(SchemaError::new(format!("{what} without time zone: {written}")).into());
    }
    Ok(seconds(&offset)?)
}

/// `offset // timedelta(seconds=1)`, where the microseconds of a zone offset are always zero.
fn seconds(delta: &Bound<'_, PyAny>) -> PyResult<i64> {
    let (days, seconds, _) = parts(delta)?;
    Ok(days * 86_400 + seconds)
}

/// What a ref is written as: the entity it names, or the node and id of the `ask` that waits on it.
fn target(target: &Target, out: &mut Vec<u8>) {
    match target {
        Target::Entity { actor, key } => {
            msgpack::write_array_len(out, 3);
            msgpack::write_str(out, "e");
            msgpack::write_str(out, actor);
            msgpack::write_str(out, key);
        }
        Target::Reply { node, id } => {
            msgpack::write_array_len(out, 4);
            msgpack::write_str(out, "r");
            match &node.address {
                Some(address) => msgpack::write_str(out, address),
                None => msgpack::write_nil(out),
            }
            msgpack::write_bin(out, &node.incarnation);
            msgpack::write_int(out, Int::Signed(*id));
        }
    }
}
