//! An annotation into the tree the walks read.
//!
//! The order the annotation is taken apart in is the order the Python compiler took it apart in, because the message
//! of every refusal is part of the contract: a test reads it word for word.

use casty_core::schema::ir::{
    Container, Dataclass, Field, Kinds, Literal, Native, Node, NodeRef, Tree, Union,
};
use casty_core::schema::{Kind, SchemaError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyInt, PyString, PyType};

use super::failure::{Failure, Outcome};
use super::introspect::Introspect;
use super::naming::{name, replacement};

/// A compiled annotation and the classes its dataclasses are.
pub struct Compiled {
    pub tree: Tree,
    pub classes: Vec<Py<PyType>>,
}

/// Compile `annotation`, with `canonical` ordering for the containers whose state is compared byte for byte.
pub fn compile(
    introspect: &Introspect<'_>,
    annotation: &Bound<'_, PyAny>,
    canonical: bool,
) -> Outcome<Compiled> {
    let mut compiler = Compiler {
        introspect,
        nodes: Vec::new(),
        classes: Vec::new(),
        shared: Vec::new(),
        canonical,
    };
    let root = compiler.compile(annotation, &[], &[])?;
    compiler.finish(root)
}

type Env<'a, 'py> = &'a [(Bound<'py, PyAny>, NodeRef)];

struct Compiler<'a, 'py> {
    introspect: &'a Introspect<'py>,
    /// Filled as the walk returns; a node still empty at the end is a type that refers only to itself.
    nodes: Vec<Option<Node>>,
    classes: Vec<Py<PyType>>,
    /// What each dataclass and alias compiled to, by the type arguments it was compiled with.
    shared: Vec<(Py<PyAny>, Vec<NodeRef>, NodeRef)>,
    canonical: bool,
}

impl<'py> Compiler<'_, 'py> {
    fn finish(self, root: NodeRef) -> Outcome<Compiled> {
        let mut nodes = Vec::with_capacity(self.nodes.len());
        for node in self.nodes {
            let Some(node) = node else {
                return Err(SchemaError::new(
                    "a recursive type refers to itself outside of a container or dataclass",
                )
                .into());
            };
            nodes.push(node);
        }
        Ok(Compiled {
            tree: Tree::new(nodes, root)?,
            classes: self.classes,
        })
    }

    fn compile(
        &mut self,
        annotation: &Bound<'py, PyAny>,
        path: &[String],
        env: Env<'_, 'py>,
    ) -> Outcome<NodeRef> {
        let introspect = self.introspect;
        let origin = introspect.origin(annotation)?;
        let args = introspect.args(annotation)?;

        if annotation.is_instance(&introspect.type_var)? {
            for (variable, node) in env {
                if variable.is(annotation) {
                    return Ok(*node);
                }
            }
            let named = annotation.getattr("__name__")?.extract::<String>()?;
            return Err(error(path, format!("{named} has no value")));
        }
        if annotation.is_instance(&introspect.type_alias_type)? {
            let value = annotation.getattr("__value__")?;
            let path = path.to_vec();
            return self.shared(annotation, &[], |compiler| {
                Ok(Node::Alias(compiler.compile(&value, &path, &[])?))
            });
        }
        if origin.is_instance(&introspect.type_alias_type)? {
            let nodes = self.all(&args, path, env)?;
            let parameters: Vec<Bound<'py, PyAny>> = origin
                .getattr("__type_params__")?
                .try_iter()?
                .collect::<PyResult<_>>()?;
            let bound: Vec<(Bound<'py, PyAny>, NodeRef)> =
                parameters.into_iter().zip(nodes.iter().copied()).collect();
            let value = origin.getattr("__value__")?;
            let path = path.to_vec();
            return self.shared(&origin, &nodes, move |compiler| {
                Ok(Node::Alias(compiler.compile(&value, &path, &bound)?))
            });
        }
        if let Some(node) = self.leaf(annotation) {
            return Ok(self.push(node));
        }
        if origin.is(&introspect.literal) {
            return self.literal(annotation, &args, path);
        }
        if origin.is(&introspect.union_type) || origin.is(&introspect.union) {
            return self.union(annotation, path, env);
        }
        if let Some(node) = self.container(&origin, &args, path, env)? {
            return Ok(self.push(node));
        }
        if let Some(class) = self.dataclass_of(annotation)? {
            let nodes = self.all(&args, path, env)?;
            let path = path.to_vec();
            let target = class.clone();
            let arguments = nodes.clone();
            return self.shared(class.as_any(), &nodes, move |compiler| {
                compiler.dataclass(&target, &arguments, &path)
            });
        }
        let container = if origin.is_none() {
            annotation.clone()
        } else {
            origin
        };
        let named = name(introspect, annotation)?;
        if let Some(instead) = replacement(introspect, &container, &args)? {
            return Err(error(
                path,
                format!("{named} is not supported; use {instead}"),
            ));
        }
        Err(error(path, format!("{named} is not supported")))
    }

    /// An annotation that stands for itself: a native type, `Never`, a `datetime` or a `UUID`.
    fn leaf(&self, annotation: &Bound<'py, PyAny>) -> Option<Node> {
        let introspect = self.introspect;
        let native = if annotation.is_none() || annotation.is(&introspect.none_type) {
            Native::None
        } else if annotation.is(&introspect.bool_type) {
            Native::Bool
        } else if annotation.is(&introspect.int_type) {
            Native::Int
        } else if annotation.is(&introspect.float_type) {
            Native::Float
        } else if annotation.is(&introspect.str_type) {
            Native::Str
        } else if annotation.is(&introspect.bytes_type) {
            Native::Bytes
        } else if annotation.is(&introspect.never) {
            return Some(Node::Never);
        } else if annotation.is(&introspect.datetime) {
            return Some(Node::Datetime);
        } else if annotation.is(&introspect.uuid) {
            return Some(Node::Uuid);
        } else {
            return None;
        };
        Some(Node::Native(native))
    }

    /// A container of other annotations, which the arguments of the origin say what holds what.
    fn container(
        &mut self,
        origin: &Bound<'py, PyAny>,
        args: &[Bound<'py, PyAny>],
        path: &[String],
        env: Env<'_, 'py>,
    ) -> Outcome<Option<Node>> {
        let introspect = self.introspect;
        if origin.is(&introspect.tuple_type) {
            if args.len() == 2 && args[1].is(&introspect.ellipsis) {
                let item = self.compile(&args[0], path, env)?;
                return Ok(Some(Node::Items {
                    item,
                    container: Container::Tuple,
                    canonical: false,
                }));
            }
            let items = self.all(args, path, env)?;
            return Ok(Some(Node::Tuple(items)));
        }
        if origin.is(&introspect.frozenset_type) {
            let item = self.compile(&args[0], path, env)?;
            return Ok(Some(Node::Items {
                item,
                container: Container::FrozenSet,
                canonical: self.canonical,
            }));
        }
        if origin.is(&introspect.mapping) {
            let key = self.compile(&args[0], path, env)?;
            let value = self.compile(&args[1], path, env)?;
            return Ok(Some(Node::Mapping {
                key,
                value,
                canonical: self.canonical,
            }));
        }
        if origin.is(&introspect.reference) {
            let messages = self.compile(&args[0], path, env)?;
            // What the ref writes is what travels, so a dataclass at the top of its messages goes tagged.
            let messages = self.sent(messages);
            return Ok(Some(Node::Ref(messages)));
        }
        Ok(None)
    }

    fn literal(
        &mut self,
        annotation: &Bound<'py, PyAny>,
        args: &[Bound<'py, PyAny>],
        path: &[String],
    ) -> Outcome<NodeRef> {
        let mut values = Vec::with_capacity(args.len());
        for arg in args {
            // `bool` before `int`, which it is a subclass of, so that `True` stays a bool on the wire.
            let value = if let Ok(value) = arg.cast::<PyBool>() {
                Literal::Bool(value.is_true())
            } else if let Ok(value) = arg.cast::<PyInt>() {
                Literal::Int(value.extract()?)
            } else if let Ok(value) = arg.cast::<PyString>() {
                Literal::Str(value.to_str()?.to_owned())
            } else {
                let named = name(self.introspect, annotation)?;
                let why = format!("{named} is not supported; use literals of str, int or bool");
                return Err(error(path, why));
            };
            values.push(value);
        }
        Ok(self.push(Node::Literal(values)))
    }

    fn union(
        &mut self,
        annotation: &Bound<'py, PyAny>,
        path: &[String],
        env: Env<'_, 'py>,
    ) -> Outcome<NodeRef> {
        let introspect = self.introspect;
        let named = name(introspect, annotation)?;
        let mut untagged: Vec<(Bound<'py, PyAny>, NodeRef)> = Vec::new();
        let mut tagged: Vec<(String, NodeRef)> = Vec::new();
        for alternative in self.alternatives(annotation)? {
            let node = self.compile(&alternative, path, env)?;
            match self.dataclass_of(&alternative)? {
                None => {
                    let kinds = self.kinds(node);
                    for (other, other_node) in &untagged {
                        if kinds.intersects(self.kinds(*other_node)) {
                            let same = format!(
                                "{} and {} have the same structure",
                                name(introspect, other)?,
                                name(introspect, &alternative)?
                            );
                            return Err(error(path, format!("{named} is ambiguous: {same}")));
                        }
                    }
                    untagged.push((alternative, node));
                }
                Some(class) => {
                    let qualname = qualname(&class)?;
                    if tagged.iter().any(|(existing, _)| *existing == qualname) {
                        let why = format!("{named} is ambiguous: two dataclasses named {qualname}");
                        return Err(error(path, why));
                    }
                    tagged.push((qualname, node));
                }
            }
        }
        if !tagged.is_empty() {
            for (alternative, node) in &untagged {
                if self.kinds(*node).has(Kind::List) {
                    let same = format!(
                        "{} and the dataclasses have the same structure",
                        name(introspect, alternative)?
                    );
                    return Err(error(path, format!("{named} is ambiguous: {same}")));
                }
            }
        }
        let union = Union {
            untagged: untagged.into_iter().map(|(_, node)| node).collect(),
            tagged,
        };
        Ok(self.push(Node::Union(union)))
    }

    /// The alternatives of a union, with the ones that are aliases of another union folded in.
    fn alternatives(&self, annotation: &Bound<'py, PyAny>) -> PyResult<Vec<Bound<'py, PyAny>>> {
        let introspect = self.introspect;
        let mut found = Vec::new();
        for alternative in introspect.args(annotation)? {
            let nested = alternative.is_instance(&introspect.type_alias_type)? && {
                let value = alternative.getattr("__value__")?;
                let origin = introspect.origin(&value)?;
                origin.is(&introspect.union_type) || origin.is(&introspect.union)
            };
            if nested {
                found.extend(self.alternatives(&alternative.getattr("__value__")?)?);
            } else {
                found.push(alternative);
            }
        }
        Ok(found)
    }

    fn dataclass(
        &mut self,
        class: &Bound<'py, PyType>,
        nodes: &[NodeRef],
        path: &[String],
    ) -> Outcome<Node> {
        let introspect = self.introspect;
        let qualname = qualname(class)?;
        let frozen = class
            .getattr("__dict__")?
            .get_item("__dataclass_params__")?
            .getattr("frozen")?;
        if !frozen.is_truthy()? {
            let why = format!("{qualname} is not supported; use @dataclass(frozen=True)");
            return Err(error(path, why));
        }
        let path: Vec<String> = if path.is_empty() {
            vec![qualname.clone()]
        } else {
            path.to_vec()
        };
        let hints = match introspect.hints(class) {
            Ok(hints) => hints,
            Err(raised)
                if raised.is_instance_of::<pyo3::exceptions::PyNameError>(introspect.py()) =>
            {
                let why = raised.value(introspect.py()).str()?.to_str()?.to_owned();
                return Err(error(&path, why));
            }
            Err(raised) => return Err(raised.into()),
        };
        let parameters: Vec<Bound<'py, PyAny>> = class
            .getattr("__type_params__")?
            .try_iter()?
            .collect::<PyResult<_>>()?;
        let env: Vec<(Bound<'py, PyAny>, NodeRef)> =
            parameters.into_iter().zip(nodes.iter().copied()).collect();
        let mut fields = Vec::new();
        for (field, required) in introspect.dataclass_fields(class)? {
            let hint = hints.get_item(&field)?.ok_or_else(|| {
                Failure::Schema(SchemaError::at(
                    &path.iter().map(String::as_str).collect::<Vec<_>>(),
                    format!("{qualname}.{field} has no annotation"),
                ))
            })?;
            let mut under = path.clone();
            under.push(field.clone());
            let node = self.compile(&hint, &under, &env)?;
            fields.push(Field {
                name: field,
                node,
                required,
            });
        }
        #[allow(clippy::cast_possible_truncation)]
        let reference = casty_core::schema::ClassRef(self.classes.len() as u32);
        self.classes.push(class.clone().unbind());
        Ok(Node::Dataclass(Dataclass {
            class: reference,
            qualname,
            fields,
        }))
    }

    fn dataclass_of(&self, annotation: &Bound<'py, PyAny>) -> PyResult<Option<Bound<'py, PyType>>> {
        let origin = self.introspect.origin(annotation)?;
        let candidate = if origin.is_none() {
            annotation.clone()
        } else {
            origin
        };
        let Ok(class) = candidate.cast_into::<PyType>() else {
            return Ok(None);
        };
        if self.introspect.is_dataclass(class.as_any())? {
            Ok(Some(class))
        } else {
            Ok(None)
        }
    }

    fn all(
        &mut self,
        args: &[Bound<'py, PyAny>],
        path: &[String],
        env: Env<'_, 'py>,
    ) -> Outcome<Vec<NodeRef>> {
        args.iter()
            .map(|arg| self.compile(arg, path, env))
            .collect()
    }

    /// Compile `origin` with `nodes` once, so that a type that refers to itself ends.
    ///
    /// The place in the arena is taken before the walk goes in, so a reference that comes back around finds the index
    /// it will be filled with.
    fn shared(
        &mut self,
        origin: &Bound<'py, PyAny>,
        nodes: &[NodeRef],
        compile: impl FnOnce(&mut Self) -> Outcome<Node>,
    ) -> Outcome<NodeRef> {
        for (held, arguments, at) in &self.shared {
            if held.bind(self.introspect.py()).is(origin) && arguments == nodes {
                return Ok(*at);
            }
        }
        self.nodes.push(None);
        #[allow(clippy::cast_possible_truncation)]
        let at = self.nodes.len() as NodeRef - 1;
        self.shared
            .push((origin.clone().unbind(), nodes.to_vec(), at));
        let node = compile(self)?;
        self.nodes[at as usize] = Some(node);
        Ok(at)
    }

    /// The node `at` travels as: a dataclass goes tagged, and anything else as it is.
    fn sent(&mut self, at: NodeRef) -> NodeRef {
        if matches!(self.nodes[at as usize], Some(Node::Dataclass(_))) {
            return self.push(Node::Tagged(at));
        }
        at
    }

    fn push(&mut self, node: Node) -> NodeRef {
        self.nodes.push(Some(node));
        #[allow(clippy::cast_possible_truncation)]
        {
            self.nodes.len() as NodeRef - 1
        }
    }

    /// The kinds of a node while the arena is still being filled, where a node not yet written has none.
    fn kinds(&self, at: NodeRef) -> Kinds {
        self.kinds_seen(at, &mut Vec::new())
    }

    fn kinds_seen(&self, at: NodeRef, seen: &mut Vec<NodeRef>) -> Kinds {
        if seen.contains(&at) {
            return Kinds::NONE;
        }
        seen.push(at);
        let Some(node) = &self.nodes[at as usize] else {
            return Kinds::NONE;
        };
        match node {
            Node::Never => Kinds::NONE,
            Node::Native(native) => Kinds::of(native.kind()),
            Node::Literal(values) => values.iter().fold(Kinds::NONE, |found, value| {
                found.union(Kinds::of(value.kind()))
            }),
            Node::Uuid => Kinds::of(Kind::Bytes),
            Node::Dataclass(_) => Kinds::of(Kind::Map),
            Node::Datetime
            | Node::Items { .. }
            | Node::Tuple(_)
            | Node::Mapping { .. }
            | Node::Tagged(_)
            | Node::Ref(_) => Kinds::of(Kind::List),
            Node::Alias(named) => self.kinds_seen(*named, seen),
            Node::Union(union) => {
                let tagged = if union.tagged.is_empty() {
                    Kinds::NONE
                } else {
                    Kinds::of(Kind::List)
                };
                union.untagged.iter().fold(tagged, |found, item| {
                    found.union(self.kinds_seen(*item, seen))
                })
            }
        }
    }
}

fn error(path: &[String], message: String) -> Failure {
    let path: Vec<&str> = path.iter().map(String::as_str).collect();
    Failure::Schema(SchemaError::at(&path, message))
}

fn qualname(class: &Bound<'_, PyType>) -> PyResult<String> {
    class.getattr("__qualname__")?.extract::<String>()
}
