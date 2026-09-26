//! An annotation into the tree the walks read.
//!
//! The order the annotation is taken apart in is the order the Python compiler took it apart in, because the message
//! of every refusal is part of the contract: a test reads it word for word.

use casty_core::schema::ir::{
    CodecRef, Container, Dataclass, Enum, Field, Kinds, Literal, Native, Node, NodeRef, Opaque,
    Tree, Union,
};
use casty_core::schema::{ClassRef, Kind, SchemaError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyInt, PyString, PyType};

use super::failure::{Failure, Outcome};
use super::introspect::Introspect;
use super::naming::{name, replacement};

/// The functions of an `Opaque`, which the value it annotates is written and read with.
#[derive(Debug)]
pub struct Codec {
    pub encode: Py<PyAny>,
    pub decode: Py<PyAny>,
}

/// A compiled annotation, the classes its dataclasses, enums and paths are, and the functions of its opaque values.
pub struct Compiled {
    pub tree: Tree,
    pub classes: Vec<Py<PyType>>,
    pub codecs: Vec<Codec>,
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
        codecs: Vec::new(),
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
    codecs: Vec<Codec>,
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
            codecs: self.codecs,
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
        if origin.is(&introspect.annotated) {
            return self.annotated(annotation, &args, path, env);
        }
        if let Some(node) = self.leaf(annotation) {
            return Ok(self.push(node));
        }
        if let Some(class) = self.path_class(annotation) {
            let class = self.class(&class);
            return Ok(self.push(Node::Path(class)));
        }
        if let Some((class, names)) = self.enumeration(annotation)? {
            let target = class.clone();
            return self.shared(class.as_any(), &[], move |compiler| {
                Ok(Node::Enum(Enum {
                    qualname: qualname(&target)?,
                    class: compiler.class(&target),
                    names,
                }))
            });
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

    /// An annotation that stands for itself: a native type, `Never`, or a value class of the standard library.
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
        } else if annotation.is(&introspect.date) {
            return Some(Node::Date);
        } else if annotation.is(&introspect.time) {
            return Some(Node::Time);
        } else if annotation.is(&introspect.timedelta) {
            return Some(Node::Timedelta);
        } else if annotation.is(&introspect.decimal) {
            return Some(Node::Decimal);
        } else if annotation.is(&introspect.uuid) {
            return Some(Node::Uuid);
        } else {
            return None;
        };
        Some(Node::Native(native))
    }

    /// The `pathlib` class `annotation` is, which is what its string is read back as.
    fn path_class(&self, annotation: &Bound<'py, PyAny>) -> Option<Bound<'py, PyType>> {
        self.introspect
            .paths
            .iter()
            .find(|class| annotation.is(*class))
            .cloned()
    }

    /// `Annotated[T, ...]`: a value written by the caller's functions when an `Opaque` is among its metadata, and `T`
    /// when none is.
    ///
    /// `T` is not compiled for an opaque value, since the schema never reads it: it is only what the checkers see and
    /// what a mismatch names.
    fn annotated(
        &mut self,
        annotation: &Bound<'py, PyAny>,
        args: &[Bound<'py, PyAny>],
        path: &[String],
        env: Env<'_, 'py>,
    ) -> Outcome<NodeRef> {
        let introspect = self.introspect;
        let Some((inner, metadata)) = args.split_first() else {
            let named = name(introspect, annotation)?;
            return Err(error(path, format!("{named} is not supported")));
        };
        match self.hatches(metadata)?.as_slice() {
            [] => self.compile(inner, path, env),
            [hatch] => {
                #[allow(clippy::cast_possible_truncation)]
                let codec = CodecRef(self.codecs.len() as u32);
                self.codecs.push(Codec {
                    encode: hatch.getattr("encode")?.unbind(),
                    decode: hatch.getattr("decode")?.unbind(),
                });
                let opaque = Opaque {
                    codec,
                    name: name(introspect, inner)?,
                };
                Ok(self.push(Node::Opaque(opaque)))
            }
            _ => {
                let named = name(introspect, annotation)?;
                Err(error(path, format!("{named} has more than one Opaque")))
            }
        }
    }

    /// The `Opaque`s among the metadata of an `Annotated`.
    fn hatches(&self, metadata: &[Bound<'py, PyAny>]) -> PyResult<Vec<Bound<'py, PyAny>>> {
        let mut found = Vec::new();
        for item in metadata {
            if item.is_instance(&self.introspect.opaque)? {
                found.push(item.clone());
            }
        }
        Ok(found)
    }

    /// `annotation` without the `Annotated` around it, which only an `Opaque` among its metadata keeps.
    fn bare(&self, annotation: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let introspect = self.introspect;
        if introspect.origin(annotation)?.is(&introspect.annotated) {
            let args = introspect.args(annotation)?;
            if let Some((inner, metadata)) = args.split_first()
                && self.hatches(metadata)?.is_empty()
            {
                return self.bare(inner);
            }
        }
        Ok(annotation.clone())
    }

    /// An `Enum` with members, and every name they answer to.
    ///
    /// A `Flag` is not one: a combination of flags has no single name, and an `IntFlag` keeps bits no member names. An
    /// enum without members is a base class, whose values would be members of subclasses it cannot name.
    fn enumeration(
        &self,
        annotation: &Bound<'py, PyAny>,
    ) -> PyResult<Option<(Bound<'py, PyType>, Vec<String>)>> {
        let introspect = self.introspect;
        let Ok(class) = annotation.cast::<PyType>() else {
            return Ok(None);
        };
        if !class.is_subclass(&introspect.enumeration)? || class.is_subclass(&introspect.flag)? {
            return Ok(None);
        }
        let names: Vec<String> = class
            .getattr("__members__")?
            .try_iter()?
            .map(|name| name?.extract::<String>())
            .collect::<PyResult<_>>()?;
        if names.is_empty() {
            return Ok(None);
        }
        Ok(Some((class.clone(), names)))
    }

    /// A container of other annotations, which the arguments of the origin say what holds what.
    ///
    /// Nothing when the arguments are not the ones the container takes, which `typing.FrozenSet` or `Mapping[K]` leave
    /// out: the annotation is then refused as not supported.
    fn container(
        &mut self,
        origin: &Bound<'py, PyAny>,
        args: &[Bound<'py, PyAny>],
        path: &[String],
        env: Env<'_, 'py>,
    ) -> Outcome<Option<Node>> {
        let introspect = self.introspect;
        if origin.is(&introspect.tuple_type) {
            if let [item, ellipsis] = args
                && ellipsis.is(&introspect.ellipsis)
            {
                let item = self.compile(item, path, env)?;
                return Ok(Some(Node::Items {
                    item,
                    container: Container::Tuple,
                    canonical: false,
                }));
            }
            let items = self.all(args, path, env)?;
            return Ok(Some(Node::Tuple(items)));
        }
        if origin.is(&introspect.frozenset_type)
            && let [item] = args
        {
            let item = self.compile(item, path, env)?;
            return Ok(Some(Node::Items {
                item,
                container: Container::FrozenSet,
                canonical: self.canonical,
            }));
        }
        if origin.is(&introspect.mapping)
            && let [key, value] = args
        {
            let key = self.compile(key, path, env)?;
            let value = self.compile(value, path, env)?;
            return Ok(Some(Node::Mapping {
                key,
                value,
                canonical: self.canonical,
            }));
        }
        if origin.is(&introspect.reference)
            && let [messages] = args
        {
            let messages = self.compile(messages, path, env)?;
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

    /// The alternatives of a union, with the ones that are unions themselves folded in: an alias of another union, or
    /// a union inside an `Annotated` that is not opaque.
    ///
    /// A dataclass inside such an `Annotated` is tagged as the dataclass is, which is what it was when the metadata was
    /// dropped before the compiler saw it.
    fn alternatives(&self, annotation: &Bound<'py, PyAny>) -> PyResult<Vec<Bound<'py, PyAny>>> {
        let introspect = self.introspect;
        let mut found = Vec::new();
        for alternative in introspect.args(annotation)? {
            let alternative = self.bare(&alternative)?;
            let value = if alternative.is_instance(&introspect.type_alias_type)? {
                alternative.getattr("__value__")?
            } else {
                alternative.clone()
            };
            let origin = introspect.origin(&value)?;
            if origin.is(&introspect.union_type) || origin.is(&introspect.union) {
                found.extend(self.alternatives(&value)?);
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
        let mut env: Vec<(Bound<'py, PyAny>, NodeRef)> =
            parameters.into_iter().zip(nodes.iter().copied()).collect();
        self.inherited(class, &path, &mut env)?;
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
        Ok(Node::Dataclass(Dataclass {
            class: self.class(class),
            qualname,
            fields,
        }))
    }

    /// Bind the type parameters of the generic classes `class` derives from to what its bases give them, so that a
    /// field a base declares reads as the subclass says: the `R` of `Askable[R]` is `bool` in `Withdraw(Askable[bool])`.
    fn inherited(
        &mut self,
        class: &Bound<'py, PyType>,
        path: &[String],
        env: &mut Vec<(Bound<'py, PyAny>, NodeRef)>,
    ) -> Outcome<()> {
        let introspect = self.introspect;
        // `__orig_bases__` is inherited like any attribute: only the class's own names its bases.
        let named = class
            .getattr("__dict__")?
            .call_method1("get", ("__orig_bases__",))?;
        let bases = if named.is_none() {
            class.getattr("__bases__")?
        } else {
            named
        };
        for base in bases.try_iter()? {
            let base = base?;
            let origin = introspect.origin(&base)?;
            let generic = !origin.is_none();
            let Ok(parent) = (if generic { origin } else { base.clone() }).cast_into::<PyType>()
            else {
                continue;
            };
            if generic {
                let parameters: Vec<Bound<'py, PyAny>> = parent
                    .getattr("__type_params__")?
                    .try_iter()?
                    .collect::<PyResult<_>>()?;
                for (parameter, argument) in parameters.into_iter().zip(introspect.args(&base)?) {
                    let node = self.compile(&argument, path, env)?;
                    env.push((parameter, node));
                }
            }
            self.inherited(&parent, path, env)?;
        }
        Ok(())
    }

    /// Keep `class` in the table beside the tree, and give back where it is.
    fn class(&mut self, class: &Bound<'py, PyType>) -> ClassRef {
        #[allow(clippy::cast_possible_truncation)]
        let at = ClassRef(self.classes.len() as u32);
        self.classes.push(class.clone().unbind());
        at
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
        if let Some(Node::Dataclass(dataclass)) = &self.nodes[at as usize] {
            let tagged = Node::Tagged(dataclass.clone());
            return self.push(tagged);
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
            Node::Literal(values) => values.iter().fold(Kinds::NONE, |found, value| {
                found.union(Kinds::of(value.kind()))
            }),
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
            other => other.kind().map_or(Kinds::NONE, Kinds::of),
        }
    }
}

fn error(path: &[String], message: String) -> Failure {
    let path: Vec<&str> = path.iter().map(String::as_str).collect();
    Failure::Schema(SchemaError::at(&path, message))
}

pub fn qualname(class: &Bound<'_, PyType>) -> PyResult<String> {
    class.getattr("__qualname__")?.extract::<String>()
}
