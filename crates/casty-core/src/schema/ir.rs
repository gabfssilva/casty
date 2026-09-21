//! The tree an annotation compiles to.
//!
//! The nodes live in one arena and refer to each other by index, so a recursive type is a cycle of indices and needs
//! no deferred node. Whatever the tree holds of Python — the class of a dataclass — is a `ClassRef` into a table the
//! caller owns, which is what keeps this crate free of the interpreter.

use super::error::SchemaError;
use super::msgpack::Kind;

/// A node of the tree, by its place in the arena.
pub type NodeRef = u32;

/// A class the caller holds, by its place in the table it keeps beside the tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClassRef(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Native {
    None,
    Bool,
    Int,
    Float,
    Str,
    Bytes,
}

impl Native {
    #[must_use]
    pub fn kind(self) -> Kind {
        match self {
            Self::None => Kind::None,
            Self::Bool => Kind::Bool,
            Self::Int => Kind::Int,
            Self::Float => Kind::Float,
            Self::Str => Kind::Str,
            Self::Bytes => Kind::Bytes,
        }
    }

    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Bool => "bool",
            Self::Int => "int",
            Self::Float => "float",
            Self::Str => "str",
            Self::Bytes => "bytes",
        }
    }
}

/// An alternative of `Literal[...]`, which the annotation limits to `str`, `int` and `bool`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Literal {
    Bool(bool),
    Int(i64),
    Str(String),
}

impl Literal {
    #[must_use]
    pub fn kind(&self) -> Kind {
        match self {
            Self::Bool(_) => Kind::Bool,
            Self::Int(_) => Kind::Int,
            Self::Str(_) => Kind::Str,
        }
    }
}

/// The container of a homogeneous list of items.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Container {
    Tuple,
    FrozenSet,
}

impl Container {
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Tuple => "tuple",
            Self::FrozenSet => "frozenset",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub node: NodeRef,
    /// Whether the dataclass has no default for it, so that a payload without it is an error and not a default.
    pub required: bool,
}

#[derive(Debug, Clone)]
pub struct Dataclass {
    pub class: ClassRef,
    /// `__qualname__`, which is the tag a dataclass travels under.
    pub qualname: String,
    pub fields: Vec<Field>,
}

impl Dataclass {
    /// The field called `name`, and where its value goes among the ones being collected.
    #[must_use]
    pub fn position(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|field| field.name == name)
    }
}

/// Dataclass alternatives travel as `[qualname, fields]`; the others go as they are and are told apart by kind.
#[derive(Debug, Clone, Default)]
pub struct Union {
    pub untagged: Vec<NodeRef>,
    pub tagged: Vec<(String, NodeRef)>,
}

impl Union {
    #[must_use]
    pub fn tag(&self, qualname: &str) -> Option<NodeRef> {
        self.tagged
            .iter()
            .find(|(name, _)| name == qualname)
            .map(|(_, node)| *node)
    }
}

#[derive(Debug, Clone)]
pub enum Node {
    /// `Never`: no value is expected, and any value is an error.
    Never,
    Native(Native),
    Literal(Vec<Literal>),
    /// `[microseconds since the epoch, offset in seconds]`.
    Datetime,
    /// The sixteen bytes of a `UUID`.
    Uuid,
    Items {
        item: NodeRef,
        container: Container,
        /// Whether a `frozenset` is written in a fixed order, which a state that is compared byte for byte needs.
        canonical: bool,
    },
    Tuple(Vec<NodeRef>),
    Mapping {
        key: NodeRef,
        value: NodeRef,
        canonical: bool,
    },
    Dataclass(Dataclass),
    /// A dataclass at the top of a value that travels, written as `[qualname, fields]` like a union alternative.
    Tagged(NodeRef),
    Union(Union),
    /// A `Ref[M]`, as `["e", actor, key]` or `["r", address, incarnation, id]`.
    Ref(NodeRef),
    /// What a `type` alias compiled to, kept while the arena is filled so that a recursive alias ends.
    ///
    /// `Tree::new` rewrites every reference to it into the node it names, so no walk ever sees one.
    Alias(NodeRef),
}

/// The set of shapes a node writes, which is how a union picks the alternative that reads a payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Kinds(u8);

impl Kinds {
    pub const NONE: Self = Self(0);

    #[must_use]
    pub fn of(kind: Kind) -> Self {
        Self(1 << kind as u8)
    }

    #[must_use]
    pub fn has(self, kind: Kind) -> bool {
        self.0 & (1 << kind as u8) != 0
    }

    #[must_use]
    pub fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    #[must_use]
    pub fn intersects(self, other: Self) -> bool {
        self.0 & other.0 != 0
    }

    #[must_use]
    pub fn is_empty(self) -> bool {
        self.0 == 0
    }
}

/// A compiled annotation: the arena, the node the annotation itself is, and the node it travels as.
#[derive(Debug, Clone)]
pub struct Tree {
    nodes: Vec<Node>,
    kinds: Vec<Kinds>,
    root: NodeRef,
    sent: NodeRef,
}

impl Tree {
    /// Build the tree from the arena a compiler filled, with `root` as the annotation it compiled.
    ///
    /// The aliases are rewritten away first, and then the node a value travels as is added: a dataclass at the top
    /// goes tagged, because the two ends of a message do not have to agree on whether the annotation is a union.
    pub fn new(mut nodes: Vec<Node>, root: NodeRef) -> Result<Self, SchemaError> {
        let root = flatten(&mut nodes, root)?;
        let sent = if matches!(nodes[root as usize], Node::Dataclass(_)) {
            nodes.push(Node::Tagged(root));
            #[allow(clippy::cast_possible_truncation)]
            {
                nodes.len() as NodeRef - 1
            }
        } else {
            root
        };
        let kinds = kinds_of(&nodes);
        Ok(Self {
            nodes,
            kinds,
            root,
            sent,
        })
    }

    #[must_use]
    pub fn node(&self, at: NodeRef) -> &Node {
        &self.nodes[at as usize]
    }

    #[must_use]
    pub fn kinds(&self, at: NodeRef) -> Kinds {
        self.kinds[at as usize]
    }

    /// The annotation as it was written, which is what the pages of a state are built from.
    #[must_use]
    pub fn root(&self) -> NodeRef {
        self.root
    }

    /// The annotation as it travels between systems.
    #[must_use]
    pub fn sent(&self) -> NodeRef {
        self.sent
    }

    /// The dataclass at the root, whose top level fields are the pages of a state.
    #[must_use]
    pub fn pages(&self) -> Option<&Dataclass> {
        match &self.nodes[self.root as usize] {
            Node::Dataclass(dataclass) => Some(dataclass),
            _ => None,
        }
    }
}

/// The kinds of every node, settled by repeating the pass until nothing changes.
///
/// A recursive type refers to itself, so one pass is not enough and a walk would not end. The kinds only grow, and
/// the arena is finite, so the repetition stops.
fn kinds_of(nodes: &[Node]) -> Vec<Kinds> {
    let mut kinds = vec![Kinds::NONE; nodes.len()];
    loop {
        let mut changed = false;
        for (at, node) in nodes.iter().enumerate() {
            let found = match node {
                // An alias is rewritten away by `flatten`, and left in the arena with nothing pointing at it.
                Node::Never | Node::Alias(_) => Kinds::NONE,
                Node::Native(native) => Kinds::of(native.kind()),
                Node::Literal(values) => values.iter().fold(Kinds::NONE, |found, value| {
                    found.union(Kinds::of(value.kind()))
                }),
                Node::Datetime
                | Node::Uuid
                | Node::Items { .. }
                | Node::Tuple(_)
                | Node::Mapping { .. }
                | Node::Tagged(_)
                | Node::Ref(_) => Kinds::of(kind_of(node)),
                Node::Dataclass(_) => Kinds::of(Kind::Map),
                Node::Union(union) => {
                    let tagged = if union.tagged.is_empty() {
                        Kinds::NONE
                    } else {
                        Kinds::of(Kind::List)
                    };
                    union
                        .untagged
                        .iter()
                        .fold(tagged, |found, item| found.union(kinds[*item as usize]))
                }
            };
            if found != kinds[at] {
                kinds[at] = found;
                changed = true;
            }
        }
        if !changed {
            return kinds;
        }
    }
}

/// The single kind of a node that always writes the same shape.
fn kind_of(node: &Node) -> Kind {
    match node {
        Node::Uuid => Kind::Bytes,
        Node::Dataclass(_) => Kind::Map,
        _ => Kind::List,
    }
}

/// Rewrite every reference to an alias into the node the alias names, and give back where the root ended up.
fn flatten(nodes: &mut [Node], root: NodeRef) -> Result<NodeRef, SchemaError> {
    let targets: Result<Vec<NodeRef>, SchemaError> = (0..nodes.len())
        .map(|at| {
            #[allow(clippy::cast_possible_truncation)]
            named(nodes, at as NodeRef)
        })
        .collect();
    let targets = targets?;
    for node in nodes.iter_mut() {
        match node {
            Node::Never | Node::Native(_) | Node::Literal(_) | Node::Datetime | Node::Uuid => {}
            Node::Items { item, .. } => *item = targets[*item as usize],
            Node::Tuple(items) => {
                for item in items {
                    *item = targets[*item as usize];
                }
            }
            Node::Mapping { key, value, .. } => {
                *key = targets[*key as usize];
                *value = targets[*value as usize];
            }
            Node::Dataclass(dataclass) => {
                for field in &mut dataclass.fields {
                    field.node = targets[field.node as usize];
                }
            }
            Node::Tagged(inner) | Node::Ref(inner) | Node::Alias(inner) => {
                *inner = targets[*inner as usize];
            }
            Node::Union(union) => {
                for item in &mut union.untagged {
                    *item = targets[*item as usize];
                }
                for (_, item) in &mut union.tagged {
                    *item = targets[*item as usize];
                }
            }
        }
    }
    Ok(targets[root as usize])
}

/// The node an alias names, following a chain of them; more hops than there are nodes is a chain that closes.
fn named(nodes: &[Node], at: NodeRef) -> Result<NodeRef, SchemaError> {
    let mut found = at;
    for _ in 0..=nodes.len() {
        let Node::Alias(next) = nodes[found as usize] else {
            return Ok(found);
        };
        found = next;
    }
    Err(SchemaError::new(
        "a recursive type refers to itself outside of a container or dataclass",
    ))
}
