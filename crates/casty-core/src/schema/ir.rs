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

/// The pair of functions an opaque value is written and read with, by its place in the table the caller keeps beside
/// the tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CodecRef(pub u32);

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

/// An `Enum`, which travels as the name of its member so that changing a value is not a wire change.
#[derive(Debug, Clone)]
pub struct Enum {
    pub class: ClassRef,
    pub qualname: String,
    /// Every name a member answers to, aliases included: a member renamed with its old name kept as an alias still
    /// reads what the old name wrote.
    pub names: Vec<String>,
}

impl Enum {
    #[must_use]
    pub fn has(&self, name: &str) -> bool {
        self.names.iter().any(|known| known == name)
    }
}

/// A value the schema does not read: the caller's functions turn it into bytes and back.
#[derive(Debug, Clone)]
pub struct Opaque {
    pub codec: CodecRef,
    /// The annotation the functions stand in for, which is what a mismatch names.
    pub name: String,
}

/// Dataclass alternatives travel as `[qualname, fields]`; the others go as they are and are told apart by kind.
#[derive(Debug, Clone, Default)]
pub struct Union {
    pub untagged: Vec<NodeRef>,
    pub tagged: Vec<(String, NodeRef)>,
}

#[derive(Debug, Clone)]
pub enum Node {
    /// `Never`: no value is expected, and any value is an error.
    Never,
    Native(Native),
    Literal(Vec<Literal>),
    /// `[microseconds since the epoch, offset in seconds]`.
    Datetime,
    /// The days since the epoch.
    Date,
    /// `[microseconds since midnight, offset in seconds]`.
    Time,
    /// The microseconds of a `timedelta`.
    Timedelta,
    /// `str(value)`, which is exact where a float is not.
    Decimal,
    /// The sixteen bytes of a `UUID`.
    Uuid,
    /// The string of a `pathlib` path, read back as the class it was annotated with.
    Path(ClassRef),
    /// The name of the member.
    Enum(Enum),
    /// The bytes the caller's `encode` made of the value.
    Opaque(Opaque),
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
    ///
    /// It holds a copy of the dataclass rather than its place in the arena, so that what it tags is one by its type.
    Tagged(Dataclass),
    Union(Union),
    /// A `Ref[M]`, as `["e", actor, key]` or `["r", address, incarnation, id]`.
    Ref(NodeRef),
    /// What a `type` alias compiled to, kept while the arena is filled so that a recursive alias ends.
    ///
    /// `Tree::new` rewrites every reference to it into the node it names, so a walk never reaches one; it would read
    /// it as the node it names.
    Alias(NodeRef),
}

impl Node {
    /// The one kind a node writes, for the nodes that always write the same shape.
    ///
    /// `Never` writes nothing, a literal or a union writes the kinds of what it holds, and an alias is rewritten away
    /// by `flatten` and left in the arena with nothing pointing at it.
    #[must_use]
    pub fn kind(&self) -> Option<Kind> {
        Some(match self {
            Self::Native(native) => native.kind(),
            Self::Date | Self::Timedelta => Kind::Int,
            Self::Decimal | Self::Enum(_) | Self::Path(_) => Kind::Str,
            Self::Uuid | Self::Opaque(_) => Kind::Bytes,
            Self::Dataclass(_) => Kind::Map,
            Self::Datetime
            | Self::Time
            | Self::Items { .. }
            | Self::Tuple(_)
            | Self::Mapping { .. }
            | Self::Tagged(_)
            | Self::Ref(_) => Kind::List,
            Self::Never | Self::Literal(_) | Self::Union(_) | Self::Alias(_) => return None,
        })
    }
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
        let sent = if let Node::Dataclass(dataclass) = &nodes[root as usize] {
            let tagged = Node::Tagged(dataclass.clone());
            nodes.push(tagged);
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

    /// The dataclass alternative of `union` that travels tagged `qualname`, if the union has one.
    #[must_use]
    pub fn alternative(&self, union: &Union, qualname: &str) -> Option<&Dataclass> {
        union
            .tagged
            .iter()
            .filter(|(name, _)| name == qualname)
            .find_map(|(_, at)| match self.node(*at) {
                Node::Dataclass(dataclass) => Some(dataclass),
                _ => None,
            })
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
                Node::Literal(values) => values.iter().fold(Kinds::NONE, |found, value| {
                    found.union(Kinds::of(value.kind()))
                }),
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
                other => other.kind().map_or(Kinds::NONE, Kinds::of),
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
            Node::Never
            | Node::Native(_)
            | Node::Literal(_)
            | Node::Datetime
            | Node::Date
            | Node::Time
            | Node::Timedelta
            | Node::Decimal
            | Node::Uuid
            | Node::Path(_)
            | Node::Enum(_)
            | Node::Opaque(_) => {}
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
            Node::Dataclass(dataclass) | Node::Tagged(dataclass) => {
                for field in &mut dataclass.fields {
                    field.node = targets[field.node as usize];
                }
            }
            Node::Ref(inner) | Node::Alias(inner) => {
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

#[cfg(test)]
mod tests {
    use super::{ClassRef, CodecRef, Enum, Kind, Kinds, Native, Node, Opaque, Tree, Union};

    fn colour() -> Node {
        Node::Enum(Enum {
            class: ClassRef(0),
            qualname: "Colour".to_owned(),
            names: vec!["RED".to_owned(), "CRIMSON".to_owned()],
        })
    }

    fn numbers() -> Node {
        Node::Opaque(Opaque {
            codec: CodecRef(0),
            name: "list[int]".to_owned(),
        })
    }

    #[test]
    fn a_path_writes_a_str_and_an_opaque_value_bytes() {
        let nodes = vec![Node::Path(ClassRef(0)), numbers(), Node::Tuple(vec![0, 1])];
        let tree = Tree::new(nodes, 2).expect("a tree without aliases");
        assert_eq!(tree.kinds(0), Kinds::of(Kind::Str));
        assert_eq!(tree.kinds(1), Kinds::of(Kind::Bytes));
    }

    #[test]
    fn an_optional_opaque_value_reads_either_by_kind() {
        let nodes = vec![
            numbers(),
            Node::Native(Native::None),
            Node::Union(Union {
                untagged: vec![0, 1],
                tagged: Vec::new(),
            }),
        ];
        let tree = Tree::new(nodes, 2).expect("a union without aliases");
        let kinds = tree.kinds(tree.root());
        assert!(kinds.has(Kind::Bytes) && kinds.has(Kind::None));
        assert!(!kinds.has(Kind::List));
    }

    #[test]
    fn an_alias_of_an_opaque_value_is_the_opaque_value() {
        let nodes = vec![numbers(), Node::Alias(0), Node::Tuple(vec![1])];
        let tree = Tree::new(nodes, 2).expect("an alias that ends");
        let Node::Tuple(items) = tree.node(tree.root()) else {
            unreachable!("built as a tuple");
        };
        assert_eq!(items, &vec![0]);
        assert!(matches!(tree.node(0), Node::Opaque(opaque) if opaque.codec == CodecRef(0)));
    }

    #[test]
    fn the_standard_library_leaves_each_write_one_kind() {
        let nodes = vec![
            colour(),
            Node::Decimal,
            Node::Timedelta,
            Node::Date,
            Node::Time,
            Node::Tuple(vec![0, 1, 2, 3, 4]),
        ];
        let tree = Tree::new(nodes, 5).expect("a tree without aliases");
        let expected = [Kind::Str, Kind::Str, Kind::Int, Kind::Int, Kind::List];
        for (at, kind) in (0..).zip(expected) {
            assert_eq!(tree.kinds(at), Kinds::of(kind), "node {at}");
        }
    }

    #[test]
    fn a_union_of_an_enum_and_an_int_reads_either_by_kind() {
        let nodes = vec![
            colour(),
            Node::Native(Native::Int),
            Node::Union(Union {
                untagged: vec![0, 1],
                tagged: Vec::new(),
            }),
            Node::Alias(2),
        ];
        let tree = Tree::new(nodes, 3).expect("an alias of a union");
        assert_eq!(tree.root(), 2);
        let kinds = tree.kinds(tree.root());
        assert!(kinds.has(Kind::Str) && kinds.has(Kind::Int));
        assert!(!kinds.has(Kind::List));
    }

    #[test]
    fn an_enum_knows_its_aliases() {
        let Node::Enum(colour) = colour() else {
            unreachable!("built as an enum");
        };
        assert!(colour.has("CRIMSON"));
        assert!(!colour.has("GREEN"));
    }
}
