//! What a state or message annotation compiles to, and the format it is written in.

pub mod error;
pub mod ir;
pub mod msgpack;

pub use error::SchemaError;
pub use ir::{
    ClassRef, Container, Dataclass, Field, Kinds, Literal, Native, Node, NodeRef, Tree, Union,
};
pub use msgpack::{Int, Kind, Malformed, Reader};
