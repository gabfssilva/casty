//! The core of casty, with nothing of Python in it.
//!
//! `schema` holds the IR that a state or message annotation compiles to, and the msgpack it is written in. The walk
//! over Python values lives in `casty-py`, which is the only crate that knows what an interpreter is.

pub mod backoff;
pub mod chain;
pub mod handoff;
pub mod mailbox;
pub mod membership;
pub mod node;
pub mod outcome;
pub mod placement;
pub mod replication;
pub mod rolls;
pub mod schedule;
pub mod schema;
pub mod store;
pub mod wire;
