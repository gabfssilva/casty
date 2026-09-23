//! The components of a node over the wire: who is in the cluster, where the keys are, and who owns each one.
//!
//! Nothing here touches an interpreter. A component deals in nodes, records, terms and envelopes, so it runs on the
//! threads of the transport; only the activation of a key and the body it runs cross into Python, and that crossing
//! lives in `casty-py`.

pub mod events;
pub mod handoff;
pub mod membership;
pub mod node;
pub mod placement;
pub mod replication;
pub mod routing;
