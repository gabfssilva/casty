//! Who a message is for: an entity, or whoever is waiting for the answer of an `ask`.

/// The identity of a running system.
///
/// `address` is the advertised `host:port`, or nothing for clients and local systems. `incarnation` is new on every
/// start, so a process restarted on the same address is another node.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId {
    pub address: Option<String>,
    pub incarnation: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Target {
    Entity { actor: String, key: String },
    Reply { node: NodeId, id: i64 },
}

impl NodeId {
    /// An incarnation nothing else has: a process restarted on the same address is another node.
    #[must_use]
    pub fn fresh(address: Option<String>) -> Self {
        use std::hash::{BuildHasher, RandomState};
        let mut incarnation = [0_u8; 16];
        // Every `RandomState` draws its keys from the operating system, so each half is a fresh draw.
        incarnation[..8].copy_from_slice(&RandomState::new().hash_one(0_u8).to_be_bytes());
        incarnation[8..].copy_from_slice(&RandomState::new().hash_one(1_u8).to_be_bytes());
        Self {
            address,
            incarnation,
        }
    }
}
