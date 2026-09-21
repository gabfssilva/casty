//! What an owner and the replicas of a key say to each other.

use crate::node::NodeId;
use crate::store::Pages;

/// Reserved page that marks a key as active.
pub const ACTIVE: &str = "@active";

/// The replicas that confirm a `save`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Write {
    One,
    Majority,
    All,
}

impl Write {
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::One => "one",
            Self::Majority => "majority",
            Self::All => "all",
        }
    }

    #[must_use]
    pub fn of(written: &str) -> Option<Self> {
        match written {
            "one" => Some(Self::One),
            "majority" => Some(Self::Majority),
            "all" => Some(Self::All),
            _ => None,
        }
    }
}

/// Term of an owner over a key, ordered by round and then by the incarnation of the node.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Epoch {
    pub round: u64,
    pub node: NodeId,
}

impl Epoch {
    /// Whether this term comes before `other`. Two terms of different nodes in the same round never tie, because
    /// no two nodes share an incarnation.
    #[must_use]
    pub fn before(&self, other: &Self) -> bool {
        (self.round, self.node.incarnation) < (other.round, other.node.incarnation)
    }
}

/// Identity of a write: the epoch of its owner and a version that grows on every attempt.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Stamp {
    pub epoch: Epoch,
    pub version: u64,
}

impl Stamp {
    #[must_use]
    pub fn before(&self, other: &Self) -> bool {
        self.epoch.before(&other.epoch)
            || (self.epoch == other.epoch && self.version < other.version)
    }
}

/// A key as a replica keeps it: the write it accepted, the epoch it promised, and the pages of that write.
///
/// `pages` may carry part of the state when the whole of it does not fit in one message, and `final_part` says it is
/// the last part of the key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Copy {
    pub key: String,
    pub accepted: Option<Stamp>,
    pub promised: Option<Epoch>,
    pub pages: Pages,
    pub final_part: bool,
}

/// What an owner asks of a replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Prepare {
        actor: String,
        key: String,
        epoch: Epoch,
    },
    /// Part `part` of the write `stamp`.
    ///
    /// `pages` and `dropped` are a delta against the write `base`, or the whole state when there is no base. A
    /// replica publishes the write on the final part, and only if it received every earlier part in order.
    Accept {
        actor: String,
        key: String,
        stamp: Stamp,
        base: Option<Stamp>,
        part: u32,
        final_part: bool,
        pages: Pages,
        dropped: Vec<String>,
    },
    FetchPages {
        actor: String,
        key: String,
        epoch: Epoch,
        names: Vec<String>,
    },
}

impl Request {
    #[must_use]
    pub fn actor(&self) -> &str {
        match self {
            Self::Prepare { actor, .. }
            | Self::Accept { actor, .. }
            | Self::FetchPages { actor, .. } => actor,
        }
    }

    #[must_use]
    pub fn key(&self) -> &str {
        match self {
            Self::Prepare { key, .. } | Self::Accept { key, .. } | Self::FetchPages { key, .. } => {
                key
            }
        }
    }

    /// The node the answer goes back to.
    #[must_use]
    pub fn owner(&self) -> &NodeId {
        match self {
            Self::Prepare { epoch, .. } | Self::FetchPages { epoch, .. } => &epoch.node,
            Self::Accept { stamp, .. } => &stamp.epoch.node,
        }
    }
}

/// What a replica answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    /// `sizes` indexes the accepted pages; `pages` carries them too when they fit in one message.
    Promise {
        actor: String,
        key: String,
        epoch: Epoch,
        replica: NodeId,
        accepted: Option<Stamp>,
        sizes: Vec<(String, usize)>,
        pages: Pages,
        receiving: bool,
    },
    Rejected {
        actor: String,
        key: String,
        replica: NodeId,
        promised: Epoch,
    },
    Pages {
        actor: String,
        key: String,
        epoch: Epoch,
        accepted: Option<Stamp>,
        pages: Pages,
    },
    Accepted {
        actor: String,
        key: String,
        stamp: Stamp,
        replica: NodeId,
        receiving: bool,
    },
    /// The replica did not accept the base of the delta.
    NeedFull {
        actor: String,
        key: String,
        stamp: Stamp,
        replica: NodeId,
    },
}
