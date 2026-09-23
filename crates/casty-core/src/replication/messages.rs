//! What an owner and the replicas of a key say to each other.

use crate::node::NodeId;
use crate::store::Pages;

/// Reserved page that marks a key as active.
pub const ACTIVE: &str = "@active";

/// Reserved page that marks a key as deleted.
///
/// A deletion is a write like any other, of a state that holds this page and nothing else: the tombstone. It takes
/// the place of the state on the replicas that accept it, fences the older copies the way any later write does, and an
/// activation that finds it as the latest write starts the key from `initial`, as one nothing ever wrote.
pub const DELETED: &str = "@deleted";

/// The state a deletion writes.
#[must_use]
pub fn tombstone() -> Pages {
    Pages::from([(DELETED.to_owned(), Vec::new())])
}

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
/// `pages` may carry part of the state when the whole of it does not fit in one message: part `part` of the key holds
/// the next bytes of the pages it names, as `parts::split` cuts them, and `final_part` says it is the last one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Copy {
    pub key: String,
    pub accepted: Option<Stamp>,
    pub promised: Option<Epoch>,
    pub pages: Pages,
    pub part: u32,
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
    /// `pages` and `dropped` are a delta against the write `base`, or the whole state when there is no base. A page
    /// larger than a message goes on across parts, each with the next bytes of it. A replica publishes the write on
    /// the final part, and only if it received every earlier part in order.
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
    /// Part `part` of the pages `names` of the write a replica accepted, cut to fit in a message.
    FetchPages {
        actor: String,
        key: String,
        epoch: Epoch,
        names: Vec<String>,
        part: u32,
    },
    /// Whether the replica keeps a copy of `key` older than the deletion `stamp`, which it replaces by the tombstone of
    /// that deletion if it does. `node` keeps the tombstone, and lets it go once every other replica has answered.
    ///
    /// It is not a request of the owner: any replica holding a tombstone asks it, once the tombstone has lingered.
    Bury {
        actor: String,
        key: String,
        stamp: Stamp,
        node: NodeId,
    },
}

impl Request {
    #[must_use]
    pub fn actor(&self) -> &str {
        match self {
            Self::Prepare { actor, .. }
            | Self::Accept { actor, .. }
            | Self::FetchPages { actor, .. }
            | Self::Bury { actor, .. } => actor,
        }
    }

    #[must_use]
    pub fn key(&self) -> &str {
        match self {
            Self::Prepare { key, .. }
            | Self::Accept { key, .. }
            | Self::FetchPages { key, .. }
            | Self::Bury { key, .. } => key,
        }
    }

    /// The node the answer goes back to.
    #[must_use]
    pub fn owner(&self) -> &NodeId {
        match self {
            Self::Prepare { epoch, .. } | Self::FetchPages { epoch, .. } => &epoch.node,
            Self::Accept { stamp, .. } => &stamp.epoch.node,
            Self::Bury { node, .. } => node,
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
    /// Part `part` of the pages a `FetchPages` asked for, and `final_part` says there is none after it.
    Pages {
        actor: String,
        key: String,
        epoch: Epoch,
        accepted: Option<Stamp>,
        part: u32,
        final_part: bool,
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
    /// The replica keeps nothing of `key` older than the deletion `stamp`. `receiving` says a range it is still
    /// filling holds the key, which may bring an older copy yet, so the answer does not count.
    Buried {
        actor: String,
        key: String,
        stamp: Stamp,
        replica: NodeId,
        receiving: bool,
    },
}

impl Reply {
    #[must_use]
    pub fn actor(&self) -> &str {
        match self {
            Self::Promise { actor, .. }
            | Self::Rejected { actor, .. }
            | Self::Pages { actor, .. }
            | Self::Accepted { actor, .. }
            | Self::NeedFull { actor, .. }
            | Self::Buried { actor, .. } => actor,
        }
    }

    #[must_use]
    pub fn key(&self) -> &str {
        match self {
            Self::Promise { key, .. }
            | Self::Rejected { key, .. }
            | Self::Pages { key, .. }
            | Self::Accepted { key, .. }
            | Self::NeedFull { key, .. }
            | Self::Buried { key, .. } => key,
        }
    }
}
