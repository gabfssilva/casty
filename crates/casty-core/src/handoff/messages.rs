//! What nodes say to each other while keys move between them.

use crate::node::NodeId;
use crate::placement::Range;
use crate::replication::messages::Copy;

/// What travels while a range changes hands, in the band of `replication`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pull {
    /// Ask a node of the previous ring for the keys it keeps in `ranges`.
    ///
    /// `transfer` numbers the request at the node that asks, which has one per step of the ring it is waiting for.
    PullRange {
        actor: String,
        node: NodeId,
        transfer: u64,
        ranges: Vec<Range>,
    },
    /// Part of an answer to a `PullRange`: keys in order, split so that each part fits in a message.
    ///
    /// `stream` names the answer among every stream of keys the source sent, and `part` numbers its messages from the
    /// first: an answer a message of which went missing leaves keys out, and does not count. `final_part` says the
    /// source has nothing more for the request, and `receiving` that the source is itself waiting for one of the
    /// ranges, so that this answer does not count toward the threshold of the node that asked.
    RangeKeys {
        actor: String,
        replica: NodeId,
        transfer: u64,
        stream: u64,
        part: u32,
        final_part: bool,
        receiving: bool,
        keys: Vec<Copy>,
    },
    /// Part of a handover: keys the sender stopped replicating, for a node that replicates them now, in a numbered
    /// stream as for a range.
    HandKeys {
        actor: String,
        node: NodeId,
        stream: u64,
        part: u32,
        final_part: bool,
        keys: Vec<Copy>,
    },
    /// The keys one message of a handover completed here, which is what lets the sender drop its copies.
    ///
    /// Each message is answered for on its own: a list of every key of a handover would not fit in one message, while
    /// the names of the keys one message completed take less room than that message did.
    TookKeys {
        actor: String,
        replica: NodeId,
        keys: Vec<String>,
    },
}

impl Pull {
    #[must_use]
    pub fn actor(&self) -> &str {
        match self {
            Self::PullRange { actor, .. }
            | Self::RangeKeys { actor, .. }
            | Self::HandKeys { actor, .. }
            | Self::TookKeys { actor, .. } => actor,
        }
    }

    /// Who sent it, whichever name the message gives that node.
    #[must_use]
    pub fn sender(&self) -> &NodeId {
        match self {
            Self::PullRange { node, .. } | Self::HandKeys { node, .. } => node,
            Self::RangeKeys { replica, .. } | Self::TookKeys { replica, .. } => replica,
        }
    }
}
