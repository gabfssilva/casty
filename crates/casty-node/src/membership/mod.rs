//! Who is in the cluster, and how a node hears about it.

pub mod directory;
pub mod service;
pub mod wire;

use std::collections::BTreeSet;

use casty_core::membership::table::Transition;
use casty_core::node::NodeId;

use self::service::{Member, Outgoing};

/// Where a node gets the member table: gossip (`Membership`), or asking a member for the whole of it (`Directory`).
///
/// A client is in no ring and hosts nothing. It keeps the table to know where to send, asks for it again when a
/// message comes back from the wrong owner, and is never in it itself. What only a member does is nothing by default.
pub trait Members: Send {
    /// The members this node knows right now.
    fn members(&self) -> Vec<Member>;

    /// What this node must put on the wire, taken out so that the caller sends it.
    fn take(&mut self) -> Vec<Outgoing>;

    /// A message on the band of `membership`.
    fn receive(&mut self, payload: &[u8], now: f64);

    /// Whether the members changed since the last call.
    fn changed(&mut self) -> bool;

    /// Have the next `changed` say they did, so that the node reads the members again.
    fn mark(&mut self);

    /// Whether this node is in a cluster, which is what its start waits for.
    fn joined(&self) -> bool;

    /// The changes of status since the last call, in the order the table made them.
    fn transitions(&mut self) -> Vec<Transition>;

    /// Ask to enter the cluster.
    fn join(&mut self);

    /// Exchange the table with a member, which the node does once every `anti_entropy` of its timings.
    fn anti_entropy(&mut self);

    /// The transport dropped what was sent to `node`, and whether that is the death of the node here. A member only
    /// suspects it, and leaves the verdict to `dead_after` and the gossip of the others; a client, which has neither,
    /// takes it as the death.
    fn unreached(&mut self, node: &NodeId, now: f64) -> bool;

    /// Whether the cluster declared this node left. It never comes back under the same identity.
    fn removed(&self) -> bool {
        false
    }

    /// Whether this node sees a majority of the members alive. A client owns no key, so it never needs one.
    fn majority(&self) -> bool {
        false
    }

    /// Watch the members, once every `heartbeat` of the timings.
    fn probe(&mut self, now: f64) {
        let _ = now;
    }

    /// Declare dead and remove the members whose time is up, once every `heartbeat` of the timings.
    fn expire(&mut self, now: f64) {
        let _ = now;
    }

    fn graft(&mut self, now: f64) {
        let _ = now;
    }

    fn shuffle(&mut self, now: f64) {
        let _ = now;
    }

    /// Tell the cluster of types this node met.
    fn know(&mut self, types: &BTreeSet<String>, now: f64) {
        let _ = (types, now);
    }

    /// Tell the cluster this node is on its way out.
    fn leave(&mut self, now: f64) {
        let _ = now;
    }

    /// Tell the cluster this node is gone.
    fn depart(&mut self, now: f64) {
        let _ = now;
    }

    /// An envelope of any component arrived from `node`, which a member counts as a sign of life.
    fn heard(&mut self, node: &NodeId, now: f64) {
        let _ = (node, now);
    }

    /// A connection to `node` ended. A member leaves it to its failure detector; a client, which has none, asks the
    /// node again at once.
    fn lost(&mut self, node: &NodeId) {
        let _ = node;
    }

    /// Ask for the whole table again, which only a client does: the one it holds is older than the owner's.
    fn refresh(&mut self) {}
}
