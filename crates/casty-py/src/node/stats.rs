//! What a node counts, read when it is asked: `ActorSystem.stats()` and `Client.stats()`, and the activations
//! themselves, `ActorSystem.activations()`.
//!
//! Nothing is counted on the loop as messages go by. The activations and the answers waited for are read from the
//! tables that hold them, and each mailbox from the core that queues it; the writes and the traffic from the counters
//! that the replication and the transport keep on their own threads. A reading walks every activation of the node once.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::time::UNIX_EPOCH;

use casty_node::node::Tally;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use super::Node;
use crate::lock::Locked;

/// The activations of one type: how many there are, the messages their mailboxes hold together, and the most that one
/// of them holds.
#[derive(Debug, Default, Clone, Copy)]
struct Activations {
    active: usize,
    queued: usize,
    deepest: usize,
}

/// What `node` counts now, as the `Stats` of `casty`.
///
/// Every type the node has met is listed, with nothing counted for one that has no activation here, so that what an
/// exporter reads from it does not come and go with the keys.
pub fn snapshot<'py>(node: &Node, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    node.started(py)?;
    let met = node.catalog.locked().kinds();
    let mut actors: BTreeMap<String, Activations> = met
        .into_iter()
        .map(|kind| (kind.actor, Activations::default()))
        .collect();
    for held in node.census(py) {
        let activation = held.bind(py).get();
        let queued = activation.queued();
        let counted = actors.entry(activation.entry().to_owned()).or_default();
        counted.active += 1;
        counted.queued += queued;
        counted.deepest = counted.deepest.max(queued);
    }
    let asks = node.replies.locked().pending();
    let tally = match node.cluster() {
        Some(cluster) => cluster.node().tally(),
        None => Tally {
            confirmed: node.saved.load(Ordering::Relaxed),
            ..Tally::default()
        },
    };
    let casty = py.import("casty")?;
    let of_type = casty.getattr("ActorStats")?;
    let by_type = PyDict::new(py);
    for (actor, counted) in actors {
        let counts = of_type.call1((counted.active, counted.queued, counted.deepest))?;
        by_type.set_item(actor, counts)?;
    }
    let fields = PyDict::new(py);
    fields.set_item("actors", by_type)?;
    fields.set_item("asks_in_flight", asks)?;
    fields.set_item("writes_confirmed", tally.confirmed)?;
    fields.set_item("writes_failed", tally.failed)?;
    fields.set_item("connections", tally.connections)?;
    fields.set_item("bytes_sent", tally.sent)?;
    fields.set_item("bytes_received", tally.received)?;
    casty.getattr("Stats")?.call((), Some(&fields))
}

/// The activations `node` holds now, one `Activation` of `casty` each, in the order of their type and key.
pub fn listing<'py>(node: &Node, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
    node.started(py)?;
    let mut held: Vec<(String, String, f64, usize)> = node
        .census(py)
        .iter()
        .map(|held| {
            let activation = held.bind(py).get();
            let since = activation
                .since()
                .duration_since(UNIX_EPOCH)
                .map_or(0.0, |elapsed| elapsed.as_secs_f64());
            (
                activation.entry().to_owned(),
                activation.key().to_owned(),
                since,
                activation.queued(),
            )
        })
        .collect();
    held.sort_by(|one, other| (&one.0, &one.1).cmp(&(&other.0, &other.1)));
    let row = py.import("casty")?.getattr("Activation")?;
    let datetime = py.import("datetime")?;
    let at = datetime.getattr("datetime")?.getattr("fromtimestamp")?;
    let utc = datetime.getattr("timezone")?.getattr("utc")?;
    let rows = held
        .into_iter()
        .map(|(actor, key, since, queued)| {
            row.call1((actor, key, at.call1((since, &utc))?, queued))
        })
        .collect::<PyResult<Vec<_>>>()?;
    PyTuple::new(py, rows)
}
