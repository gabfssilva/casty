//! What a node reports to the observer it was built with, and how a report reaches it.
//!
//! The observer is never called where an event happens. Each event is scheduled on the loop as a callback of its own
//! with `call_soon`: in the middle of a step of the node when it happens on the loop, and from the inbox of the loop
//! when it comes from the threads of the transport, which never enter Python. So the observer runs on the loop, after
//! the step that emitted the event, and it may call back into the system; and an observer that raises is a callback
//! that raised, which the loop hands to its exception handler while the node goes on.
//!
//! Nor is an event of a kind the observer does not take ever built. An observer with a `wants` method is asked, when
//! the system enters, whether it takes each kind, and what it said no to is dropped before anything reaches the
//! interpreter. That is what keeps the default observer free on a busy node: it logs activations starting and ending
//! at debug, and without debug records it takes neither.

use std::sync::atomic::{AtomicU16, Ordering};

use casty_core::membership::table::Status;
use casty_node::events::{Direction, Event};
use casty_node::replication::service::Failure;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use super::identity;

/// A kind of event: the class of `casty` that carries it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    MemberChanged,
    ActivationStarted,
    ActivationEnded,
    ActivationFailed,
    WriteFailed,
    HandoffStarted,
    HandoffEnded,
    ConnectionLost,
    MessageDropped,
}

impl Kind {
    const EVERY: [Self; 9] = [
        Self::MemberChanged,
        Self::ActivationStarted,
        Self::ActivationEnded,
        Self::ActivationFailed,
        Self::WriteFailed,
        Self::HandoffStarted,
        Self::HandoffEnded,
        Self::ConnectionLost,
        Self::MessageDropped,
    ];

    fn class(self) -> &'static str {
        match self {
            Self::MemberChanged => "MemberChanged",
            Self::ActivationStarted => "ActivationStarted",
            Self::ActivationEnded => "ActivationEnded",
            Self::ActivationFailed => "ActivationFailed",
            Self::WriteFailed => "WriteFailed",
            Self::HandoffStarted => "HandoffStarted",
            Self::HandoffEnded => "HandoffEnded",
            Self::ConnectionLost => "ConnectionLost",
            Self::MessageDropped => "MessageDropped",
        }
    }

    fn bit(self) -> u16 {
        1 << (self as u16)
    }
}

/// The kinds of event an observer takes: every kind until it is asked, and for an observer without `wants`.
#[derive(Debug)]
pub struct Wanted(AtomicU16);

impl Default for Wanted {
    fn default() -> Self {
        Self(AtomicU16::new(u16::MAX))
    }
}

impl Wanted {
    /// Ask `observer` whether it takes each kind of event, once, and go by what it answers from now on.
    pub fn ask(&self, observer: &Bound<'_, PyAny>) -> PyResult<()> {
        if !observer.hasattr("wants")? {
            self.0.store(u16::MAX, Ordering::Relaxed);
            return Ok(());
        }
        let wants = observer.getattr("wants")?;
        let casty = observer.py().import("casty")?;
        let mut taken = 0;
        for kind in Kind::EVERY {
            if wants.call1((casty.getattr(kind.class())?,))?.is_truthy()? {
                taken |= kind.bit();
            }
        }
        self.0.store(taken, Ordering::Relaxed);
        Ok(())
    }

    #[must_use]
    pub fn takes(&self, kind: Kind) -> bool {
        self.0.load(Ordering::Relaxed) & kind.bit() != 0
    }
}

/// An event on its way to the observer, before it is the dataclass of `casty` the observer takes.
#[derive(Debug)]
pub enum Observed {
    /// What the node of a cluster reported.
    Cluster(Event),
    Started {
        actor: String,
        key: String,
    },
    Ended {
        actor: String,
        key: String,
    },
    /// The body raised, or the activation could not go on: `error` is the exception that says why.
    Failed {
        actor: String,
        key: String,
        error: Py<PyAny>,
    },
    /// A message nobody waits for ended on this node, on its way to an activation here.
    Dropped {
        actor: String,
        key: String,
        reason: String,
    },
}

impl Observed {
    #[must_use]
    pub fn kind(&self) -> Kind {
        match self {
            Self::Started { .. } => Kind::ActivationStarted,
            Self::Ended { .. } => Kind::ActivationEnded,
            Self::Failed { .. } => Kind::ActivationFailed,
            Self::Dropped { .. } | Self::Cluster(Event::MessageDropped { .. }) => {
                Kind::MessageDropped
            }
            Self::Cluster(Event::MemberChanged { .. }) => Kind::MemberChanged,
            Self::Cluster(Event::WriteFailed { .. }) => Kind::WriteFailed,
            Self::Cluster(Event::HandoffStarted { .. }) => Kind::HandoffStarted,
            Self::Cluster(Event::HandoffEnded { .. } | Event::HandoffAbandoned { .. }) => {
                Kind::HandoffEnded
            }
            Self::Cluster(Event::ConnectionLost { .. }) => Kind::ConnectionLost,
        }
    }

    fn value(self, py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
        let casty = py.import("casty")?;
        match self {
            Self::Started { actor, key } => casty.getattr("ActivationStarted")?.call1((actor, key)),
            Self::Ended { actor, key } => casty.getattr("ActivationEnded")?.call1((actor, key)),
            Self::Failed { actor, key, error } => casty
                .getattr("ActivationFailed")?
                .call1((actor, key, error)),
            Self::Dropped { actor, key, reason }
            | Self::Cluster(Event::MessageDropped { actor, key, reason }) => {
                casty.getattr("MessageDropped")?.call1((actor, key, reason))
            }
            Self::Cluster(Event::MemberChanged {
                node,
                status,
                previous,
            }) => casty.getattr("MemberChanged")?.call1((
                identity(py, &node)?,
                status.name(),
                previous.map(Status::name),
            )),
            Self::Cluster(Event::WriteFailed {
                actor,
                key,
                operation,
                failure,
            }) => {
                let (reason, message) = match failure {
                    Failure::Unavailable(message) => ("unavailable", message),
                    Failure::Fencing(message) => ("fenced", message),
                    Failure::TooLarge(message) => ("too_large", message),
                };
                casty
                    .getattr("WriteFailed")?
                    .call1((actor, key, operation.name(), reason, message))
            }
            Self::Cluster(Event::HandoffStarted { actor, direction }) => casty
                .getattr("HandoffStarted")?
                .call1((actor, direction.name())),
            Self::Cluster(Event::HandoffEnded { actor, direction }) => casty
                .getattr("HandoffEnded")?
                .call1((actor, direction.name())),
            Self::Cluster(Event::HandoffAbandoned { actor, keys }) => casty
                .getattr("HandoffEnded")?
                .call1((actor, Direction::Out.name(), PyTuple::new(py, keys)?)),
            Self::Cluster(Event::ConnectionLost { node }) => casty
                .getattr("ConnectionLost")?
                .call1((identity(py, &node)?,)),
        }
    }
}

/// Schedule `observer(event)` on `running_loop`, from the loop itself.
///
/// An event that cannot be scheduled is let go: the loop has closed, which is a system on its way out, and nobody is
/// left to hear of it.
pub fn deliver(
    py: Python<'_>,
    running_loop: &Bound<'_, PyAny>,
    observer: &Py<PyAny>,
    event: Observed,
) {
    let _ = event
        .value(py)
        .and_then(|value| running_loop.call_method1("call_soon", (observer, value)));
}
