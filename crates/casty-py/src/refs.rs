//! The address of an entity, or of whoever waits for the answer of an `ask`.

use std::sync::Arc;

use casty_core::chain::Chain;
use casty_core::mailbox::{Command, Deliver};
use casty_core::node::Target;
use casty_core::outcome::Outcome;
use casty_core::schema::ir::NodeRef;
use pyo3::prelude::*;
use pyo3::types::PyType;

use crate::awaited::Awaited;
use crate::node::Node;
use crate::node::context::stopped;
use crate::schema::Schema;

/// A ref created by a system, which is the only kind that can be serialized.
///
/// Two refs to the same target are equal: what a ref carries besides it is the way to reach it from here. The schema
/// it holds is the one its messages are written with, and `messages` is the node of that tree they travel as.
#[pyclass(frozen, eq, hash, module = "casty._casty")]
#[derive(Debug)]
pub struct Ref {
    /// An entity, or the `ask` that waits for an answer. Nothing for the ref that reaches nobody, the `reply_to` of an
    /// `Askable` that no `ask` sent: what is told to it is dropped.
    target: Option<Target>,
    schema: Py<Schema>,
    messages: NodeRef,
    /// The node that made it. Without one, or once that node has stopped, the ref reaches nothing.
    node: Option<Arc<Node>>,
}

impl Ref {
    #[must_use]
    pub fn new(
        target: Target,
        schema: Py<Schema>,
        messages: NodeRef,
        node: Option<Arc<Node>>,
    ) -> Self {
        Self {
            target: Some(target),
            schema,
            messages,
            node,
        }
    }

    /// The ref that reaches nobody, whose answers are written as `messages` of `schema` says.
    #[must_use]
    pub fn nobody(schema: Py<Schema>, messages: NodeRef, node: Option<Arc<Node>>) -> Self {
        Self {
            target: None,
            schema,
            messages,
            node,
        }
    }

    /// The ref of an entity, whose messages are what `schema` was compiled from.
    #[must_use]
    pub fn entity_of(
        schema: Py<Schema>,
        actor: String,
        key: String,
        node: Option<Arc<Node>>,
    ) -> Self {
        let messages = schema.get().tree().sent();
        Self::new(Target::Entity { actor, key }, schema, messages, node)
    }

    #[must_use]
    pub fn target(&self) -> Option<&Target> {
        self.target.as_ref()
    }

    /// The node this ref reaches, or the error a ref of a system that has stopped raises.
    fn reachable(&self) -> PyResult<&Arc<Node>> {
        match &self.node {
            Some(node) if !node.stopped() => Ok(node),
            _ => Err(stopped()),
        }
    }

    fn send(
        &self,
        py: Python<'_>,
        node: &Arc<Node>,
        data: Vec<u8>,
        reply: Option<Target>,
    ) -> PyResult<()> {
        match &self.target {
            None => Ok(()),
            Some(Target::Entity { actor, key }) => {
                // Only an `ask` keeps its sender waiting.
                let chain = match reply {
                    Some(_) => node.chain(py),
                    None => Chain::default(),
                };
                node.hand(
                    py,
                    Command::Deliver(Deliver {
                        actor: actor.clone(),
                        key: key.clone(),
                        message: data,
                        reply,
                        chain,
                    }),
                )
            }
            Some(answered @ Target::Reply { .. }) => {
                node.told(py, answered);
                node.answer(py, answered, &Outcome::Value(data))
            }
        }
    }
}

impl PartialEq for Ref {
    fn eq(&self, other: &Self) -> bool {
        self.target == other.target
    }
}

impl core::hash::Hash for Ref {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.target.hash(state);
    }
}

#[pymethods]
impl Ref {
    /// Send `msg` without waiting. Delivery is at most once.
    fn tell(&self, py: Python<'_>, msg: &Bound<'_, PyAny>) -> PyResult<()> {
        if self.target.is_none() {
            return Ok(());
        }
        let node = self.reachable()?;
        let data = Schema::write(self.schema.bind(py), self.messages, msg)?;
        self.send(py, node, data, None)
    }

    /// Send `msg`, an `Askable`, with a `reply_to` of its own, and wait for the value told to it.
    fn ask<'py>(&self, py: Python<'py>, msg: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let Some(asked) = &self.target else {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "nobody is asked: this ref reaches nothing",
            ));
        };
        let node = self.reachable()?;
        let id = node.reply();
        let reply = Target::Reply {
            node: node.id(),
            id,
        };
        let (data, answers) =
            Schema::write_asking(self.schema.bind(py), self.messages, msg, &reply)?;
        let Some(answers) = answers else {
            return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                "{} has no reply_to to answer to; a message sent by ask is an Askable",
                msg.repr()?
            )));
        };
        let answer = node.future(py)?;
        node.request(py, id, &self.schema, answers, &answer);
        let within = crate::node::replies::timeout(py, node, asked);
        crate::node::armed(py, node, id, within)?;
        self.send(py, node, data, Some(reply))?;
        Ok(Bound::new(py, Awaited::answer(answer, node, id))?.into_any())
    }

    #[classmethod]
    fn __class_getitem__<'py>(
        class: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        crate::generic::alias(class, item)
    }

    fn __repr__(&self) -> String {
        match &self.target {
            None => "Ref(nobody)".to_owned(),
            Some(Target::Entity { actor, key }) => format!("Ref({actor}/{key})"),
            Some(Target::Reply { node, id }) => {
                let address = node.address.as_deref().unwrap_or("local");
                format!("Ref(reply {id} to {address})")
            }
        }
    }
}

/// The ref that reaches nobody, which is the `reply_to` of an `Askable` that no `ask` sent.
#[pyfunction]
pub fn nobody(py: Python<'_>) -> PyResult<Ref> {
    let schema = Schema::of(py, &py.None().into_bound(py), false)?;
    let messages = schema.tree().sent();
    Ok(Ref::nobody(
        Bound::new(py, schema)?.unbind(),
        messages,
        None,
    ))
}
