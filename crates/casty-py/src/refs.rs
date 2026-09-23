//! The address of an entity, or of whoever waits for the answer of an `ask`.

use std::sync::Arc;

use casty_core::chain::Chain;
use casty_core::mailbox::{Command, Deliver};
use casty_core::node::Target;
use casty_core::outcome::Outcome;
use casty_core::schema::ir::NodeRef;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple, PyType};

use crate::awaited::Awaited;
use crate::generic::{alias, subscript};
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
    target: Target,
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
            target,
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
    pub fn target(&self) -> &Target {
        &self.target
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
            Target::Entity { actor, key } => {
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
            answered @ Target::Reply { .. } => {
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
        let node = self.reachable()?;
        let data = Schema::write(self.schema.bind(py), self.messages, msg)?;
        self.send(py, node, data, None)
    }

    /// Send `build(reply_to, *args, **kwargs)` and wait for the value told to `reply_to`.
    #[pyo3(signature = (build, /, *args, **kwargs))]
    fn ask<'py>(
        &self,
        py: Python<'py>,
        build: &Bound<'py, PyAny>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let node = self.reachable()?;
        let schema = node.answers(py, build)?;
        let answer = node.future(py)?;
        let id = node.request(py, &schema, &answer);
        let target = Target::Reply {
            node: node.id(),
            id,
        };
        let messages = schema.get().tree().sent();
        let reply_to = Bound::new(
            py,
            Self::new(target.clone(), schema, messages, Some(node.clone())),
        )?;
        let mut all: Vec<Bound<'py, PyAny>> = vec![reply_to.into_any()];
        all.extend(args.iter());
        let msg = build.call(PyTuple::new(py, all)?, kwargs)?;
        let data = Schema::write(self.schema.bind(py), self.messages, &msg)?;
        let within = crate::node::replies::timeout(py, node, &self.target);
        crate::node::armed(py, node, id, within)?;
        self.send(py, node, data, Some(target))?;
        Ok(Bound::new(py, Awaited::answer(answer, node, id))?.into_any())
    }

    #[classmethod]
    fn __class_getitem__<'py>(
        class: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        alias(class, &subscript(item))
    }

    fn __repr__(&self) -> String {
        match &self.target {
            Target::Entity { actor, key } => format!("Ref({actor}/{key})"),
            Target::Reply { node, id } => {
                let address = node.address.as_deref().unwrap_or("local");
                format!("Ref(reply {id} to {address})")
            }
        }
    }
}
