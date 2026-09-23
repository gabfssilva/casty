//! The actor types one system has met, by name. Nothing is listed beforehand.

use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;

use crate::actor::Behavior;

/// A type is met when this process uses it, or when its name arrives from another node.
///
/// The name of a type is where its body lives, `module:qualname`, so a node that never touched the type imports it
/// from there: every member of a cluster runs the same code. A name that does not import is a type of a version this
/// node does not have yet.
#[derive(Debug, Default)]
pub struct Catalog {
    known: HashMap<String, Behavior>,
    unknown: HashSet<String>,
}

impl Catalog {
    /// Meet a type this process holds. The first definition under a name is the one this system runs.
    pub fn learn(&mut self, behavior: &Behavior, py: Python<'_>) {
        let name = behavior.definition().name.clone();
        if self.known.contains_key(&name) {
            return;
        }
        self.unknown.remove(&name);
        self.known.insert(name, behavior.clone_ref(py));
    }

    #[must_use]
    pub fn known(&self, name: &str) -> Option<&Behavior> {
        self.known.get(name)
    }

    /// Every type this process holds, as a node of a cluster needs it.
    #[must_use]
    pub fn kinds(&self) -> Vec<casty_node::node::Kind> {
        self.known
            .values()
            .map(|behavior| behavior.definition().kind())
            .collect()
    }

    #[must_use]
    pub fn gave_up(&self, name: &str) -> bool {
        self.unknown.contains(name)
    }

    pub fn give_up(&mut self, name: &str) {
        self.unknown.insert(name.to_owned());
    }
}

/// Import the type called `name`, which is `module:qualname`.
#[must_use]
pub fn imported(py: Python<'_>, name: &str) -> Option<Behavior> {
    let (module, qualname) = name.split_once(':')?;
    let mut found = py.import(module).ok()?.into_any();
    for part in qualname.split('.') {
        found = found.getattr(part).ok()?;
    }
    let behavior = Behavior::of(&found).ok()?;
    (behavior.definition().name == name).then_some(behavior)
}
