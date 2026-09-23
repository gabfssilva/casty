//! The actor types one system has met, by name. Nothing is listed beforehand.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use pyo3::prelude::*;

use crate::actor::Definition;

/// A type is met when this process uses it, or when its name arrives from another node.
///
/// The name of a type is where its body lives, `module:qualname`, so a node that never touched the type imports it
/// from there: every member of a cluster runs the same code. A name that does not import is a type of a version this
/// node does not have yet.
#[derive(Debug, Default)]
pub struct Catalog {
    /// Each type met, with the object of the `actor` decorator it was met as.
    known: HashMap<String, (Py<PyAny>, Arc<Definition>)>,
    unknown: HashSet<String>,
}

impl Catalog {
    /// Meet the type `actor` defines. The first definition under a name is the one this system runs.
    pub fn learn(&mut self, actor: &Bound<'_, PyAny>, definition: &Arc<Definition>) {
        if self.known.contains_key(&definition.name) {
            return;
        }
        self.unknown.remove(&definition.name);
        let held = (actor.clone().unbind(), Arc::clone(definition));
        self.known.insert(definition.name.clone(), held);
    }

    #[must_use]
    pub fn known(&self, name: &str) -> Option<&Arc<Definition>> {
        self.known.get(name).map(|(_, definition)| definition)
    }

    /// The object the type called `name` was met as.
    #[must_use]
    pub fn object(&self, py: Python<'_>, name: &str) -> Option<Py<PyAny>> {
        self.known.get(name).map(|(actor, _)| actor.clone_ref(py))
    }

    /// Every type this process holds, as a node of a cluster needs it.
    #[must_use]
    pub fn kinds(&self) -> Vec<casty_node::node::Kind> {
        self.known
            .values()
            .map(|(_, definition)| definition.kind())
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

/// Import the type called `name`, which is `module:qualname`: the object found there, and what it defines.
#[must_use]
pub fn imported<'py>(py: Python<'py>, name: &str) -> Option<(Bound<'py, PyAny>, Arc<Definition>)> {
    let (module, qualname) = name.split_once(':')?;
    let mut found = py.import(module).ok()?.into_any();
    for part in qualname.split('.') {
        found = found.getattr(part).ok()?;
    }
    let definition = Definition::of(&found).ok()?;
    (definition.name == name).then_some((found, definition))
}
