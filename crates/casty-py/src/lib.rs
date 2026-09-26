//! `casty._casty`: the extension module the Python facade imports.

mod actor;
mod awaited;
mod collections;
mod errors;
mod generic;
mod lock;
mod node;
mod refs;
mod runtime;
mod schema;

use pyo3::prelude::*;

/// The core of casty.
///
/// Nothing here is global to the process: everything the core owns hangs off a node handle, so that a
/// second interpreter in the same process gets a second core instead of a shared one.
#[pymodule(gil_used = false)]
fn _casty(module: &Bound<'_, PyModule>) -> PyResult<()> {
    errors::register(module)?;
    module.add_class::<awaited::Awaited>()?;
    awaited::register(module)?;
    module.add_class::<actor::Actor>()?;
    module.add_class::<actor::DefaultedActor>()?;
    module.add_class::<node::ActorSystem>()?;
    module.add_class::<node::Client>()?;
    module.add_class::<node::Writes>()?;
    module.add_class::<node::context::Context>()?;
    module.add_class::<node::context::State>()?;
    module.add_class::<node::context::Schedule>()?;
    module.add_class::<refs::Ref>()?;
    module.add_class::<runtime::Runtime>()?;
    module.add_class::<schema::Schema>()?;
    module.add_function(wrap_pyfunction!(actor::actor, module)?)?;
    module.add_function(wrap_pyfunction!(node::replicas, module)?)?;
    module.add_function(wrap_pyfunction!(refs::nobody, module)?)?;
    Ok(())
}
