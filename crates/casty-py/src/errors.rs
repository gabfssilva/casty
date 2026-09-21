//! What the API raises. The core raises them directly: they belong to it, not to a module it imports.

use pyo3::prelude::*;
use pyo3::types::PyModule;

pyo3::create_exception!(
    _casty,
    SchemaError,
    pyo3::exceptions::PyTypeError,
    "A state or message type cannot be serialized. The message names the path of the field."
);

pyo3::create_exception!(
    _casty,
    NotStarted,
    pyo3::exceptions::PyException,
    "`ask` to a key that does not exist, of a type without a default `initial`."
);

pyo3::create_exception!(
    _casty,
    UnknownActor,
    pyo3::exceptions::PyException,
    "The node that owns the key does not have the actor type: it runs a version of the code without it.\n\n\
     Members of a cluster run the same code, so this lasts as long as a deploy that brings the type in."
);

pyo3::create_exception!(
    _casty,
    MailboxFull,
    pyo3::exceptions::PyException,
    "`ask` to an activation whose bounded mailbox is full."
);

pyo3::create_exception!(
    _casty,
    Unavailable,
    pyo3::exceptions::PyException,
    "The owner is unreachable, replicas are insufficient, or the owner is changing.\n\n\
     The message may or may not have been processed."
);

pyo3::create_exception!(
    _casty,
    Refused,
    pyo3::exceptions::PyException,
    "The node or client could not join the cluster: different cluster name, codec, or protocol version."
);

pyo3::create_exception!(
    _casty,
    ActorFailed,
    pyo3::exceptions::PyException,
    "The body raised while processing the message of an `ask`.\n\n\
     The exception may have happened on another node, so only its class name and text are kept."
);

/// What `ActorFailed` carries: where it happened and what the body raised there.
///
/// The four parts are attributes of the exception, not of its message, so that a caller can act on the class of the
/// error without reading the text.
pub fn failed(py: Python<'_>, actor: &str, key: &str, error: &str, message: &str) -> PyErr {
    let raised = ActorFailed::new_err(format!("{actor}/{key} raised {error}: {message}"));
    let value = raised.value(py);
    for (name, held) in [
        ("actor", actor),
        ("key", key),
        ("error", error),
        ("message", message),
    ] {
        if let Err(failed) = value.setattr(name, held) {
            return failed;
        }
    }
    raised
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = module.py();
    module.add("SchemaError", py.get_type::<SchemaError>())?;
    module.add("NotStarted", py.get_type::<NotStarted>())?;
    module.add("UnknownActor", py.get_type::<UnknownActor>())?;
    module.add("MailboxFull", py.get_type::<MailboxFull>())?;
    module.add("Unavailable", py.get_type::<Unavailable>())?;
    module.add("Refused", py.get_type::<Refused>())?;
    module.add("ActorFailed", py.get_type::<ActorFailed>())?;
    Ok(())
}
