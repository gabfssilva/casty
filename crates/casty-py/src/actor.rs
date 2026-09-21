//! An actor type, which is a body and what the system needs to know about it.

use casty_core::replication::messages::Write;
use casty_core::schema::SchemaError;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyType;

use crate::node::context::Context;
use crate::schema::Schema;
use crate::schema::failure::{Failure, Outcome};
use crate::schema::introspect::Introspect;

/// The replicas that confirm a `save`, as the decorator writes it.
fn written(write: &str) -> PyResult<Write> {
    Write::of(write).ok_or_else(|| {
        PyValueError::new_err(format!(
            "write is {write:?}, not one of 'one', 'majority', 'all'"
        ))
    })
}

/// What every actor type carries, whatever its state and message types.
#[derive(Debug)]
pub struct Definition {
    pub name: String,
    pub body: Py<PyAny>,
    /// The body in Rust, for the types this process has one for. Nothing means the body on the loop runs.
    pub native: Option<std::sync::Arc<dyn crate::collections::Native>>,
    pub replicas: usize,
    pub write: Write,
    pub mailbox: Option<usize>,
    pub state: Py<Schema>,
    pub messages: Py<Schema>,
    /// The state a key starts from when its ref offers none; without it, the ref has to offer one.
    pub initial: Option<Py<PyAny>>,
}

impl Definition {
    /// The same type under another name, which is how a collection makes one per configuration.
    fn configured(&self, py: Python<'_>, name: &str, replicas: usize, write: Write) -> Self {
        Self {
            native: self.native.clone(),
            name: name.to_owned(),
            body: self.body.clone_ref(py),
            replicas,
            write,
            mailbox: self.mailbox,
            state: self.state.clone_ref(py),
            messages: self.messages.clone_ref(py),
            initial: self.initial.as_ref().map(|initial| initial.clone_ref(py)),
        }
    }

    fn new(
        py: Python<'_>,
        body: &Bound<'_, PyAny>,
        replicas: usize,
        write: Write,
        mailbox: Option<usize>,
        initial: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let name = named(body)?;
        if replicas < 1 {
            return Err(PyValueError::new_err(format!(
                "{name}: replicas is {replicas}, so the state would live nowhere"
            )));
        }
        if mailbox == Some(0) {
            return Err(PyValueError::new_err(format!(
                "{name}: mailbox is 0, so every message would be refused"
            )));
        }
        let (state, messages) = schemas(py, body, &name)?;
        Ok(Self {
            native: crate::collections::native(&name),
            name,
            body: body.clone().unbind(),
            replicas,
            write,
            mailbox,
            state,
            messages,
            initial,
        })
    }
}

/// An actor type whose keys are created only by `start`.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
pub struct Actor(pub Definition);

/// An actor type whose keys are created on demand with `initial`.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
pub struct DefaultedActor(pub Definition);

macro_rules! definition_methods {
    ($class:ident $($extra:tt)*) => {
        #[pymethods]
        impl $class {
            #[getter]
            fn name(&self) -> &str {
                &self.0.name
            }

            #[getter]
            fn body(&self) -> &Py<PyAny> {
                &self.0.body
            }

            #[getter]
            fn replicas(&self) -> usize {
                self.0.replicas
            }

            #[getter]
            fn write(&self) -> &'static str {
                self.0.write.name()
            }

            #[getter]
            fn mailbox(&self) -> Option<usize> {
                self.0.mailbox
            }

            #[classmethod]
            fn __class_getitem__<'py>(
                class: &Bound<'py, PyType>,
                item: &Bound<'py, PyAny>,
            ) -> PyResult<Bound<'py, PyAny>> {
                crate::generic::alias(class, &crate::generic::subscript(item))
            }

            /// The same type under another name, with the replicas and write level of a configured collection.
            fn configured(
                &self,
                py: Python<'_>,
                name: &str,
                replicas: usize,
                write: &str,
            ) -> PyResult<Self> {
                Ok(Self(self.0.configured(py, name, replicas, written(write)?)))
            }

            fn __repr__(&self) -> String {
                format!("{}({})", stringify!($class), self.0.name)
            }

            $($extra)*
        }
    };
}

definition_methods!(Actor);
definition_methods!(DefaultedActor
    #[getter]
    fn initial(&self) -> Option<&Py<PyAny>> {
        self.0.initial.as_ref()
    }
);

/// An actor type as a system holds it, whichever of the two classes it is.
#[derive(Debug)]
pub enum Behavior {
    Plain(Py<Actor>),
    Defaulted(Py<DefaultedActor>),
}

impl Behavior {
    pub fn of(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(plain) = value.cast::<Actor>() {
            return Ok(Self::Plain(plain.clone().unbind()));
        }
        if let Ok(defaulted) = value.cast::<DefaultedActor>() {
            return Ok(Self::Defaulted(defaulted.clone().unbind()));
        }
        Err(PyTypeError::new_err(format!(
            "{} is not an actor type; decorate its body with @actor",
            value.get_type().name()?
        )))
    }

    /// The type as Python holds it, which is one of the two classes.
    #[must_use]
    pub fn held(&self, py: Python<'_>) -> Py<PyAny> {
        match self {
            Self::Plain(actor) => actor.clone_ref(py).into_any(),
            Self::Defaulted(actor) => actor.clone_ref(py).into_any(),
        }
    }

    #[must_use]
    pub fn definition(&self) -> &Definition {
        match self {
            Self::Plain(actor) => &actor.get().0,
            Self::Defaulted(actor) => &actor.get().0,
        }
    }

    #[must_use]
    pub fn clone_ref(&self, py: Python<'_>) -> Self {
        match self {
            Self::Plain(actor) => Self::Plain(actor.clone_ref(py)),
            Self::Defaulted(actor) => Self::Defaulted(actor.clone_ref(py)),
        }
    }

    #[must_use]
    pub fn object<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        match self {
            Self::Plain(actor) => actor.bind(py).clone().into_any(),
            Self::Defaulted(actor) => actor.bind(py).clone().into_any(),
        }
    }
}

/// Define an actor type from its body.
///
/// The type is named after where its body lives, `module:qualname`. That is what nodes tell each other, and how a
/// node that never used the type finds it: every member of a cluster runs the same code.
#[pyfunction]
#[pyo3(signature = (body = None, /, *, initial = None, replicas = 3, write = "majority", mailbox = None))]
pub fn actor(
    py: Python<'_>,
    body: Option<&Bound<'_, PyAny>>,
    initial: Option<&Bound<'_, PyAny>>,
    replicas: usize,
    write: &str,
    mailbox: Option<usize>,
) -> PyResult<Py<PyAny>> {
    let write = written(write)?;
    if let Some(body) = body {
        let defined = Actor(Definition::new(py, body, replicas, write, mailbox, None)?);
        return Ok(Bound::new(py, defined)?.into_any().unbind());
    }
    let decorator = Decorator {
        initial: initial.map(|value| value.clone().unbind()),
        replicas,
        write,
        mailbox,
    };
    Ok(Bound::new(py, decorator)?.into_any().unbind())
}

/// `@actor(...)` before it meets the body it decorates.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
struct Decorator {
    initial: Option<Py<PyAny>>,
    replicas: usize,
    write: Write,
    mailbox: Option<usize>,
}

#[pymethods]
impl Decorator {
    fn __call__(&self, py: Python<'_>, body: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let initial = self.initial.as_ref().map(|value| value.clone_ref(py));
        let defined = Definition::new(py, body, self.replicas, self.write, self.mailbox, initial)?;
        Ok(match self.initial {
            None => Bound::new(py, Actor(defined))?.into_any().unbind(),
            Some(_) => Bound::new(py, DefaultedActor(defined))?.into_any().unbind(),
        })
    }
}

/// The state and message schemas of `body`, from the `Context[S, M]` its parameter is annotated as.
fn schemas(
    py: Python<'_>,
    body: &Bound<'_, PyAny>,
    name: &str,
) -> PyResult<(Py<Schema>, Py<Schema>)> {
    let introspect = Introspect::new(py, py.get_type::<crate::refs::Ref>().into_any())?;
    let parameters = match introspect.parameters(body) {
        Ok(parameters) => parameters,
        Err(raised) if raised.is_instance_of::<pyo3::exceptions::PyNameError>(py) => {
            let why = raised.value(py).str()?.to_str()?.to_owned();
            return Err(Failure::Schema(SchemaError::new(format!("{name}: {why}"))).into());
        }
        Err(raised) => return Err(raised),
    };
    let shape = || {
        PyTypeError::new_err(format!(
            "the parameter of {name} must be annotated as Context[S, M]"
        ))
    };
    let [only] = parameters.as_slice() else {
        return Err(shape());
    };
    if !introspect.origin(only)?.is(py.get_type::<Context>()) {
        return Err(shape());
    }
    let args = introspect.args(only)?;
    let [state, messages] = args.as_slice() else {
        return Err(shape());
    };
    Ok((
        compiled(py, &introspect, state, "state")?,
        compiled(py, &introspect, messages, "message")?,
    ))
}

fn compiled(
    py: Python<'_>,
    introspect: &Introspect<'_>,
    annotation: &Bound<'_, PyAny>,
    role: &str,
) -> PyResult<Py<Schema>> {
    let built: Outcome<Schema> = Schema::compiled(py, introspect, annotation, false);
    match built {
        Ok(schema) => Ok(Bound::new(py, schema)?.unbind()),
        Err(failure) => Err(failure.under(role).into()),
    }
}

fn named(body: &Bound<'_, PyAny>) -> PyResult<String> {
    let module: String = body.getattr("__module__")?.extract()?;
    let qualname: String = body.getattr("__qualname__")?.extract()?;
    Ok(format!("{module}:{qualname}"))
}
