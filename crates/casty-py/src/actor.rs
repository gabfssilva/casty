//! An actor type, which is a body and what the system needs to know about it.

use core::time::Duration;

use casty_core::mailbox::{Backoff, OnFull};
use casty_core::replication::messages::Write;
use casty_core::schema::SchemaError;
use casty_core::store::Durable;
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

/// What a full mailbox does with a message, as the decorator writes it.
fn when_full(on_full: &str) -> PyResult<OnFull> {
    OnFull::of(on_full).ok_or_else(|| {
        PyValueError::new_err(format!(
            "on_full is {on_full:?}, not one of 'refuse', 'wait'"
        ))
    })
}

/// Refuse a pinned type with more than one copy: the node its key names is the only one that keeps it.
fn single_copy(name: &str, settings: &Settings) -> PyResult<()> {
    if settings.pinned && settings.replicas > 1 {
        return Err(PyValueError::new_err(format!(
            "{name}: replicas is {}, and a pinned type keeps its one copy on the node its key names",
            settings.replicas
        )));
    }
    Ok(())
}

/// A period the API takes as a `timedelta`, named in the error when it is negative.
pub fn period(parameter: &str, value: &Bound<'_, PyAny>) -> PyResult<Duration> {
    value.extract::<Duration>().map_err(|error| {
        if error.is_instance_of::<PyValueError>(value.py()) {
            PyValueError::new_err(format!("{parameter} is {value}, which is negative"))
        } else {
            error
        }
    })
}

/// When the store of the system keeps the writes of the type, as the decorator writes it: `"write"` or a period.
fn durability(durable: &Bound<'_, PyAny>) -> PyResult<Durable> {
    if let Ok(named) = durable.extract::<String>() {
        return match named.as_str() {
            "write" => Ok(Durable::Write),
            _ => Err(PyValueError::new_err(format!(
                "durable is {named:?}, not 'write', a timedelta or None"
            ))),
        };
    }
    match durable.extract::<Duration>() {
        Ok(every) => Ok(Durable::Every(every)),
        Err(error) if error.is_instance_of::<PyValueError>(durable.py()) => Err(
            PyValueError::new_err(format!("durable is {durable}, which is negative")),
        ),
        Err(_) => Err(PyTypeError::new_err(format!(
            "durable is {durable}, not 'write', a timedelta or None"
        ))),
    }
}

/// A `casty.Backoff`, as a type or a system declares it.
pub fn backed_off(backoff: &Bound<'_, PyAny>) -> PyResult<Backoff> {
    Ok(Backoff {
        first: period("backoff.first", &backoff.getattr("first")?)?,
        limit: period("backoff.limit", &backoff.getattr("limit")?)?,
        factor: backoff.getattr("factor")?.extract()?,
    })
}

/// What a type sets for itself with `@actor`, besides its body and its initial state.
///
/// A timing it leaves unset is the one of the system that runs it. The same type can run on systems of different
/// settings, so the definition holds only what the type declared, and `over` is what an activation goes by.
#[derive(Debug, Clone, Copy)]
pub struct Settings {
    /// Whether a key names the node it runs on, instead of the ring placing it.
    pub pinned: bool,
    pub replicas: usize,
    pub write: Write,
    pub mailbox: Option<usize>,
    /// What a full mailbox does with a message, which the caller reads from its own copy of the definition too: an
    /// `ask` that is held keeps its message to send again.
    pub on_full: OnFull,
    /// How many runs of the body read the mailbox at once. Above one, the state is read-only.
    pub concurrency: usize,
    pub idle_after: Option<Duration>,
    /// The deadline of an `ask` to the type, which the caller reads from its own copy of the definition.
    pub ask_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
    pub backoff: Option<Backoff>,
    /// When the store of the system keeps the writes of the type; nothing keeps them in memory only. Like `replicas`
    /// and `write`, it is the one of the type a key started as, which its writes go under.
    pub durable: Option<Durable>,
}

impl Settings {
    /// The settings an activation of the type goes by on a system that runs by `system`.
    #[must_use]
    pub fn over(&self, system: &crate::node::Settings) -> crate::node::Settings {
        crate::node::Settings {
            idle_after: self.idle_after.unwrap_or(system.idle_after),
            ask_timeout: self.ask_timeout.unwrap_or(system.ask_timeout),
            write_timeout: self.write_timeout.unwrap_or(system.write_timeout),
            backoff: self.backoff.unwrap_or(system.backoff),
            ..*system
        }
    }
}

/// What every actor type carries, whatever its state and message types.
#[derive(Debug)]
pub struct Definition {
    pub name: String,
    pub body: Py<PyAny>,
    /// The body in Rust, for the types this process has one for. Nothing means the body on the loop runs.
    pub native: Option<std::sync::Arc<dyn crate::collections::Native>>,
    pub settings: Settings,
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
            settings: Settings {
                replicas,
                write,
                ..self.settings
            },
            state: self.state.clone_ref(py),
            messages: self.messages.clone_ref(py),
            initial: self.initial.as_ref().map(|initial| initial.clone_ref(py)),
        }
    }

    fn new(
        py: Python<'_>,
        body: &Bound<'_, PyAny>,
        settings: Settings,
        initial: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let name = named(body)?;
        if settings.replicas < 1 {
            return Err(PyValueError::new_err(format!(
                "{name}: replicas is {}, so the state would live nowhere",
                settings.replicas
            )));
        }
        single_copy(&name, &settings)?;
        if settings.mailbox == Some(0) {
            return Err(PyValueError::new_err(format!(
                "{name}: mailbox is 0, so every message would be refused"
            )));
        }
        if settings.on_full == OnFull::Wait && settings.mailbox.is_none() {
            return Err(PyValueError::new_err(format!(
                "{name}: on_full is 'wait', and a mailbox without a bound is never full; set mailbox"
            )));
        }
        if settings.concurrency == 0 {
            return Err(PyValueError::new_err(format!(
                "{name}: concurrency is 0, so no message would be read"
            )));
        }
        let native = crate::collections::native(&name);
        // The protocols of the collections rely on their body taking one message at a time.
        if native.is_some() && settings.concurrency > 1 {
            return Err(PyValueError::new_err(format!(
                "{name}: concurrency is {}, and the body of a collection takes one message at a time",
                settings.concurrency
            )));
        }
        let (state, messages) = schemas(py, body, &name)?;
        Ok(Self {
            native,
            name,
            body: body.clone().unbind(),
            settings,
            state,
            messages,
            initial,
        })
    }

    /// What a node of a cluster needs of the type to place its keys and write their state.
    #[must_use]
    pub fn kind(&self) -> casty_node::node::Kind {
        casty_node::node::Kind {
            actor: self.name.clone(),
            replicas: self.settings.replicas,
            write: self.settings.write,
            write_timeout: self.settings.write_timeout,
            pinned: self.settings.pinned,
            durable: self.settings.durable,
        }
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
            fn pinned(&self) -> bool {
                self.0.settings.pinned
            }

            #[getter]
            fn replicas(&self) -> usize {
                self.0.settings.replicas
            }

            #[getter]
            fn write(&self) -> &'static str {
                self.0.settings.write.name()
            }

            #[getter]
            fn mailbox(&self) -> Option<usize> {
                self.0.settings.mailbox
            }

            #[getter]
            fn on_full(&self) -> &'static str {
                self.0.settings.on_full.name()
            }

            #[getter]
            fn concurrency(&self) -> usize {
                self.0.settings.concurrency
            }

            #[getter]
            fn idle_after(&self) -> Option<Duration> {
                self.0.settings.idle_after
            }

            #[getter]
            fn ask_timeout(&self) -> Option<Duration> {
                self.0.settings.ask_timeout
            }

            #[getter]
            fn write_timeout(&self) -> Option<Duration> {
                self.0.settings.write_timeout
            }

            #[getter]
            fn backoff<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
                self.0
                    .settings
                    .backoff
                    .map(|set| {
                        py.import("casty")?
                            .getattr("Backoff")?
                            .call1((set.first, set.limit, set.factor))
                    })
                    .transpose()
            }

            #[getter]
            fn durable<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
                self.0
                    .settings
                    .durable
                    .map(|durable| match durable {
                        Durable::Write => Ok(pyo3::types::PyString::new(py, "write").into_any()),
                        Durable::Every(every) => Ok(every.into_pyobject(py)?.into_any()),
                    })
                    .transpose()
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
                let configured = self.0.configured(py, name, replicas, written(write)?);
                single_copy(&configured.name, &configured.settings)?;
                Ok(Self(configured))
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
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (
    body = None,
    /,
    *,
    initial = Initial::Unset,
    pinned = false,
    replicas = None,
    write = "majority",
    mailbox = None,
    on_full = "refuse",
    concurrency = 1,
    idle_after = None,
    ask_timeout = None,
    write_timeout = None,
    backoff = None,
    durable = None,
))]
pub fn actor(
    py: Python<'_>,
    body: Option<&Bound<'_, PyAny>>,
    initial: Initial,
    pinned: bool,
    replicas: Option<usize>,
    write: &str,
    mailbox: Option<usize>,
    on_full: &str,
    concurrency: usize,
    idle_after: Option<&Bound<'_, PyAny>>,
    ask_timeout: Option<&Bound<'_, PyAny>>,
    write_timeout: Option<&Bound<'_, PyAny>>,
    backoff: Option<&Bound<'_, PyAny>>,
    durable: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let settings = Settings {
        pinned,
        replicas: replicas.unwrap_or(if pinned { 1 } else { 3 }),
        write: written(write)?,
        mailbox,
        on_full: when_full(on_full)?,
        concurrency,
        idle_after: idle_after
            .map(|value| period("idle_after", value))
            .transpose()?,
        ask_timeout: ask_timeout
            .map(|value| period("ask_timeout", value))
            .transpose()?,
        write_timeout: write_timeout
            .map(|value| period("write_timeout", value))
            .transpose()?,
        backoff: backoff.map(backed_off).transpose()?,
        durable: durable
            .filter(|durable| !durable.is_none())
            .map(durability)
            .transpose()?,
    };
    if let Some(body) = body {
        let defined = Actor(Definition::new(py, body, settings, None)?);
        return Ok(Bound::new(py, defined)?.into_any().unbind());
    }
    let decorator = Decorator {
        initial: match initial {
            Initial::Unset => None,
            Initial::Given(value) => Some(value),
        },
        settings,
    };
    Ok(Bound::new(py, decorator)?.into_any().unbind())
}

/// The `initial` of `@actor`, told apart from none: `initial=None` is a default like any other.
#[derive(Debug)]
pub enum Initial {
    Unset,
    Given(Py<PyAny>),
}

impl<'a, 'py> FromPyObject<'a, 'py> for Initial {
    type Error = PyErr;

    fn extract(obj: pyo3::Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        Ok(Self::Given(obj.to_owned().unbind()))
    }
}

/// `@actor(...)` before it meets the body it decorates.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
struct Decorator {
    initial: Option<Py<PyAny>>,
    settings: Settings,
}

#[pymethods]
impl Decorator {
    fn __call__(&self, py: Python<'_>, body: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let initial = self.initial.as_ref().map(|value| value.clone_ref(py));
        let defined = Definition::new(py, body, self.settings, initial)?;
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
    let built: Outcome<Schema> = Schema::compiled(introspect, annotation, false);
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
