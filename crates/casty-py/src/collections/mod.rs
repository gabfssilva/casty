//! The bodies of the collection types, which run in Rust instead of on the event loop.
//!
//! A collection is an actor like any other: the same messages on the wire, the same state in pages, the same writes
//! to the replicas. What is different is that its body never enters an interpreter, so a message it takes costs no
//! coroutine, no task and no turn of the loop.

pub mod barrier;
pub mod counter;
pub mod entry;
pub mod queue;
pub mod register;
pub mod semaphore;
pub mod table;

use std::sync::Arc;

use casty_core::node::Target;
use casty_core::schema::msgpack::Malformed;
use casty_core::store::Pages;
use casty_core::wire::{Reading, Result, Writer};

/// What a native body was given to work on.
#[derive(Debug)]
pub enum Given<'a> {
    /// A message from the mailbox, as it travelled.
    Message(&'a [u8]),
    /// The deadline the last turn asked for has passed, and no message came in the meantime.
    Alarm,
    /// The answer to what the last turn asked for, with the message that is still being worked on.
    Answered { message: &'a [u8], answer: &'a [u8] },
}

impl<'a> Given<'a> {
    /// The message the turn is on, which an answer comes back with and the alarm has none of.
    fn message(&self) -> Option<&'a [u8]> {
        match *self {
            Self::Message(message) | Self::Answered { message, .. } => Some(message),
            Self::Alarm => None,
        }
    }

    /// The answer to what the last turn asked for.
    fn answer(&self) -> Option<&'a [u8]> {
        match *self {
            Self::Answered { answer, .. } => Some(answer),
            Self::Message(_) | Self::Alarm => None,
        }
    }
}

/// What one message does: what it writes, what it answers, and what it waits for.
///
/// `ask` is the one thing that suspends a body: the rest of the message takes effect only once the answer arrives,
/// and the body sees the same message again with it.
#[derive(Debug, Default)]
pub struct Turn {
    pub save: Option<Pages>,
    /// Delete the state of the key instead of writing one, at the write level of the type; `save` goes unwritten.
    /// The body goes on from `initial`, which is where the key starts from the next time it is activated.
    pub delete: bool,
    pub replies: Vec<(Target, Vec<u8>)>,
    /// When to run again with no message, for a body that has deadlines of its own.
    pub alarm: Option<f64>,
    pub ask: Option<Asking>,
}

/// A message written around the target its answer comes back to, which only the activation can mint.
pub type Addressed = Box<dyn Fn(&Target) -> Vec<u8> + Send + Sync>;

pub struct Asking {
    pub to: Target,
    pub message: Addressed,
}

impl core::fmt::Debug for Asking {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("Asking")
            .field("to", &self.to)
            .finish_non_exhaustive()
    }
}

/// A body that runs in Rust.
pub trait Native: Send + Sync + core::fmt::Debug + 'static {
    /// The pages a key of this type starts from.
    fn initial(&self) -> Pages;

    /// One message, the alarm going off, or the answer the last message was waiting for, and nothing for a message
    /// the body does not take.
    ///
    /// `at` is the wall clock in seconds, which only a body with deadlines reads.
    fn turn(&self, held: &Pages, given: &Given<'_>, at: f64) -> Option<Turn>;

    /// The turn `given` takes, where a message the body does not take changes nothing and answers no one.
    fn step(&self, held: &Pages, given: &Given<'_>, at: f64) -> Turn {
        self.turn(held, given, at).unwrap_or_default()
    }

    /// Whether this body ever asks for a deadline. One that does not is never woken without a message.
    fn timed(&self) -> bool {
        false
    }

    /// Whether a key that holds `held` when its activation ends keeps nothing a new activation would miss, and is
    /// deleted then instead of written back. By default, a key back at `initial`.
    fn disposable(&self, held: &Pages) -> bool {
        *held == self.initial()
    }
}

/// The body of `name`, when this process has a native one for it.
///
/// Only the types of the collections have one, and a name is a type only where it lives: an actor of another module
/// that happens to be called `counter` is a type of its own, with the body its author wrote.
#[must_use]
pub fn native(name: &str) -> Option<Arc<dyn Native>> {
    let kind = collection(name)?;
    match kind {
        "counter" => Some(Arc::new(counter::Counter)),
        "register" => Some(Arc::new(register::Register)),
        "entry" => Some(Arc::new(entry::Entry)),
        "table" => Some(Arc::new(table::Table)),
        "table_segment" => Some(Arc::new(table::Segment)),
        "queue" => Some(Arc::new(queue::Queue)),
        "queue_segment" => Some(Arc::new(queue::Segment)),
        "barrier" => Some(Arc::new(barrier::Barrier)),
        "semaphore" => Some(Arc::new(semaphore::Semaphore)),
        _ => None,
    }
}

/// The kind of collection `name` is a type of, as it is declared or as a configuration of it.
///
/// A collection is configured by replicas and write level, and each configuration is a type of its own named
/// `kind_replicas_level`. The body is the same for all of them.
fn collection(name: &str) -> Option<&str> {
    let (module, qualname) = name.split_once(':')?;
    if module != "casty.collections" {
        return None;
    }
    // The type as it is declared, which is the body of the namespace the kind is grouped under.
    if let Some(kind) = qualname.strip_suffix(".actor") {
        return Some(kind);
    }
    // A configuration of it, which is a type of its own so that two settings never share a key.
    let (held, level) = qualname.rsplit_once('_')?;
    if !matches!(level, "one" | "majority" | "all") {
        return None;
    }
    let (kind, replicas) = held.rsplit_once('_')?;
    if replicas.is_empty() || !replicas.chars().all(|digit| digit.is_ascii_digit()) {
        return None;
    }
    Some(kind)
}

/// The single page a state that is not a dataclass lives in.
pub const WHOLE: &str = ".";

/// A message as it travels: its name within its type, who it answers, and every other field, which `field` reads by
/// its name or steps over. Nothing when the message is not one, or answers no one.
fn fields(
    message: &[u8],
    field: impl FnMut(&str, &mut Reading<'_>) -> Result<()>,
) -> Option<(&str, Target)> {
    let (tag, reply) = told(message, field)?;
    Some((tag, reply?))
}

/// A message as `fields` reads it, where one without `reply_to` is a `tell` that answers no one.
///
/// The name within the type is the last part of the qualname the message travels under.
fn told(
    message: &[u8],
    mut field: impl FnMut(&str, &mut Reading<'_>) -> Result<()>,
) -> Option<(&str, Option<Target>)> {
    let mut reading = Reading::new(message);
    let tag = reading.tag().ok()?;
    let len = reading.fields().ok()?;
    let mut reply = None;
    for _ in 0..len {
        match reading.name().ok()? {
            "reply_to" => reply = Some(reading.target().ok()?),
            name => field(name, &mut reading).ok()?,
        }
    }
    Some((&tag[tag.rfind('.').map_or(0, |dot| dot + 1)..], reply))
}

/// `int`, as an answer or a page.
fn number(value: i64) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.int(value);
    writer.finish()
}

/// `bool`, as an answer or a page.
fn truth(value: bool) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.bool(value);
    writer.finish()
}

/// `None`, which is what a message that returns nothing answers.
fn nil() -> Vec<u8> {
    optional(None)
}

/// `bytes | None`, as an answer or a page.
fn optional(value: Option<&[u8]>) -> Vec<u8> {
    let mut writer = Writer::new();
    match value {
        Some(value) => writer.bytes(value),
        None => writer.nil(),
    }
    writer.finish()
}

/// A `bytes | None` field.
fn optional_bytes(reading: &mut Reading<'_>) -> Result<Option<Vec<u8>>> {
    if reading.nil()? {
        return Ok(None);
    }
    Ok(Some(reading.bytes()?))
}

/// A `UUID` field, which travels as its sixteen bytes.
fn uuid(reading: &mut Reading<'_>) -> Result<[u8; 16]> {
    <[u8; 16]>::try_from(reading.bytes()?.as_slice()).map_err(|_| Malformed::Truncated)
}

/// A count as the schema writes one, which is a signed number.
fn count(held: usize) -> i64 {
    i64::try_from(held).unwrap_or(i64::MAX)
}

/// The number the page `name` holds, or `default` when it holds none.
fn number_in(pages: &Pages, name: &str, default: i64) -> i64 {
    pages
        .get(name)
        .and_then(|page| Reading::new(page).int().ok())
        .unwrap_or(default)
}

/// The `bytes | None` the page `name` holds, and nothing when it holds none.
fn optional_in(pages: &Pages, name: &str) -> Option<Vec<u8>> {
    optional_bytes(&mut Reading::new(pages.get(name)?))
        .ok()
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::collection;

    #[test]
    fn a_collection_is_one_where_it_is_declared_and_under_every_configuration_of_it() {
        assert_eq!(
            collection("casty.collections:counter.actor"),
            Some("counter")
        );
        assert_eq!(
            collection("casty.collections:counter_3_majority"),
            Some("counter")
        );
        assert_eq!(
            collection("casty.collections:semaphore_1_one"),
            Some("semaphore")
        );
    }

    #[test]
    fn a_type_of_another_module_is_not_one_however_it_is_named() {
        assert_eq!(collection("benchmarks.actors:counter"), None);
        assert_eq!(collection("tests.app:queue"), None);
        assert_eq!(collection("casty.collections:counter_3_some"), None);
        assert_eq!(collection("casty.collections:counter_x_all"), None);
        assert_eq!(collection("counter"), None);
    }
}
