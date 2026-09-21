//! What waits for an activation, in arrival order.

use std::collections::VecDeque;
use std::time::Duration;

use crate::node::Target;

/// A message for a key, encoded with the message schema of its type, and where its `ask` waits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deliver {
    pub actor: String,
    pub key: String,
    pub message: Vec<u8>,
    pub reply: Option<Target>,
}

/// The creation of a key and its activation, which is what obtaining a ref asks for. Nobody waits for it.
///
/// `state` is the encoded state the key starts from when it has none; without it, the default of the type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Start {
    pub actor: String,
    pub key: String,
    pub state: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Deliver(Deliver),
    Start(Start),
}

impl Command {
    #[must_use]
    pub fn actor(&self) -> &str {
        match self {
            Self::Deliver(deliver) => &deliver.actor,
            Self::Start(start) => &start.actor,
        }
    }

    #[must_use]
    pub fn key(&self) -> &str {
        match self {
            Self::Deliver(deliver) => &deliver.key,
            Self::Start(start) => &start.key,
        }
    }

    #[must_use]
    pub fn reply(&self) -> Option<&Target> {
        match self {
            Self::Deliver(deliver) => deliver.reply.as_ref(),
            Self::Start(_) => None,
        }
    }
}

/// The messages waiting for an activation, in arrival order.
///
/// With a capacity, a full mailbox refuses the new message: dropping the oldest would remove one the sender already
/// considers delivered.
#[derive(Debug)]
pub struct Mailbox {
    capacity: Option<usize>,
    waiting: VecDeque<Deliver>,
}

impl Mailbox {
    #[must_use]
    pub fn new(capacity: Option<usize>) -> Self {
        Self {
            capacity,
            waiting: VecDeque::new(),
        }
    }

    #[must_use]
    pub fn empty(&self) -> bool {
        self.waiting.is_empty()
    }

    /// Queue `deliver`, unless the mailbox is full.
    pub fn put(&mut self, deliver: Deliver) -> bool {
        if self
            .capacity
            .is_some_and(|capacity| self.waiting.len() >= capacity)
        {
            return false;
        }
        self.waiting.push_back(deliver);
        true
    }

    pub fn take(&mut self) -> Option<Deliver> {
        self.waiting.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::{Deliver, Mailbox};

    fn deliver(entry: u8) -> Deliver {
        Deliver {
            actor: "a".to_owned(),
            key: "k".to_owned(),
            message: vec![entry],
            reply: None,
        }
    }

    #[test]
    fn keeps_arrival_order_and_refuses_what_does_not_fit() {
        let mut mailbox = Mailbox::new(Some(2));
        assert!(mailbox.empty());
        assert!(mailbox.put(deliver(1)));
        assert!(mailbox.put(deliver(2)));
        // Full refuses the new one: dropping the oldest would lose one the sender already considers delivered.
        assert!(!mailbox.put(deliver(3)));
        assert_eq!(mailbox.take().map(|taken| taken.message), Some(vec![1]));
        assert!(mailbox.put(deliver(3)));
        assert_eq!(mailbox.take().map(|taken| taken.message), Some(vec![2]));
        assert_eq!(mailbox.take().map(|taken| taken.message), Some(vec![3]));
        assert!(mailbox.take().is_none());
        assert!(mailbox.empty());
    }

    #[test]
    fn without_a_capacity_it_takes_whatever_arrives() {
        let mut mailbox = Mailbox::new(None);
        for entry in 0..1_000 {
            assert!(mailbox.put(deliver(u8::try_from(entry % 256).unwrap())));
        }
        assert!(!mailbox.empty());
    }
}

/// Delay before trying again: `first`, multiplied by `factor` on each consecutive failure, up to `limit`.
///
/// A body that raised waits this long before it runs again, and so does a node that is owed an answer.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Backoff {
    pub first: Duration,
    pub limit: Duration,
    pub factor: f64,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            first: Duration::from_millis(100),
            limit: Duration::from_secs(10),
            factor: 2.0,
        }
    }
}

impl Backoff {
    /// The delay after `held`, which never grows past the limit.
    #[must_use]
    pub fn next(&self, held: Duration) -> Duration {
        held.mul_f64(self.factor).min(self.limit)
    }
}
