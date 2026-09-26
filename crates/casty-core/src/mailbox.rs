//! What waits for an activation, in arrival order.

use std::collections::VecDeque;

use crate::chain::Chain;
use crate::node::Target;

/// A message for a key, encoded with the message schema of its type, and where its `ask` waits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deliver {
    pub actor: String,
    pub key: String,
    pub message: Vec<u8>,
    pub reply: Option<Target>,
    /// The bodies waiting for the answer of this `ask`, down to the one that sent it. A `tell` keeps nobody waiting.
    pub chain: Chain,
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

/// What a bounded mailbox does with a message that finds it full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnFull {
    /// Refuse it: its `ask` hears `Full`, and a `tell` is dropped.
    Refuse,
    /// Hold the caller of an `ask` until there is room, and call it back with `Full` then, which it answers by sending
    /// the message again. A `tell` has no caller to hold and is dropped.
    Wait,
}

impl OnFull {
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Refuse => "refuse",
            Self::Wait => "wait",
        }
    }

    #[must_use]
    pub fn of(named: &str) -> Option<Self> {
        match named {
            "refuse" => Some(Self::Refuse),
            "wait" => Some(Self::Wait),
            _ => None,
        }
    }
}

/// What became of a message put in a mailbox.
#[derive(Debug, PartialEq, Eq)]
pub enum Put {
    Queued,
    /// The mailbox is full and refuses it.
    Refused(Deliver),
    /// The mailbox is full and holds its caller until there is room. The message itself is not kept: the sender still
    /// has it, so a caller that waits costs its own node the message and not this one.
    Held,
}

/// What withdrawing a request whose caller stopped waiting found of it.
#[derive(Debug, PartialEq, Eq)]
pub enum Withdrawn {
    /// It was queued and is not any more. The place it leaves goes to the callers returned, as a take's would.
    Queued(Vec<Target>),
    /// Its caller was held for room and is not any more.
    Held,
    /// Neither: it was taken already, or it has not arrived.
    Absent,
}

/// The messages waiting for an activation, in arrival order.
///
/// With a capacity, a full mailbox refuses the new message or holds its caller: dropping the oldest would remove one
/// the sender already considers delivered.
#[derive(Debug)]
pub struct Mailbox {
    capacity: Option<usize>,
    on_full: OnFull,
    /// Each message queued, numbered in the order it was.
    waiting: VecDeque<(u64, Deliver)>,
    /// Where the `ask`s that found the mailbox full wait, in arrival order.
    held: VecDeque<Target>,
    /// How many messages were queued so far, which is the number the next one takes.
    arrived: u64,
}

impl Mailbox {
    #[must_use]
    pub fn new(capacity: Option<usize>, on_full: OnFull) -> Self {
        Self {
            capacity,
            on_full,
            waiting: VecDeque::new(),
            held: VecDeque::new(),
            arrived: 0,
        }
    }

    #[must_use]
    pub fn empty(&self) -> bool {
        self.waiting.is_empty()
    }

    /// How many messages are waiting.
    #[must_use]
    pub fn queued(&self) -> usize {
        self.waiting.len()
    }

    /// How many messages were queued so far. Whatever arrives beside the mailbox marks when it did with it.
    #[must_use]
    pub fn arrived(&self) -> u64 {
        self.arrived
    }

    /// Whether a message queued before `mark`, a count `arrived` gave, still waits.
    #[must_use]
    pub fn queued_before(&self, mark: u64) -> bool {
        self.waiting.front().is_some_and(|(at, _)| *at < mark)
    }

    /// Queue `deliver`, unless the mailbox is full.
    pub fn put(&mut self, deliver: Deliver) -> Put {
        if self
            .capacity
            .is_none_or(|capacity| self.waiting.len() < capacity)
        {
            self.waiting.push_back((self.arrived, deliver));
            self.arrived += 1;
            return Put::Queued;
        }
        if self.on_full == OnFull::Wait
            && let Some(caller) = &deliver.reply
        {
            self.held.push_back(caller.clone());
            return Put::Held;
        }
        Put::Refused(deliver)
    }

    /// The next message, and the callers held for room that are called back now.
    ///
    /// A message taken makes room for one. When there is nothing to take, every caller held is called back: the reader
    /// takes nothing more until a message arrives, and a caller called back before may have stopped waiting and never
    /// send again, so one left held could wait for room that is already there.
    pub fn take(&mut self) -> (Option<Deliver>, Vec<Target>) {
        let deliver = self.waiting.pop_front().map(|(_, deliver)| deliver);
        let called = match deliver {
            Some(_) => self.held.pop_front().into_iter().collect(),
            None => self.held.drain(..).collect(),
        };
        (deliver, called)
    }

    /// Everything queued, in arrival order, and every caller held: what an activation that ends leaves behind.
    ///
    /// The callers come out last and all at once, so that what they send again cannot overtake what was queued before
    /// them.
    pub fn drain(&mut self) -> (Vec<Deliver>, Vec<Target>) {
        (
            self.waiting.drain(..).map(|(_, deliver)| deliver).collect(),
            self.held.drain(..).collect(),
        )
    }

    /// Take out the request whose answer goes to `caller`, queued or held, because nobody waits for it any more.
    pub fn withdraw(&mut self, caller: &Target) -> Withdrawn {
        if let Some(at) = self
            .waiting
            .iter()
            .position(|(_, deliver)| deliver.reply.as_ref() == Some(caller))
        {
            self.waiting.remove(at);
            return Withdrawn::Queued(self.held.pop_front().into_iter().collect());
        }
        if let Some(at) = self.held.iter().position(|held| held == caller) {
            self.held.remove(at);
            return Withdrawn::Held;
        }
        Withdrawn::Absent
    }
}

#[cfg(test)]
mod tests {
    use super::{Deliver, Mailbox, OnFull, Put, Withdrawn};
    use crate::chain::Chain;
    use crate::node::{NodeId, Target};

    fn deliver(entry: u8) -> Deliver {
        Deliver {
            actor: "a".to_owned(),
            key: "k".to_owned(),
            message: vec![entry],
            reply: None,
            chain: Chain::default(),
        }
    }

    fn caller(id: i64) -> Target {
        Target::Reply {
            node: NodeId {
                address: None,
                incarnation: [7; 16],
            },
            id,
        }
    }

    fn asked(entry: u8) -> Deliver {
        Deliver {
            reply: Some(caller(i64::from(entry))),
            ..deliver(entry)
        }
    }

    fn taken(mailbox: &mut Mailbox) -> Option<Vec<u8>> {
        mailbox.take().0.map(|taken| taken.message)
    }

    #[test]
    fn keeps_arrival_order_and_refuses_what_does_not_fit() {
        let mut mailbox = Mailbox::new(Some(2), OnFull::Refuse);
        assert!(mailbox.empty());
        assert_eq!(mailbox.put(deliver(1)), Put::Queued);
        assert_eq!(mailbox.put(asked(2)), Put::Queued);
        // Full refuses the new one: dropping the oldest would lose one the sender already considers delivered.
        assert_eq!(mailbox.put(asked(3)), Put::Refused(asked(3)));
        assert_eq!(mailbox.put(deliver(4)), Put::Refused(deliver(4)));
        assert_eq!(taken(&mut mailbox), Some(vec![1]));
        assert_eq!(mailbox.put(deliver(3)), Put::Queued);
        assert_eq!(taken(&mut mailbox), Some(vec![2]));
        assert_eq!(taken(&mut mailbox), Some(vec![3]));
        assert_eq!(mailbox.take(), (None, Vec::new()));
        assert!(mailbox.empty());
    }

    #[test]
    fn waiting_holds_the_callers_that_do_not_fit_and_calls_them_back_as_room_is_made() {
        let mut mailbox = Mailbox::new(Some(1), OnFull::Wait);
        assert_eq!(mailbox.put(asked(1)), Put::Queued);
        assert_eq!(mailbox.put(asked(2)), Put::Held);
        assert_eq!(mailbox.put(asked(3)), Put::Held);
        // A `tell` has nobody to call back, so it is refused as it would be without waiting.
        assert_eq!(mailbox.put(deliver(4)), Put::Refused(deliver(4)));
        assert_eq!(mailbox.queued(), 1);

        // Each message taken calls back the caller held longest, for the one place it leaves.
        assert_eq!(mailbox.take(), (Some(asked(1)), vec![caller(2)]));
        assert_eq!(mailbox.put(asked(2)), Put::Queued);
        assert_eq!(mailbox.take(), (Some(asked(2)), vec![caller(3)]));

        assert_eq!(mailbox.put(asked(5)), Put::Queued);
        assert_eq!(mailbox.put(asked(6)), Put::Held);
        assert_eq!(mailbox.put(asked(7)), Put::Held);
        assert_eq!(mailbox.take(), (Some(asked(5)), vec![caller(6)]));
        // 6 never sends again, so the reader finds nothing, and whoever is still held is called back.
        assert_eq!(mailbox.take(), (None, vec![caller(7)]));
        assert_eq!(mailbox.take(), (None, Vec::new()));
    }

    #[test]
    fn an_ending_mailbox_gives_what_is_queued_before_whoever_is_held() {
        let mut mailbox = Mailbox::new(Some(2), OnFull::Wait);
        for entry in 1..=4 {
            mailbox.put(asked(entry));
        }
        assert_eq!(
            mailbox.drain(),
            (vec![asked(1), asked(2)], vec![caller(3), caller(4)])
        );
        assert_eq!(mailbox.take(), (None, Vec::new()));
    }

    #[test]
    fn a_request_nobody_waits_for_leaves_the_queue_or_the_callers_held_and_nothing_else() {
        let mut mailbox = Mailbox::new(Some(2), OnFull::Wait);
        for entry in 1..=4 {
            mailbox.put(asked(entry));
        }
        // Queued: it goes, and the place it leaves is the first held caller's, as a take's would be.
        assert_eq!(
            mailbox.withdraw(&caller(2)),
            Withdrawn::Queued(vec![caller(3)])
        );
        assert_eq!(mailbox.queued(), 1);
        // Held: the caller goes at once, instead of being called back for room it no longer wants.
        assert_eq!(mailbox.withdraw(&caller(4)), Withdrawn::Held);
        // Taken, answered or never arrived: there is nothing of it here.
        assert_eq!(mailbox.withdraw(&caller(2)), Withdrawn::Absent);
        assert_eq!(mailbox.withdraw(&caller(9)), Withdrawn::Absent);
        assert_eq!(mailbox.take(), (Some(asked(1)), Vec::new()));
        assert_eq!(mailbox.take(), (None, Vec::new()));
    }

    #[test]
    fn a_tell_is_never_withdrawn() {
        let mut mailbox = Mailbox::new(None, OnFull::Refuse);
        mailbox.put(deliver(1));
        assert_eq!(mailbox.withdraw(&caller(1)), Withdrawn::Absent);
        assert_eq!(mailbox.queued(), 1);
    }

    #[test]
    fn tells_whether_a_message_queued_before_a_mark_still_waits() {
        let mut mailbox = Mailbox::new(Some(2), OnFull::Refuse);
        mailbox.put(deliver(1));
        let mark = mailbox.arrived();
        mailbox.put(deliver(2));
        // Refused: it never queued, so it takes no number.
        mailbox.put(deliver(3));
        assert_eq!(mailbox.arrived(), 2);
        assert!(mailbox.queued_before(mark));
        assert_eq!(taken(&mut mailbox), Some(vec![1]));
        assert!(!mailbox.queued_before(mark));
        assert!(mailbox.queued_before(mailbox.arrived()));
        assert_eq!(taken(&mut mailbox), Some(vec![2]));
        assert!(!mailbox.queued_before(mailbox.arrived()));
    }

    #[test]
    fn without_a_capacity_it_takes_whatever_arrives() {
        let mut mailbox = Mailbox::new(None, OnFull::Wait);
        for entry in 0..1_000 {
            assert_eq!(
                mailbox.put(deliver(u8::try_from(entry % 256).unwrap())),
                Put::Queued
            );
        }
        assert!(!mailbox.empty());
    }
}
