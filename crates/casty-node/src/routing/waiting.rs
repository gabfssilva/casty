//! The messages a node keeps until their key has an owner that takes them.
//!
//! A message waits here for one of two reasons. Its key is moving to this node and the range it is in has not arrived
//! yet: the node that had the key gave it up as soon as it saw the change, so the message waits for the range instead
//! of going back and forth between the two. Or the owner this node sent it to refused it, the views of the two nodes
//! being apart: it goes again a little later, once the change that set them apart has reached both.
//!
//! What waits for a key keeps the order it came in, and whatever of the key this node sends meanwhile waits behind it,
//! so the messages of one node to one key still arrive in the order they were sent. Nothing waits past its deadline,
//! and nothing more waits once `limit` messages do.
//!
//! Nothing here does I/O: the node says where each key is now, and sends on what is released.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use tokio::time::Instant;

use super::wire::{Cancel, Routed};

/// How many messages a node keeps at most.
pub const LIMIT: usize = 100_000;

/// How often a node looks again at what waits, for what is due to go and what is past its deadline.
const TICK: Duration = Duration::from_millis(10);

#[derive(Debug)]
struct Entry {
    routed: Routed,
    /// When it may go: at once for one that waits for its key, later for one that came back.
    after: Instant,
    /// When it gives up.
    until: Instant,
}

/// What leaves the queue of a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Released {
    /// It goes on, to wherever its key is now.
    Ready(Routed),
    /// Its deadline passed, and whoever waits for it is told that nobody took it.
    Expired(Routed),
}

/// The messages waiting on this node, in a queue per key.
#[derive(Debug)]
pub struct Waiting {
    keys: BTreeMap<(String, String), VecDeque<Entry>>,
    count: usize,
    limit: usize,
    /// When all of it was last looked at.
    looked: Option<Instant>,
}

impl Waiting {
    #[must_use]
    pub fn new(limit: usize) -> Self {
        Self {
            keys: BTreeMap::new(),
            count: 0,
            limit,
            looked: None,
        }
    }

    /// Whether messages for the key wait here, which what this node takes or sends for it next waits behind.
    #[must_use]
    pub fn holds(&self, actor: &str, key: &str) -> bool {
        self.keys.contains_key(&(actor.to_owned(), key.to_owned()))
    }

    /// Whether `limit` messages wait already, so that no more may.
    #[must_use]
    pub fn full(&self) -> bool {
        self.count >= self.limit
    }

    /// Keep `routed` behind what waits for its key, to go no sooner than `delay` from `now` and to give up `timeout`
    /// after `now`. Whoever keeps one asks whether it is `full` first.
    pub fn keep(&mut self, routed: Routed, now: Instant, delay: Duration, timeout: Duration) {
        if self.count == 0 {
            self.looked = Some(now);
        }
        let entity = (
            routed.command.actor().to_owned(),
            routed.command.key().to_owned(),
        );
        self.keys.entry(entity).or_default().push_back(Entry {
            routed,
            after: now + delay,
            until: now + timeout,
        });
        self.count += 1;
    }

    /// Drop the request `cancel` names, and say whether it was waiting here.
    pub fn cancel(&mut self, cancel: &Cancel) -> bool {
        let entity = (cancel.actor.clone(), cancel.key.clone());
        let Some(queue) = self.keys.get_mut(&entity) else {
            return false;
        };
        let before = queue.len();
        queue.retain(|entry| entry.routed.command.reply() != Some(&cancel.request));
        let dropped = before - queue.len();
        if queue.is_empty() {
            self.keys.remove(&entity);
        }
        self.count -= dropped;
        dropped > 0
    }

    /// The keys something waits for.
    #[must_use]
    pub fn keys(&self) -> Vec<(String, String)> {
        self.keys.keys().cloned().collect()
    }

    /// What leaves the queue of `(actor, key)` at `now`: what is past its deadline, and, unless the key is still
    /// `holding` its messages here, what is due, in order, up to the first that is not.
    ///
    /// Every message of a key waits as long as it, so the ones past their deadline are always at the front.
    pub fn release(
        &mut self,
        actor: &str,
        key: &str,
        holding: bool,
        now: Instant,
    ) -> Vec<Released> {
        let entity = (actor.to_owned(), key.to_owned());
        let Some(queue) = self.keys.get_mut(&entity) else {
            return Vec::new();
        };
        let mut released = Vec::new();
        while let Some(front) = queue.front() {
            let expired = front.until <= now;
            if !expired && (holding || front.after > now) {
                break;
            }
            let Some(entry) = queue.pop_front() else {
                break;
            };
            released.push(if expired {
                Released::Expired(entry.routed)
            } else {
                Released::Ready(entry.routed)
            });
        }
        if queue.is_empty() {
            self.keys.remove(&entity);
        }
        self.count -= released.len();
        released
    }

    /// Note that every key was looked at `now`.
    pub fn looked(&mut self, now: Instant) {
        self.looked = Some(now);
    }

    /// When to look at every key again: a tick after the last look, and never while nothing waits.
    #[must_use]
    pub fn due(&self) -> Option<Instant> {
        self.looked
            .filter(|_| self.count > 0)
            .map(|looked| looked + TICK)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use casty_core::chain::Chain;
    use casty_core::mailbox::{Command, Deliver};
    use casty_core::node::{NodeId, Target};
    use casty_core::rolls::Rolls;
    use tokio::time::Instant;

    use super::{Released, Waiting};
    use crate::routing::wire::{Cancel, Routed};

    const ACTOR: &str = "tests.app:account";
    const TIMEOUT: Duration = Duration::from_secs(5);

    fn node(seed: u64) -> NodeId {
        Rolls::seeded(seed).nodes(1).remove(0)
    }

    fn routed(origin: &NodeId, key: &str, id: i64) -> Routed {
        Routed {
            command: Command::Deliver(Deliver {
                actor: ACTOR.to_owned(),
                key: key.to_owned(),
                message: vec![1],
                reply: Some(Target::Reply {
                    node: origin.clone(),
                    id,
                }),
                chain: Chain::default(),
            }),
            origin: origin.clone(),
            attempt: 1,
        }
    }

    #[test]
    fn what_waits_for_a_key_stays_while_the_key_holds_it_and_goes_in_order_once_it_does_not() {
        let origin = &node(71);
        let now = Instant::now();
        let mut waiting = Waiting::new(10);
        for id in 0..3 {
            waiting.keep(routed(origin, "acc-1", id), now, Duration::ZERO, TIMEOUT);
        }
        assert!(waiting.holds(ACTOR, "acc-1"));
        assert!(!waiting.holds(ACTOR, "acc-2"));

        assert_eq!(waiting.release(ACTOR, "acc-1", true, now), Vec::new());
        assert_eq!(
            waiting.release(ACTOR, "acc-1", false, now),
            (0..3)
                .map(|id| Released::Ready(routed(origin, "acc-1", id)))
                .collect::<Vec<_>>()
        );
        assert!(!waiting.holds(ACTOR, "acc-1"));
        assert_eq!(waiting.due(), None);
    }

    #[test]
    fn a_message_that_came_back_holds_back_what_waits_behind_it_until_its_delay_has_passed() {
        let origin = &node(72);
        let now = Instant::now();
        let delay = Duration::from_millis(40);
        let mut waiting = Waiting::new(10);
        waiting.keep(routed(origin, "acc-1", 1), now, delay, TIMEOUT);
        waiting.keep(routed(origin, "acc-1", 2), now, Duration::ZERO, TIMEOUT);

        assert_eq!(waiting.release(ACTOR, "acc-1", false, now), Vec::new());
        assert_eq!(
            waiting.release(ACTOR, "acc-1", false, now + delay),
            vec![
                Released::Ready(routed(origin, "acc-1", 1)),
                Released::Ready(routed(origin, "acc-1", 2)),
            ]
        );
    }

    #[test]
    fn a_message_past_its_deadline_leaves_even_while_its_key_holds_it() {
        let origin = &node(73);
        let now = Instant::now();
        let mut waiting = Waiting::new(10);
        waiting.keep(routed(origin, "acc-1", 1), now, Duration::ZERO, TIMEOUT);
        waiting.keep(
            routed(origin, "acc-1", 2),
            now + Duration::from_secs(1),
            Duration::ZERO,
            TIMEOUT,
        );

        assert_eq!(
            waiting.release(ACTOR, "acc-1", true, now + TIMEOUT),
            vec![Released::Expired(routed(origin, "acc-1", 1))]
        );
        assert!(waiting.holds(ACTOR, "acc-1"));
    }

    #[test]
    fn a_cancellation_drops_the_request_it_names_and_leaves_the_rest() {
        let origin = &node(74);
        let now = Instant::now();
        let mut waiting = Waiting::new(10);
        for id in 1..=2 {
            waiting.keep(routed(origin, "acc-1", id), now, Duration::ZERO, TIMEOUT);
        }
        let cancel = |id| Cancel {
            actor: ACTOR.to_owned(),
            key: "acc-1".to_owned(),
            request: Target::Reply {
                node: origin.clone(),
                id,
            },
        };

        assert!(waiting.cancel(&cancel(1)));
        assert!(!waiting.cancel(&cancel(1)));
        assert_eq!(
            waiting.release(ACTOR, "acc-1", false, now),
            vec![Released::Ready(routed(origin, "acc-1", 2))]
        );
        assert!(!waiting.cancel(&cancel(2)));
    }

    #[test]
    fn nothing_more_waits_once_the_limit_does_and_a_look_is_due_a_tick_after_the_last() {
        let origin = &node(75);
        let now = Instant::now();
        let mut waiting = Waiting::new(2);
        assert_eq!(waiting.due(), None);
        assert!(!waiting.full());
        for id in 0..2 {
            waiting.keep(routed(origin, "acc-1", id), now, Duration::ZERO, TIMEOUT);
        }

        assert!(waiting.full());
        let due = waiting.due().expect("something waits");
        assert!(due > now);
        waiting.looked(due);
        assert!(waiting.due().expect("something waits") > due);
    }
}
