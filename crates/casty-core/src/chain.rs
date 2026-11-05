//! The callers an `ask` keeps waiting, which is how a key tells that it would never read a message.
//!
//! A body that awaits an `ask` does not read its mailbox until the answer arrives. So an `ask` carries the callers
//! waiting on the message its body is on, plus that body, and the key it reaches can tell when answering it needs a
//! body that is itself waiting down the same chain: a cycle, which would otherwise end only at the deadline.

/// A body waiting for the answer of an `ask`: the key it runs, and the stretch of the body that asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Link {
    pub actor: String,
    pub key: String,
    /// Which stretch of the body asked: from one read of its mailbox to the next, numbered by the node that runs the
    /// key. A body that has read since is not waiting on this `ask` any more, whatever it does next.
    pub hold: u64,
}

/// The callers an `ask` keeps waiting, the one that sent it last. A body that asks names at most `depth` of them, its
/// system's: the oldest go first, so a deep chain does not grow the message without limit. A cycle of up to that many
/// keys is found however deep the chain above it is; a longer one ends at the deadline of its asks.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Chain(Vec<Link>);

impl Chain {
    /// A chain as it arrived. One longer than the depth of this system is kept whole, within the size of a message,
    /// and cut to that depth when a body here asks down it.
    #[must_use]
    pub fn of(links: Vec<Link>) -> Self {
        Self(links)
    }

    #[must_use]
    pub fn links(&self) -> &[Link] {
        &self.0
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// This chain with `link` waiting on it too, cut to its most recent `depth` callers.
    #[must_use]
    pub fn then(&self, link: Link, depth: usize) -> Self {
        let kept = self.0.len().min(depth.saturating_sub(1));
        let mut links = Vec::with_capacity(kept + 1);
        links.extend_from_slice(&self.0[self.0.len() - kept..]);
        links.push(link);
        Self(links)
    }

    /// The cycle a message of this chain closes at `(actor, key)`, named hop by hop, when the body of that key would
    /// never read it.
    ///
    /// `holding` is the stretch the body of the key is on while it is not reading. The message is never read when
    /// that stretch asked down this chain: the body waits for an answer that waits for this message. A body that is
    /// reading reads it.
    #[must_use]
    pub fn closed(&self, actor: &str, key: &str, holding: Option<u64>) -> Option<String> {
        let hold = holding?;
        let first = self
            .0
            .iter()
            .position(|link| link.actor == actor && link.key == key && link.hold == hold)?;
        let mut hops: Vec<String> = self.0[first..]
            .iter()
            .map(|link| format!("{}/{}", link.actor, link.key))
            .collect();
        hops.push(format!("{actor}/{key}"));
        Some(hops.join(" -> "))
    }
}

#[cfg(test)]
mod tests {
    use super::{Chain, Link};

    const DEPTH: usize = 16;

    fn link(actor: &str, key: &str, hold: u64) -> Link {
        Link {
            actor: actor.to_owned(),
            key: key.to_owned(),
            hold,
        }
    }

    fn chain(links: &[Link]) -> Chain {
        links.iter().fold(Chain::default(), |chain, link| {
            chain.then(link.clone(), DEPTH)
        })
    }

    #[test]
    fn a_key_asking_itself_closes_a_cycle_of_one() {
        let asked = chain(&[link("a", "k", 1)]);
        assert_eq!(
            asked.closed("a", "k", Some(1)),
            Some("a/k -> a/k".to_owned())
        );
    }

    #[test]
    fn two_keys_asking_each_other_close_a_cycle_naming_both() {
        let asked = chain(&[link("a", "k", 1), link("b", "k", 7)]);
        assert_eq!(
            asked.closed("a", "k", Some(1)),
            Some("a/k -> b/k -> a/k".to_owned())
        );
    }

    #[test]
    fn a_run_that_has_read_since_is_not_waiting_on_the_chain() {
        // The body asked without waiting and went on to its next message, or is reading.
        let asked = chain(&[link("a", "k", 1), link("b", "k", 7)]);
        assert_eq!(asked.closed("a", "k", Some(2)), None);
        assert_eq!(asked.closed("a", "k", None), None);
    }

    #[test]
    fn a_key_the_chain_does_not_name_is_not_reentered() {
        let asked = chain(&[link("a", "k", 1), link("b", "k", 7)]);
        assert_eq!(asked.closed("c", "k", Some(1)), None);
        assert_eq!(asked.closed("a", "other", Some(1)), None);
        // Another type under the same key is another key.
        assert_eq!(asked.closed("b", "k", Some(1)), None);
    }

    #[test]
    fn the_chain_keeps_its_most_recent_callers_up_to_its_depth() {
        let deep: Vec<Link> = (0..40_u64)
            .map(|hop| link("hop", &hop.to_string(), hop))
            .collect();
        let grown = chain(&deep);
        assert_eq!(grown.links(), &deep[40 - DEPTH..]);
        // One that arrived deeper is kept, and cut once a body asks down it.
        let arrived = Chain::of(deep.clone());
        assert_eq!(arrived.links(), &deep[..]);
        let asked = arrived.then(link("a", "k", 1), 4);
        assert_eq!(&asked.links()[..3], &deep[37..]);
        assert_eq!(asked.links().len(), 4);
    }

    #[test]
    fn a_cycle_within_the_depth_is_found_under_a_deep_chain() {
        let mut deep: Vec<Link> = (0..40_u64)
            .map(|hop| link("hop", &hop.to_string(), hop))
            .collect();
        deep.push(link("a", "k", 100));
        deep.push(link("b", "k", 101));
        assert_eq!(
            chain(&deep).closed("a", "k", Some(100)),
            Some("a/k -> b/k -> a/k".to_owned())
        );
        // A cycle longer than the depth has lost its first caller, and ends at the deadline instead.
        let mut long = vec![link("a", "k", 100)];
        long.extend((0..DEPTH).map(|hop| link("hop", &hop.to_string(), 0)));
        assert_eq!(chain(&long).closed("a", "k", Some(100)), None);
    }
}
