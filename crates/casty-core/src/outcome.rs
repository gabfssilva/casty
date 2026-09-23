//! What an `ask` gets back when it is not a value.

/// The answer of a request: the encoded value, or why there is none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Value(Vec<u8>),
    /// The body raised while processing the message.
    Failed {
        actor: String,
        key: String,
        error: String,
        message: String,
    },
    /// The key does not exist, and its type has no default state.
    Missing {
        actor: String,
        key: String,
    },
    /// The bounded mailbox of the activation is full.
    Full {
        actor: String,
        key: String,
    },
    /// The message was not processed: it never reached the key, or reached it and was dropped.
    Unreached {
        actor: String,
        key: String,
    },
    /// The owner of the key does not have the actor type.
    Unknown {
        actor: String,
        key: String,
    },
    /// The answer does not fit in one message between two nodes. It says how large it is and what the limit is.
    TooLarge(String),
    /// The `ask` closes a cycle: every run of the key it went to waits, down its chain, for its answer. It names the
    /// cycle, `actor/key` by `actor/key`.
    Cycle(String),
}

impl Outcome {
    #[must_use]
    pub fn missing(actor: &str, key: &str) -> Self {
        Self::Missing {
            actor: actor.to_owned(),
            key: key.to_owned(),
        }
    }

    #[must_use]
    pub fn full(actor: &str, key: &str) -> Self {
        Self::Full {
            actor: actor.to_owned(),
            key: key.to_owned(),
        }
    }

    #[must_use]
    pub fn unreached(actor: &str, key: &str) -> Self {
        Self::Unreached {
            actor: actor.to_owned(),
            key: key.to_owned(),
        }
    }

    #[must_use]
    pub fn unknown(actor: &str, key: &str) -> Self {
        Self::Unknown {
            actor: actor.to_owned(),
            key: key.to_owned(),
        }
    }
}
