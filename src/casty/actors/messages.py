from __future__ import annotations

from casty.serde.registry import message

ACTOR_CALL = 0x40

# RemoteError codes carried over the wire, mapped back to typed exceptions
CODE_UNKNOWN_ACTOR_TYPE = 10
CODE_WRONG_OWNER = 11
CODE_ACTOR_FAILED = 12
CODE_REENTRANCY = 13
CODE_UNAVAILABLE = 14
CODE_RANGE_MOVING = 15
CODE_QUORUM_UNAVAILABLE = 16


@message(name="casty.ActorCall")
class ActorCall:
    actor: str  # actor wire name
    key: str
    method: str
    args: list[bytes]  # each arg encoded via encode_raw; decoded against the param annotation
    chain: list[str]  # "wire/key" identities of the ask chain, for reentrancy detection


@message(name="casty.StreamOpen")
class StreamOpen:
    """Descriptor for a streaming call (spec 07), carried in the BULK_OPEN meta of
    a `casty.stream`. `args` are the unary args only; the input iterator arrives as
    STREAM_ITEM frames."""

    actor: str
    key: str
    method: str
    args: list[bytes]
    chain: list[str]
