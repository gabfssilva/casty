"""What a system reports as it runs, and the observer it reports to.

A system built with `observer=` calls it with one event at a time, on its event loop, after the step that produced the
event has finished: the observer may call back into the system, and it must not block, because the bodies run on the
same loop. Nothing waits for it, so an event reaches it shortly after it happened, in the order this node saw it.

An event is what one node saw, not something the cluster agreed on: two nodes can see a member change at different
moments, and a node that was cut off sees the changes of that time only when it hears of them.

An observer that raises is reported to the exception handler of the loop, and the system carries on.

A system built without an observer reports to a `LoggingObserver`, which writes every event to the `casty` logger.

Events say what happened; `stats()` says how much there is. A system counts its activations, the answers it waits for,
the writes of its keys and what its connections carried, and `ActorSystem.stats()` and `Client.stats()` read those
counts at one moment, as a `Stats`. `ActorSystem.activations()` reads the activations those counts are made of, one
`Activation` each.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol, assert_never

from casty import NodeId

__all__ = [
    "Activation",
    "ActivationEnded",
    "ActivationFailed",
    "ActivationStarted",
    "ActorStats",
    "ConnectionLost",
    "Event",
    "HandoffEnded",
    "HandoffStarted",
    "LoggingObserver",
    "MemberChanged",
    "MessageDropped",
    "Observer",
    "Stats",
    "WriteFailed",
]

logger = logging.getLogger("casty")


@dataclass(frozen=True)
class MemberChanged:
    """A member of the cluster took `status`: from `previous`, or from nothing when this node had not seen it before.

    Each change of status in the member table of this node is reported once, in the order the table made it. A crash
    is `suspect`, then `dead`, then `left` once `remove_after` passes on a side that sees a majority alive; an orderly
    shutdown is `leaving`, then `left`. A node that first hears of a later status skips the ones before it, and
    `previous` says where the member came from. A member this node no longer lists is `left`: it said goodbye, or it
    stayed dead until it was removed. A client reports what changed from one table it was sent to the next.
    """

    node: NodeId
    status: Literal["alive", "leaving", "suspect", "dead", "left"]
    previous: Literal["alive", "leaving", "suspect", "dead", "left"] | None


@dataclass(frozen=True)
class ActivationStarted:
    """A key became active on this node: a message or a ref reached it, or it moved here."""

    actor: str
    key: str


@dataclass(frozen=True)
class ActivationEnded:
    """A key stopped being active on this node: it idled out, it moved, or the node is shutting down.

    The next message to the key starts it again, here or on the node that owns it by then.
    """

    actor: str
    key: str


@dataclass(frozen=True)
class ActivationFailed:
    """The body of a key raised `error` on a message and restarts from the saved state, or the key cannot run here."""

    actor: str
    key: str
    error: BaseException


@dataclass(frozen=True)
class WriteFailed:
    """An operation over the replicas of a key did not go through, and whoever waited for it was told why.

    `operation` is `activate` for taking the key over, and `write` for a save of the state or the write that lets the
    key go. `reason` is `unavailable` when too few replicas answered in time, `fenced` when another owner took the key,
    and `too_large` when the state does not fit in a message.
    """

    actor: str
    key: str
    operation: Literal["activate", "write"]
    reason: Literal["unavailable", "fenced", "too_large"]
    message: str


@dataclass(frozen=True)
class HandoffStarted:
    """Keys of `actor` started moving after a change of the ring.

    `in` is this node filling the ranges the ring gave it, from the nodes that had them; `out` is this node handing the
    keys the ring took from it to the nodes that replicate them now.
    """

    actor: str
    direction: Literal["in", "out"]


@dataclass(frozen=True)
class HandoffEnded:
    """Nothing of `actor` is moving in `direction` any more.

    `abandoned` names the keys an orderly leave went without: the nodes that replicate them now had not taken them when
    `leave_timeout` ran out, and whatever of them only this node held left with it. It is empty when every key was
    taken, and always for `in`.
    """

    actor: str
    direction: Literal["in", "out"]
    abandoned: tuple[str, ...] = ()


@dataclass(frozen=True)
class ConnectionLost:
    """A connection to `node` ended while this system was running. The next envelope for it dials again."""

    node: NodeId


@dataclass(frozen=True)
class MessageDropped:
    """A message nobody waits for, a `tell`, ended on this node without reaching its key, because of `reason`."""

    actor: str
    key: str
    reason: str


type Event = (
    MemberChanged
    | ActivationStarted
    | ActivationEnded
    | ActivationFailed
    | WriteFailed
    | HandoffStarted
    | HandoffEnded
    | ConnectionLost
    | MessageDropped
)
"""Anything a system reports to its `Observer`, one event at a time."""


@dataclass(frozen=True)
class ActorStats:
    """The activations of one actor type on a node, as `stats()` read them.

    Attributes
    ----------
    active
        How many keys of the type are active on the node.
    queued
        How many messages wait in their mailboxes, all of them together.
    deepest
        The most messages waiting in any one of those mailboxes, which is what a bounded `mailbox` fills up to.
    """

    active: int
    queued: int
    deepest: int


@dataclass(frozen=True)
class Stats:
    """What a system counts, read at one moment by `ActorSystem.stats()` or `Client.stats()`.

    `actors`, `asks_in_flight` and `connections` say how much there is now. The writes and the bytes only grow: they
    count from when the system entered, and start again from zero when a node the cluster removed joins it again under
    a new identity. Nothing is counted as a message goes by: the activations, their mailboxes and the answers are read
    from the node when it is asked, and the writes and the bytes from counters kept where they happen.

    Attributes
    ----------
    actors
        The activations of every actor type the system has met, by the name of the type. A type with no activation on
        this node, and every type on a client, which hosts no key, counts zero.
    asks_in_flight
        The `ask`s of this system, those of its bodies included, that wait for their answer.
    writes_confirmed
        Writes of the keys this node owns, a save, a deletion or the last write of an activation, that the replicas
        confirmed at the write level of the type. Without a cluster, every write the node made.
    writes_failed
        Writes of those keys that did not go through, each one also reported as a `WriteFailed`. Taking a key over is
        not a write and is counted in neither, though a failure of it is reported as a `WriteFailed` too.
    connections
        Connections to other nodes and clients that are open now.
    bytes_sent
        Bytes written to those connections since the system entered, handshakes included: compressed when a frame is,
        and before TLS encrypts them.
    bytes_received
        Bytes read from them.
    """

    actors: Mapping[str, ActorStats]
    asks_in_flight: int
    writes_confirmed: int
    writes_failed: int
    connections: int
    bytes_sent: int
    bytes_received: int


@dataclass(frozen=True)
class Activation:
    """A key active on a node, as `ActorSystem.activations()` read it.

    Attributes
    ----------
    actor
        The name of the type the key started as, which it keeps after `become`.
    key
        The key as `ctx.key` reads it: a key of a pinned type starts with the address of its node.
    since
        When the key became active on the node, by the clock of its machine, in UTC.
    queued
        How many messages wait in its mailbox, besides the ones its body is on.
    """

    actor: str
    key: str
    since: datetime
    queued: int


class Observer(Protocol):
    """What a system reports to: anything called with one `Event` at a time.

    An observer may also say which kinds of event it takes, with a method `wants(kind: type[Event], /) -> bool`. The
    system asks it about each kind once, when it enters, and from then on builds no event of a kind it said no to, so
    an observer that leaves out the frequent ones, `ActivationStarted` and `ActivationEnded`, costs a busy node
    nothing for them. An observer without `wants` takes every kind.
    """

    def __call__(self, event: Event, /) -> None: ...


class LoggingObserver:
    """The observer of a system built without one: every event as a record of the `casty` logger.

    A dropped message, a body or a write that failed, keys a leave went without, and a member turning suspect or dead
    are warnings, with the traceback of the body; a member joining, leaving or coming back, a lost connection and keys
    moving are info; an activation starting or ending is debug, and is reported to it only when the `casty` logger
    takes debug records as the system enters (see `wants`).

    A system given an observer of its own logs none of it. To keep the logging, call one of these from that observer::

        logged = casty.LoggingObserver()

        def observer(event: casty.Event, /) -> None:
            logged(event)
            ...
    """

    def wants(self, kind: type[Event], /) -> bool:
        """Whether a system reports events of `kind` to this observer.

        Every kind but an activation starting or ending, which are debug records: those only when the `casty` logger
        takes debug records at the moment the system asks, as it enters. A system that entered with the logger above
        debug builds none of them; lowering the level afterwards logs them from the next system that enters.
        """
        return kind not in (ActivationStarted, ActivationEnded) or logger.isEnabledFor(logging.DEBUG)

    def __call__(self, event: Event, /) -> None:
        match event:
            case MemberChanged(node=node, status=status, previous=previous):
                level = logging.WARNING if status in ("suspect", "dead") else logging.INFO
                if previous is None:
                    logger.log(level, "member %s is %s", _named(node), status)
                else:
                    logger.log(level, "member %s went from %s to %s", _named(node), previous, status)
            case ActivationStarted(actor=actor, key=key):
                logger.debug("activated %s/%s", actor, key)
            case ActivationEnded(actor=actor, key=key):
                logger.debug("deactivated %s/%s", actor, key)
            case ActivationFailed(actor=actor, key=key, error=error):
                logger.warning("%s/%s failed: %s", actor, key, error, exc_info=error)
            case WriteFailed(actor=actor, key=key, operation=operation, message=message):
                done = "taken over" if operation == "activate" else "written"
                logger.warning("%s/%s was not %s: %s", actor, key, done, message)
            case HandoffStarted(actor=actor, direction=direction):
                logger.info("keys of %s started moving %s this node", actor, _towards(direction))
            case HandoffEnded(actor=actor, direction=direction, abandoned=()):
                logger.info("keys of %s stopped moving %s this node", actor, _towards(direction))
            case HandoffEnded(actor=actor, direction=direction, abandoned=abandoned):
                logger.warning(
                    "keys of %s stopped moving %s this node without handing over %d of them: %s",
                    actor,
                    _towards(direction),
                    len(abandoned),
                    ", ".join(abandoned),
                )
            case ConnectionLost(node=node):
                logger.info("lost the connection to %s", _named(node))
            case MessageDropped(actor=actor, key=key, reason=reason):
                logger.warning("dropped a message to %s/%s: %s", actor, key, reason)
            case _:
                assert_never(event)


def _named(node: NodeId) -> str:
    """A node as a log line names it: its address, and the start of the incarnation that tells a restart apart."""
    return f"{node.address or 'a client'} ({node.incarnation.hex[:8]})"


def _towards(direction: Literal["in", "out"]) -> str:
    return "to" if direction == "in" else "off"
