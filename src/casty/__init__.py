"""casty: virtual actors with a Rust core.

Everything that runs is in `casty._casty`: the schema, the transport, the placement, the replication and the bodies of
the collections. What is here is the surface those things are reached through — the values a caller builds, the
protocols a caller writes against, and the names the core answers to.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from importlib import import_module
from typing import Literal, Protocol, Self, runtime_checkable
from uuid import UUID

_core = import_module("casty._casty")

ActorFailed = _core.ActorFailed
MailboxFull = _core.MailboxFull
NotStarted = _core.NotStarted
Refused = _core.Refused
SchemaError = _core.SchemaError
Unavailable = _core.Unavailable
UnknownActor = _core.UnknownActor

Actor = _core.Actor
ActorSystem = _core.ActorSystem
Client = _core.Client
Context = _core.Context
DefaultedActor = _core.DefaultedActor
Ref = _core.Ref
State = _core.State
actor = _core.actor

#: The nodes that keep a key, the first being the one the ring gives it to. A test hook, not part of the API.
replicas = _core.replicas


@dataclass(frozen=True)
class NodeId:
    """Identity of a running system.

    `address` is the advertised `host:port`, or `None` for clients and local systems. `incarnation` is new on every
    start, so a process restarted on the same address is a different node.
    """

    address: str | None
    incarnation: UUID


@dataclass(frozen=True)
class Member:
    """A node of a cluster, as the one being asked sees it."""

    node: NodeId
    status: Literal["alive", "leaving", "suspect", "dead"]
    types: frozenset[str]


@dataclass(frozen=True)
class TLS:
    """Certificates for connections between nodes and clients.

    With `ca`, both sides verify the peer certificate against it, and `require_client_cert` makes it mutual TLS. Host
    names are not verified: nodes are addressed by IP and authenticated by the CA.
    """

    cert: str
    key: str
    ca: str | None = None
    require_client_cert: bool = True


@dataclass(frozen=True)
class Compression:
    """Compression offered on each connection and negotiated with the peer.

    `codecs=None` offers every one this build has, in the order zstd, lz4, zlib. Payloads smaller than
    `min_bytes` are sent uncompressed.
    """

    codecs: tuple[Literal["zstd", "lz4", "zlib"], ...] | None = None
    min_bytes: int = 4096


@dataclass(frozen=True)
class Backoff:
    """Delay before restarting a body that raised: `first`, multiplied by `factor` on each consecutive failure, up to
    `limit`."""

    first: timedelta = timedelta(milliseconds=100)
    limit: timedelta = timedelta(seconds=10)
    factor: float = 2.0

    def __post_init__(self) -> None:
        if self.factor < 1:
            raise ValueError(f"backoff.factor is {self.factor}, so a body that keeps failing would retry ever faster")
        if self.first > self.limit:
            raise ValueError(f"backoff.first is {self.first}, above backoff.limit of {self.limit}")


@dataclass(frozen=True)
class Overlay:
    """Sizes and periods of the membership overlay.

    `join_walk` is how far a join is forwarded; `passive_walk` is how far a shuffle walks, and the point of a join walk
    where the new node enters passive views.
    """

    active: int = 5
    passive: int = 30
    join_walk: int = 6
    passive_walk: int = 3
    shuffle_every: timedelta = timedelta(seconds=10)
    graft_after: timedelta = timedelta(milliseconds=500)

    def __post_init__(self) -> None:
        if self.active < 1:
            raise ValueError(f"overlay.active is {self.active}: a node with no neighbor reaches nobody")
        if self.passive < self.active:
            raise ValueError(f"overlay.passive is {self.passive}, below overlay.active of {self.active}")


@dataclass(frozen=True)
class Cluster:
    """Network of a node.

    `advertise` defaults to `bind`. Seeds equal to the advertised address are ignored; with no other seed, the node
    starts a cluster alone. `address_map` replaces an advertised address by the one to dial, for tunnels and NAT.
    `remove_after=None` disables the automatic removal of dead members.
    """

    bind: str
    seeds: tuple[str, ...] = ()
    advertise: str | None = None
    name: str = "casty"
    tls: TLS | None = None
    compression: Compression = Compression()
    address_map: Callable[[str], str] | None = None
    heartbeat: timedelta = timedelta(seconds=1)
    suspect_after: timedelta = timedelta(seconds=5)
    dead_after: timedelta = timedelta(seconds=5)
    remove_after: timedelta | None = timedelta(minutes=1)
    anti_entropy: timedelta = timedelta(seconds=30)
    overlay: Overlay = Overlay()

    def __post_init__(self) -> None:
        address("bind", self.bind)
        if self.advertise is not None:
            address("advertise", self.advertise)
        for seed in self.seeds:
            address("seeds", seed)
        if self.suspect_after <= self.heartbeat:
            raise ValueError(
                f"suspect_after is {self.suspect_after}, not above the heartbeat of {self.heartbeat}: "
                "a node would be suspected before it could answer a single ping"
            )


def address(parameter: str, value: str, /) -> None:
    """Check that `value` is `host:port`, naming `parameter` when it is not. A port of 0 binds wherever it can."""
    host, colon, port = value.rpartition(":")
    if not host or not colon or not port.isdigit() or int(port) > 65535:
        raise ValueError(f"{parameter} is {value!r}, which is not host:port")


type Body[S, M] = Callable[[Context[S, M]], Awaitable[None]]
type Write = Literal["one", "majority", "all"]


@runtime_checkable
class System(Protocol):
    """Entry point to actors: a node (`ActorSystem`) or a client of a cluster (`Client`)."""

    @property
    def node(self) -> NodeId: ...

    def ref[S, M](self, actor: Actor[S, M] | DefaultedActor[S, M], key: str, /, *, initial: S | None = None) -> Ref[M]:
        """Reference to the entity `(actor, key)`, wherever it is placed.

        Obtaining it asks the owner to create the key, if it does not exist, and to activate it; nobody waits for
        that, and what goes wrong with it shows in the first `ask`. A key that does not exist starts from `initial`,
        or from the default of its type. A type without a default takes `initial`, unless its state can be `None`,
        which is then what the key starts from; anything else raises `TypeError`.
        """
        ...

    def _schema(self, annotation: object, /) -> object:
        """`annotation` compiled by this implementation, in the canonical order a stored value is compared in."""
        ...

    def _encode(self, schema: object, value: object, /) -> bytes:
        """`value` as the bytes a collection stores. A ref in it is written as the target it points at."""
        ...

    def _decode(self, schema: object, data: bytes, /) -> object:
        """The value `data` holds, with every ref in it bound to this system."""
        ...


@runtime_checkable
class ActorDefinition(Protocol):
    """An actor type as a system registers it, whatever its state and message types."""

    @property
    def name(self) -> str:
        """Where the body lives, `module:qualname`, which is what nodes tell each other."""
        ...

    @property
    def replicas(self) -> int: ...

    @property
    def write(self) -> Write: ...

    @property
    def mailbox(self) -> int | None: ...

    def configured(self, name: str, replicas: int, write: Write, /) -> Self:
        """The same type under another name, with the replicas and write level it was configured with."""
        ...


from casty.collections import Collections as Collections  # noqa: E402

__all__ = [
    "TLS",
    "Actor",
    "ActorDefinition",
    "ActorFailed",
    "ActorSystem",
    "Backoff",
    "Body",
    "Client",
    "Cluster",
    "Collections",
    "Compression",
    "Context",
    "DefaultedActor",
    "MailboxFull",
    "Member",
    "NodeId",
    "NotStarted",
    "Overlay",
    "Ref",
    "Refused",
    "SchemaError",
    "State",
    "System",
    "Unavailable",
    "UnknownActor",
    "Write",
    "actor",
]
