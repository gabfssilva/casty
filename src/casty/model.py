"""The part of the API written in Python: the values a caller builds and the protocols a caller writes against.

`casty` re-exports every name here, which is where the core reads them from. This module has no stub: the checkers and
the reference read it as it is, through the re-exports of `casty/__init__.pyi`.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Literal, Protocol, Self, overload, runtime_checkable
from uuid import UUID

if TYPE_CHECKING:
    from casty import Actor, Context, DefaultedActor, Ref


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
class Placement:
    """Where a key is, as the system asked sees it, which is what it sends a message to the key by.

    Attributes
    ----------
    owner
        The first of `replicas` that is `alive` or `suspect`: the node a message to the key goes to, and where the key
        activates. `None` when none of them is, and an `ask` raises `Unavailable`.
    replicas
        The nodes that keep the state of the key, in the order the ring gives them, or the one node a pinned key names.
        While the ring changes, a key whose copies are still on their way to the node asked stays where it was until
        they arrive.
    """

    owner: NodeId | None
    replicas: tuple[NodeId, ...]


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

    `codecs=None` offers every one this build has, in the order zstd, lz4, zlib. A frame shorter than `min_bytes` is
    sent uncompressed, so a payload smaller than it never is compressed.
    """

    codecs: tuple[Literal["zstd", "lz4", "zlib"], ...] | None = None
    min_bytes: int = 4096

    def __post_init__(self) -> None:
        if self.min_bytes < 0:
            raise ValueError(f"compression.min_bytes is {self.min_bytes}, and a size is not negative")


@dataclass(frozen=True)
class Limits:
    """Sizes, in bytes, of what crosses a connection.

    `message` bounds one envelope between two nodes: a message to a key, its answer, and each message that moves state
    between replicas. A message or an initial state over it raises `MessageTooLarge` where it is sent, and an answer
    over it reaches the caller as `MessageTooLarge`, also when the key is on the node that sends it. `frame` bounds
    each piece an envelope is cut into on the wire, and `window` is how much of a stream may be in flight before the
    receiver has taken it. Every node and client of a cluster has the same limits: the handshake refuses a peer whose
    limits differ.
    """

    frame: int = 256 * 1024
    message: int = 4 * 1024 * 1024
    window: int = 256 * 1024

    def __post_init__(self) -> None:
        for name, size in (("frame", self.frame), ("message", self.message), ("window", self.window)):
            if not 0 < size <= 2**31:
                raise ValueError(f"limits.{name} is {size}, and a size on the wire is between 1 byte and 2 GiB")
        if self.message < 128 * 1024:
            raise ValueError(
                f"limits.message is {self.message}, below 128 KiB: a message that moves state keeps 64 KiB of it for "
                "what surrounds the state"
            )
        if self.frame > self.message:
            raise ValueError(f"limits.frame is {self.frame}, above limits.message of {self.message}")
        if self.window < self.frame:
            raise ValueError(f"limits.window is {self.window}, below limits.frame of {self.frame}")


@dataclass(frozen=True)
class Opaque[T]:
    """The functions a value travels with when the schema does not take its type: `encode` to bytes, `decode` back.

    It goes in the metadata of an `Annotated`, so that the checkers see `T` and the wire carries bytes::

        type Frame = Annotated[np.ndarray, Opaque(encode=to_bytes, decode=from_bytes)]

    The functions are taken from the annotation when it is compiled, and called for every value. casty does not read
    the bytes, so what the schema does for other types is up to them: a node reading bytes written by another version
    of `T` gets whatever `decode` makes of them, a reader in another language gets bytes, and every node must `decode`
    what any other `encode`s. Two values are the same stored value only if `encode` gives them the same bytes, which a
    collection key and `compare_and_set` rely on.

    Parameters
    ----------
    encode
        The bytes of a value. A result that is not `bytes` raises `SchemaError`; what it raises reaches the writer as
        it is.
    decode
        The value `encode` made the bytes from. What it raises reaches the reader as it is.
    """

    encode: Callable[[T], bytes]
    decode: Callable[[bytes], T]


@dataclass(frozen=True)
class Backoff:
    """Delay before restarting a body that raised: `first`, multiplied by `factor` on each consecutive failure, up to
    `limit`."""

    first: timedelta = timedelta(milliseconds=100)
    limit: timedelta = timedelta(seconds=10)
    factor: float = 2.0

    def __post_init__(self) -> None:
        # Negated so that NaN, which no comparison holds for, is refused as well.
        if not self.factor >= 1:
            raise ValueError(f"backoff.factor is {self.factor}, and the delay grows only by a factor of at least 1")
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
    `limits` must be the same on every node and client. `remove_after=None` disables the automatic removal of dead
    members.

    Attributes
    ----------
    bind
        The `host:port` the node listens on. A port of 0 binds wherever it can.
    seeds
        Nodes to join the cluster through, as `host:port`.
    advertise
        The `host:port` other nodes and clients dial.
    name
        The name of the cluster. A node or client of another name is refused.
    tls
        Certificates for the connections. `None` connects without TLS.
    compression
        Compression offered on each connection.
    address_map
        The address to dial for an advertised one.
    limits
        Sizes of what crosses a connection.
    heartbeat
        Period of the heartbeats a node sends the neighbors of its active view.
    suspect_after
        Silence after which a neighbor is `suspect`. It is above `heartbeat`.
    dead_after
        Time as `suspect`, without a refutation, after which a member is `dead`.
    remove_after
        Time as `dead` after which a node that sees a majority of the members alive removes it.
    anti_entropy
        Period of the exchange of the whole member table with a random member.
    overlay
        Sizes and periods of the membership overlay.
    """

    bind: str
    seeds: tuple[str, ...] = ()
    advertise: str | None = None
    name: str = "casty"
    tls: TLS | None = None
    compression: Compression = Compression()
    address_map: Callable[[str], str] | None = None
    limits: Limits = Limits()
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
"""The async function an actor type is made of, called with the `Context` of a key each time the key activates.

The key is active while the call runs, which is usually as long as it reads `ctx.inbox`, and deactivates when it
returns. A call that raises is made again after the `backoff` of the type, from the last saved state, with the mailbox
kept.
"""

type Write = Literal["one", "majority", "all"]
"""How many of the replicas of a key confirm a write of its state before the write returns.

`"majority"` is more than half: confirmed writes survive the loss of a minority of the copies, and the minority side of
a partition cannot write. `"all"` survives the loss of every copy but one, and refuses writes while any replica is
away. `"one"` writes while any replica is reachable; a change of owner can lose confirmed writes, and two owners can
write during one. Too few confirmations within the `write_timeout` of the type raise `Unavailable`.
"""

type OnFull = Literal["refuse", "wait"]
"""What an `ask` that finds the bounded mailbox of a key full meets.

`"refuse"` fails it with `MailboxFull`. `"wait"` holds it, within its deadline, until there is room. A `tell` that finds
the mailbox full is dropped either way.
"""

type Durable = Literal["write"] | timedelta
"""When the store of the system keeps the writes of a durable type.

`"write"` saves every confirmed write before it returns. A `timedelta` saves the latest confirmed write at most that
long after it, and the write a key lets go with, or is deleted by, at once; writes return without waiting for the
store.
"""


@runtime_checkable
class System(Protocol):
    """Entry point to actors: a node (`ActorSystem`) or a client of a cluster (`Client`)."""

    @property
    def node(self) -> NodeId: ...

    @overload
    def ref[S, M](self, actor: DefaultedActor[S, M], key: str, /, *, at: Member | NodeId | str | None = None) -> Ref[M]:
        """Reference to the entity `(actor, key)`, wherever it is placed.

        Obtaining it asks the owner to create the key, if it does not exist, and to activate it; nobody waits for
        that, and what goes wrong with it shows in the first `ask`. A key that does not exist starts from `initial`,
        or from the default of its type. A type without a default takes `initial`, unless its state can be `None`,
        which is then what the key starts from; anything else raises `TypeError`. In a cluster, an `initial` larger
        than `Limits.message` raises `MessageTooLarge`.

        A type declared with `pinned=True` runs the key on the node `at` names: a member of `members`, its `NodeId`,
        or the `host:port` it advertises. The key carries that address, `@host:port/key` as `ctx.key` reads it, so the
        ref reaches the node after a restart, and `ask` raises `Unavailable` while no member up advertises it. Any
        other type is placed by the ring and takes no `at`.
        """
        ...

    @overload
    def ref[S, M](
        self, actor: Actor[S | None, M], key: str, /, *, at: Member | NodeId | str | None = None
    ) -> Ref[M]: ...

    @overload
    def ref[S, M](
        self,
        actor: Actor[S, M] | DefaultedActor[S, M],
        key: str,
        /,
        *,
        initial: S,
        at: Member | NodeId | str | None = None,
    ) -> Ref[M]: ...

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
    def pinned(self) -> bool:
        """Whether each key runs on the node its ref names with `at`, instead of where the ring places it."""
        ...

    @property
    def replicas(self) -> int: ...

    @property
    def write(self) -> Write: ...

    @property
    def mailbox(self) -> int | None: ...

    @property
    def on_full(self) -> OnFull:
        """What an `ask` that finds the bounded mailbox full meets.

        `"refuse"` fails it with `MailboxFull`. `"wait"` holds it, within its deadline, until there is room: the
        message stays with its sender, which sends it again when the owner calls it back. A `tell` that finds the
        mailbox full is dropped either way.
        """
        ...

    @property
    def concurrency(self) -> int:
        """How many messages of one key the body handles at once, 1 by default.

        Above 1, up to that many runs of the body read the same `inbox`, each taking the next message as it reads,
        so an `ask` or any other `await` in one run no longer holds up the others. The state is then read-only:
        `state.set`, `state.update` and `become` raise `RuntimeError`, since two runs writing it is the race one
        mailbox per key exists to prevent. The types of the collections take one message at a time and refuse it.
        """
        ...

    @property
    def idle_after(self) -> timedelta | None:
        """Time without messages after which `inbox` ends. `None` is the system's."""
        ...

    @property
    def ask_timeout(self) -> timedelta | None:
        """Deadline of an `ask` to this type. `None` is the system's."""
        ...

    @property
    def write_timeout(self) -> timedelta | None:
        """How long a write of the state or an activation waits for replicas. `None` is the system's."""
        ...

    @property
    def backoff(self) -> Backoff | None:
        """Delay before restarting a body that raised. `None` is the system's."""
        ...

    @property
    def durable(self) -> Durable | None:
        """When the store of the system keeps the writes of the type. `None` keeps them in memory only.

        `"write"` saves every confirmed write before it returns. A `timedelta` saves the latest confirmed write at
        most that long after it, and the write a key lets go with, or is deleted by, at once; writes return without
        waiting for the store.
        """
        ...

    def configured(self, name: str, replicas: int, write: Write, /) -> Self:
        """The same type under another name, with the replicas and write level it was configured with.

        Everything else the type sets, its mailbox and its timings, carries over.
        """
        ...


@runtime_checkable
class Store(Protocol):
    """Where the state of the durable types outlives every node: one record per key, shared by the nodes of a cluster.

    Each node of a cluster is given a store that reaches the same records (a database, object storage, a directory
    every machine mounts), and a type opts in with `@actor(durable=...)`. A record is a `version` and a `state`. The
    version orders the writes of a key as bytes compared in order, and a store keeps the greatest it was given: saves
    reach it in any order, and a late save of a node that lost the key must not undo a later one. `state` is what casty
    wrote, which the store keeps as it is, or `None` for a deletion.

    Each method is called on the event loop and runs as a task, bounded by the write timeout of the type: a call that
    has not returned by then is cancelled and counts as failed. A load that fails fails the activation, and a save
    that fails fails the write of a type saved on every write.

    `casty.sqlite.SQLiteStore` is one in a SQLite file, for a system alone or the nodes of one machine.
    """

    async def load(self, actor: str, key: str, /) -> tuple[bytes, bytes | None] | None:
        """The record of `(actor, key)` as `(version, state)`, or `None` when there is none."""
        ...

    async def save(self, actor: str, key: str, version: bytes, state: bytes | None, /) -> None:
        """Keep `(version, state)` as the record of `(actor, key)`, unless its record has a greater version."""
        ...

    async def drop(self, actor: str, key: str, version: bytes, /) -> None:
        """Forget the record of `(actor, key)` if its version is not greater than `version`."""
        ...
