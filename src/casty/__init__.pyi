"""The public API of casty.

Everything that runs is in `casty._casty`; this is the contract it answers to. The checkers read this file instead of
`__init__.py`, so the shape of the API is written once, here, whatever the core does under it.
"""

from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Generic, Literal, Never, Protocol, Self, TypeVar, overload, runtime_checkable
from uuid import UUID

from casty.collections import Collections as Collections

class SchemaError(TypeError):
    """A state or message type cannot be serialized. The message names the path of the field."""

class NotStarted(Exception):
    """`ask` to a key that does not exist, of a type without a default `initial`.

    A ref obtained from a system creates its key, so this is what a ref that came inside a message can meet.
    """

class UnknownActor(Exception):
    """The node that owns the key does not have the actor type: it runs a version of the code without it.

    Members of a cluster run the same code, so this lasts as long as a deploy that brings the type in.
    """

class MailboxFull(Exception):
    """`ask` to an activation whose bounded mailbox is full."""

class Unavailable(Exception):
    """The owner is unreachable, replicas are insufficient, or the owner is changing.

    The message may or may not have been processed.
    """

class Refused(Exception):
    """The node or client could not join the cluster: different cluster name, codec, or protocol version."""

class ActorFailed(Exception):
    """The body raised while processing the message of an `ask`.

    The exception may have happened on another node, so only its class name and text are kept.
    """

    actor: str
    key: str
    error: str
    message: str

type Body[S, M] = Callable[[Context[S, M]], Awaitable[None]]
type Write = Literal["one", "majority", "all"]

@runtime_checkable
class Ref[M](Protocol):
    """Address of an entity, or of whoever is waiting for the reply of an `ask`."""

    def tell(self, msg: M, /) -> None:
        """Send `msg` without waiting. Delivery is at most once."""

    async def ask[R, **P](self, build: MessageBuilder[P, R, M], /, *args: P.args, **kwargs: P.kwargs) -> R:
        """Send `build(reply_to, *args, **kwargs)` and wait for the value told to `reply_to`.

        The first parameter of `build` must be annotated as `Ref[R]`, because the reply is decoded with that
        annotation. Raises `TimeoutError` after the system's `ask_timeout`; keyword arguments go to `build`, so a
        shorter deadline is set with `asyncio.timeout`.
        """

@runtime_checkable
class MessageBuilder[**P, R, M](Protocol):
    """What `ask` sends: a message built around the ref its answer comes back to."""

    def __call__(self, reply_to: Ref[R], /, *args: P.args, **kwargs: P.kwargs) -> M: ...

# A state is read and written as the same type, so it is invariant, for the reason the types of an actor are.
_T = TypeVar("_T")

@runtime_checkable
class State(Protocol[_T]):
    """The state of an entity, as its body reads and writes it."""

    @property
    def value(self) -> _T:
        """The last saved state."""

    async def set(self, state: _T, /) -> None:
        """Store `state` on the replicas and return once the type's write level confirms it.

        `value` changes only after the confirmation.
        """

    @overload
    async def update(self, change: Callable[[_T], Awaitable[_T]], /) -> _T:
        """Store what `change` makes of the last saved state, and answer it once it is confirmed.

        `change` answers the new state or an awaitable of it. The body handles one message at a time, so nothing
        else writes the state in between.
        """

    @overload
    async def update(self, change: Callable[[_T], _T], /) -> _T: ...

@runtime_checkable
class Context[S, M = Never](Protocol):
    """What the body of an activation receives."""

    @property
    def key(self) -> str: ...
    @property
    def state(self) -> State[S]:
        """The state of the key: `value`, `set` and `update`."""

    @property
    def inbox(self) -> AsyncIterator[M]:
        """Messages in arrival order. Ends after the system's `idle_after` without messages."""

    @property
    def self(self) -> Ref[M]:
        """Reference to this entity, to hand to other actors."""

    @property
    def system(self) -> System:
        """The system of the node where this activation runs."""

    @overload
    async def become[T](self, behavior: Actor[T, M] | DefaultedActor[T, M], state: T, /) -> None:
        """Hand the key to `behavior`, which runs it from the next read of `inbox` or `merge` on, from `state`.

        `behavior` takes the same messages, so every ref to the key stays good, and the key stays what it was: the
        type it started as, and `key`. The change is saved with the state, so it survives the node. The code after
        `become` still runs, which is where the message in hand is answered; `state.set` after it raises, because the
        state is no longer of this type.
        """

    @overload
    async def become(self, behavior: Actor[S, M] | DefaultedActor[S, M], /) -> None:
        """Hand the key to `behavior`, which has the state type of this one and goes on from the last saved state."""

    def merge[T](self, source: AsyncIterable[T], /) -> AsyncIterator[M | T]:
        """Messages and items of `source` in arrival order, until `source` ends.

        Idleness does not end it, and an exception raised by `source` propagates to the body.
        """

@runtime_checkable
class System(Protocol):
    """Entry point to actors: a node (`ActorSystem`) or a client of a cluster (`Client`)."""

    @property
    def node(self) -> NodeId: ...
    @overload
    def ref[S, M](self, actor: DefaultedActor[S, M], key: str, /) -> Ref[M]:
        """Reference to the entity `(actor, key)`, wherever it is placed.

        Obtaining it asks the owner to create the key, if it does not exist, and to activate it; nobody waits for
        that, and what goes wrong with it shows in the first `ask`. A key that does not exist starts from `initial`,
        or from the default of its type. A type without a default takes `initial`, unless its state can be `None`,
        which is then what the key starts from; anything else raises `TypeError`.
        """
    @overload
    def ref[S, M](self, actor: Actor[S | None, M], key: str, /) -> Ref[M]: ...
    @overload
    def ref[S, M](self, actor: Actor[S, M] | DefaultedActor[S, M], key: str, /, *, initial: S) -> Ref[M]: ...
    def _schema(self, annotation: object, /) -> object: ...
    def _encode(self, schema: object, value: object, /) -> bytes: ...
    def _decode(self, schema: object, data: bytes, /) -> object: ...

@runtime_checkable
class ActorDefinition(Protocol):
    """An actor type as a system registers it, whatever its state and message types."""

    @property
    def name(self) -> str:
        """Where the body lives, `module:qualname`, which is what nodes tell each other."""

    @property
    def replicas(self) -> int: ...
    @property
    def write(self) -> Write: ...
    @property
    def mailbox(self) -> int | None: ...
    def configured(self, name: str, replicas: int, write: Write, /) -> Self:
        """The same type under another name, with the replicas and write level it was configured with."""

# The state and message types of an actor are invariant: a type of one state is not a type of another, and a ref to
# it takes exactly the messages it declares. Written with `TypeVar` and not with a type parameter list, because the
# variance of a type parameter is inferred and what is inferred here is not invariance.
_S = TypeVar("_S")
_M = TypeVar("_M")

class Actor(Generic[_S, _M]):  # noqa: UP046
    """Actor type without a default state: a key starts from the `initial` its ref is obtained with."""

    @property
    def name(self) -> str: ...
    @property
    def body(self) -> Body[_S, _M]: ...
    @property
    def replicas(self) -> int: ...
    @property
    def write(self) -> Write: ...
    @property
    def mailbox(self) -> int | None: ...
    def configured(self, name: str, replicas: int, write: Write, /) -> Self: ...

class DefaultedActor(Generic[_S, _M]):  # noqa: UP046
    """Actor type whose keys are created on demand with `initial`."""

    @property
    def name(self) -> str: ...
    @property
    def body(self) -> Body[_S, _M]: ...
    @property
    def replicas(self) -> int: ...
    @property
    def write(self) -> Write: ...
    @property
    def mailbox(self) -> int | None: ...
    @property
    def initial(self) -> _S: ...
    def configured(self, name: str, replicas: int, write: Write, /) -> Self: ...

@overload
def actor[S, M](body: Body[S, M], /) -> Actor[S, M]: ...
@overload
def actor[S, M](
    *, replicas: int = 3, write: Write = "majority", mailbox: int | None = None
) -> Callable[[Body[S, M]], Actor[S, M]]: ...
@overload
def actor[S, M](
    *, initial: S, replicas: int = 3, write: Write = "majority", mailbox: int | None = None
) -> Callable[[Body[S, M]], DefaultedActor[S, M]]: ...

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

class ActorSystem:
    """A node. It hosts every actor type it meets: the ones this process uses, and the ones the cluster tells it of.

    Without `cluster`, the system runs in this process only. Leaving `async with` normally is an orderly shutdown;
    leaving it by an exception or cancellation is a crash.
    """

    def __init__(
        self,
        *,
        cluster: Cluster | None = None,
        codec: Literal["msgpack"] = "msgpack",
        idle_after: timedelta = ...,
        backoff: Backoff = ...,
        ask_timeout: timedelta = ...,
        write_timeout: timedelta = ...,
        leave_timeout: timedelta = ...,
    ) -> None: ...
    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, *exc: object) -> None: ...
    @property
    def node(self) -> NodeId: ...
    @property
    def members(self) -> tuple[Member, ...]:
        """Members of the cluster as this node sees them, itself included."""

    @overload
    def ref[S, M](self, actor: DefaultedActor[S, M], key: str, /) -> Ref[M]: ...
    @overload
    def ref[S, M](self, actor: Actor[S | None, M], key: str, /) -> Ref[M]: ...
    @overload
    def ref[S, M](self, actor: Actor[S, M] | DefaultedActor[S, M], key: str, /, *, initial: S) -> Ref[M]: ...
    def _schema(self, annotation: object, /) -> object: ...
    def _encode(self, schema: object, value: object, /) -> bytes: ...
    def _decode(self, schema: object, data: bytes, /) -> object: ...
    def _learn(self, actor: ActorDefinition, /) -> None: ...
    def _resolve(self, name: str, /) -> ActorDefinition | None: ...
    def _writes(self) -> Writes: ...

class Writes:
    """The pages a node writes, for a test that has to see them."""

    payloads: list[bytes]
    fail_on: bytes | None

class Client:
    """Sends messages to the actors of a cluster, without hosting keys or joining the membership.

    `__aenter__` returns after the first member table is received from a seed.
    """

    def __init__(
        self,
        *,
        seeds: tuple[str, ...],
        name: str = "casty",
        codec: Literal["msgpack"] = "msgpack",
        tls: TLS | None = None,
        compression: Compression = ...,
        address_map: Callable[[str], str] | None = None,
        ask_timeout: timedelta = ...,
        sync_every: timedelta = ...,
    ) -> None: ...
    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, *exc: object) -> None: ...
    @property
    def node(self) -> NodeId: ...
    @property
    def members(self) -> tuple[Member, ...]: ...
    @overload
    def ref[S, M](self, actor: DefaultedActor[S, M], key: str, /) -> Ref[M]: ...
    @overload
    def ref[S, M](self, actor: Actor[S | None, M], key: str, /) -> Ref[M]: ...
    @overload
    def ref[S, M](self, actor: Actor[S, M] | DefaultedActor[S, M], key: str, /, *, initial: S) -> Ref[M]: ...
    def _schema(self, annotation: object, /) -> object: ...
    def _encode(self, schema: object, value: object, /) -> bytes: ...
    def _decode(self, schema: object, data: bytes, /) -> object: ...

def address(parameter: str, value: str, /) -> None:
    """Check that `value` is `host:port`, naming `parameter` when it is not. A port of 0 binds wherever it can."""

def replicas(actor: str, key: str, nodes: list[NodeId], count: int, /) -> list[NodeId]:
    """The nodes that keep `key` among `nodes`, the first being the one the ring gives it to."""

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
