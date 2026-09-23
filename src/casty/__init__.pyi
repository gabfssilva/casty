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
from casty.observer import Activation as Activation
from casty.observer import ActivationEnded as ActivationEnded
from casty.observer import ActivationFailed as ActivationFailed
from casty.observer import ActivationStarted as ActivationStarted
from casty.observer import ActorStats as ActorStats
from casty.observer import ConnectionLost as ConnectionLost
from casty.observer import Event as Event
from casty.observer import HandoffEnded as HandoffEnded
from casty.observer import HandoffStarted as HandoffStarted
from casty.observer import LoggingObserver as LoggingObserver
from casty.observer import MemberChanged as MemberChanged
from casty.observer import MessageDropped as MessageDropped
from casty.observer import Observer as Observer
from casty.observer import Stats as Stats
from casty.observer import WriteFailed as WriteFailed

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
    """`ask` to an activation whose bounded mailbox is full, of a type that refuses (`on_full="refuse"`)."""

class Unavailable(Exception):
    """The owner is unreachable, replicas are insufficient, or the owner is changing.

    The message may or may not have been processed.
    """

class Refused(Exception):
    """The node or client could not join the cluster: different cluster name, limits, or protocol version."""

class MessageTooLarge(ValueError):
    """A message, an initial state or an answer is larger than `Limits.message`, the most one message between two
    nodes carries.

    Nothing of it was sent. It is refused wherever the key is, on the node that owns it too. A write of a state
    (`state.set`, `state.update`, `become`) raises it when a field has a name that leaves no room in a message for
    its data; a larger value is not refused, it travels across as many messages as it takes.
    """

class ReentrancyError(Exception):
    """`ask` to a key whose body waits, down the chain of asks this one belongs to, for its answer.

    The key would never read the message: its body reads the next message only once the `ask` it awaits is answered.
    Raised at once, where waiting would end only at the deadline, and the message names the cycle, `actor/key` by
    `actor/key`. A key whose type has a `concurrency` above 1 takes the message as long as one of its runs is not
    waiting down the chain.
    """

class ActorFailed(Exception):
    """The body raised while processing the message of an `ask`.

    The exception may have happened on another node, so only its class name and text are kept.
    """

    actor: str
    key: str
    error: str
    message: str

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
class Ref[M](Protocol):
    """Address of an entity, or of whoever is waiting for the reply of an `ask`."""

    def tell(self, msg: M, /) -> None:
        """Send `msg` without waiting. Delivery is at most once.

        In a cluster, raises `MessageTooLarge` when `msg` is larger than `Limits.message`. Told to the `reply_to` of
        an `ask`, an answer that large reaches the one asking as `MessageTooLarge` instead.
        """

    async def ask[R, **P](self, build: MessageBuilder[P, R, M], /, *args: P.args, **kwargs: P.kwargs) -> R:
        """Send `build(reply_to, *args, **kwargs)` and wait for the value told to `reply_to`.

        The first parameter of `build` must be annotated as `Ref[R]`, because the reply is decoded with that
        annotation. Raises `TimeoutError` after the `ask_timeout` of the actor type, or the system's when the type
        sets none; keyword arguments go to `build`, so a shorter deadline is set with `asyncio.timeout`. In a cluster,
        raises `MessageTooLarge` at once when the message or its answer is larger than `Limits.message`.

        An `ask` cancelled, or past its deadline, raises `CancelledError` or `TimeoutError` here as any await does, and
        the key hears that nobody waits: a message still queued is dropped unread, and a body working on it is
        cancelled at the `await` it is on and runs again, from the last confirmed state, for the next message. That is
        a request to stop, not a promise that nothing happened: what the body did before the `await`, and a write
        confirmed before it, stays. A collection finishes the message it is on. An answer that arrived first is kept,
        and the key hears nothing; a cancellation that crosses an answer already told to `reply_to` changes nothing
        for the body, which goes on with what it does after answering.

        Made by a body, from the task that read its message, an `ask` carries the keys whose bodies wait for its
        answer: those waiting on that message, and the body itself, until it reads again. The key it goes to raises
        `ReentrancyError` here at once when it is one of them and none of its runs is free to read it: a body asking
        its own key, or two keys asking each other. The chain names at most 16 keys, the most recent, so a longer
        cycle ends at the deadline. A `tell`, an `ask` from outside a body, and one from a task the body started
        beside it keep nobody waiting.
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

        `value` changes only after the confirmation. Raises `RuntimeError` in a type whose `concurrency` is above 1,
        whose state is read-only.
        """

    @overload
    async def update(self, change: Callable[[_T], Awaitable[_T]], /) -> _T:
        """Store what `change` makes of the last saved state, and answer it once it is confirmed.

        `change` answers the new state or an awaitable of it. A type of `concurrency=1` handles one message at a
        time, so nothing else writes the state in between; above 1 the state is read-only, and this raises
        `RuntimeError`.
        """

    @overload
    async def update(self, change: Callable[[_T], _T], /) -> _T: ...
    async def delete(self) -> None:
        """Delete the state from the replicas and return once the type's write level confirms it.

        The key is then one nothing wrote: activated again, it starts from `initial`. The body goes on from the default
        of its type, which `value` reads from here on; a type without a default has no state to read until the next
        `set`, which writes the key again. A body that ends without writing again leaves nothing of the key on any
        replica. Raises `Unavailable` when too few replicas confirm it, as `set` does, and `RuntimeError` in a type
        whose `concurrency` is above 1.
        """

@runtime_checkable
class Context[S, M = Never](Protocol):
    """What the body of an activation receives."""

    @property
    def key(self) -> str:
        """The key of this entity. A key of a pinned type carries the address of its node: `@host:port/key`."""

    @property
    def state(self) -> State[S]:
        """The state of the key: `value`, `set`, `update` and `delete`."""

    @property
    def inbox(self) -> AsyncIterator[M]:
        """Messages in arrival order. Ends after `idle_after` without messages: the type's, or the system's.

        Reading the next message is what ends the one before, so everything a body awaits in between, an `ask`
        included, holds up the messages of the key, unless the type declares a `concurrency` above 1.
        """

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
        state is no longer of this type. Raises `RuntimeError` in a type whose `concurrency` is above 1, whose other
        runs are in the middle of their messages.
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
    def pinned(self) -> bool:
        """Whether each key runs on the node its ref names with `at`, instead of where the ring places it."""

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

    @property
    def concurrency(self) -> int:
        """How many messages of one key the body handles at once, 1 by default.

        Above 1, up to that many runs of the body read the same `inbox`, each taking the next message as it reads,
        so an `ask` or any other `await` in one run no longer holds up the others. The state is then read-only:
        `state.set`, `state.update` and `become` raise `RuntimeError`, since two runs writing it is the race one
        mailbox per key exists to prevent. The types of the collections take one message at a time and refuse it.
        """

    @property
    def idle_after(self) -> timedelta | None:
        """Time without messages after which `inbox` ends. `None` is the system's."""

    @property
    def ask_timeout(self) -> timedelta | None:
        """Deadline of an `ask` to this type. `None` is the system's."""

    @property
    def write_timeout(self) -> timedelta | None:
        """How long a write of the state or an activation waits for replicas. `None` is the system's."""

    @property
    def backoff(self) -> Backoff | None:
        """Delay before restarting a body that raised. `None` is the system's."""

    @property
    def durable(self) -> Durable | None:
        """When the store of the system keeps the writes of the type. `None` keeps them in memory only.

        `"write"` saves every confirmed write before it returns. A `timedelta` saves the latest confirmed write at
        most that long after it, and the write a key lets go with, or is deleted by, at once; writes return without
        waiting for the store.
        """

    def configured(self, name: str, replicas: int, write: Write, /) -> Self:
        """The same type under another name, with the replicas and write level it was configured with.

        Everything else the type sets, its mailbox and its timings, carries over.
        """

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

    async def save(self, actor: str, key: str, version: bytes, state: bytes | None, /) -> None:
        """Keep `(version, state)` as the record of `(actor, key)`, unless its record has a greater version."""

    async def drop(self, actor: str, key: str, version: bytes, /) -> None:
        """Forget the record of `(actor, key)` if its version is not greater than `version`."""

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
    def pinned(self) -> bool: ...
    @property
    def replicas(self) -> int: ...
    @property
    def write(self) -> Write: ...
    @property
    def mailbox(self) -> int | None: ...
    @property
    def on_full(self) -> OnFull: ...
    @property
    def concurrency(self) -> int: ...
    @property
    def idle_after(self) -> timedelta | None: ...
    @property
    def ask_timeout(self) -> timedelta | None: ...
    @property
    def write_timeout(self) -> timedelta | None: ...
    @property
    def backoff(self) -> Backoff | None: ...
    @property
    def durable(self) -> Durable | None: ...
    def configured(self, name: str, replicas: int, write: Write, /) -> Self: ...

class DefaultedActor(Generic[_S, _M]):  # noqa: UP046
    """Actor type whose keys are created on demand with `initial`."""

    @property
    def name(self) -> str: ...
    @property
    def body(self) -> Body[_S, _M]: ...
    @property
    def pinned(self) -> bool: ...
    @property
    def replicas(self) -> int: ...
    @property
    def write(self) -> Write: ...
    @property
    def mailbox(self) -> int | None: ...
    @property
    def on_full(self) -> OnFull: ...
    @property
    def concurrency(self) -> int: ...
    @property
    def idle_after(self) -> timedelta | None: ...
    @property
    def ask_timeout(self) -> timedelta | None: ...
    @property
    def write_timeout(self) -> timedelta | None: ...
    @property
    def backoff(self) -> Backoff | None: ...
    @property
    def durable(self) -> Durable | None: ...
    @property
    def initial(self) -> _S: ...
    def configured(self, name: str, replicas: int, write: Write, /) -> Self: ...

@overload
def actor[S, M](body: Body[S, M], /) -> Actor[S, M]:
    """Define an actor type from its body.

    The type is named after where its body lives, `module:qualname`. That is what nodes tell each other, and how a
    node that never used the type finds it: every member of a cluster runs the same code. The state and message types
    are read from the `Context[S, M]` the body is annotated with, and checked when the decorator runs.

    Bare, `@actor` makes an `Actor`, whose keys start from the `initial` their ref is obtained with. With `initial=`,
    it makes a `DefaultedActor`, whose keys start from that value.

    Parameters
    ----------
    body
        The function the type runs for each activation of a key.
    initial
        The state a key nothing wrote starts from.
    pinned
        Run each key on the node its ref names with `at`, instead of where the ring places it.
    replicas
        How many nodes keep a copy of each key, capped by the size of the cluster: 3 by default, and 1, the only
        value it takes, for a pinned type.
    write
        How many of those replicas confirm a write of the state.
    mailbox
        How many messages wait for a key before its mailbox is full. `None` does not bound it.
    on_full
        What an `ask` that finds the mailbox full meets. `"wait"` takes a `mailbox`.
    concurrency
        How many messages of one key the body handles at once. Above 1, the state is read-only.
    idle_after
        Time without messages after which `ctx.inbox` ends. `None` is the system's.
    ask_timeout
        Deadline of an `ask` to the type. `None` is the system's.
    write_timeout
        How long a write of the state or an activation waits for replicas. `None` is the system's.
    backoff
        Delay before restarting a body that raised. `None` is the system's.
    durable
        When the store of the system keeps the writes of the type. `None` keeps them in memory only.

    Raises
    ------
    SchemaError
        The state or a message type of the body cannot be serialized.
    ValueError
        `replicas`, `mailbox` or `concurrency` is 0, a pinned type has more than one replica, `on_full="wait"` has no
        `mailbox`, a period is negative, or `write`, `on_full` or `durable` is a string it does not take.
    TypeError
        `durable` is neither `"write"`, a `timedelta` nor `None`.
    """

@overload
def actor[S, M](
    *,
    pinned: bool = False,
    replicas: int = ...,
    write: Write = "majority",
    mailbox: int | None = None,
    on_full: OnFull = "refuse",
    concurrency: int = 1,
    idle_after: timedelta | None = None,
    ask_timeout: timedelta | None = None,
    write_timeout: timedelta | None = None,
    backoff: Backoff | None = None,
    durable: Durable | None = None,
) -> Callable[[Body[S, M]], Actor[S, M]]: ...
@overload
def actor[S, M](
    *,
    initial: S,
    pinned: bool = False,
    replicas: int = ...,
    write: Write = "majority",
    mailbox: int | None = None,
    on_full: OnFull = "refuse",
    concurrency: int = 1,
    idle_after: timedelta | None = None,
    ask_timeout: timedelta | None = None,
    write_timeout: timedelta | None = None,
    backoff: Backoff | None = None,
    durable: Durable | None = None,
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

class ActorSystem:
    """A node. It hosts every actor type it meets: the ones this process uses, and the ones the cluster tells it of.

    Without `cluster`, the system runs in this process only. Leaving `async with` normally is an orderly shutdown;
    leaving it by an exception or cancellation is a crash. `observer` is called with every `Event` of this node of a
    kind it takes (see `Observer`), on the event loop and after the step that produced it; without one, a
    `LoggingObserver` writes them to the `casty` logger.

    `store` keeps the state of the types declared with `durable=` outside every process (see `Store`). Every node of
    a cluster is given one that reaches the same records; a durable type on a node without one fails to activate
    with `Unavailable`. An object without `load`, `save` and `drop` methods raises `TypeError`.

    A negative period raises `ValueError`, here or, for a period of `cluster`, in `__aenter__`, which also refuses a
    `heartbeat`, `anti_entropy`, `overlay.graft_after` or `overlay.shuffle_every` of zero: the node repeats them.

    Parameters
    ----------
    cluster
        The network of the node. `None` runs the system in this process only.
    idle_after
        Time without messages after which `ctx.inbox` ends, for the types that set none.
    backoff
        Delay before restarting a body that raised, for the types that set none: `Backoff()` when not given.
    ask_timeout
        Deadline of an `ask`, for the types that set none.
    write_timeout
        How long a write of the state or an activation waits for replicas, for the types that set none.
    leave_timeout
        How long an orderly shutdown waits for the nodes that replicate the keys of this one to take them.
    observer
        What the events of this node are reported to. `None` is a `LoggingObserver`.
    store
        Where the state of the durable types outlives every node.
    """

    def __init__(
        self,
        *,
        cluster: Cluster | None = None,
        idle_after: timedelta = timedelta(minutes=1),
        backoff: Backoff = ...,
        ask_timeout: timedelta = timedelta(seconds=10),
        write_timeout: timedelta = timedelta(seconds=5),
        leave_timeout: timedelta = timedelta(seconds=30),
        observer: Observer | None = None,
        store: Store | None = None,
    ) -> None: ...
    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, *exc: object) -> None: ...
    @property
    def node(self) -> NodeId: ...
    @property
    def members(self) -> tuple[Member, ...]:
        """Members of the cluster as this node sees them, itself included."""

    def stats(self) -> Stats:
        """What this node counts now: its activations and their mailboxes by type, the answers it waits for, the writes
        of its keys, and its connections and the bytes they carried.

        Nothing is counted as messages go by, so the counts cost nothing until they are read; a reading walks every
        activation of the node once. Raises `RuntimeError` before the system enters and after it exits.
        """

    def activations(self) -> tuple[Activation, ...]:
        """The keys active on this node now, by type and key: when each became active and what waits in its mailbox.

        It is what `stats()` counts, one row per key, read the same way: a reading walks every activation of the node
        once. Raises `RuntimeError` before the system enters and after it exits.
        """

    async def placement(
        self, actor: ActorDefinition, key: str, /, *, at: Member | NodeId | str | None = None
    ) -> Placement:
        """Where the key a ref of `actor` goes to is, as this node sees it: the node its messages go to, and the nodes
        that keep it.

        `key` and `at` are those `ref` takes, so a pinned type names its node with `at`. The answer is what this node
        routes by at that moment; another node may see a change of the cluster a moment before or after it. A system
        without a cluster is the owner and the only replica of every key. Raises `RuntimeError` before the system
        enters and after it exits.
        """

    async def release(self, actor: ActorDefinition, key: str, /) -> bool:
        """End the activation of `(actor, key)` on this node as if it had idled out now, and answer whether it had one.

        `key` is the key as the activation has it, `ctx.key`: a key of a pinned type starts with the address of its
        node. Nothing is cut short. The body ends at its next read of `inbox` or `merge`, after the message it is on
        and once the writes it has in flight have landed, and the key lets go with its last write, as at `idle_after`;
        the key of a collection ends between two messages, once no deadline of its own is pending. Messages that reach
        the key meanwhile wait, and once that write is out they go, in the order they came, to a new activation, which
        starts from the last confirmed state. Answers `False` at once when the key has no activation here, and `True`
        once the one it had has ended; a body that never reads again never ends. Raises `RuntimeError` before the
        system enters and after it exits.
        """

    @overload
    def ref[S, M](self, actor: DefaultedActor[S, M], key: str, /, *, at: Member | NodeId | str | None = None) -> Ref[M]:
        """Reference to the entity `(actor, key)`, wherever it is placed, as `System.ref` describes it."""

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
    def _schema(self, annotation: object, /) -> object: ...
    def _encode(self, schema: object, value: object, /) -> bytes: ...
    def _decode(self, schema: object, data: bytes, /) -> object: ...
    def _learn(self, actor: ActorDefinition, /) -> None: ...
    def _resolve(self, name: str, /) -> ActorDefinition | None: ...
    def _queued(self, actor: ActorDefinition, key: str, /) -> int | None: ...
    def _chain(self) -> list[str]: ...
    def _cancel(self, actor: ActorDefinition, key: str, reply_to: Ref[Never], /) -> None: ...
    def _writes(self) -> Writes: ...
    def _stored(self) -> Awaitable[list[tuple[str, str, bool]]]: ...

class Writes:
    """The pages a node writes, for a test that has to see them."""

    payloads: list[bytes]
    fail_on: bytes | None

class Client:
    """Sends messages to the actors of a cluster, without hosting keys or joining the membership.

    `__aenter__` returns after the first member table is received from a seed. `limits` must be those of the cluster:
    a seed refuses a client whose limits differ, and `__aenter__` raises `Refused`. `observer` hears the members,
    connections and dropped messages of this client, as it does for a node.

    Parameters
    ----------
    seeds
        Nodes of the cluster to reach it through, as `host:port`. An empty tuple raises `ValueError`.
    name
        The name of the cluster.
    tls
        Certificates for the connections, as the nodes of the cluster use them.
    compression
        Compression offered on each connection: `Compression()` when not given.
    address_map
        Replaces an advertised address by the one to dial, for tunnels and NAT.
    limits
        Sizes of what crosses a connection, those of the cluster: `Limits()` when not given.
    ask_timeout
        Deadline of an `ask`, for the types that set none. A negative one raises `ValueError`.
    sync_every
        How often the client asks a member for the member table it places keys by. Zero or a negative one raises
        `ValueError`.
    observer
        What the events of this client are reported to. `None` is a `LoggingObserver`.
    """

    def __init__(
        self,
        *,
        seeds: tuple[str, ...],
        name: str = "casty",
        tls: TLS | None = None,
        compression: Compression = ...,
        address_map: Callable[[str], str] | None = None,
        limits: Limits = ...,
        ask_timeout: timedelta = timedelta(seconds=10),
        sync_every: timedelta = timedelta(seconds=5),
        observer: Observer | None = None,
    ) -> None: ...
    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, *exc: object) -> None: ...
    @property
    def node(self) -> NodeId: ...
    @property
    def members(self) -> tuple[Member, ...]: ...
    def stats(self) -> Stats:
        """What this client counts now: the answers it waits for, and its connections and the bytes they carried.

        A client hosts no key and owns no write, so those counts are zero. Raises `RuntimeError` before the client
        enters and after it exits.
        """

    async def placement(
        self, actor: ActorDefinition, key: str, /, *, at: Member | NodeId | str | None = None
    ) -> Placement:
        """Where the key a ref of `actor` goes to is, as this client sees it: the node it sends to, and the nodes that
        keep the key.

        `key` and `at` are those `ref` takes. A client reads it from the member table it last asked a member for, once
        every `sync_every`, so it can lag the members by that long; a message it sends meanwhile to a node that no
        longer owns the key comes back, and the client asks for the table again. Raises `RuntimeError` before the
        client enters and after it exits.
        """

    @overload
    def ref[S, M](self, actor: DefaultedActor[S, M], key: str, /, *, at: Member | NodeId | str | None = None) -> Ref[M]:
        """Reference to the entity `(actor, key)`, wherever it is placed, as `System.ref` describes it."""

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
    def _schema(self, annotation: object, /) -> object: ...
    def _encode(self, schema: object, value: object, /) -> bytes: ...
    def _decode(self, schema: object, data: bytes, /) -> object: ...

def address(parameter: str, value: str, /) -> None:
    """Check that `value` is `host:port`, naming `parameter` when it is not. A port of 0 binds wherever it can."""

def replicas(actor: str, key: str, nodes: list[NodeId], count: int, /) -> list[NodeId]:
    """The nodes that keep `key` among `nodes`, the first being the one the ring gives it to."""

__all__ = [
    "TLS",
    "Activation",
    "ActivationEnded",
    "ActivationFailed",
    "ActivationStarted",
    "Actor",
    "ActorDefinition",
    "ActorFailed",
    "ActorStats",
    "ActorSystem",
    "Backoff",
    "Body",
    "Client",
    "Cluster",
    "Collections",
    "Compression",
    "ConnectionLost",
    "Context",
    "DefaultedActor",
    "Durable",
    "Event",
    "HandoffEnded",
    "HandoffStarted",
    "Limits",
    "LoggingObserver",
    "MailboxFull",
    "Member",
    "MemberChanged",
    "MessageDropped",
    "MessageTooLarge",
    "NodeId",
    "NotStarted",
    "Observer",
    "OnFull",
    "Opaque",
    "Overlay",
    "Placement",
    "ReentrancyError",
    "Ref",
    "Refused",
    "SchemaError",
    "State",
    "Stats",
    "Store",
    "System",
    "Unavailable",
    "UnknownActor",
    "Write",
    "WriteFailed",
    "actor",
]
