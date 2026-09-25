"""The public API of casty.

Everything that runs is in `casty._casty`; this is the contract it answers to. The checkers read this file instead of
`__init__.py`, so the shape of the core is written once, here, whatever it does under it. What is written in Python is
re-exported from the module that defines it: `casty.model`, `casty.collections` and `casty.observer`.
"""

from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Mapping
from datetime import datetime, timedelta
from typing import Generic, Never, Protocol, Self, overload, runtime_checkable

from typing_extensions import TypeVar

from casty.collections import Collections as Collections
from casty.model import TLS as TLS
from casty.model import ActorDefinition as ActorDefinition
from casty.model import Backoff as Backoff
from casty.model import Body as Body
from casty.model import Cluster as Cluster
from casty.model import Compression as Compression
from casty.model import Durable as Durable
from casty.model import Limits as Limits
from casty.model import Member as Member
from casty.model import NodeId as NodeId
from casty.model import OnFull as OnFull
from casty.model import Opaque as Opaque
from casty.model import Overlay as Overlay
from casty.model import Placement as Placement
from casty.model import Store as Store
from casty.model import System as System
from casty.model import Write as Write
from casty.model import address as address
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
    `actor/key`.
    """

class ActorFailed(Exception):
    """The body raised while processing the message of an `ask`.

    The exception may have happened on another node, so only its class name and text are kept.
    """

    actor: str
    key: str
    error: str
    message: str

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
        `ReentrancyError` here at once when it is one of them: a body asking its own key, or two keys asking each
        other. The chain names at most 16 keys, the most recent, so a longer
        cycle ends at the deadline. A `tell`, an `ask` from outside a body, and one from a task the body started
        beside it keep nobody waiting.
        """

@runtime_checkable
class MessageBuilder[**P, R, M](Protocol):
    """What `ask` sends: a message built around the ref its answer comes back to."""

    def __call__(self, reply_to: Ref[R], /, *args: P.args, **kwargs: P.kwargs) -> M: ...

@runtime_checkable
class Schedule[M](Protocol):
    """A message a key tells itself at a time, once or on an interval, made by `Context.schedule`."""

    @property
    def name(self) -> str:
        """What the key calls it. No two schedules of a key share a name."""

    @property
    def message(self) -> M:
        """The message it tells the key."""

    @property
    def interval(self) -> timedelta | None:
        """How long after each time it goes off it goes off again. `None` for a schedule that goes off once."""

    @property
    def due(self) -> datetime | None:
        """When it goes off next, in UTC, or `None` once it is over or another of its name took its place."""

    async def cancel(self) -> None:
        """Stop it, and return once the type's write level confirms it.

        A schedule that is over, or that another of its name replaced, returns at once. Raises `RuntimeError` when the
        activation that made it is over.
        """

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

        `change` answers the new state or an awaitable of it. A key handles one message at a time, so nothing else
        writes the state in between.
        """

    @overload
    async def update(self, change: Callable[[_T], _T], /) -> _T: ...
    async def delete(self) -> None:
        """Delete the state from the replicas and return once the type's write level confirms it.

        The key is then one nothing wrote: activated again, it starts from `initial`. The body goes on from the default
        of its type, which `value` reads from here on; a type without a default has no state to read until the next
        `set`, which writes the key again. A body that ends without writing again leaves nothing of the key on any
        replica. Raises `Unavailable` when too few replicas confirm it, as `set` does.
        """

# The state and message types of an actor are invariant: a type of one state is not a type of another, and a ref to
# it takes exactly the messages it declares. Written with `TypeVar` and not with a type parameter list, because the
# variance of a type parameter is inferred and what is inferred here is not invariance.
_S = TypeVar("_S")
_M = TypeVar("_M")
# `Context[S]` is `Context[S, Never]`: a body that takes no message at all. The `TypeVar` of `typing_extensions`,
# because a default is 3.13 syntax in a type parameter list and 3.13 API in `typing.TypeVar`, and 3.12 reads this stub.
_Received = TypeVar("_Received", default=Never)

@runtime_checkable
class Context(Protocol[_S, _Received]):
    """What the body of an activation receives."""

    @property
    def key(self) -> str:
        """The key of this entity. A key of a pinned type carries the address of its node: `@host:port/key`."""

    @property
    def state(self) -> State[_S]:
        """The state of the key: `value`, `set`, `update` and `delete`."""

    @property
    def inbox(self) -> AsyncIterator[_Received]:
        """Messages in arrival order. Ends after `idle_after` without messages: the type's, or the system's.

        Reading the next message is what ends the one before, so everything a body awaits in between, an `ask`
        included, holds up the messages of the key.
        """

    @property
    def self(self) -> Ref[_Received]:
        """Reference to this entity, to hand to other actors."""

    @property
    def system(self) -> System:
        """The system of the node where this activation runs."""

    @overload
    async def become[T](self, behavior: Actor[T, _Received] | DefaultedActor[T, _Received], state: T, /) -> None:
        """Hand the key to `behavior`, which runs it from the next read of `inbox` or `merge` on, from `state`.

        `behavior` takes the same messages, so every ref to the key stays good, and the key stays what it was: the
        type it started as, and `key`. The change is saved with the state, so it survives the node. The code after
        `become` still runs, which is where the message in hand is answered; `state.set` after it raises, because the
        state is no longer of this type.
        """

    @overload
    async def become(self, behavior: Actor[_S, _Received] | DefaultedActor[_S, _Received], /) -> None:
        """Hand the key to `behavior`, which has the state type of this one and goes on from the last saved state."""

    async def schedule(
        self, name: str, delay: timedelta, interval: timedelta | None, message: _Received, /
    ) -> Schedule[_Received]:
        """Tell this entity `message` `delay` from now, and then every `interval`; once when `interval` is `None`.

        The schedule takes the place of the one of the key named `name`, if there is one, so scheduling under the same
        name again, at the start of the body or on every message that asks for it, never adds a second one. Returns
        the schedule once the type's write level confirms it. A schedule is saved with the state of the key, so
        the node that runs the key after this one, when this one fails or the key moves, takes it up where it was. It
        goes off on the node where the key is active, and only while it is: a key with schedules does not idle out, and
        one that is not active, after its body returns or `ActorSystem.release`, takes them up when it is activated
        again. A time that passed while no node ran the key goes off at once.

        Times are kept by the wall clock. An interval counts from the time the schedule was due, not from when its
        message is read, and a time missed by more than one interval, by a busy loop or a key taken over late, is
        skipped. Each time it goes off is a message queued like a `tell`, and a write of the time it goes off next, or
        of its end. A message queued on a node that dies is lost as any queued message is, and a time whose write was
        not confirmed goes off again on the node that takes the key over.

        The schedules go on under the behavior `become` hands the key to, which takes the same messages, and
        `state.delete` cancels them. Raises `SchemaError` when `message` is not one of the messages of the type, and
        `ValueError` when `delay` is negative or `interval` is not positive.
        """

    @property
    def schedules(self) -> Mapping[str, Schedule[_Received]]:
        """The schedules of the key that are not over, by name, in the order they were made."""

    def merge[T](self, source: AsyncIterable[T], /) -> AsyncIterator[_Received | T]:
        """Messages and items of `source` in arrival order, until `source` ends.

        Idleness does not end it, and an exception raised by `source` propagates to the body.
        """

    @overload
    def to_self(self, work: Awaitable[_Received], /, *, failed: Callable[[Exception], _Received] | None = None) -> None:
        """Await `work` beside the body, and tell this entity what it gives, as a message. Returns at once.

        The body goes on reading its messages meanwhile, so an `ask` handed over this way holds up nothing: its answer
        is queued behind the messages that arrived before it, and it goes wherever the key is by then, activating it
        again if it went idle. `work` is any awaitable: an `ask`, a `gather` of several, a call to a database.

        When `work` raises, `failed` makes the message of what it raised. Without `failed`, and when a message cannot
        be told, nothing is sent and the system reports a `MessageDropped`. Work that has not ended when the system
        stops is cancelled, and sends nothing.

        Nothing the body asked on this message before calling it keeps it waiting any more, as far as
        `ReentrancyError` goes: a key asked that way may ask this one back, and is queued. A cycle through such an
        `ask` that the body still awaits ends at the deadline instead.
        """

    @overload
    def to_self[T](
        self,
        work: Awaitable[T],
        mapper: Callable[[T], _Received],
        /,
        *,
        failed: Callable[[Exception], _Received] | None = None,
    ) -> None:
        """Await `work` beside the body, and tell this entity what `mapper` makes of what it gives.

        `mapper` runs when `work` ends, after the body has moved on: a `lambda` that reads the message of the loop reads
        the one the body is on by then. `functools.partial(Withdrawn, msg)` binds it when `to_self` is called.
        """

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
    idle_after
        Time without messages after which `ctx.inbox` ends. `None` is the system's.
    ask_timeout
        Deadline of an `ask` to the type. `None` is the system's.
    write_timeout
        How long a write of the state or an activation waits for replicas, and a message for an owner to take it.
        `None` is the system's.
    backoff
        Delay before restarting a body that raised. `None` is the system's.
    durable
        When the store of the system keeps the writes of the type. `None` keeps them in memory only.

    Raises
    ------
    SchemaError
        The state or a message type of the body cannot be serialized.
    ValueError
        `replicas` or `mailbox` is 0, a pinned type has more than one replica, `on_full="wait"` has no `mailbox`, a
        period is negative, or `write`, `on_full` or `durable` is a string it does not take.
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
    idle_after: timedelta | None = None,
    ask_timeout: timedelta | None = None,
    write_timeout: timedelta | None = None,
    backoff: Backoff | None = None,
    durable: Durable | None = None,
) -> Callable[[Body[S, M]], DefaultedActor[S, M]]: ...

class Runtime:
    """Threads for the transport of the systems and clients given it.

    Without one, each `ActorSystem` in a cluster and each `Client` starts a thread per core of its own. Given the same
    `Runtime`, they share its threads, and a system that ends stops only its node, its listener and its connections;
    the threads go when nothing holds the runtime any more.

    Parameters
    ----------
    threads
        How many threads carry the transport. Zero raises `ValueError`.
    """

    def __init__(self, *, threads: int) -> None: ...
    def _tasks(self) -> int: ...

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
        How long a write of the state or an activation waits for replicas, and a message for an owner to take it,
        for the types that set none.
    leave_timeout
        How long an orderly shutdown waits for the nodes that replicate the keys of this one to take them.
    observer
        What the events of this node are reported to. `None` is a `LoggingObserver`.
    store
        Where the state of the durable types outlives every node.
    runtime
        The threads the transport of the node runs on, shared with the other systems given the same one. `None` starts
        a thread per core for this node alone. A system without `cluster` has no transport and uses none.
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
        runtime: Runtime | None = None,
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
        Replaces an advertised address by the one to dial, for tunnels and NAT. It is called on the event loop at
        every dial, so what it answers may change from one dial to the next.
    limits
        Sizes of what crosses a connection, those of the cluster: `Limits()` when not given.
    ask_timeout
        Deadline of an `ask`, for the types that set none. A negative one raises `ValueError`.
    sync_every
        How often the client asks a member for the member table it places keys by. Zero or a negative one raises
        `ValueError`.
    observer
        What the events of this client are reported to. `None` is a `LoggingObserver`.
    runtime
        The threads the transport of the client runs on, shared with the other systems given the same one. `None`
        starts a thread per core for this client alone.
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
        runtime: Runtime | None = None,
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
    "Runtime",
    "Schedule",
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
