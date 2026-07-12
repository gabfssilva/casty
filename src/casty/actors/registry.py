from __future__ import annotations

import collections.abc
import dataclasses
import datetime
import inspect
import types
import typing
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum, auto

from casty.errors import SerializationSchemaError
from casty.serde import codec
from casty.serde import registry as serde

_ACTIVATE_MARK = "__casty_activate__"
_DEACTIVATE_MARK = "__casty_deactivate__"
_TRANSIENT_KEY = "casty_transient"
_PAGER_KEY = "casty_pager"


class Directive(Enum):
    """What a supervisor does with an activation whose handler raised.

    `KEEP` retains the in-memory state (the default), `RESET` reactivates
    from the last committed state, `STOP` deactivates. In every case the
    original error reaches the caller as `ActorFailedError`. Also exported
    as `casty.KEEP` / `casty.RESET` / `casty.STOP`.
    """

    KEEP = auto()
    RESET = auto()
    STOP = auto()


class Consistency(Enum):
    """Symbolic ack counts, resolved against an actor's `replicas`:
    `ONE` = 1, `MAJORITY` = replicas // 2 + 1, `ALL` = replicas. Also exported
    as `casty.ONE` / `casty.MAJORITY` / `casty.ALL`; a plain int works too.
    """

    ONE = auto()
    MAJORITY = auto()
    ALL = auto()


def resolve_quorum(level: Consistency | int, replicas: int) -> int:
    """Ack count for a consistency level, against the *configured* replica
    count — never capped at the live view size, or a partitioned minority
    would fence nothing."""
    match level:
        case Consistency.ONE:
            return 1
        case Consistency.MAJORITY:
            return replicas // 2 + 1
        case Consistency.ALL:
            return replicas
        case int() as n:
            if not 1 <= n <= replicas:
                raise ValueError(f"quorum {n} out of range for {replicas} replicas")
            return n


@dataclass(frozen=True, slots=True)
class FailureContext:
    """What a supervisor knows about the failure it is deciding on.

    Attributes
    ----------
    failures : int
        Consecutive failures of this activation, this one included.
    """

    failures: int


type Supervisor = Callable[[type, str, Exception, FailureContext], Directive]
"""Failure policy: `(actor_cls, key, exc, ctx) -> Directive`, called after a
handler raises. Set globally on `start`/`local` or per class on `@actor`."""


def default_supervisor(
    actor_cls: type, key: str, exc: Exception, ctx: FailureContext
) -> Directive:
    return Directive.KEEP


@dataclass(frozen=True, slots=True)
class MethodInfo:
    name: str
    params: tuple[tuple[str, object], ...]  # unary (name, annotation); self/stream param excluded
    returns: object  # scalar return; for server-streaming it is the AsyncIterator annotation
    # streaming (spec 07): element types, not the iterator. stream_in names the
    # single AsyncIterator param; stream_out is the yielded element type.
    stream_in: tuple[str, object] | None = None
    stream_out: object | None = None
    param_order: tuple[str, ...] = ()  # every param name in declaration order (incl. the iterator)

    @property
    def is_streaming(self) -> bool:
        return self.stream_in is not None or self.stream_out is not None


@dataclass(frozen=True, slots=True)
class Integral:
    """One page per field (spec 09 §3): the field is re-encoded after every handler
    and its bytes compared against the last committed page."""

    name: str
    hint: object
    default: bytes  # encoded default: a field with no committed page restores to it
    immutable: bool  # no in-place mutation possible, so `==` against the last value settles it


@dataclass(frozen=True, slots=True)
class Tracked:
    """One page per dict entry (spec 09 §4): `paging.TrackedDict` records the keys a
    handler touched, so a commit encodes only those."""

    name: str
    hint: object
    key: type[str] | type[int] | type[bytes]  # how an entry key encodes into a page key
    value_hint: object


@dataclass(frozen=True, slots=True)
class PageSet:
    """What a pager reports: the pages that changed since its last snapshot,
    and the ones the value no longer has.

    Attributes
    ----------
    changed : dict[str, bytes]
        Page key -> new page bytes.
    dropped : list[str]
        Keys of pages the value no longer has.
    """

    changed: dict[str, bytes]
    dropped: list[str] = dataclasses.field(default_factory=list)


class Pager[T](typing.Protocol):
    """How a field type pages itself, for values casty cannot encode on
    its own — a DataFrame, an array. Attach one to a state field with
    `casty.paged(pager)`.

    The truth is `pages()` diffed against the previous snapshot, never write
    interception: a C extension walks straight past any Python-level hook. Reporting
    a change that did not happen is allowed; missing one is silent divergence.

    Only `restore` is typed in T. The framework hands values and snapshots back as
    `object` — it stored them without knowing what they were — so an implementation
    narrows them itself.
    """

    def snapshot(self, value: object) -> object:
        """A cheap handle on `value` as it stands, for the next diff.

        Taken after every commit, so it must not copy the data.

        Parameters
        ----------
        value : object
            The field's current value.

        Returns
        -------
        object
            Whatever `pages` and `rollback` need to look back at this moment.
        """
        ...

    def pages(self, value: object, previous: object) -> PageSet:
        """What changed in `value` since the `previous` snapshot.

        Parameters
        ----------
        value : object
            The field's value after the handler ran.
        previous : object
            The snapshot taken at the last commit.

        Returns
        -------
        PageSet
            Changed page bytes and dropped page keys.
        """
        ...

    def restore(self, pages: Mapping[str, bytes]) -> T:
        """Rebuild the value from its committed pages.

        Parameters
        ----------
        pages : Mapping[str, bytes]
            Every committed page of the field; empty means the field was
            never committed and must come back as its default value.

        Returns
        -------
        T
            The reconstructed value.
        """
        ...

    def rollback(self, previous: object) -> T:
        """The value as of `previous`: an uncommitted handler is being undone.

        Parameters
        ----------
        previous : object
            The snapshot taken at the last commit.

        Returns
        -------
        T
            The value the handler started from.
        """
        ...


@dataclass(frozen=True, slots=True)
class Paged:
    """A field that brings its own pager (spec 09 §5): the pager decides what a page
    is and what changed."""

    name: str
    hint: object
    pager: Pager[object]


type Regime = Integral | Tracked | Paged

type StateOf = Callable[[object], dict[str, object]]
type RestoreInto = Callable[[object, dict[str, object]], None]


@dataclass(frozen=True, slots=True)
class ActorInfo:
    cls: type
    wire_name: str
    state_cls: type  # synthetic message `<wire>#state`: validates the state is serializable
    state_fields: tuple[str, ...]
    regimes: dict[str, Regime]  # how each state field pages (spec 09 §3)
    state_of: StateOf  # the state field values of an instance
    restore_into: RestoreInto  # write values back (partial dicts allowed)
    transient_fields: tuple[str, ...]
    methods: dict[str, MethodInfo]
    activate_hook: str | None
    deactivate_hook: str | None
    idle_timeout: float | None  # None = Config.default_idle_timeout
    supervisor: Supervisor | None
    replicas: int
    write: Consistency | int
    read: Consistency | int
    # True when *every* state field is immutable: comparing the state values then
    # skips the page encode entirely after a read-only handler.
    immutable_state: bool = False
    # "service" (spec 08): stateless, unplaced, one interchangeable activation per
    # node under SERVICE_KEY. Routing skips the ring for it.
    kind: typing.Literal["actor", "service"] = "actor"

    @property
    def replicated(self) -> bool:
        return self.replicas > 1

    @property
    def durable_activity(self) -> bool:
        """Replicated + infinite idle: node death never ends the actor's
        activity — the new owner reactivates it unprompted."""
        return (
            self.kind == "actor"
            and self.replicas > 1
            and self.idle_timeout == float("inf")
        )

    @property
    def write_quorum(self) -> int:
        return resolve_quorum(self.write, self.replicas)


_by_type: dict[type, ActorInfo] = {}
_by_name: dict[str, ActorInfo] = {}

# the key every service activation lives under: one per node, interchangeable
SERVICE_KEY = "@"


def info_of(cls: type) -> ActorInfo | None:
    return _by_type.get(cls)


def info_by_name(wire_name: str) -> ActorInfo | None:
    return _by_name.get(wire_name)


def transient(*, default: object = None, factory: Callable[[], object] | None = None) -> typing.Any:
    """Declare a state field excluded from the actor's snapshot.

    The field is neither serialized nor replicated; rebuild it in the
    `@casty.activate` hook. Its type need not be serializable.

    Parameters
    ----------
    default : object
        Value the field starts with on activation.
    factory : Callable[[], object] | None
        Zero-argument factory, for mutable defaults. Wins over `default`.

    Returns
    -------
    typing.Any
        A `dataclasses.field`, typed `Any` so the field annotation
        typechecks.
    """
    metadata = {_TRANSIENT_KEY: True}
    if factory is not None:
        return dataclasses.field(default_factory=factory, metadata=metadata)
    return dataclasses.field(default=default, metadata=metadata)


def paged[T](pager: Pager[T]) -> T:
    """Declare a state field that replicates through `pager`
    instead of being encoded whole.

    Parameters
    ----------
    pager : Pager[T]
        Decides what a page is and what changed; `casty.pagers` ships one
        for pandas DataFrames.

    Returns
    -------
    T
        A `dataclasses.field` whose default is whatever the pager restores
        from no pages.
    """
    return dataclasses.field(
        default_factory=lambda: pager.restore({}), metadata={_PAGER_KEY: pager}
    )


def activate[F: Callable[..., object]](func: F) -> F:
    """Mark the async method run after an activation's state is restored and
    before its first message; rebuild `transient()` fields here. At most one
    per class, `self` only."""
    setattr(func, _ACTIVATE_MARK, True)
    return func


def deactivate[F: Callable[..., object]](func: F) -> F:
    """Mark the async method run before an activation is dropped — on idle
    timeout, `ctx.deactivate()`, a STOP directive or node drain. At most one
    per class, `self` only."""
    setattr(func, _DEACTIVATE_MARK, True)
    return func


@typing.overload
def actor[T](cls: type[T], /) -> type[T]: ...


@typing.overload
def actor[T](
    *,
    name: str | None = None,
    idle_timeout: float | None = None,
    supervisor: Supervisor | None = None,
    replicas: int = 1,
    write: Consistency | int = Consistency.MAJORITY,
    read: Consistency | int = Consistency.ONE,
) -> Callable[[type[T]], type[T]]: ...


def actor(
    cls: type | None = None,
    /,
    *,
    name: str | None = None,
    idle_timeout: float | None = None,
    supervisor: Supervisor | None = None,
    replicas: int = 1,
    write: Consistency | int = Consistency.MAJORITY,
    read: Consistency | int = Consistency.ONE,
) -> type | Callable[[type], type]:
    """Declare a virtual actor class.

    Annotated fields become replicable state; public async methods become the
    proxy interface. Validation (serializability, defaults, hooks) happens at
    import time.

    Parameters
    ----------
    name : str | None
        Stable wire name. Defaults to `module.QualName` — set it to rename the
        class later without breaking the cluster.
    idle_timeout : float | None
        Seconds without messages before deactivation. None uses
        `Config.default_idle_timeout`; `float("inf")` never deactivates on
        idle.
    supervisor : Supervisor | None
        Failure policy override for this class.
    replicas : int
        Physical nodes holding the state snapshot. 1 disables replication.
    write : Consistency | int
        Acks required per mutation, resolved against `replicas`.
    read : Consistency | int
        Reserved for per-method reads; the activation handshake uses `write`.

    Raises
    ------
    SerializationSchemaError
        At import time: unserializable state or method annotation, missing
        field default, duplicate wire name, invalid hooks or quorum.

    Examples
    --------
    >>> @casty.actor(replicas=3)
    ... class Counter:
    ...     value: int = 0
    ...
    ...     async def increment(self) -> int:
    ...         self.value += 1
    ...         return self.value
    """
    if cls is None:

        def apply(inner: type) -> type:
            return _register(inner, name, idle_timeout, supervisor, replicas, write, read)

        return apply
    return _register(cls, name, idle_timeout, supervisor, replicas, write, read)


def register_service(cls: type, name: str | None) -> type:
    """Register a synthesized service coordinator (spec 08) as an actor: same
    slots, mailbox, wire registration and proxy, but flagged `kind="service"` so
    routing skips the ring."""
    return _register(
        cls, name, None, None, 1, Consistency.ONE, Consistency.ONE, kind="service"
    )


def _register(
    cls: type,
    name: str | None,
    idle_timeout: float | None,
    supervisor: Supervisor | None,
    replicas: int,
    write: Consistency | int,
    read: Consistency | int,
    kind: typing.Literal["actor", "service"] = "actor",
) -> type:
    wire_name = name or f"{cls.__module__}.{cls.__qualname__}"
    if wire_name in _by_name:
        raise SerializationSchemaError(
            f"actor wire name {wire_name!r} already registered by {_by_name[wire_name].cls!r}"
        )
    if not dataclasses.is_dataclass(cls):
        cls = dataclasses.dataclass(slots=True, eq=False)(cls)

    state_fields: list[str] = []
    transient_fields: list[str] = []
    pagers: dict[str, Pager[object]] = {}
    for f in dataclasses.fields(cls):
        if f.metadata.get(_TRANSIENT_KEY):
            transient_fields.append(f.name)
            continue
        if f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING:
            raise SerializationSchemaError(
                f"{wire_name}.{f.name}: actor state fields need a default "
                f"(activation constructs the class with no arguments)"
            )
        pager: Pager[object] | None = f.metadata.get(_PAGER_KEY)
        if pager is not None:
            pagers[f.name] = pager
        state_fields.append(f.name)

    # a paged field stays out of the state schema: its type is exactly the kind casty
    # cannot encode, which is why it has a pager
    encoded = [name for name in state_fields if name not in pagers]
    state_cls = _register_state_schema(cls, wire_name, encoded)
    hooks = _find_hooks(cls, wire_name)
    methods = _collect_methods(cls, wire_name, hooks)
    resolve_quorum(write, replicas)  # fail at import on nonsense quorums
    resolve_quorum(read, replicas)

    state_hints = serde.fields_of(state_cls)
    declared = {f.name: f for f in dataclasses.fields(cls)}
    regimes: dict[str, Regime] = {
        name: (
            Paged(name=name, hint=declared[name].type, pager=pagers[name])
            if name in pagers
            else _regime(wire_name, name, state_hints[name], declared[name])
        )
        for name in state_fields
    }
    state_of, restore_into = _state_accessors(tuple(state_fields))

    info = ActorInfo(
        cls=cls,
        wire_name=wire_name,
        state_cls=state_cls,
        state_fields=tuple(state_fields),
        regimes=regimes,
        state_of=state_of,
        restore_into=restore_into,
        transient_fields=tuple(transient_fields),
        methods=methods,
        activate_hook=hooks[0],
        deactivate_hook=hooks[1],
        idle_timeout=idle_timeout,
        supervisor=supervisor,
        replicas=replicas,
        write=write,
        read=read,
        immutable_state=all(
            isinstance(regime, Integral) and regime.immutable for regime in regimes.values()
        ),
        kind=kind,
    )
    _by_type[cls] = info
    _by_name[wire_name] = info
    return cls


def explain(cls: type) -> str:
    """How each state field of an actor replicates.

    Two of the three regimes are chosen silently by type hint — this is
    where you look to see what was picked.

    Parameters
    ----------
    cls : type
        A `@casty.actor` class.

    Returns
    -------
    str
        One line per state field: name, annotation, regime (integral /
        tracked / paged) and a note.

    Raises
    ------
    ValueError
        If `cls` is not a `@casty.actor`.
    """
    info = info_of(cls)
    if info is None:
        raise ValueError(f"{cls.__qualname__} is not a @casty.actor")

    def annotation(hint: object) -> str:
        return hint.__name__ if isinstance(hint, type) else str(hint)

    rows: list[tuple[str, str, str, str]] = []
    for regime in info.regimes.values():
        match regime:
            case Integral(name=name, hint=hint, immutable=immutable):
                note = "immutable: compared, not re-encoded" if immutable else ""
                rows.append((name, annotation(hint), "integral", note))
            case Tracked(name=name, hint=hint):
                rows.append((name, annotation(hint), "tracked", "one page per entry"))
            case Paged(name=name, hint=hint, pager=pager):
                rows.append((name, annotation(hint), "paged", type(pager).__name__))
            case _:
                typing.assert_never(regime)
    header = f"{info.wire_name}  replicas={info.replicas} write={info.write_quorum}"
    if not rows:
        return f"{header}\n  (no state)"
    widths = [max(len(row[column]) for row in rows) for column in range(3)]
    body = [
        f"  {name:<{widths[0]}}  {hint:<{widths[1]}}  {kind:<{widths[2]}}  {note}".rstrip()
        for name, hint, kind, note in rows
    ]
    return "\n".join([header, *body])


def _regime(
    wire_name: str, name: str, hint: object, f: dataclasses.Field[object]
) -> Regime:
    default = _default_value(f)
    args = typing.get_args(hint)
    if typing.get_origin(hint) is dict and _immutable(args[1]):
        if default != {}:
            raise SerializationSchemaError(
                f"{wire_name}.{name}: a dict replicated per entry must default to empty "
                f"(an absent page means the entry is gone, not that it is at its default)"
            )
        return Tracked(name=name, hint=hint, key=args[0], value_hint=args[1])
    return Integral(
        name=name,
        hint=hint,
        default=codec.encode_raw(default),
        immutable=_immutable(hint),
    )


def _state_accessors(fields: tuple[str, ...]) -> tuple[StateOf, RestoreInto]:
    def state_of(instance: object) -> dict[str, object]:
        return {name: getattr(instance, name) for name in fields}

    def restore_into(instance: object, values: dict[str, object]) -> None:
        for name, value in values.items():
            if name in fields:  # unknown fields ignored (forward compat)
                setattr(instance, name, value)

    return state_of, restore_into


def _default_value(f: dataclasses.Field[object]) -> object:
    if f.default_factory is not dataclasses.MISSING:
        return f.default_factory()
    return f.default


_IMMUTABLE_PLAIN: tuple[type, ...] = (
    int, float, bool, str, bytes, datetime.datetime, uuid.UUID,
)


class _Params(typing.Protocol):
    frozen: bool


@typing.runtime_checkable
class _Dataclass(typing.Protocol):
    __dataclass_params__: _Params


def _immutable(tp: object, seen: frozenset[type] = frozenset()) -> bool:
    """Whether values of this annotation can never be mutated in place. Enums
    qualify (their values are primitives); tuples and frozensets only if their
    elements do; a registered message only if it is a frozen dataclass all the way
    down."""
    if tp is None or tp is types.NoneType:
        return True
    origin = typing.get_origin(tp)
    if origin is None:
        if not isinstance(tp, type):
            return False
        if tp in _IMMUTABLE_PLAIN or issubclass(tp, Enum):
            return True
        if tp in seen:
            return True  # a cycle back into a frozen message adds no mutability
        frozen = isinstance(tp, _Dataclass) and tp.__dataclass_params__.frozen
        if not (frozen and serde.is_registered(tp)):
            return False
        return all(_immutable(hint, seen | {tp}) for hint in serde.fields_of(tp).values())
    args = typing.get_args(tp)
    if origin in (types.UnionType, typing.Union) or origin in (tuple, frozenset):
        return all(_immutable(arg, seen) for arg in args if arg is not Ellipsis)
    return False  # list, set, dict


def _register_state_schema(cls: type, wire_name: str, state_fields: list[str]) -> type:
    """The state snapshot IS a message: a synthetic dataclass with the
    non-transient fields, registered as `<wire_name>#state`. Serializability of
    the state is validated for free, at import time."""
    specs: list[tuple[str, object, dataclasses.Field[object]]] = []
    for f in dataclasses.fields(cls):
        if f.name not in state_fields:
            continue
        placeholder: dataclasses.Field[object] = (
            dataclasses.field(default_factory=f.default_factory)
            if f.default_factory is not dataclasses.MISSING
            else dataclasses.field(default=f.default)
        )
        specs.append((f.name, f.type, placeholder))
    state_cls = dataclasses.make_dataclass(f"{cls.__name__}State", specs)
    # assigned after the fact: make_dataclass overwrites __module__ in `namespace`
    # with its *caller's*, and the annotations resolve against the actor's module
    state_cls.__module__ = cls.__module__
    return serde.message(name=f"{wire_name}#state")(state_cls)


def _find_hooks(cls: type, wire_name: str) -> tuple[str | None, str | None]:
    activate_hook: str | None = None
    deactivate_hook: str | None = None
    for attr_name, attr in inspect.getmembers(cls, inspect.isfunction):
        for mark, current in ((_ACTIVATE_MARK, activate_hook), (_DEACTIVATE_MARK, deactivate_hook)):
            if not getattr(attr, mark, False):
                continue
            if current is not None:
                kind = "activate" if mark == _ACTIVATE_MARK else "deactivate"
                raise SerializationSchemaError(
                    f"{wire_name}: duplicate {kind} hook ({current} and {attr_name})"
                )
            if not inspect.iscoroutinefunction(attr):
                raise SerializationSchemaError(f"{wire_name}.{attr_name}: hooks must be async")
            params = list(inspect.signature(attr).parameters)
            if params != ["self"]:
                raise SerializationSchemaError(
                    f"{wire_name}.{attr_name}: hooks take only self, got {params[1:]}"
                )
            if mark == _ACTIVATE_MARK:
                activate_hook = attr_name
            else:
                deactivate_hook = attr_name
    return activate_hook, deactivate_hook


_ASYNC_ITER_ORIGINS = (
    collections.abc.AsyncIterator,
    collections.abc.AsyncIterable,
    collections.abc.AsyncGenerator,
)


def _stream_elem(tp: object) -> object | None:
    """The element type T if tp is AsyncIterator[T]/AsyncIterable[T]/
    AsyncGenerator[T, None], else None. Bare (unparametrized) forms return the
    sentinel _NO_ELEM so the caller can reject them."""
    if typing.get_origin(tp) in _ASYNC_ITER_ORIGINS:
        args = typing.get_args(tp)
        return args[0] if args else _NO_ELEM
    return None


_NO_ELEM = object()


def _collect_methods(
    cls: type, wire_name: str, hooks: tuple[str | None, str | None]
) -> dict[str, MethodInfo]:
    methods: dict[str, MethodInfo] = {}
    for attr_name, attr in inspect.getmembers(cls, inspect.isfunction):
        if attr_name.startswith("_") or attr_name in hooks:
            continue
        is_async_gen = inspect.isasyncgenfunction(attr)
        if not (inspect.iscoroutinefunction(attr) or is_async_gen):
            raise SerializationSchemaError(
                f"{wire_name}.{attr_name}: public actor methods must be async "
                f"(prefix internal helpers with _)"
            )
        try:
            hints = typing.get_type_hints(attr)
        except NameError as exc:
            raise SerializationSchemaError(
                f"{wire_name}.{attr_name}: cannot resolve annotations ({exc}); "
                f"types must be importable at module scope"
            ) from exc
        signature = inspect.signature(attr)
        params: list[tuple[str, object]] = []
        param_order: list[str] = []
        stream_in: tuple[str, object] | None = None
        for param_name, param in signature.parameters.items():
            if param_name == "self":
                continue
            param_order.append(param_name)
            if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
                raise SerializationSchemaError(
                    f"{wire_name}.{attr_name}: *args/**kwargs are not supported"
                )
            if param_name not in hints:
                raise SerializationSchemaError(
                    f"{wire_name}.{attr_name}({param_name}): parameter needs a type annotation"
                )
            annotation = hints[param_name]
            elem = _stream_elem(annotation)
            if elem is not None:
                if elem is _NO_ELEM:
                    raise SerializationSchemaError(
                        f"{wire_name}.{attr_name}({param_name}): AsyncIterator needs an "
                        f"element type"
                    )
                if stream_in is not None:
                    raise SerializationSchemaError(
                        f"{wire_name}.{attr_name}: at most one AsyncIterator parameter "
                        f"(got {stream_in[0]!r} and {param_name!r})"
                    )
                serde._validate(elem, f"{wire_name}.{attr_name}({param_name})[]")
                stream_in = (param_name, elem)
                continue
            serde._validate(annotation, f"{wire_name}.{attr_name}({param_name})")
            params.append((param_name, annotation))
        if "return" not in hints:
            raise SerializationSchemaError(
                f"{wire_name}.{attr_name}: return type annotation is required"
            )
        returns = hints["return"]
        stream_out = _stream_elem(returns)
        if stream_out is not None:
            if stream_out is _NO_ELEM:
                raise SerializationSchemaError(
                    f"{wire_name}.{attr_name} return: AsyncIterator needs an element type"
                )
            if not is_async_gen:
                raise SerializationSchemaError(
                    f"{wire_name}.{attr_name}: returns AsyncIterator but is not an async "
                    f"generator (use `yield`)"
                )
            serde._validate(stream_out, f"{wire_name}.{attr_name} return[]")
        else:
            if is_async_gen:
                raise SerializationSchemaError(
                    f"{wire_name}.{attr_name}: async generator must be annotated "
                    f"-> AsyncIterator[T]"
                )
            serde._validate(returns, f"{wire_name}.{attr_name} return")
        methods[attr_name] = MethodInfo(
            name=attr_name,
            params=tuple(params),
            returns=returns,
            stream_in=stream_in,
            stream_out=stream_out,
            param_order=tuple(param_order),
        )
    return methods
