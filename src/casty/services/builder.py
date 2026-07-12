from __future__ import annotations

import asyncio
import inspect
import itertools
import typing
from collections.abc import Awaitable, Callable, Iterator

from casty.actors import registry
from casty.actors.context import current_context
from casty.actors.host import Reply
from casty.errors import ActorFailedError, ActorUnavailableError, SerializationSchemaError

type _Method = Callable[..., Awaitable[object]]


class _Coordinator(typing.Protocol):
    """The shape of every generated coordinator. All transient: a service holds
    nothing across activations."""

    _impl: object  # the user's instance, built by the activate hook
    _pending: dict[int, Reply]  # call id -> the caller's detached reply
    _seq: Iterator[int]
    _tasks: set[asyncio.Task[None]]
    _sem: asyncio.Semaphore | None  # concurrency, or None for unbounded


@typing.overload
def service[T](cls: type[T], /) -> type[T]: ...


@typing.overload
def service[T](
    *, name: str | None = None, concurrency: int | None = None
) -> Callable[[type[T]], type[T]]: ...


def service(
    cls: type | None = None,
    /,
    *,
    name: str | None = None,
    concurrency: int | None = None,
) -> type | Callable[[type], type]:
    """Declare a stateless service: concurrent RPC on top of an actor.

    Public async methods become the proxy interface, exactly as on an actor. The
    difference is the handler: it starts the method as a detached task and
    returns, so N calls to the same service progress together instead of
    queueing behind one another. State belongs in actors — reach them with
    `casty.context().actor(...)` from inside a method.

    Parameters
    ----------
    name : str | None
        Stable wire name. Defaults to `module.QualName`.
    concurrency : int | None
        Concurrent calls allowed per activation. None is unbounded; a value
        applies backpressure at the mailbox once the limit is reached.

    Raises
    ------
    SerializationSchemaError
        At import time: annotated fields (a service is stateless), lifecycle
        hooks, streaming or non-async methods, or an unserializable
        annotation.

    Examples
    --------
    >>> @casty.service
    ... class Notifier:
    ...     async def send(self, email: str, body: str) -> bool:
    ...         ...
    """
    if cls is None:

        def apply(inner: type) -> type:
            return _build(inner, name, concurrency)

        return apply
    return _build(cls, name, concurrency)


def _build(user_cls: type, name: str | None, concurrency: int | None) -> type:
    qualname = user_cls.__qualname__
    if user_cls.__dict__.get("__annotations__"):
        raise SerializationSchemaError(
            f"{qualname}: a service is stateless; put state in an actor"
        )
    for attr in vars(user_cls).values():
        if getattr(attr, registry._ACTIVATE_MARK, False) or getattr(
            attr, registry._DEACTIVATE_MARK, False
        ):
            raise SerializationSchemaError(
                f"{qualname}: services generate their own lifecycle hooks"
            )

    namespace: dict[str, object] = {
        "__module__": user_cls.__module__,
        "__qualname__": qualname,
        "__annotations__": {
            "_impl": object,
            "_pending": dict[int, Reply],
            "_seq": Iterator[int],
            "_tasks": set[asyncio.Task[None]],
            "_sem": asyncio.Semaphore | None,
        },
        "_impl": registry.transient(),
        "_pending": registry.transient(factory=dict),
        "_seq": registry.transient(factory=lambda: itertools.count()),
        "_tasks": registry.transient(factory=set),
        "_sem": registry.transient(
            factory=(lambda: asyncio.Semaphore(concurrency))
            if concurrency is not None
            else lambda: None
        ),
        "_boot": _boot(user_cls),
        "_shutdown": _shutdown,
    }
    for method_name, method in vars(user_cls).items():
        if method_name.startswith("_") or not inspect.isfunction(method):
            continue
        _validate(qualname, method_name, method)
        namespace[method_name] = _dispatcher(method_name, method)

    coordinator = type(user_cls.__name__, (), namespace)
    return registry.register_service(coordinator, name)


def _validate(qualname: str, method_name: str, method: Callable[..., object]) -> None:
    if inspect.isasyncgenfunction(method) or not inspect.iscoroutinefunction(method):
        raise SerializationSchemaError(
            f"{qualname}.{method_name}: public service methods must be async "
            f"(and not async generators — streaming services are not supported)"
        )
    try:
        hints = typing.get_type_hints(method)
    except NameError as exc:
        raise SerializationSchemaError(
            f"{qualname}.{method_name}: cannot resolve annotations ({exc}); "
            f"types must be importable at module scope"
        ) from exc
    for hint_name, hint in hints.items():
        if registry._stream_elem(hint) is not None:
            raise SerializationSchemaError(
                f"{qualname}.{method_name}({hint_name}): streaming is not supported "
                f"in services"
            )


def _dispatcher(method_name: str, method: _Method) -> _Method:
    """The coordinator's handler: register the caller's reply, fire the work off
    the mailbox, return. Serial (it runs in the actor's worker) and O(1), so the
    next request is served immediately."""

    async def dispatcher(self: _Coordinator, *args: object, **kwargs: object) -> None:
        ctx = current_context()
        if self._sem is not None:
            await self._sem.acquire()  # over the limit, hold the mailbox: real backpressure
        reply = ctx.detach()
        call_id = next(self._seq)
        self._pending[call_id] = reply
        task = asyncio.create_task(
            _drive(
                _wire_name(ctx.actor_class),
                self._impl,
                self._pending,
                self._sem,
                call_id,
                method,
                args,
                kwargs,
            )
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    dispatcher.__name__ = dispatcher.__qualname__ = method_name
    # the registry reads params/kinds off the dispatcher: cloning the real
    # signature keeps *args/**kwargs out of its sight and the proxy identical to
    # the declared API. Hints resolved here (in the user's module) spare it the
    # coordinator's __globals__, which cannot see the user's names.
    dispatcher.__signature__ = inspect.signature(method)  # type: ignore[attr-defined]
    dispatcher.__annotations__ = typing.get_type_hints(method)
    return dispatcher


def _wire_name(coordinator: type) -> str:
    info = registry.info_of(coordinator)
    assert info is not None  # the coordinator is registered before any call reaches it
    return info.wire_name


async def _drive(
    wire_name: str,
    impl: object,
    pending: dict[int, Reply],
    sem: asyncio.Semaphore | None,
    call_id: int,
    method: _Method,
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> None:
    """The user's method, running outside the mailbox and concurrent with every
    other call. Inherits the handler's context (create_task copies it), so
    `casty.context().actor(...)` and the ask chain work as usual."""
    try:
        value = await method(impl, *args, **kwargs)
    except asyncio.CancelledError:
        reply = pending.pop(call_id, None)
        if reply is not None:  # shutdown: the caller gets a typed error, not a hang
            reply.fail(
                ActorUnavailableError(f"{wire_name}.{method.__name__}: cancelled while in flight")
            )
        raise
    except Exception as exc:
        # the reply resolves out of band, so the host never sees this raise: wrap
        # it here into the same error a failing actor handler produces
        pending.pop(call_id).fail(
            ActorFailedError(
                wire_name, registry.SERVICE_KEY, f"{type(exc).__name__}: {exc}"
            )
        )
    else:
        pending.pop(call_id).set(value)
    finally:
        if sem is not None:
            sem.release()


def _boot(user_cls: type) -> Callable[[_Coordinator], Awaitable[None]]:
    async def _boot(self: _Coordinator) -> None:
        self._impl = user_cls()

    return registry.activate(_boot)


@registry.deactivate
async def _shutdown(self: _Coordinator) -> None:
    tasks = list(self._tasks)
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
