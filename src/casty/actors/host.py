from __future__ import annotations

import asyncio
import contextlib
import logging
import typing
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from casty.actors import paging
from casty.actors.context import ActorContext, reset_context, set_context
from casty.actors.registry import (
    ActorInfo,
    Directive,
    FailureContext,
    MethodInfo,
    Supervisor,
    default_supervisor,
)
from casty.actors.replication import Hlc, Replication, StaleActivationError
from casty.actors.streaming import StreamSink
from casty.errors import (
    ActorFailedError,
    ActorUnavailableError,
    CastyError,
    QuorumUnavailableError,
    RangeMovingError,
    ReentrancyError,
    SerializationError,
)

logger = logging.getLogger("casty.actors")


class Router(typing.Protocol):
    """What the host needs back from its owner (Node/Client): proxy creation
    for ctx.actor()."""

    def actor[T](self, cls: type[T], key: str) -> T: ...


@dataclass(slots=True)
class _CallItem:
    method_name: str
    args: list[object]
    chain: list[str]
    future: asyncio.Future[object]


@dataclass(slots=True)
class _StreamItem:
    """A streaming call (spec 07). Occupies the worker for the stream's whole
    life; commits its snapshot once at the end, like any handler. `stream_out`
    means the method is an async generator; otherwise it is a coroutine whose
    single return value is sent as the sole element."""

    method_name: str
    kwargs: dict[str, object]  # unary args + the input iterator under its param name
    stream_out: bool
    sink: StreamSink
    chain: list[str]
    done: asyncio.Future[None]
    aborted: bool = False  # consumer abandoned the stream; body cancel is not a worker fault
    body: asyncio.Task[None] | None = None


_Item = _CallItem | _StreamItem


@dataclass(slots=True)
class _Activation:
    info: ActorInfo
    key: str
    instance: object
    queue: asyncio.Queue[_Item | None] = field(default_factory=asyncio.Queue)
    worker: asyncio.Task[None] | None = None
    closing: bool = False
    run_deactivate_hook: bool = True
    draining: bool = False
    failures: int = 0
    done: asyncio.Event = field(default_factory=asyncio.Event)
    hlc: Hlc | None = None  # last committed HLC (replicated actors)
    state: paging.State = field(default_factory=paging.State)  # committed basis (spec 09)
    # last committed state values; with immutable_state a cheap == against the
    # current values skips the page encode after read-only handlers. None forces
    # the encode path (e.g. after a rollback).
    last_values: dict[str, object] | None = None
    # replies detached by handlers (spec 08) and not yet resolved. Idle
    # deactivation waits for zero: the work runs outside the mailbox.
    inflight: int = 0

    @property
    def identity(self) -> str:
        return f"{self.info.wire_name}/{self.key}"


@dataclass(slots=True)
class Reply:
    """The caller's pending result, handed to a handler by `ctx.detach()`. Fires
    once; the activation's in-flight count drops on the first `set`/`fail`."""

    _future: asyncio.Future[object]
    _activation: _Activation
    _fired: bool = False

    def set(self, value: object) -> None:
        if self._release() and not self._future.done():
            self._future.set_result(value)

    def fail(self, exc: BaseException) -> None:
        if self._release() and not self._future.done():
            self._future.set_exception(exc)

    def _release(self) -> bool:
        # fire-once and independent of the future's state: a cancelled local
        # caller cancels its own future (done() = True), and if the decrement
        # hung off that, inflight would leak and the activation never deactivate.
        if self._fired:
            return False
        self._fired = True
        self._activation.inflight -= 1
        return True


class ActorHost:
    """Owns the activations of this node: one instance + FIFO mailbox + worker
    per (actor type, key). One handler at a time per actor."""

    def __init__(
        self,
        *,
        router: Router,
        replication: Replication | None,
        supervisor: Supervisor | None = None,
        default_idle_timeout: float = 300.0,
    ) -> None:
        self.router = router
        self.replication = replication  # None: this host runs actors single-copy
        self._supervisor = supervisor or default_supervisor
        self._default_idle_timeout = default_idle_timeout
        self._activations: dict[tuple[str, str], _Activation] = {}
        self._draining = False

    def _replication_for(self, info: ActorInfo) -> Replication | None:
        """The replicator for this actor, or None when it runs single-copy —
        either because the class opts out (replicas=1) or the host has no
        replication at all (local mode)."""
        return self.replication if info.replicated else None

    @property
    def activation_count(self) -> int:
        return len(self._activations)

    def is_active(self, wire_name: str, key: str) -> bool:
        return (wire_name, key) in self._activations

    async def dispatch(
        self, info: ActorInfo, key: str, method_name: str, args: list[object], chain: list[str]
    ) -> object:
        if self._draining:
            raise ActorUnavailableError(f"node is draining; {info.wire_name}/{key} rejected")
        identity = f"{info.wire_name}/{key}"
        if identity in chain:
            raise ReentrancyError(f"ask cycle detected: {' -> '.join([*chain, identity])}")
        while True:
            activation = self._activations.get((info.wire_name, key))
            if activation is None:
                activation = self._create(info, key)
            if activation.closing:
                await activation.done.wait()
                continue
            future: asyncio.Future[object] = asyncio.get_running_loop().create_future()
            activation.queue.put_nowait(_CallItem(method_name, args, chain, future))
            return await future

    async def dispatch_stream(
        self,
        info: ActorInfo,
        key: str,
        method: MethodInfo,
        kwargs: dict[str, object],
        in_iter: AsyncIterator[object] | None,
        sink: StreamSink,
        chain: list[str],
    ) -> None:
        """Run a streaming handler (spec 07 §4). Returns once the handler has
        finished and the sink has been closed/failed. Errors are delivered on
        the sink; this coroutine itself does not raise on handler failure."""
        identity = f"{info.wire_name}/{key}"
        if self._draining:
            await sink.fail(ActorUnavailableError(f"node is draining; {identity} rejected"))
            return
        if identity in chain:
            cycle = " -> ".join([*chain, identity])
            await sink.fail(ReentrancyError(f"ask cycle detected: {cycle}"))
            return
        while True:
            activation = self._activations.get((info.wire_name, key))
            if activation is None:
                activation = self._create(info, key)
            if activation.closing:
                await activation.done.wait()
                continue
            call_kwargs = dict(kwargs)
            if method.stream_in is not None and in_iter is not None:
                call_kwargs[method.stream_in[0]] = in_iter
            done: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            stream_item = _StreamItem(
                method.name, call_kwargs, method.stream_out is not None, sink, chain, done
            )
            activation.queue.put_nowait(stream_item)
            try:
                await done
            except asyncio.CancelledError:
                # consumer abandoned (e.g. `break`): abort the in-flight body so a
                # long/infinite stream stops holding the worker.
                stream_item.aborted = True
                if stream_item.body is not None and not stream_item.body.done():
                    stream_item.body.cancel()
                raise
            return

    async def stop(self, drain_timeout: float) -> None:
        """Stop accepting work, drain in-flight mailboxes, run deactivate hooks."""
        self._draining = True
        activations = list(self._activations.values())
        for activation in activations:
            activation.draining = True
            activation.queue.put_nowait(None)  # wake an idle worker
        if activations:
            waiters = [activation.done.wait() for activation in activations]
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(asyncio.gather(*waiters), drain_timeout)
        for activation in activations:  # anything that outlived the timeout
            if activation.worker is not None and not activation.worker.done():
                activation.worker.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await activation.worker

    # --- internals ------------------------------------------------------------------

    def _create(self, info: ActorInfo, key: str) -> _Activation:
        activation = _Activation(info=info, key=key, instance=info.cls())
        self._activations[(info.wire_name, key)] = activation
        activation.worker = asyncio.create_task(self._run(activation))
        return activation

    async def _run(self, activation: _Activation) -> None:
        info = activation.info
        idle_timeout = (
            info.idle_timeout if info.idle_timeout is not None else self._default_idle_timeout
        )
        try:
            if not await self._startup(activation):
                return
            while True:
                try:
                    # under load the queue is rarely empty; get_nowait skips the
                    # idle timer that wait_for would arm and cancel per message
                    item = activation.queue.get_nowait()
                except asyncio.QueueEmpty:
                    try:
                        item = await asyncio.wait_for(activation.queue.get(), idle_timeout)
                    except TimeoutError:
                        if activation.queue.empty() and activation.inflight == 0:
                            activation.closing = True
                            break
                        continue
                if item is None:  # drain wake-up
                    if activation.draining and activation.queue.empty():
                        activation.closing = True
                        break
                    continue
                if isinstance(item, _StreamItem):
                    await self._handle_stream(activation, item)
                else:
                    await self._handle(activation, item)
                if activation.closing:
                    break
                if activation.draining and activation.queue.empty():
                    activation.closing = True
                    break
        except asyncio.CancelledError:
            activation.closing = True
            raise
        finally:
            await self._finalize(activation)

    async def _startup(self, activation: _Activation) -> bool:
        """Activation sequence: quorum handshake (replicated actors) -> page basis ->
        state restore -> activate hook. Failure fails all queued callers and discards
        the activation."""
        info = activation.info
        replication = self._replication_for(info)
        try:
            if replication is not None:
                stored = await replication.handshake(info, activation.key)
                activation.state = paging.activate(
                    info, stored.pages if stored is not None else {}, activation.instance
                )
                if stored is not None:
                    activation.hlc = stored.hlc
                # snapshotted *before* the hook: these are the values the page basis was
                # built from, so a field the hook mutates still looks dirty to the next
                # commit and rides along on it (spec 09 §9)
                activation.last_values = info.state_of(activation.instance)
            if info.activate_hook is not None:
                await self._run_hook(activation, info.activate_hook)
            return True
        except Exception as exc:
            activation.closing = True
            activation.run_deactivate_hook = False
            error: CastyError = (
                exc
                if isinstance(exc, CastyError)
                else ActorFailedError(
                    info.wire_name,
                    activation.key,
                    f"activation failed: {type(exc).__name__}: {exc}",
                )
            )
            while not activation.queue.empty():
                item = activation.queue.get_nowait()
                if item is not None:
                    await self._fail_item(item, error)
            return False

    async def _handle(self, activation: _Activation, item: _CallItem) -> None:
        info = activation.info
        reply = Reply(item.future, activation)
        ctx = ActorContext(
            self,
            info.cls,
            activation.key,
            chain=[*item.chain, activation.identity],
            reply=reply,
        )
        token = set_context(ctx)
        try:
            method = getattr(activation.instance, item.method_name)
            result = await method(*item.args)
        except asyncio.CancelledError:
            if not item.future.done():
                item.future.set_exception(
                    ActorUnavailableError(f"{activation.identity}: node stopping")
                )
            raise
        except Exception as exc:
            activation.failures += 1
            directive = self._decide(info, activation, exc)
            if not item.future.done():
                item.future.set_exception(
                    ActorFailedError(
                        info.wire_name, activation.key, f"{type(exc).__name__}: {exc}"
                    )
                )
            if ctx.detached:
                # the handler failed after detaching: nothing else will fire the
                # Reply, so release the in-flight slot here (the future is already
                # resolved above; fire-once leaves it untouched)
                reply.fail(exc)
            if directive is Directive.RESET:
                activation.closing = True
                activation.run_deactivate_hook = False
            elif directive is Directive.STOP:
                activation.closing = True
        else:
            activation.failures = 0
            commit_error = await self._commit_if_mutated(activation)
            if ctx.detached:
                pass  # the Reply resolves the caller out of band
            elif not item.future.done():
                if commit_error is None:
                    item.future.set_result(result)
                else:
                    item.future.set_exception(commit_error)
            if ctx.deactivation_requested:
                activation.closing = True
        finally:
            reset_context(token)

    async def _handle_stream(self, activation: _Activation, item: _StreamItem) -> None:
        """Run the streaming body as a child task so it can be cancelled on
        consumer abandon (item.aborted) without taking the worker down. A cancel
        that is *not* an abort is worker shutdown — propagate it."""
        item.body = asyncio.create_task(self._stream_body(activation, item))
        try:
            await item.body
        except asyncio.CancelledError:
            if item.aborted:
                return  # consumer abandoned; nothing to report, worker keeps serving
            item.body.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await item.body
            raise
        finally:
            if not item.done.done():
                item.done.set_result(None)

    async def _stream_body(self, activation: _Activation, item: _StreamItem) -> None:
        info = activation.info
        ctx = ActorContext(
            self, info.cls, activation.key, chain=[*item.chain, activation.identity]
        )
        token = set_context(ctx)
        try:
            method = getattr(activation.instance, item.method_name)
            if item.stream_out:
                async for elem in method(**item.kwargs):
                    await item.sink.send(elem)
            else:
                await item.sink.send(await method(**item.kwargs))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            activation.failures += 1
            directive = self._decide(info, activation, exc)
            error = ActorFailedError(
                info.wire_name, activation.key, f"{type(exc).__name__}: {exc}"
            )
            with contextlib.suppress(Exception):
                await item.sink.fail(error)
            if directive is Directive.RESET:
                activation.closing = True
                activation.run_deactivate_hook = False
            elif directive is Directive.STOP:
                activation.closing = True
        else:
            activation.failures = 0
            commit_error = await self._commit_if_mutated(activation)
            with contextlib.suppress(Exception):
                if commit_error is None:
                    await item.sink.close()
                else:
                    await item.sink.fail(commit_error)
            if ctx.deactivation_requested:
                activation.closing = True
        finally:
            reset_context(token)

    async def _commit_if_mutated(self, activation: _Activation) -> CastyError | None:
        """Replicate the pages this handler changed. On failure the in-memory state
        rolls back to the last committed one — a fenced minority owner never
        advances."""
        info = activation.info
        replication = self._replication_for(info)
        if replication is None:
            return None
        values = info.state_of(activation.instance)
        if info.immutable_state and values == activation.last_values:
            return None
        try:
            delta = paging.diff(
                info, activation.instance, values, activation.last_values, activation.state
            )
        except SerializationError as exc:
            # the encode itself failed, part-way through: no field is trustworthy
            paging.reset(info, activation.state, activation.instance)
            activation.last_values = None
            return exc
        if delta.empty:
            paging.commit(activation.state, delta)
            activation.last_values = values
            return None
        try:
            activation.hlc = await replication.commit(
                info, activation.key, activation.hlc, delta.pages, delta.dropped
            )
        except (QuorumUnavailableError, SerializationError) as exc:
            # nothing was committed anywhere: undo the pages this handler touched
            paging.rollback(info, activation.instance, activation.state, delta)
            activation.last_values = None
            return exc
        except StaleActivationError as exc:
            # another owner committed newer history; this activation is stale
            activation.closing = True
            activation.run_deactivate_hook = False
            return RangeMovingError(
                f"{activation.identity}: stale activation discarded ({exc}); retry"
            )
        paging.commit(activation.state, delta)
        activation.last_values = values
        return None

    def _decide(self, info: ActorInfo, activation: _Activation, exc: Exception) -> Directive:
        supervisor = info.supervisor or self._supervisor
        try:
            return supervisor(
                info.cls, activation.key, exc, FailureContext(failures=activation.failures)
            )
        except Exception:
            logger.exception("supervisor for %s raised; falling back to KEEP", info.wire_name)
            return Directive.KEEP

    async def _run_hook(self, activation: _Activation, hook_name: str) -> None:
        """Hooks run with a context (ctx.key, ctx.actor work), chained to the
        activation itself so a hook asking its own actor fails fast."""
        ctx = ActorContext(
            self, activation.info.cls, activation.key, chain=[activation.identity]
        )
        token = set_context(ctx)
        try:
            await getattr(activation.instance, hook_name)()
        finally:
            reset_context(token)

    async def _finalize(self, activation: _Activation) -> None:
        if activation.run_deactivate_hook and activation.info.deactivate_hook is not None:
            try:
                await self._run_hook(activation, activation.info.deactivate_hook)
            except Exception:
                logger.exception("%s: deactivate hook raised", activation.identity)
        current = self._activations.get((activation.info.wire_name, activation.key))
        if current is activation:
            del self._activations[(activation.info.wire_name, activation.key)]
        activation.done.set()
        self._requeue_leftovers(activation)

    def _requeue_leftovers(self, activation: _Activation) -> None:
        """Items still in the mailbox after deactivation are re-dispatched to a
        fresh activation (or failed, if the host is draining). Streaming items
        are always failed terminally — the caller reopens (the input iterator may
        already be partly consumed, so a silent redispatch would drop elements)."""
        leftovers: list[_Item] = []
        while not activation.queue.empty():
            item = activation.queue.get_nowait()
            if item is not None:
                leftovers.append(item)
        for item in leftovers:
            if isinstance(item, _StreamItem):
                error = ActorUnavailableError(f"{activation.identity}: reopen stream")
                task = asyncio.ensure_future(self._fail_item(item, error))
                task.add_done_callback(lambda _: None)
                continue
            if self._draining:
                if not item.future.done():
                    item.future.set_exception(
                        ActorUnavailableError(f"{activation.identity}: node stopping")
                    )
                continue
            task = asyncio.create_task(self._redispatch(activation.info, activation.key, item))
            task.add_done_callback(lambda _: None)

    async def _fail_item(self, item: _Item, error: CastyError) -> None:
        if isinstance(item, _StreamItem):
            with contextlib.suppress(Exception):
                await item.sink.fail(error)
            if not item.done.done():
                item.done.set_result(None)
        elif not item.future.done():
            item.future.set_exception(error)

    async def _redispatch(self, info: ActorInfo, key: str, item: _CallItem) -> None:
        try:
            result = await self.dispatch(info, key, item.method_name, item.args, item.chain)
        except Exception as exc:  # forwarded to the caller's future
            if not item.future.done():
                item.future.set_exception(exc)
        else:
            if not item.future.done():
                item.future.set_result(result)
