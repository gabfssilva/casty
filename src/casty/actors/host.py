from __future__ import annotations

import asyncio
import contextlib
import logging
import typing
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime

from casty.actors import inspection, paging
from casty.actors.context import ActorContext, Schedule, reset_context, set_context
from casty.actors.reactivation import ACTIVE_DATA, ACTIVE_PAGE
from casty.actors.registry import (
    ActorInfo,
    Directive,
    FailureContext,
    MethodInfo,
    Paged,
    Supervisor,
    default_supervisor,
)
from casty.actors.replication import Hlc, Page, Replication, StaleActivationError
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
from casty.serde import codec

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
    kwargs: dict[str, object] = field(default_factory=dict)
    # a schedule tick (spec 04 §10): dropped, not re-dispatched, if the
    # activation closes before it runs — a tick never creates an activation
    tick: bool = False


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
    unary_args: list[object] = field(default_factory=list)  # for spy payloads (spec 11)
    aborted: bool = False  # consumer abandoned the stream; body cancel is not a worker fault
    body: asyncio.Task[None] | None = None


_Item = _CallItem | _StreamItem

# why the activation is closing: decides what happens to the durable mark
type CloseReason = typing.Literal[
    "idle", "deactivate", "stop", "reset", "moved", "drain", "stale", "activation-failed"
]


@dataclass(slots=True)
class _Activation:
    info: ActorInfo
    key: str
    instance: object
    queue: asyncio.Queue[_Item | None] = field(default_factory=asyncio.Queue)
    worker: asyncio.Task[None] | None = None
    closing: bool = False
    close_reason: CloseReason | None = None  # stamped where closing is decided
    seq: int = 0  # last spy event sequence number (spec 11 §1)
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
    # live schedules by name (spec 04 §10); cancelled in _finalize
    schedules: dict[str, asyncio.Task[None]] = field(default_factory=dict)

    @property
    def identity(self) -> str:
        return f"{self.info.wire_name}/{self.key}"

    def close(self, reason: CloseReason) -> None:
        """Mark closing; the first reason wins."""
        self.closing = True
        if self.close_reason is None:
            self.close_reason = reason


@dataclass(slots=True)
class Reply:
    """The caller's pending result, handed to a handler by `ctx.detach()`.

    Fires once: the first `set`/`fail` resolves the caller and drops the
    activation's in-flight count; later calls are ignored. An activation with
    a pending Reply does not idle-deactivate.
    """

    _future: asyncio.Future[object]
    _activation: _Activation
    _fired: bool = False

    def set(self, value: object) -> None:
        """Resolve the caller with `value`. No-op if already fired."""
        if self._release() and not self._future.done():
            self._future.set_result(value)

    def fail(self, exc: BaseException) -> None:
        """Resolve the caller with an exception. No-op if already fired."""
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
        reactivation_retry: float = 5.0,
        hub: inspection.Hub | None = None,
        interceptor: inspection.Interceptor | None = None,
    ) -> None:
        self.router = router
        self.replication = replication  # None: this host runs actors single-copy
        self.hub = hub if hub is not None else inspection.Hub(node=uuid.uuid4())
        self._interceptor = interceptor
        self._supervisor = supervisor or default_supervisor
        self._default_idle_timeout = default_idle_timeout
        self._reactivation_retry = reactivation_retry
        self._activations: dict[tuple[str, str], _Activation] = {}
        self._draining = False
        # STOP clears that did not land: retried in the background, suppressing
        # this node's own poke so it does not resurrect what it is burying.
        self.pending_clears: set[tuple[str, str]] = set()
        self._clear_tasks: set[asyncio.Task[None]] = set()

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

    def placed_activations(self) -> list[tuple[str, str]]:
        """Live placed activations (services are unplaced and excluded)."""
        return [
            (a.info.wire_name, a.key)
            for a in self._activations.values()
            if a.info.kind == "actor"
        ]

    def ensure_active(self, info: ActorInfo, key: str) -> None:
        """Activation without a message: no-op if the activation exists,
        otherwise the normal startup sequence runs and the worker waits on its
        empty mailbox."""
        if self._draining or (info.wire_name, key) in self._activations:
            return
        self._create(info, key)

    def discard(self, wire_name: str, key: str) -> None:
        """The ring moved this key away: drain the mailbox and close as
        `moved`. The mark is untouched — the new owner's poke resurrects the
        activity."""
        activation = self._activations.get((wire_name, key))
        if activation is None or activation.closing:
            return
        if activation.close_reason is None:
            activation.close_reason = "moved"
        activation.draining = True
        activation.queue.put_nowait(None)

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
                method.name,
                call_kwargs,
                method.stream_out is not None,
                sink,
                chain,
                done,
                unary_args=[kwargs[name] for name, _ in method.params],
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
        for task in list(self._clear_tasks):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    def emit(
        self,
        activation: _Activation,
        method_name: str,
        args: list[object],
        kwargs: dict[str, object],
    ) -> None:
        """Fire-and-forget self-message (spec 04 §10): enqueue now, no reply.
        Failures were already routed through the supervisor; the future is
        observed only so the exception does not die unretrieved."""
        future: asyncio.Future[object] = asyncio.get_running_loop().create_future()
        future.add_done_callback(
            lambda f: None if f.cancelled() else f.exception()
        )
        activation.queue.put_nowait(
            _CallItem(
                method_name,
                args,
                chain=[activation.identity],
                future=future,
                kwargs=kwargs,
                tick=True,
            )
        )

    def start_schedule(
        self,
        activation: _Activation,
        name: str,
        method_name: str,
        args: list[object],
        kwargs: dict[str, object],
        *,
        after: float,
        every: float | None,
    ) -> Schedule:
        """Arm a schedule on this activation (spec 04 §10). Re-using a name
        cancels and replaces the previous schedule."""
        existing = activation.schedules.pop(name, None)
        if existing is not None:
            existing.cancel()
        task = asyncio.create_task(
            self._schedule_loop(activation, name, method_name, args, kwargs, after, every)
        )
        activation.schedules[name] = task
        return Schedule(name, task)

    async def _schedule_loop(
        self,
        activation: _Activation,
        name: str,
        method_name: str,
        args: list[object],
        kwargs: dict[str, object],
        after: float,
        every: float | None,
    ) -> None:
        """Fixed-delay: the next tick arms only after the previous handler
        completed, so a congested mailbox never accumulates ticks."""
        try:
            delay = after if after > 0 else (every if every is not None else 0.0)
            while True:
                await asyncio.sleep(delay)
                if activation.closing:
                    return
                future: asyncio.Future[object] = asyncio.get_running_loop().create_future()
                activation.queue.put_nowait(
                    _CallItem(
                        method_name,
                        args,
                        chain=[activation.identity],
                        future=future,
                        kwargs=kwargs,
                        tick=True,
                    )
                )
                # tick failures were already routed through the supervisor
                with contextlib.suppress(Exception):
                    await future
                if every is None:
                    return
                delay = every
        finally:
            if activation.schedules.get(name) is asyncio.current_task():
                del activation.schedules[name]

    def state_values(self, wire_name: str, key: str) -> dict[str, object] | None:
        """Live state values of an activation (spec 11 §8), or None when
        `(wire_name, key)` is not active here. `transient()` and `paged()`
        fields are excluded — the serializable surface, same as replication."""
        activation = self._activations.get((wire_name, key))
        if activation is None:
            return None
        info = activation.info
        return {
            name: value
            for name, value in info.state_of(activation.instance).items()
            if not isinstance(info.regimes[name], Paged)
        }

    # --- inspection (spec 11 §5): events are born here, the only place handlers run --

    def _observed(self) -> bool:
        return self._interceptor is not None or self.hub.active

    def _publish(self, event: inspection.SpyEvent) -> None:
        if self._interceptor is not None:
            try:
                self._interceptor(event)
            except Exception:
                logger.exception("interceptor raised on %s", type(event).__name__)
        self.hub.publish(event)

    def _stamp(self, activation: _Activation) -> tuple[int, datetime]:
        activation.seq += 1
        return activation.seq, datetime.now(UTC)

    def _encode_payloads(self, values: list[object]) -> list[bytes] | None:
        # best-effort: emit args (e.g. of a tick) may not be serializable
        try:
            return [codec.encode_raw(value) for value in values]
        except Exception:
            return None

    def _spy_activated(self, activation: _Activation) -> None:
        if not self._observed():
            return
        seq, ts = self._stamp(activation)
        self._publish(
            inspection.Activated(
                actor=activation.info.wire_name, key=activation.key,
                node=self.hub.node, seq=seq, ts=ts,
            )
        )

    def _spy_received(
        self,
        activation: _Activation,
        method_name: str,
        streaming: bool,
        chain: list[str],
        args: list[object],
    ) -> None:
        seq, ts = self._stamp(activation)
        self._publish(
            inspection.Received(
                actor=activation.info.wire_name, key=activation.key,
                node=self.hub.node, seq=seq, ts=ts,
                method=method_name, streaming=streaming, chain=list(chain),
                args=self._encode_payloads(args) if self.hub.payloads_armed else None,
            )
        )

    def _spy_completed(
        self,
        activation: _Activation,
        method_name: str,
        streaming: bool,
        started: float,
        changed: list[str] | None,
        result: object,
        commit_error: CastyError | None,
    ) -> None:
        if not self._observed():
            return
        result_bytes: bytes | None = None
        if self.hub.payloads_armed and not streaming:
            encoded = self._encode_payloads([result])
            result_bytes = encoded[0] if encoded is not None else None
        duration = asyncio.get_running_loop().time() - started if started else 0.0
        seq, ts = self._stamp(activation)
        self._publish(
            inspection.Completed(
                actor=activation.info.wire_name, key=activation.key,
                node=self.hub.node, seq=seq, ts=ts,
                method=method_name, streaming=streaming, duration=duration,
                changed=changed, result=result_bytes,
                commit_error=str(commit_error) if commit_error is not None else None,
            )
        )

    def _spy_failed(
        self, activation: _Activation, method_name: str, exc: Exception, directive: Directive
    ) -> None:
        if not self._observed():
            return
        seq, ts = self._stamp(activation)
        self._publish(
            inspection.Failed(
                actor=activation.info.wire_name, key=activation.key,
                node=self.hub.node, seq=seq, ts=ts,
                method=method_name, error=f"{type(exc).__name__}: {exc}",
                directive=directive.name.lower(),
            )
        )

    def _spy_deactivated(self, activation: _Activation) -> None:
        if not self._observed():
            return
        seq, ts = self._stamp(activation)
        self._publish(
            inspection.Deactivated(
                actor=activation.info.wire_name, key=activation.key,
                node=self.hub.node, seq=seq, ts=ts,
                reason=activation.close_reason or "drain",
            )
        )

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
            self._spy_activated(activation)
            while True:
                try:
                    # under load the queue is rarely empty; get_nowait skips the
                    # idle timer that wait_for would arm and cancel per message
                    item = activation.queue.get_nowait()
                except asyncio.QueueEmpty:
                    if idle_timeout == float("inf"):  # a real untimed wait, not a timer
                        item = await activation.queue.get()
                    else:
                        try:
                            item = await asyncio.wait_for(activation.queue.get(), idle_timeout)
                        except TimeoutError:
                            if activation.queue.empty() and activation.inflight == 0:
                                activation.close("idle")
                                break
                            continue
                if item is None:  # drain wake-up
                    if activation.draining and activation.queue.empty():
                        activation.close("drain")
                        break
                    continue
                if isinstance(item, _StreamItem):
                    await self._handle_stream(activation, item)
                else:
                    await self._handle(activation, item)
                if activation.closing:
                    break
                if activation.draining and activation.queue.empty():
                    activation.close("drain")
                    break
        except asyncio.CancelledError:
            activation.close("drain")
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
                if info.durable_activity:
                    # the mark doubles as the activation fence: committing it advances
                    # the key's HLC chain, so a superseded activation's later commit or
                    # clear arrives on a stale basis and is refused
                    activation.hlc = await replication.commit(
                        info,
                        activation.key,
                        activation.hlc,
                        [Page(key=ACTIVE_PAGE, data=ACTIVE_DATA)],
                        [],
                    )
                    activation.state.pages[ACTIVE_PAGE] = ACTIVE_DATA
            if info.activate_hook is not None:
                await self._run_hook(activation, info.activate_hook)
            return True
        except Exception as exc:
            stale = isinstance(exc, StaleActivationError)
            activation.close("stale" if stale else "activation-failed")
            activation.run_deactivate_hook = False
            error: CastyError
            if stale:
                error = RangeMovingError(
                    f"{activation.identity}: stale activation discarded ({exc}); retry"
                )
            elif isinstance(exc, CastyError):
                error = exc
            else:
                error = ActorFailedError(
                    info.wire_name,
                    activation.key,
                    f"activation failed: {type(exc).__name__}: {exc}",
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
            activation,
            info.cls,
            activation.key,
            chain=[*item.chain, activation.identity],
            reply=reply,
        )
        token = set_context(ctx)
        started = 0.0
        if self._observed():
            started = asyncio.get_running_loop().time()
            self._spy_received(activation, item.method_name, False, item.chain, item.args)
        try:
            method = getattr(activation.instance, item.method_name)
            result = await method(*item.args, **item.kwargs)
        except asyncio.CancelledError:
            if not item.future.done():
                item.future.set_exception(
                    ActorUnavailableError(f"{activation.identity}: node stopping")
                )
            raise
        except Exception as exc:
            activation.failures += 1
            directive = self._decide(info, activation, exc)
            self._spy_failed(activation, item.method_name, exc, directive)
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
                activation.close("reset")
                activation.run_deactivate_hook = False
            elif directive is Directive.STOP:
                activation.close("stop")
        else:
            activation.failures = 0
            commit_error, changed = await self._commit_if_mutated(activation)
            self._spy_completed(
                activation, item.method_name, False, started, changed, result, commit_error
            )
            if ctx.detached:
                pass  # the Reply resolves the caller out of band
            elif not item.future.done():
                if commit_error is None:
                    item.future.set_result(result)
                else:
                    item.future.set_exception(commit_error)
            if ctx.deactivation_requested:
                activation.close("deactivate")
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
            self, activation, info.cls, activation.key, chain=[*item.chain, activation.identity]
        )
        token = set_context(ctx)
        started = 0.0
        if self._observed():
            started = asyncio.get_running_loop().time()
            self._spy_received(activation, item.method_name, True, item.chain, item.unary_args)
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
            self._spy_failed(activation, item.method_name, exc, directive)
            error = ActorFailedError(
                info.wire_name, activation.key, f"{type(exc).__name__}: {exc}"
            )
            with contextlib.suppress(Exception):
                await item.sink.fail(error)
            if directive is Directive.RESET:
                activation.close("reset")
                activation.run_deactivate_hook = False
            elif directive is Directive.STOP:
                activation.close("stop")
        else:
            activation.failures = 0
            commit_error, changed = await self._commit_if_mutated(activation)
            self._spy_completed(
                activation, item.method_name, True, started, changed, None, commit_error
            )
            with contextlib.suppress(Exception):
                if commit_error is None:
                    await item.sink.close()
                else:
                    await item.sink.fail(commit_error)
            if ctx.deactivation_requested:
                activation.close("deactivate")
        finally:
            reset_context(token)

    async def _commit_if_mutated(
        self, activation: _Activation
    ) -> tuple[CastyError | None, list[str] | None]:
        """Replicate the pages this handler changed. On failure the in-memory state
        rolls back to the last committed one — a fenced minority owner never
        advances. Also returns the committed page keys (None when the actor runs
        single-copy), so a spy `Completed` has `changed` for free (spec 11 §3)."""
        info = activation.info
        replication = self._replication_for(info)
        if replication is None:
            return None, None
        values = info.state_of(activation.instance)
        if info.immutable_state and values == activation.last_values:
            return None, []
        try:
            delta = paging.diff(
                info, activation.instance, values, activation.last_values, activation.state
            )
        except SerializationError as exc:
            # the encode itself failed, part-way through: no field is trustworthy
            paging.reset(info, activation.state, activation.instance)
            activation.last_values = None
            return exc, None
        if delta.empty:
            paging.commit(activation.state, delta)
            activation.last_values = values
            return None, []
        try:
            activation.hlc = await replication.commit(
                info, activation.key, activation.hlc, delta.pages, delta.dropped
            )
        except (QuorumUnavailableError, SerializationError) as exc:
            # nothing was committed anywhere: undo the pages this handler touched
            paging.rollback(info, activation.instance, activation.state, delta)
            activation.last_values = None
            return exc, None
        except StaleActivationError as exc:
            # another owner committed newer history; this activation is stale
            activation.close("stale")
            activation.run_deactivate_hook = False
            return RangeMovingError(
                f"{activation.identity}: stale activation discarded ({exc}); retry"
            ), None
        paging.commit(activation.state, delta)
        activation.last_values = values
        return None, [page.key for page in delta.pages] + list(delta.dropped)

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
            self, activation, activation.info.cls, activation.key, chain=[activation.identity]
        )
        token = set_context(ctx)
        try:
            await getattr(activation.instance, hook_name)()
        finally:
            reset_context(token)

    async def _finalize(self, activation: _Activation) -> None:
        self._spy_deactivated(activation)
        for schedule_task in list(activation.schedules.values()):
            schedule_task.cancel()
        try:
            if activation.run_deactivate_hook and activation.info.deactivate_hook is not None:
                try:
                    await self._run_hook(activation, activation.info.deactivate_hook)
                except Exception:
                    logger.exception("%s: deactivate hook raised", activation.identity)
            await self._settle_mark(activation)
        finally:
            current = self._activations.get((activation.info.wire_name, activation.key))
            if current is activation:
                del self._activations[(activation.info.wire_name, activation.key)]
            activation.done.set()
            self._requeue_leftovers(activation)

    async def _settle_mark(self, activation: _Activation) -> None:
        """A deliberate end clears the durable mark. `deactivate` gates on the
        clear landing — a minority owner cannot deliberately die, since the
        majority side would resurrect it. `stop` tries once, then retries in
        the background: the supervisor demanded the end. Every other reason
        leaves the mark, and the actor is resurrected elsewhere."""
        info = activation.info
        replication = self._replication_for(info)
        if replication is None or not info.durable_activity:
            return
        match activation.close_reason:
            case "deactivate":
                while True:
                    try:
                        await replication.commit(
                            info, activation.key, activation.hlc, [], [ACTIVE_PAGE]
                        )
                    except QuorumUnavailableError:
                        await asyncio.sleep(self._reactivation_retry)
                    except StaleActivationError:
                        return  # another activation advanced the chain: not ours to clear
                    else:
                        return
            case "stop":
                try:
                    await replication.commit(
                        info, activation.key, activation.hlc, [], [ACTIVE_PAGE]
                    )
                except QuorumUnavailableError:
                    self.pending_clears.add((info.wire_name, activation.key))
                    self._spawn_clear(replication, info, activation.key, activation.hlc)
                except StaleActivationError:
                    pass
            case _:
                return

    def _spawn_clear(
        self, replication: Replication, info: ActorInfo, key: str, hlc: Hlc | None
    ) -> None:
        async def retry() -> None:
            while True:
                await asyncio.sleep(self._reactivation_retry)
                try:
                    await replication.commit(info, key, hlc, [], [ACTIVE_PAGE])
                except QuorumUnavailableError:
                    continue
                except StaleActivationError:
                    pass
                self.pending_clears.discard((info.wire_name, key))
                return

        task = asyncio.create_task(retry())
        self._clear_tasks.add(task)
        task.add_done_callback(self._clear_tasks.discard)

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
            if isinstance(item, _CallItem) and item.tick:
                if not item.future.done():  # never re-dispatched: a tick died with its activation
                    item.future.set_result(None)
                continue
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
