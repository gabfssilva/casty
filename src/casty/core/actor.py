"""Minimal ActorCell built on the Behavior primitive.

Owns the mailbox, runs the message loop, manages children. All cross-cutting
concerns (supervision, lifecycle, spy) are composed into the Behavior before
the cell ever sees it.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, cast, overload, TYPE_CHECKING

from casty.core.behavior import Behavior, Signal
from casty.core.events import ActorRestarted, ActorStarted, ActorStopped, DeadLetter
from casty.core.event_stream import EventStreamMsg, Publish
from casty.core.mailbox import Mailbox
from casty.core.messages import Terminated
from casty.core.ref import ActorId, ActorRef, LocalActorRef
from casty.core.task_runner import RunTask, TaskRunnerMsg

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from casty.core.context import System


class CellContext[M]:
    """Concrete ActorContext implementation backed by an ActorCell."""

    def __init__(self, cell: ActorCell[M]) -> None:
        self._cell = cell

    @property
    def self(self) -> ActorRef[M]:
        return self._cell.ref

    @property
    def system(self) -> System:
        if self._cell.system is None:
            msg = "No system available in this context"
            raise RuntimeError(msg)
        return self._cell.system

    @property
    def log(self) -> logging.Logger:
        return self._cell.logger

    def spawn[C](
        self,
        behavior: Behavior[C],
        name: str,
        *,
        mailbox: Mailbox[C] | None = None,
    ) -> ActorRef[C]:
        child_id = f"{self._cell.id}/{name}"
        child: ActorCell[C] = ActorCell(
            behavior=behavior,
            id=child_id,
            parent=self._cell,
            event_stream=self._cell.event_stream,
            task_runner=self._cell.task_runner,
            system=self._cell.system,
        )
        if mailbox is not None:
            child.mailbox = mailbox
        self._cell.children[name] = child
        asyncio.get_running_loop().create_task(child.start())
        return child.ref

    def stop(self, ref: ActorRef[Any]) -> None:
        for child in self._cell.children.values():
            if child.ref.id == ref.id:
                asyncio.get_running_loop().create_task(child.stop())
                return

    def watch(self, ref: ActorRef[Any]) -> None:
        for child in self._cell.children.values():
            if child.ref.id == ref.id:
                child.watchers.add(self._cell)
                return
        if self._cell.parent is not None:
            for sibling in self._cell.parent.children.values():
                if sibling.ref.id == ref.id:
                    sibling.watchers.add(self._cell)
                    return

    def unwatch(self, ref: ActorRef[Any]) -> None:
        for child in self._cell.children.values():
            if child.ref.id == ref.id:
                child.watchers.discard(self._cell)
                return
        if self._cell.parent is not None:
            for sibling in self._cell.parent.children.values():
                if sibling.ref.id == ref.id:
                    sibling.watchers.discard(self._cell)
                    return

    def register_interceptor(self, interceptor: Callable[[object], bool]) -> None:
        self._cell.add_interceptor(interceptor)

    @overload
    def pipe_to_self(
        self,
        coro: Awaitable[M],
        *,
        on_failure: Callable[[Exception], M] | None = None,
    ) -> None: ...

    @overload
    def pipe_to_self[T](
        self,
        coro: Awaitable[T],
        mapper: Callable[[T], M],
        on_failure: Callable[[Exception], M] | None = None,
    ) -> None: ...

    def pipe_to_self[T](
        self,
        coro: Awaitable[T],
        mapper: Callable[[T], M] | None = None,
        on_failure: Callable[[Exception], M] | None = None,
    ) -> None:
        ref = self._cell.ref

        async def run() -> None:
            try:
                result = await coro
                if mapper is not None:
                    ref.tell(mapper(result))
                else:
                    ref.tell(cast(M, result))
            except Exception as exc:
                if on_failure is not None:
                    ref.tell(on_failure(exc))
                else:
                    self._cell.logger.warning(
                        "pipe_to_self failed (no on_failure handler): %s",
                        exc,
                    )

        tr = self._cell.task_runner
        if tr is not None:
            tr.tell(RunTask(run))
        else:
            asyncio.get_running_loop().create_task(run())


class ActorCell[M]:
    """Minimal runtime engine for an actor.

    Owns the mailbox, runs the message loop, manages children.
    Does NOT know about supervision, lifecycle, spy, or discovery —
    those are composed into the Behavior before the cell sees it.
    """

    def __init__(
        self,
        behavior: Behavior[M],
        id: ActorId,
        parent: ActorCell[Any] | None = None,
        event_stream: ActorRef[EventStreamMsg] | None = None,
        task_runner: ActorRef[TaskRunnerMsg] | None = None,
        system: System | None = None,
    ) -> None:
        self._initial_behavior = behavior
        self._id = id
        self._parent = parent
        self._event_stream = event_stream
        self._task_runner = task_runner
        self._system = system
        self._mailbox: Mailbox[Any] = Mailbox()
        self._logger = logging.getLogger(f"casty.actor.{id}")

        self._stopped = False
        self._children: dict[str, ActorCell[Any]] = {}
        self._watchers: set[ActorCell[Any]] = set()
        self._current_handler: Callable[..., Awaitable[Behavior[M]]] | None = None
        self._loop_task: asyncio.Task[None] | None = None
        self._interceptors: list[Callable[[object], bool]] = []

        self._ctx: CellContext[M] = CellContext(self)
        if system is not None:
            self._ref: ActorRef[M] = system.__make_ref__(id, self._deliver)
        else:
            self._ref = LocalActorRef(id=id, _deliver=self._deliver)

    @property
    def id(self) -> ActorId:
        return self._id

    @property
    def ref(self) -> ActorRef[M]:
        return self._ref

    @property
    def is_stopped(self) -> bool:
        return self._stopped

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @property
    def children(self) -> dict[str, ActorCell[Any]]:
        return self._children

    @property
    def watchers(self) -> set[ActorCell[Any]]:
        return self._watchers

    @property
    def parent(self) -> ActorCell[Any] | None:
        return self._parent

    @property
    def event_stream(self) -> ActorRef[EventStreamMsg] | None:
        return self._event_stream

    @property
    def task_runner(self) -> ActorRef[TaskRunnerMsg] | None:
        return self._task_runner

    @property
    def system(self) -> System | None:
        return self._system

    @property
    def mailbox(self) -> Mailbox[Any]:
        return self._mailbox

    @mailbox.setter
    def mailbox(self, value: Mailbox[Any]) -> None:
        self._mailbox = value

    def add_interceptor(self, interceptor: Callable[[object], bool]) -> None:
        self._interceptors.append(interceptor)

    def _publish(self, event: object) -> None:
        if self._event_stream is not None:
            self._event_stream.tell(Publish(event))

    def _deliver(self, msg: M) -> None:
        if self._stopped:
            match msg:
                case DeadLetter():
                    pass
                case _:
                    self._publish(DeadLetter(message=msg, intended_ref=self._ref))
            return
        self._mailbox.put(msg)

    async def start(self) -> None:
        await self._initialize(self._initial_behavior)
        self._loop_task = asyncio.get_running_loop().create_task(self._run_loop())
        self._publish(ActorStarted(ref=self._ref))
        self._logger.info("Started")

    async def _initialize(self, behavior: Behavior[M]) -> None:
        match (behavior.on_setup, behavior.on_receive, behavior.signal):
            case (factory, None, None) if factory is not None:
                result = await factory(self._ctx)
                await self._initialize(result)
            case (None, handler, None) if handler is not None:
                self._current_handler = handler
            case _:
                msg = f"Cannot initialize with behavior: {behavior}"
                raise TypeError(msg)

    async def _run_loop(self) -> None:
        while not self._stopped:
            try:
                msg = await self._mailbox.get()
                if self._stopped:
                    break

                intercepted = False
                for interceptor in self._interceptors:
                    if interceptor(msg):
                        intercepted = True
                        break
                if intercepted:
                    continue

                if self._current_handler is None:
                    continue

                try:
                    next_behavior = await self._current_handler(self._ctx, msg)
                except Exception:
                    self._logger.exception("Actor %s failed", self._id)
                    await self._do_stop()
                    break

                await self._apply(next_behavior)

            except asyncio.CancelledError:
                break
            except Exception:
                self._logger.exception("Unexpected error in message loop")
                break

    async def _apply(self, behavior: Behavior[M]) -> None:
        match behavior.signal:
            case Signal.same:
                pass
            case Signal.stopped:
                await self._do_stop()
            case Signal.restart:
                await self._do_restart()
            case Signal.unhandled:
                pass
            case None:
                if behavior.on_receive is not None:
                    self._current_handler = behavior.on_receive
                elif behavior.on_setup is not None:
                    await self._initialize(behavior)

    async def _do_restart(self) -> None:
        self._logger.info("Restarting")
        await self._initialize(self._initial_behavior)
        self._publish(ActorRestarted(ref=self._ref, exception=RuntimeError("restart")))

    async def _do_stop(self) -> None:
        if self._stopped:
            return

        self._stopped = True
        self._logger.info("Stopping")

        for child in list(self._children.values()):
            try:
                await child.stop()
            except Exception:
                self._logger.exception("Error stopping child %s", child.id)

        for watcher in self._watchers:
            try:
                watcher._deliver(Terminated(ref=self._ref))
            except Exception:
                self._logger.exception("Error notifying watcher")

        self._publish(ActorStopped(ref=self._ref))

    async def stop(self) -> None:
        if self._stopped:
            return

        await self._do_stop()

        if self._loop_task is not None and not self._loop_task.done():
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
