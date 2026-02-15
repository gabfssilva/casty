"""Actor system entry point for spawning and managing top-level actors.

Provides ``ActorSystem``, the main runtime container that owns root actors,
handles request-reply (``ask``), path-based lookup, scheduling, and
graceful shutdown.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any, cast, TYPE_CHECKING

from casty.core.actor import ActorCell
from casty.core.behavior import Behavior
from casty.core.event_stream import EventStreamMsg, event_stream_actor
from casty.core.mailbox import Mailbox
from casty.core.ref import ActorRef, LocalActorRef
from casty.core.task_runner import TaskRunnerMsg, task_runner as task_runner_behavior

if TYPE_CHECKING:
    from casty.config import CastyConfig
    from casty.core.context import System
    from casty.core.scheduler import SchedulerMsg


class ActorSystem:
    """Main entry point for creating and managing actors.

    Use as an async context manager for automatic shutdown.
    """

    def __init__(
        self, name: str = "casty-system", *, config: CastyConfig | None = None
    ) -> None:
        self._name = name
        self._config = config
        self._root_cells: dict[str, ActorCell[Any]] = {}
        self._event_stream_ref: ActorRef[EventStreamMsg] | None = None
        self._task_runner_ref: ActorRef[TaskRunnerMsg] | None = None
        self._scheduler_ref: ActorRef[SchedulerMsg] | None = None
        self._logger = logging.getLogger(f"casty.system.{name}")

    def __make_ref__[M](self, id: str, deliver: Callable[[Any], None]) -> ActorRef[M]:
        return LocalActorRef(id=id, _deliver=deliver)

    @property
    def name(self) -> str:
        return self._name

    @property
    def event_stream(self) -> ActorRef[EventStreamMsg]:
        return self._ensure_event_stream()

    @property
    def scheduler(self) -> ActorRef[SchedulerMsg]:
        if self._scheduler_ref is None:
            from casty.core.scheduler import scheduler as scheduler_behavior

            self._scheduler_ref = self.spawn(scheduler_behavior(), "_scheduler")
        return self._scheduler_ref

    async def __aenter__(self) -> ActorSystem:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.shutdown()

    def _ensure_event_stream(self) -> ActorRef[EventStreamMsg]:
        if self._event_stream_ref is not None:
            return self._event_stream_ref

        cell: ActorCell[EventStreamMsg] = ActorCell(
            behavior=event_stream_actor(),
            id="_event_stream",
            system=cast("System", self),
        )
        self._root_cells["_event_stream"] = cell
        asyncio.get_running_loop().create_task(cell.start())
        self._event_stream_ref = cell.ref
        return cell.ref

    def _ensure_task_runner(self) -> ActorRef[TaskRunnerMsg]:
        if self._task_runner_ref is not None:
            return self._task_runner_ref

        es_ref = self._ensure_event_stream()

        cell: ActorCell[TaskRunnerMsg] = ActorCell(
            behavior=task_runner_behavior(),
            id="_task_runner",
            event_stream=es_ref,
            system=cast("System", self),
        )
        self._root_cells["_task_runner"] = cell
        asyncio.get_running_loop().create_task(cell.start())
        self._task_runner_ref = cell.ref
        return cell.ref

    def spawn[M](
        self,
        behavior: Behavior[M],
        name: str,
        *,
        mailbox: Mailbox[M] | None = None,
    ) -> ActorRef[M]:
        """Spawn a root-level actor in this system."""
        if name in self._root_cells:
            raise ValueError(f"Root actor '{name}' already exists")

        if mailbox is None and self._config is not None and not name.startswith("_"):
            from casty.core.mailbox import MailboxOverflowStrategy

            resolved = self._config.resolve_actor(name)
            mailbox = Mailbox(
                capacity=resolved.mailbox.capacity,
                overflow=MailboxOverflowStrategy[resolved.mailbox.strategy],
            )

        es_ref = self._ensure_event_stream()
        tr_ref = self._ensure_task_runner()

        cell: ActorCell[M] = ActorCell(
            behavior=behavior,
            id=name,
            event_stream=es_ref,
            task_runner=tr_ref,
            system=cast("System", self),
        )
        if mailbox is not None:
            cell.mailbox = mailbox
        self._root_cells[name] = cell
        asyncio.get_running_loop().create_task(cell.start())
        self._logger.info("Spawning root actor: %s", name)
        return cell.ref

    async def ask[M, R](
        self,
        ref: ActorRef[M],
        msg_factory: Callable[[ActorRef[R]], M],
        *,
        timeout: float,
    ) -> R:
        """Send a message and wait for a reply (request-reply pattern)."""
        future: asyncio.Future[R] = asyncio.get_running_loop().create_future()

        def on_reply(msg: Any) -> None:
            if not future.done():
                future.set_result(msg)

        temp_ref: ActorRef[R] = self.__make_ref__(
            f"_ask/{id(future)}",
            on_reply,
        )
        ref.tell(msg_factory(temp_ref))
        return await asyncio.wait_for(future, timeout=timeout)

    async def ask_or_none[M, R](
        self,
        ref: ActorRef[M],
        msg_factory: Callable[[ActorRef[R]], M],
        *,
        timeout: float,
    ) -> R | None:
        """Like ``ask()``, but returns ``None`` on timeout instead of raising."""
        try:
            return await self.ask(ref, msg_factory, timeout=timeout)
        except asyncio.TimeoutError:
            return None

    def lookup(self, path: str) -> ActorRef[Any] | None:
        """Look up an actor by its path in the actor tree."""
        parts = path.strip("/").split("/")
        if not parts:
            return None

        cell = self._root_cells.get(parts[0])
        if cell is None:
            return None

        for part in parts[1:]:
            child = cell.children.get(part)
            if child is None:
                return None
            cell = child

        return cell.ref

    async def shutdown(self) -> None:
        """Shut down the actor system, stopping all root actors."""
        self._logger.info("Shutting down (%d root actors)", len(self._root_cells))
        internal = {"_task_runner", "_event_stream", "_scheduler"}
        for name, cell in list(self._root_cells.items()):
            if name not in internal:
                await cell.stop()
        for name in ("_scheduler", "_task_runner", "_event_stream"):
            if name in self._root_cells:
                await self._root_cells[name].stop()
        self._root_cells.clear()
