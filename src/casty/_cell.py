from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from casty.actor import (
    Behavior,
    LifecycleBehavior,
    ReceiveBehavior,
    RestartBehavior,
    SameBehavior,
    SetupBehavior,
    StoppedBehavior,
    SupervisedBehavior,
    UnhandledBehavior,
)
from casty.events import (
    ActorRestarted,
    ActorStarted,
    ActorStopped,
    DeadLetter,
    EventStream,
)
from casty.address import ActorAddress
from casty.mailbox import Mailbox
from casty.messages import Terminated
from casty.ref import ActorRef
from casty.supervision import Directive, SupervisionStrategy
from casty.transport import LocalTransport


class _CellContext[M]:
    """Concrete implementation of the ActorContext protocol for a cell."""

    def __init__(self, cell: ActorCell[M]) -> None:
        self._cell = cell

    @property
    def self(self) -> ActorRef[M]:
        return self._cell.ref

    @property
    def log(self) -> logging.Logger:
        return self._cell.logger

    def spawn[C](
        self,
        behavior: Any,
        name: str,
        *,
        mailbox: Mailbox[C] | None = None,
    ) -> ActorRef[C]:
        child: ActorCell[C] = ActorCell(
            behavior=behavior,
            name=f"{self._cell.cell_name}/{name}",
            parent=self._cell,
            event_stream=self._cell.event_stream,
            system_name=self._cell.system_name,
            local_transport=self._cell.local_transport,
        )
        if mailbox is not None:
            child.mailbox = mailbox
        self._cell.children[name] = child
        asyncio.get_running_loop().create_task(child.start())
        return child.ref

    def stop(self, ref: ActorRef[Any]) -> None:
        for _name, child in self._cell.children.items():
            if child.ref is ref:
                asyncio.get_running_loop().create_task(child.stop())
                return

    def watch(self, ref: ActorRef[Any]) -> None:
        # Search children and siblings for the cell matching this ref
        for child in self._cell.children.values():
            if child.ref is ref:
                child.watchers.add(self._cell)
                return
        # Search parent's children (siblings)
        if self._cell.parent is not None:
            for sibling in self._cell.parent.children.values():
                if sibling.ref is ref:
                    sibling.watchers.add(self._cell)
                    return

    def unwatch(self, ref: ActorRef[Any]) -> None:
        for child in self._cell.children.values():
            if child.ref is ref:
                child.watchers.discard(self._cell)
                return
        if self._cell.parent is not None:
            for sibling in self._cell.parent.children.values():
                if sibling.ref is ref:
                    sibling.watchers.discard(self._cell)
                    return


class ActorCell[M]:
    """Internal runtime engine for an actor.

    Owns the mailbox, runs the message loop as an asyncio task,
    unwraps behavior layers, manages children, handles failures
    via supervision, and fires lifecycle hooks.
    """

    def __init__(
        self,
        behavior: Behavior[M],
        name: str,
        parent: ActorCell[Any] | None,
        event_stream: EventStream,
        system_name: str,
        local_transport: LocalTransport,
    ) -> None:
        self._initial_behavior: Behavior[M] = behavior
        self._name = name
        self._parent = parent
        self._event_stream = event_stream
        self._system_name = system_name
        self._local_transport = local_transport
        self._mailbox: Mailbox[Any] = Mailbox()
        self._logger = logging.getLogger(f"casty.actor.{name}")

        # State
        self._stopped = False
        self._children: dict[str, ActorCell[Any]] = {}
        self._watchers: set[ActorCell[Any]] = set()
        self._current_handler: Callable[..., Awaitable[Behavior[M]]] | None = None
        self._strategy: SupervisionStrategy | None = None
        self._loop_task: asyncio.Task[None] | None = None

        # Lifecycle hooks
        self._pre_start: Callable[..., Awaitable[None]] | None = None
        self._post_stop: Callable[..., Awaitable[None]] | None = None
        self._pre_restart: Callable[..., Awaitable[None]] | None = None
        self._post_restart: Callable[..., Awaitable[None]] | None = None

        # Context
        self._ctx: _CellContext[M] = _CellContext(self)

        # Create the ref with address + transport
        addr = ActorAddress(system=system_name, path=f"/{name}")
        self._ref: ActorRef[M] = ActorRef(address=addr, _transport=local_transport)
        local_transport.register(f"/{name}", self._deliver)

    @property
    def ref(self) -> ActorRef[M]:
        return self._ref

    @property
    def is_stopped(self) -> bool:
        return self._stopped

    @property
    def cell_name(self) -> str:
        return self._name

    @property
    def event_stream(self) -> EventStream:
        return self._event_stream

    @property
    def system_name(self) -> str:
        return self._system_name

    @property
    def local_transport(self) -> LocalTransport:
        return self._local_transport

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
    def mailbox(self) -> Mailbox[Any]:
        return self._mailbox

    @mailbox.setter
    def mailbox(self, value: Mailbox[Any]) -> None:
        self._mailbox = value

    def _deliver(self, msg: M) -> None:
        """Delivery callback for ActorRef. Puts into mailbox or publishes DeadLetter."""
        if self._stopped:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(
                    self._event_stream.publish(
                        DeadLetter(message=msg, intended_ref=self._ref)
                    )
                )
            except RuntimeError:
                # No running event loop — silently drop
                pass
            return
        self._mailbox.put(msg)

    async def start(self) -> None:
        """Initialize the behavior and start the message loop."""
        await self._initialize_behavior(self._initial_behavior)
        self._loop_task = asyncio.get_running_loop().create_task(self._run_loop())
        await self._event_stream.publish(ActorStarted(ref=self._ref))

    async def _initialize_behavior(self, behavior: Behavior[M]) -> None:
        """Recursively unwrap behavior layers and set up the cell."""
        # Reset hooks and strategy for fresh initialization
        self._pre_start = None
        self._post_stop = None
        self._pre_restart = None
        self._post_restart = None
        self._strategy = None
        await self._unwrap_behavior(behavior)

    async def _unwrap_behavior(self, behavior: Behavior[M]) -> None:
        """Recursively process behavior layers."""
        match behavior:
            case SupervisedBehavior(inner_behavior, strategy):
                self._strategy = strategy
                await self._unwrap_behavior(inner_behavior)

            case LifecycleBehavior(
                inner_behavior, pre_start, post_stop, pre_restart, post_restart
            ):
                if pre_start is not None:
                    self._pre_start = pre_start
                if post_stop is not None:
                    self._post_stop = post_stop
                if pre_restart is not None:
                    self._pre_restart = pre_restart
                if post_restart is not None:
                    self._post_restart = post_restart
                await self._unwrap_behavior(inner_behavior)

            case SetupBehavior(factory):
                result = await factory(self._ctx)
                await self._unwrap_behavior(result)

            case ReceiveBehavior(handler):
                self._current_handler = handler
                if self._pre_start is not None:
                    await self._pre_start(self._ctx)

            case _:
                msg = f"Cannot initialize with behavior type: {type(behavior)}"
                raise TypeError(msg)

    async def _run_loop(self) -> None:
        """Main message processing loop."""
        while not self._stopped:
            try:
                msg = await self._mailbox.get()
                if self._stopped:
                    break

                if self._current_handler is None:
                    continue

                try:
                    next_behavior = await self._current_handler(self._ctx, msg)
                except Exception as exc:
                    await self._handle_failure(exc)
                    continue

                await self._apply_behavior(next_behavior)

            except asyncio.CancelledError:
                break
            except Exception:
                self._logger.exception("Unexpected error in message loop")
                break

    async def _apply_behavior(self, behavior: Behavior[M]) -> None:
        """Apply the result of a message handler."""
        match behavior:
            case SameBehavior():
                pass  # Keep current handler

            case StoppedBehavior():
                await self._do_stop()

            case ReceiveBehavior(handler):
                self._current_handler = handler

            case UnhandledBehavior():
                pass  # Could publish UnhandledMessage event

            case RestartBehavior():
                await self._do_restart(RuntimeError("Explicit restart requested"))

            case SetupBehavior() | LifecycleBehavior() | SupervisedBehavior():
                # These wrapper behaviors shouldn't be returned from handlers
                msg = f"Unexpected behavior type from handler: {type(behavior)}"
                raise TypeError(msg)

            case _:
                pass

    async def _handle_failure(self, exception: Exception) -> None:
        """Handle an exception from the message handler using supervision."""
        self._logger.error(
            "Actor %s failed: %s", self._name, exception, exc_info=True
        )

        if self._strategy is None:
            # No strategy — stop the actor
            await self._do_stop()
            return

        directive = self._strategy.decide(exception, child_id=self._name)

        match directive:
            case Directive.restart:
                await self._do_restart(exception)
            case Directive.stop:
                await self._do_stop()
            case Directive.escalate:
                if self._parent is not None:
                    await self._parent._handle_failure(exception)
                else:
                    await self._do_stop()

    async def _do_restart(self, exception: Exception) -> None:
        """Restart the actor: call pre_restart, re-initialize, call post_restart."""
        if self._pre_restart is not None:
            try:
                await self._pre_restart(self._ctx, exception)
            except Exception:
                self._logger.exception("Error in pre_restart hook")

        # Store strategy before re-initialization (it will be reset)
        saved_strategy = self._strategy

        # Re-initialize from the initial behavior
        await self._initialize_behavior(self._initial_behavior)

        # Restore strategy if initial behavior has one; otherwise keep saved
        if self._strategy is None and saved_strategy is not None:
            self._strategy = saved_strategy

        if self._post_restart is not None:
            try:
                await self._post_restart(self._ctx)
            except Exception:
                self._logger.exception("Error in post_restart hook")

        await self._event_stream.publish(
            ActorRestarted(ref=self._ref, exception=exception)
        )

    async def _do_stop(self) -> None:
        """Stop the actor: run hooks, stop children, notify watchers."""
        if self._stopped:
            return

        self._stopped = True

        # Call post_stop hook
        if self._post_stop is not None:
            try:
                await self._post_stop(self._ctx)
            except Exception:
                self._logger.exception("Error in post_stop hook")

        # Stop all children
        for child in list(self._children.values()):
            try:
                await child.stop()
            except Exception:
                self._logger.exception("Error stopping child %s", child.cell_name)

        # Notify watchers with Terminated
        for watcher in self._watchers:
            try:
                watcher._deliver(Terminated(ref=self._ref))
            except Exception:
                self._logger.exception("Error notifying watcher")

        # Publish ActorStopped event
        await self._event_stream.publish(ActorStopped(ref=self._ref))

    async def stop(self) -> None:
        """Stop the actor externally."""
        if self._stopped:
            return

        await self._do_stop()

        # Cancel the message loop task
        if self._loop_task is not None and not self._loop_task.done():
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass

    def watch(self, other: ActorCell[Any]) -> None:
        """Register this cell as a watcher of another cell.

        When the other cell stops, this cell will receive a Terminated message.
        """
        other.watchers.add(self)
