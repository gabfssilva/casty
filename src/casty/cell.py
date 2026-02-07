from __future__ import annotations

import asyncio
import logging
import time as _time
from collections.abc import Awaitable, Callable
from typing import Any, cast

from casty.actor import (
    Behavior,
    EventSourcedBehavior,
    LifecycleBehavior,
    PersistedBehavior,
    ReceiveBehavior,
    RestartBehavior,
    SameBehavior,
    SetupBehavior,
    StoppedBehavior,
    SupervisedBehavior,
    UnhandledBehavior,
)
from casty.journal import PersistedEvent, Snapshot
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
from casty.transport import LocalTransport, MessageTransport


class CellContext[M]:
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
        behavior: Behavior[C],
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
            ref_transport=self._cell.ref_transport,
            ref_host=self._cell.ref_host,
            ref_port=self._cell.ref_port,
        )
        if mailbox is not None:
            child.mailbox = mailbox
        self._cell.children[name] = child
        asyncio.get_running_loop().create_task(child.start())
        return child.ref

    def stop(self, ref: ActorRef[Any]) -> None:
        for _name, child in self._cell.children.items():
            if child.ref == ref:
                asyncio.get_running_loop().create_task(child.stop())
                return

    def watch(self, ref: ActorRef[Any]) -> None:
        # Search children and siblings for the cell matching this ref
        for child in self._cell.children.values():
            if child.ref == ref:
                child.watchers.add(self._cell)
                return
        # Search parent's children (siblings)
        if self._cell.parent is not None:
            for sibling in self._cell.parent.children.values():
                if sibling.ref == ref:
                    sibling.watchers.add(self._cell)
                    return

    def unwatch(self, ref: ActorRef[Any]) -> None:
        for child in self._cell.children.values():
            if child.ref == ref:
                child.watchers.discard(self._cell)
                return
        if self._cell.parent is not None:
            for sibling in self._cell.parent.children.values():
                if sibling.ref == ref:
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
        ref_transport: MessageTransport | None = None,
        ref_host: str | None = None,
        ref_port: int | None = None,
    ) -> None:
        self._initial_behavior: Behavior[M] = behavior
        self._name = name
        self._parent = parent
        self._event_stream = event_stream
        self._system_name = system_name
        self._local_transport = local_transport
        self._ref_transport = ref_transport
        self._ref_host = ref_host
        self._ref_port = ref_port
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

        # Event sourcing state
        self._es_entity_id: str | None = None
        self._es_journal: Any = None
        self._es_on_event: Any = None
        self._es_on_command: Any = None
        self._es_state: Any = None
        self._es_sequence_nr: int = 0
        self._es_snapshot_policy: Any = None
        self._es_events_since_snapshot: int = 0
        self._es_replica_refs: tuple[Any, ...] = ()
        self._es_replication: Any = None
        self._ack_queue: asyncio.Queue[Any] = asyncio.Queue()

        # Context
        self._ctx: CellContext[M] = CellContext(self)

        # Create the ref with address + transport
        effective_transport: MessageTransport = ref_transport or local_transport
        addr = ActorAddress(
            system=system_name, path=f"/{name}", host=ref_host, port=ref_port
        )
        self._ref: ActorRef[M] = ActorRef(address=addr, _transport=effective_transport)
        # Always register locally for TCP inbound delivery
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
    def ref_transport(self) -> MessageTransport | None:
        return self._ref_transport

    @property
    def ref_host(self) -> str | None:
        return self._ref_host

    @property
    def ref_port(self) -> int | None:
        return self._ref_port

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
        from casty.replication import ReplicateEventsAck

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
        # Route ack messages to the dedicated queue to avoid polluting the main mailbox
        if isinstance(msg, ReplicateEventsAck):
            self._ack_queue.put_nowait(msg)
            return
        self._mailbox.put(msg)

    async def start(self) -> None:
        """Initialize the behavior and start the message loop."""
        await self._initialize_behavior(self._initial_behavior)
        if self._es_entity_id is not None:
            await self._recover_event_sourced()
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

            case EventSourcedBehavior() as b:
                self._es_entity_id = b.entity_id
                self._es_journal = b.journal
                self._es_on_event = b.on_event
                self._es_on_command = b.on_command
                self._es_snapshot_policy = b.snapshot_policy
                self._es_state = b.initial_state
                self._es_sequence_nr = 0
                self._es_events_since_snapshot = 0
                self._es_replica_refs = b.replica_refs if b.replica_refs else ()
                self._es_replication = b.replication
                # Set handler so loop knows to run (it checks _current_handler is not None)
                self._current_handler = b.on_command  # type: ignore[assignment]
                if self._pre_start is not None:
                    await self._pre_start(self._ctx)

            case _:
                msg = f"Cannot initialize with behavior type: {type(behavior)}"
                raise TypeError(msg)

    async def _recover_event_sourced(self) -> None:
        """Replay events from journal to reconstruct state."""
        if self._es_journal is None or self._es_entity_id is None:
            return

        # Load snapshot
        snapshot = await self._es_journal.load_snapshot(self._es_entity_id)
        if snapshot is not None:
            self._es_state = snapshot.state
            self._es_sequence_nr = snapshot.sequence_nr

        # Load and replay events after snapshot
        events = await self._es_journal.load(
            self._es_entity_id, from_sequence_nr=self._es_sequence_nr + 1
        )
        for persisted in events:
            self._es_state = self._es_on_event(self._es_state, persisted.event)
            self._es_sequence_nr = persisted.sequence_nr

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
                    if self._es_on_command is not None:
                        next_behavior = await self._es_on_command(
                            self._ctx, self._es_state, msg
                        )
                    else:
                        next_behavior = await self._current_handler(self._ctx, msg)
                except Exception as exc:
                    await self._handle_failure(exc)
                    continue

                # Handle PersistedBehavior for event-sourced actors
                match next_behavior:
                    case PersistedBehavior():
                        await self._handle_persisted(
                            cast(PersistedBehavior[M, Any], next_behavior)
                        )
                    case _:
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

    async def _handle_persisted(
        self, persisted_behavior: PersistedBehavior[M, Any]
    ) -> None:
        """Persist events, apply to state, check snapshot policy."""
        from casty.actor import SnapshotEvery

        if self._es_journal is None or self._es_entity_id is None:
            return

        # Wrap raw events as PersistedEvent
        wrapped: list[PersistedEvent[Any]] = []
        for event in persisted_behavior.events:
            self._es_sequence_nr += 1
            wrapped.append(
                PersistedEvent(
                    sequence_nr=self._es_sequence_nr,
                    event=event,
                    timestamp=_time.time(),
                )
            )

        # Persist to journal
        await self._es_journal.persist(self._es_entity_id, wrapped)

        # Apply events to state
        for persisted_event in wrapped:
            self._es_state = self._es_on_event(self._es_state, persisted_event.event)

        # Snapshot policy
        self._es_events_since_snapshot += len(wrapped)
        match self._es_snapshot_policy:
            case SnapshotEvery(n_events=n_events):
                if self._es_events_since_snapshot >= n_events:
                    snapshot = Snapshot(
                        sequence_nr=self._es_sequence_nr,
                        state=self._es_state,
                        timestamp=_time.time(),
                    )
                    await self._es_journal.save_snapshot(self._es_entity_id, snapshot)
                    self._es_events_since_snapshot = 0
            case _:
                pass

        # Push to replicas
        if self._es_replica_refs:
            from casty.replication import ReplicateEvents
            replication_msg = ReplicateEvents(
                entity_id=self._es_entity_id,
                shard_id=0,
                events=tuple(wrapped),
                reply_to=self._ref if (self._es_replication is not None and hasattr(self._es_replication, 'min_acks') and self._es_replication.min_acks > 0) else None,
            )
            for replica_ref in self._es_replica_refs:
                replica_ref.tell(replication_msg)

        # Wait for acks if required
        if self._es_replica_refs:
            from casty.replication import ReplicationConfig
            match self._es_replication:
                case ReplicationConfig(min_acks=min_acks, ack_timeout=ack_timeout) if min_acks > 0:
                    acks_needed = min(min_acks, len(self._es_replica_refs))
                    ack_count = 0
                    deadline = _time.monotonic() + ack_timeout

                    while ack_count < acks_needed and _time.monotonic() < deadline:
                        try:
                            remaining_time = max(0.001, deadline - _time.monotonic())
                            await asyncio.wait_for(
                                self._ack_queue.get(),
                                timeout=remaining_time,
                            )
                            ack_count += 1
                        except asyncio.TimeoutError:
                            self._logger.warning(
                                "Replication ack timeout for %s: got %d/%d acks",
                                self._es_entity_id, ack_count, acks_needed,
                            )
                            break
                case _:
                    pass

        # Apply the "then" behavior
        await self._apply_behavior(persisted_behavior.then)

    async def _handle_failure(self, exception: Exception) -> None:
        """Handle an exception from the message handler using supervision."""
        self._logger.error("Actor %s failed: %s", self._name, exception, exc_info=True)

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
