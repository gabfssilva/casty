"""Actor system runtime for Casty - local execution."""

from __future__ import annotations

import asyncio
import copy
import dataclasses
import logging
from asyncio import Queue, Task
from contextlib import asynccontextmanager, AsyncExitStack
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import msgpack

from . import System
from .actor import Actor, ActorId, LocalActorRef, Context, Envelope, _extract_actor_message_types
from .scheduler import Scheduler, Schedule, Cancel
from .ticker import Ticker, Subscribe as TickerSubscribe, Unsubscribe as TickerUnsubscribe
from .persistence import WriteAheadLog, Recover, Append, Snapshot, Close
from .cluster.scope import Scope, ClusterScope


class _StopSentinel:
    """Sentinel message to signal actor loop termination."""
    __slots__ = ()


_STOP = _StopSentinel()
from .supervision import (
    ActorStopSignal,
    MultiChildStrategy,
    SupervisionNode,
    SupervisionStrategy,
    SupervisionTreeManager,
    SupervisorConfig,
)

if TYPE_CHECKING:
    pass

log = logging.getLogger(__name__)


class LocalSystem(System):
    """Runtime that manages local actors with supervision support.

    The LocalSystem is responsible for:
    - Creating and managing actor lifecycles
    - Routing messages to actors
    - Implementing supervision strategies
    - Graceful shutdown of all actors

    Usage:
        async with LocalSystem() as system:
            counter = await system.actor(Counter, name="counter")
            await counter.send(Increment(5))
            result = await counter.ask(GetCount())
    """

    def __init__(self) -> None:
        self._actors: dict[ActorId, Task[None]] = {}
        self._mailboxes: dict[ActorId, Queue[Envelope[Any]]] = {}
        self._registry: dict[str, LocalActorRef[Any]] = {}
        self._supervision_tree = SupervisionTreeManager()
        self._running = True
        self._scheduler: LocalActorRef | None = None
        self._ticker: LocalActorRef | None = None

    @staticmethod
    def _build_actor_name(
        cls: type[Actor[Any]],
        name: str,
        parent_path: str | None = None
    ) -> str:
        """Build the internal full name for an actor."""
        type_name = cls.__name__
        if parent_path:
            return f"{parent_path}/{type_name}/{name}"
        return f"{type_name}/{name}"

    @staticmethod
    def _validate_name(name: str, internal: bool = False) -> None:
        """Validate that name doesn't use reserved prefix."""
        if name.startswith("__") and not internal:
            raise ValueError(
                f"Names starting with '__' are reserved for internal use: {name}"
            )

    @classmethod
    @asynccontextmanager
    async def all(cls, *system: tuple["LocalSystem", ...]):
        async with AsyncExitStack() as stack:
            for s in system:
                await stack.enter_async_context(s)

            yield system

    async def schedule[R](
        self,
        timeout: float,
        listener: LocalActorRef[R],
        message: R
    ) -> str | None:
        """Schedule a message to be sent after timeout. Returns task_id for cancellation."""
        if not self._running:
            return None
        if self._scheduler is None:
            self._scheduler = await self._actor_internal(Scheduler, name="__scheduler__")
        task_id = await self._scheduler.ask(Schedule(timeout=timeout, listener=listener, message=message))
        return task_id

    async def cancel_schedule(self, task_id: str) -> None:
        """Cancel a scheduled message by task_id."""
        if self._scheduler:
            await self._scheduler.send(Cancel(task_id))

    async def tick[R](
        self,
        message: R,
        interval: float,
        listener: LocalActorRef[R],
    ) -> str | None:
        """Start periodic message delivery. Returns subscription_id for cancellation.

        Args:
            message: The message to send periodically
            interval: Time between messages in seconds
            listener: The actor to receive the messages

        Returns:
            Subscription ID for use with cancel_tick(), or None if system is shutting down
        """
        if not self._running:
            return None
        if self._ticker is None:
            self._ticker = await self._actor_internal(Ticker, name="__ticker__")
        subscription_id = await self._ticker.ask(
            TickerSubscribe(listener=listener, message=message, interval=interval)
        )
        return subscription_id

    async def cancel_tick(self, subscription_id: str) -> None:
        """Cancel periodic message delivery by subscription_id."""
        if self._ticker:
            await self._ticker.send(TickerUnsubscribe(subscription_id))

    async def actor[M](
        self,
        actor_cls: type[Actor[M]],
        *,
        name: str,
        scope: Scope = 'local',
        supervision: SupervisorConfig | None = None,
        durable: bool = False,
        **kwargs: Any,
    ) -> LocalActorRef[M]:
        """Get or create an actor by name.

        Args:
            actor_cls: The actor class to instantiate
            name: Required name for the actor (part of identity)
            scope: 'local', 'cluster', or ClusterScope (logs warning on LocalSystem)
            supervision: Override supervision configuration
            durable: If True, persist actor state with WAL
            **kwargs: Constructor arguments for the actor

        Returns:
            Reference to the actor (existing or newly created)
        """
        self._validate_name(name)

        if scope != 'local' and not isinstance(scope, str):
            log.warning(
                "LocalSystem does not support cluster scope, treating as 'local'"
            )
        elif scope == 'cluster':
            log.warning(
                "LocalSystem does not support scope='cluster', treating as 'local'"
            )

        full_name = self._build_actor_name(actor_cls, name)

        # Get or create
        if full_name in self._registry:
            return self._registry[full_name]

        return await self._actor_internal(
            actor_cls=actor_cls,
            name=name,
            supervision=supervision,
            durable=durable,
            **kwargs,
        )

    async def _actor_internal[M](
        self,
        actor_cls: type[Actor[M]],
        *,
        name: str,
        supervision: SupervisorConfig | None = None,
        parent_path: str | None = None,
        parent_id: ActorId | None = None,
        durable: bool = False,
        **kwargs: Any,
    ) -> LocalActorRef[M]:
        """Internal actor creation (bypasses name validation for reserved names)."""
        full_name = self._build_actor_name(actor_cls, name, parent_path)

        # Get or create
        if full_name in self._registry:
            return self._registry[full_name]

        # Check class-level supervision config if not explicitly provided
        if supervision is None:
            supervision = getattr(actor_cls, "supervision_config", None)

        ref = await self._create_actor(
            actor_cls=actor_cls,
            name=name,
            full_name=full_name,
            supervision=supervision,
            parent_id=parent_id,
            durable=durable,
            **kwargs,
        )

        self._registry[full_name] = ref
        return ref

    async def spawn[M](
        self,
        actor_cls: type[Actor[M]],
        *,
        name: str | None = None,
        **kwargs: Any,
    ) -> LocalActorRef[M]:
        """Internal spawn method for ephemeral actors (e.g., reply actors).

        Generates a unique name automatically if not provided.
        Used by LocalActorRef.ask(). For named actors, use actor() instead.
        """
        actual_name = name if name else f"__{actor_cls.__name__}_{uuid4().hex[:8]}__"
        return await self._actor_internal(actor_cls, name=actual_name, **kwargs)

    async def stop(self, ref: LocalActorRef[Any]) -> bool:
        """Stop an actor by its reference.

        Args:
            ref: Reference to the actor to stop

        Returns:
            True if actor was found and stopped, False otherwise
        """
        actor_id = ref.id
        task = self._actors.get(actor_id)
        if task:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            return True
        return False

    async def _actor_child[M](
        self,
        parent_ctx: Context[Any],
        actor_cls: type[Actor[M]],
        *,
        name: str,
        root: bool = False,
        supervision: SupervisorConfig | None = None,
        **kwargs: Any,
    ) -> LocalActorRef[M]:
        """Get or create a child actor (called from Context.actor).

        The child inherits supervision configuration from:
        1. Explicit supervision parameter
        2. Actor class supervision_config attribute
        3. Parent's default_child_supervision (if defined)
        4. System default
        """
        self._validate_name(name)

        parent_node = parent_ctx._supervision_node

        # Determine parent path for naming
        if root:
            parent_path = None
            parent_id = None
        else:
            parent_path = parent_ctx.self_ref.id.name if parent_ctx.self_ref.id.name else None
            parent_id = parent_node.actor_id if parent_node else None

        full_name = self._build_actor_name(actor_cls, name, parent_path)

        # Get or create
        if full_name in self._registry:
            return self._registry[full_name]

        # Determine supervision config
        if supervision is None:
            supervision = getattr(actor_cls, "supervision_config", None)
        if supervision is None and parent_node:
            parent_actor = parent_node.actor_instance
            supervision = getattr(parent_actor, "default_child_supervision", None)
        if supervision is None:
            supervision = SupervisorConfig()

        ref = await self._create_actor(
            actor_cls=actor_cls,
            name=name,
            full_name=full_name,
            supervision=supervision,
            parent_id=parent_id,
            **kwargs,
        )

        self._registry[full_name] = ref
        return ref

    async def _create_actor[M](
        self,
        actor_cls: type[Actor[M]],
        *,
        name: str,
        full_name: str,
        supervision: SupervisorConfig | None = None,
        parent_id: ActorId | None = None,
        durable: bool = False,
        **kwargs: Any,
    ) -> LocalActorRef[M]:
        """Internal actor creation with supervision registration."""
        actor = actor_cls(**kwargs)
        actor_id = ActorId(uid=uuid4(), name=full_name)
        mailbox: Queue[Envelope[M]] = Queue()

        # Extract accepted message types from actor class
        accepted_types = _extract_actor_message_types(actor_cls)

        ref: LocalActorRef[M] = LocalActorRef(actor_id, mailbox, self, accepted_types=accepted_types)

        config = supervision or SupervisorConfig()

        # Register in supervision tree
        node = await self._supervision_tree.register(
            actor_id=actor_id,
            actor_ref=ref,
            actor_instance=actor,
            actor_cls=actor_cls,
            kwargs=kwargs,
            config=config,
            parent_id=parent_id,
        )

        ctx: Context[M] = Context(
            self_ref=ref,
            system=self,
            parent=node.parent.actor_ref if node.parent else None,
            _supervision_node=node,
        )
        actor._ctx = ctx

        # Setup WAL if durable
        wal_ref: LocalActorRef | None = None
        if durable:
            # Create WAL actor as child of durable actor
            wal_ref = await self._actor_internal(
                actor_cls=WriteAheadLog,
                name=f"__wal_{name}__",
                parent_path=full_name,
                parent_id=actor_id,
                actor_id=actor_id,
            )

            # Recover state from WAL
            snapshot, events = await wal_ref.ask(Recover(), timeout=10.0)

            if snapshot is not None:
                # Restore from snapshot
                state = msgpack.unpackb(snapshot)
                self._apply_state(actor, state)

            # TODO: Decide if we replay events after snapshot

        # Store mailbox for restart
        self._mailboxes[actor_id] = mailbox

        # Start actor task
        task = asyncio.create_task(
            self._run_actor_loop(actor, actor_id, mailbox, ctx, node, wal_ref)
        )
        self._actors[actor_id] = task
        return ref

    def _apply_state(self, actor: Actor[Any], state: dict[str, Any]) -> None:
        """Apply state to actor."""
        if hasattr(actor, "set_state"):
            actor.set_state(state)
        else:
            # Default: restore public attributes
            for key, value in state.items():
                if not key.startswith("_"):
                    setattr(actor, key, value)

    def _get_state(self, actor: Actor[Any]) -> dict[str, Any]:
        """Get state from actor."""
        if hasattr(actor, "get_state"):
            return actor.get_state()

        # Default: serialize public attributes
        exclude = getattr(actor, "__casty_exclude_fields__", set())
        state = {}
        for key, value in actor.__dict__.items():
            if not key.startswith("_") and key not in exclude:
                state[key] = value
        return state

    async def snapshot_durable_actor[M](self, actor_ref: LocalActorRef[M]) -> None:
        """Force a state snapshot on a durable actor.

        This immediately persists the current actor state to the WAL,
        bypassing the automatic snapshot interval (default 1000 messages).

        Useful before graceful shutdown or at strategic points in your application.

        Args:
            actor_ref: Reference to the durable actor

        Example:
            counter = await system.actor(Counter, name="counter", durable=True)
            await counter.send(Increment(10))
            await system.snapshot_durable_actor(counter)  # Force snapshot now
        """
        # Get the supervision node for this actor
        actor_id = actor_ref.id
        node = self._supervision_tree.get_node(actor_id)
        if not node:
            raise ValueError(f"Actor {actor_id} not found")

        # Find WAL child actor
        wal_ref = None
        for child_id in node.children.keys():
            child_node = self._supervision_tree.get_node(child_id)
            if child_node and "wal" in str(child_node.actor_id):
                wal_ref = child_node.actor_ref
                break

        if not wal_ref:
            raise ValueError(f"Actor {actor_id} is not durable (no WAL actor found)")

        # Extract and snapshot state
        state = self._get_state(node.actor_instance)
        state_bytes = msgpack.packb(state, use_bin_type=True)
        await wal_ref.send(Snapshot(state_bytes))

    async def _run_actor_loop[M](
        self,
        actor: Actor[M],
        actor_id: ActorId,
        mailbox: Queue[Envelope[M]],
        ctx: Context[M],
        node: SupervisionNode,
        wal_ref: LocalActorRef | None = None,
    ) -> None:
        """Actor run loop with supervision handling."""
        current_msg: M | None = None
        snapshot_counter = 0

        try:
            await actor.on_start()

            while True:
                envelope = await mailbox.get()

                # Check for stop sentinel
                if isinstance(envelope.payload, _StopSentinel):
                    break

                # Create immutable context for this message
                # This allows async callbacks to safely use ctx.reply() after handler returns
                msg_ctx = dataclasses.replace(
                    ctx,
                    _current_envelope=envelope,
                    sender=envelope.sender,
                )
                current_msg = envelope.payload

                try:
                    # WAL: Log message BEFORE processing
                    if wal_ref:
                        # Convert dataclass to dict for serialization
                        if dataclasses.is_dataclass(current_msg):
                            msg_dict = dataclasses.asdict(current_msg)
                            msg_bytes = msgpack.packb(msg_dict, use_bin_type=True)
                        else:
                            msg_bytes = msgpack.packb(current_msg, use_bin_type=True)
                        await wal_ref.send(Append(msg_bytes))

                    # Use behavior from stack if available, otherwise default receive
                    if msg_ctx._behavior_stack:
                        await msg_ctx._behavior_stack[-1](envelope.payload, msg_ctx)
                    else:
                        await actor.receive(envelope.payload, msg_ctx)

                    # WAL: Snapshot periodically
                    if wal_ref:
                        snapshot_counter += 1
                        if snapshot_counter >= 1000:  # TODO: configurable
                            state = self._get_state(actor)
                            state_bytes = msgpack.packb(state, use_bin_type=True)
                            await wal_ref.send(Snapshot(state_bytes))
                            snapshot_counter = 0

                    # Success - reset failure tracking
                    if node.restart_record:
                        node.restart_record.record_success()
                except Exception as exc:
                    log.exception(f"Actor {actor_id} failed processing message")
                    await self._handle_failure(node, exc, current_msg)
                finally:
                    current_msg = None

        except asyncio.CancelledError:
            pass
        except ActorStopSignal:
            pass
        finally:
            try:
                await actor.on_stop()
            except Exception:
                log.exception(f"Error in on_stop for {actor_id}")

            # Close WAL
            if wal_ref:
                await wal_ref.send(Close())

            await self._supervision_tree.unregister(actor_id)
            self._actors.pop(actor_id, None)
            self._mailboxes.pop(actor_id, None)
            # Clean up registry (actor_id.name is the full_name)
            if actor_id.name:
                self._registry.pop(actor_id.name, None)

    async def _handle_failure(
        self,
        node: SupervisionNode,
        exc: Exception,
        msg: Any | None,
    ) -> None:
        """Handle an actor failure according to supervision strategy."""
        config = node.config

        # Check restart limits
        if node.restart_record and node.restart_record.exceeds_limit(config):
            log.warning(
                f"Actor {node.actor_id} exceeded restart limit, "
                f"escalating or stopping"
            )
            if node.parent:
                # Escalate to parent
                await self._handle_child_failure(node.parent, node, exc, msg)
            else:
                # Root actor - stop
                raise ActorStopSignal()
            return

        # Apply strategy
        if node.parent:
            await self._handle_child_failure(node.parent, node, exc, msg)
        else:
            # Root actor supervision
            match config.strategy:
                case SupervisionStrategy.STOP:
                    raise ActorStopSignal()
                case SupervisionStrategy.ESCALATE:
                    # No parent - treat as stop
                    log.warning(f"Root actor {node.actor_id} escalated, stopping")
                    raise ActorStopSignal()
                case SupervisionStrategy.RESTART:
                    await self._restart_actor(node, exc, msg)

    async def _handle_child_failure(
        self,
        parent_node: SupervisionNode,
        child_node: SupervisionNode,
        exc: Exception,
        msg: Any | None,
    ) -> None:
        """Handle failure of a child actor."""
        config = child_node.config

        # Allow parent to override strategy
        parent_actor = parent_node.actor_instance
        try:
            decision = await parent_actor.on_child_failure(child_node.actor_ref, exc)
        except Exception:
            log.exception(f"Error in on_child_failure for {parent_node.actor_id}")
            decision = None

        strategy = decision.action if decision else config.strategy

        match strategy:
            case SupervisionStrategy.STOP:
                raise ActorStopSignal()

            case SupervisionStrategy.ESCALATE:
                # Propagate to grandparent
                if parent_node.parent:
                    await self._handle_child_failure(
                        parent_node.parent, parent_node, exc, msg
                    )
                else:
                    raise ActorStopSignal()

            case SupervisionStrategy.RESTART:
                # Handle multi-child strategy
                match config.multi_child:
                    case MultiChildStrategy.ONE_FOR_ONE:
                        await self._restart_actor(child_node, exc, msg)

                    case MultiChildStrategy.ONE_FOR_ALL:
                        # Restart all siblings
                        all_children = self._supervision_tree.get_children(
                            parent_node.actor_id
                        )
                        for sibling in all_children:
                            if sibling.actor_id != child_node.actor_id:
                                await self._restart_actor(sibling, exc, None)
                        await self._restart_actor(child_node, exc, msg)

                    case MultiChildStrategy.REST_FOR_ONE:
                        # Restart this child and all spawned after
                        children = self._supervision_tree.get_children_after(
                            parent_node.actor_id, child_node.actor_id
                        )
                        for child in children:
                            await self._restart_actor(
                                child, exc, msg if child == child_node else None
                            )

    async def _restart_actor(
        self,
        node: SupervisionNode,
        exc: Exception,
        msg: Any | None,
    ) -> None:
        """Restart an actor with backoff."""
        config = node.config
        actor = node.actor_instance

        # Calculate backoff
        if node.restart_record:
            backoff = node.restart_record.calculate_next_backoff(config)
            node.restart_record.record_restart(backoff)
        else:
            backoff = config.backoff_initial

        # Apply backoff delay
        if backoff > 0:
            log.debug(f"Actor {node.actor_id} backing off for {backoff:.2f}s")
            await asyncio.sleep(backoff)

        # Call lifecycle hooks
        try:
            await actor.pre_restart(exc, msg)
        except Exception:
            log.exception(f"Error in pre_restart for {node.actor_id}")

        # Create new actor instance
        new_actor = node.actor_cls(**node.kwargs)

        # Deep copy state from old to new instance
        for key, value in actor.__dict__.items():
            if key != '_ctx':
                try:
                    setattr(new_actor, key, copy.deepcopy(value))
                except TypeError:
                    setattr(new_actor, key, value)

        # Update node with new instance
        node.actor_instance = new_actor

        # Transfer context with updated supervision node (Context is frozen)
        new_actor._ctx = dataclasses.replace(actor._ctx, _supervision_node=node)

        # Call post_restart
        try:
            await new_actor.post_restart(exc)
        except Exception:
            log.exception(f"Error in post_restart for {node.actor_id}")

        log.info(
            f"Restarted actor {node.actor_id} after {exc.__class__.__name__}, "
            f"backoff={backoff:.2f}s"
        )

    async def _stop_child(self, ctx: Context[Any], child_id: ActorId) -> bool:
        """Stop a specific child actor."""
        node = self._supervision_tree.get_node(child_id)
        if not node:
            return False

        # Verify it's actually a child
        if ctx._supervision_node and child_id not in ctx._supervision_node.children:
            return False

        task = self._actors.get(child_id)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        return True

    async def _restart_child(
        self, ctx: Context[Any], child_id: ActorId
    ) -> LocalActorRef[Any]:
        """Manually restart a child actor."""
        node = self._supervision_tree.get_node(child_id)
        if not node:
            raise ValueError(f"Child {child_id} not found")

        # Verify it's actually a child
        if ctx._supervision_node and child_id not in ctx._supervision_node.children:
            raise ValueError(f"{child_id} is not a child of this actor")

        # Restart
        await self._restart_actor(node, RuntimeError("Manual restart"), None)
        return node.actor_ref

    async def _stop_all_children(self, ctx: Context[Any]) -> None:
        """Stop all child actors."""
        if not ctx._supervision_node:
            return

        for child_id in list(ctx._supervision_node.children.keys()):
            await self._stop_child(ctx, child_id)

    async def shutdown(self) -> None:
        """Stop all actors gracefully.

        Sends stop sentinel to all mailboxes and waits for actors to finish.
        Safe to call from within an actor.
        """
        self._running = False

        # Send stop sentinel to all mailboxes
        for mailbox in self._mailboxes.values():
            await mailbox.put(Envelope(_STOP))

        # Wait for all actor tasks to complete
        current = asyncio.current_task()
        others = [t for t in self._actors.values() if t is not current]
        if others:
            await asyncio.gather(*others, return_exceptions=True)

    async def start(self) -> None:
        pass

    async def __aenter__(self) -> "LocalSystem":
        """Enter async context manager."""
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: object,
    ) -> None:
        """Exit async context manager, ensuring graceful shutdown."""
        await self.shutdown()
