"""Actor system runtime for Casty - local execution."""

from __future__ import annotations

import asyncio
import dataclasses
import logging
from asyncio import Queue, Task
from contextlib import contextmanager, asynccontextmanager, AsyncExitStack
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from .actor import Actor, ActorId, LocalRef, Context, Envelope, ShardedRef, _extract_actor_message_types
from .scheduler import Scheduler, Schedule
from .supervision import (
    ActorStopSignal,
    MultiChildStrategy,
    RestartRecord,
    SupervisionNode,
    SupervisionStrategy,
    SupervisionTreeManager,
    SupervisorConfig,
)

if TYPE_CHECKING:
    from .persistence import ReplicationConfig
    from .cluster import DistributedActorSystem
    from .cluster.clustered_system import ClusteredActorSystem
    from .cluster.config import ClusterConfig

log = logging.getLogger(__name__)


class ActorSystem:
    """Runtime that manages local actors with supervision support.

    The ActorSystem is responsible for:
    - Creating and managing actor lifecycles
    - Routing messages to actors
    - Implementing supervision strategies
    - Graceful shutdown of all actors

    Usage:
        async with ActorSystem() as system:
            counter = await system.spawn(Counter)
            await counter.send(Increment(5))
            result = await counter.ask(GetCount())
    """

    def __init__(self) -> None:
        self._actors: dict[ActorId, Task[None]] = {}
        self._mailboxes: dict[ActorId, Queue[Envelope[Any]]] = {}
        self._supervision_tree = SupervisionTreeManager()
        self._running = True
        self._scheduler: LocalRef[Schedule] | None = None

    @classmethod
    @asynccontextmanager
    async def all(cls, *system: tuple["ActorSystem", ...]):
        async with AsyncExitStack() as stack:
            for s in system:
                await stack.enter_async_context(s)

            yield system

    @staticmethod
    def clustered(
        host: str = "0.0.0.0",
        port: int = 0,
        seeds: list[str] | None = None,
        *,
        node_id: str | None = None,
        advertise_host: str | None = None,
        advertise_port: int | None = None,
    ) -> "ClusteredActorSystem":
        """Factory method to create a clustered actor system.

        Creates an ActorSystem where named actors are automatically
        registered in the cluster for remote discovery.

        Args:
            host: Host to bind the server to (default: 0.0.0.0)
            port: Port to bind the server to (default: 0 = auto-assign)
            seeds: List of seed nodes in "host:port" format
            node_id: Unique identifier for this node (auto-generated if None)
            advertise_host: Host to announce to other nodes (for NAT/proxy)
            advertise_port: Port to announce to other nodes (for NAT/proxy)

        Returns:
            A ClusteredActorSystem instance

        Example:
            async with ActorSystem.clustered(
                host="0.0.0.0",
                port=7946,
                seeds=["192.168.1.10:7946"],
            ) as system:
                # Named actor → automatically registered in cluster
                worker = await system.spawn(Worker, name="worker-1")

                # Get reference to remote actor
                remote = await system.get_ref("worker-2")
                result = await remote.ask(GetStatus())
        """
        from .cluster.clustered_system import ClusteredActorSystem
        from .cluster.config import ClusterConfig

        # Create base config
        config = ClusterConfig(
            bind_host=host,
            bind_port=port,
            advertise_host=advertise_host,
            advertise_port=advertise_port,
            node_id=node_id,
        )

        # Add seeds if provided (this also updates SwimConfig with parsed seeds)
        if seeds:
            config = config.with_seeds(seeds)

        return ClusteredActorSystem(config)

    async def schedule[R](
        self,
        timeout: float,
        listener: LocalRef[R],
        message: R
    ) -> None:
        if self._scheduler is None:
            self._scheduler = await self.spawn(Scheduler)
        await (self._scheduler >> Schedule(timeout=timeout, listener=listener, message=message))

    async def spawn[M](
        self,
        actor_cls: type[Actor[M]],
        *,
        name: str | None = None,
        supervision: SupervisorConfig | None = None,
        durable: bool = False,
        **kwargs: Any,
    ) -> LocalRef[M]:
        """Create and start a new root actor.

        Args:
            actor_cls: The actor class to instantiate
            name: Optional name for the actor
            supervision: Override supervision configuration
            durable: If True, persist actor state with WAL
            **kwargs: Constructor arguments for the actor

        Returns:
            Reference to the spawned actor
        """
        return await self._spawn_internal(
            actor_cls=actor_cls,
            name=name,
            supervision=supervision,
            parent_id=None,
            durable=durable,
            **kwargs,
        )

    async def _spawn_child[M](
        self,
        parent_ctx: Context[Any],
        actor_cls: type[Actor[M]],
        *,
        name: str | None = None,
        supervision: SupervisorConfig | None = None,
        **kwargs: Any,
    ) -> LocalRef[M]:
        """Spawn a child actor (called from Context.spawn).

        The child inherits supervision configuration from:
        1. Explicit supervision parameter
        2. Actor class supervision_config attribute
        3. Parent's default_child_supervision (if defined)
        4. System default
        """
        parent_node = parent_ctx._supervision_node
        parent_id = parent_node.actor_id if parent_node else None

        # Determine supervision config
        if supervision is None:
            supervision = getattr(actor_cls, "supervision_config", None)
        if supervision is None and parent_node:
            parent_actor = parent_node.actor_instance
            supervision = getattr(parent_actor, "default_child_supervision", None)
        if supervision is None:
            supervision = SupervisorConfig()

        return await self._spawn_internal(
            actor_cls=actor_cls,
            name=name,
            supervision=supervision,
            parent_id=parent_id,
            **kwargs,
        )

    async def _spawn_internal[M](
        self,
        actor_cls: type[Actor[M]],
        *,
        name: str | None = None,
        supervision: SupervisorConfig | None = None,
        parent_id: ActorId | None = None,
        durable: bool = False,
        **kwargs: Any,
    ) -> LocalRef[M]:
        """Internal spawn logic with supervision registration."""
        actor = actor_cls(**kwargs)
        actor_id = ActorId(uid=uuid4(), name=name)
        mailbox: Queue[Envelope[M]] = Queue()

        # Extract accepted message types from actor class
        accepted_types = _extract_actor_message_types(actor_cls)

        ref: LocalRef[M] = LocalRef(actor_id, mailbox, self, accepted_types=accepted_types)

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
        wal_ref: LocalRef | None = None
        if durable:
            from .persistence import WriteAheadLog, Recover
            import msgpack

            # Spawn WAL actor as child
            wal_ref = await self._spawn_child(
                parent_ctx=ctx,
                actor_cls=WriteAheadLog,
                name=f"wal-{actor_id.name or actor_id.uid.hex[:8]}",
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

    async def snapshot_durable_actor[M](self, actor_ref: LocalRef[M]) -> None:
        """Force a state snapshot on a durable actor.

        This immediately persists the current actor state to the WAL,
        bypassing the automatic snapshot interval (default 1000 messages).

        Useful before graceful shutdown or at strategic points in your application.

        Args:
            actor_ref: Reference to the durable actor

        Example:
            counter = await system.spawn(Counter, durable=True)
            await counter.send(Increment(10))
            await system.snapshot_durable_actor(counter)  # Force snapshot now
        """
        from .persistence import Snapshot
        import msgpack

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
        wal_ref: LocalRef | None = None,
    ) -> None:
        """Actor run loop with supervision handling."""
        current_msg: M | None = None
        snapshot_counter = 0

        try:
            await actor.on_start()

            while self._running:
                try:
                    envelope = await asyncio.wait_for(mailbox.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    continue

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
                        from .persistence import Append
                        import msgpack

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
                            from .persistence import Snapshot
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
                from .persistence import Close
                await wal_ref.send(Close())

            await self._supervision_tree.unregister(actor_id)
            self._actors.pop(actor_id, None)
            self._mailboxes.pop(actor_id, None)

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
    ) -> LocalRef[Any]:
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

        Sets _running to False and waits for actors to finish.
        Safe to call from within an actor.
        """
        self._running = False
        current = asyncio.current_task()
        others = [t for t in self._actors.values() if t is not current]
        for task in others:
            task.cancel()
        if others:
            await asyncio.gather(*others, return_exceptions=True)

    async def start(self) -> None:
        pass

    async def __aenter__(self) -> "ActorSystem":
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
