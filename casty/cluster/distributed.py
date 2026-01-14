"""Distributed actor system for Casty.

Provides cluster-wide actor deployment with:
- Control plane (Raft) for membership and singletons
- Data plane (consistent hashing + gossip) for entity routing
- Event sourcing for persistence
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from asyncio import Queue

from ..actor import Actor, ActorRef, ActorId, ShardedRef, EntityRef, Envelope, Context
from ..system import ActorSystem, ActorStopSignal
from ..supervision import SupervisorConfig, SupervisionNode

from .control_plane import (
    ClusterState,
    RaftManager,
    RaftConfig,
    JoinCluster,
    LeaveCluster,
    NodeFailed,
    RegisterSingleton,
    UnregisterSingleton,
    RegisterEntityType,
    RegisterNamedActor,
    UnregisterNamedActor,
    VoteRequest,
    VoteResponse,
    AppendEntriesRequest,
    AppendEntriesResponse,
)
from .data_plane import (
    HashRing,
    HashRingConfig,
    SWIMProtocol,
    GossipConfig,
    DataPlaneRouter,
    Ping,
    PingReq,
    Ack,
)
from .transport import Transport, MessageType, WireMessage
from .serialize import serialize, deserialize

from ..persistence import ReplicationConfig

logger = logging.getLogger(__name__)


class DistributedActorSystem(ActorSystem):
    """Actor system with distributed cluster support.

    Extends the local ActorSystem with:
    - Cluster membership via Raft consensus
    - Singleton actors (cluster-wide unique)
    - Sharded actors with consistent hashing
    - Event replication for fault tolerance

    Architecture:
        Control Plane (Raft):
        - Cluster membership (join/leave/fail)
        - Singleton registry
        - Named actor registry
        - Entity type configurations

        Data Plane:
        - Consistent hashing for entity distribution
        - SWIM gossip for failure detection
        - Direct TCP routing for messages

    Usage:
        system = ActorSystem.distributed(
            host="0.0.0.0",
            port=8001,
            seeds=["node2:8001", "node3:8001"],
        )

        async with system:
            # Singleton actor
            cache = await system.spawn(CacheManager, name="cache", singleton=True)

            # Sharded actors
            accounts = await system.spawn(Account, name="accounts", sharded=True)
            await accounts["user-123"].send(Deposit(100))

            await system.serve()
    """

    def __init__(
        self,
        host: str,
        port: int,
        seeds: list[str] | None = None,
        *,
        node_id: str | None = None,
        persistence_dir: str | Path | None = None,
        replication: "ReplicationConfig | None" = None,
        raft_config: RaftConfig | None = None,
        hash_ring_config: HashRingConfig | None = None,
        gossip_config: GossipConfig | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__()

        self._host = host
        self._port = port
        self._seeds = seeds or []
        self._node_id = node_id or self._generate_node_id()
        self._persistence_dir = Path(persistence_dir) if persistence_dir else None
        self._replication = replication

        # Parse seed addresses
        self._seed_addresses: list[tuple[str, int]] = []
        for seed in self._seeds:
            if ":" in seed:
                h, p = seed.rsplit(":", 1)
                self._seed_addresses.append((h, int(p)))
            else:
                self._seed_addresses.append((seed, port))

        # Control plane - Raft for cluster membership and singletons
        self._raft = RaftManager(
            node_id=self._node_id,
            peers=[],  # Will add peers when joining
            config=raft_config,
        )
        self._raft.on_leadership_change = self._on_leadership_change
        self._raft.on_state_change = self._on_cluster_state_change

        # Data plane - Router with hash ring and gossip
        self._router = DataPlaneRouter(
            node_id=self._node_id,
            address=(host, port),
            hash_config=hash_ring_config,
            gossip_config=gossip_config,
        )

        # Transport for network communication
        self._transport = Transport(self._node_id, host, port)

        # Wire up transport to components
        self._raft.set_transport(self._transport)
        self._router.set_transport(self._transport)

        # Register message handlers
        self._setup_message_handlers()

        # Singleton tracking
        self._local_singletons: dict[str, ActorRef] = {}

        # Sharded entity tracking
        self._entity_types: dict[str, type[Actor]] = {}
        self._local_entities: dict[str, dict[str, ActorRef]] = {}  # type -> {id -> ref}
        self._entity_replication_configs: dict[str, "ReplicationConfig"] = {}  # type -> config

        # Named actor tracking
        self._named_actors: dict[str, ActorRef] = {}

        # Cluster state
        self._cluster_joined = False
        self._serve_task: asyncio.Task | None = None

    def _generate_node_id(self) -> str:
        """Generate a unique node ID."""
        return f"{self._host}:{self._port}-{uuid4().hex[:8]}"

    def _setup_message_handlers(self) -> None:
        """Set up message handlers for the transport."""
        # Actor messages
        self._transport.set_handler(
            MessageType.ACTOR_MSG,
            self._handle_actor_message,
        )
        self._transport.set_handler(
            MessageType.ASK_REQUEST,
            self._handle_ask_request,
        )

        # Entity messages
        self._transport.set_handler(
            MessageType.ENTITY_MSG,
            self._handle_entity_message,
        )
        self._transport.set_handler(
            MessageType.ENTITY_ASK_REQUEST,
            self._handle_entity_ask_request,
        )

        # Raft messages
        self._transport.set_handler(
            MessageType.VOTE_REQUEST,
            self._handle_vote_request,
        )
        self._transport.set_handler(
            MessageType.APPEND_ENTRIES_REQUEST,
            self._handle_append_entries,
        )

        # Gossip messages
        self._transport.set_handler(
            MessageType.GOSSIP_PING,
            self._handle_gossip_ping,
        )
        self._transport.set_handler(
            MessageType.GOSSIP_PING_REQ,
            self._handle_gossip_ping_req,
        )
        self._transport.set_handler(
            MessageType.GOSSIP_ACK,
            self._handle_gossip_ack,
        )

        # Lookup messages
        self._transport.set_handler(
            MessageType.LOOKUP_REQUEST,
            self._handle_lookup_request,
        )

        # State replication messages
        self._transport.set_handler(
            MessageType.STATE_UPDATE,
            self._handle_state_update,
        )

    @property
    def node_id(self) -> str:
        """Get this node's unique identifier."""
        return self._node_id

    @property
    def is_leader(self) -> bool:
        """Check if this node is the Raft leader."""
        return self._raft.is_leader

    @property
    def leader_id(self) -> str | None:
        """Get the current Raft leader's node ID."""
        return self._raft.leader_id

    @property
    def cluster_state(self) -> ClusterState:
        """Get the current cluster state."""
        return self._raft.state

    async def start(self) -> None:
        """Start the distributed actor system.

        Starts the transport, connects to seeds, and joins the cluster.
        """
        # Start transport
        await self._transport.start()

        # Start router (gossip)
        await self._router.start()

        # Start Raft
        await self._raft.start()

        # Connect to seed nodes
        await self._connect_to_seeds()

        # Join cluster
        await self._join_cluster()

        logger.info(f"DistributedActorSystem started on {self._host}:{self._port}")

    async def shutdown(self) -> None:
        """Shut down the distributed actor system.

        Gracefully leaves the cluster, stops all actors, and closes connections.
        """
        # Leave cluster
        await self._leave_cluster()

        # Stop Raft
        await self._raft.stop()

        # Stop router (gossip)
        await self._router.stop()

        # Stop transport
        await self._transport.stop()

        # Stop local actors
        await super().shutdown()

        logger.info(f"DistributedActorSystem stopped")

    async def serve(self) -> None:
        """Run the distributed system until shutdown.

        This method:
        - Keeps the system running
        - Participates in Raft consensus
        - Processes messages
        - Handles cluster events

        Blocks until shutdown() is called.
        """
        self._serve_task = asyncio.current_task()

        try:
            while self._running:
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            pass

    async def _connect_to_seeds(self) -> None:
        """Connect to seed nodes."""
        for host, port in self._seed_addresses:
            seed_id = f"{host}:{port}"
            self._transport.add_peer(seed_id, host, port)
            self._raft.add_peer(seed_id)
            self._router.add_node(seed_id, (host, port))
            logger.debug(f"Added seed node {seed_id}")

    async def _join_cluster(self) -> None:
        """Join the cluster via Raft."""
        # If we're alone, we'll become leader automatically
        if not self._seeds:
            self._cluster_joined = True
            return

        # Wait for Raft to stabilize
        await asyncio.sleep(0.5)

        # Propose join command (only works if leader)
        if self._raft.is_leader:
            cmd = JoinCluster(
                node_id=self._node_id,
                address=(self._host, self._port),
                timestamp=time.time(),
            )
            await self._raft.append_command(cmd)
            self._cluster_joined = True
        else:
            # Wait for leader to accept our join
            # In a real implementation, we'd forward to leader
            await asyncio.sleep(1.0)
            self._cluster_joined = True

    async def _leave_cluster(self) -> None:
        """Leave the cluster gracefully."""
        if not self._cluster_joined:
            return

        # Unregister singletons on this node
        for name in list(self._local_singletons.keys()):
            if self._raft.is_leader:
                await self._raft.append_command(UnregisterSingleton(name))

        # Unregister named actors on this node
        for name in list(self._named_actors.keys()):
            if self._raft.is_leader:
                await self._raft.append_command(UnregisterNamedActor(name))

        # Send leave command
        if self._raft.is_leader:
            cmd = LeaveCluster(
                node_id=self._node_id,
                timestamp=time.time(),
            )
            await self._raft.append_command(cmd)

        self._cluster_joined = False

    async def spawn(
        self,
        actor_cls: type[Actor],
        *,
        name: str | None = None,
        singleton: bool = False,
        sharded: bool = False,
        replication: ReplicationConfig | None = None,
        supervision: SupervisorConfig | None = None,
        **kwargs: Any,
    ) -> ActorRef | ShardedRef:
        """Spawn an actor in the cluster.

        Args:
            actor_cls: Actor class to instantiate
            name: Optional unique name
            singleton: If True, creates a cluster-wide singleton
            sharded: If True, creates a sharded entity type
            replication: Replication configuration (factor, mode, timeout)
            supervision: Supervision configuration
            **kwargs: Arguments passed to actor constructor

        Returns:
            ActorRef for regular/singleton actors, ShardedRef for sharded
        """
        if singleton and sharded:
            raise ValueError("Actor cannot be both singleton and sharded")

        if singleton:
            return await self._spawn_singleton(
                actor_cls, name=name, supervision=supervision, **kwargs
            )

        if sharded:
            return await self._spawn_sharded(
                actor_cls,
                name=name,
                replication=replication,
                **kwargs,
            )

        # Regular actor - spawn locally
        ref = await super().spawn(
            actor_cls, name=name, supervision=supervision, **kwargs
        )

        # Register named actor if name provided
        if name:
            self._named_actors[name] = ref
            if self._raft.is_leader:
                cmd = RegisterNamedActor(
                    name=name,
                    node_id=self._node_id,
                    actor_id=str(ref.id.uid),
                )
                await self._raft.append_command(cmd)

        return ref

    async def _spawn_singleton(
        self,
        actor_cls: type[Actor],
        *,
        name: str | None = None,
        supervision: SupervisorConfig | None = None,
        **kwargs: Any,
    ) -> ActorRef:
        """Spawn a singleton actor.

        Singletons are cluster-wide unique. Only one instance exists.
        """
        if not name:
            raise ValueError("Singleton actors require a name")

        # Check if singleton already exists
        existing = self._raft.state.get_singleton(name)
        if existing:
            if existing.node_id == self._node_id:
                # We host it
                if name in self._local_singletons:
                    return self._local_singletons[name]
            else:
                # Another node hosts it - return remote ref
                # For now, raise error (remote refs need more work)
                raise RuntimeError(f"Singleton '{name}' exists on {existing.node_id}")

        # Spawn locally
        ref = await super().spawn(
            actor_cls, name=name, supervision=supervision, **kwargs
        )
        self._local_singletons[name] = ref

        # Register in cluster state
        if self._raft.is_leader:
            cmd = RegisterSingleton(
                name=name,
                node_id=self._node_id,
                actor_class=f"{actor_cls.__module__}.{actor_cls.__qualname__}",
                version=1,
            )
            await self._raft.append_command(cmd)

        return ref

    async def _spawn_sharded(
        self,
        actor_cls: type[Actor],
        *,
        name: str | None = None,
        replication: ReplicationConfig | None = None,
        **kwargs: Any,
    ) -> ShardedRef:
        """Spawn a sharded entity type.

        Sharded entities are distributed across the cluster using
        consistent hashing.

        Args:
            actor_cls: Actor class for entities
            name: Entity type name (defaults to class name)
            replication: Replication configuration (factor, mode, timeout)
        """
        entity_type = name or actor_cls.__name__

        # Use default config if not provided
        if replication is None:
            replication = ReplicationConfig()

        # Register entity type
        self._entity_types[entity_type] = actor_cls
        self._local_entities[entity_type] = {}
        self._entity_replication_configs[entity_type] = replication
        self._router.configure_entity_type(entity_type, replication.factor)

        # Register in cluster state
        if self._raft.is_leader:
            cmd = RegisterEntityType(
                entity_type=entity_type,
                actor_class=f"{actor_cls.__module__}.{actor_cls.__qualname__}",
                replication_factor=replication.factor,
            )
            await self._raft.append_command(cmd)

        return ShardedRef(
            entity_type=entity_type,
            system=self,
            send_callback=self._sharded_send,
            ask_callback=self._sharded_ask,
            replication_factor=replication.factor,
        )

    async def _sharded_send(
        self,
        entity_type: str,
        entity_id: str,
        msg: Any,
    ) -> None:
        """Send a message to a sharded entity."""
        # Check if we should handle locally
        if self._router.is_primary(entity_type, entity_id):
            ref = await self.get_or_create_entity(entity_type, entity_id)
            await ref.send(msg)
        else:
            # Route to primary node
            result = await self._router.route_message(
                entity_type, entity_id, msg, prefer_local=False
            )
            if not result.success:
                raise RuntimeError(f"Failed to route message: {result.error}")

    async def _sharded_ask(
        self,
        entity_type: str,
        entity_id: str,
        msg: Any,
        timeout: float,
    ) -> Any:
        """Ask a sharded entity for a response."""
        # Check if we should handle locally
        if self._router.is_primary(entity_type, entity_id):
            ref = await self.get_or_create_entity(entity_type, entity_id)
            return await ref.ask(msg, timeout=timeout)
        else:
            # Route to primary node
            result = await self._router.route_ask(
                entity_type, entity_id, msg, timeout=timeout
            )
            if result.success:
                return result.response
            raise RuntimeError(f"Failed to route ask: {result.error}")

    async def get_or_create_entity(
        self,
        entity_type: str,
        entity_id: str,
    ) -> ActorRef:
        """Get or create a local entity instance.

        Called when this node is responsible for an entity.

        Args:
            entity_type: Type of entity
            entity_id: Entity's unique ID

        Returns:
            ActorRef to the local entity
        """
        # Check if we already have it
        if entity_type in self._local_entities:
            if entity_id in self._local_entities[entity_type]:
                return self._local_entities[entity_type][entity_id]

        # Create new instance
        actor_cls = self._entity_types.get(entity_type)
        if not actor_cls:
            raise ValueError(f"Unknown entity type: {entity_type}")

        # Spawn with entity_id as constructor arg
        ref = await super().spawn(
            actor_cls,
            name=f"{entity_type}:{entity_id}",
            entity_id=entity_id,
        )

        if entity_type not in self._local_entities:
            self._local_entities[entity_type] = {}
        self._local_entities[entity_type][entity_id] = ref

        return ref

    async def lookup(self, name: str) -> ActorRef | None:
        """Look up a named actor in the cluster.

        Args:
            name: The actor name to look up

        Returns:
            ActorRef if found, None otherwise
        """
        # Check local actors first
        if name in self._named_actors:
            return self._named_actors[name]

        # Check cluster state
        actor_info = self._raft.state.get_named_actor(name)
        if actor_info:
            if actor_info.node_id == self._node_id:
                return self._named_actors.get(name)
            # TODO: Return remote ref
            return None

        # Check singletons
        if name in self._local_singletons:
            return self._local_singletons[name]

        singleton_info = self._raft.state.get_singleton(name)
        if singleton_info:
            if singleton_info.node_id == self._node_id:
                return self._local_singletons.get(name)
            # TODO: Return remote ref
            return None

        return None

    # Actor run loop with replication

    async def _run_actor_loop[M](
        self,
        actor: Actor[M],
        actor_id: ActorId,
        mailbox: Queue[Envelope[M]],
        ctx: Context[M],
        node: SupervisionNode,
    ) -> None:
        """Actor run loop with state replication for entities.

        Extends the base run loop to trigger state replication after
        an entity processes a message.
        """
        current_msg: M | None = None

        try:
            await actor.on_start()

            while self._running:
                try:
                    envelope = await asyncio.wait_for(mailbox.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    continue

                ctx._current_envelope = envelope
                ctx.sender = envelope.sender
                current_msg = envelope.payload

                try:
                    await actor.receive(envelope.payload, ctx)
                    # Success - reset failure tracking
                    if node.restart_record:
                        node.restart_record.record_success()

                    # Trigger state replication for entities
                    await self._maybe_replicate_entity_state(actor, actor_id)

                except Exception as exc:
                    logger.exception(f"Actor {actor_id} failed processing message")
                    await self._handle_failure(node, exc, current_msg)
                finally:
                    ctx._current_envelope = None
                    ctx.sender = None
                    current_msg = None

        except asyncio.CancelledError:
            pass
        except ActorStopSignal:
            pass
        finally:
            try:
                await actor.on_stop()
            except Exception:
                logger.exception(f"Error in on_stop for {actor_id}")
            await self._supervision_tree.unregister(actor_id)
            self._actors.pop(actor_id, None)
            self._mailboxes.pop(actor_id, None)

    async def _maybe_replicate_entity_state(
        self,
        actor: Actor,
        actor_id: ActorId,
    ) -> None:
        """Replicate entity state to backup nodes if applicable.

        Called after an entity successfully processes a message.
        Only replicates if:
        - The actor is an entity (has a name like "entity_type:entity_id")
        - The actor has a get_state() method
        - This node is the primary for the entity
        """
        # Check if this is an entity by looking at its name
        if not actor_id.name or ":" not in actor_id.name:
            return

        # Check if actor has get_state method
        if not hasattr(actor, "get_state"):
            return

        # Parse entity_type and entity_id from name
        parts = actor_id.name.split(":", 1)
        if len(parts) != 2:
            return

        entity_type, entity_id = parts

        # Check if this is a registered entity type
        if entity_type not in self._entity_types:
            return

        # Only replicate if we're the primary
        if not self._router.is_primary(entity_type, entity_id):
            return

        # Get replication config for this entity type
        config = self._entity_replication_configs.get(entity_type)
        if config is None:
            config = ReplicationConfig()  # Default async replication

        # Get state and replicate
        try:
            state = actor.get_state()
            state_bytes = serialize(state)

            if config.is_sync:
                # Synchronous replication - wait for replicas
                success = await self._router.replicate_state(
                    entity_type,
                    entity_id,
                    state_bytes,
                    config=config,
                )
                if not success:
                    logger.warning(
                        f"Sync replication failed for {entity_type}:{entity_id}"
                    )
            else:
                # Asynchronous replication - fire-and-forget
                asyncio.create_task(
                    self._router.replicate_state(
                        entity_type,
                        entity_id,
                        state_bytes,
                        config=config,
                    )
                )
        except Exception as e:
            logger.warning(f"Failed to replicate state for {entity_type}:{entity_id}: {e}")

    # Message handlers

    async def _handle_actor_message(
        self,
        msg: WireMessage,
        from_addr: tuple[str, int],
    ) -> WireMessage | None:
        """Handle an incoming actor message."""
        actor_name = msg.name
        payload = deserialize(msg.payload)

        # Find actor
        ref = await self.lookup(actor_name)
        if ref:
            await ref.send(payload)

        return None

    async def _handle_ask_request(
        self,
        msg: WireMessage,
        from_addr: tuple[str, int],
    ) -> WireMessage | None:
        """Handle an incoming ask request."""
        # Parse name: "actor_name:request_id"
        parts = msg.name.split(":", 1)
        actor_name = parts[0]
        request_id = parts[1] if len(parts) > 1 else ""

        payload = deserialize(msg.payload)

        # Find actor
        ref = await self.lookup(actor_name)
        if ref:
            try:
                response = await ref.ask(payload, timeout=5.0)
                return WireMessage(
                    msg_type=MessageType.ASK_RESPONSE,
                    name=request_id,
                    payload=serialize(response),
                )
            except Exception as e:
                return WireMessage(
                    msg_type=MessageType.ASK_RESPONSE,
                    name=request_id,
                    payload=serialize({"error": str(e)}),
                )

        return WireMessage(
            msg_type=MessageType.ASK_RESPONSE,
            name=request_id,
            payload=serialize({"error": f"Actor '{actor_name}' not found"}),
        )

    async def _handle_entity_message(
        self,
        msg: WireMessage,
        from_addr: tuple[str, int],
    ) -> WireMessage | None:
        """Handle an incoming entity message."""
        # Parse name: "entity_type:entity_id"
        parts = msg.name.split(":", 1)
        entity_type = parts[0]
        entity_id = parts[1] if len(parts) > 1 else ""

        payload = deserialize(msg.payload)

        # Get or create entity
        try:
            ref = await self.get_or_create_entity(entity_type, entity_id)
            await ref.send(payload)
        except Exception as e:
            logger.error(f"Error handling entity message: {e}")

        return None

    async def _handle_entity_ask_request(
        self,
        msg: WireMessage,
        from_addr: tuple[str, int],
    ) -> WireMessage | None:
        """Handle an incoming entity ask request."""
        # Parse name: "entity_type:entity_id:request_id"
        parts = msg.name.split(":", 2)
        entity_type = parts[0]
        entity_id = parts[1] if len(parts) > 1 else ""
        request_id = parts[2] if len(parts) > 2 else ""

        payload = deserialize(msg.payload)

        try:
            ref = await self.get_or_create_entity(entity_type, entity_id)
            response = await ref.ask(payload, timeout=5.0)
            return WireMessage(
                msg_type=MessageType.ENTITY_ASK_RESPONSE,
                name=request_id,
                payload=serialize(response),
            )
        except Exception as e:
            return WireMessage(
                msg_type=MessageType.ENTITY_ASK_RESPONSE,
                name=request_id,
                payload=serialize({"error": str(e)}),
            )

    async def _handle_vote_request(
        self,
        msg: WireMessage,
        from_addr: tuple[str, int],
    ) -> WireMessage | None:
        """Handle Raft VoteRequest."""
        request = deserialize(msg.payload)
        if isinstance(request, VoteRequest):
            response = await self._raft.handle_vote_request(request)
            return WireMessage(
                msg_type=MessageType.VOTE_RESPONSE,
                name=msg.name,
                payload=serialize(response),
            )
        return None

    async def _handle_append_entries(
        self,
        msg: WireMessage,
        from_addr: tuple[str, int],
    ) -> WireMessage | None:
        """Handle Raft AppendEntries."""
        request = deserialize(msg.payload)
        if isinstance(request, AppendEntriesRequest):
            response = await self._raft.handle_append_entries(request)
            return WireMessage(
                msg_type=MessageType.APPEND_ENTRIES_RESPONSE,
                name=msg.name,
                payload=serialize(response),
            )
        return None

    async def _handle_gossip_ping(
        self,
        msg: WireMessage,
        from_addr: tuple[str, int],
    ) -> WireMessage | None:
        """Handle gossip Ping."""
        data = deserialize(msg.payload)
        message = data.get("message")
        updates = data.get("updates", [])

        if isinstance(message, Ping):
            await self._router.gossip.handle_ping(message, from_addr)
        if updates:
            await self._router.gossip.handle_membership_updates(updates)

        return None

    async def _handle_gossip_ping_req(
        self,
        msg: WireMessage,
        from_addr: tuple[str, int],
    ) -> WireMessage | None:
        """Handle gossip PingReq."""
        data = deserialize(msg.payload)
        message = data.get("message")
        updates = data.get("updates", [])

        if isinstance(message, PingReq):
            await self._router.gossip.handle_ping_req(message, from_addr)
        if updates:
            await self._router.gossip.handle_membership_updates(updates)

        return None

    async def _handle_gossip_ack(
        self,
        msg: WireMessage,
        from_addr: tuple[str, int],
    ) -> WireMessage | None:
        """Handle gossip Ack."""
        data = deserialize(msg.payload)
        message = data.get("message")
        updates = data.get("updates", [])

        if isinstance(message, Ack):
            await self._router.gossip.handle_ack(message, from_addr)
        if updates:
            await self._router.gossip.handle_membership_updates(updates)

        return None

    async def _handle_lookup_request(
        self,
        msg: WireMessage,
        from_addr: tuple[str, int],
    ) -> WireMessage | None:
        """Handle actor lookup request."""
        actor_name = msg.name
        request_id = deserialize(msg.payload)

        ref = await self.lookup(actor_name)
        response = {
            "found": ref is not None,
            "node_id": self._node_id if ref else None,
        }

        return WireMessage(
            msg_type=MessageType.LOOKUP_RESPONSE,
            name=request_id,
            payload=serialize(response),
        )

    async def _handle_state_update(
        self,
        msg: WireMessage,
        from_addr: tuple[str, int],
    ) -> WireMessage | None:
        """Handle state update from primary node (for replication)."""
        # Parse name: "entity_type:entity_id"
        parts = msg.name.split(":", 1)
        entity_type = parts[0]
        entity_id = parts[1] if len(parts) > 1 else ""

        state_data = deserialize(msg.payload)

        # Get or create the entity on this replica
        try:
            ref = await self.get_or_create_entity(entity_type, entity_id)

            # If the actor supports state restoration, apply the state
            # Get actor instance from supervision tree
            node = self._supervision_tree.get_node(ref.id)
            if node and hasattr(node.actor_instance, "set_state"):
                node.actor_instance.set_state(state_data)
                logger.debug(f"Applied state update for {entity_type}:{entity_id}")

            # Send ACK
            return WireMessage(
                msg_type=MessageType.STATE_UPDATE_ACK,
                name=msg.name,
                payload=serialize({"success": True}),
            )
        except Exception as e:
            logger.error(f"Failed to apply state update: {e}")
            return WireMessage(
                msg_type=MessageType.STATE_UPDATE_ACK,
                name=msg.name,
                payload=serialize({"success": False, "error": str(e)}),
            )

    # Callbacks

    async def _on_leadership_change(self, is_leader: bool) -> None:
        """Called when Raft leadership changes."""
        if is_leader:
            logger.info(f"Node {self._node_id} became cluster leader")
            # Leader might need to rebalance singletons, etc.
        else:
            logger.info(f"Node {self._node_id} lost leadership")

    async def _on_cluster_state_change(self, state: ClusterState) -> None:
        """Called when cluster state changes."""
        # Update router with current members
        for node_id, info in state.get_all_members().items():
            if info.status == "active" and node_id != self._node_id:
                self._router.add_node(node_id, info.address)
            elif info.status in ("failed", "leaving"):
                self._router.remove_node(node_id)

    async def __aenter__(self) -> "DistributedActorSystem":
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.shutdown()

    def __repr__(self) -> str:
        return (
            f"DistributedActorSystem("
            f"node={self._node_id}, "
            f"leader={self._raft.is_leader}, "
            f"members={len(self._raft.state.get_all_members())})"
        )
