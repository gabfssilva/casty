"""Cluster Actor - 100% actor-based cluster coordination.

The Cluster actor is the main entry point for cluster functionality in Casty.
It manages:
- TransportMux for single-port communication
- SWIM membership via SwimCoordinator child actor
- Gossip-based actor registry via GossipManager child actor
- Remote actor messaging (send/ask)
- Sharded entities via consistent hashing
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import msgpack

from casty import Actor, LocalRef, Context
from casty.scheduler import Scheduler, Schedule
from casty.transport import (
    TransportMux,
    ProtocolType,
    RegisterHandler,
    ConnectTo,
    Send,
    Listening,
    PeerConnected,
    PeerDisconnected,
    Incoming,
)
from casty.swim import (
    SwimCoordinator,
    Subscribe as SwimSubscribe,
    MemberJoined,
    MemberFailed,
    MemberLeft,
)
from casty.gossip import (
    GossipManager,
    GossipConfig,
    Publish,
    Subscribe as GossipSubscribe,
    StateChanged,
    GossipStarted,
)

from .codec import ActorCodec
from .config import ClusterConfig
from .consistency import ShardConsistency, Eventual, DEFAULT_CONSISTENCY
from .hash_ring import HashRing
from .messages import (
    # Wire protocol
    ActorMessage,
    AskRequest,
    AskResponse,
    EntitySend,
    EntityAskRequest,
    EntityAskResponse,
    SingletonSend,
    SingletonAskRequest,
    SingletonAskResponse,
    ShardedTypeRegister,
    ShardedTypeAck,
    MergeRequest,
    MergeState,
    MergeComplete,
    # Internal API
    RemoteSend,
    RemoteAsk,
    LocalRegister,
    LocalUnregister,
    GetRemoteRef,
    GetMembers,
    GetLocalActors,
    Subscribe,
    Unsubscribe,
    # Entity API
    RemoteEntitySend,
    RemoteEntityAsk,
    RegisterShardedType,
    # Singleton API
    RegisterSingleton,
    RemoteSingletonSend,
    RemoteSingletonAsk,
    # Events
    NodeJoined,
    NodeLeft,
    NodeFailed,
    ActorRegistered,
    ActorUnregistered,
    ClusterEvent,
)
from casty.merge import MergeableActor, is_mergeable
from .remote_ref import RemoteRef, RemoteActorError
from .singleton_ref import SingletonRef, SingletonError

if TYPE_CHECKING:
    pass

log = logging.getLogger(__name__)


# =============================================================================
# Message Deserialization
# =============================================================================


def _import_class(fully_qualified_name: str) -> type:
    """Import a class from its fully qualified name."""
    import importlib

    parts = fully_qualified_name.rsplit(".", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid class name: {fully_qualified_name}")

    module_path, class_name = parts

    try:
        module = importlib.import_module(module_path)
        return getattr(module, class_name)
    except (ImportError, AttributeError):
        module_parts = module_path.rsplit(".", 1)
        if len(module_parts) == 2:
            parent_module_path, parent_name = module_parts
            module = importlib.import_module(parent_module_path)
            parent = getattr(module, parent_name)
            return getattr(parent, class_name)
        raise


def _deserialize_recursive(cls: type, data: Any) -> Any:
    """Recursively deserialize data into a dataclass instance."""
    import dataclasses
    from typing import get_type_hints, get_origin, get_args, Union

    if not isinstance(data, dict):
        return data

    if not dataclasses.is_dataclass(cls):
        try:
            return cls(**data)
        except TypeError:
            return data

    try:
        hints = get_type_hints(cls)
    except Exception:
        return cls(**data)

    processed = {}
    for field_name, value in data.items():
        if field_name not in hints:
            processed[field_name] = value
            continue

        field_type = hints[field_name]
        origin = get_origin(field_type)

        if origin is Union:
            args = get_args(field_type)
            for arg in args:
                if arg is type(None):
                    continue
                if dataclasses.is_dataclass(arg) and isinstance(value, dict):
                    try:
                        processed[field_name] = _deserialize_recursive(arg, value)
                        break
                    except (TypeError, ValueError):
                        continue
            else:
                processed[field_name] = value
        elif dataclasses.is_dataclass(field_type) and isinstance(value, dict):
            processed[field_name] = _deserialize_recursive(field_type, value)
        elif origin is list and isinstance(value, list):
            args = get_args(field_type)
            if args and dataclasses.is_dataclass(args[0]):
                processed[field_name] = [
                    _deserialize_recursive(args[0], item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                processed[field_name] = value
        else:
            processed[field_name] = value

    return cls(**processed)


def deserialize_message(payload_type: str, payload_dict: dict) -> Any:
    """Deserialize a message from its type name and dict representation."""
    cls = _import_class(payload_type)
    return _deserialize_recursive(cls, payload_dict)


# =============================================================================
# Internal State Classes
# =============================================================================


@dataclass
class PendingAsk:
    """Tracks an in-flight ask request."""
    request_id: str
    target_actor: str
    target_node: str
    future: asyncio.Future[Any]


@dataclass
class PendingRegistration:
    """Tracks a sharded type registration waiting for acks."""
    entity_type: str
    required_acks: int
    received_acks: set[str]  # node_ids that have acked
    reply_future: asyncio.Future[Any] | None  # The reply_to from the ask envelope


@dataclass(frozen=True, slots=True)
class _RegistrationTimeout:
    """Internal: Timeout for sharded type registration."""
    request_id: str


# =============================================================================
# Cluster Actor
# =============================================================================


class Cluster(Actor):
    """Actor-based cluster coordinator.

    Uses TransportMux for protocol multiplexing over a single TCP port.
    Routes SWIM and GOSSIP protocols to their respective handlers.
    Handles ACTOR protocol for remote actor messaging.

    Example:
        async with ActorSystem() as system:
            cluster = await system.spawn(
                Cluster,
                config=ClusterConfig.development().with_seeds(["192.168.1.10:7946"]),
            )

            # Register a local actor
            worker = await system.spawn(Worker, name="worker-1")
            await cluster.send(LocalRegister("worker-1", worker))

            # Get a remote reference
            remote_ref = await cluster.ask(GetRemoteRef("worker-2", "node-2"))
            await remote_ref.send(DoWork())
    """

    def __init__(self, config: ClusterConfig | None = None) -> None:
        self.config = config or ClusterConfig()
        self.node_id = self.config.node_id or f"node-{uuid.uuid4().hex[:8]}"

        # Child actors
        self._mux: LocalRef | None = None
        self._swim: LocalRef | None = None
        self._gossip: LocalRef | None = None
        self._scheduler: LocalRef | None = None

        # Actor registry
        self._local_actors: dict[str, LocalRef] = {}
        self._actor_locations: dict[str, str] = {}  # actor_name -> node_id

        # Sharding state
        self._hash_ring = HashRing()
        self._sharded_types: dict[str, type[Actor[Any]]] = {}
        self._local_entities: dict[str, dict[str, LocalRef]] = {}
        self._entity_wrappers: dict[str, dict[str, MergeableActor]] = {}  # For merge tracking

        # Singleton state
        self._singleton_types: dict[str, type[Actor[Any]]] = {}  # name -> actor class
        self._local_singletons: dict[str, LocalRef] = {}  # name -> ref (if owned)
        self._singleton_locations: dict[str, str] = {}  # name -> node_id (from gossip)

        # Request tracking
        self._pending_asks: dict[str, PendingAsk] = {}
        self._request_counter = 0

        # Sharded type registration tracking (for consistency levels)
        self._pending_registrations: dict[str, PendingRegistration] = {}

        # Subscribers
        self._subscribers: set[LocalRef[ClusterEvent]] = set()

        # Server state
        self._bound_address: tuple[str, int] | None = None

    async def on_start(self) -> None:
        """Initialize the cluster."""
        ctx = self._ctx

        # Add self to hash ring
        self._hash_ring.add_node(self.node_id)

        # Spawn scheduler for timeouts
        self._scheduler = await ctx.spawn(Scheduler)

        # Spawn TransportMux
        self._mux = await ctx.spawn(
            TransportMux,
            node_id=self.node_id,
            bind_address=(self.config.bind_host, self.config.bind_port),
        )

    async def on_stop(self) -> None:
        """Cleanup on stop."""
        for pending in self._pending_asks.values():
            if not pending.future.done():
                pending.future.cancel()

    async def receive(self, msg: Any, ctx: Context) -> None:
        """Handle cluster messages."""
        match msg:
            # ----- TransportMux Events -----
            case Listening(address):
                await self._handle_listening(address, ctx)

            case PeerConnected(node_id, address):
                await self._handle_peer_connected(node_id, address, ctx)

            case PeerDisconnected(node_id, reason):
                await self._handle_peer_disconnected(node_id, reason, ctx)

            case Incoming(from_node, from_address, data):
                await self._handle_incoming(from_node, from_address, data, ctx)

            # ----- SWIM Events -----
            case MemberJoined(node_id, address):
                await self._handle_member_joined(node_id, address, ctx)

            case MemberFailed(node_id):
                await self._handle_member_failed(node_id, ctx)

            case MemberLeft(node_id):
                await self._handle_member_left(node_id, ctx)

            # ----- Gossip Events -----
            case StateChanged(key, value, version, source):
                await self._handle_state_changed(key, value, ctx)

            case GossipStarted():
                pass  # Gossip is ready

            # ----- Internal API -----
            case RemoteSend(target_actor, target_node, payload):
                await self._handle_remote_send(target_actor, target_node, payload)

            case RemoteAsk(target_actor, target_node, payload):
                await self._handle_remote_ask(target_actor, target_node, payload, ctx)

            case LocalRegister(actor_name, actor_ref):
                await self._handle_local_register(actor_name, actor_ref)

            case LocalUnregister(actor_name):
                await self._handle_local_unregister(actor_name)

            case GetRemoteRef(actor_name, node_id):
                await self._handle_get_remote_ref(actor_name, node_id, ctx)

            case GetMembers():
                ctx.reply(dict(self._actor_locations))

            case GetLocalActors():
                ctx.reply(dict(self._local_actors))

            case Subscribe(subscriber):
                self._subscribers.add(subscriber)

            case Unsubscribe(subscriber):
                self._subscribers.discard(subscriber)

            # ----- Entity API -----
            case RegisterShardedType(entity_type, actor_cls, consistency):
                await self._handle_register_sharded_type(entity_type, actor_cls, consistency, ctx)

            case RemoteEntitySend(entity_type, entity_id, payload):
                await self._handle_entity_send(entity_type, entity_id, payload, ctx)

            case RemoteEntityAsk(entity_type, entity_id, payload):
                await self._handle_entity_ask(entity_type, entity_id, payload, ctx)

            # ----- Singleton API -----
            case RegisterSingleton(singleton_name, actor_cls):
                await self._handle_register_singleton(singleton_name, actor_cls, ctx)

            # ----- Internal Timeouts -----
            case _RegistrationTimeout(request_id):
                await self._handle_registration_timeout(request_id)

            case RemoteSingletonSend(singleton_name, payload):
                await self._handle_singleton_send(singleton_name, payload, ctx)

            case RemoteSingletonAsk(singleton_name, payload):
                await self._handle_singleton_ask(singleton_name, payload, ctx)

    # =========================================================================
    # TransportMux Event Handlers
    # =========================================================================

    async def _handle_listening(
        self, address: tuple[str, int], ctx: Context
    ) -> None:
        """Handle TransportMux bound event."""
        self._bound_address = address
        log.info(f"Cluster {self.node_id} listening on {address[0]}:{address[1]}")

        # Spawn SWIM coordinator
        swim_config = self.config.swim_config.with_bind_address(address[0], address[1])
        self._swim = await ctx.spawn(
            SwimCoordinator,
            node_id=self.node_id,
            config=swim_config,
            transport=self._mux,  # Use TransportMux for connections
        )
        await self._swim.send(SwimSubscribe(ctx.self_ref))

        # Spawn Gossip manager (embedded mode - no TCP, uses TransportMux)
        gossip_config = GossipConfig(gossip_interval=0.5, fanout=3)
        self._gossip = await ctx.spawn(
            GossipManager,
            node_id=self.node_id,
            bind_address=address,
            config=gossip_config,
            transport=self._mux,  # Use TransportMux for sending
        )
        await self._gossip.send(GossipSubscribe("_actors/*", ctx.self_ref))
        await self._gossip.send(GossipSubscribe("_sharded/*", ctx.self_ref))
        await self._gossip.send(GossipSubscribe("_singletons/*", ctx.self_ref))
        await self._gossip.send(GossipSubscribe("_singleton_meta/*", ctx.self_ref))
        await self._gossip.send(GossipSubscribe("__merge/*", ctx.self_ref))  # For three-way merge

        # Register protocol handlers with TransportMux
        await self._mux.send(RegisterHandler(ProtocolType.SWIM, self._swim))
        await self._mux.send(RegisterHandler(ProtocolType.GOSSIP, self._gossip))
        await self._mux.send(RegisterHandler(ProtocolType.ACTOR, ctx.self_ref))

    async def _handle_peer_connected(
        self, node_id: str, address: tuple[str, int], ctx: Context
    ) -> None:
        """Handle peer connected event from TransportMux."""
        log.info(f"Peer connected: {node_id} at {address}")
        # Hash ring is updated when SWIM reports MemberJoined

    async def _handle_peer_disconnected(
        self, node_id: str, reason: str, ctx: Context
    ) -> None:
        """Handle peer disconnected event from TransportMux."""
        log.info(f"Peer disconnected: {node_id} ({reason})")
        # Cleanup is handled when SWIM reports MemberFailed

    async def _handle_incoming(
        self,
        from_node: str,
        from_address: tuple[str, int],
        data: bytes,
        ctx: Context,
    ) -> None:
        """Handle incoming ACTOR protocol data."""
        # Decode actor message
        try:
            msg = ActorCodec.decode(data)
        except Exception as e:
            log.error(f"[{self.node_id}] Exception decoding actor message from {from_node}: {e}")
            return
        if msg is None:
            log.warning(f"[{self.node_id}] Failed to decode actor message from {from_node}")
            return

        await self._process_actor_message(from_node, msg, ctx)

    # =========================================================================
    # SWIM Event Handlers
    # =========================================================================

    async def _handle_member_joined(
        self, node_id: str, address: tuple[str, int], ctx: Context
    ) -> None:
        """Handle SWIM member joined event."""
        if node_id == self.node_id:
            return

        log.info(f"Member joined: {node_id} at {address}")
        self._hash_ring.add_node(node_id)

        # Tell TransportMux to connect
        if self._mux:
            await self._mux.send(ConnectTo(node_id, address))

        await self._broadcast_event(NodeJoined(node_id, address))

    async def _handle_member_failed(self, node_id: str, ctx: Context) -> None:
        """Handle SWIM member failed event."""
        log.warning(f"Member failed: {node_id}")
        self._hash_ring.remove_node(node_id)

        # Remove actor locations for this node
        for actor_name in list(self._actor_locations.keys()):
            if self._actor_locations.get(actor_name) == node_id:
                del self._actor_locations[actor_name]
                await self._broadcast_event(ActorUnregistered(actor_name, node_id))

        # Fail pending asks to this node
        for request_id, pending in list(self._pending_asks.items()):
            if pending.target_node == node_id:
                if not pending.future.done():
                    pending.future.set_exception(
                        RemoteActorError(pending.target_actor, pending.target_node, "Node failed")
                    )
                del self._pending_asks[request_id]

        # Check for singleton failover
        await self._check_singleton_failover(node_id, ctx)

        await self._broadcast_event(NodeFailed(node_id))

    async def _handle_member_left(self, node_id: str, ctx: Context) -> None:
        """Handle SWIM member left event."""
        log.info(f"Member left: {node_id}")
        await self._handle_member_failed(node_id, ctx)
        await self._broadcast_event(NodeLeft(node_id))

    # =========================================================================
    # Gossip Event Handlers
    # =========================================================================

    async def _handle_state_changed(
        self, key: str, value: bytes | None, ctx: Context
    ) -> None:
        """Handle gossip state changed event."""
        log.debug(f"[{self.node_id}] StateChanged: {key}")

        # Handle actor registrations
        if key.startswith("_actors/"):
            actor_name = key[len("_actors/"):]

            if value is None:
                old_node = self._actor_locations.pop(actor_name, None)
                if old_node:
                    await self._broadcast_event(ActorUnregistered(actor_name, old_node))
            else:
                node_id = value.decode("utf-8")
                self._actor_locations[actor_name] = node_id
                await self._broadcast_event(ActorRegistered(actor_name, node_id))
            return

        # Handle sharded type registrations (from gossip - for late-joining nodes)
        if key.startswith("_sharded/"):
            entity_type = key[len("_sharded/"):]

            if value is None:
                # Sharded type unregistered
                self._sharded_types.pop(entity_type, None)
                self._local_entities.pop(entity_type, None)
                log.debug(f"Unregistered sharded type from cluster: {entity_type}")
            else:
                # Import and register the sharded type (if not already known)
                if entity_type not in self._sharded_types:
                    class_fqn = value.decode("utf-8")
                    try:
                        actor_cls = _import_class(class_fqn)
                        self._sharded_types[entity_type] = actor_cls
                        if entity_type not in self._local_entities:
                            self._local_entities[entity_type] = {}
                        log.debug(f"Registered sharded type from gossip: {entity_type} -> {class_fqn}")
                    except Exception as e:
                        log.error(f"Failed to import sharded type {entity_type}: {class_fqn}: {e}")
            return

        # Handle singleton location updates
        if key.startswith("_singletons/"):
            singleton_name = key[len("_singletons/"):]

            if value is None:
                # Singleton unregistered
                self._singleton_locations.pop(singleton_name, None)
                log.debug(f"Singleton unregistered from cluster: {singleton_name}")
            else:
                # Singleton location updated
                owner_node = value.decode("utf-8")
                self._singleton_locations[singleton_name] = owner_node
                log.debug(f"Singleton {singleton_name} located at {owner_node}")
            return

        # Handle singleton metadata (class FQN for remote spawning)
        if key.startswith("_singleton_meta/"):
            singleton_name = key[len("_singleton_meta/"):]

            if value is None:
                # Metadata removed
                self._singleton_types.pop(singleton_name, None)
            else:
                # Import and register the singleton type
                if singleton_name not in self._singleton_types:
                    class_fqn = value.decode("utf-8")
                    try:
                        actor_cls = _import_class(class_fqn)
                        self._singleton_types[singleton_name] = actor_cls
                        log.debug(f"Registered singleton type from cluster: {singleton_name} -> {class_fqn}")

                        # Check if we should own this singleton
                        await self._maybe_spawn_singleton(singleton_name, ctx)
                    except Exception as e:
                        log.error(f"Failed to import singleton type {singleton_name}: {class_fqn}: {e}")
            return

        # Handle merge version updates (for three-way merge conflict detection)
        if key.startswith("__merge/"):
            await self._handle_merge_version_change(key, value, ctx)
            return

    # =========================================================================
    # Entity (Sharding) Handlers
    # =========================================================================

    async def _handle_register_sharded_type(
        self,
        entity_type: str,
        actor_cls: type[Actor[Any]],
        consistency: ShardConsistency | None,
        ctx: Context,
    ) -> None:
        """Register a sharded actor type and propagate to cluster.

        If consistency requires waiting, defers reply until enough
        nodes have acknowledged the registration via wire protocol.
        """
        effective_consistency: ShardConsistency = consistency if consistency is not None else Eventual()

        self._sharded_types[entity_type] = actor_cls
        if entity_type not in self._local_entities:
            self._local_entities[entity_type] = {}
        log.debug(f"Registered sharded type: {entity_type} -> {actor_cls.__name__}")

        # Get fully qualified class name for remote nodes to import
        class_fqn = f"{actor_cls.__module__}.{actor_cls.__qualname__}"

        # Propagate to cluster via Gossip (for discovery by nodes that join later)
        if self._gossip:
            await self._gossip.send(
                Publish(key=f"_sharded/{entity_type}", value=class_fqn.encode("utf-8"))
            )

        # If consistency requires waiting, send wire protocol registration requests
        if effective_consistency.should_wait():
            total_nodes = self._hash_ring.node_count
            required_acks = effective_consistency.required_acks(total_nodes)

            # Generate request ID for tracking
            self._request_counter += 1
            request_id = f"{self.node_id}-sharded-reg-{self._request_counter}"

            # Capture the reply future so we can reply later when acks arrive
            reply_future = ctx._current_envelope.reply_to if ctx._current_envelope else None

            pending = PendingRegistration(
                entity_type=entity_type,
                required_acks=required_acks,
                received_acks={self.node_id},  # Count self
                reply_future=reply_future,
            )
            self._pending_registrations[request_id] = pending

            log.debug(
                f"Waiting for {required_acks} acks for {entity_type} "
                f"(consistency={effective_consistency}, request_id={request_id})"
            )

            # Send wire protocol registration requests to all other nodes
            if self._mux:
                wire_msg = ShardedTypeRegister(
                    request_id=request_id,
                    entity_type=entity_type,
                    actor_cls_fqn=class_fqn,
                )
                data = ActorCodec.encode(wire_msg)

                # Send to all known nodes (from hash ring)
                for node_id in self._hash_ring.nodes:
                    if node_id != self.node_id:
                        log.debug(f"Sending ShardedTypeRegister to {node_id}")
                        await self._mux.send(Send(ProtocolType.ACTOR, node_id, data))

            # Check if we already have enough (single node cluster)
            if len(pending.received_acks) >= required_acks:
                log.debug(f"Already have enough acks for {entity_type}, replying immediately")
                self._pending_registrations.pop(request_id, None)
                ctx.reply(None)
            else:
                # Schedule timeout via scheduler (proper actor pattern)
                if self._scheduler:
                    await self._scheduler.send(
                        Schedule(
                            timeout=self.config.ask_timeout,
                            listener=ctx.self_ref,
                            message=_RegistrationTimeout(request_id),
                        )
                    )
                # Don't reply here - reply will happen when acks arrive or timeout
        else:
            ctx.reply(None)  # Eventual consistency - reply immediately

    async def _handle_registration_timeout(self, request_id: str) -> None:
        """Handle timeout for sharded type registration."""
        pending = self._pending_registrations.pop(request_id, None)
        if pending and pending.reply_future and not pending.reply_future.done():
            log.warning(
                f"Timeout waiting for acks for {pending.entity_type}: "
                f"got {len(pending.received_acks)}/{pending.required_acks}"
            )
            # Reply with None on timeout (registration still happened locally)
            pending.reply_future.set_result(None)

    async def _get_or_create_entity(
        self, entity_type: str, entity_id: str
    ) -> LocalRef:
        """Get or create a local entity actor."""
        entities = self._local_entities.get(entity_type)
        if entities is None:
            raise ValueError(f"Unknown entity type: {entity_type}")

        if entity_id not in entities:
            actor_cls = self._sharded_types.get(entity_type)
            if actor_cls is None:
                raise ValueError(f"Unknown sharded type: {entity_type}")

            # Spawn the actor
            ref = await self._ctx.spawn(actor_cls, entity_id=entity_id)
            entities[entity_id] = ref

            # Check if actor is Mergeable and wrap it
            # Access actor instance via supervision tree
            child_node = self._ctx._supervision_node.children.get(ref.id)
            actor_instance = child_node.actor_instance if child_node else None
            if actor_instance and is_mergeable(actor_instance):
                wrapper = MergeableActor(actor_instance)
                wrapper.set_node_id(self.node_id)

                # Track wrapper
                if entity_type not in self._entity_wrappers:
                    self._entity_wrappers[entity_type] = {}
                self._entity_wrappers[entity_type][entity_id] = wrapper

                # Take initial snapshot
                wrapper.take_snapshot()

                # Publish initial version to gossip
                await self._publish_version(entity_type, entity_id, wrapper)

                log.debug(f"Spawned mergeable entity: {entity_type}:{entity_id}")
            else:
                log.debug(f"Spawned entity: {entity_type}:{entity_id}")

        return entities[entity_id]

    async def _handle_entity_send(
        self, entity_type: str, entity_id: str, payload: Any, ctx: Context
    ) -> None:
        """Handle entity send request with hash ring routing."""
        key = f"{entity_type}:{entity_id}"
        target_node = self._hash_ring.get_node(key)

        if target_node is None:
            log.warning(f"Cannot route entity {key}: empty hash ring")
            return

        if target_node == self.node_id:
            # Local delivery
            try:
                ref = await self._get_or_create_entity(entity_type, entity_id)
                await ref.send(payload)

                # Increment version for mergeable entities
                wrapper = self._entity_wrappers.get(entity_type, {}).get(entity_id)
                if wrapper:
                    wrapper.increment_version()
                    await self._publish_version(entity_type, entity_id, wrapper)
            except Exception as e:
                log.error(f"Failed to deliver to entity {key}: {e}")
        else:
            # Forward to remote node
            payload_type = f"{type(payload).__module__}.{type(payload).__qualname__}"
            payload_bytes = msgpack.packb(payload.__dict__, use_bin_type=True)

            # Include actor class FQN for auto-registration on remote
            actor_cls = self._sharded_types.get(entity_type)
            actor_cls_fqn = f"{actor_cls.__module__}.{actor_cls.__qualname__}" if actor_cls else ""

            wire_msg = EntitySend(
                entity_type=entity_type,
                entity_id=entity_id,
                payload_type=payload_type,
                payload=payload_bytes,
                actor_cls_fqn=actor_cls_fqn,
            )
            data = ActorCodec.encode(wire_msg)
            await self._mux.send(Send(ProtocolType.ACTOR, target_node, data))

    async def _handle_entity_ask(
        self, entity_type: str, entity_id: str, payload: Any, ctx: Context
    ) -> None:
        """Handle entity ask request with hash ring routing."""
        key = f"{entity_type}:{entity_id}"
        target_node = self._hash_ring.get_node(key)

        if target_node is None:
            log.warning(f"Cannot route entity {key}: empty hash ring")
            ctx.reply(None)
            return

        if target_node == self.node_id:
            # Local delivery
            try:
                ref = await self._get_or_create_entity(entity_type, entity_id)
                result = await ref.ask(payload, timeout=self.config.ask_timeout)

                # Increment version for mergeable entities
                wrapper = self._entity_wrappers.get(entity_type, {}).get(entity_id)
                if wrapper:
                    wrapper.increment_version()
                    await self._publish_version(entity_type, entity_id, wrapper)

                ctx.reply(result)
            except Exception as e:
                log.error(f"Failed to ask entity {key}: {e}")
                ctx.reply(None)
        else:
            # Forward to remote node
            self._request_counter += 1
            request_id = f"{self.node_id}-entity-{self._request_counter}"

            loop = asyncio.get_running_loop()
            future: asyncio.Future[Any] = loop.create_future()

            # Capture reply envelope so we can reply asynchronously
            reply_envelope = ctx._current_envelope

            self._pending_asks[request_id] = PendingAsk(
                request_id=request_id,
                target_actor=f"{entity_type}:{entity_id}",
                target_node=target_node,
                future=future,
            )

            payload_type = f"{type(payload).__module__}.{type(payload).__qualname__}"
            payload_bytes = msgpack.packb(payload.__dict__, use_bin_type=True)

            # Include actor class FQN for auto-registration on remote
            actor_cls = self._sharded_types.get(entity_type)
            actor_cls_fqn = f"{actor_cls.__module__}.{actor_cls.__qualname__}" if actor_cls else ""

            wire_msg = EntityAskRequest(
                request_id=request_id,
                entity_type=entity_type,
                entity_id=entity_id,
                payload_type=payload_type,
                payload=payload_bytes,
                actor_cls_fqn=actor_cls_fqn,
            )
            data = ActorCodec.encode(wire_msg)
            await self._mux.send(Send(ProtocolType.ACTOR, target_node, data))

            # Spawn a task to wait for response (non-blocking)
            # This allows receive() to continue processing other messages
            async def wait_for_response() -> None:
                try:
                    result = await asyncio.wait_for(future, timeout=self.config.ask_timeout)
                    if reply_envelope and reply_envelope.reply_to:
                        reply_envelope.reply_to.set_result(result)
                except asyncio.TimeoutError:
                    self._pending_asks.pop(request_id, None)
                    if reply_envelope and reply_envelope.reply_to:
                        reply_envelope.reply_to.set_exception(
                            TimeoutError(f"Entity ask timed out: {entity_type}:{entity_id}")
                        )
                except Exception as e:
                    if reply_envelope and reply_envelope.reply_to and not reply_envelope.reply_to.done():
                        reply_envelope.reply_to.set_exception(e)

            asyncio.create_task(wait_for_response())

    # =========================================================================
    # Internal API Handlers
    # =========================================================================

    async def _handle_remote_send(
        self, target_actor: str, target_node: str, payload: Any
    ) -> None:
        """Handle remote send request."""
        payload_type = f"{type(payload).__module__}.{type(payload).__qualname__}"
        payload_bytes = msgpack.packb(payload.__dict__, use_bin_type=True)

        wire_msg = ActorMessage(
            target_actor=target_actor,
            payload_type=payload_type,
            payload=payload_bytes,
        )
        data = ActorCodec.encode(wire_msg)
        await self._mux.send(Send(ProtocolType.ACTOR, target_node, data))

    async def _handle_remote_ask(
        self, target_actor: str, target_node: str, payload: Any, ctx: Context
    ) -> None:
        """Handle remote ask request."""
        self._request_counter += 1
        request_id = f"{self.node_id}-{self._request_counter}"

        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()

        self._pending_asks[request_id] = PendingAsk(
            request_id=request_id,
            target_actor=target_actor,
            target_node=target_node,
            future=future,
        )

        payload_type = f"{type(payload).__module__}.{type(payload).__qualname__}"
        payload_bytes = msgpack.packb(payload.__dict__, use_bin_type=True)

        wire_msg = AskRequest(
            request_id=request_id,
            target_actor=target_actor,
            payload_type=payload_type,
            payload=payload_bytes,
        )
        data = ActorCodec.encode(wire_msg)
        await self._mux.send(Send(ProtocolType.ACTOR, target_node, data))

        try:
            result = await asyncio.wait_for(future, timeout=self.config.ask_timeout)
            ctx.reply(result)
        except asyncio.TimeoutError:
            self._pending_asks.pop(request_id, None)
            ctx.reply(None)
        except Exception as e:
            self._pending_asks.pop(request_id, None)
            raise e

    async def _handle_local_register(
        self, actor_name: str, actor_ref: LocalRef
    ) -> None:
        """Handle local actor registration."""
        self._local_actors[actor_name] = actor_ref
        self._actor_locations[actor_name] = self.node_id

        if self._gossip:
            await self._gossip.send(
                Publish(key=f"_actors/{actor_name}", value=self.node_id.encode("utf-8"))
            )

        log.info(f"Registered local actor: {actor_name}")
        await self._broadcast_event(ActorRegistered(actor_name, self.node_id))

    async def _handle_local_unregister(self, actor_name: str) -> None:
        """Handle local actor unregistration."""
        self._local_actors.pop(actor_name, None)
        self._actor_locations.pop(actor_name, None)

        if self._gossip:
            from casty.gossip import Delete
            await self._gossip.send(Delete(key=f"_actors/{actor_name}"))

        log.info(f"Unregistered local actor: {actor_name}")
        await self._broadcast_event(ActorUnregistered(actor_name, self.node_id))

    async def _handle_get_remote_ref(
        self, actor_name: str, node_id: str | None, ctx: Context
    ) -> None:
        """Handle get remote reference request."""
        if node_id is None:
            node_id = self._actor_locations.get(actor_name)

        if node_id is None:
            ctx.reply(None)
            return

        remote_ref = RemoteRef(
            actor_name=actor_name,
            node_id=node_id,
            cluster=ctx.self_ref,
        )
        ctx.reply(remote_ref)

    # =========================================================================
    # Actor Message Processing
    # =========================================================================

    async def _process_actor_message(
        self, from_node: str, msg: Any, ctx: Context
    ) -> None:
        """Process an actor protocol message."""
        match msg:
            case ActorMessage(target_actor, payload_type, payload):
                local_ref = self._local_actors.get(target_actor)
                if local_ref:
                    try:
                        payload_dict = msgpack.unpackb(payload, raw=False)
                        message = deserialize_message(payload_type, payload_dict)
                        await local_ref.send(message)
                    except Exception as e:
                        log.error(f"Failed to deliver to {target_actor}: {e}")
                else:
                    log.warning(f"Actor not found: {target_actor}")

            case AskRequest(request_id, target_actor, payload_type, payload):
                local_ref = self._local_actors.get(target_actor)

                if not local_ref:
                    response = AskResponse(
                        request_id=request_id,
                        success=False,
                        payload_type=None,
                        payload=b"Actor not found",
                    )
                    data = ActorCodec.encode(response)
                    await self._mux.send(Send(ProtocolType.ACTOR, from_node, data))
                    return

                try:
                    payload_dict = msgpack.unpackb(payload, raw=False)
                    message = deserialize_message(payload_type, payload_dict)
                    result = await local_ref.ask(message, timeout=self.config.ask_timeout)

                    result_type = f"{type(result).__module__}.{type(result).__qualname__}"
                    if hasattr(result, "__dict__"):
                        result_bytes = msgpack.packb(result.__dict__, use_bin_type=True)
                    else:
                        result_bytes = msgpack.packb(result, use_bin_type=True)

                    response = AskResponse(
                        request_id=request_id,
                        success=True,
                        payload_type=result_type,
                        payload=result_bytes,
                    )
                except Exception as e:
                    response = AskResponse(
                        request_id=request_id,
                        success=False,
                        payload_type=None,
                        payload=str(e).encode("utf-8"),
                    )

                data = ActorCodec.encode(response)
                await self._mux.send(Send(ProtocolType.ACTOR, from_node, data))

            case AskResponse(request_id, success, payload_type, payload):
                pending = self._pending_asks.pop(request_id, None)
                if pending and not pending.future.done():
                    if success:
                        try:
                            result = msgpack.unpackb(payload, raw=False)
                            pending.future.set_result(result)
                        except Exception as e:
                            pending.future.set_exception(e)
                    else:
                        error_msg = payload.decode("utf-8") if payload else "Unknown error"
                        pending.future.set_exception(
                            RemoteActorError(pending.target_actor, pending.target_node, error_msg)
                        )

            case EntitySend(entity_type, entity_id, payload_type, payload, actor_cls_fqn):
                try:
                    # Auto-register sharded type if not present
                    if entity_type not in self._sharded_types and actor_cls_fqn:
                        try:
                            actor_cls = _import_class(actor_cls_fqn)
                            self._sharded_types[entity_type] = actor_cls
                            if entity_type not in self._local_entities:
                                self._local_entities[entity_type] = {}
                            log.debug(f"Auto-registered sharded type: {entity_type} -> {actor_cls_fqn}")
                        except Exception as e:
                            log.error(f"Failed to auto-register sharded type {entity_type}: {e}")

                    ref = await self._get_or_create_entity(entity_type, entity_id)
                    payload_dict = msgpack.unpackb(payload, raw=False)
                    message = deserialize_message(payload_type, payload_dict)
                    await ref.send(message)

                    # Increment version for mergeable entities
                    wrapper = self._entity_wrappers.get(entity_type, {}).get(entity_id)
                    if wrapper:
                        wrapper.increment_version()
                        await self._publish_version(entity_type, entity_id, wrapper)
                except Exception as e:
                    log.error(f"Failed to deliver to entity {entity_type}:{entity_id}: {e}")

            case EntityAskRequest(request_id, entity_type, entity_id, payload_type, payload, actor_cls_fqn):
                log.debug(f"[{self.node_id}] EntityAskRequest {request_id} for {entity_type}:{entity_id} from {from_node}")
                try:
                    # Auto-register sharded type if not present
                    if entity_type not in self._sharded_types and actor_cls_fqn:
                        try:
                            actor_cls = _import_class(actor_cls_fqn)
                            self._sharded_types[entity_type] = actor_cls
                            if entity_type not in self._local_entities:
                                self._local_entities[entity_type] = {}
                            log.debug(f"Auto-registered sharded type: {entity_type} -> {actor_cls_fqn}")
                        except Exception as e:
                            log.error(f"Failed to auto-register sharded type {entity_type}: {e}")

                    ref = await self._get_or_create_entity(entity_type, entity_id)
                    payload_dict = msgpack.unpackb(payload, raw=False)
                    message = deserialize_message(payload_type, payload_dict)
                    result = await ref.ask(message, timeout=self.config.ask_timeout)
                    log.debug(f"[{self.node_id}] Entity responded with: {result}")

                    # Increment version for mergeable entities
                    wrapper = self._entity_wrappers.get(entity_type, {}).get(entity_id)
                    if wrapper:
                        wrapper.increment_version()
                        await self._publish_version(entity_type, entity_id, wrapper)

                    result_type = f"{type(result).__module__}.{type(result).__qualname__}"
                    if hasattr(result, "__dict__"):
                        result_bytes = msgpack.packb(result.__dict__, use_bin_type=True)
                    else:
                        result_bytes = msgpack.packb(result, use_bin_type=True)

                    response = EntityAskResponse(
                        request_id=request_id,
                        success=True,
                        payload_type=result_type,
                        payload=result_bytes,
                    )
                except Exception as e:
                    log.error(f"[{self.node_id}] EntityAskRequest failed: {e}")
                    response = EntityAskResponse(
                        request_id=request_id,
                        success=False,
                        payload_type=None,
                        payload=str(e).encode("utf-8"),
                    )

                log.debug(f"[{self.node_id}] Sending EntityAskResponse {request_id} to {from_node}")
                data = ActorCodec.encode(response)
                await self._mux.send(Send(ProtocolType.ACTOR, from_node, data))

            case EntityAskResponse(request_id, success, payload_type, payload):
                log.debug(f"[{self.node_id}] EntityAskResponse {request_id} success={success}")
                pending = self._pending_asks.pop(request_id, None)
                if pending and not pending.future.done():
                    log.debug(f"[{self.node_id}] Found pending ask {request_id}, resolving future")
                    if success:
                        try:
                            result = msgpack.unpackb(payload, raw=False)
                            pending.future.set_result(result)
                        except Exception as e:
                            pending.future.set_exception(e)
                    else:
                        error_msg = payload.decode("utf-8") if payload else "Unknown error"
                        pending.future.set_exception(
                            RemoteActorError(pending.target_actor, pending.target_node, error_msg)
                        )
                else:
                    log.warning(f"[{self.node_id}] No pending ask for {request_id}, pending_ids={list(self._pending_asks.keys())}")

            case SingletonSend(singleton_name, payload_type, payload, actor_cls_fqn):
                try:
                    # Auto-register singleton type if not present
                    if singleton_name not in self._singleton_types and actor_cls_fqn:
                        try:
                            actor_cls = _import_class(actor_cls_fqn)
                            self._singleton_types[singleton_name] = actor_cls
                            log.debug(f"Auto-registered singleton type: {singleton_name} -> {actor_cls_fqn}")
                        except Exception as e:
                            log.error(f"Failed to auto-register singleton type {singleton_name}: {e}")

                    ref = self._local_singletons.get(singleton_name)
                    if ref:
                        payload_dict = msgpack.unpackb(payload, raw=False)
                        message = deserialize_message(payload_type, payload_dict)
                        await ref.send(message)
                    else:
                        log.warning(f"Singleton {singleton_name} not found locally")
                except Exception as e:
                    log.error(f"Failed to deliver to singleton {singleton_name}: {e}")

            case SingletonAskRequest(request_id, singleton_name, payload_type, payload, actor_cls_fqn):
                log.debug(f"[{self.node_id}] SingletonAskRequest {request_id} for {singleton_name} from {from_node}")
                try:
                    # Auto-register singleton type if not present
                    if singleton_name not in self._singleton_types and actor_cls_fqn:
                        try:
                            actor_cls = _import_class(actor_cls_fqn)
                            self._singleton_types[singleton_name] = actor_cls
                            log.debug(f"Auto-registered singleton type: {singleton_name} -> {actor_cls_fqn}")
                        except Exception as e:
                            log.error(f"Failed to auto-register singleton type {singleton_name}: {e}")

                    ref = self._local_singletons.get(singleton_name)
                    if not ref:
                        response = SingletonAskResponse(
                            request_id=request_id,
                            success=False,
                            payload_type=None,
                            payload=b"Singleton not found",
                        )
                    else:
                        payload_dict = msgpack.unpackb(payload, raw=False)
                        message = deserialize_message(payload_type, payload_dict)
                        result = await ref.ask(message, timeout=self.config.ask_timeout)
                        log.debug(f"[{self.node_id}] Singleton responded with: {result}")

                        result_type = f"{type(result).__module__}.{type(result).__qualname__}"
                        if hasattr(result, "__dict__"):
                            result_bytes = msgpack.packb(result.__dict__, use_bin_type=True)
                        else:
                            result_bytes = msgpack.packb(result, use_bin_type=True)

                        response = SingletonAskResponse(
                            request_id=request_id,
                            success=True,
                            payload_type=result_type,
                            payload=result_bytes,
                        )
                except Exception as e:
                    log.error(f"[{self.node_id}] SingletonAskRequest failed: {e}")
                    response = SingletonAskResponse(
                        request_id=request_id,
                        success=False,
                        payload_type=None,
                        payload=str(e).encode("utf-8"),
                    )

                log.debug(f"[{self.node_id}] Sending SingletonAskResponse {request_id} to {from_node}")
                data = ActorCodec.encode(response)
                await self._mux.send(Send(ProtocolType.ACTOR, from_node, data))

            case SingletonAskResponse(request_id, success, payload_type, payload):
                log.debug(f"[{self.node_id}] SingletonAskResponse {request_id} success={success}")
                pending = self._pending_asks.pop(request_id, None)
                if pending and not pending.future.done():
                    log.debug(f"[{self.node_id}] Found pending singleton ask {request_id}, resolving future")
                    if success:
                        try:
                            result = msgpack.unpackb(payload, raw=False)
                            pending.future.set_result(result)
                        except Exception as e:
                            pending.future.set_exception(e)
                    else:
                        error_msg = payload.decode("utf-8") if payload else "Unknown error"
                        pending.future.set_exception(
                            SingletonError(pending.target_actor, error_msg)
                        )
                else:
                    log.warning(f"[{self.node_id}] No pending singleton ask for {request_id}")

            case ShardedTypeRegister(request_id, entity_type, actor_cls_fqn):
                # Handle sharded type registration request from another node
                log.debug(f"[{self.node_id}] ShardedTypeRegister {entity_type} from {from_node}")
                success = True
                error: str | None = None

                try:
                    # Import and register the sharded type if not already known
                    if entity_type not in self._sharded_types:
                        actor_cls = _import_class(actor_cls_fqn)
                        self._sharded_types[entity_type] = actor_cls
                        if entity_type not in self._local_entities:
                            self._local_entities[entity_type] = {}
                        log.debug(f"Registered sharded type from wire: {entity_type} -> {actor_cls_fqn}")
                except Exception as e:
                    log.error(f"Failed to register sharded type {entity_type}: {e}")
                    success = False
                    error = str(e)

                # Send ack back to requester
                ack = ShardedTypeAck(
                    request_id=request_id,
                    entity_type=entity_type,
                    success=success,
                    error=error,
                )
                log.debug(f"[{self.node_id}] Sending ShardedTypeAck to {from_node}: {ack}")
                data = ActorCodec.encode(ack)
                if self._mux:
                    await self._mux.send(Send(ProtocolType.ACTOR, from_node, data))
                else:
                    log.error(f"[{self.node_id}] Cannot send ack: _mux is None")

            case ShardedTypeAck(request_id, entity_type, success, error):
                # Handle sharded type registration ack from another node
                log.debug(f"[{self.node_id}] ShardedTypeAck {entity_type} from {from_node} success={success}")
                pending = self._pending_registrations.get(request_id)
                if pending:
                    if success:
                        pending.received_acks.add(from_node)
                        log.debug(
                            f"Received wire ack for {entity_type} from {from_node} "
                            f"({len(pending.received_acks)}/{pending.required_acks})"
                        )

                        # Check if we have enough acks
                        if len(pending.received_acks) >= pending.required_acks:
                            self._pending_registrations.pop(request_id, None)
                            if pending.reply_future and not pending.reply_future.done():
                                pending.reply_future.set_result(None)
                                log.debug(f"Sharded type {entity_type} registration complete")
                    else:
                        log.warning(f"ShardedTypeAck failed from {from_node}: {error}")
                else:
                    log.debug(f"No pending registration for request_id={request_id}")

            # Three-way merge messages
            case MergeRequest(request_id, entity_type, entity_id, their_version, their_base_version):
                await self._handle_merge_request(
                    request_id, entity_type, entity_id,
                    their_version, their_base_version,
                    from_node
                )

            case MergeState(request_id, entity_type, entity_id, version, base_version, state, base_state):
                await self._handle_merge_state(
                    request_id, entity_type, entity_id,
                    version, base_version, state, base_state,
                    from_node
                )

            case MergeComplete(request_id, entity_type, entity_id, new_version):
                await self._handle_merge_complete(
                    request_id, entity_type, entity_id, new_version, from_node
                )

            case _:
                log.warning(f"[{self.node_id}] Unhandled actor message type: {type(msg).__name__}: {msg}")

    # =========================================================================
    # Singleton Handlers
    # =========================================================================

    async def _handle_register_singleton(
        self,
        singleton_name: str,
        actor_cls: type[Actor[Any]],
        ctx: Context,
    ) -> None:
        """Register a singleton actor type.

        Determines ownership via consistent hashing. If we own it, spawns
        the actor locally. Otherwise, publishes metadata for the owner to spawn.
        """
        self._singleton_types[singleton_name] = actor_cls
        class_fqn = f"{actor_cls.__module__}.{actor_cls.__qualname__}"

        # Determine owner via consistent hashing
        owner = self._hash_ring.get_node(f"_singleton:{singleton_name}")

        if owner == self.node_id:
            # We own this singleton - spawn locally
            ref = await ctx.spawn(actor_cls)
            self._local_singletons[singleton_name] = ref
            self._singleton_locations[singleton_name] = self.node_id

            # Publish our ownership to the cluster
            if self._gossip:
                await self._gossip.send(
                    Publish(key=f"_singletons/{singleton_name}", value=self.node_id.encode())
                )
            log.info(f"Spawned singleton {singleton_name} locally")
        else:
            # Remote owner - publish metadata so they can spawn
            if self._gossip:
                await self._gossip.send(
                    Publish(key=f"_singleton_meta/{singleton_name}", value=class_fqn.encode())
                )
            log.debug(f"Published singleton metadata for {singleton_name}, owner: {owner}")

        # Return SingletonRef
        singleton_ref: SingletonRef[Any] = SingletonRef(singleton_name, ctx.self_ref)
        ctx.reply(singleton_ref)

    async def _maybe_spawn_singleton(self, singleton_name: str, ctx: Context) -> None:
        """Check if we should spawn a singleton and do so if we're the owner."""
        # Don't spawn if already owned
        if singleton_name in self._singleton_locations:
            return

        # Check if we're the owner
        owner = self._hash_ring.get_node(f"_singleton:{singleton_name}")
        if owner != self.node_id:
            return

        # Spawn the singleton
        actor_cls = self._singleton_types.get(singleton_name)
        if actor_cls and singleton_name not in self._local_singletons:
            ref = await ctx.spawn(actor_cls)
            self._local_singletons[singleton_name] = ref
            self._singleton_locations[singleton_name] = self.node_id

            # Publish our ownership
            if self._gossip:
                await self._gossip.send(
                    Publish(key=f"_singletons/{singleton_name}", value=self.node_id.encode())
                )
            log.info(f"Spawned singleton {singleton_name} (owner via hash ring)")

    async def _handle_singleton_send(
        self,
        singleton_name: str,
        payload: Any,
        ctx: Context,
    ) -> None:
        """Route send to singleton owner."""
        owner = self._singleton_locations.get(singleton_name)

        if owner is None:
            # Use hash ring to determine owner
            owner = self._hash_ring.get_node(f"_singleton:{singleton_name}")

        if owner is None:
            log.warning(f"Cannot route singleton {singleton_name}: no owner found")
            return

        if owner == self.node_id:
            # Local delivery
            ref = self._local_singletons.get(singleton_name)
            if ref:
                await ref.send(payload)
            else:
                log.warning(f"Singleton {singleton_name} not found locally")
        else:
            # Forward to remote owner
            payload_type = f"{type(payload).__module__}.{type(payload).__qualname__}"
            payload_bytes = msgpack.packb(payload.__dict__, use_bin_type=True)
            actor_cls = self._singleton_types.get(singleton_name)
            actor_cls_fqn = f"{actor_cls.__module__}.{actor_cls.__qualname__}" if actor_cls else ""

            wire_msg = SingletonSend(
                singleton_name=singleton_name,
                payload_type=payload_type,
                payload=payload_bytes,
                actor_cls_fqn=actor_cls_fqn,
            )
            data = ActorCodec.encode(wire_msg)
            if self._mux:
                await self._mux.send(Send(ProtocolType.ACTOR, owner, data))

    async def _handle_singleton_ask(
        self,
        singleton_name: str,
        payload: Any,
        ctx: Context,
    ) -> None:
        """Route ask to singleton owner."""
        owner = self._singleton_locations.get(singleton_name)

        if owner is None:
            owner = self._hash_ring.get_node(f"_singleton:{singleton_name}")

        if owner is None:
            log.warning(f"Cannot route singleton {singleton_name}: no owner found")
            ctx.reply(None)
            return

        if owner == self.node_id:
            # Local delivery
            ref = self._local_singletons.get(singleton_name)
            if ref:
                result = await ref.ask(payload, timeout=self.config.ask_timeout)
                ctx.reply(result)
            else:
                log.warning(f"Singleton {singleton_name} not found locally")
                ctx.reply(None)
        else:
            # Forward to remote owner
            self._request_counter += 1
            request_id = f"{self.node_id}-singleton-{self._request_counter}"

            loop = asyncio.get_running_loop()
            future: asyncio.Future[Any] = loop.create_future()

            # Capture reply envelope
            reply_envelope = ctx._current_envelope

            self._pending_asks[request_id] = PendingAsk(
                request_id=request_id,
                target_actor=f"singleton:{singleton_name}",
                target_node=owner,
                future=future,
            )

            payload_type = f"{type(payload).__module__}.{type(payload).__qualname__}"
            payload_bytes = msgpack.packb(payload.__dict__, use_bin_type=True)
            actor_cls = self._singleton_types.get(singleton_name)
            actor_cls_fqn = f"{actor_cls.__module__}.{actor_cls.__qualname__}" if actor_cls else ""

            wire_msg = SingletonAskRequest(
                request_id=request_id,
                singleton_name=singleton_name,
                payload_type=payload_type,
                payload=payload_bytes,
                actor_cls_fqn=actor_cls_fqn,
            )
            data = ActorCodec.encode(wire_msg)
            if self._mux:
                await self._mux.send(Send(ProtocolType.ACTOR, owner, data))

            # Wait for response asynchronously
            async def wait_for_response() -> None:
                try:
                    result = await asyncio.wait_for(future, timeout=self.config.ask_timeout)
                    if reply_envelope and reply_envelope.reply_to:
                        reply_envelope.reply_to.set_result(result)
                except asyncio.TimeoutError:
                    self._pending_asks.pop(request_id, None)
                    if reply_envelope and reply_envelope.reply_to:
                        reply_envelope.reply_to.set_exception(
                            TimeoutError(f"Singleton ask timed out: {singleton_name}")
                        )
                except Exception as e:
                    if reply_envelope and reply_envelope.reply_to and not reply_envelope.reply_to.done():
                        reply_envelope.reply_to.set_exception(e)

            asyncio.create_task(wait_for_response())

    async def _check_singleton_failover(self, failed_node: str, ctx: Context) -> None:
        """Check if we should take over any singletons from a failed node."""
        for singleton_name in list(self._singleton_locations.keys()):
            if self._singleton_locations.get(singleton_name) == failed_node:
                # This singleton needs failover
                # Remove stale location
                del self._singleton_locations[singleton_name]

                # Check if we're the new owner
                new_owner = self._hash_ring.get_node(f"_singleton:{singleton_name}")

                if new_owner == self.node_id:
                    # We're the new owner - spawn locally
                    actor_cls = self._singleton_types.get(singleton_name)
                    if actor_cls:
                        ref = await ctx.spawn(actor_cls)
                        self._local_singletons[singleton_name] = ref
                        self._singleton_locations[singleton_name] = self.node_id

                        # Publish new location
                        if self._gossip:
                            await self._gossip.send(
                                Publish(key=f"_singletons/{singleton_name}", value=self.node_id.encode())
                            )
                        log.info(f"Singleton {singleton_name} failed over to this node")
                    else:
                        log.warning(f"Cannot failover singleton {singleton_name}: class not registered")

    # =========================================================================
    # Three-Way Merge Handlers
    # =========================================================================

    async def _publish_version(
        self, entity_type: str, entity_id: str, wrapper: MergeableActor
    ) -> None:
        """Publish entity version to gossip for conflict detection."""
        if not self._gossip:
            return

        key = f"__merge/{entity_type}:{entity_id}"
        value_dict = {
            "version": wrapper.version,
            "node_id": wrapper.node_id,
        }
        value_bytes = msgpack.packb(value_dict, use_bin_type=True)

        await self._gossip.send(
            Publish(key=key, value=value_bytes, ttl=None),
        )

    async def _handle_merge_version_change(
        self,
        key: str,
        value: bytes | None,
        ctx: Context,
    ) -> None:
        """Handle version update from gossip for three-way merge detection."""
        if value is None:
            return  # Deletion - ignore

        # Parse key: "__merge/entity_type:entity_id"
        entity_key = key[9:]  # Strip "__merge/"
        parts = entity_key.split(":", 1)
        if len(parts) != 2:
            return

        entity_type, entity_id = parts

        # Parse value
        value_dict = msgpack.unpackb(value, raw=False)
        remote_version = value_dict.get("version", 0)
        remote_node_id = value_dict.get("node_id", "")

        # Skip our own updates
        if remote_node_id == self.node_id:
            return

        # Check if we have this entity locally
        wrapper = self._entity_wrappers.get(entity_type, {}).get(entity_id)
        if not wrapper:
            return  # We don't have it, no conflict

        local_version = wrapper.version
        local_node_id = wrapper.node_id

        # Detect conflict: same version, different nodes
        if local_version == remote_version and local_node_id != remote_node_id:
            log.info(
                f"Conflict detected for {entity_type}:{entity_id} "
                f"(local={local_node_id}:v{local_version}, remote={remote_node_id}:v{remote_version})"
            )

            # Deterministic tiebreaker: lower node_id initiates
            if self.node_id < remote_node_id:
                await self._initiate_merge(entity_type, entity_id, remote_node_id, ctx)

    async def _initiate_merge(
        self,
        entity_type: str,
        entity_id: str,
        peer_node: str,
        ctx: Context,
    ) -> None:
        """Initiate three-way merge with peer node."""
        wrapper = self._entity_wrappers.get(entity_type, {}).get(entity_id)
        if not wrapper:
            log.warning(f"Cannot initiate merge: no wrapper for {entity_type}:{entity_id}")
            return

        self._request_counter += 1
        request_id = f"{self.node_id}-merge-{self._request_counter}"

        version, base_version, _, _ = wrapper.get_merge_info()

        msg = MergeRequest(
            request_id=request_id,
            entity_type=entity_type,
            entity_id=entity_id,
            my_version=version,
            my_base_version=base_version,
        )

        if self._mux:
            data = ActorCodec.encode(msg)
            await self._mux.send(Send(ProtocolType.ACTOR, peer_node, data))
            log.debug(f"Sent MergeRequest {request_id} to {peer_node}")

    async def _handle_merge_request(
        self,
        request_id: str,
        entity_type: str,
        entity_id: str,
        _their_version: int,
        _their_base_version: int,
        from_node: str,
    ) -> None:
        """Handle MergeRequest - send our state back."""
        wrapper = self._entity_wrappers.get(entity_type, {}).get(entity_id)
        if not wrapper:
            log.warning(f"MergeRequest for unknown entity: {entity_type}:{entity_id}")
            return

        version, base_version, state, base_state = wrapper.get_merge_info()

        msg = MergeState(
            request_id=request_id,
            entity_type=entity_type,
            entity_id=entity_id,
            version=version,
            base_version=base_version,
            state=msgpack.packb(state, use_bin_type=True),
            base_state=msgpack.packb(base_state, use_bin_type=True) if base_state else b"",
        )

        if self._mux:
            data = ActorCodec.encode(msg)
            await self._mux.send(Send(ProtocolType.ACTOR, from_node, data))
            log.debug(f"Sent MergeState {request_id} to {from_node}")

    async def _handle_merge_state(
        self,
        request_id: str,
        entity_type: str,
        entity_id: str,
        their_version: int,
        _their_base_version: int,
        state_bytes: bytes,
        base_state_bytes: bytes,
        from_node: str,
    ) -> None:
        """Handle MergeState - execute merge locally."""
        wrapper = self._entity_wrappers.get(entity_type, {}).get(entity_id)
        if not wrapper:
            log.warning(f"MergeState for unknown entity: {entity_type}:{entity_id}")
            return

        their_state = msgpack.unpackb(state_bytes, raw=False)
        their_base_state = msgpack.unpackb(base_state_bytes, raw=False) if base_state_bytes else None

        # Execute three-way merge
        wrapper.execute_merge(their_base_state, their_state, their_version)

        # Take snapshot after merge
        wrapper.take_snapshot()

        # Publish new version to gossip
        await self._publish_version(entity_type, entity_id, wrapper)

        # Send completion acknowledgment
        msg = MergeComplete(
            request_id=request_id,
            entity_type=entity_type,
            entity_id=entity_id,
            new_version=wrapper.version,
        )

        if self._mux:
            data = ActorCodec.encode(msg)
            await self._mux.send(Send(ProtocolType.ACTOR, from_node, data))

        log.info(f"Merge complete for {entity_type}:{entity_id}, new version={wrapper.version}")

    async def _handle_merge_complete(
        self,
        request_id: str,
        entity_type: str,
        entity_id: str,
        _new_version: int,
        _from_node: str,
    ) -> None:
        """Handle MergeComplete - peer has merged."""
        # Log completion
        log.debug(f"Merge protocol complete: {request_id}, entity={entity_type}:{entity_id}")

    # =========================================================================
    # Helpers
    # =========================================================================

    async def _broadcast_event(self, event: ClusterEvent) -> None:
        """Broadcast event to all subscribers."""
        for subscriber in self._subscribers:
            await subscriber.send(event)
