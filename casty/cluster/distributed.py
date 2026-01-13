"""Distributed actor system with cluster support and full Raft consensus."""

from __future__ import annotations

import asyncio
import logging
import random
import ssl
import time
from asyncio import Queue
from dataclasses import dataclass, field
from typing import Any, Literal
from uuid import uuid4

from ..actor import Actor, ActorId, ActorRef, Context, Envelope, ShardedRef
from ..system import ActorSystem
from .mixins import (
    MessagingMixin,
    PeerMixin,
    SingletonMixin,
)
from .multiraft import MultiRaftManager
from .multiraft.manager import SYSTEM_GROUP_ID
from .persistence import (
    BackupStateManager,
    ReplicationConfig,
    restore_actor_state,
    serialize_actor_state,
)
from .raft_types import (
    EntityStateDelete,
    EntityStateUpdate,
    RaftLog,
    RegisterShardedEntityCommand,
    ShardAllocationCommand,
    ShardedEntityConfig,
    ShardedMessage,
    SingletonEntry,
)
from .serialize import _import_type, deserialize, get_type_name, serialize
from .transport import MessageType, Transport, WireMessage

log = logging.getLogger(__name__)

# Election timing constants (re-exported for backwards compatibility)
HEARTBEAT_INTERVAL = 0.3
ELECTION_TIMEOUT_MIN = 1.0
ELECTION_TIMEOUT_MAX = 2.0


@dataclass
class ClusterState:
    """State for distributed cluster mode with full Raft consensus."""

    host: str
    port: int
    seeds: list[str]
    expected_size: int = 3
    # Election timing (configurable)
    heartbeat_interval: float = HEARTBEAT_INTERVAL
    election_timeout_min: float = ELECTION_TIMEOUT_MIN
    election_timeout_max: float = ELECTION_TIMEOUT_MAX
    # Registries
    registry: dict[str, ActorRef[Any]] = field(default_factory=dict)
    peers: dict[tuple[str, int], Transport] = field(default_factory=dict)
    type_registry: dict[str, type] = field(default_factory=dict)
    pending_lookups: dict[str, asyncio.Future[bool]] = field(default_factory=dict)
    pending_asks: dict[str, asyncio.Future[Any]] = field(default_factory=dict)
    # Singleton registries
    singleton_registry: dict[str, SingletonEntry] = field(default_factory=dict)  # Global, replicated via Raft
    singleton_actor_classes: dict[str, type] = field(default_factory=dict)  # Local, for auto-recreate
    singleton_local_refs: dict[str, ActorRef[Any]] = field(default_factory=dict)  # Local refs to singletons hosted here
    # Election state (for compatibility - actual state in MultiRaftManager)
    node_id: str = field(default_factory=lambda: str(uuid4()))
    known_nodes: dict[str, Transport] = field(default_factory=dict)
    peer_addresses: dict[str, tuple[str, int]] = field(default_factory=dict)
    # Legacy fields (now delegated to MultiRaftManager, kept for API compatibility)
    term: int = 0
    voted_for: str | None = None
    votes_received: set[str] = field(default_factory=set)
    state: Literal["follower", "candidate", "leader"] = "follower"
    leader_id: str | None = None
    last_heartbeat: float = field(default_factory=time.time)
    election_timeout: float = 0.0  # Set in __post_init__
    last_heartbeat_sent: float = 0.0
    election_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    # Raft log replication state (legacy, kept for compatibility)
    raft_log: RaftLog = field(default_factory=RaftLog)
    next_index: dict[str, int] = field(default_factory=dict)  # Leader: next log index to send to each follower
    match_index: dict[str, int] = field(default_factory=dict)  # Leader: highest log index replicated on each follower

    def __post_init__(self) -> None:
        """Initialize election timeout with random jitter."""
        if self.election_timeout == 0.0:
            self.election_timeout = random.uniform(self.election_timeout_min, self.election_timeout_max)

    @property
    def quorum_size(self) -> int:
        """Minimum nodes needed for quorum (majority)."""
        return (self.expected_size // 2) + 1


class DistributedActorSystem(
    SingletonMixin,
    PeerMixin,
    MessagingMixin,
    ActorSystem,
):
    """Actor system with distributed cluster support and full Raft consensus.

    Uses MultiRaftManager internally for Raft operations while maintaining
    backwards-compatible API.
    """

    def __init__(
        self,
        host: str,
        port: int,
        seeds: list[str] | None = None,
        expected_cluster_size: int = 3,
        ssl_context: ssl.SSLContext | None = None,
        *,
        heartbeat_interval: float = HEARTBEAT_INTERVAL,
        election_timeout_min: float = ELECTION_TIMEOUT_MIN,
        election_timeout_max: float = ELECTION_TIMEOUT_MAX,
        persistence_dir: str | None = None,
        node_id: str | None = None,
        replication: ReplicationConfig | None = None,
    ) -> None:
        super().__init__()
        self._cluster = ClusterState(
            host,
            port,
            seeds or [],
            expected_size=expected_cluster_size,
            heartbeat_interval=heartbeat_interval,
            election_timeout_min=election_timeout_min,
            election_timeout_max=election_timeout_max,
        )
        # Override node_id if provided (for restart scenarios)
        if node_id:
            self._cluster.node_id = node_id

        self._ssl_context = ssl_context
        self._server: asyncio.Server | None = None
        self._election_task: asyncio.Task[None] | None = None
        self._receive_tasks: set[asyncio.Task[None]] = set()
        self._serve_task: asyncio.Task[None] | None = None
        self._persistence_dir = persistence_dir

        # Initialize MultiRaftManager
        self._raft_manager = MultiRaftManager(
            node_id=self._cluster.node_id,
            expected_size=expected_cluster_size,
            heartbeat_interval=heartbeat_interval,
            election_timeout_min=election_timeout_min,
            election_timeout_max=election_timeout_max,
            persistence_dir=persistence_dir,
        )

        # Set singleton apply callback for system group
        self._raft_manager.set_singleton_apply_callback(
            self._apply_singleton_command,
            SYSTEM_GROUP_ID,
        )

        # Initialize snapshot state
        self._current_snapshot = None
        self._snapshot_callback = None
        self._restore_callback = None

        # Initialize sharding state
        self._sharded_entities: dict[str, ShardedEntityConfig] = {}
        self._shard_allocations: dict[tuple[str, int], str] = {}
        self._entity_actors: dict[str, ActorRef[Any]] = {}
        self._entity_actor_instances: dict[str, Actor[Any]] = {}  # For state access
        self._pending_shard_asks: dict[str, asyncio.Future[Any]] = {}

        # Initialize state replication
        self._replication_config = replication or ReplicationConfig()
        self._backup_manager = BackupStateManager(
            max_entries=self._replication_config.backup_max_entries,
            max_memory_mb=self._replication_config.backup_max_memory_mb,
        )
        self._state_versions: dict[str, int] = {}  # entity_key -> version

        # Set sharding apply callback for system group
        self._raft_manager.set_apply_callback(
            self._apply_sharding_command,
            SYSTEM_GROUP_ID,
        )

    @property
    def node_id(self) -> str:
        """This node's unique ID."""
        return self._cluster.node_id

    @property
    def is_leader(self) -> bool:
        """Check if this node is the leader (of system group)."""
        return self._raft_manager.system_group.is_leader

    @property
    def leader_id(self) -> str | None:
        """Get the current leader's ID (of system group)."""
        return self._raft_manager.system_group.leader_id

    @property
    def commit_index(self) -> int:
        """Get current commit index (of system group)."""
        return self._raft_manager.system_group.commit_index

    @property
    def last_applied(self) -> int:
        """Get last applied index (of system group)."""
        return self._raft_manager.system_group.last_applied

    def register_type(self, cls: type) -> None:
        """Register a message type for remote serialization."""
        type_name = get_type_name(cls)
        self._cluster.type_registry[type_name] = cls

    # ---- Multi-Raft API ----

    async def create_raft_group(self, group_id: int) -> None:
        """Create a new Raft group.

        Args:
            group_id: Unique identifier for the group (0 is reserved for system).
        """
        if group_id == SYSTEM_GROUP_ID:
            raise ValueError("Group ID 0 is reserved for the system group")
        await self._raft_manager.create_group(group_id)

    def get_leader(self, group: int = SYSTEM_GROUP_ID) -> str | None:
        """Get the leader ID for a specific Raft group."""
        raft_group = self._raft_manager.get_group(group)
        if raft_group:
            return raft_group.leader_id
        return None

    def is_group_leader(self, group: int = SYSTEM_GROUP_ID) -> bool:
        """Check if this node is the leader of a specific group."""
        raft_group = self._raft_manager.get_group(group)
        if raft_group:
            return raft_group.is_leader
        return False

    # ---- Spawn ----

    async def spawn[M](
        self,
        actor_cls: type[Actor[M]],
        *,
        name: str | None = None,
        singleton: bool = False,
        shards: int | None = None,
        **kwargs: Any,
    ) -> ActorRef[M] | ShardedRef[M]:
        """Spawn actor and register if named.

        Args:
            actor_cls: The actor class to instantiate.
            name: Optional name for the actor.
            singleton: If True, creates a cluster-wide singleton.
                      If the singleton already exists, returns a RemoteRef.
            shards: If provided, creates a sharded actor type with N shards.
                   Returns ShardedRef instead of ActorRef.
            **kwargs: Constructor arguments for the actor.
        """
        if shards is not None:
            if name is None:
                raise ValueError("Sharded actors must have a name")
            return await self._spawn_sharded(actor_cls, name, shards, kwargs)

        if singleton:
            if name is None:
                raise ValueError("Singleton actors must have a name")
            return await self._spawn_singleton(actor_cls, name, kwargs)

        ref = await super().spawn(actor_cls, name=name, **kwargs)
        if name:
            self._cluster.registry[name] = ref
        return ref

    # ---- Log Replication (delegated to MultiRaftManager) ----

    async def append_command(self, command: Any, *, group: int = SYSTEM_GROUP_ID) -> bool:
        """Append a new command to the log (leader only).

        Args:
            command: The command to append.
            group: The Raft group ID (default: system group).

        Returns True if the command was successfully replicated to a majority.
        """
        return await self._raft_manager.append_command(
            command,
            group_id=group,
            known_nodes=self._cluster.known_nodes,
            send_callback=self._send_to_peer,
        )

    async def _send_to_peer(
        self, peer_id: str, transport: Transport, msg: WireMessage
    ) -> None:
        """Send a message to a peer."""
        try:
            await transport.send(msg)
        except Exception as e:
            log.debug(f"Failed to send to {peer_id[:8]}: {e}")

    def set_apply_callback(
        self, callback, group: int = SYSTEM_GROUP_ID
    ) -> None:
        """Set callback for applying committed commands to state machine."""
        self._raft_manager.set_apply_callback(callback, group)

    # ---- Snapshot (delegated to MultiRaftManager) ----

    def set_snapshot_callbacks(
        self,
        snapshot_cb,
        restore_cb,
        group: int = SYSTEM_GROUP_ID,
    ) -> None:
        """Set callbacks for creating and restoring snapshots."""
        self._snapshot_callback = snapshot_cb
        self._restore_callback = restore_cb
        self._raft_manager.set_snapshot_callbacks(snapshot_cb, restore_cb, group)

    async def create_snapshot(self, group: int = SYSTEM_GROUP_ID):
        """Create a snapshot of current state machine state."""
        return await self._raft_manager.create_snapshot(group)

    # ---- Server and Receive Loop ----

    async def serve(self) -> None:
        """Start TCP server, connect to seeds, and run consensus.

        This method blocks until shutdown() is called.
        """
        # Load persisted state if available
        await self._raft_manager.load_persisted_state()
        await self._raft_manager.load_persisted_log()
        await self._raft_manager.load_persisted_snapshots()

        # Sync legacy state from manager for compatibility
        self._sync_state_from_manager()

        # Connect to seeds first
        for seed in self._cluster.seeds:
            try:
                host, port_str = seed.split(":")
                await self._connect_peer(host, int(port_str))
                log.info(f"Connected to seed {seed}")
            except Exception as e:
                log.warning(f"Failed to connect to seed {seed}: {e}")

        # Start server to accept connections
        self._server = await asyncio.start_server(
            self._handle_connection,
            self._cluster.host,
            self._cluster.port,
            ssl=self._ssl_context,
        )
        log.info(f"Node {self._cluster.node_id[:8]} listening on {self._cluster.host}:{self._cluster.port}")

        # Start election loop
        self._election_task = asyncio.create_task(self._election_loop())

        # Run the server until cancelled or shutdown
        try:
            await self._server.serve_forever()
        except asyncio.CancelledError:
            pass

    def _sync_state_from_manager(self) -> None:
        """Sync legacy ClusterState fields from MultiRaftManager."""
        group = self._raft_manager.system_group
        self._cluster.term = group.term
        self._cluster.voted_for = group.voted_for
        self._cluster.state = group.state.state
        self._cluster.leader_id = group.leader_id
        self._cluster.raft_log = group.raft_log
        self._cluster.next_index = group.state.next_index
        self._cluster.match_index = group.state.match_index

    async def _election_loop(self) -> None:
        """Main election coordinator running in background."""
        while self._running:
            await asyncio.sleep(0.05)  # Fast loop for responsive heartbeats

            # Use MultiRaftManager's tick for all groups
            await self._raft_manager.tick(
                self._cluster.known_nodes,
                self._send_to_peer,
            )

            # Sync state for compatibility
            self._sync_state_from_manager()

    async def _receive_loop(self, transport: Transport) -> None:
        """Process incoming messages from a peer."""
        peer_node_id: str | None = None
        try:
            while self._running:
                try:
                    msg = await asyncio.wait_for(transport.recv(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue

                match msg.msg_type:
                    case MessageType.ACTOR_MSG:
                        await self._deliver_remote_message(msg)
                    case MessageType.LOOKUP_REQ:
                        await self._handle_lookup_request(transport, msg)
                    case MessageType.LOOKUP_RES:
                        self._handle_lookup_response(msg)
                    case MessageType.ASK_REQ:
                        await self._handle_ask_request(transport, msg)
                    case MessageType.ASK_RES:
                        # Check if this is a shard response
                        if msg.target_name.startswith("__shard__"):
                            self._handle_shard_response(msg)
                        else:
                            self._handle_ask_response(msg)
                    case MessageType.PEER_LIST:
                        match deserialize(msg.payload, {}):
                            case {"node_id": node_id}:
                                await self._handle_peer_announce(transport, msg)
                                peer_node_id = node_id
                            case {"peers": _, "addresses": _} as data:
                                await self._handle_peer_list(data)
                    case MessageType.VOTE_REQUEST:
                        await self._handle_vote_request(transport, msg)
                    case MessageType.VOTE_RESPONSE:
                        await self._handle_vote_response(msg)
                    case MessageType.HEARTBEAT:
                        await self._handle_heartbeat(transport, msg)
                    case MessageType.REGISTRY_SYNC_REQ:
                        await self._handle_registry_sync_request(transport, msg)
                    case MessageType.REGISTRY_SYNC_RES:
                        self._handle_registry_sync_response(msg)
                    case MessageType.APPEND_ENTRIES_REQ:
                        await self._handle_append_entries_request(transport, msg)
                    case MessageType.APPEND_ENTRIES_RES:
                        await self._handle_append_entries_response(msg, peer_node_id, transport)
                    case MessageType.INSTALL_SNAPSHOT_REQ:
                        await self._handle_install_snapshot_request(transport, msg)
                    case MessageType.INSTALL_SNAPSHOT_RES:
                        await self._handle_install_snapshot_response(msg)
                    case MessageType.SINGLETON_INFO_REQ:
                        await self._handle_singleton_info_request(transport, msg)
                    case MessageType.SINGLETON_INFO_RES:
                        pass  # Currently not used, for future monitoring
                    case MessageType.BATCHED_RAFT_MSG:
                        await self._handle_batched_raft_message(transport, msg, peer_node_id)

        except asyncio.IncompleteReadError:
            log.info("Peer disconnected")
        except Exception as e:
            log.error(f"Error in receive loop: {e}")
        finally:
            peer_key = transport.peer_info
            self._cluster.peers.pop(peer_key, None)
            if peer_node_id:
                self._cluster.known_nodes.pop(peer_node_id, None)
                self._raft_manager.remove_peer(peer_node_id)
                if self._running and peer_node_id == self._raft_manager.system_group.leader_id:
                    log.info(f"Leader {peer_node_id[:8]} disconnected, will trigger re-election")

    # ---- Message Handlers (delegated to MultiRaftManager) ----

    async def _handle_vote_request(self, transport: Transport, msg: WireMessage) -> None:
        """Handle incoming vote request."""
        data = deserialize(msg.payload, {})
        await self._raft_manager.handle_vote_request(
            data, transport, self._send_response
        )
        self._sync_state_from_manager()

    async def _handle_vote_response(self, msg: WireMessage) -> None:
        """Handle incoming vote response."""
        data = deserialize(msg.payload, {})
        await self._raft_manager.handle_vote_response(data, self._cluster.known_nodes)
        self._sync_state_from_manager()

    async def _handle_heartbeat(self, transport: Transport, msg: WireMessage) -> None:
        """Handle legacy heartbeat from leader (for backward compatibility)."""
        data = deserialize(msg.payload, {})
        # Convert legacy heartbeat to AppendEntries format
        data.setdefault("prev_log_index", 0)
        data.setdefault("prev_log_term", 0)
        data.setdefault("entries", [])
        data.setdefault("leader_commit", 0)
        await self._raft_manager.handle_append_entries_request(
            data, transport, self._send_response
        )
        self._sync_state_from_manager()

    async def _handle_append_entries_request(self, transport: Transport, msg: WireMessage) -> None:
        """Handle incoming AppendEntries RPC from leader."""
        data = deserialize(msg.payload, {})
        await self._raft_manager.handle_append_entries_request(
            data, transport, self._send_response
        )
        self._sync_state_from_manager()

    async def _handle_append_entries_response(
        self, msg: WireMessage, peer_id: str | None, transport: Transport
    ) -> None:
        """Handle AppendEntries response from follower."""
        if not peer_id:
            return
        data = deserialize(msg.payload, {})
        await self._raft_manager.handle_append_entries_response(
            data, peer_id, transport, self._send_response
        )
        self._sync_state_from_manager()

    async def _handle_install_snapshot_request(self, transport: Transport, msg: WireMessage) -> None:
        """Handle incoming InstallSnapshot RPC from leader."""
        data = deserialize(msg.payload, {})
        await self._raft_manager.handle_install_snapshot_request(
            data, transport, self._send_response
        )
        self._sync_state_from_manager()

    async def _handle_install_snapshot_response(self, msg: WireMessage) -> None:
        """Handle InstallSnapshot response from follower."""
        data = deserialize(msg.payload, {})
        await self._raft_manager.handle_install_snapshot_response(data)
        self._sync_state_from_manager()

    async def _handle_batched_raft_message(
        self, transport: Transport, msg: WireMessage, peer_id: str | None
    ) -> None:
        """Handle batched Raft messages."""
        data = deserialize(msg.payload, {})
        await self._raft_manager.handle_batched_message(
            data,
            transport,
            self._send_response,
            peer_id or "",
            self._cluster.known_nodes,
        )
        self._sync_state_from_manager()

    async def _send_response(self, transport: Transport, msg: WireMessage) -> None:
        """Send a response message."""
        try:
            await transport.send(msg)
        except Exception as e:
            log.debug(f"Failed to send response: {e}")

    # ---- Persistence (for compatibility) ----

    async def _persist_state(self) -> None:
        """Persist current term and voted_for to disk."""
        await self._raft_manager.persist_state()

    async def _persist_log(self) -> None:
        """Persist log entries to disk."""
        await self._raft_manager.persist_log()

    # ---- Shutdown ----

    async def shutdown(self) -> None:
        """Stop actors and close network connections."""
        log.debug(f"Node {self._cluster.node_id[:8]}: Starting shutdown")
        self._running = False
        self._raft_manager.stop()

        if self._election_task:
            log.debug(f"Node {self._cluster.node_id[:8]}: Cancelling election task")
            self._election_task.cancel()
            try:
                await asyncio.wait_for(self._election_task, timeout=2.0)
            except asyncio.CancelledError:
                pass
            except asyncio.TimeoutError:
                log.warning(f"Node {self._cluster.node_id[:8]}: Election task cancel timed out")

        # Cancel receive tasks first to allow clean connection shutdown
        log.debug(f"Node {self._cluster.node_id[:8]}: Cancelling {len(self._receive_tasks)} receive tasks")
        for task in list(self._receive_tasks):
            task.cancel()
        if self._receive_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._receive_tasks, return_exceptions=True),
                    timeout=2.0,
                )
            except asyncio.TimeoutError:
                log.warning(f"Node {self._cluster.node_id[:8]}: Receive tasks cancel timed out")
        self._receive_tasks.clear()

        # Close transports before server to ensure clean connection teardown
        log.debug(f"Node {self._cluster.node_id[:8]}: Closing {len(self._cluster.peers)} transports")
        for transport in list(self._cluster.peers.values()):
            try:
                await asyncio.wait_for(transport.close(), timeout=1.0)
            except asyncio.TimeoutError:
                log.warning(f"Node {self._cluster.node_id[:8]}: Transport close timed out")
        self._cluster.peers.clear()
        self._cluster.known_nodes.clear()

        # Now close the server - all connection handlers should be done
        if self._server:
            log.debug(f"Node {self._cluster.node_id[:8]}: Closing server")
            self._server.close()
            try:
                await asyncio.wait_for(self._server.wait_closed(), timeout=2.0)
            except asyncio.TimeoutError:
                log.warning(f"Node {self._cluster.node_id[:8]}: Server wait_closed timed out")

        log.debug(f"Node {self._cluster.node_id[:8]}: Calling super().shutdown()")
        await super().shutdown()
        log.debug(f"Node {self._cluster.node_id[:8]}: Shutdown complete")

    async def __aenter__(self) -> "DistributedActorSystem":
        """Enter async context manager, starting the server in background."""
        self._serve_task = asyncio.create_task(self.serve())
        # Give the server a moment to start
        await asyncio.sleep(0.1)
        return self

    async def __aexit__(self, _exc_type: type | None, _exc_val: Exception | None, _exc_tb: object) -> None:
        """Exit async context manager, ensuring graceful shutdown."""
        await self.shutdown()
        if self._serve_task:
            self._serve_task.cancel()
            try:
                await self._serve_task
            except asyncio.CancelledError:
                pass

    # ---- Legacy Compatibility Properties ----

    def _reset_election_timeout(self) -> None:
        """Reset election timeout (legacy compatibility)."""
        self._raft_manager.system_group._reset_election_timeout()
        self._sync_state_from_manager()

    # For SingletonMixin compatibility
    async def _append_singleton_command(self, cmd) -> bool:
        """Append a singleton command to the Raft log."""
        return await self.append_command(cmd)

    async def _apply_singleton_command(self, cmd) -> None:
        """Apply a singleton command to the local state machine."""
        # Import here to avoid circular import
        from .raft_types import SingletonCommand, SingletonEntry

        if not isinstance(cmd, SingletonCommand):
            return

        if cmd.action == "register":
            self._cluster.singleton_registry[cmd.name] = SingletonEntry(
                name=cmd.name,
                node_id=cmd.node_id,
                actor_cls_name=cmd.actor_cls_name,
                kwargs=dict(cmd.kwargs),
                status="active",
            )
            log.debug(f"Registered singleton '{cmd.name}' on node {cmd.node_id[:8]}")

        elif cmd.action == "unregister":
            self._cluster.singleton_registry.pop(cmd.name, None)
            log.debug(f"Unregistered singleton '{cmd.name}'")

        elif cmd.action == "orphan":
            if cmd.name in self._cluster.singleton_registry:
                entry = self._cluster.singleton_registry[cmd.name]
                if entry.node_id == cmd.node_id:
                    self._cluster.singleton_registry[cmd.name] = SingletonEntry(
                        name=entry.name,
                        node_id=entry.node_id,
                        actor_cls_name=entry.actor_cls_name,
                        kwargs=entry.kwargs,
                        status="orphan",
                    )
                    log.info(f"Marked singleton '{cmd.name}' as orphan (node {cmd.node_id[:8]} disconnected)")

    # ---- Sharding ----

    def _get_shard_id(self, entity_id: str, num_shards: int) -> int:
        """Calculate which shard an entity belongs to."""
        return hash(entity_id) % num_shards

    def _get_node_for_shard(self, entity_type: str, shard_id: int) -> str | None:
        """Get the node responsible for a shard."""
        return self._shard_allocations.get((entity_type, shard_id))

    async def _spawn_sharded[M](
        self,
        actor_cls: type[Actor[M]],
        name: str,
        num_shards: int,
        kwargs: dict[str, Any],
    ) -> ShardedRef[M]:
        """Spawn a sharded actor type and allocate shards across the cluster.

        Args:
            actor_cls: The actor class for entities
            name: The entity type name
            num_shards: Number of shards to create
            kwargs: Default constructor arguments for entities

        Returns:
            ShardedRef for interacting with entities
        """
        # Store configuration locally
        config = ShardedEntityConfig(
            entity_type=name,
            actor_cls_name=get_type_name(actor_cls),
            num_shards=num_shards,
            kwargs=kwargs,
        )
        self._sharded_entities[name] = config
        self._cluster.type_registry[f"__sharded_cls__{name}"] = actor_cls

        # Wait for leader election if needed (single-node cluster becomes leader)
        timeout = 5.0
        elapsed = 0.0
        while not self.is_leader and elapsed < timeout:
            await asyncio.sleep(0.1)
            elapsed += 0.1

        # If we're the leader, register the entity type and allocate shards via Raft
        if self.is_leader:
            # First, register the entity type so all nodes know about it
            register_cmd = RegisterShardedEntityCommand(
                entity_type=name,
                actor_cls_name=get_type_name(actor_cls),
                num_shards=num_shards,
                kwargs=tuple(kwargs.items()),
            )
            await self.append_command(register_cmd)

            # Then allocate shards
            await self._allocate_shards(name, num_shards)

            # Wait for allocations to be committed
            await asyncio.sleep(0.1)
        else:
            # Wait for shard allocations to be replicated
            await asyncio.sleep(0.3)

        # Create and return ShardedRef
        return ShardedRef(
            entity_type=name,
            num_shards=num_shards,
            system=self,
            send_callback=self._sharded_send,
            ask_callback=self._sharded_ask,
        )

    async def _allocate_shards(
        self,
        entity_type: str,
        num_shards: int,
    ) -> None:
        """Allocate shards to nodes using round-robin via Raft consensus."""
        # Get list of all known nodes including self
        all_nodes = [self.node_id] + list(self._cluster.known_nodes.keys())

        for shard_id in range(num_shards):
            # Round-robin allocation
            node_idx = shard_id % len(all_nodes)
            node_id = all_nodes[node_idx]

            cmd = ShardAllocationCommand(
                action="allocate",
                entity_type=entity_type,
                shard_id=shard_id,
                node_id=node_id,
            )

            # Append to Raft log for consensus
            await self.append_command(cmd)

    async def _apply_sharding_command(self, cmd: Any) -> None:
        """Apply a sharding command to local state.

        Called by Raft when command is committed.
        """
        if isinstance(cmd, RegisterShardedEntityCommand):
            # Register the sharded entity type on this node
            config = ShardedEntityConfig(
                entity_type=cmd.entity_type,
                actor_cls_name=cmd.actor_cls_name,
                num_shards=cmd.num_shards,
                kwargs=dict(cmd.kwargs),
            )
            if cmd.entity_type not in self._sharded_entities:
                self._sharded_entities[cmd.entity_type] = config
                log.debug(f"Registered sharded entity type: {cmd.entity_type}")

                # Try to auto-import the actor class if not already registered
                sharded_cls_key = f"__sharded_cls__{cmd.entity_type}"
                if sharded_cls_key not in self._cluster.type_registry:
                    try:
                        actor_cls = _import_type(cmd.actor_cls_name)
                        self._cluster.type_registry[sharded_cls_key] = actor_cls
                        log.debug(f"Auto-imported actor class: {cmd.actor_cls_name}")
                    except (ImportError, AttributeError) as e:
                        log.warning(
                            f"Could not auto-import actor class {cmd.actor_cls_name}: {e}. "
                            f"Entities of type {cmd.entity_type} may not be created on this node."
                        )

        elif isinstance(cmd, ShardAllocationCommand):
            if cmd.action == "allocate":
                self._shard_allocations[(cmd.entity_type, cmd.shard_id)] = cmd.node_id
                log.debug(
                    f"Allocated shard {cmd.entity_type}:{cmd.shard_id} to node {cmd.node_id[:8]}"
                )
            elif cmd.action == "deallocate":
                self._shard_allocations.pop((cmd.entity_type, cmd.shard_id), None)
                log.debug(f"Deallocated shard {cmd.entity_type}:{cmd.shard_id}")

        elif isinstance(cmd, ShardedMessage):
            await self._apply_sharded_message(cmd)

        elif isinstance(cmd, EntityStateUpdate):
            await self._apply_entity_state_update(cmd)

        elif isinstance(cmd, EntityStateDelete):
            await self._apply_entity_state_delete(cmd)

    async def _apply_sharded_message(self, msg: ShardedMessage) -> None:
        """Apply a sharded message after Raft commit.

        Creates the entity actor if it doesn't exist (with state restoration),
        then delivers the message and replicates updated state.
        """
        config = self._sharded_entities.get(msg.entity_type)
        if not config:
            log.warning(f"Unknown sharded entity type: {msg.entity_type}")
            return

        # Calculate which shard this entity belongs to
        shard_id = self._get_shard_id(msg.entity_id, config.num_shards)
        responsible_node = self._get_node_for_shard(msg.entity_type, shard_id)

        if responsible_node != self.node_id:
            # We're not responsible for this shard
            return

        # Get or create the entity actor
        entity_key = f"{msg.entity_type}:{msg.entity_id}"
        actor_ref = self._entity_actors.get(entity_key)

        if actor_ref is None:
            # Create the entity actor
            actor_cls = self._cluster.type_registry.get(f"__sharded_cls__{msg.entity_type}")
            if actor_cls is None:
                log.error(f"Actor class not found for {msg.entity_type}")
                return

            # Try to restore previous state (failover scenario)
            restored_state = await self._try_restore_entity_state(
                msg.entity_type,
                msg.entity_id,
            )

            # Create the actor instance first
            actor_instance = actor_cls(entity_id=msg.entity_id, **config.kwargs)

            # Restore state if found (before spawning)
            if restored_state:
                restore_actor_state(actor_instance, restored_state)
                log.info(f"Restored state for entity {entity_key}")

            # Spawn with the pre-created actor instance
            actor_ref = await self._spawn_entity_actor(
                actor_instance,
                entity_key,
            )

            self._entity_actors[entity_key] = actor_ref
            self._entity_actor_instances[entity_key] = actor_instance
            log.debug(f"Created entity actor {entity_key}")

        # Deserialize and deliver the message
        payload = msg.payload
        if isinstance(payload, bytes):
            payload = deserialize(payload, self._cluster.type_registry)

        if msg.request_id and msg.reply_to:
            # This is an ask - we need to send response back
            try:
                result = await actor_ref.ask(payload)
                # Schedule state replication after apply callback completes
                # (avoid recursion by not calling append_command during apply)
                asyncio.create_task(
                    self._replicate_entity_state(msg.entity_type, msg.entity_id)
                )
                await self._send_shard_response(
                    msg.reply_to,
                    msg.request_id,
                    result,
                    success=True,
                )
            except Exception as e:
                log.error(f"Error processing sharded ask: {e}")
                await self._send_shard_response(
                    msg.reply_to,
                    msg.request_id,
                    None,
                    success=False,
                )
        else:
            # Fire-and-forget
            await actor_ref.send(payload)
            # Schedule state replication after apply callback completes
            asyncio.create_task(
                self._replicate_entity_state(msg.entity_type, msg.entity_id)
            )

    async def _send_shard_response(
        self,
        target_node: str,
        request_id: str,
        result: Any,
        success: bool,
    ) -> None:
        """Send a sharded ask response directly to the requesting node."""
        if target_node == self.node_id:
            # Local response
            future = self._pending_shard_asks.get(request_id)
            if future and not future.done():
                if success:
                    future.set_result(result)
                else:
                    future.set_exception(RuntimeError("Sharded ask failed"))
            return

        # Remote response
        transport = self._cluster.known_nodes.get(target_node)
        if not transport:
            log.warning(f"Cannot send shard response: node {target_node[:8]} not connected")
            return

        response_data = {
            "request_id": request_id,
            "success": success,
            "result": serialize(result) if success else None,
        }

        wire_msg = WireMessage(
            msg_type=MessageType.ASK_RES,
            target_name=f"__shard__{request_id}",
            payload=serialize(response_data),
        )

        try:
            await transport.send(wire_msg)
        except Exception as e:
            log.error(f"Failed to send shard response: {e}")

    async def _sharded_send(
        self,
        entity_type: str,
        entity_id: str,
        msg: Any,
    ) -> None:
        """Send a message to a sharded entity via Raft.

        The message goes through Raft to ensure consistent ordering.
        """
        sharded_msg = ShardedMessage(
            entity_type=entity_type,
            entity_id=entity_id,
            payload=serialize(msg),
        )

        # Append to Raft log
        success = await self.append_command(sharded_msg)
        if not success:
            raise RuntimeError(f"Failed to send message to {entity_type}:{entity_id}")

    async def _sharded_ask(
        self,
        entity_type: str,
        entity_id: str,
        msg: Any,
        timeout: float,
    ) -> Any:
        """Send a message and await response from a sharded entity.

        The request goes through Raft for ordering, but the response
        comes back directly via transport.
        """
        request_id = str(uuid4())
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        self._pending_shard_asks[request_id] = future

        sharded_msg = ShardedMessage(
            entity_type=entity_type,
            entity_id=entity_id,
            payload=serialize(msg),
            reply_to=self.node_id,
            request_id=request_id,
        )

        try:
            # Append to Raft log
            success = await self.append_command(sharded_msg)
            if not success:
                raise RuntimeError(f"Failed to send ask to {entity_type}:{entity_id}")

            # Wait for response
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending_shard_asks.pop(request_id, None)
            raise
        finally:
            self._pending_shard_asks.pop(request_id, None)

    def _handle_shard_response(self, msg: WireMessage) -> None:
        """Handle a sharded ask response from another node."""
        try:
            data = deserialize(msg.payload, self._cluster.type_registry)
            request_id = data.get("request_id")
            success = data.get("success", False)
            result_bytes = data.get("result")

            future = self._pending_shard_asks.get(request_id)
            if not future or future.done():
                return

            if success and result_bytes:
                result = deserialize(result_bytes, self._cluster.type_registry)
                future.set_result(result)
            else:
                future.set_exception(RuntimeError("Sharded ask failed"))
        except Exception as e:
            log.error(f"Error handling shard response: {e}")

    def _get_entity_actor(self, entity_type: str, entity_id: str) -> ActorRef[Any] | None:
        """Get an existing entity actor reference."""
        entity_key = f"{entity_type}:{entity_id}"
        return self._entity_actors.get(entity_key)

    async def _spawn_entity_actor[M](
        self,
        actor: Actor[M],
        name: str,
    ) -> ActorRef[M]:
        """Spawn an entity actor with a pre-created actor instance.

        This is different from normal spawn as we already have the actor instance
        (useful for state restoration before starting the actor loop).
        """
        actor_id = ActorId(uid=uuid4(), name=name)
        mailbox: Queue[Envelope[M]] = Queue()
        ref: ActorRef[M] = ActorRef(actor_id, mailbox, self)
        ctx: Context[M] = Context(self_ref=ref, system=self)
        actor._ctx = ctx

        async def run_actor() -> None:
            await actor.on_start()
            try:
                while self._running:
                    try:
                        envelope = await asyncio.wait_for(mailbox.get(), timeout=0.1)
                    except asyncio.TimeoutError:
                        continue

                    ctx._current_envelope = envelope
                    try:
                        await actor.receive(envelope.payload, ctx)
                    except Exception as exc:
                        log.exception(f"Actor {actor_id} failed processing message")
                        if not actor.on_error(exc):
                            return
                    finally:
                        ctx._current_envelope = None
            except asyncio.CancelledError:
                pass
            finally:
                await actor.on_stop()
                self._actors.pop(actor_id, None)

        task = asyncio.create_task(run_actor())
        self._actors[actor_id] = task
        return ref

    # ---- State Replication ----

    async def _apply_entity_state_update(self, cmd: EntityStateUpdate) -> None:
        """Apply an entity state update from Raft.

        On backup nodes, stores the state in the backup manager.
        On the primary node (cmd.primary_node == self.node_id), this is a no-op.
        """
        # Only store backup if we're not the primary
        if cmd.primary_node != self.node_id:
            await self._backup_manager.store(
                entity_type=cmd.entity_type,
                entity_id=cmd.entity_id,
                state=cmd.state,
                version=cmd.version,
                primary_node=cmd.primary_node,
                timestamp=cmd.timestamp,
            )
            log.debug(
                f"Stored backup state for {cmd.entity_type}:{cmd.entity_id} "
                f"v{cmd.version} from {cmd.primary_node[:8]}"
            )

    async def _apply_entity_state_delete(self, cmd: EntityStateDelete) -> None:
        """Apply an entity state deletion from Raft."""
        await self._backup_manager.delete(cmd.entity_type, cmd.entity_id)
        log.debug(
            f"Deleted backup state for {cmd.entity_type}:{cmd.entity_id} "
            f"reason={cmd.reason}"
        )

    async def _replicate_entity_state(
        self,
        entity_type: str,
        entity_id: str,
    ) -> None:
        """Replicate entity state to backup nodes via Raft.

        Called after processing a message on the primary node.
        """
        config = self._replication_config

        # Skip if no replicas configured
        if config.memory_replicas == 0:
            return

        # Get the actor instance
        entity_key = f"{entity_type}:{entity_id}"
        actor = self._entity_actor_instances.get(entity_key)
        if actor is None:
            log.warning(f"Cannot replicate state: actor {entity_key} not found")
            return

        # Extract and serialize state
        state_bytes = serialize_actor_state(actor)

        # Increment version
        key = f"{entity_type}:{entity_id}"
        version = self._state_versions.get(key, 0) + 1
        self._state_versions[key] = version

        # Create state update command
        update = EntityStateUpdate(
            entity_type=entity_type,
            entity_id=entity_id,
            state=state_bytes,
            version=version,
            primary_node=self.node_id,
            timestamp=time.time(),
        )

        # Replicate via Raft
        await self.append_command(update)

    async def _try_restore_entity_state(
        self,
        entity_type: str,
        entity_id: str,
    ) -> dict[str, Any] | None:
        """Try to restore entity state from backups.

        Called when creating an entity actor to restore previous state.
        """
        config = self._replication_config

        # Try backup in memory (fastest)
        if config.prefer_memory_restore:
            result = await self._backup_manager.get(entity_type, entity_id)
            if result:
                state_bytes, version = result
                log.debug(
                    f"Restored {entity_type}:{entity_id} from memory backup (v{version})"
                )
                return deserialize(state_bytes, self._cluster.type_registry)

        # No state found
        log.debug(f"No state found for {entity_type}:{entity_id}, starting fresh")
        return None

    def get_backup_stats(self) -> dict[str, Any]:
        """Get statistics about the backup state manager."""
        return self._backup_manager.get_stats()
