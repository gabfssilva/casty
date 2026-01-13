"""Distributed actor system with cluster support and full Raft consensus."""

from __future__ import annotations

import asyncio
import logging
import random
import ssl
import time
from dataclasses import dataclass, field
from typing import Any, Literal
from uuid import uuid4

from ..actor import Actor, ActorRef
from ..system import ActorSystem
from .mixins import (
    ElectionMixin,
    LogReplicationMixin,
    MessagingMixin,
    PeerMixin,
    PersistenceMixin,
    SingletonMixin,
    SnapshotMixin,
)
from .raft_types import RaftLog, SingletonEntry
from .serialize import deserialize, get_type_name
from .transport import MessageType, Transport

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
    # Election state (Raft)
    node_id: str = field(default_factory=lambda: str(uuid4()))
    known_nodes: dict[str, Transport] = field(default_factory=dict)
    peer_addresses: dict[str, tuple[str, int]] = field(default_factory=dict)
    term: int = 0
    voted_for: str | None = None
    votes_received: set[str] = field(default_factory=set)
    state: Literal["follower", "candidate", "leader"] = "follower"
    leader_id: str | None = None
    last_heartbeat: float = field(default_factory=time.time)
    election_timeout: float = 0.0  # Set in __post_init__
    last_heartbeat_sent: float = 0.0
    election_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    # Raft log replication state
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
    ElectionMixin,
    PersistenceMixin,
    LogReplicationMixin,
    SnapshotMixin,
    SingletonMixin,
    PeerMixin,
    MessagingMixin,
    ActorSystem,
):
    """Actor system with distributed cluster support and full Raft consensus."""

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
        # Initialize mixin state
        self._init_persistence(persistence_dir)
        self._init_log_replication()
        self._init_snapshot()

    @property
    def node_id(self) -> str:
        """This node's unique ID."""
        return self._cluster.node_id

    @property
    def is_leader(self) -> bool:
        """Check if this node is the leader."""
        return self._cluster.state == "leader"

    @property
    def leader_id(self) -> str | None:
        """Get the current leader's ID."""
        return self._cluster.leader_id

    @property
    def commit_index(self) -> int:
        """Get current commit index."""
        return self._cluster.raft_log.commit_index

    @property
    def last_applied(self) -> int:
        """Get last applied index."""
        return self._cluster.raft_log.last_applied

    def register_type(self, cls: type) -> None:
        """Register a message type for remote serialization."""
        type_name = get_type_name(cls)
        self._cluster.type_registry[type_name] = cls

    async def spawn[M](
        self,
        actor_cls: type[Actor[M]],
        *,
        name: str | None = None,
        singleton: bool = False,
        **kwargs: Any,
    ) -> ActorRef[M]:
        """Spawn actor and register if named.

        Args:
            actor_cls: The actor class to instantiate.
            name: Optional name for the actor.
            singleton: If True, creates a cluster-wide singleton.
                      If the singleton already exists, returns a RemoteRef.
            **kwargs: Constructor arguments for the actor.
        """
        if singleton:
            if name is None:
                raise ValueError("Singleton actors must have a name")
            return await self._spawn_singleton(actor_cls, name, kwargs)

        ref = await super().spawn(actor_cls, name=name, **kwargs)
        if name:
            self._cluster.registry[name] = ref
        return ref

    async def serve(self) -> None:
        """Start TCP server, connect to seeds, and run consensus.

        This method blocks until shutdown() is called.
        """
        # Load persisted state if available
        await self._load_persisted_state()
        await self._load_persisted_log()
        snapshot = await self._load_persisted_snapshot()
        if snapshot:
            self._current_snapshot = snapshot

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
        # Note: We don't use `async with self._server` to avoid double wait_closed()
        # calls during shutdown (once in shutdown() and once in __aexit__).
        try:
            await self._server.serve_forever()
        except asyncio.CancelledError:
            pass

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
                        await self._handle_append_entries_response(msg)
                    case MessageType.INSTALL_SNAPSHOT_REQ:
                        await self._handle_install_snapshot_request(transport, msg)
                    case MessageType.INSTALL_SNAPSHOT_RES:
                        await self._handle_install_snapshot_response(msg)
                    case MessageType.SINGLETON_INFO_REQ:
                        await self._handle_singleton_info_request(transport, msg)
                    case MessageType.SINGLETON_INFO_RES:
                        pass  # Currently not used, for future monitoring

        except asyncio.IncompleteReadError:
            log.info("Peer disconnected")
        except Exception as e:
            log.error(f"Error in receive loop: {e}")
        finally:
            peer_key = transport.peer_info
            self._cluster.peers.pop(peer_key, None)
            if peer_node_id:
                self._cluster.known_nodes.pop(peer_node_id, None)
                self._cluster.next_index.pop(peer_node_id, None)
                self._cluster.match_index.pop(peer_node_id, None)
                if self._running and peer_node_id == self._cluster.leader_id:
                    log.info(f"Leader {peer_node_id[:8]} disconnected, will trigger re-election")
                    self._cluster.leader_id = None
                # Note: Singleton orphaning is handled lazily on next spawn attempt
                # We don't do it here to avoid blocking shutdown

    async def shutdown(self) -> None:
        """Stop actors and close network connections."""
        log.debug(f"Node {self._cluster.node_id[:8]}: Starting shutdown")
        self._running = False

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
