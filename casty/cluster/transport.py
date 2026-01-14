"""TCP transport for cluster communication.

Provides reliable message delivery between cluster nodes with
a simple length-prefixed framing protocol.
"""

from __future__ import annotations

import asyncio
import logging
import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Callable, Awaitable

from .serialize import serialize, deserialize

logger = logging.getLogger(__name__)


class MessageType(IntEnum):
    """Types of messages in the wire protocol.

    Each message type has a single-byte identifier.
    """

    # Actor messages
    ACTOR_MSG = 1
    ASK_REQUEST = 2
    ASK_RESPONSE = 3

    # Raft (control plane)
    VOTE_REQUEST = 10
    VOTE_RESPONSE = 11
    APPEND_ENTRIES_REQUEST = 12
    APPEND_ENTRIES_RESPONSE = 13
    INSTALL_SNAPSHOT_REQUEST = 14
    INSTALL_SNAPSHOT_RESPONSE = 15

    # Gossip (data plane)
    GOSSIP_PING = 20
    GOSSIP_PING_REQ = 21
    GOSSIP_ACK = 22
    GOSSIP_MEMBERSHIP = 23

    # Event replication
    REPLICATE_EVENT = 30
    REPLICATE_ACK = 31

    # Entity routing
    ENTITY_MSG = 40
    ENTITY_ASK_REQUEST = 41
    ENTITY_ASK_RESPONSE = 42

    # Lookup
    LOOKUP_REQUEST = 50
    LOOKUP_RESPONSE = 51

    # State replication
    STATE_UPDATE = 60
    STATE_UPDATE_ACK = 61


@dataclass
class WireMessage:
    """Message format for wire protocol.

    Wire format:
    [msg_type: 1B][name_len: 2B][name: UTF-8][payload_len: 4B][payload: bytes]

    Attributes:
        msg_type: Type of the message
        name: Optional name/identifier (e.g., actor name, request ID)
        payload: Serialized message payload
    """

    msg_type: MessageType
    name: str
    payload: bytes

    def to_bytes(self) -> bytes:
        """Serialize to wire format."""
        name_bytes = self.name.encode("utf-8")
        return (
            struct.pack("!B", self.msg_type)
            + struct.pack("!H", len(name_bytes))
            + name_bytes
            + struct.pack("!I", len(self.payload))
            + self.payload
        )

    @classmethod
    def from_reader(
        cls,
        reader: asyncio.StreamReader,
    ) -> Awaitable["WireMessage"]:
        """Read a message from a stream reader."""
        return cls._read_async(reader)

    @classmethod
    async def _read_async(
        cls,
        reader: asyncio.StreamReader,
    ) -> "WireMessage":
        """Async implementation of reading from stream."""
        # Read message type
        type_data = await reader.readexactly(1)
        msg_type = MessageType(struct.unpack("!B", type_data)[0])

        # Read name length and name
        name_len_data = await reader.readexactly(2)
        name_len = struct.unpack("!H", name_len_data)[0]
        name_data = await reader.readexactly(name_len)
        name = name_data.decode("utf-8")

        # Read payload length and payload
        payload_len_data = await reader.readexactly(4)
        payload_len = struct.unpack("!I", payload_len_data)[0]
        payload = await reader.readexactly(payload_len)

        return cls(msg_type=msg_type, name=name, payload=payload)


class Connection:
    """A single TCP connection to a peer.

    Handles reading and writing messages with automatic reconnection.
    """

    def __init__(
        self,
        host: str,
        port: int,
        *,
        reconnect_delay: float = 1.0,
        max_reconnect_delay: float = 30.0,
    ):
        self._host = host
        self._port = port
        self._reconnect_delay = reconnect_delay
        self._max_reconnect_delay = max_reconnect_delay

        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._connected = False
        self._connecting = False
        self._lock = asyncio.Lock()

    @property
    def is_connected(self) -> bool:
        """Check if connected."""
        return self._connected

    @property
    def address(self) -> tuple[str, int]:
        """Get the remote address."""
        return (self._host, self._port)

    async def connect(self) -> bool:
        """Establish connection.

        Returns:
            True if connected successfully
        """
        if self._connected:
            return True

        async with self._lock:
            if self._connected:
                return True

            if self._connecting:
                return False

            self._connecting = True
            try:
                self._reader, self._writer = await asyncio.wait_for(
                    asyncio.open_connection(self._host, self._port),
                    timeout=5.0,
                )
                self._connected = True
                logger.debug(f"Connected to {self._host}:{self._port}")
                return True
            except Exception as e:
                logger.debug(f"Failed to connect to {self._host}:{self._port}: {e}")
                return False
            finally:
                self._connecting = False

    async def close(self) -> None:
        """Close the connection."""
        self._connected = False
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None
            self._reader = None

    async def send(self, message: WireMessage) -> bool:
        """Send a message.

        Args:
            message: Message to send

        Returns:
            True if sent successfully
        """
        if not self._connected:
            if not await self.connect():
                return False

        try:
            if self._writer:
                self._writer.write(message.to_bytes())
                await self._writer.drain()
                return True
        except Exception as e:
            logger.debug(f"Failed to send to {self._host}:{self._port}: {e}")
            await self.close()
        return False

    async def receive(self) -> WireMessage | None:
        """Receive a message.

        Returns:
            Received message or None on error
        """
        if not self._connected or not self._reader:
            return None

        try:
            return await WireMessage.from_reader(self._reader)
        except asyncio.IncompleteReadError:
            await self.close()
            return None
        except Exception as e:
            logger.debug(f"Failed to receive from {self._host}:{self._port}: {e}")
            await self.close()
            return None


class Transport:
    """Transport layer for cluster communication.

    Manages connections to peers and provides high-level methods
    for sending different types of messages.
    """

    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
    ):
        self._node_id = node_id
        self._host = host
        self._port = port

        # Peer connections
        self._peers: dict[str, Connection] = {}
        self._peer_addresses: dict[str, tuple[str, int]] = {}

        # Server
        self._server: asyncio.Server | None = None
        self._running = False

        # Message handlers
        self._handlers: dict[MessageType, Callable[[WireMessage, tuple[str, int]], Awaitable[WireMessage | None]]] = {}

        # Pending requests (for ask pattern)
        self._pending_requests: dict[str, asyncio.Future[Any]] = {}
        self._request_counter = 0

    @property
    def node_id(self) -> str:
        """Get this node's ID."""
        return self._node_id

    @property
    def address(self) -> tuple[str, int]:
        """Get this node's address."""
        return (self._host, self._port)

    def add_peer(
        self,
        peer_id: str,
        host: str,
        port: int,
    ) -> None:
        """Add a peer to connect to.

        Args:
            peer_id: Peer's node ID
            host: Peer's hostname
            port: Peer's port
        """
        if peer_id == self._node_id:
            return

        self._peer_addresses[peer_id] = (host, port)
        if peer_id not in self._peers:
            self._peers[peer_id] = Connection(host, port)

    def remove_peer(self, peer_id: str) -> None:
        """Remove a peer.

        Args:
            peer_id: Peer's node ID

        Note: Connection close is scheduled if there's a running event loop,
        otherwise the connection is just removed from tracking.
        """
        self._peer_addresses.pop(peer_id, None)
        conn = self._peers.pop(peer_id, None)
        if conn:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(conn.close())
            except RuntimeError:
                # No running event loop - connection will be garbage collected
                pass

    def set_handler(
        self,
        msg_type: MessageType,
        handler: Callable[[WireMessage, tuple[str, int]], Awaitable[WireMessage | None]],
    ) -> None:
        """Set a handler for a message type.

        Args:
            msg_type: Message type to handle
            handler: Async function that processes the message
        """
        self._handlers[msg_type] = handler

    async def start(self) -> None:
        """Start the transport server."""
        if self._running:
            return

        self._server = await asyncio.start_server(
            self._handle_connection,
            self._host,
            self._port,
        )
        self._running = True
        logger.info(f"Transport listening on {self._host}:{self._port}")

    async def stop(self) -> None:
        """Stop the transport server."""
        self._running = False

        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        # Close all peer connections
        for conn in self._peers.values():
            await conn.close()

        # Cancel pending requests
        for fut in self._pending_requests.values():
            fut.cancel()
        self._pending_requests.clear()

        logger.info("Transport stopped")

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle an incoming connection."""
        peer_addr = writer.get_extra_info("peername")
        logger.debug(f"Incoming connection from {peer_addr}")

        try:
            while self._running:
                try:
                    msg = await asyncio.wait_for(
                        WireMessage.from_reader(reader),
                        timeout=60.0,
                    )
                except asyncio.TimeoutError:
                    continue
                except asyncio.IncompleteReadError:
                    break

                # Handle the message
                response = await self._dispatch_message(msg, peer_addr)

                # Send response if any
                if response:
                    writer.write(response.to_bytes())
                    await writer.drain()

        except Exception as e:
            logger.debug(f"Connection error from {peer_addr}: {e}")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def _dispatch_message(
        self,
        msg: WireMessage,
        from_addr: tuple[str, int],
    ) -> WireMessage | None:
        """Dispatch a message to its handler."""
        # Check for response to pending request
        if msg.msg_type in (
            MessageType.ASK_RESPONSE,
            MessageType.VOTE_RESPONSE,
            MessageType.APPEND_ENTRIES_RESPONSE,
            MessageType.ENTITY_ASK_RESPONSE,
            MessageType.LOOKUP_RESPONSE,
        ):
            request_id = msg.name
            if request_id in self._pending_requests:
                fut = self._pending_requests.pop(request_id)
                if not fut.done():
                    fut.set_result(deserialize(msg.payload))
            return None

        # Handle STATE_UPDATE_ACK - extract request_id from name
        # Name format: "entity_type:entity_id:request_id" or "entity_type:entity_id"
        if msg.msg_type == MessageType.STATE_UPDATE_ACK:
            parts = msg.name.rsplit(":", 1)
            if len(parts) == 2 and parts[1].startswith("state-"):
                request_id = parts[1]
                if request_id in self._pending_requests:
                    fut = self._pending_requests.pop(request_id)
                    if not fut.done():
                        fut.set_result(deserialize(msg.payload))
            return None

        # Dispatch to handler
        handler = self._handlers.get(msg.msg_type)
        if handler:
            return await handler(msg, from_addr)

        logger.warning(f"No handler for message type {msg.msg_type}")
        return None

    def _next_request_id(self) -> str:
        """Generate a unique request ID."""
        self._request_counter += 1
        return f"{self._node_id}:{self._request_counter}"

    async def _send_to_peer(
        self,
        peer_id: str,
        message: WireMessage,
    ) -> bool:
        """Send a message to a peer.

        Args:
            peer_id: Target peer ID
            message: Message to send

        Returns:
            True if sent successfully
        """
        conn = self._peers.get(peer_id)
        if not conn:
            addr = self._peer_addresses.get(peer_id)
            if not addr:
                return False
            conn = Connection(addr[0], addr[1])
            self._peers[peer_id] = conn

        return await conn.send(message)

    async def _send_to_address(
        self,
        address: tuple[str, int],
        message: WireMessage,
    ) -> bool:
        """Send a message to an address.

        Args:
            address: Target (host, port)
            message: Message to send

        Returns:
            True if sent successfully
        """
        # Find or create connection
        for peer_id, addr in self._peer_addresses.items():
            if addr == address:
                return await self._send_to_peer(peer_id, message)

        # Create temporary connection
        conn = Connection(address[0], address[1])
        try:
            return await conn.send(message)
        finally:
            await conn.close()

    async def _request_response(
        self,
        peer_id: str,
        request_msg: WireMessage,
        response_type: MessageType,
        timeout: float = 5.0,
    ) -> Any:
        """Send a request and wait for response.

        Args:
            peer_id: Target peer
            request_msg: Request message
            response_type: Expected response type
            timeout: Response timeout

        Returns:
            Response payload

        Raises:
            asyncio.TimeoutError: If no response within timeout
            ConnectionError: If send fails
        """
        request_id = request_msg.name

        # Create future for response
        fut: asyncio.Future[Any] = asyncio.get_event_loop().create_future()
        self._pending_requests[request_id] = fut

        try:
            # Send request
            if not await self._send_to_peer(peer_id, request_msg):
                raise ConnectionError(f"Failed to send to {peer_id}")

            # Wait for response
            return await asyncio.wait_for(fut, timeout=timeout)
        finally:
            self._pending_requests.pop(request_id, None)

    # High-level messaging methods

    async def send_actor_message(
        self,
        peer_id: str,
        actor_name: str,
        payload: Any,
    ) -> bool:
        """Send an actor message.

        Args:
            peer_id: Target node
            actor_name: Target actor name
            payload: Message payload

        Returns:
            True if sent
        """
        msg = WireMessage(
            msg_type=MessageType.ACTOR_MSG,
            name=actor_name,
            payload=serialize(payload),
        )
        return await self._send_to_peer(peer_id, msg)

    async def ask_actor(
        self,
        peer_id: str,
        actor_name: str,
        payload: Any,
        timeout: float = 5.0,
    ) -> Any:
        """Send a request-response message to an actor.

        Args:
            peer_id: Target node
            actor_name: Target actor name
            payload: Message payload
            timeout: Response timeout

        Returns:
            Response from actor
        """
        request_id = self._next_request_id()
        msg = WireMessage(
            msg_type=MessageType.ASK_REQUEST,
            name=f"{actor_name}:{request_id}",
            payload=serialize(payload),
        )

        fut: asyncio.Future[Any] = asyncio.get_event_loop().create_future()
        self._pending_requests[request_id] = fut

        try:
            if not await self._send_to_peer(peer_id, msg):
                raise ConnectionError(f"Failed to send to {peer_id}")
            return await asyncio.wait_for(fut, timeout=timeout)
        finally:
            self._pending_requests.pop(request_id, None)

    async def send_entity_message(
        self,
        address: tuple[str, int],
        entity_type: str,
        entity_id: str,
        payload: Any,
    ) -> bool:
        """Send a message to a sharded entity.

        Args:
            address: Target node address
            entity_type: Entity type
            entity_id: Entity ID
            payload: Message payload

        Returns:
            True if sent
        """
        msg = WireMessage(
            msg_type=MessageType.ENTITY_MSG,
            name=f"{entity_type}:{entity_id}",
            payload=serialize(payload),
        )
        return await self._send_to_address(address, msg)

    async def ask_entity(
        self,
        address: tuple[str, int],
        entity_type: str,
        entity_id: str,
        payload: Any,
        timeout: float = 5.0,
    ) -> Any:
        """Send a request-response message to a sharded entity.

        Args:
            address: Target node address
            entity_type: Entity type
            entity_id: Entity ID
            payload: Message payload
            timeout: Response timeout

        Returns:
            Response from entity
        """
        request_id = self._next_request_id()
        msg = WireMessage(
            msg_type=MessageType.ENTITY_ASK_REQUEST,
            name=f"{entity_type}:{entity_id}:{request_id}",
            payload=serialize(payload),
        )

        fut: asyncio.Future[Any] = asyncio.get_event_loop().create_future()
        self._pending_requests[request_id] = fut

        try:
            if not await self._send_to_address(address, msg):
                raise ConnectionError(f"Failed to send to {address}")
            return await asyncio.wait_for(fut, timeout=timeout)
        finally:
            self._pending_requests.pop(request_id, None)

    async def send_vote_request(
        self,
        peer_id: str,
        request: Any,
        timeout: float = 1.0,
    ) -> Any:
        """Send a Raft VoteRequest.

        Args:
            peer_id: Target node
            request: VoteRequest object
            timeout: Response timeout

        Returns:
            VoteResponse
        """
        request_id = self._next_request_id()
        msg = WireMessage(
            msg_type=MessageType.VOTE_REQUEST,
            name=request_id,
            payload=serialize(request),
        )

        fut: asyncio.Future[Any] = asyncio.get_event_loop().create_future()
        self._pending_requests[request_id] = fut

        try:
            if not await self._send_to_peer(peer_id, msg):
                raise ConnectionError(f"Failed to send to {peer_id}")
            return await asyncio.wait_for(fut, timeout=timeout)
        finally:
            self._pending_requests.pop(request_id, None)

    async def send_append_entries(
        self,
        peer_id: str,
        request: Any,
        timeout: float = 1.0,
    ) -> Any:
        """Send a Raft AppendEntries.

        Args:
            peer_id: Target node
            request: AppendEntriesRequest object
            timeout: Response timeout

        Returns:
            AppendEntriesResponse
        """
        request_id = self._next_request_id()
        msg = WireMessage(
            msg_type=MessageType.APPEND_ENTRIES_REQUEST,
            name=request_id,
            payload=serialize(request),
        )

        fut: asyncio.Future[Any] = asyncio.get_event_loop().create_future()
        self._pending_requests[request_id] = fut

        try:
            if not await self._send_to_peer(peer_id, msg):
                raise ConnectionError(f"Failed to send to {peer_id}")
            return await asyncio.wait_for(fut, timeout=timeout)
        finally:
            self._pending_requests.pop(request_id, None)

    async def send_gossip(
        self,
        address: tuple[str, int],
        message: Any,
        updates: list[Any],
    ) -> bool:
        """Send a gossip message.

        Args:
            address: Target node address
            message: Gossip message (Ping, PingReq, Ack)
            updates: Membership updates to piggyback

        Returns:
            True if sent
        """
        # Determine message type
        from .data_plane.gossip import Ping, PingReq, Ack

        if isinstance(message, Ping):
            msg_type = MessageType.GOSSIP_PING
        elif isinstance(message, PingReq):
            msg_type = MessageType.GOSSIP_PING_REQ
        elif isinstance(message, Ack):
            msg_type = MessageType.GOSSIP_ACK
        else:
            msg_type = MessageType.GOSSIP_MEMBERSHIP

        payload = {"message": message, "updates": updates}
        msg = WireMessage(
            msg_type=msg_type,
            name="",
            payload=serialize(payload),
        )
        return await self._send_to_address(address, msg)

    async def send_state_update(
        self,
        address: tuple[str, int],
        entity_type: str,
        entity_id: str,
        state: bytes,
        *,
        wait_ack: bool = False,
        timeout: float = 5.0,
    ) -> bool:
        """Send a state update to a replica.

        Args:
            address: Target node address
            entity_type: Entity type
            entity_id: Entity ID
            state: Serialized state
            wait_ack: If True, wait for acknowledgment from replica
            timeout: Timeout for waiting ACK (only if wait_ack=True)

        Returns:
            True if sent (async) or acknowledged (sync)
        """
        if not wait_ack:
            # Fire-and-forget mode
            msg = WireMessage(
                msg_type=MessageType.STATE_UPDATE,
                name=f"{entity_type}:{entity_id}",
                payload=state,
            )
            return await self._send_to_address(address, msg)

        # Synchronous mode - wait for ACK
        self._request_counter += 1
        request_id = f"state-{self._node_id}-{self._request_counter}"

        msg = WireMessage(
            msg_type=MessageType.STATE_UPDATE,
            name=f"{entity_type}:{entity_id}:{request_id}",
            payload=state,
        )

        fut: asyncio.Future[dict] = asyncio.get_event_loop().create_future()
        self._pending_requests[request_id] = fut

        try:
            if not await self._send_to_address(address, msg):
                return False
            result = await asyncio.wait_for(fut, timeout=timeout)
            return result.get("success", False)
        except asyncio.TimeoutError:
            return False
        except Exception:
            return False
        finally:
            self._pending_requests.pop(request_id, None)

    def __repr__(self) -> str:
        peers = len(self._peers)
        return f"Transport(node={self._node_id}, peers={peers})"
