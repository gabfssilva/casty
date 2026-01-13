"""Peer connection management and gossip protocol mixin."""

from __future__ import annotations

import asyncio
import logging
import ssl
from typing import TYPE_CHECKING

from ..serialize import deserialize, serialize
from ..transport import MessageType, Transport, WireMessage

if TYPE_CHECKING:
    from ..distributed import ClusterState

log = logging.getLogger(__name__)


class PeerMixin:
    """Peer connection management and gossip protocol."""

    _cluster: ClusterState
    _ssl_context: ssl.SSLContext | None
    _receive_tasks: set[asyncio.Task[None]]

    async def _receive_loop(self, transport: Transport) -> None:
        """Override in main class - processes incoming messages."""
        raise NotImplementedError

    async def _connect_peer(self, host: str, port: int) -> Transport:
        """Connect to a peer node."""
        reader, writer = await asyncio.open_connection(host, port, ssl=self._ssl_context)
        transport = Transport(reader, writer)
        self._cluster.peers[(host, port)] = transport

        # Start receive loop for this peer
        task = asyncio.create_task(self._receive_loop(transport))
        self._receive_tasks.add(task)
        task.add_done_callback(self._receive_tasks.discard)

        # Announce ourselves to the peer
        await self._send_announce(transport)

        return transport

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle incoming connection from another node."""
        transport = Transport(reader, writer)
        peer_info = transport.peer_info
        log.info(f"Accepted connection from {peer_info}")

        # Store peer
        self._cluster.peers[peer_info] = transport

        # Announce ourselves (with timeout to avoid hanging if peer disconnects)
        try:
            await asyncio.wait_for(self._send_announce(transport), timeout=5.0)
        except (asyncio.TimeoutError, ConnectionError, OSError) as e:
            log.warning(f"Failed to announce to {peer_info}: {e}")
            self._cluster.peers.pop(peer_info, None)
            try:
                await transport.close()
            except Exception:
                pass
            return

        # Start receive loop in background (non-blocking to allow more connections)
        task = asyncio.create_task(self._receive_loop(transport))
        self._receive_tasks.add(task)
        task.add_done_callback(self._receive_tasks.discard)

    async def _send_announce(self, transport: Transport) -> None:
        """Announce our node_id and address to a peer."""
        announce_data = serialize({
            "node_id": self._cluster.node_id,
            "address": [self._cluster.host, self._cluster.port],
        })
        msg = WireMessage(
            msg_type=MessageType.PEER_LIST,  # Reuse for node announcement
            target_name=self._cluster.node_id,
            payload=announce_data,
        )
        await transport.send(msg)

    async def _handle_peer_announce(self, transport: Transport, msg: WireMessage) -> None:
        """Handle node announcement from peer and propagate peer list."""
        data = deserialize(msg.payload, {})
        peer_node_id = data["node_id"]
        peer_address = data.get("address")  # (host, port) if provided

        # Register this node
        is_new = peer_node_id not in self._cluster.known_nodes
        self._cluster.known_nodes[peer_node_id] = transport
        log.debug(f"Node {self._cluster.node_id[:8]}: Discovered peer {peer_node_id[:8]}")

        # If new peer, tell them about other known peers with their addresses
        if is_new and len(self._cluster.known_nodes) > 1:
            # Build peer list with addresses from our peer registry
            peers_info = []
            for nid, t in self._cluster.known_nodes.items():
                if nid != peer_node_id:
                    # Get address from peer_addresses registry
                    addr = self._cluster.peer_addresses.get(nid)
                    if addr:
                        peers_info.append({"node_id": nid, "host": addr[0], "port": addr[1]})

            if peers_info:
                peer_list_data = serialize({
                    "peers": [p["node_id"] for p in peers_info],
                    "addresses": peers_info,
                })
                peer_list_msg = WireMessage(
                    msg_type=MessageType.PEER_LIST,
                    target_name="__peer_list__",
                    payload=peer_list_data,
                )
                try:
                    await transport.send(peer_list_msg)
                except Exception:
                    pass

        # Store this peer's address if provided
        if peer_address:
            self._cluster.peer_addresses[peer_node_id] = tuple(peer_address)

    async def _handle_peer_list(self, data: dict) -> None:
        """Connect to peers we don't know about yet."""
        addresses = data.get("addresses", [])
        for peer_info in addresses:
            peer_id = peer_info["node_id"]
            if peer_id not in self._cluster.known_nodes and peer_id != self._cluster.node_id:
                host = peer_info["host"]
                port = peer_info["port"]
                try:
                    log.debug(f"Node {self._cluster.node_id[:8]}: Connecting to peer {peer_id[:8]} at {host}:{port}")
                    await self._connect_peer(host, port)
                except Exception as e:
                    log.debug(f"Failed to connect to peer {peer_id[:8]}: {e}")
