"""TCP transport layer for Casty cluster."""

from __future__ import annotations

import asyncio
import struct
from dataclasses import dataclass
from enum import IntEnum


class MessageType(IntEnum):
    """Types of wire messages."""

    ACTOR_MSG = 1  # Regular actor message
    LOOKUP_REQ = 2  # Actor lookup request
    LOOKUP_RES = 3  # Actor lookup response
    PEER_LIST = 4  # List of known peers (for gossip)
    ASK_REQ = 5  # Request-response ask
    ASK_RES = 6  # Response to ask
    # Election messages (Raft)
    VOTE_REQUEST = 7  # Candidate requests vote
    VOTE_RESPONSE = 8  # Response with vote
    HEARTBEAT = 9  # Leader heartbeat (legacy, now uses APPEND_ENTRIES_REQ)
    REGISTRY_SYNC_REQ = 10  # Follower requests registry
    REGISTRY_SYNC_RES = 11  # Leader sends registry
    # Log replication messages (Raft)
    APPEND_ENTRIES_REQ = 12  # Leader AppendEntries RPC
    APPEND_ENTRIES_RES = 13  # Follower AppendEntries response
    # Snapshot messages (Raft)
    INSTALL_SNAPSHOT_REQ = 14  # Leader InstallSnapshot RPC
    INSTALL_SNAPSHOT_RES = 15  # Follower InstallSnapshot response
    # Singleton messages
    SINGLETON_INFO_REQ = 16  # Request singleton info from cluster
    SINGLETON_INFO_RES = 17  # Response with singleton info


@dataclass(slots=True)
class WireMessage:
    """Message format for network communication."""

    msg_type: MessageType
    target_name: str
    payload: bytes


class Transport:
    """TCP communication with simple length-prefix framing."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self._closed = False

    @property
    def peer_info(self) -> tuple[str, int]:
        """Get peer address info."""
        return self.writer.get_extra_info("peername")

    async def send(self, msg: WireMessage) -> None:
        """Send a wire message with framing.

        Frame format:
        - msg_type: 1 byte
        - name_len: 2 bytes (big-endian)
        - name: name_len bytes (utf-8)
        - payload_len: 4 bytes (big-endian)
        - payload: payload_len bytes
        """
        if self._closed:
            raise ConnectionError("Transport is closed")

        name_bytes = msg.target_name.encode("utf-8")
        frame = (
            struct.pack(">B", msg.msg_type)
            + struct.pack(">H", len(name_bytes))
            + name_bytes
            + struct.pack(">I", len(msg.payload))
            + msg.payload
        )
        self.writer.write(frame)
        await self.writer.drain()

    async def recv(self) -> WireMessage:
        """Receive a wire message."""
        if self._closed:
            raise ConnectionError("Transport is closed")

        # Read message type
        msg_type_bytes = await self.reader.readexactly(1)
        msg_type = MessageType(struct.unpack(">B", msg_type_bytes)[0])

        # Read target name
        name_len_bytes = await self.reader.readexactly(2)
        name_len = struct.unpack(">H", name_len_bytes)[0]
        name = (await self.reader.readexactly(name_len)).decode("utf-8")

        # Read payload
        payload_len_bytes = await self.reader.readexactly(4)
        payload_len = struct.unpack(">I", payload_len_bytes)[0]
        payload = await self.reader.readexactly(payload_len)

        return WireMessage(msg_type=msg_type, target_name=name, payload=payload)

    async def close(self) -> None:
        """Close the transport."""
        if not self._closed:
            self._closed = True
            self.writer.close()
            await self.writer.wait_closed()
