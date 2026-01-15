"""UDP Connection handler - manages a single peer connection."""

from dataclasses import dataclass, field


@dataclass
class PeerConnection:
    """Lightweight peer connection tracker for UDP server."""
    peer_addr: tuple[str, int]
    recv_buffer: bytearray = field(default_factory=bytearray)

    def buffer_datagram(self, data: bytes):
        """Buffer incoming datagram (for future fragmentation support)."""
        self.recv_buffer.extend(data)

    def clear_buffer(self):
        """Clear buffer after processing."""
        self.recv_buffer.clear()
