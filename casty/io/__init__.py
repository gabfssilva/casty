"""
Casty I/O - Network and I/O actors.

Submodules:
    tcp  - Akka-style TCP actors with backpressure and close semantics
    udp  - UDP transport actors for datagram communication
    quic - QUIC transport with stream multiplexing and 0-RTT support (optional)
           Install with: pip install casty[quic]
"""

from casty.io import tcp
from casty.io import udp

__all__ = ["tcp", "udp"]

# Optional QUIC support - requires aioquic
try:
    from casty.io import quic

    __all__.append("quic")
except ImportError:
    quic = None  # type: ignore
