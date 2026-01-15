"""
QUIC Options and TLS Configuration.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from aioquic.quic.configuration import QuicConfiguration


@dataclass(frozen=True, slots=True)
class QuicOptions:
    """QUIC connection options.

    Args:
        alpn_protocols: List of ALPN protocols to advertise.
        max_datagram_size: Maximum datagram size (for RFC 9221 datagrams).
        idle_timeout: Connection idle timeout in seconds.
        max_stream_data_bidi_local: Max flow control for locally-initiated bidi streams.
        max_stream_data_bidi_remote: Max flow control for remotely-initiated bidi streams.
        max_stream_data_uni: Max flow control for unidirectional streams.
        max_streams_bidi: Maximum concurrent bidirectional streams.
        max_streams_uni: Maximum concurrent unidirectional streams.
        enable_datagrams: Enable unreliable datagram support (RFC 9221).
        initial_rtt: Initial RTT estimate in milliseconds.
        verify_peer: Verify peer certificate (client mode).
        max_data: Maximum connection-level flow control.
        quic_logger: Enable QUIC event logging.
    """

    alpn_protocols: tuple[str, ...] = ("h3",)
    max_datagram_size: int = 65535
    idle_timeout: float = 30.0
    max_stream_data_bidi_local: int = 1048576  # 1MB
    max_stream_data_bidi_remote: int = 1048576
    max_stream_data_uni: int = 1048576
    max_streams_bidi: int = 100
    max_streams_uni: int = 100
    enable_datagrams: bool = False
    initial_rtt: float = 0.1  # 100ms in seconds
    verify_peer: bool = True
    max_data: int = 1048576  # 1MB
    quic_logger: bool = False

    def apply_to_config(self, config: "QuicConfiguration") -> None:
        """Apply these options to an aioquic QuicConfiguration."""
        config.alpn_protocols = list(self.alpn_protocols)
        config.idle_timeout = self.idle_timeout
        config.max_datagram_frame_size = (
            self.max_datagram_size if self.enable_datagrams else None
        )

        # Flow control settings
        config.max_stream_data_bidirectional_local = self.max_stream_data_bidi_local
        config.max_stream_data_bidirectional_remote = self.max_stream_data_bidi_remote
        config.max_stream_data_unidirectional = self.max_stream_data_uni

        # Stream limits
        config.max_streams_bidirectional = self.max_streams_bidi
        config.max_streams_unidirectional = self.max_streams_uni

        # Connection-level flow control
        config.max_data = self.max_data

        # Verify mode (client only)
        if not self.verify_peer:
            config.verify_mode = None


@dataclass(frozen=True, slots=True)
class TlsCertificateConfig:
    """TLS certificate configuration for QUIC.

    For servers, certfile and keyfile are required.
    For clients, ca_certs is used for server verification.

    Args:
        certfile: Path to certificate file (PEM format).
        keyfile: Path to private key file (PEM format).
        password: Password for encrypted key file.
        ca_certs: Path to CA certificates for verification.
        verify_mode: Client certificate verification mode (server only).
    """

    certfile: str | None = None
    keyfile: str | None = None
    password: str | None = None
    ca_certs: str | None = None
    verify_mode: Literal["none", "optional", "required"] = "none"

    def load_into_config(self, config: "QuicConfiguration") -> None:
        """Load certificate into aioquic configuration."""
        if self.certfile and self.keyfile:
            config.load_cert_chain(
                certfile=self.certfile,
                keyfile=self.keyfile,
                password=self.password,
            )

        if self.ca_certs:
            config.load_verify_locations(self.ca_certs)


# ============================================================================
# PRESET CONFIGURATIONS
# ============================================================================


DEFAULT_OPTIONS = QuicOptions()

LOW_LATENCY_OPTIONS = QuicOptions(
    idle_timeout=10.0,
    initial_rtt=0.05,  # 50ms
    max_stream_data_bidi_local=262144,  # 256KB
    max_stream_data_bidi_remote=262144,
)

HIGH_THROUGHPUT_OPTIONS = QuicOptions(
    max_stream_data_bidi_local=4194304,  # 4MB
    max_stream_data_bidi_remote=4194304,
    max_stream_data_uni=4194304,
    max_data=16777216,  # 16MB
    max_streams_bidi=256,
    max_streams_uni=256,
)

DATAGRAM_OPTIONS = QuicOptions(
    enable_datagrams=True,
    max_datagram_size=1200,  # Conservative MTU
)

SERVER_OPTIONS = QuicOptions(
    idle_timeout=60.0,  # Longer timeout for servers
    max_streams_bidi=1000,
    max_streams_uni=1000,
)
