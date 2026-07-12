from __future__ import annotations

import ssl
from dataclasses import dataclass, field

from casty.serde.registry import message


@dataclass(frozen=True, slots=True)
class TLS:
    """TLS material for the whole cluster.

    With `ca` set, peers are verified in both roles; with `require_client_cert`,
    connections are mutually authenticated (mTLS). Hostname checking is
    disabled: cluster nodes are addressed by IP and authenticated by the CA,
    not by name.

    Parameters
    ----------
    cert : str
        Path to this node's PEM certificate chain.
    key : str
        Path to the certificate's PEM private key.
    ca : str | None
        Path to the CA bundle peers are verified against. None skips peer
        verification (encryption only).
    require_client_cert : bool
        Whether the server side demands a client certificate (mTLS). Only
        effective with `ca` set.
    """

    cert: str
    key: str
    ca: str | None = None
    require_client_cert: bool = True

    def server_context(self) -> ssl.SSLContext:
        """Build the `ssl.SSLContext` used when accepting connections."""
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(self.cert, self.key)
        if self.ca is not None:
            context.load_verify_locations(self.ca)
            if self.require_client_cert:
                context.verify_mode = ssl.CERT_REQUIRED
        return context

    def client_context(self) -> ssl.SSLContext:
        """Build the `ssl.SSLContext` used when dialing peers."""
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.load_cert_chain(self.cert, self.key)
        context.check_hostname = False
        if self.ca is not None:
            context.load_verify_locations(self.ca)
            context.verify_mode = ssl.CERT_REQUIRED
        else:
            context.verify_mode = ssl.CERT_NONE
        return context


@message(name="casty.CompressionConfig")
class CompressionConfig:
    """Per-connection compression, negotiated at handshake.

    Parameters
    ----------
    codecs : list[str] | None
        Codec names offered in preference order. None auto-detects the
        installed built-ins; an empty list disables compression.
    min_bytes : int
        Payloads below this many (uncompressed) bytes are sent as-is.
    """

    codecs: list[str] | None = None
    min_bytes: int = 4096


@message(name="casty.MembershipConfig")
class MembershipConfig:
    """Knobs of the membership layer: HyParView overlay, Plumtree broadcast and
    the failure detector.

    Parameters
    ----------
    active_view_size : int
        Peers this node keeps TCP connections to (HyParView active view).
    passive_view_size : int
        Standby peers for repairing the active view (passive view).
    active_rwl : int
        ARWL — initial ttl of a FORWARD_JOIN random walk.
    passive_rwl : int
        PRWL — SHUFFLE ttl / depth at which a walking join is also inserted
        into passive views.
    shuffle_interval : float
        Seconds between passive-view shuffles.
    shuffle_active_sample : int
        Active-view members included in each SHUFFLE.
    shuffle_passive_sample : int
        Passive-view members included in each SHUFFLE.
    suspicion_timeout : float
        Seconds a SUSPECT member has to refute before being declared DEAD.
    anti_entropy_interval : float
        Seconds between full-table SYNC exchanges with a random peer.
    tombstone_ttl : float
        Seconds a DEAD/LEFT record is retained so it wins merges against
        stale ALIVE gossip.
    graft_timeout : float
        Seconds Plumtree waits for a lazy-pushed message id before GRAFTing
        the tree edge.
    seen_ttl : float
        Seconds a broadcast message id is remembered for dedup.
    sweep_interval : float
        Seconds between timer sweeps (suspicion, grafts, tombstones).
    join_timeout : float
        Seconds a JOIN attempt against one seed may take before the next
        seed is tried.
    """

    active_view_size: int = 5
    passive_view_size: int = 30
    active_rwl: int = 6
    passive_rwl: int = 3
    shuffle_interval: float = 10.0
    shuffle_active_sample: int = 3
    shuffle_passive_sample: int = 4
    suspicion_timeout: float = 5.0
    anti_entropy_interval: float = 30.0
    tombstone_ttl: float = 60.0
    graft_timeout: float = 0.5
    seen_ttl: float = 60.0
    sweep_interval: float = 0.25
    join_timeout: float = 10.0


@message(name="casty.TransportConfig")
class TransportConfig:
    """Knobs of the transport layer: framing, flow control, keepalive and
    reconnection.

    Parameters
    ----------
    max_frame_bytes : int
        Largest frame accepted on the wire; larger payloads are chunked.
    max_message_bytes : int
        Largest single message (request, reply or stream element) accepted.
    initial_window_bytes : int
        Per-stream flow-control credit granted at open, in uncompressed bytes.
    max_concurrent_streams : int
        Streams a peer may hold open on one connection.
    handshake_timeout : float
        Seconds a new connection has to complete the handshake.
    request_timeout : float
        Default seconds an `ask` waits for its reply.
    connect_timeout : float
        Seconds a dial attempt may take before failing.
    keepalive_interval : float
        Seconds of quiet before a PING is sent.
    keepalive_timeout : float
        Seconds to wait for the PONG before declaring the connection dead.
    reconnect_base : float
        Initial backoff, in seconds, between reconnection attempts.
    reconnect_max : float
        Backoff ceiling for reconnection attempts.
    max_connections : int
        Connections the pool holds before refusing new dials.
    compression : CompressionConfig
        Compression negotiation settings.
    """

    max_frame_bytes: int = 256 * 1024
    max_message_bytes: int = 4 * 1024 * 1024
    initial_window_bytes: int = 256 * 1024
    max_concurrent_streams: int = 1024
    handshake_timeout: float = 5.0
    request_timeout: float = 10.0
    connect_timeout: float = 5.0
    keepalive_interval: float = 15.0
    keepalive_timeout: float = 5.0
    reconnect_base: float = 0.1
    reconnect_max: float = 15.0
    max_connections: int = 1024
    compression: CompressionConfig = field(default_factory=CompressionConfig)
