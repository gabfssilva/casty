from __future__ import annotations

import ssl
from dataclasses import dataclass, field

from casty.serde.registry import message


@dataclass(frozen=True, slots=True)
class TLS:
    """TLS material for the whole cluster. With `ca` set, peers are verified in
    both roles; with `require_client_cert`, connections are mutually
    authenticated (mTLS). Hostname checking is disabled: cluster nodes are
    addressed by IP and authenticated by the CA, not by name."""

    cert: str
    key: str
    ca: str | None = None
    require_client_cert: bool = True

    def server_context(self) -> ssl.SSLContext:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(self.cert, self.key)
        if self.ca is not None:
            context.load_verify_locations(self.ca)
            if self.require_client_cert:
                context.verify_mode = ssl.CERT_REQUIRED
        return context

    def client_context(self) -> ssl.SSLContext:
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
    codecs: list[str] | None = None  # None = auto (installed built-ins); [] = disabled
    min_bytes: int = 4096


@message(name="casty.MembershipConfig")
class MembershipConfig:
    active_view_size: int = 5
    passive_view_size: int = 30
    active_rwl: int = 6  # ARWL: initial FORWARD_JOIN ttl
    passive_rwl: int = 3  # PRWL: SHUFFLE ttl / passive-insertion depth
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
