"""
TCP Socket and TLS Options.
"""

import socket
import ssl
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, slots=True)
class SocketOptions:
    """TCP socket options.

    Args:
        keep_alive: Enable TCP keepalive probes.
        tcp_no_delay: Disable Nagle's algorithm for lower latency.
        send_buffer_size: SO_SNDBUF size in bytes.
        recv_buffer_size: SO_RCVBUF size in bytes.
        reuse_address: Allow binding to address in TIME_WAIT state.
        connect_timeout: Timeout for connect() in seconds.
        idle_timeout: Close connection after this many seconds of inactivity.
    """

    keep_alive: bool = False
    tcp_no_delay: bool = False
    send_buffer_size: int | None = None
    recv_buffer_size: int | None = None
    reuse_address: bool = True
    connect_timeout: float | None = None
    idle_timeout: float | None = None

    def apply(self, sock: socket.socket) -> None:
        """Apply these options to a socket."""
        if self.reuse_address:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        if self.keep_alive:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        if self.tcp_no_delay:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        if self.send_buffer_size is not None:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.send_buffer_size)

        if self.recv_buffer_size is not None:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.recv_buffer_size)


@dataclass(frozen=True, slots=True)
class TLSConfig:
    """TLS/SSL configuration.

    Args:
        certfile: Path to certificate file (PEM format).
        keyfile: Path to private key file (PEM format).
        ca_certs: Path to CA certificates file for verification.
        server_hostname: Hostname for SNI (Server Name Indication).
        verify: Whether to verify peer certificate.
        check_hostname: Whether to check hostname matches certificate.
        purpose: CLIENT_AUTH for servers, SERVER_AUTH for clients.
    """

    certfile: str | None = None
    keyfile: str | None = None
    password: str | None = None
    ca_certs: str | None = None
    server_hostname: str | None = None
    verify: bool = True
    check_hostname: bool = True
    purpose: Literal["client", "server"] = "client"

    def create_context(self) -> ssl.SSLContext:
        """Create an SSLContext from this configuration."""
        purpose = (
            ssl.Purpose.CLIENT_AUTH
            if self.purpose == "server"
            else ssl.Purpose.SERVER_AUTH
        )

        ctx = ssl.create_default_context(purpose)

        if self.certfile:
            ctx.load_cert_chain(
                certfile=self.certfile,
                keyfile=self.keyfile,
                password=self.password,
            )

        if self.ca_certs:
            ctx.load_verify_locations(cafile=self.ca_certs)

        if not self.verify:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        else:
            ctx.check_hostname = self.check_hostname
            ctx.verify_mode = ssl.CERT_REQUIRED

        return ctx


# Preset configurations


DEFAULT_OPTIONS = SocketOptions()

LOW_LATENCY_OPTIONS = SocketOptions(
    tcp_no_delay=True,
    keep_alive=True,
)

HIGH_THROUGHPUT_OPTIONS = SocketOptions(
    tcp_no_delay=False,  # Allow Nagle's algorithm
    send_buffer_size=256 * 1024,
    recv_buffer_size=256 * 1024,
)

SERVER_OPTIONS = SocketOptions(
    reuse_address=True,
    keep_alive=True,
)
