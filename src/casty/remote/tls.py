"""TLS configuration for inter-node communication.

Provides ``Config`` for enabling mutual TLS on all cluster TCP connections.
Use ``Config.from_paths`` for the common case (certificate files) or construct
``Config`` directly with pre-built ``ssl.SSLContext`` objects.
"""

from __future__ import annotations

import ssl
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    """TLS configuration holding server and client SSL contexts.

    Casty always uses mutual TLS: both sides of every connection present
    and verify certificates.  Use ``from_paths`` to build contexts from
    certificate files, or pass pre-built ``ssl.SSLContext`` objects directly.

    Parameters
    ----------
    server_context : ssl.SSLContext
        Context for inbound (server-side) connections.
    client_context : ssl.SSLContext
        Context for outbound (client-side) connections.

    Examples
    --------
    >>> tls = Config.from_paths(certfile="node.pem", cafile="ca.pem")
    >>> tls = Config(server_context=server_ctx, client_context=client_ctx)
    """

    server_context: ssl.SSLContext
    client_context: ssl.SSLContext

    @classmethod
    def from_paths(
        cls,
        *,
        certfile: str,
        cafile: str,
        keyfile: str | None = None,
    ) -> Config:
        """Build a mutual-TLS config from certificate file paths.

        Parameters
        ----------
        certfile : str
            Path to the node's certificate (PEM).  If *keyfile* is ``None``,
            the private key must be concatenated in this file.
        cafile : str
            Path to the CA certificate used to verify peer nodes.
        keyfile : str | None
            Path to the private key file.  When ``None``, the key is read
            from *certfile*.

        Returns
        -------
        Config

        Examples
        --------
        >>> tls = Config.from_paths(certfile="node.pem", cafile="ca.pem")
        >>> tls = Config.from_paths(
        ...     certfile="node.crt", keyfile="node.key", cafile="ca.pem",
        ... )
        """
        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(certfile, keyfile)
        server_context.verify_mode = ssl.CERT_REQUIRED
        server_context.load_verify_locations(cafile)

        client_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        client_context.load_cert_chain(certfile, keyfile)
        client_context.load_verify_locations(cafile)

        return cls(server_context=server_context, client_context=client_context)
