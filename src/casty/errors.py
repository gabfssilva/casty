from __future__ import annotations


class CastyError(Exception):
    """Base for all casty errors."""


class TransportError(CastyError):
    """Base for connection-level failures (framing, handshake, stream state)."""


class ConnectionLostError(TransportError):
    """The peer connection dropped with requests or streams still in flight."""


class StreamResetError(TransportError):
    """A multiplexed stream was reset before completing.

    Attributes
    ----------
    stream_id : int
        Transport-level id of the stream that was reset.
    """

    def __init__(self, stream_id: int, message: str = "") -> None:
        super().__init__(message or f"stream {stream_id} reset")
        self.stream_id = stream_id


class HandshakeError(TransportError):
    """The peer rejected the connection handshake (cluster name or TLS mismatch).

    Attributes
    ----------
    code : int
        Rejection code from the wire.
    reason : str
        Human-readable reason sent by the peer.
    """

    def __init__(self, code: int, reason: str) -> None:
        super().__init__(f"handshake rejected (code={code}): {reason}")
        self.code = code
        self.reason = reason


class ProtocolError(TransportError):
    """The peer violated the wire protocol. Fatal by contract: the connection dies."""


class CastyTimeoutError(CastyError):
    """A configured deadline elapsed (call, handshake, lock acquisition, ...)."""


class RemoteError(CastyError):
    """Peer answered with an ERROR envelope that maps to no specific error class.

    Attributes
    ----------
    code : int
        Error code from the envelope.
    remote_message : str
        Message produced on the remote node.
    """

    def __init__(self, code: int, message: str) -> None:
        super().__init__(f"remote error (code={code}): {message}")
        self.code = code
        self.remote_message = message


class SerializationError(CastyError):
    """A value could not be encoded or decoded against its declared type."""


class SerializationSchemaError(SerializationError):
    """A declared schema is invalid — raised at import time, when `@message`,
    `@actor` or `@service` validate their annotations."""


class ActorFailedError(CastyError):
    """The actor's handler raised; the supervisor kept/reset/stopped the
    activation and the original error surfaces to the caller.

    Attributes
    ----------
    actor : str
        Wire name of the actor class.
    key : str
        Key of the failing activation.
    remote_message : str
        `type: message` of the exception the handler raised.
    """

    def __init__(self, actor: str, key: str, message: str) -> None:
        super().__init__(f"{actor}/{key} failed: {message}")
        self.actor = actor
        self.key = key
        self.remote_message = message


class ActorUnavailableError(CastyError):
    """The call could not be routed: no known members, the owner is
    unreachable, or the owning node is draining or stopped."""


class ReentrancyError(CastyError):
    """An ask cycle (A -> B -> A) was detected via the call chain."""


class UnknownActorTypeError(CastyError):
    """The owner node does not have this actor class registered."""


class QuorumUnavailableError(CastyError):
    """Fewer than W replicas acknowledged a write (or R for a read). The
    handler's uncommitted mutation was rolled back."""


class RangeMovingError(CastyError):
    """The key's token range is being handed off; retry after the move."""
