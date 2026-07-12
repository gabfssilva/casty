from __future__ import annotations


class CastyError(Exception):
    """Base for all casty errors."""


class TransportError(CastyError):
    pass


class ConnectionLostError(TransportError):
    pass


class StreamResetError(TransportError):
    def __init__(self, stream_id: int, message: str = "") -> None:
        super().__init__(message or f"stream {stream_id} reset")
        self.stream_id = stream_id


class HandshakeError(TransportError):
    def __init__(self, code: int, reason: str) -> None:
        super().__init__(f"handshake rejected (code={code}): {reason}")
        self.code = code
        self.reason = reason


class ProtocolError(TransportError):
    pass


class CastyTimeoutError(CastyError):
    pass


class RemoteError(CastyError):
    """Peer answered with an ERROR envelope."""

    def __init__(self, code: int, message: str) -> None:
        super().__init__(f"remote error (code={code}): {message}")
        self.code = code
        self.remote_message = message


class SerializationError(CastyError):
    pass


class SerializationSchemaError(SerializationError):
    pass


class ActorFailedError(CastyError):
    """The actor's handler raised; the supervisor kept/reset/stopped the
    activation and the original error surfaces to the caller."""

    def __init__(self, actor: str, key: str, message: str) -> None:
        super().__init__(f"{actor}/{key} failed: {message}")
        self.actor = actor
        self.key = key
        self.remote_message = message


class ActorUnavailableError(CastyError):
    """The owner node is draining or stopped."""


class ReentrancyError(CastyError):
    """An ask cycle (A -> B -> A) was detected via the call chain."""


class UnknownActorTypeError(CastyError):
    """The owner node does not have this actor class registered."""


class QuorumUnavailableError(CastyError):
    """Fewer than W replicas acknowledged a write (or R for a read)."""


class RangeMovingError(CastyError):
    """The key's token range is being handed off; retry after the move."""
