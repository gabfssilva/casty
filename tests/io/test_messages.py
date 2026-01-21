import pytest
from dataclasses import is_dataclass

from casty.io.messages import (
    Bind, Connect, Register, Write, Close,
    Bound, BindFailed, Connected, ConnectFailed,
    Received, PeerClosed, ErrorClosed, Aborted, WritingResumed,
)


def test_all_messages_are_dataclasses():
    messages = [
        Bind, Connect, Register, Write, Close,
        Bound, BindFailed, Connected, ConnectFailed,
        Received, PeerClosed, ErrorClosed, Aborted, WritingResumed,
    ]
    for msg in messages:
        assert is_dataclass(msg), f"{msg.__name__} is not a dataclass"


def test_bind_has_required_fields():
    from casty.ref import ActorRef
    from unittest.mock import MagicMock

    handler = MagicMock(spec=ActorRef)
    bind = Bind(handler=handler, port=8080)

    assert bind.handler is handler
    assert bind.port == 8080
    assert bind.host == "0.0.0.0"
    assert bind.framing is None


def test_connected_has_required_fields():
    from casty.ref import ActorRef
    from unittest.mock import MagicMock

    conn = MagicMock(spec=ActorRef)
    connected = Connected(
        connection=conn,
        remote_address=("127.0.0.1", 12345),
        local_address=("0.0.0.0", 8080),
    )

    assert connected.connection is conn
    assert connected.remote_address == ("127.0.0.1", 12345)
    assert connected.local_address == ("0.0.0.0", 8080)
