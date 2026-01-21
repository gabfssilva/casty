import pytest
from dataclasses import is_dataclass
from unittest.mock import MagicMock

from casty.remote.messages import (
    Listen, Connect, Listening, Connected, ListenFailed, ConnectFailed,
    Expose, Unexpose, Lookup, Exposed, Unexposed, LookupResult,
)


def test_all_messages_are_dataclasses():
    messages = [
        Listen, Connect, Listening, Connected, ListenFailed, ConnectFailed,
        Expose, Unexpose, Lookup, Exposed, Unexposed, LookupResult,
    ]
    for msg in messages:
        assert is_dataclass(msg), f"{msg.__name__} is not a dataclass"


def test_listen_defaults():
    listen = Listen(port=9000)
    assert listen.port == 9000
    assert listen.host == "0.0.0.0"
    assert listen.serializer is None


def test_connect_required_fields():
    connect = Connect(host="localhost", port=9000)
    assert connect.host == "localhost"
    assert connect.port == 9000


def test_lookup_result_optional_ref():
    result = LookupResult(ref=None)
    assert result.ref is None

    mock_ref = MagicMock()
    result = LookupResult(ref=mock_ref)
    assert result.ref is mock_ref
