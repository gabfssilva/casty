import pytest
from dataclasses import dataclass

from casty.remote.serializer import MsgPackSerializer


@dataclass
class SampleMessage:
    value: int
    name: str


class TestMsgPackSerializer:
    def test_encode_decode_primitive(self):
        s = MsgPackSerializer()
        data = s.encode({"key": "value", "num": 42})
        result = s.decode(data)
        assert result == {"key": "value", "num": 42}

    def test_encode_decode_bytes(self):
        s = MsgPackSerializer()
        data = s.encode(b"hello")
        result = s.decode(data)
        assert result == b"hello"

    def test_encode_decode_nested(self):
        s = MsgPackSerializer()
        obj = {"items": [1, 2, 3], "nested": {"a": "b"}}
        data = s.encode(obj)
        result = s.decode(data)
        assert result == obj
