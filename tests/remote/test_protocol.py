import pytest
from casty.remote.protocol import RemoteEnvelope, RemoteError
from casty.remote.serializer import MsgPackSerializer


class TestRemoteEnvelope:
    def test_deliver_envelope(self):
        env = RemoteEnvelope(
            type="deliver",
            target="counter",
            payload=b"data",
        )
        assert env.type == "deliver"
        assert env.target == "counter"
        assert env.payload == b"data"

    def test_ask_envelope(self):
        env = RemoteEnvelope(
            type="ask",
            target="counter",
            payload=b"data",
            correlation_id="123",
        )
        assert env.correlation_id == "123"

    def test_reply_envelope(self):
        env = RemoteEnvelope(
            type="reply",
            correlation_id="123",
            payload=b"result",
        )
        assert env.type == "reply"

    def test_error_envelope(self):
        env = RemoteEnvelope(
            type="error",
            correlation_id="123",
            error="Actor not found",
        )
        assert env.error == "Actor not found"

    def test_serialize_deserialize(self):
        s = MsgPackSerializer()
        env = RemoteEnvelope(
            type="ask",
            target="counter",
            payload=b"data",
            correlation_id="123",
        )
        data = s.encode(env.to_dict())
        restored = RemoteEnvelope.from_dict(s.decode(data))
        assert restored.type == env.type
        assert restored.target == env.target
        assert restored.payload == env.payload
        assert restored.correlation_id == env.correlation_id

    def test_to_dict_excludes_none(self):
        env = RemoteEnvelope(type="deliver", target="x")
        d = env.to_dict()
        assert "correlation_id" not in d
        assert "payload" not in d


def test_remote_error():
    err = RemoteError("test error")
    assert str(err) == "test error"
