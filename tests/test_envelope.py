import pytest
from dataclasses import dataclass

def test_envelope_creation():
    from casty.envelope import Envelope

    @dataclass
    class Ping:
        value: int

    env = Envelope(payload=Ping(42))
    assert env.payload == Ping(42)
    assert env.sender is None
    assert env.reply_to is None


def test_envelope_with_sender():
    from casty.envelope import Envelope

    env = Envelope(payload="hello", sender="actor/sender")
    assert env.payload == "hello"
    assert env.sender == "actor/sender"
