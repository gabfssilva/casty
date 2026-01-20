import pytest
from dataclasses import fields, is_dataclass


def test_message_creates_dataclass():
    from casty.message import message

    @message
    class Ping:
        value: int

    assert is_dataclass(Ping)
    instance = Ping(value=42)
    assert instance.value == 42


def test_message_is_serializable():
    from casty.message import message
    from casty.serializable import serialize, deserialize

    @message
    class Ping:
        value: int

    original = Ping(value=42)
    data = serialize(original)
    restored = deserialize(data)

    assert restored.value == 42


def test_message_readonly_false_by_default():
    from casty.message import message

    @message
    class Increment:
        amount: int

    assert Increment.__readonly__ is False


def test_message_readonly_true():
    from casty.message import message

    @message(readonly=True)
    class Get:
        pass

    assert Get.__readonly__ is True
