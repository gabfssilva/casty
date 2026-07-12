from __future__ import annotations

import pytest

from casty.errors import ProtocolError
from casty.transport import frame as fr

MAX = 256 * 1024


def decoder() -> fr.Decoder:
    return fr.Decoder(max_frame_bytes=MAX)


def test_roundtrip_data_frame() -> None:
    original = fr.Frame(fr.FrameType.DATA, fr.Flags.SYN | fr.Flags.FIN, 7, b"hello")
    [decoded] = decoder().feed(fr.encode(original))
    assert decoded == original


def test_window_update_credits_in_length_field() -> None:
    original = fr.Frame(fr.FrameType.WINDOW_UPDATE, fr.Flags.NONE, 3, credits=65536)
    encoded = fr.encode(original)
    assert len(encoded) == fr.HEADER_SIZE  # no payload on the wire
    [decoded] = decoder().feed(encoded)
    assert decoded.credits == 65536 and decoded.payload == b""


def test_incremental_feed_across_boundaries() -> None:
    payload = b"x" * 1000
    encoded = fr.encode(fr.Frame(fr.FrameType.DATA, fr.Flags.NONE, 1, payload))
    d = decoder()
    assert d.feed(encoded[:5]) == []  # partial header
    assert d.feed(encoded[5:20]) == []  # partial payload
    [decoded] = d.feed(encoded[20:])
    assert decoded.payload == payload


def test_multiple_frames_in_one_feed() -> None:
    a = fr.encode(fr.Frame(fr.FrameType.DATA, fr.Flags.SYN, 1, b"a"))
    b = fr.encode(fr.Frame(fr.FrameType.PING, fr.Flags.NONE, 0, b"12345678"))
    frames = decoder().feed(a + b)
    assert [f.type for f in frames] == [fr.FrameType.DATA, fr.FrameType.PING]


def test_unknown_version_rejected() -> None:
    encoded = bytearray(fr.encode(fr.Frame(fr.FrameType.DATA, fr.Flags.NONE, 1, b"")))
    encoded[0] = 99
    with pytest.raises(ProtocolError, match="version"):
        decoder().feed(bytes(encoded))


def test_unknown_type_rejected() -> None:
    encoded = bytearray(fr.encode(fr.Frame(fr.FrameType.DATA, fr.Flags.NONE, 1, b"")))
    encoded[1] = 0x7F
    with pytest.raises(ProtocolError, match="frame type"):
        decoder().feed(bytes(encoded))


def test_reserved_flags_rejected() -> None:
    encoded = bytearray(fr.encode(fr.Frame(fr.FrameType.DATA, fr.Flags.NONE, 1, b"")))
    encoded[3] = 0x80
    with pytest.raises(ProtocolError, match="reserved flag"):
        decoder().feed(bytes(encoded))


def test_oversized_data_frame_rejected() -> None:
    header = fr.HEADER.pack(fr.PROTOCOL_VERSION, fr.FrameType.DATA, 0, 1, MAX + 1)
    with pytest.raises(ProtocolError, match="exceeds max_frame_bytes"):
        decoder().feed(header)


def test_ping_payload_must_be_8_bytes() -> None:
    header = fr.HEADER.pack(fr.PROTOCOL_VERSION, fr.FrameType.PING, 0, 0, 4)
    with pytest.raises(ProtocolError, match="PING"):
        decoder().feed(header + b"1234")
