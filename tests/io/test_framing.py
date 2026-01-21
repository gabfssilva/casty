import pytest
from casty.io.framing import RawFramer, LengthPrefixedFramer


class TestRawFramer:
    def test_feed_returns_data_as_single_frame(self):
        framer = RawFramer()
        frames = framer.feed(b"hello")
        assert frames == [b"hello"]

    def test_encode_returns_data_unchanged(self):
        framer = RawFramer()
        encoded = framer.encode(b"hello")
        assert encoded == b"hello"


class TestLengthPrefixedFramer:
    def test_encode_prepends_length(self):
        framer = LengthPrefixedFramer()
        encoded = framer.encode(b"hello")
        assert encoded == b"\x00\x00\x00\x05hello"

    def test_feed_single_complete_frame(self):
        framer = LengthPrefixedFramer()
        frames = framer.feed(b"\x00\x00\x00\x05hello")
        assert frames == [b"hello"]

    def test_feed_incomplete_frame_returns_empty(self):
        framer = LengthPrefixedFramer()
        frames = framer.feed(b"\x00\x00\x00\x05hel")
        assert frames == []

    def test_feed_incomplete_header_returns_empty(self):
        framer = LengthPrefixedFramer()
        frames = framer.feed(b"\x00\x00")
        assert frames == []

    def test_feed_multiple_frames(self):
        framer = LengthPrefixedFramer()
        data = b"\x00\x00\x00\x02hi\x00\x00\x00\x05world"
        frames = framer.feed(data)
        assert frames == [b"hi", b"world"]

    def test_feed_incremental(self):
        framer = LengthPrefixedFramer()

        frames = framer.feed(b"\x00\x00")
        assert frames == []

        frames = framer.feed(b"\x00\x05hel")
        assert frames == []

        frames = framer.feed(b"lo")
        assert frames == [b"hello"]

    def test_feed_frame_plus_partial(self):
        framer = LengthPrefixedFramer()
        data = b"\x00\x00\x00\x02hi\x00\x00\x00\x05wor"
        frames = framer.feed(data)
        assert frames == [b"hi"]

        frames = framer.feed(b"ld")
        assert frames == [b"world"]
