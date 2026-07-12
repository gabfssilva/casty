from __future__ import annotations

import zlib

import pytest

from casty.config import CompressionConfig, TransportConfig
from casty.errors import ProtocolError
from casty.transport import mux as mx


def pair(
    config: TransportConfig | None = None,
    compressor: mx.Compressor | None = None,
) -> tuple[mx.Mux, mx.Mux]:
    return (
        mx.Mux(initiator=True, config=config, compressor=compressor),
        mx.Mux(initiator=False, config=config, compressor=compressor),
    )


def pump(source: mx.Mux, sink: mx.Mux) -> list[mx.Event]:
    return sink.feed(source.data_to_send())


def test_open_send_receive_fin() -> None:
    client, server = pair()
    sid = client.open_stream()
    assert sid == 1
    client.send_data(sid, b"hello", fin=True)
    events = pump(client, server)
    assert events == [
        mx.StreamOpened(sid),
        mx.DataReceived(sid, b"hello"),
        mx.StreamFinished(sid),
    ]


def test_bidirectional_streams_have_distinct_parity() -> None:
    client, server = pair()
    client_sid = client.open_stream()
    server_sid = server.open_stream()
    assert client_sid % 2 == 1 and server_sid % 2 == 0


def test_reply_on_same_stream() -> None:
    client, server = pair()
    sid = client.open_stream()
    client.send_data(sid, b"ping?")
    pump(client, server)
    server.send_data(sid, b"pong!", fin=True)
    events = pump(server, client)
    assert mx.DataReceived(sid, b"pong!") in events


def test_flow_control_blocks_and_resumes() -> None:
    config = TransportConfig(initial_window_bytes=8, max_frame_bytes=4)
    client, server = pair(config)
    sid = client.open_stream()
    client.send_data(sid, b"12345678")
    assert client.send_window(sid) == 0
    with pytest.raises(ProtocolError, match="exceeds window"):
        client.send_data(sid, b"x")
    events = pump(client, server)
    received = b"".join(e.data for e in events if isinstance(e, mx.DataReceived))
    assert received == b"12345678"
    server.consume(sid, len(received))
    events = pump(server, client)
    assert mx.WindowAvailable(sid) in events
    assert client.send_window(sid) == 8


def test_peer_violating_window_kills_connection() -> None:
    config = TransportConfig(initial_window_bytes=4)
    client, server = pair(config)
    sid = client.open_stream()
    # bypass the client's own window bookkeeping to forge a violation
    from casty.transport import frame as fr

    forged = fr.encode(fr.Frame(fr.FrameType.DATA, fr.Flags.SYN, sid, b"way too much data"))
    with pytest.raises(ProtocolError, match="recv window"):
        server.feed(forged)


def test_large_transfer_chunks_and_interleaves() -> None:
    config = TransportConfig(initial_window_bytes=1024 * 1024, max_frame_bytes=1024)
    client, server = pair(config)
    bulk = client.open_stream()
    chat = client.open_stream()
    client.send_data(bulk, b"B" * 10_000)
    client.send_data(chat, b"small", fin=True)
    events = pump(client, server)
    # the small message arrives even though a 10x larger transfer was queued first
    assert mx.DataReceived(chat, b"small") in events
    received = [e for e in events if isinstance(e, mx.DataReceived) and e.stream_id == bulk]
    bulk_bytes = sum(len(e.data) for e in received)
    assert bulk_bytes == 10_000


def test_reset_stream() -> None:
    client, server = pair()
    sid = client.open_stream()
    client.send_data(sid, b"start")
    pump(client, server)
    server.reset(sid)
    events = pump(server, client)
    assert events == [mx.StreamRejected(sid)]
    with pytest.raises(ProtocolError, match="unknown stream"):
        client.send_data(sid, b"more")


def test_max_concurrent_streams_refused_with_rst() -> None:
    config = TransportConfig(max_concurrent_streams=1)
    client, server = pair(config)
    first = client.open_stream()
    second = client.open_stream()
    client.send_data(first, b"a")
    client.send_data(second, b"b")
    events = pump(client, server)
    assert mx.StreamOpened(first) in events
    assert mx.StreamOpened(second) not in events
    events = pump(server, client)
    assert mx.StreamRejected(second) in events


def test_ping_pong() -> None:
    client, server = pair()
    client.ping(b"8bytes!!")
    pump(client, server)  # server auto-answers
    events = pump(server, client)
    assert events == [mx.PongReceived(b"8bytes!!")]


def test_go_away() -> None:
    client, server = pair()
    client.go_away(b"maintenance")
    events = pump(client, server)
    assert events == [mx.ConnectionClosed(b"maintenance")]
    with pytest.raises(ProtocolError, match="closed"):
        server.open_stream()


def test_wrong_parity_rejected() -> None:
    _client, server = pair()
    from casty.transport import frame as fr

    forged = fr.encode(fr.Frame(fr.FrameType.DATA, fr.Flags.SYN, 2, b"x"))
    with pytest.raises(ProtocolError, match="parity"):
        server.feed(forged)  # server expects odd ids from the initiator


def test_stream_id_reuse_rejected() -> None:
    client, server = pair()
    sid = client.open_stream()
    client.send_data(sid, b"x", fin=True)
    pump(client, server)
    server.send_data(sid, b"reply", fin=True)  # legal: closes the other half
    pump(server, client)
    from casty.transport import frame as fr

    reopened = fr.encode(fr.Frame(fr.FrameType.DATA, fr.Flags.SYN, sid, b"again"))
    with pytest.raises(ProtocolError, match="reused"):
        server.feed(reopened)


class ZlibCodec:
    name = "zlib"

    def compress(self, data: bytes) -> bytes:
        return zlib.compress(data)

    def decompress(self, data: bytes, max_size: int) -> bytes:
        out = zlib.decompress(data)
        if len(out) > max_size:
            raise ProtocolError("decompression bomb")
        return out


def test_compression_roundtrip_above_threshold() -> None:
    config = TransportConfig(compression=CompressionConfig(min_bytes=64))
    client, server = pair(config, compressor=ZlibCodec())
    sid = client.open_stream()
    payload = b"A" * 10_000  # highly compressible
    client.send_data(sid, payload, fin=True)
    wire = client.data_to_send()
    assert len(wire) < len(payload)  # actually compressed on the wire
    events = server.feed(wire)
    received = b"".join(e.data for e in events if isinstance(e, mx.DataReceived))
    assert received == payload


def test_small_payload_not_compressed() -> None:
    config = TransportConfig(compression=CompressionConfig(min_bytes=64))
    client, _server = pair(config, compressor=ZlibCodec())
    sid = client.open_stream()
    client.send_data(sid, b"tiny", fin=True)
    wire = client.data_to_send()
    from casty.transport import frame as fr

    frames = fr.Decoder(max_frame_bytes=config.max_frame_bytes).feed(wire)
    assert all(not f.flags & fr.Flags.COMPRESSED for f in frames)


def test_window_counts_uncompressed_bytes() -> None:
    config = TransportConfig(
        initial_window_bytes=20_000, compression=CompressionConfig(min_bytes=64)
    )
    client, server = pair(config, compressor=ZlibCodec())
    sid = client.open_stream()
    client.send_data(sid, b"A" * 10_000)
    assert client.send_window(sid) == 10_000  # decremented by uncompressed size
    pump(client, server)
