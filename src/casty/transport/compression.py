from __future__ import annotations

import zlib

from casty.config import CompressionConfig
from casty.errors import ProtocolError


class ZlibCodec:
    name = "zlib"

    def compress(self, data: bytes) -> bytes:
        return zlib.compress(data, level=1)

    def decompress(self, data: bytes, max_size: int) -> bytes:
        decompressor = zlib.decompressobj()
        out = decompressor.decompress(data, max_size)
        if decompressor.unconsumed_tail:
            raise ProtocolError("decompressed payload exceeds limit (bomb?)")
        return out


class Lz4Codec:
    name = "lz4"

    def compress(self, data: bytes) -> bytes:
        import lz4.frame

        compressed: bytes = lz4.frame.compress(data)
        return compressed

    def decompress(self, data: bytes, max_size: int) -> bytes:
        import lz4.frame

        out: bytes = lz4.frame.decompress(data)
        if len(out) > max_size:
            raise ProtocolError("decompressed payload exceeds limit (bomb?)")
        return out


class ZstdCodec:
    name = "zstd"

    def compress(self, data: bytes) -> bytes:
        import zstandard

        compressed: bytes = zstandard.ZstdCompressor().compress(data)
        return compressed

    def decompress(self, data: bytes, max_size: int) -> bytes:
        import zstandard

        out: bytes = zstandard.ZstdDecompressor().decompress(data, max_output_size=max_size)
        return out


def _installed() -> dict[str, type]:
    codecs: dict[str, type] = {"zlib": ZlibCodec}
    try:
        import lz4.frame  # noqa: F401

        codecs["lz4"] = Lz4Codec
    except ImportError:
        pass
    try:
        import zstandard  # noqa: F401

        codecs["zstd"] = ZstdCodec
    except ImportError:
        pass
    return codecs


_AUTO_PREFERENCE = ["zstd", "lz4", "zlib"]


def offered(config: CompressionConfig) -> list[str]:
    """Codec names to offer in HELLO, in preference order."""
    available = _installed()
    if config.codecs is None:
        return [name for name in _AUTO_PREFERENCE if name in available]
    return [name for name in config.codecs if name in available]


def negotiate(initiator_offer: list[str], config: CompressionConfig) -> str | None:
    """Acceptor side: first codec from the initiator's list that we also support."""
    ours = set(offered(config))
    for name in initiator_offer:
        if name in ours:
            return name
    return None


def make(name: str) -> ZlibCodec | Lz4Codec | ZstdCodec:
    codec_cls = _installed().get(name)
    if codec_cls is None:
        raise ProtocolError(f"negotiated unknown compression codec {name!r}")
    instance: ZlibCodec | Lz4Codec | ZstdCodec = codec_cls()
    return instance
