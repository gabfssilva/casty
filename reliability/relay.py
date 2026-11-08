"""What a node of a run dials goes through here, as through the gateway in front of a machine of a cloud that cuts its
connections now and then.

The relay listens on the loopback interface, once for each address the node dials, and forwards each connection to the
address. It runs on a thread and an event loop of its own, so the traffic it carries does not wait for the loop of the
node, which the actor bodies keep busy. The run asks it to cut the connections it carries to some addresses, in one of
two ways (`cut`):

- `sever` resets both sockets at once, without a goodbye: each side reads an error, and both dial again at once;
- `strand` holds what the node writes for a moment, resets the socket of the node alone, and leaves the one of the peer
  open: the peer still holds a connection that looks alive, while the node dials it again, and what was held reaches
  the peer later, on that old connection, `late` seconds after the node lost it.

Either way the relay can then be `deaf` for a while: a connection the node dials meanwhile is accepted and goes nowhere,
as a relay whose way out does not open, and is reset when that time is over. The dials of the peer still arrive.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import threading
import time
from functools import partial

STRANDED = 0.3
"""Seconds a stranded connection holds what the node writes before the node loses it."""


class Relay:
    """The connections a node dials, each through a listener of this relay for the address dialed."""

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        threading.Thread(target=self._loop.run_forever, name="relay", daemon=True).start()
        self._lock = threading.Lock()
        self._listening: dict[str, str] = {}
        # Touched on the loop of the relay alone.
        self._carried: set[_Carried] = set()
        self._servers: list[asyncio.Server] = []
        self._deaf: dict[str, float] = {}

    def through(self, address: str, /) -> str:
        """The address the node dials to reach `address`: a listener of this relay, made the first time."""
        with self._lock:
            if (local := self._listening.get(address)) is None:
                # asyncio sets TCP_NODELAY only on the sockets of a listener whose proto is IPPROTO_TCP.
                listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
                listener.bind(("127.0.0.1", 0))
                listener.listen()
                listener.setblocking(False)
                local = f"127.0.0.1:{listener.getsockname()[1]}"
                self._listening[address] = local
                asyncio.run_coroutine_threadsafe(self._serve(listener, address), self._loop).result()
        return local

    def cut(self, addresses: frozenset[str], /, *, strand: bool, late: float, deaf: float) -> int:
        """Cut every connection carried to one of `addresses`, stay `deaf` to them for that many seconds, and answer
        how many connections there were."""
        cutting = asyncio.run_coroutine_threadsafe(self._cut(addresses, strand, late, deaf), self._loop)
        return cutting.result()

    async def _serve(self, listener: socket.socket, address: str, /) -> None:
        self._servers.append(await asyncio.start_server(partial(self._carry, address), sock=listener))

    async def _carry(self, address: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, /) -> None:
        if (deaf := self._deaf.get(address, 0.0) - time.monotonic()) > 0:
            await asyncio.sleep(deaf)
            writer.transport.abort()
            return
        host, port = address.rsplit(":", 1)
        try:
            upstream = await asyncio.open_connection(host, int(port))
        except OSError:
            writer.transport.abort()
            return
        carried = _Carried(address, (reader, writer), upstream)
        self._carried.add(carried)
        try:
            await carried.run()
        finally:
            self._carried.discard(carried)

    async def _cut(self, addresses: frozenset[str], strand: bool, late: float, deaf: float, /) -> int:
        until = time.monotonic() + deaf
        for address in addresses:
            self._deaf[address] = until
        cut = [carried for carried in self._carried if carried.address in addresses]
        for carried in cut:
            if strand:
                carried.strand(late)
            else:
                carried.sever()
        return len(cut)


type _Pair = tuple[asyncio.StreamReader, asyncio.StreamWriter]


class _Carried:
    """One connection the node dialed: its end on the node's side and its end on the peer's."""

    def __init__(self, address: str, node: _Pair, peer: _Pair, /) -> None:
        self.address = address
        self._node = node
        self._peer = peer
        self._held: bytearray | None = None
        self._done = asyncio.Event()
        self._stranding: asyncio.Future[None] | None = None

    async def run(self) -> None:
        pumps = (
            asyncio.ensure_future(self._pump(self._node[0], self._peer[1], outward=True)),
            asyncio.ensure_future(self._pump(self._peer[0], self._node[1], outward=False)),
        )
        await self._done.wait()
        for pump in pumps:
            pump.cancel()
        for _, writer in (self._node, self._peer):
            writer.transport.abort()

    def sever(self) -> None:
        self._done.set()

    def strand(self, late: float) -> None:
        self._held = bytearray()
        self._stranding = asyncio.ensure_future(self._stranded(late))

    async def _stranded(self, late: float) -> None:
        await asyncio.sleep(STRANDED)
        self._node[1].transport.abort()
        await asyncio.sleep(late)
        held = self._held or bytearray()
        with contextlib.suppress(OSError):
            self._peer[1].write(bytes(held))
            await self._peer[1].drain()
            # Long enough for the peer to read what was late before its socket resets.
            await asyncio.sleep(1.0)
        self._done.set()

    async def _pump(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, /, *, outward: bool) -> None:
        try:
            while data := await reader.read(64 * 1024):
                if self._held is not None:
                    # Stranded: what the node writes waits to arrive late, and nothing reaches the node any more.
                    if outward:
                        self._held += data
                    continue
                writer.write(data)
                await writer.drain()
        except OSError:
            pass
        if self._held is None:
            self._done.set()
