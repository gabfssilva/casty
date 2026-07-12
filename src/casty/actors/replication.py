"""Primary-based state replication (specs 05 + 09). The state of an actor
instance is a set of pages; a commit carries only the pages the handler changed.
HLC per commit, W-quorum acks with minority fencing, activation handshake, read
repair, handoff.

A delta only applies onto the exact basis it was computed against, so a replica
that is *behind* the writer refuses it (CODE_NEED_FULL) and gets the whole page
set instead; one that knows *newer* history fences the writer (CODE_STALE_WRITE).
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
import time
import typing
import uuid
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field

from casty.actors.registry import ActorInfo, info_by_name
from casty.errors import (
    CastyError,
    QuorumUnavailableError,
    RangeMovingError,
    RemoteError,
    SerializationError,
)
from casty.placement.ring import Ring, key_token
from casty.serde import codec
from casty.serde.registry import message

logger = logging.getLogger("casty.replication")

REPLICATE = 0x60
FETCH_STATE = 0x61
FETCH_PAGES = 0x62

CODE_STALE_WRITE = 20
CODE_NEED_FULL = 21

# measured msgpack framing of one Page / one PageRef, without its key and data: the
# embedded Hlc is a message of its own and costs most of it
_PAGE_OVERHEAD = 96
_REF_OVERHEAD = 96
_MESSAGE_SLACK = 4096  # Replicate framing around the pages (actor, key, hlcs, flags)
_FETCH_CONCURRENCY = 4  # page batches in flight while pulling a state


@message(name="casty.Hlc")
class Hlc:
    millis: int
    counter: int
    node: uuid.UUID

    def order(self) -> tuple[int, int, bytes]:
        return (self.millis, self.counter, self.node.bytes)


def newer(a: Hlc | None, b: Hlc | None) -> bool:
    """True if `a` is strictly newer than `b` (None = no history)."""
    if a is None:
        return False
    return b is None or a.order() > b.order()


class HlcClock:
    def __init__(self, node_id: uuid.UUID) -> None:
        self._node_id = node_id
        self._millis = 0
        self._counter = 0

    def next(self) -> Hlc:
        wall = time.time_ns() // 1_000_000
        if wall > self._millis:
            self._millis, self._counter = wall, 0
        else:
            self._counter += 1
        return Hlc(millis=self._millis, counter=self._counter, node=self._node_id)

    def observe(self, hlc: Hlc) -> None:
        if (hlc.millis, hlc.counter) > (self._millis, self._counter):
            self._millis, self._counter = hlc.millis, hlc.counter


@message(name="casty.Page")
class Page:
    key: str
    data: bytes
    # the commit that wrote this page. None on a delta (it is the message's own
    # hlc); carried on a full transfer so the receiver keeps the per-page history
    # a later catch-up diffs against.
    hlc: Hlc | None = None


@message(name="casty.PageRef")
class PageRef:
    key: str
    hlc: Hlc
    size: int


@message(name="casty.Replicate")
class Replicate:
    actor: str
    key: str
    prev_hlc: Hlc | None  # the basis this commit was computed against
    hlc: Hlc
    pages: list[Page] = field(default_factory=list)
    dropped: list[str] = field(default_factory=list)
    full: bool = False  # `pages` is the whole state, not a delta: replace
    final: bool = True  # last message of this commit: publish it
    repair: bool = False  # not a write: merge if newer, never fence the sender


@message(name="casty.FetchState")
class FetchState:
    actor: str
    key: str
    after: int = 0  # index refs already received: a page set too big to describe in
    # one message is scanned in chunks


@message(name="casty.StateReply")
class StateReply:
    hlc: Hlc | None
    index: list[PageRef] = field(default_factory=list)
    pages: list[Page] = field(default_factory=list)  # inlined when the state is small
    more: bool = False  # the index is truncated; ask again from `after`


@message(name="casty.FetchPages")
class FetchPages:
    actor: str
    key: str
    keys: list[str]


@message(name="casty.PagesReply")
class PagesReply:
    hlc: Hlc | None  # the entry these came from: the fetcher checks it did not move
    pages: list[Page] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class StoredPage:
    hlc: Hlc  # the commit that last wrote this page
    data: bytes


@dataclass(slots=True)
class Stored:
    """The replicated state of one instance on this node: the HLC of its last
    commit and the pages it left behind."""

    hlc: Hlc
    pages: dict[str, StoredPage]

    def index(self) -> list[PageRef]:
        return [PageRef(key=k, hlc=p.hlc, size=len(p.data)) for k, p in self.pages.items()]

    def size(self) -> int:
        return sum(len(p.data) for p in self.pages.values())


@dataclass(slots=True)
class _Staged:
    """A commit arriving across several messages, held until `final`: a replica
    never publishes half of one."""

    hlc: Hlc
    full: bool
    pages: dict[str, StoredPage] = field(default_factory=dict)
    dropped: set[str] = field(default_factory=set)


def _wire(pages: Iterable[tuple[str, StoredPage]]) -> Iterator[Page]:
    for key, page in pages:
        yield Page(key=key, data=page.data, hlc=page.hlc)


class ReplicaStore:
    """Last committed page set per (actor, key) on this node. In-memory: a dead
    node's state comes back from the other replicas, not from disk."""

    def __init__(self) -> None:
        self._entries: dict[tuple[str, str], Stored] = {}

    def get(self, actor: str, key: str) -> Stored | None:
        return self._entries.get((actor, key))

    def put(self, actor: str, key: str, stored: Stored) -> None:
        self._entries[(actor, key)] = stored

    def items(self) -> list[tuple[str, str, Stored]]:
        return [(actor, key, stored) for (actor, key), stored in self._entries.items()]


class StaleActivationError(CastyError):
    """Internal: a replica NACKed because it knows newer history — this owner's
    activation is stale and must be discarded."""


class RingView(typing.Protocol):
    """What replication needs from the node: current ring + dialable addresses."""

    @property
    def ring(self) -> Ring | None: ...

    def addr_of(self, node_id: uuid.UUID) -> str | None: ...


class Asker(typing.Protocol):
    """The one call replication makes on a connection."""

    async def ask(
        self,
        msg_type: int,
        body: bytes,
        *,
        timeout: float | None = None,
        replication: bool = False,
    ) -> bytes: ...


class Dialer(typing.Protocol):
    """What replication needs from the transport: a connection per address."""

    async def get(self, addr: str) -> Asker: ...


class Replication:
    def __init__(
        self,
        *,
        node_id: uuid.UUID,
        pool: Dialer,
        view: RingView,
        replication_timeout: float,
        handoff_timeout: float,
        batch_bytes: int,
        max_message_bytes: int,
    ) -> None:
        self._node_id = node_id
        self._pool = pool
        self._view = view
        self._timeout = replication_timeout
        self._handoff_timeout = handoff_timeout
        # a page is atomic on the wire: it cannot be split across messages, so one
        # has to fit in a message on its own
        self._max_page_bytes = max_message_bytes - _MESSAGE_SLACK
        self._batch_bytes = min(batch_bytes, self._max_page_bytes)
        self.clock = HlcClock(node_id)
        self.store = ReplicaStore()
        self._staging: dict[tuple[str, str], _Staged] = {}

    # --- replica side (any node) ---------------------------------------------------

    def handle_replicate(self, req: Replicate) -> None:
        """Apply an incoming commit. A write whose basis is older than what this
        replica knows is fenced; a delta whose basis this replica does not hold is
        refused, and the owner answers it with the whole page set."""
        stored = self.store.get(req.actor, req.key)
        current = stored.hlc if stored is not None else None
        if req.repair:
            if not newer(req.hlc, current):
                return  # we already hold this commit, or a newer one
            if not req.full and current != req.prev_hlc:
                return  # a delta on a basis we do not have; the next handshake repairs us
        else:
            if newer(current, req.prev_hlc):
                raise RemoteError(
                    CODE_STALE_WRITE,
                    f"{req.actor}/{req.key}: replica has newer history than the writer's basis",
                )
            if not req.full and current != req.prev_hlc:
                raise RemoteError(
                    CODE_NEED_FULL,
                    f"{req.actor}/{req.key}: replica is behind the writer's basis "
                    f"and cannot apply a delta",
                )
        self._stage(req, stored)

    def _stage(self, req: Replicate, stored: Stored | None) -> None:
        ident = (req.actor, req.key)
        staged = self._staging.get(ident)
        if staged is None or staged.hlc != req.hlc:
            staged = _Staged(hlc=req.hlc, full=req.full)
            self._staging[ident] = staged
        for page in req.pages:
            staged.pages[page.key] = StoredPage(hlc=page.hlc or req.hlc, data=page.data)
        staged.dropped.update(req.dropped)
        if not req.final:
            return
        del self._staging[ident]
        self._apply(
            req.actor, req.key, stored, req.hlc, staged.pages, staged.dropped, full=staged.full
        )

    def _apply(
        self,
        actor: str,
        key: str,
        stored: Stored | None,
        hlc: Hlc,
        pages: dict[str, StoredPage],
        dropped: Iterable[str],
        *,
        full: bool,
    ) -> None:
        if full or stored is None:
            self.store.put(actor, key, Stored(hlc=hlc, pages=pages))
            return
        stored.pages.update(pages)
        for page_key in dropped:
            stored.pages.pop(page_key, None)
        stored.hlc = hlc

    def handle_fetch(self, req: FetchState) -> StateReply:
        """The page index of this replica's copy, with the pages themselves inlined
        when the whole state is small enough to ride along."""
        stored = self.store.get(req.actor, req.key)
        if stored is None:
            return StateReply(hlc=None)
        index, more = self._index_chunk(stored, req.after)
        inline = req.after == 0 and not more and stored.size() <= self._batch_bytes
        pages = list(_wire(stored.pages.items())) if inline else []
        return StateReply(hlc=stored.hlc, index=index, pages=pages, more=more)

    def _index_chunk(self, stored: Stored, after: int) -> tuple[list[PageRef], bool]:
        """The refs from `after` on, up to a batch. The cursor is positional because
        the page dict only changes on a commit, and a commit moves the entry's HLC —
        which the scanner checks."""
        chunk: list[PageRef] = []
        size = 0
        for page_key, page in itertools.islice(stored.pages.items(), after, None):
            size += len(page_key) + _REF_OVERHEAD
            if chunk and size > self._batch_bytes:
                return chunk, True
            chunk.append(PageRef(key=page_key, hlc=page.hlc, size=len(page.data)))
        return chunk, False

    def handle_fetch_pages(self, req: FetchPages) -> PagesReply:
        stored = self.store.get(req.actor, req.key)
        if stored is None:
            return PagesReply(hlc=None)
        pages = [
            Page(key=key, data=page.data, hlc=page.hlc)
            for key in req.keys
            if (page := stored.pages.get(key)) is not None
        ]
        return PagesReply(hlc=stored.hlc, pages=pages)

    # --- owner side ---------------------------------------------------------------------

    def replica_set(self, info: ActorInfo, key: str) -> tuple[uuid.UUID, ...]:
        ring = self._view.ring
        if ring is None:
            return ()
        token = key_token(f"{info.wire_name}/{key}")
        return ring.replicas_for_token(token, info.replicas)

    async def handshake(self, info: ActorInfo, key: str) -> Stored | None:
        """Quorum activation: gather each replica's page index, require W responses
        (empty ones count — they are authoritative), adopt the newest, pull only the
        pages this node is missing, read-repair the stale."""
        quorum = info.write_quorum  # overlaps any committed write's W replicas
        local = self.store.get(info.wire_name, key)
        replies: dict[uuid.UUID, StateReply] = {
            self._node_id: (
                StateReply(hlc=local.hlc, index=local.index())
                if local is not None
                else StateReply(hlc=None)
            )
        }
        remote = [n for n in self.replica_set(info, key) if n != self._node_id]
        request = codec.encode(FetchState(actor=info.wire_name, key=key))

        async def fetch(node: uuid.UUID) -> None:
            reply = codec.decode(await self._ask(node, FETCH_STATE, request))
            if isinstance(reply, StateReply):
                replies[node] = reply

        results = await asyncio.gather(*(fetch(n) for n in remote), return_exceptions=True)
        for node, result in zip(remote, results, strict=True):
            if isinstance(result, BaseException):
                logger.debug("handshake fetch from %s failed: %s", node, result)
        if len(replies) < quorum:
            raise QuorumUnavailableError(
                f"{info.wire_name}/{key}: activation handshake reached "
                f"{len(replies)}/{quorum} replicas"
            )
        source: uuid.UUID | None = None
        best: Hlc | None = None
        for node, reply in replies.items():
            if newer(reply.hlc, best):
                source, best = node, reply.hlc
        if source is None or best is None:
            return None
        self.clock.observe(best)
        stored = await self._adopt(info, key, source, best, replies[source], local)
        self._read_repair(info, key, stored, replies)
        return stored

    async def _adopt(
        self,
        info: ActorInfo,
        key: str,
        source: uuid.UUID,
        hlc: Hlc,
        reply: StateReply,
        local: Stored | None,
    ) -> Stored:
        """Materialize the newest page set here: keep the pages this node already
        holds at that same commit, take the inlined ones, fetch the rest."""
        held = local.pages if local is not None else {}
        inlined = {page.key: page for page in reply.pages}
        pages: dict[str, StoredPage] = {}
        missing: list[PageRef] = []
        for ref in await self._scan_index(source, info.wire_name, key, hlc, reply):
            page = inlined.get(ref.key)
            if page is not None:
                pages[ref.key] = StoredPage(hlc=page.hlc or hlc, data=page.data)
                continue
            mine = held.get(ref.key)
            if mine is not None and mine.hlc == ref.hlc:
                pages[ref.key] = mine  # written by the same commit: same bytes
                continue
            missing.append(ref)
        for page in await self._fetch_pages(info, key, source, hlc, missing):
            pages[page.key] = StoredPage(hlc=page.hlc or hlc, data=page.data)
        stored = Stored(hlc=hlc, pages=pages)
        self.store.put(info.wire_name, key, stored)
        return stored

    async def _scan_index(
        self, node: uuid.UUID, actor: str, key: str, hlc: Hlc, first: StateReply
    ) -> list[PageRef]:
        """The whole index behind a reply. A page set of a million entries does not
        fit in one message, so it arrives in chunks; a commit on `node` mid-scan
        moves its HLC and the caller reactivates rather than read a torn index."""
        index = list(first.index)
        reply = first
        while reply.more:
            request = codec.encode(FetchState(actor=actor, key=key, after=len(index)))
            decoded = codec.decode(await self._ask(node, FETCH_STATE, request))
            if not isinstance(decoded, StateReply) or decoded.hlc != hlc:
                raise RangeMovingError(f"{actor}/{key}: state moved on {node} mid-scan; retry")
            reply = decoded
            index.extend(reply.index)
        return index

    async def _fetch_pages(
        self, info: ActorInfo, key: str, source: uuid.UUID, hlc: Hlc, refs: list[PageRef]
    ) -> list[Page]:
        if not refs:
            return []
        gate = asyncio.Semaphore(_FETCH_CONCURRENCY)

        async def batch(keys: list[str]) -> list[Page]:
            request = codec.encode(FetchPages(actor=info.wire_name, key=key, keys=keys))
            async with gate:
                reply = codec.decode(await self._ask(source, FETCH_PAGES, request))
            if not isinstance(reply, PagesReply) or reply.hlc != hlc:
                raise RangeMovingError(
                    f"{info.wire_name}/{key}: state moved on {source} mid-fetch; retry"
                )
            return reply.pages

        try:
            async with asyncio.TaskGroup() as group:
                batches = [group.create_task(batch(keys)) for keys in self._split(refs)]
        except* CastyError as failed:
            # one bad batch aborts the pull; the caller reactivates from scratch
            raise failed.exceptions[0] from None
        return [page for task in batches for page in task.result()]

    def _split(self, refs: list[PageRef]) -> Iterator[list[str]]:
        keys: list[str] = []
        size = 0
        for ref in refs:
            cost = ref.size + len(ref.key) + _PAGE_OVERHEAD
            if keys and size + cost > self._batch_bytes:
                yield keys
                keys, size = [], 0
            keys.append(ref.key)
            size += cost
        if keys:
            yield keys

    def _read_repair(
        self, info: ActorInfo, key: str, stored: Stored, replies: dict[uuid.UUID, StateReply]
    ) -> None:
        """Push each stale replica exactly the pages it is missing. Best-effort, in
        the background; the snapshot is taken now because this owner's own commits
        keep mutating the store."""
        snapshot = dict(stored.pages)
        for node, reply in replies.items():
            if node == self._node_id or not newer(stored.hlc, reply.hlc):
                continue
            task = asyncio.create_task(
                self._repair(info, key, node, reply, stored.hlc, snapshot)
            )
            task.add_done_callback(lambda _: None)

    async def _repair(
        self,
        info: ActorInfo,
        key: str,
        node: uuid.UUID,
        reply: StateReply,
        hlc: Hlc,
        snapshot: dict[str, StoredPage],
    ) -> None:
        with contextlib.suppress(CastyError, OSError):
            index = (
                reply.index
                if reply.hlc is None
                else await self._scan_index(node, info.wire_name, key, reply.hlc, reply)
            )
            held = {ref.key: ref.hlc for ref in index}
            behind = [
                (page_key, page)
                for page_key, page in snapshot.items()
                if held.get(page_key) != page.hlc
            ]
            await self._push(
                node,
                self._messages(
                    info.wire_name,
                    key,
                    prev_hlc=reply.hlc,
                    hlc=hlc,
                    pages=_wire(behind),
                    dropped=[page_key for page_key in held if page_key not in snapshot],
                    full=reply.hlc is None,
                    repair=True,
                ),
            )

    async def commit(
        self,
        info: ActorInfo,
        key: str,
        prev_hlc: Hlc | None,
        changed: list[Page],
        dropped: list[str],
    ) -> Hlc:
        """Replicate one mutation as a page delta; return its HLC once W replicas
        (this node included) acknowledged. Raises QuorumUnavailableError (fencing)
        or StaleActivationError (someone else's history is newer)."""
        actor = info.wire_name
        stored = self.store.get(actor, key)
        current = stored.hlc if stored is not None else None
        if current != prev_hlc:
            # another owner committed into this node's store: our basis is gone
            raise StaleActivationError(
                f"{actor}/{key}: local store moved to {current} under a writer on {prev_hlc}"
            )
        for page in changed:
            if len(page.data) > self._max_page_bytes:
                raise SerializationError(
                    f"{actor}/{key}: page {page.key!r} encodes to {len(page.data)} bytes, "
                    f"over the {self._max_page_bytes} byte limit for a single page"
                )
        hlc = self.clock.next()
        remote = [n for n in self.replica_set(info, key) if n != self._node_id]
        needed = info.write_quorum - 1  # the owner acks locally
        stale: list[str] = []

        async def push(node: uuid.UUID, parts: Iterator[Replicate]) -> bool | None:
            """True: acked. False: failed. None: the replica cannot apply a delta."""
            try:
                await self._push(node, parts)
                return True
            except RemoteError as exc:
                if exc.code == CODE_NEED_FULL:
                    return None
                if exc.code == CODE_STALE_WRITE:
                    stale.append(exc.remote_message)
                return False
            except (CastyError, OSError):
                return False

        async def replicate(node: uuid.UUID) -> bool:
            acked = await push(
                node,
                self._messages(
                    actor, key, prev_hlc=prev_hlc, hlc=hlc,
                    pages=iter(changed), dropped=dropped, full=False,
                ),
            )
            if acked is None:  # the replica is behind our basis: hand it everything
                acked = await push(
                    node,
                    self._messages(
                        actor, key, prev_hlc=prev_hlc, hlc=hlc,
                        pages=self._merged(stored, changed, dropped), dropped=[], full=True,
                    ),
                ) is True
            return acked

        results = await asyncio.gather(*(replicate(n) for n in remote))
        if stale:
            raise StaleActivationError(stale[0])
        acks = sum(results)
        if acks < needed:
            raise QuorumUnavailableError(
                f"{actor}/{key}: write got {acks + 1}/{info.write_quorum} acks"
            )
        self._apply(
            actor,
            key,
            stored,
            hlc,
            {page.key: StoredPage(hlc=hlc, data=page.data) for page in changed},
            dropped,
            full=False,
        )
        return hlc

    def _merged(
        self, stored: Stored | None, changed: list[Page], dropped: list[str]
    ) -> Iterator[Page]:
        """The whole page set as of this commit: what the store holds, plus the
        delta, minus the drops."""
        overrides = {page.key: page for page in changed}
        skip = set(dropped)
        if stored is not None:
            for key, page in list(stored.pages.items()):
                if key in skip:
                    continue
                override = overrides.pop(key, None)
                yield override or Page(key=key, data=page.data, hlc=page.hlc)
        yield from overrides.values()

    def _messages(
        self,
        actor: str,
        key: str,
        *,
        prev_hlc: Hlc | None,
        hlc: Hlc,
        pages: Iterator[Page],
        dropped: list[str],
        full: bool,
        repair: bool = False,
    ) -> Iterator[Replicate]:
        """Split one commit into messages that fit under max_message_bytes. Only the
        last carries `final`, and a replica publishes the commit on it — so a commit
        that spans messages still lands atomically."""
        batch: list[Page] = []
        size = 0
        for page in pages:
            cost = len(page.data) + len(page.key) + _PAGE_OVERHEAD
            if batch and size + cost > self._batch_bytes:
                yield Replicate(
                    actor=actor, key=key, prev_hlc=prev_hlc, hlc=hlc,
                    pages=batch, full=full, final=False, repair=repair,
                )
                batch, size = [], 0
            batch.append(page)
            size += cost
        yield Replicate(
            actor=actor, key=key, prev_hlc=prev_hlc, hlc=hlc, pages=batch,
            dropped=dropped, full=full, final=True, repair=repair,
        )

    async def handoff(self) -> None:
        """Push every locally stored page set to its replica set in the ring without
        this node. Best-effort: commits already live on W replicas."""
        ring = self._view.ring
        if ring is None:
            return
        remaining = ring.nodes - {self._node_id}
        if not remaining:
            return
        next_ring = Ring.build(remaining)
        from casty.collections import ensure  # local: avoids a hard layer cycle

        sends: list[typing.Coroutine[object, object, None]] = []
        for actor, key, stored in self.store.items():
            info = info_by_name(actor) or ensure(actor)
            if info is None:
                continue
            token = key_token(f"{actor}/{key}")
            targets = next_ring.replicas_for_token(token, info.replicas)
            pages = list(stored.pages.items())
            for node in targets:
                sends.append(
                    self._push_quietly(
                        node,
                        self._messages(
                            actor, key, prev_hlc=None, hlc=stored.hlc,
                            pages=_wire(pages), dropped=[], full=True, repair=True,
                        ),
                    )
                )
        if not sends:
            return
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(self._handoff_timeout):
                await asyncio.gather(*sends)

    # --- plumbing ------------------------------------------------------------------------

    async def _ask(self, node: uuid.UUID, msg_type: int, body: bytes) -> bytes:
        addr = self._view.addr_of(node)
        if addr is None:
            raise QuorumUnavailableError(f"replica {node} has no known address")
        conn = await self._pool.get(addr)
        return await conn.ask(msg_type, body, timeout=self._timeout, replication=True)

    async def _push(self, node: uuid.UUID, parts: Iterator[Replicate]) -> None:
        for part in parts:
            await self._ask(node, REPLICATE, codec.encode(part))

    async def _push_quietly(self, node: uuid.UUID, parts: Iterator[Replicate]) -> None:
        with contextlib.suppress(CastyError, OSError):
            await self._push(node, parts)
