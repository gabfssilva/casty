"""Paged state (spec 09): what a commit actually puts on the wire, and how a
replica that fell behind gets back in sync.

The cluster here is a handful of Replication instances calling each other in
process — no sockets, so every replicated byte is countable.
"""

from __future__ import annotations

import dataclasses

import pytest

import casty
from casty.actors import replication as rep
from casty.actors.registry import ActorInfo, Integral, Tracked, info_of
from casty.errors import (
    QuorumUnavailableError,
    SerializationError,
    SerializationSchemaError,
)
from casty.serde import codec
from tests.unit.cluster import BATCH, Cluster

pytestmark = pytest.mark.asyncio

LINE = "x" * 1024


@casty.actor(name="unit.PagedDoc", replicas=3, write=casty.MAJORITY)
class PagedDoc:
    title: str = ""
    body: list[str] = dataclasses.field(default_factory=list)
    views: int = 0

    async def rename(self, title: str) -> None:
        self.title = title

    async def append(self, line: str) -> None:
        self.body.append(line)

    async def view(self) -> int:
        self.views += 1
        return self.views

    async def read(self) -> str:
        return self.title

    async def lines(self) -> int:
        return len(self.body)


@casty.actor(name="unit.Hooked", replicas=3, write=casty.MAJORITY)
class Hooked:
    """The activate hook mutates state (spec 09 §9). The basis is what the last commit
    left, not what the hook produced, so the hook's write is still dirty afterwards."""

    activations: int = 0
    label: str = ""

    @casty.activate
    async def _up(self) -> None:
        self.activations += 1

    async def rename(self, label: str) -> None:
        self.label = label

    async def count(self) -> int:
        return self.activations


@casty.actor(name="unit.PagedIndex", replicas=3, write=casty.MAJORITY)
class PagedIndex:
    """A dict-tracked field (spec 09 §4) plus an integral one, so a commit that
    touches one entry can be told apart from one that touches the whole field."""

    entries: dict[str, int] = dataclasses.field(default_factory=dict)
    label: str = ""

    async def seed(self, n: int) -> None:
        for i in range(n):
            self.entries[f"k{i}"] = i

    async def bump(self, key: str) -> None:
        self.entries[key] += 1

    async def drop(self, key: str) -> None:
        del self.entries[key]

    async def take(self, key: str) -> int | None:
        return self.entries.pop(key, None)

    async def merge(self, other: dict[str, int]) -> None:
        self.entries |= other

    async def replace(self, other: dict[str, int]) -> None:
        self.entries = other

    async def wipe(self) -> None:
        self.entries.clear()

    async def rename(self, label: str) -> None:
        self.label = label

    async def total(self) -> int:
        return sum(self.entries.values())

    async def size(self) -> int:
        return len(self.entries)


def _doc() -> ActorInfo:
    info = info_of(PagedDoc)
    assert info is not None
    return info


def _index() -> ActorInfo:
    info = info_of(PagedIndex)
    assert info is not None
    return info


def _hooked() -> ActorInfo:
    info = info_of(Hooked)
    assert info is not None
    return info


async def test_commit_carries_only_the_pages_the_handler_changed() -> None:
    info, cluster = _doc(), Cluster(4)
    owner = cluster.owner(info, "doc")
    for _ in range(64):
        await owner.call(info, "doc", "append", LINE)
    body = owner.pages(info, "doc")["body"]
    assert len(body) > 64 * 1024
    written = owner.page_hlc(info, "doc", "body")

    cluster.reset()
    await owner.call(info, "doc", "view")  # touches `views` only

    assert cluster.replicated < 1024  # two replicas, one small page each
    for replica in cluster.replicas(info, "doc"):
        assert replica.pages(info, "doc")["body"] == body
        assert replica.page_hlc(info, "doc", "body") == written  # not rewritten


async def test_read_only_handler_commits_nothing() -> None:
    info, cluster = _doc(), Cluster(4)
    owner = cluster.owner(info, "doc")
    await owner.call(info, "doc", "rename", "v1")

    cluster.reset()
    assert await owner.call(info, "doc", "read") == "v1"
    assert cluster.replicated == 0


async def test_a_mutation_in_the_activate_hook_rides_the_next_commit() -> None:
    info, cluster = _hooked(), Cluster(4)
    owner = cluster.owner(info, "h")
    await owner.call(info, "h", "rename", "v1")

    for node in cluster.replicas(info, "h"):
        assert codec.decode_raw(node.pages(info, "h")["activations"], int) == 1

    elsewhere = next(node for node in cluster.replicas(info, "h") if node is not owner)
    elsewhere.fresh_host()
    assert await elsewhere.call(info, "h", "count") == 2  # restored 1, hook bumped it


async def test_replica_behind_the_basis_is_handed_the_whole_state() -> None:
    info, cluster = _doc(), Cluster(4)
    owner = cluster.owner(info, "doc")
    lagging = next(node for node in cluster.replicas(info, "doc") if node is not owner)
    await owner.call(info, "doc", "rename", "v1")

    cluster.down.add(lagging.node_id)  # W=2 still commits: owner + the other replica
    await owner.call(info, "doc", "rename", "v2")
    await owner.call(info, "doc", "append", LINE)
    cluster.down.discard(lagging.node_id)
    assert lagging.pages(info, "doc") != owner.pages(info, "doc")

    cluster.reset()
    await owner.call(info, "doc", "view")

    assert rep.CODE_NEED_FULL in cluster.nacks  # the delta was refused...
    assert lagging.pages(info, "doc") == owner.pages(info, "doc")  # ...and the gap closed


async def test_activation_fetches_only_the_pages_this_node_lacks() -> None:
    info, cluster = _doc(), Cluster(4)
    owner = cluster.owner(info, "doc")
    for _ in range(300):  # a state too big for a fetch reply to inline
        await owner.call(info, "doc", "append", LINE)
    assert sum(len(page) for page in owner.pages(info, "doc").values()) > 256 * 1024

    cluster.reset()
    owner.fresh_host()  # deactivated, reactivating where the pages already are
    assert await owner.call(info, "doc", "lines") == 300
    assert cluster.fetched == 0

    cluster.reset()
    outsider = cluster.outsider(info, "doc")
    assert await outsider.call(info, "doc", "lines") == 300
    assert cluster.fetched > 256 * 1024  # holds nothing: pulls the whole state


async def test_quorum_failure_rolls_back_only_the_dirty_pages() -> None:
    info, cluster = _doc(), Cluster(4)
    owner = cluster.owner(info, "doc")
    await owner.call(info, "doc", "rename", "v1")
    await owner.call(info, "doc", "append", LINE)

    cluster.down.update(node.node_id for node in cluster.replicas(info, "doc"))
    with pytest.raises(QuorumUnavailableError):
        await owner.call(info, "doc", "rename", "v2")
    cluster.down.clear()

    assert await owner.call(info, "doc", "read") == "v1"  # the failed write is gone
    assert await owner.call(info, "doc", "lines") == 1  # the untouched page survived it


async def test_page_over_the_message_limit_fails_the_call_not_the_connection() -> None:
    info, cluster = _doc(), Cluster(4)
    owner = cluster.owner(info, "doc")
    await owner.call(info, "doc", "rename", "v1")

    with pytest.raises(SerializationError, match=r"over the \d+ byte limit"):
        await owner.call(info, "doc", "rename", "x" * (5 * 1024 * 1024))

    assert await owner.call(info, "doc", "read") == "v1"


# --- dict-tracked regime (spec 09 §4) ---------------------------------------------


async def test_touching_one_entry_of_a_large_dict_replicates_one_page() -> None:
    info, cluster = _index(), Cluster(4)
    owner = cluster.owner(info, "i")
    await owner.call(info, "i", "seed", 20_000)
    assert len(owner.pages(info, "i")) == 20_000  # one page per entry; `label` is still default

    cluster.reset()
    await owner.call(info, "i", "bump", "k7")

    assert cluster.replicated < 1024  # two replicas, one entry page each
    assert await owner.call(info, "i", "total") == sum(range(20_000)) + 1
    for replica in cluster.replicas(info, "i"):
        assert len(replica.pages(info, "i")) == 20_000
        assert replica.pages(info, "i") == owner.pages(info, "i")


async def test_every_bulk_mutator_reaches_the_wire() -> None:
    """`dict`'s C methods write past `__setitem__`; a mutation the wrapper missed
    would diverge in silence, so each one is exercised against the replicas."""
    info, cluster = _index(), Cluster(4)
    owner = cluster.owner(info, "i")
    await owner.call(info, "i", "seed", 4)  # k0..k3
    await owner.call(info, "i", "merge", {"k9": 9})  # |=
    await owner.call(info, "i", "take", "k0")  # pop
    await owner.call(info, "i", "drop", "k1")  # del

    assert await owner.call(info, "i", "size") == 3
    for replica in cluster.replicas(info, "i"):
        assert set(replica.pages(info, "i")) == {"entries/k2", "entries/k3", "entries/k9"}

    await owner.call(info, "i", "wipe")  # clear
    for replica in cluster.replicas(info, "i"):
        assert replica.pages(info, "i") == {}


async def test_a_stale_replica_does_not_resurrect_a_deleted_entry() -> None:
    info, cluster = _index(), Cluster(4)
    owner = cluster.owner(info, "i")
    await owner.call(info, "i", "seed", 8)
    lagging = next(node for node in cluster.replicas(info, "i") if node is not owner)

    cluster.down.add(lagging.node_id)  # W=2 still commits: owner + the other replica
    await owner.call(info, "i", "drop", "k3")
    cluster.down.discard(lagging.node_id)
    assert "entries/k3" in lagging.pages(info, "i")  # it still holds the entry

    owner.fresh_host()  # reactivate: the handshake read-repairs the stale replica
    assert await owner.call(info, "i", "size") == 7
    await cluster.settle()

    assert "entries/k3" not in lagging.pages(info, "i")
    assert lagging.pages(info, "i") == owner.pages(info, "i")


async def test_reassigning_the_whole_dict_rearms_the_tracker() -> None:
    info, cluster = _index(), Cluster(4)
    owner = cluster.owner(info, "i")
    await owner.call(info, "i", "seed", 3)

    await owner.call(info, "i", "replace", {"a": 1})  # the wrapper is gone: full rewrite
    for replica in cluster.replicas(info, "i"):
        assert set(replica.pages(info, "i")) == {"entries/a"}

    cluster.reset()
    await owner.call(info, "i", "bump", "a")  # ...and per-entry tracking is back

    assert cluster.replicated < 1024
    assert await owner.call(info, "i", "total") == 2


async def test_quorum_failure_rolls_back_a_tracked_dict() -> None:
    info, cluster = _index(), Cluster(4)
    owner = cluster.owner(info, "i")
    await owner.call(info, "i", "seed", 3)  # k0=0 k1=1 k2=2

    cluster.down.update(node.node_id for node in cluster.replicas(info, "i"))
    with pytest.raises(QuorumUnavailableError):
        await owner.call(info, "i", "merge", {"k0": 100, "zz": 1})  # overwrites and adds
    cluster.down.clear()

    assert await owner.call(info, "i", "size") == 3  # the added entry is gone
    assert await owner.call(info, "i", "total") == 3  # and k0 is back to 0

    cluster.reset()
    await owner.call(info, "i", "bump", "k1")
    assert cluster.replicated < 1024  # the rollback cleared the marks: one page, not three


async def test_an_index_too_big_for_one_message_is_scanned_in_chunks() -> None:
    info, cluster = _index(), Cluster(4)
    owner = cluster.owner(info, "i")
    await owner.call(info, "i", "seed", 20_000)

    cluster.reset()
    outsider = cluster.outsider(info, "i")  # holds nothing: pulls index and pages
    assert await outsider.call(info, "i", "size") == 20_000

    assert cluster.scans > 3  # the index did not fit in one reply...
    assert 0 < cluster.widest <= BATCH  # ...and every chunk stayed under the batch target
    assert outsider.pages(info, "i") == owner.pages(info, "i")


async def test_handoff_of_a_large_dict_batches_its_pages() -> None:
    info, cluster = _index(), Cluster(4)
    owner = cluster.owner(info, "i")
    await owner.call(info, "i", "seed", 20_000)

    cluster.reset()
    await owner.replication.handoff()

    assert cluster.messages < 100  # batched: nowhere near one message per page
    assert cluster.widest <= BATCH  # and every batch stayed under its target
    targets = [node for node in cluster.nodes.values() if node is not owner]
    assert any(node.pages(info, "i") == owner.pages(info, "i") for node in targets)


# --- regimes are inspectable (spec 09 §3) -----------------------------------------


@casty.message(name="unit.Point")
@dataclasses.dataclass(frozen=True, slots=True)
class Point:
    x: int = 0


@casty.actor(name="unit.Regimes", replicas=1)
class Regimes:
    scalar: int = 0
    listy: list[int] = dataclasses.field(default_factory=list)
    counts: dict[str, int] = dataclasses.field(default_factory=dict)
    points: dict[int, Point] = dataclasses.field(default_factory=dict)
    nested: dict[str, list[int]] = dataclasses.field(default_factory=dict)

    async def noop(self) -> None: ...


async def test_explain_names_the_regime_of_every_field() -> None:
    info = info_of(Regimes)
    assert info is not None
    assert isinstance(info.regimes["counts"], Tracked)
    assert isinstance(info.regimes["points"], Tracked)  # frozen message values cannot alias
    assert isinstance(info.regimes["nested"], Integral)  # list values can be mutated in place
    assert isinstance(info.regimes["listy"], Integral)

    lines = {line.split()[0]: line for line in casty.explain(Regimes).splitlines()[1:]}
    assert "tracked" in lines["counts"]
    assert "tracked" in lines["points"]
    assert "integral" in lines["nested"]
    assert "immutable" in lines["scalar"]


async def test_a_dict_replicated_per_entry_rejects_a_non_empty_default() -> None:
    with pytest.raises(SerializationSchemaError, match="must default to empty"):

        @casty.actor(name="unit.SeededDict")
        class SeededDict:
            seeded: dict[str, int] = dataclasses.field(default_factory=lambda: {"a": 1})

            async def noop(self) -> None: ...
