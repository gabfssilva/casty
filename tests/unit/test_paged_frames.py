"""The pandas pager (spec 09 §5.1): what a cell edit on a large frame actually puts on
the wire, and whether the frame comes back intact on a node that never held it."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

import casty
from casty.actors.registry import ActorInfo, Paged, info_of
from casty.errors import QuorumUnavailableError, SerializationError, SerializationSchemaError
from casty.pagers import PandasPager
from casty.serde import codec
from tests.unit.cluster import Cluster, Node

pytestmark = pytest.mark.asyncio

ROWS = 400_000  # 5 float columns: a ~16 MB frame, 25 row blocks per column
BLOCK = 16_384 * 8  # one float64 row block, serialized


@casty.actor(name="unit.Frames", replicas=3, write=casty.MAJORITY)
class Frames:
    df: pd.DataFrame = casty.paged(PandasPager())
    label: str = ""

    async def seed(self, rows: int) -> None:
        self.df = pd.DataFrame({f"c{i}": np.arange(rows, dtype=float) + i for i in range(5)})

    async def edit(self, row: int, column: str, value: float) -> None:
        self.df.loc[row, column] = value

    async def add_column(self, name: str) -> None:
        self.df[name] = np.zeros(len(self.df))

    async def drop_column(self, name: str) -> None:
        del self.df[name]

    async def append(self, rows: int) -> None:
        extra = pd.DataFrame({c: np.zeros(rows) for c in self.df.columns})
        self.df = pd.concat([self.df, extra], ignore_index=True)

    async def rename(self, label: str) -> None:
        self.label = label

    async def regroup(self, column: str) -> None:
        self.label = "grouped"
        self.df = self.df.set_index(column, append=True)  # a MultiIndex: the pager refuses it

    async def name(self) -> str:
        return self.label

    async def cell(self, row: int, column: str) -> float:
        return float(self.df[column].to_numpy()[row])

    async def shape(self) -> tuple[int, int]:
        return (len(self.df), len(self.df.columns))

    async def checksum(self) -> float:
        return float(self.df.to_numpy().sum())


@casty.actor(name="unit.Typed", replicas=3, write=casty.MAJORITY)
class Typed:
    """Mixed dtypes over a labelled (non-range) index: the parts of a frame that page
    separately from its float columns."""

    df: pd.DataFrame = casty.paged(PandasPager())

    async def put(self, ints: list[int], names: list[str], flags: list[bool]) -> None:
        frame = pd.DataFrame(
            {
                "n": pd.Series(ints, dtype="int64"),
                "name": pd.Series(names, dtype="str"),
                "flag": pd.Series(flags, dtype="bool"),
                "when": pd.Series(["2026-07-11"] * len(ints), dtype="datetime64[ns]"),
            }
        )
        # assigned, not passed to the constructor: a labelled index there would *align*
        # the range-indexed columns against it and leave every value NaN
        frame.index = pd.Index([f"r{i}" for i in range(len(ints))], name="rid")
        self.df = frame

    async def touch(self, row: str, value: int) -> None:
        self.df.loc[row, "n"] = value

    async def dtypes(self) -> list[str]:
        return [str(dtype) for dtype in self.df.dtypes]


def _info(cls: type) -> ActorInfo:
    info = info_of(cls)
    assert info is not None
    return info


def _stored(node: Node, info: ActorInfo, key: str) -> pd.DataFrame:
    """The frame as this node's replica store holds it — rebuilt through the pager, the
    same way an activation on this node would."""
    pages = {
        page_key.removeprefix("df/"): data
        for page_key, data in node.pages(info, key).items()
        if page_key.startswith("df/")
    }
    return PandasPager().restore(pages)


async def test_editing_one_cell_replicates_one_row_block() -> None:
    info, cluster = _info(Frames), Cluster(4)
    owner = cluster.owner(info, "f")
    await owner.call(info, "f", "seed", ROWS)
    assert sum(len(page) for page in owner.pages(info, "f").values()) > 15_000_000

    cluster.reset()
    await owner.call(info, "f", "edit", 3, "c2", 99.0)

    # the five float columns share one consolidated block, so writing c2 moves all five
    # buffers; only the row-block comparison keeps the other four off the wire
    assert cluster.replicated < 2 * (BLOCK + 4096)  # one block, to two replicas
    assert await owner.call(info, "f", "cell", 3, "c2") == 99.0
    for replica in cluster.replicas(info, "f"):
        assert replica.pages(info, "f") == owner.pages(info, "f")


async def test_a_read_only_handler_pages_nothing() -> None:
    info, cluster = _info(Frames), Cluster(4)
    owner = cluster.owner(info, "f")
    await owner.call(info, "f", "seed", 50_000)

    cluster.reset()
    assert await owner.call(info, "f", "shape") == (50_000, 5)
    assert cluster.replicated == 0


async def test_writing_the_other_field_does_not_repage_the_frame() -> None:
    """The two regimes share a commit: a handler that only touches the integral field
    must leave the pager's pages alone."""
    info, cluster = _info(Frames), Cluster(4)
    owner = cluster.owner(info, "f")
    await owner.call(info, "f", "seed", 50_000)
    frame_pages = {k: v for k, v in owner.pages(info, "f").items() if k.startswith("df/")}

    cluster.reset()
    await owner.call(info, "f", "rename", "v2")

    assert cluster.replicated < 1024  # the label page, to two replicas
    assert owner.pages(info, "f")["label"] == codec.encode_raw("v2")
    assert {k: v for k, v in owner.pages(info, "f").items() if k.startswith("df/")} == frame_pages


async def test_a_frame_activates_intact_on_a_node_that_never_held_it() -> None:
    info, cluster = _info(Frames), Cluster(4)
    owner = cluster.owner(info, "f")
    await owner.call(info, "f", "seed", 50_000)
    await owner.call(info, "f", "edit", 7, "c1", -5.0)
    await owner.call(info, "f", "add_column", "extra")
    checksum = await owner.call(info, "f", "checksum")

    outsider = cluster.outsider(info, "f")
    assert await outsider.call(info, "f", "checksum") == checksum
    assert await outsider.call(info, "f", "shape") == (50_000, 6)
    assert_frame_equal(_stored(owner, info, "f"), _stored(outsider, info, "f"))


async def test_dtypes_and_a_labelled_index_survive_the_round_trip() -> None:
    info, cluster = _info(Typed), Cluster(4)
    owner = cluster.owner(info, "t")
    await owner.call(info, "t", "put", [1, 2, 3], ["a", "b", "c"], [True, False, True])
    dtypes = await owner.call(info, "t", "dtypes")
    assert dtypes == ["int64", "str", "bool", "datetime64[ns]"]

    outsider = cluster.outsider(info, "t")
    assert await outsider.call(info, "t", "dtypes") == dtypes  # rebuilt from the pages

    frame = _stored(outsider, info, "t")
    assert frame.index.name == "rid"
    assert list(frame.index) == ["r0", "r1", "r2"]
    assert_frame_equal(_stored(owner, info, "t"), frame)


async def test_a_labelled_index_is_not_repaged_when_only_a_cell_moves() -> None:
    info, cluster = _info(Typed), Cluster(4)
    owner = cluster.owner(info, "t")
    await owner.call(info, "t", "put", [1, 2, 3], ["a", "b", "c"], [True, False, True])
    index_hlc = owner.page_hlc(info, "t", "df/index/0")

    await owner.call(info, "t", "touch", "r1", -1)

    assert owner.page_hlc(info, "t", "df/index/0") == index_hlc  # index page untouched
    assert _stored(owner, info, "t")["n"].tolist() == [1, -1, 3]


async def test_adding_and_dropping_a_column_moves_only_that_column() -> None:
    info, cluster = _info(Frames), Cluster(4)
    owner = cluster.owner(info, "f")
    await owner.call(info, "f", "seed", 50_000)
    before = set(owner.pages(info, "f"))

    await owner.call(info, "f", "add_column", "extra")
    assert len(set(owner.pages(info, "f")) - before) == 4  # 50k rows: four row blocks

    await owner.call(info, "f", "drop_column", "extra")
    assert set(owner.pages(info, "f")) == before
    for replica in cluster.replicas(info, "f"):
        assert replica.pages(info, "f") == owner.pages(info, "f")


async def test_quorum_failure_hands_the_frame_back_unchanged() -> None:
    info, cluster = _info(Frames), Cluster(4)
    owner = cluster.owner(info, "f")
    await owner.call(info, "f", "seed", 20_000)
    checksum = await owner.call(info, "f", "checksum")

    cluster.down.update(node.node_id for node in cluster.replicas(info, "f"))
    with pytest.raises(QuorumUnavailableError):
        await owner.call(info, "f", "edit", 1, "c0", 1e9)
    cluster.down.clear()

    assert await owner.call(info, "f", "checksum") == checksum  # the shadow came back

    cluster.reset()
    await owner.call(info, "f", "edit", 1, "c0", 7.0)
    assert await owner.call(info, "f", "cell", 1, "c0") == 7.0  # ...and the next edit pages
    assert 0 < cluster.replicated < 2 * (BLOCK + 4096)


async def test_appending_rows_repages_the_frame() -> None:
    info, cluster = _info(Frames), Cluster(4)
    owner = cluster.owner(info, "f")
    await owner.call(info, "f", "seed", 20_000)

    cluster.reset()
    await owner.call(info, "f", "append", 5_000)  # the row blocks no longer line up

    assert await owner.call(info, "f", "shape") == (25_000, 5)
    assert cluster.replicated > 1_000_000  # the whole frame, by design
    for replica in cluster.replicas(info, "f"):
        assert replica.pages(info, "f") == owner.pages(info, "f")


async def test_pandas_below_3_is_refused_where_the_actor_is_declared(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pd, "__version__", "2.2.3")
    with pytest.raises(SerializationSchemaError, match=r"pandas >= 3\.0"):
        PandasPager()  # constructed in the class body: the actor never registers


async def test_the_pager_refuses_a_multi_index() -> None:
    with pytest.raises(SerializationError, match="MultiIndex"):
        PandasPager().snapshot(
            pd.DataFrame({"v": [1]}, index=pd.MultiIndex.from_tuples([("a", 1)]))
        )


async def test_a_frame_the_pager_cannot_page_undoes_the_whole_state() -> None:
    """A diff that raises may have stopped half way, so no field is trustworthy: the
    commit rebuilds every field from the committed pages (spec 09 §6), including the
    integral one the failed handler had already written."""
    info, cluster = _info(Frames), Cluster(4)
    owner = cluster.owner(info, "f")
    await owner.call(info, "f", "seed", 10)
    await owner.call(info, "f", "rename", "v1")
    committed = _stored(owner, info, "f")

    with pytest.raises(SerializationError, match="MultiIndex"):
        await owner.call(info, "f", "regroup", "c0")

    assert await owner.call(info, "f", "name") == "v1"
    assert await owner.call(info, "f", "shape") == (10, 5)
    assert_frame_equal(_stored(owner, info, "f"), committed)


def _mixed() -> pd.DataFrame:
    return pd.DataFrame({"a": np.arange(3.0), "b": np.arange(3.0), "s": list("xyz")})


def _floats() -> pd.DataFrame:
    return pd.DataFrame({"a": np.arange(3.0), "b": np.arange(3.0)})


async def test_no_write_pandas_offers_slips_past_the_diff() -> None:
    """The pager's whole safety argument: a mutation it cannot see is silent divergence,
    so every path pandas offers has to move a buffer. Copy-on-write is what makes that
    true — the check sits in the block manager, underneath all of these."""

    def loc(frame: pd.DataFrame) -> None:
        frame.loc[0, "a"] = 9.0

    def iloc(frame: pd.DataFrame) -> None:
        frame.iloc[0, 0] = 9.0

    def at(frame: pd.DataFrame) -> None:
        frame.at[0, "a"] = 9.0

    def iat(frame: pd.DataFrame) -> None:
        frame.iat[0, 0] = 9.0

    def whole_column(frame: pd.DataFrame) -> None:
        frame["a"] = np.ones(3)

    def column_slice(frame: pd.DataFrame) -> None:
        frame.loc[:, "a"] = 1.0

    def string_cell(frame: pd.DataFrame) -> None:
        frame.loc[1, "s"] = "ZZZ"

    def sort_in_place(frame: pd.DataFrame) -> None:
        frame.sort_values("a", ascending=False, inplace=True)

    def rename_in_place(frame: pd.DataFrame) -> None:
        frame.rename(columns={"a": "z"}, inplace=True)

    def drop_in_place(frame: pd.DataFrame) -> None:
        frame.drop(columns=["b"], inplace=True)

    def new_column(frame: pd.DataFrame) -> None:
        frame["extra"] = np.zeros(3)

    pager = PandasPager()
    writes = {
        "loc": loc,
        "iloc": iloc,
        "at": at,
        "iat": iat,
        "whole column": whole_column,
        "loc[:, c]": column_slice,
        "string cell": string_cell,
        "sort_values(inplace)": sort_in_place,
        "rename(inplace)": rename_in_place,
        "drop(inplace)": drop_in_place,
        "new column": new_column,
    }
    for name, write in writes.items():
        frame = _mixed()
        shadow = pager.snapshot(frame)
        write(frame)
        pages = pager.pages(frame, shadow)
        assert pages.changed or pages.dropped, f"{name} slipped past the diff"


async def test_numpy_cannot_write_behind_the_pagers_back() -> None:
    """`.values` is the way out of pandas. On a frame of one dtype it hands back a
    read-only view of the block; on a mixed one it builds a fresh array — so a write
    through it never reaches the frame the pager is watching."""
    pager = PandasPager()
    for build in (_mixed, _floats):
        frame = build()
        shadow = pager.snapshot(frame)
        before = frame["a"].to_numpy()[0]
        try:
            frame.values[0, 0] = 42.0
        except ValueError:
            continue  # read-only: pandas refused the write outright
        assert frame["a"].to_numpy()[0] == before  # it wrote into a copy, not the frame
        assert not pager.pages(frame, shadow).changed


async def test_explain_names_the_pager() -> None:
    info = _info(Frames)
    assert isinstance(info.regimes["df"], Paged)
    lines = {line.split()[0]: line for line in casty.explain(Frames).splitlines()[1:]}
    assert "paged" in lines["df"]
    assert "PandasPager" in lines["df"]
    assert "integral" in lines["label"]
