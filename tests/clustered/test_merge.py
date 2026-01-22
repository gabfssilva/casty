import pytest
from dataclasses import dataclass

from casty.cluster.merge import Merge
from casty.serializable import serialize, deserialize


@dataclass
class SampleData:
    count: int


class TestMergeMessage:
    def test_merge_with_base(self):
        merge = Merge(
            base=SampleData(count=0),
            local=SampleData(count=5),
            remote=SampleData(count=3)
        )

        assert merge.base.count == 0
        assert merge.local.count == 5
        assert merge.remote.count == 3

    def test_merge_without_base(self):
        merge = Merge(
            base=None,
            local=SampleData(count=5),
            remote=SampleData(count=3)
        )

        assert merge.base is None

    def test_merge_is_generic(self):
        merge_int: Merge[int] = Merge(base=0, local=5, remote=3)
        merge_str: Merge[str] = Merge(base="a", local="b", remote="c")

        assert merge_int.local == 5
        assert merge_str.local == "b"
