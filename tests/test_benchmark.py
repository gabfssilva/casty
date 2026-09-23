from pathlib import Path

import pytest

from benchmarks import index
from benchmarks.actors import Read, counter
from benchmarks.performance import Sample, exercise, read_sample, summarize, write_sample
from casty import ActorSystem


def test_aggregate_uses_common_elapsed_and_pooled_latencies() -> None:
    result = summarize((Sample((0.001, 0.002), 1, 2.0), Sample((0.003, 0.004), 0, 3.0)))
    assert result.operations == 4
    assert result.errors == 1
    assert result.ops_per_second == pytest.approx(4 / 3)
    assert result.error_rate == pytest.approx(0.2)
    assert result.p50_ms == 2
    assert result.p99_ms == 4


def test_all_failed_has_no_success_latency() -> None:
    result = summarize((Sample((), 3, 1.0),))
    assert result.ops_per_second == 0
    assert result.error_rate == 1
    assert result.p50_ms is None


async def test_write_load_counts_saved_increments() -> None:
    async with ActorSystem() as system:
        sample = await exercise(system, "increment", "test", keys=4, concurrency=2, duration=0.03)
        total = sum([await system.ref(counter, f"test:{i}").ask(Read, b"x" * 64) for i in range(4)])
        assert sample.errors == 0
        assert sample.latencies
        assert total == len(sample.latencies)


async def test_read_load_does_not_change_state() -> None:
    async with ActorSystem() as system:
        sample = await exercise(system, "read", "test", keys=4, concurrency=2, duration=0.03)
        assert sample.latencies
        assert sample.errors == 0
        for index in range(4):
            assert await system.ref(counter, f"test:{index}").ask(Read, b"x" * 64) == 0


async def test_index_additions_write_about_as_much_at_ten_times_the_keys() -> None:
    checkpoints = await index.run(keys=10_000, sample=500, concurrency=16, shards=1)
    for collection in ("dict", "set"):
        written = [checkpoint.bytes_per_add for checkpoint in checkpoints if checkpoint.collection == collection]
        assert [checkpoint.keys for checkpoint in checkpoints if checkpoint.collection == collection] == [1_000, 10_000]
        assert max(written) < 2 * min(written)


def test_raw_samples_preserve_errors_and_latency(tmp_path: Path) -> None:
    sample = Sample((0.001, 0.002), 3, 2.5)
    path = tmp_path / "sample.csv"
    write_sample(path, sample)
    assert read_sample(path) == sample
