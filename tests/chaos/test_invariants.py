"""The checks of the chaos suite, on histories written by hand. They start nothing, so they run with every `pytest`."""

from __future__ import annotations

from casty import Unavailable
from tests.chaos.journal import Operation, ambiguous
from tests.chaos.workloads import erased, impossible
from tests.traffic import kept


def describe_a_read_of_a_register() -> None:
    def it_may_give_the_last_confirmed_write() -> None:
        assert impossible(1, [_put(1, 0.0, 1.0)], 2.0, 3.0) is None

    def it_may_not_give_a_write_that_a_later_confirmed_one_overwrote() -> None:
        writes = [_put(1, 0.0, 1.0), _put(2, 1.5, 2.0)]
        assert impossible(1, writes, 3.0, 4.0) is not None
        assert impossible(2, writes, 3.0, 4.0) is None

    def it_may_give_either_of_two_writes_that_overlapped() -> None:
        writes = [_put(1, 0.0, 2.0), _put(2, 1.0, 1.5)]
        assert impossible(1, writes, 3.0, 4.0) is None
        assert impossible(2, writes, 3.0, 4.0) is None

    def it_may_give_a_write_never_confirmed_at_any_moment_after_it_began() -> None:
        writes = [_put(1, 0.0, 1.0), _put(2, 1.5, 2.0, confirmed=False), _put(3, 2.5, 3.0)]
        assert impossible(2, writes, 4.0, 5.0) is None
        assert impossible(1, writes, 4.0, 5.0) is not None

    def it_may_give_absence_before_any_confirmed_write_and_after_a_remove() -> None:
        put = _put(1, 0.0, 1.0)
        assert impossible(None, [], 0.0, 1.0) is None
        assert impossible(None, [put], 2.0, 3.0) is not None
        assert impossible(None, [put, _remove(1.5, 2.0)], 3.0, 4.0) is None

    def it_may_not_give_what_no_write_had_begun_to_write() -> None:
        assert impossible(9, [_put(1, 0.0, 1.0)], 2.0, 3.0) is not None
        assert impossible(1, [_put(1, 5.0, 6.0)], 2.0, 3.0) is not None

    def when_an_outage_took_it() -> None:
        def it_may_give_absence_and_not_what_was_confirmed_before() -> None:
            writes = [_put(1, 0.0, 1.0), erased("dict", "k", 2.0)]
            assert impossible(None, writes, 3.0, 4.0) is None
            assert impossible(1, writes, 3.0, 4.0) is not None

        def it_may_give_a_write_still_under_way_when_the_machines_died() -> None:
            writes = [_put(1, 1.5, 2.5), erased("dict", "k", 2.0)]
            assert impossible(1, writes, 3.0, 4.0) is None
            assert impossible(None, writes, 3.0, 4.0) is None


def describe_the_check_of_a_ledger() -> None:
    def it_names_what_was_lost_invented_or_applied_twice() -> None:
        assert kept("key", (1, 2), {1}, {1, 2, 3}) == []
        assert kept("key", (2,), {1}, {1, 2}) == ["key lost {1}"]
        assert kept("key", (1, 4), set(), {1}) == ["key invented {4}"]
        assert kept("key", (1, 1), {1}, {1}) == ["key applied an entry twice: (1, 1)"]


def describe_an_ambiguous_error() -> None:
    def it_leaves_the_call_maybe_applied_alone_or_as_every_error_of_a_fan_out() -> None:
        assert ambiguous(TimeoutError())
        assert ambiguous(Unavailable("gone"))
        assert ambiguous(ExceptionGroup("fan-out", [TimeoutError(), Unavailable("gone")]))
        assert not ambiguous(ExceptionGroup("fan-out", [TimeoutError(), ValueError()]))
        assert not ambiguous(RuntimeError())


def _put(value: int, started: float, ended: float, *, confirmed: bool = True) -> Operation:
    return Operation(value, "dict", "k", "put", value, started, ended, "confirmed" if confirmed else "ambiguous")


def _remove(started: float, ended: float) -> Operation:
    return Operation(0, "dict", "k", "remove", None, started, ended, "confirmed")
