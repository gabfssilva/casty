"""The planner of the chaos run. It starts nothing, so it runs with every `pytest`."""

from __future__ import annotations

import json
from itertools import pairwise

from reliability.chaos.schedule import (
    FAULTS,
    Audit,
    Crash,
    Heal,
    Leave,
    Outage,
    Partition,
    Restart,
    Start,
    Step,
    decoded,
    encoded,
    kind,
    plan,
    removal,
)
from reliability.node import timing

NODES = 20
SLOTS = 24
TIMING = timing(NODES)


def describe_plan() -> None:
    def it_plans_the_same_schedule_for_the_same_seed() -> None:
        assert _planned(7) == _planned(7)
        assert _planned(7) != _planned(8)

    def it_reads_back_what_a_dump_writes() -> None:
        schedule = _planned(7)
        assert tuple(decoded(json.loads(json.dumps(encoded(step)))) for step in schedule) == schedule

    def it_uses_every_kind_of_fault() -> None:
        kinds = {kind(step.action) for step in _planned(7, minutes=60)}
        assert kinds == {
            "crash",
            "restart",
            "leave",
            "start",
            "outage",
            "partition",
            "isolate",
            "slow",
            "skew",
            "heal",
            "audit",
        }

    def it_plans_only_audits_without_faults() -> None:
        schedule = plan(seed=7, nodes=NODES, slots=SLOTS, duration=600.0, faults=(), timing=TIMING)
        assert {kind(step.action) for step in schedule} == {"audit"}

    def it_takes_one_machine_out_at_a_time_and_audits_before_the_next() -> None:
        for seed in range(20):
            up = set(range(NODES))
            changed = False
            for step in _planned(seed):
                match step.action:
                    case Crash(slot) | Leave(slot):
                        assert not changed and slot in up, f"seed {seed}: {step}"
                        up.remove(slot)
                        changed = True
                    case Start(slot):
                        assert not changed and slot not in up, f"seed {seed}: {step}"
                        up.add(slot)
                        changed = True
                    case Restart(slot):
                        assert not changed and slot in up, f"seed {seed}: {step}"
                        changed = True
                    case Outage():
                        assert not changed, f"seed {seed}: {step}"
                        changed = True
                    case Audit():
                        changed = False
                    case _:
                        pass
                assert NODES - 2 <= len(up) <= NODES + 2, f"seed {seed}: {len(up)} up after {step}"
                assert len({0, 1, 2} - up) <= 1, f"seed {seed}: two seeds down after {step}"

    def it_heals_a_partition_before_the_majority_could_remove_the_minority() -> None:
        for seed in range(20):
            for step, then in pairwise(_planned(seed)):
                match step.action, then.action:
                    case Partition((minority, majority)), Heal():
                        assert len(minority) < len(majority), f"seed {seed}: {step}"
                        assert then.after < removal(TIMING), f"seed {seed}: {step} lasts {then.after}s"
                    case Partition(), _:
                        raise AssertionError(f"seed {seed}: {step} is followed by {then}, not by a heal")
                    case _:
                        pass


def _planned(seed: int, *, minutes: float = 10.0) -> tuple[Step, ...]:
    return plan(seed=seed, nodes=NODES, slots=SLOTS, duration=minutes * 60, faults=FAULTS, timing=TIMING)
