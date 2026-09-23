"""The faults of a chaos run, decided from its seed alone before it starts.

A schedule names machines by slot, an address the run keeps for its whole length, and says how long after the previous
step each one comes. Deciding it up front is what makes a run replayable: the same seed gives the same schedule
whatever the cluster did with the last one, and a dump carries the schedule itself, so a replay does not even depend on
this planner staying the same.

The planner keeps each fault within what the README promises to survive, since a violation beyond it would be noise:

- one machine is out at a time — crashed, restarted, leaving, joining — and the cluster converges and is audited before
  the next one goes, so a key never loses two of its replicas before the ring has replaced the first;
- a partition ends before the majority side would remove the other one (`suspect_after + dead_after + remove_after`),
  because a removal while two replicas of a key sit on the minority side rebuilds the key from the one that is left;
  an isolated machine is one replica of any key, so its isolation may run past that and see it removed and rejoin;
- slow links stay well under `suspect_after`, and clock skew well under the TTLs of leases and barriers, which the
  README says need synchronized clocks;
- at most one of the first three slots is down at a time, since they are the seeds of the clients;
- an outage kills every machine at once and starts each again on its slot. The README promises nothing across it of
  the state held in memory only, so each workload stops expecting what it confirmed before it (`Workload.outage`), but
  for the ledger its store keeps.
"""

from __future__ import annotations

import random
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from typing import assert_never

from tests.chaos import records
from tests.chaos.node import Version, version
from tests.cluster import Timing


@dataclass(frozen=True)
class Crash:
    """Kill the process on `slot`: nothing is drained, and its state is gone."""

    slot: int


@dataclass(frozen=True)
class Leave:
    """Shut the node on `slot` down in an orderly way."""

    slot: int


@dataclass(frozen=True)
class Start:
    """Start a node on `slot`, which is down: a join, or the return of a machine that crashed or left."""

    slot: int
    version: Version = "app"


@dataclass(frozen=True)
class Restart:
    """Kill the process on `slot` and start a new one on the same address at once, before anyone noticed."""

    slot: int


@dataclass(frozen=True)
class Outage:
    """Kill the process on every slot at once, and start each again on its address, running the code it ran: a power
    cut, or a deploy that stops everything. Nothing survives it but what the store keeps."""


@dataclass(frozen=True)
class Partition:
    """Cut every link between two slots of different groups, both ways."""

    groups: tuple[tuple[int, ...], ...]


@dataclass(frozen=True)
class Isolate:
    """Cut every link of `slot`: the machine runs, and nothing reaches it or leaves it."""

    slot: int


@dataclass(frozen=True)
class Slow:
    """Delay every link of `slot` by `delay` seconds, both ways."""

    slot: int
    delay: float


@dataclass(frozen=True)
class Skew:
    """Set the wall clock of the machine of `slot` `offset` seconds off; it stays so across its restarts."""

    slot: int
    offset: float


@dataclass(frozen=True)
class Heal:
    """End every partition, isolation and slow link."""


@dataclass(frozen=True)
class Audit:
    """Wait for the cluster to converge on the machines that are up, then check every invariant."""


type Action = Crash | Leave | Start | Restart | Outage | Partition | Isolate | Slow | Skew | Heal | Audit


@dataclass(frozen=True)
class Step:
    """An action, `after` seconds after the previous step finished."""

    after: float
    action: Action


FAULTS = ("crash", "restart", "leave", "join", "upgrade", "outage", "partition", "isolate", "slow", "skew")
"""The kinds of fault the planner chooses from. `join` starts a machine on a free slot; `upgrade` is a step of a
rolling deploy: a machine leaves and comes back running `tests.deploy`; `outage` restarts the whole cluster."""

SKEW = 1.5
"""The furthest a clock is set off, in seconds."""


def plan(
    *, seed: int, nodes: int, slots: int, duration: float, faults: Collection[str], timing: Timing
) -> tuple[Step, ...]:
    """The steps of a run: episodes of one fault each, and an audit after each, for `duration` seconds.

    The cluster starts with slots `0..nodes-1` up and stays within two machines of `nodes`.
    """
    if unknown := set(faults) - set(FAULTS):
        raise ValueError(f"unknown faults {sorted(unknown)}; the planner knows {', '.join(FAULTS)}")
    rng = random.Random(seed)
    machines = _Machines(nodes, slots)
    steps: list[Step] = []
    elapsed = 0.0
    while elapsed < duration:
        episode = _episode(rng, machines, [fault for fault in FAULTS if fault in faults], timing)
        steps += episode
        elapsed += sum(step.after for step in episode)
    return tuple(steps)


def removal(timing: Timing) -> float:
    """Seconds from the last word of a node to its removal from the ring, at the soonest."""
    return (timing.suspect_after + timing.dead_after + (timing.remove_after or timing.dead_after)).total_seconds()


def kind(action: Action) -> str:
    match action:
        case Crash():
            return "crash"
        case Leave():
            return "leave"
        case Start():
            return "start"
        case Restart():
            return "restart"
        case Outage():
            return "outage"
        case Partition():
            return "partition"
        case Isolate():
            return "isolate"
        case Slow():
            return "slow"
        case Skew():
            return "skew"
        case Heal():
            return "heal"
        case Audit():
            return "audit"
        case _:
            assert_never(action)


def described(action: Action) -> str:
    match action:
        case Crash(slot) | Leave(slot) | Restart(slot) | Isolate(slot):
            return f"{kind(action)} slot {slot}"
        case Start(slot, running):
            return f"start slot {slot}" + ("" if running == "app" else f" running {running}")
        case Partition(groups):
            return "partition " + " | ".join(",".join(map(str, group)) for group in groups)
        case Slow(slot, delay):
            return f"slow slot {slot} by {delay * 1000:.0f} ms"
        case Skew(slot, offset):
            return f"skew slot {slot} by {offset:+.2f} s"
        case Outage() | Heal() | Audit():
            return kind(action)
        case _:
            assert_never(action)


def encoded(step: Step) -> dict[str, object]:
    action = step.action
    fields: dict[str, object]
    match action:
        case Crash(slot) | Leave(slot) | Restart(slot) | Isolate(slot):
            fields = {"slot": slot}
        case Start(slot, running):
            fields = {"slot": slot, "version": running}
        case Partition(groups):
            fields = {"groups": [list(group) for group in groups]}
        case Slow(slot, delay):
            fields = {"slot": slot, "delay": delay}
        case Skew(slot, offset):
            fields = {"slot": slot, "offset": offset}
        case Outage() | Heal() | Audit():
            fields = {}
        case _:
            assert_never(action)
    return {"after": step.after, "kind": kind(action), **fields}


def decoded(raw: object) -> Step:
    held = records.record(raw)
    action: Action
    match records.text(held["kind"]):
        case "crash":
            action = Crash(records.integer(held["slot"]))
        case "leave":
            action = Leave(records.integer(held["slot"]))
        case "start":
            action = Start(records.integer(held["slot"]), version(held["version"]))
        case "restart":
            action = Restart(records.integer(held["slot"]))
        case "outage":
            action = Outage()
        case "partition":
            groups = (records.items(group) for group in records.items(held["groups"]))
            action = Partition(tuple(tuple(records.integer(slot) for slot in group) for group in groups))
        case "isolate":
            action = Isolate(records.integer(held["slot"]))
        case "slow":
            action = Slow(records.integer(held["slot"]), records.number(held["delay"]))
        case "skew":
            action = Skew(records.integer(held["slot"]), records.number(held["offset"]))
        case "heal":
            action = Heal()
        case "audit":
            action = Audit()
        case other:
            raise ValueError(f"unknown step {other!r}")
    return Step(records.number(held["after"]), action)


class _Machines:
    """Which slots the schedule has up so far, and the code each one runs, as the planner goes."""

    def __init__(self, nodes: int, slots: int) -> None:
        self.nodes = nodes
        self.up: set[int] = set(range(nodes))
        self.slots = slots
        self.versions: dict[int, Version] = {slot: "app" for slot in range(slots)}

    def down(self) -> list[int]:
        return [slot for slot in range(self.slots) if slot not in self.up]

    def removable(self) -> list[int]:
        """The slots that can go down now without taking the cluster below its floor or a second seed with it."""
        if len(self.up) - 1 < max(3, self.nodes - 2):
            return []
        return [slot for slot in sorted(self.up) if slot >= _SEEDS or set(range(_SEEDS)) - {slot} <= self.up]


def _episode(rng: random.Random, machines: _Machines, faults: Sequence[str], timing: Timing) -> list[Step]:
    """One fault and what ends it, the machines it touches chosen by `rng`, and the audit after it."""
    bound = removal(timing)
    up = sorted(machines.up)
    possible = [fault for fault in faults if _possible(fault, machines)]
    pause = rng.uniform(1.0, 4.0)
    if not possible:
        return [Step(max(pause, 10.0), Audit())]
    match rng.choice(possible):
        case "crash":
            slot = rng.choice(machines.removable())
            machines.up.remove(slot)
            return [Step(pause, Crash(slot)), Step(0.0, Audit())]
        case "leave":
            slot = rng.choice(machines.removable())
            machines.up.remove(slot)
            return [Step(pause, Leave(slot)), Step(0.0, Audit())]
        case "restart":
            slot = rng.choice(machines.removable())
            return [Step(pause, Restart(slot)), Step(0.0, Audit())]
        case "join":
            slot = rng.choice(machines.down())
            machines.up.add(slot)
            return [Step(pause, Start(slot, machines.versions[slot])), Step(0.0, Audit())]
        case "upgrade":
            slot = rng.choice([slot for slot in machines.removable() if machines.versions[slot] == "app"])
            machines.versions[slot] = "deploy"
            return [Step(pause, Leave(slot)), Step(0.0, Audit()), Step(1.0, Start(slot, "deploy")), Step(0.0, Audit())]
        case "outage":
            return [Step(pause, Outage()), Step(0.0, Audit())]
        case "partition":
            minority = sorted(rng.sample(up, rng.randint(2, (len(up) - 1) // 2)))
            rest = [slot for slot in up if slot not in minority]
            lasting = rng.uniform(0.3, 0.7) * bound
            return [Step(pause, Partition((tuple(minority), tuple(rest)))), Step(lasting, Heal()), Step(0.0, Audit())]
        case "isolate":
            lasting = rng.uniform(0.3, 2.0) * bound
            return [Step(pause, Isolate(rng.choice(up))), Step(lasting, Heal()), Step(0.0, Audit())]
        case "slow":
            delay = rng.uniform(0.02, min(0.2, timing.suspect_after.total_seconds() / 10))
            return [Step(pause, Slow(rng.choice(up), delay)), Step(rng.uniform(2.0, 10.0), Heal()), Step(0.0, Audit())]
        case "skew":
            offset = 0.0 if rng.random() < 0.25 else rng.uniform(-SKEW, SKEW)
            return [Step(pause, Skew(rng.choice(up), offset))]
        case other:
            raise AssertionError(f"no episode for {other!r}")


def _possible(fault: str, machines: _Machines) -> bool:
    match fault:
        case "crash" | "leave" | "restart":
            return bool(machines.removable())
        case "join":
            return bool(machines.down()) and len(machines.up) < machines.nodes + 2
        case "upgrade":
            return any(machines.versions[slot] == "app" for slot in machines.removable())
        case "outage":
            return True
        case "partition":
            return len(machines.up) >= 5
        case "isolate" | "slow" | "skew":
            return len(machines.up) >= 3
        case _:
            return False


_SEEDS = 3
"""The slots whose addresses the clients are seeded with."""
