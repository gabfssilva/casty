"""A chaos run: node processes, the traffic of every workload, the faults of a seeded schedule, and the invariants
checked after each fault and once more at rest.

`arranged` reads what run the environment asks for, `run` runs it, and the `Report` says what came of it. Whether it
passed or not, the output directory keeps `run.json` (the settings, the seed, the schedule and each step as it went),
`journal.jsonl.gz` (every call of the traffic), `report.txt`, the stderr of every node process, and the SQLite store
the nodes shared. Pointing `CHAOS_REPLAY` at a `run.json` runs its schedule again, with its seed.
"""

from __future__ import annotations

import asyncio
import json
import random
import tempfile
from collections import Counter as Tally
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import assert_never

from tests.chaos import records
from tests.chaos.fleet import Fleet, Stuck
from tests.chaos.journal import Journal, Stage, Verdict, Workload
from tests.chaos.node import open_files, timing
from tests.chaos.schedule import (
    FAULTS,
    Audit,
    Crash,
    Heal,
    Isolate,
    Leave,
    Outage,
    Partition,
    Restart,
    Skew,
    Slow,
    Start,
    Step,
    decoded,
    described,
    encoded,
    kind,
    plan,
    removal,
)
from tests.chaos.workloads import WORKLOADS
from tests.cluster import Timing

KINDS = tuple(WORKLOADS)
"""The names of every workload, which is what a run puts under its traffic unless told otherwise."""


@dataclass(frozen=True)
class Settings:
    """What a run is made of. The seed decides the schedule and every choice the traffic makes.

    `nodes` start the cluster, and `spare` more slots are there for machines to join on. `minutes` is how long the
    schedule runs, not counting the waits for the cluster to converge. `workers` share `rate` calls per second over
    `clients` clients. An audit waits `settle` seconds after the cluster converged, and gives each invariant `within`
    seconds to hold.
    """

    seed: int
    nodes: int = 20
    spare: int = 4
    minutes: float = 5.0
    workers: int = 16
    clients: int = 4
    rate: float = 300.0
    faults: tuple[str, ...] = FAULTS
    workloads: tuple[str, ...] = KINDS
    settle: float = 2.0
    within: float = 30.0

    def __post_init__(self) -> None:
        if self.nodes < 5:
            raise ValueError(f"a run needs at least 5 nodes to lose one at a time within quorum, not {self.nodes}")
        if unknown := set(self.workloads) - WORKLOADS.keys():
            raise ValueError(f"unknown workloads {sorted(unknown)}; there are {', '.join(WORKLOADS)}")

    @property
    def slots(self) -> int:
        return self.nodes + self.spare

    @classmethod
    def environment(cls, environ: Mapping[str, str], /) -> Settings:
        """The settings the `CHAOS_*` variables ask for, the defaults for the rest, and a random seed if none is set:

        `CHAOS_SEED`, `CHAOS_NODES`, `CHAOS_SPARE`, `CHAOS_MINUTES`, `CHAOS_WORKERS`, `CHAOS_CLIENTS`, `CHAOS_RATE`,
        `CHAOS_SETTLE`, `CHAOS_WITHIN`, and `CHAOS_FAULTS` and `CHAOS_WORKLOADS` as lists separated by commas.
        """
        default = cls(seed=0)
        seed = environ.get("CHAOS_SEED")
        return cls(
            seed=random.SystemRandom().randrange(2**32) if seed is None else int(seed),
            nodes=int(environ.get("CHAOS_NODES", default.nodes)),
            spare=int(environ.get("CHAOS_SPARE", default.spare)),
            minutes=float(environ.get("CHAOS_MINUTES", default.minutes)),
            workers=int(environ.get("CHAOS_WORKERS", default.workers)),
            clients=int(environ.get("CHAOS_CLIENTS", default.clients)),
            rate=float(environ.get("CHAOS_RATE", default.rate)),
            faults=_names(environ.get("CHAOS_FAULTS"), default.faults),
            workloads=_names(environ.get("CHAOS_WORKLOADS"), default.workloads),
            settle=float(environ.get("CHAOS_SETTLE", default.settle)),
            within=float(environ.get("CHAOS_WITHIN", default.within)),
        )

    def encoded(self) -> dict[str, object]:
        return {
            "seed": self.seed,
            "nodes": self.nodes,
            "spare": self.spare,
            "minutes": self.minutes,
            "workers": self.workers,
            "clients": self.clients,
            "rate": self.rate,
            "faults": list(self.faults),
            "workloads": list(self.workloads),
            "settle": self.settle,
            "within": self.within,
        }

    @classmethod
    def decoded(cls, raw: object, /) -> Settings:
        held = records.record(raw)
        return cls(
            seed=records.integer(held["seed"]),
            nodes=records.integer(held["nodes"]),
            spare=records.integer(held["spare"]),
            minutes=records.number(held["minutes"]),
            workers=records.integer(held["workers"]),
            clients=records.integer(held["clients"]),
            rate=records.number(held["rate"]),
            faults=records.texts(held["faults"]),
            workloads=records.texts(held["workloads"]),
            settle=records.number(held["settle"]),
            within=records.number(held["within"]),
        )


def arranged(environ: Mapping[str, str], /) -> tuple[Settings, tuple[Step, ...]]:
    """The settings and the schedule of the run the environment asks for: the replay of a dump, or a new run."""
    if replay := environ.get("CHAOS_REPLAY"):
        held = records.record(json.loads(Path(replay).read_text()))
        return Settings.decoded(held["settings"]), tuple(decoded(step) for step in records.items(held["schedule"]))
    settings = Settings.environment(environ)
    schedule = plan(
        seed=settings.seed,
        nodes=settings.nodes,
        slots=settings.slots,
        duration=settings.minutes * 60,
        faults=settings.faults,
        timing=timing(settings.nodes),
    )
    return settings, schedule


def destination(environ: Mapping[str, str], settings: Settings, /) -> Path:
    """Where a run keeps what it leaves: `CHAOS_OUTPUT`, or a new temporary directory named after the seed."""
    if chosen := environ.get("CHAOS_OUTPUT"):
        path = Path(chosen)
        path.mkdir(parents=True, exist_ok=True)
        return path
    return Path(tempfile.mkdtemp(prefix=f"casty-chaos-{settings.seed}-"))


@dataclass(frozen=True)
class Applied:
    """A step as it went: when it began on the clock of the run, how long it took, and what came of it."""

    index: int
    step: Step
    began: float
    took: float
    note: str = ""

    def __str__(self) -> str:
        note = f" — {self.note}" if self.note else ""
        return f"[{self.began:8.1f}s +{self.took:5.1f}s] #{self.index} {described(self.step.action)}{note}"


@dataclass
class Report:
    """What a run did, and what its invariants came to."""

    settings: Settings
    schedule: tuple[Step, ...]
    output: Path
    applied: list[Applied] = field(default_factory=list[Applied])
    verdicts: list[Verdict] = field(default_factory=list[Verdict])
    operations: Tally[tuple[str, str]] = field(default_factory=Tally[tuple[str, str]])
    """Calls of the traffic, by workload and outcome."""
    started: int = 0
    peak: int = 0
    elapsed: float = 0.0
    failure: str | None = None

    @property
    def replay(self) -> str:
        return f"CASTY_CHAOS=1 CHAOS_REPLAY={self.output / 'run.json'} uv run pytest tests/chaos -s"

    def text(self) -> str:
        settings = self.settings
        faults = Tally(kind(applied.step.action) for applied in self.applied)
        lines = [
            f"casty chaos run, seed {settings.seed}: {'FAILED' if self.failure else 'passed'}",
            f"  nodes: {settings.nodes} at the start on {settings.slots} slots, {self.started} started in all, "
            f"{self.peak} up at most",
            f"  traffic: {settings.workers} workers over {settings.clients} clients, up to {settings.rate:.0f} calls/s",
            f"  time: {settings.minutes:.1f} minutes of schedule, {self.elapsed / 60:.1f} minutes in all",
            f"  steps applied: {len(self.applied)} of {len(self.schedule)} — "
            + (", ".join(f"{count} {name}" for name, count in faults.most_common()) or "none"),
            *self._calls(),
            "  invariants:",
            *self._invariants(),
            f"  output: {self.output}",
            f"  replay: {self.replay}",
        ]
        if self.failure is not None:
            lines += [f"  failure: {self.failure}", "  the steps up to it:", *(f"    {step}" for step in self.applied)]
        return "\n".join(lines)

    def encoded(self) -> dict[str, object]:
        return {
            "settings": self.settings.encoded(),
            "schedule": [encoded(step) for step in self.schedule],
            "applied": [
                {"index": step.index, "began": step.began, "took": step.took, "note": step.note, **encoded(step.step)}
                for step in self.applied
            ],
            "verdicts": [
                {"invariant": verdict.invariant, "checked": verdict.checked, "violations": list(verdict.violations)}
                for verdict in self.verdicts
            ],
            "elapsed": self.elapsed,
            "failure": self.failure,
        }

    def _calls(self) -> list[str]:
        workloads = sorted({workload for workload, _ in self.operations})

        def tally(counts: Mapping[str, int]) -> str:
            confirmed, ambiguous = counts.get("confirmed", 0), counts.get("ambiguous", 0)
            return f"{sum(counts.values())} calls, {confirmed} confirmed, {ambiguous} ambiguous"

        per = {
            workload: {outcome: n for (name, outcome), n in self.operations.items() if name == workload}
            for workload in workloads
        }
        whole: Tally[str] = Tally()
        for counts in per.values():
            whole.update(counts)
        return [f"  calls: {tally(whole)}", *(f"    {workload}: {tally(per[workload])}" for workload in workloads)]

    def _invariants(self) -> list[str]:
        audits: Tally[str] = Tally()
        checked: Tally[str] = Tally()
        broken: dict[str, list[str]] = {}
        for verdict in self.verdicts:
            audits[verdict.invariant] += 1
            checked[verdict.invariant] += verdict.checked
            broken.setdefault(verdict.invariant, []).extend(verdict.violations)
        lines: list[str] = []
        for invariant, times in audits.items():
            violations = broken[invariant]
            mark = "FAIL" if violations else "ok  "
            lines.append(
                f"    {mark} {invariant}: {checked[invariant]} checked over {times} audits, "
                f"{len(violations)} violations"
            )
            lines += [f"           {violation}" for violation in violations[:_SHOWN]]
        return lines or ["    none checked"]


async def run(settings: Settings, schedule: Sequence[Step], output: Path, /) -> Report:
    """Run `schedule` against a fleet under the traffic of `settings`, and report what came of it.

    However the run ends, an exception included, the report is printed and what the run leaves is written to `output`.
    """
    open_files()
    report = Report(settings, tuple(schedule), output)
    journal = Journal()
    _say(journal, f"seed {settings.seed}: {settings.nodes} nodes, {len(schedule)} steps; output in {output}")
    ended = False
    try:
        period = timing(settings.nodes)
        async with Fleet.running(slots=settings.slots, size=settings.nodes, timing=period, output=output) as fleet:
            report.failure = await _drive(settings, fleet, journal, period, report)
            report.started, report.peak = fleet.started, fleet.peak
        ended = True
    finally:
        report.elapsed = journal.now()
        report.operations = Tally((operation.workload, operation.outcome) for operation in journal.operations)
        if not ended and report.failure is None:
            report.failure = "the run raised: the traceback follows"
        print(report.text(), flush=True)
        _dump(report, journal)
    return report


async def _drive(settings: Settings, fleet: Fleet, journal: Journal, period: Timing, report: Report) -> str | None:
    """Boot the fleet, run the traffic through the schedule, and check what holds at rest; answer what failed."""
    recovery = timedelta(seconds=3 * removal(period) + 30.0)
    within = timedelta(seconds=settings.within)
    try:
        await fleet.boot(range(settings.nodes), recovery + timedelta(seconds=settings.nodes))
    except Stuck as stuck:
        return f"the cluster did not start: {stuck}"
    senders = await fleet.clients(settings.clients)
    workloads = [WORKLOADS[name](Stage(journal, senders, fleet.reachable)) for name in settings.workloads]
    _say(journal, f"{len(fleet.up)} nodes up, traffic of {', '.join(settings.workloads)} starting")
    stop = asyncio.Event()
    async with asyncio.TaskGroup() as traffic:
        for worker in range(settings.workers):
            traffic.create_task(_work(worker, settings, workloads, len(senders), stop))
        try:
            failure = await _apply(settings, fleet, journal, workloads, report, recovery)
        finally:
            stop.set()
    if failure is not None:
        return failure
    _say(journal, "schedule done, traffic stopped; checking at rest")
    began = journal.now()
    try:
        await fleet.heal()
        await fleet.converged(recovery)
    except Stuck as stuck:
        return f"the cluster did not settle at rest: {stuck}"
    await asyncio.sleep(settings.settle)
    verdicts = [verdict for workload in workloads for verdict in await workload.rest(0, within)]
    report.verdicts += verdicts
    report.applied.append(Applied(len(report.schedule), Step(0.0, Audit()), began, journal.now() - began, "at rest"))
    _say(journal, _summary(verdicts))
    return _broken(verdicts, "at rest")


async def _apply(
    settings: Settings,
    fleet: Fleet,
    journal: Journal,
    workloads: Sequence[Workload],
    report: Report,
    recovery: timedelta,
) -> str | None:
    """Apply every step of the schedule in turn; answer the first failure, and stop there."""
    within = timedelta(seconds=settings.within)
    audits = 0
    for index, step in enumerate(report.schedule):
        await asyncio.sleep(step.after)
        began = journal.now()
        note = ""
        action = step.action
        try:
            fleet.check()
            match action:
                case Crash(slot):
                    await fleet.crash(slot)
                case Leave(slot):
                    await fleet.leave(slot)
                case Start(slot, version):
                    await fleet.start(slot, version)
                case Restart(slot):
                    await fleet.restart(slot)
                case Outage():
                    slots = await fleet.halt()
                    dead = journal.now()
                    for workload in workloads:
                        workload.outage(dead)
                    await fleet.boot(slots, recovery + timedelta(seconds=len(slots)))
                case Partition(groups):
                    await fleet.partition(groups)
                case Isolate(slot):
                    await fleet.isolate(slot)
                case Slow(slot, delay):
                    await fleet.slow(slot, delay)
                case Skew(slot, offset):
                    await fleet.skew(slot, offset)
                case Heal():
                    await fleet.heal()
                case Audit():
                    converged = await fleet.converged(recovery)
                    await asyncio.sleep(settings.settle)
                    reader = audits % settings.clients
                    audits += 1
                    verdicts = [verdict for workload in workloads for verdict in await workload.audit(reader, within)]
                    report.verdicts += verdicts
                    note = f"converged in {converged:.1f}s; {_summary(verdicts)}; {len(journal.operations)} calls"
                    if (failure := _broken(verdicts, f"at step #{index}")) is not None:
                        report.applied.append(Applied(index, step, began, journal.now() - began, note))
                        print(report.applied[-1], flush=True)
                        return failure
                case _:
                    assert_never(action)
        except Stuck as stuck:
            report.applied.append(Applied(index, step, began, journal.now() - began, f"stuck: {stuck}"))
            print(report.applied[-1], flush=True)
            return f"at step #{index} ({described(action)}): {stuck}"
        applied = Applied(index, step, began, journal.now() - began, note)
        report.applied.append(applied)
        report.started, report.peak = fleet.started, fleet.peak
        print(applied, flush=True)
    return None


async def _work(
    worker: int, settings: Settings, workloads: Sequence[Workload], senders: int, stop: asyncio.Event
) -> None:
    """Make calls until `stop`, each of a workload and from a sender that the seed of the worker chooses."""
    rng = random.Random(f"{settings.seed}/{worker}")
    pace = settings.workers / settings.rate
    while not stop.is_set():
        await rng.choice(workloads).operate(rng.randrange(senders), rng)
        await asyncio.sleep(pace)


def _broken(verdicts: Sequence[Verdict], when: str) -> str | None:
    broken = [verdict for verdict in verdicts if verdict.violations]
    if not broken:
        return None
    shown = "; ".join(f"{verdict.invariant}: {verdict.violations[0]}" for verdict in broken)
    return f"{len(broken)} invariants broken {when} — {shown}"


def _summary(verdicts: Sequence[Verdict]) -> str:
    checked = sum(verdict.checked for verdict in verdicts)
    violations = sum(len(verdict.violations) for verdict in verdicts)
    return f"{len(verdicts)} invariants, {checked} checked, {violations} violations"


def _names(raw: str | None, default: tuple[str, ...]) -> tuple[str, ...]:
    if raw is None:
        return default
    return tuple(name.strip() for name in raw.split(",") if name.strip())


def _say(journal: Journal, text: str) -> None:
    print(f"[{journal.now():8.1f}s] {text}", flush=True)


def _dump(report: Report, journal: Journal) -> None:
    report.output.mkdir(parents=True, exist_ok=True)
    (report.output / "run.json").write_text(json.dumps(report.encoded(), indent=1))
    (report.output / "report.txt").write_text(report.text() + "\n")
    journal.dump(report.output / "journal.jsonl.gz")


_SHOWN = 20
"""How many violations of one invariant a report lists."""
