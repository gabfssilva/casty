"""One node of a chaos run, in a process of its own, driven by the runner over its stdin and stdout.

    python -m tests.chaos.node '{"bind": "127.0.0.1:20001", "seeds": ["127.0.0.1:20000"], "size": 20, "store": "s.db"}'

The argument is the `Launch` of the node: the address it binds and advertises, its seeds, the size of the cluster its
`timing` is scaled to, the SQLite file of the store every node of the run shares, and the `version` of the code it
runs. Every connection the node dials goes through a `tests.cluster.Proxy` of its own, so the runner cuts, slows or
skews this machine by sending it its `Network`. Killing the process is the crash.

The protocol is one JSON object per line. The runner sends `{"network": ...}`, `{"members": <question>}` and
`{"leave": null}`; the node says `{"ready": <node>}` once it is in the cluster, `{"members": <view>}` to each question,
and `{"left": null}` once it is out. The end of its input is a leave, so a runner that dies takes its nodes with it.
"""

from __future__ import annotations

import asyncio
import json
import resource
import sys
import time
from collections.abc import Coroutine, Mapping
from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import Literal
from unittest.mock import patch
from uuid import UUID

from casty import ActorDefinition, ActorSystem, Cluster, NodeId
from casty.sqlite import SQLiteStore
from tests import deploy
from tests.chaos import records
from tests.cluster import FAST, OVERLAY, Proxy, Timing, Versioned

type Version = Literal["app", "deploy"]

VERSIONS: Mapping[Version, tuple[ActorDefinition, ...]] = {"app": (), "deploy": (deploy.ledger,)}
"""The code a node runs: `tests.app` as it is, or the next deploy of it, which `tests.deploy` is."""


def version(raw: object) -> Version:
    match records.text(raw):
        case "app":
            return "app"
        case "deploy":
            return "deploy"
        case other:
            raise ValueError(f"unknown version {other!r}")


def timing(size: int) -> Timing:
    """The periods of a run whose cluster is `size` nodes, the same on every node and on the runner.

    A node pings its active view and one member outside it per heartbeat, so a member that is not a neighbour hears
    from it about once every `size` heartbeats: the time to suspect grows with the cluster, or a member merely far down
    that round is taken for dead. Removal follows death closely, so that the ring heals between two faults.
    """
    heartbeat = timedelta(milliseconds=100)
    return replace(
        FAST,
        heartbeat=heartbeat,
        suspect_after=max(timedelta(seconds=2), heartbeat * 3 * size),
        dead_after=timedelta(seconds=1),
        remove_after=timedelta(seconds=2),
        anti_entropy=timedelta(seconds=1),
        shuffle_every=timedelta(seconds=1),
        graft_after=timedelta(milliseconds=200),
        idle_after=timedelta(seconds=10),
        ask_timeout=timedelta(seconds=5),
        write_timeout=timedelta(seconds=1),
        sync_every=timedelta(milliseconds=250),
        leave_timeout=timedelta(seconds=15),
    )


@dataclass(frozen=True)
class Launch:
    """What a node process is started with."""

    bind: str
    seeds: tuple[str, ...]
    size: int
    store: str
    version: Version = "app"

    def encoded(self) -> str:
        return json.dumps(
            {
                "bind": self.bind,
                "seeds": list(self.seeds),
                "size": self.size,
                "store": self.store,
                "version": self.version,
            }
        )

    @classmethod
    def decoded(cls, raw: str) -> Launch:
        held = records.record(json.loads(raw))
        return cls(
            records.text(held["bind"]),
            records.texts(held["seeds"]),
            records.integer(held["size"]),
            records.text(held["store"]),
            version(held.get("version", "app")),
        )


@dataclass(frozen=True)
class Network:
    """What the links of a machine are: the addresses it cannot reach, the delay to each slow one, and how far off
    its clock runs."""

    blocked: frozenset[str] = frozenset()
    delays: Mapping[str, float] = field(default_factory=dict[str, float])
    skew: float = 0.0

    def encoded(self) -> dict[str, object]:
        return {"blocked": sorted(self.blocked), "delays": dict(self.delays), "skew": self.skew}

    @classmethod
    def decoded(cls, raw: object) -> Network:
        held = records.record(raw)
        delays = records.record(held["delays"])
        return cls(
            frozenset(records.texts(held["blocked"])),
            {address: records.number(delay) for address, delay in delays.items()},
            records.number(held["skew"]),
        )


@dataclass(frozen=True)
class View:
    """The member table of a node as it answered one question: who the node is now, and every member it has not
    forgotten, with its status.

    Who the node is changes when the cluster removed it and it came back under a new incarnation.
    """

    question: int
    me: NodeId
    table: frozenset[tuple[NodeId, str]]

    def encoded(self) -> dict[str, object]:
        return {
            "question": self.question,
            "me": identity(self.me),
            "table": [{"node": identity(node), "status": status} for node, status in self.table],
        }

    @classmethod
    def decoded(cls, raw: object) -> View:
        held = records.record(raw)
        table = (records.record(row) for row in records.items(held["table"]))
        return cls(
            records.integer(held["question"]),
            known(held["me"]),
            frozenset((known(row["node"]), records.text(row["status"])) for row in table),
        )


def identity(node: NodeId) -> dict[str, object]:
    return {"address": node.address, "incarnation": str(node.incarnation)}


def known(raw: object) -> NodeId:
    held = records.record(raw)
    address = held["address"]
    return NodeId(None if address is None else records.text(address), UUID(records.text(held["incarnation"])))


def open_files() -> None:
    """Raise the soft limit of open files, which a node of a large cluster, and the runner of one, go over.

    Every peer costs a proxy listener and the connections through it, and macOS starts a process at 256.
    """
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    ceiling = 65_536 if hard == resource.RLIM_INFINITY else min(hard, 65_536)
    # macOS reports an infinite hard limit and refuses anything above `kern.maxfilesperproc`, so smaller ones follow.
    for limit in (ceiling, 24_576, 10_240, 4_096):
        if limit <= soft:
            return
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (limit, hard))
        except (ValueError, OSError):
            continue
        return


class Clock:
    """The wall clock of this machine: the real one, `skew` seconds off.

    The core reads the wall clock through `time.time`, and only for the deadlines that travel between nodes — a lease,
    a barrier — so skewing it is what a machine whose clock drifts looks like to them.
    """

    def __init__(self) -> None:
        self.skew = 0.0
        self._real = time.time

    def time(self) -> float:
        return self._real() + self.skew


class Links:
    """The proxies this node dials the others through, one per address, set to the network the runner last sent."""

    def __init__(self, tasks: asyncio.TaskGroup, /) -> None:
        self._tasks = tasks
        self._proxies: dict[str, Proxy] = {}
        self._plumbing: set[asyncio.Task[None]] = set()
        self._network = Network()

    def address(self, target: str) -> str:
        """The address to dial for `target`: the `address_map` of the node."""
        proxy = self._proxies.get(target)
        if proxy is None:
            proxy = self._proxies[target] = Proxy(self._spawn, target)
            self._set(target, proxy)
            self._spawn(proxy.serve())
        return proxy.address

    def apply(self, network: Network) -> None:
        self._network = network
        for target, proxy in self._proxies.items():
            self._set(target, proxy)

    async def close(self) -> None:
        plumbing = tuple(self._plumbing)
        for task in plumbing:
            task.cancel()
        if plumbing:
            await asyncio.wait(plumbing)
        for proxy in self._proxies.values():
            await proxy.close()

    def _set(self, target: str, proxy: Proxy, /) -> None:
        proxy.block(target in self._network.blocked)
        proxy.slow(timedelta(seconds=self._network.delays.get(target, 0.0)))

    def _spawn(self, work: Coroutine[None, None, None], /) -> None:
        task = self._tasks.create_task(work)
        self._plumbing.add(task)
        task.add_done_callback(self._plumbing.discard)


def say(kind: str, body: object, /) -> None:
    print(json.dumps({kind: body}), flush=True)


async def control(system: ActorSystem, links: Links, clock: Clock, /) -> None:
    """Do what the runner says, until it says to leave or stops saying anything."""
    commands = asyncio.StreamReader()
    await asyncio.get_running_loop().connect_read_pipe(lambda: asyncio.StreamReaderProtocol(commands), sys.stdin)
    while line := await commands.readline():
        command = records.record(json.loads(line))
        if "network" in command:
            network = Network.decoded(command["network"])
            links.apply(network)
            clock.skew = network.skew
        elif "members" in command:
            table = frozenset((member.node, member.status) for member in system.members)
            say("members", View(records.integer(command["members"]), system.node, table).encoded())
        elif "leave" in command:
            return
        else:
            raise ValueError(f"unknown command {command!r}")


async def main(launch: Launch, /) -> None:
    clock = Clock()
    period = timing(launch.size)
    with patch.object(time, "time", clock.time):
        async with SQLiteStore(launch.store) as store, asyncio.TaskGroup() as tasks:
            links = Links(tasks)
            try:
                cluster = Cluster(
                    bind=launch.bind,
                    seeds=launch.seeds,
                    address_map=links.address,
                    heartbeat=period.heartbeat,
                    suspect_after=period.suspect_after,
                    dead_after=period.dead_after,
                    remove_after=period.remove_after,
                    anti_entropy=period.anti_entropy,
                    overlay=replace(OVERLAY, shuffle_every=period.shuffle_every, graft_after=period.graft_after),
                )
                system = Versioned(
                    VERSIONS[launch.version],
                    cluster=cluster,
                    idle_after=period.idle_after,
                    ask_timeout=period.ask_timeout,
                    write_timeout=period.write_timeout,
                    leave_timeout=period.leave_timeout,
                    store=store,
                )
                async with system:
                    say("ready", identity(system.node))
                    await control(system, links, clock)
            finally:
                await links.close()
    say("left", None)


if __name__ == "__main__":
    open_files()
    asyncio.run(main(Launch.decoded(sys.argv[1])))
