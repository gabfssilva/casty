"""One node of a run, in a pod of its own.

    python -m reliability.node '{"address": "slot-3.casty:7400", "seeds": ["slot-0.casty:7400"], "size": 5, ...}'

The argument is the `Launch` of the node: the address it advertises, the name the headless service of the run gives
its pod; its seeds; the size of the cluster the periods of a chaos run are scaled to, or none for the periods casty has
by default; the URL of the store every node of the run shares; and the `version` of the code it runs. It listens on
every interface, on the port of its address. Its links are cut and slowed from outside, by Chaos Mesh, so it dials
every address as it is. The deletion of its pod sends it SIGTERM, which is the leave.

It says `{"ready": <node>}` once it is in the cluster and `{"left": null}` once it is out, on stdout, which the runner
reads from the log of the pod. The runner asks it things on the control port, one JSON object per line each way:
`{"members": <question>}` is answered with its view, `{"network": ...}` sets its clock and is answered with `{}`, and
`{"crash": null}` ends the process at once — by `os._exit`, since the first process of a container ignores a signal
it sends itself.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import time
from collections.abc import Mapping
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import timedelta
from functools import partial
from typing import Literal
from unittest.mock import patch
from uuid import UUID

from casty import ActorDefinition, ActorSystem, Cluster, NodeId, Overlay, Store
from casty.stores import SQL
from reliability import deploy, records

CONTROL = 7401
"""The port a pod of a run is asked things on."""

type Version = Literal["app", "deploy"]

VERSIONS: Mapping[Version, tuple[ActorDefinition, ...]] = {"app": (), "deploy": (deploy.ledger,)}
"""The code a node runs: `reliability.actors` as it is, or the next deploy of it, which `reliability.deploy` is."""


def version(raw: object) -> Version:
    match records.text(raw):
        case "app":
            return "app"
        case "deploy":
            return "deploy"
        case other:
            raise ValueError(f"unknown version {other!r}")


@dataclass(frozen=True)
class Timing:
    """The periods of a chaos run, spread over `Cluster`, `ActorSystem` and `Client`."""

    heartbeat: timedelta
    suspect_after: timedelta
    dead_after: timedelta
    remove_after: timedelta
    anti_entropy: timedelta
    shuffle_every: timedelta
    graft_after: timedelta
    idle_after: timedelta
    ask_timeout: timedelta
    write_timeout: timedelta
    sync_every: timedelta
    leave_timeout: timedelta


def timing(size: int) -> Timing:
    """The periods of a chaos run whose cluster is `size` nodes, the same on every node and on the runner.

    A node pings its active view and one member outside it per heartbeat, so a member that is not a neighbour hears
    from it about once every `size` heartbeats: the time to suspect grows with the cluster, or a member merely far down
    that round is taken for dead. Removal follows death closely, so that the ring heals between two faults.
    """
    heartbeat = timedelta(milliseconds=100)
    return Timing(
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
    """What a node is started with: the address it advertises, its seeds, the size of the cluster its periods are
    scaled to (`None` for the defaults of casty), and the URL of the store every node of the run shares, if any."""

    address: str
    seeds: tuple[str, ...]
    size: int | None
    store: str | None
    version: Version = "app"

    def encoded(self) -> str:
        return json.dumps(
            {
                "address": self.address,
                "seeds": list(self.seeds),
                "size": self.size,
                "store": self.store,
                "version": self.version,
            }
        )

    @classmethod
    def decoded(cls, raw: str) -> Launch:
        held = records.record(json.loads(raw))
        size, store = held["size"], held["store"]
        return cls(
            records.text(held["address"]),
            records.texts(held["seeds"]),
            None if size is None else records.integer(size),
            None if store is None else records.text(store),
            version(held["version"]),
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


def say(kind: str, body: object, /) -> None:
    print(json.dumps({kind: body}), flush=True)


def system(launch: Launch, /, *, store: Store | None) -> ActorSystem:
    """The node `launch` describes, listening on every interface on the port of its address."""
    _, port = launch.address.rsplit(":", 1)
    bind = f"0.0.0.0:{port}"
    if launch.size is None:
        node = ActorSystem(cluster=Cluster(bind=bind, advertise=launch.address, seeds=launch.seeds), store=store)
    else:
        period = timing(launch.size)
        cluster = Cluster(
            bind=bind,
            advertise=launch.address,
            seeds=launch.seeds,
            heartbeat=period.heartbeat,
            suspect_after=period.suspect_after,
            dead_after=period.dead_after,
            remove_after=period.remove_after,
            anti_entropy=period.anti_entropy,
            overlay=Overlay(shuffle_every=period.shuffle_every, graft_after=period.graft_after),
        )
        node = ActorSystem(
            cluster=cluster,
            idle_after=period.idle_after,
            ask_timeout=period.ask_timeout,
            write_timeout=period.write_timeout,
            leave_timeout=period.leave_timeout,
            store=store,
        )
    # A node finds a type by importing it, and this image holds every version. The ones learned here are met before
    # the cluster names them, so this node runs them and not what the import would bring.
    for definition in VERSIONS[launch.version]:
        node._learn(definition)  # pyright: ignore[reportPrivateUsage]
    return node


async def main(launch: Launch, /) -> None:
    clock = Clock()
    leave = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, leave.set)
    opened = nullcontext() if launch.store is None else SQL(launch.store)
    with patch.object(time, "time", clock.time):
        async with opened as store:
            node = system(launch, store=store)
            async with node:
                control = await asyncio.start_server(partial(_answer, node, clock), port=CONTROL)
                say("ready", identity(node.node))
                await leave.wait()
                control.close()
    say("left", None)


async def _answer(
    node: ActorSystem, clock: Clock, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, /
) -> None:
    try:
        while line := await reader.readline():
            command = records.record(json.loads(line))
            answer: dict[str, object]
            if "members" in command:
                table = frozenset((member.node, member.status) for member in node.members)
                answer = View(records.integer(command["members"]), node.node, table).encoded()
            elif "network" in command:
                clock.skew = Network.decoded(command["network"]).skew
                answer = {}
            elif "crash" in command:
                os._exit(137)
            else:
                raise ValueError(f"unknown command {command!r}")
            writer.write((json.dumps(answer) + "\n").encode())
            await writer.drain()
    finally:
        writer.close()


if __name__ == "__main__":
    asyncio.run(main(Launch.decoded(sys.argv[1])))
