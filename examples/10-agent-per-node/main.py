"""One agent per node, reached by naming the node. Run with `uv run main.py`.

A type declared with `pinned=True` is not placed by the ring: its ref names the node with `at=`, as the `NodeId`, a
member of the table or the `host:port` the node advertises. The key carries that address, so an agent never moves when
nodes join or leave, has a single copy, and is unavailable while no node is up at its address. A process that comes
back on the address is reached by the same ref, and its agent starts from `initial`.
"""

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import assert_never

from casty import ActorSystem, Cluster, Context, NodeId, Ref, Unavailable, actor

PORTS = (7441, 7442, 7443, 7444)
SEEDS = tuple(f"127.0.0.1:{port}" for port in PORTS[:3])


@dataclass(frozen=True)
class Agent:
    jobs: int = 0


@dataclass(frozen=True)
class Run:
    reply_to: Ref[str]
    job: str


@dataclass(frozen=True)
class Status:
    node: NodeId
    jobs: int


@dataclass(frozen=True)
class Report:
    reply_to: Ref[Status]


type AgentMsg = Run | Report


# What a sidecar, or a worker holding a GPU, is: one per machine, doing what only that machine can.
@actor(pinned=True, initial=Agent())
async def agent(ctx: Context[Agent, AgentMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Run(reply_to, job):
                await ctx.state.set(Agent(ctx.state.value.jobs + 1))
                reply_to.tell(f"{job} ran on {ctx.system.node.address}")
            case Report(reply_to):
                reply_to.tell(Status(ctx.system.node, ctx.state.value.jobs))
            case _:
                assert_never(msg)


def node(port: int, /) -> ActorSystem:
    cluster = Cluster(bind=f"127.0.0.1:{port}", seeds=SEEDS, heartbeat=timedelta(milliseconds=200))
    return ActorSystem(cluster=cluster, leave_timeout=timedelta(seconds=2))


async def host(system: ActorSystem, stop: asyncio.Event, /) -> None:
    async with system:
        await stop.wait()


async def formed(systems: list[ActorSystem], /) -> None:
    """Wait until every node sees every other. A node is only usable once its `async with` is entered."""
    while True:
        try:
            if all(len(system.members) == len(systems) for system in systems):
                return
        except RuntimeError:
            pass
        await asyncio.sleep(0.1)


async def report(title: str, caller: ActorSystem, /) -> None:
    """Ask the agent of every member, each named by its entry in the member table of `caller`."""
    print(title)
    for member in sorted(caller.members, key=lambda member: member.node.address or ""):
        status = await caller.ref(agent, "agent", at=member).ask(Report)
        print(f"  the agent of {member.node.address} runs on {status.node.address}: {status.jobs} job(s)")


async def main() -> None:
    systems = {port: node(port) for port in PORTS}
    stops = {port: asyncio.Event() for port in PORTS}
    first, second, _, fourth = PORTS
    async with asyncio.TaskGroup() as nodes:
        hosts = {port: nodes.create_task(host(systems[port], stops[port])) for port in PORTS[:3]}
        await formed([systems[port] for port in PORTS[:3]])

        # Each node reaches its own agent through its own identity.
        for port in PORTS[:3]:
            system = systems[port]
            print(await system.ref(agent, "agent", at=system.node).ask(Run, "warm-up"))
        await report("three nodes", systems[first])

        # Keys placed by the ring move to a node that joins. The agents stay where they are, with what they did.
        hosts[fourth] = nodes.create_task(host(systems[fourth], stops[fourth]))
        await formed(list(systems.values()))
        await report("a fourth node joined", systems[first])

        # By the address alone, which is all a caller needs to know of a node, even one it has not seen yet.
        ref = systems[first].ref(agent, "agent", at=f"127.0.0.1:{second}")
        before = await ref.ask(Report)

        # The second node leaves in an orderly way: its agent ends with it, and no other node takes it over.
        stops[second].set()
        await hosts[second]
        await formed([systems[port] for port in PORTS if port != second])
        try:
            await ref.ask(Report)
        except Unavailable:
            print(f"the agent of 127.0.0.1:{second} is unavailable: nothing runs at that address")

        # A new process on the same address: the same ref reaches it, and the agent there starts from `initial`.
        systems[second], stops[second] = node(second), asyncio.Event()
        hosts[second] = nodes.create_task(host(systems[second], stops[second]))
        await formed(list(systems.values()))
        after = await ref.ask(Report)
        restarted = after.node.incarnation != before.node.incarnation
        print(f"the same ref reaches {after.node.address} again, a new process: {restarted}, {after.jobs} job(s)")

        for stop in stops.values():
            stop.set()


asyncio.run(main())
