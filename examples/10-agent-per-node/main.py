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

from local_cluster import nodes

from casty import ActorSystem, Context, NodeId, Ref, Unavailable, actor

PORTS = (7441, 7442, 7443, 7444)


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


async def report(title: str, caller: ActorSystem, /) -> None:
    """Ask the agent of every member, each named by its entry in the member table of `caller`."""
    print(title)
    for member in sorted(caller.members, key=lambda member: member.node.address or ""):
        status = await caller.ref(agent, "agent", at=member).ask(Report)
        print(f"  the agent of {member.node.address} runs on {status.node.address}: {status.jobs} job(s)")


async def main() -> None:
    _, second, _, fourth = PORTS
    async with nodes(PORTS[:3], leave_timeout=timedelta(seconds=2)) as cluster:
        # Each node reaches its own agent through its own identity.
        for system in cluster.systems:
            print(await system.ref(agent, "agent", at=system.node).ask(Run, "warm-up"))
        first = cluster.systems[0]
        await report("three nodes", first)

        # Keys placed by the ring move to a node that joins. The agents stay where they are, with what they did.
        await cluster.start(fourth)
        await report("a fourth node joined", first)

        # By the address alone, which is all a caller needs to know of a node, even one it has not seen yet.
        ref = first.ref(agent, "agent", at=f"127.0.0.1:{second}")
        before = await ref.ask(Report)

        # The second node leaves in an orderly way: its agent ends with it, and no other node takes it over.
        await cluster.stop(cluster.systems[1])
        try:
            await ref.ask(Report)
        except Unavailable:
            print(f"the agent of 127.0.0.1:{second} is unavailable: nothing runs at that address")

        # A new process on the same address: the same ref reaches it, and the agent there starts from `initial`.
        await cluster.start(second)
        after = await ref.ask(Report)
        restarted = after.node.incarnation != before.node.incarnation
        print(f"the same ref reaches {after.node.address} again, a new process: {restarted}, {after.jobs} job(s)")


asyncio.run(main())
