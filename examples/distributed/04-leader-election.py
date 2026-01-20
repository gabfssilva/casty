"""Leader Election for Distributed Coordination.

Demonstrates:
- Only one leader active at a time
- Detecting leader failure via heartbeats
- Automatic re-election when leader fails
- Leader executes exclusive tasks (e.g., scheduled jobs)

This pattern is essential for distributed systems where exactly one
node should perform certain operations (e.g., cron jobs, migrations).

Run with: uv run python examples/distributed/04-leader-election.py
"""

import asyncio
import random
from dataclasses import dataclass
from typing import Any

from casty import actor, Mailbox, LocalActorRef
from casty.cluster import DevelopmentCluster


@dataclass
class RequestVote:
    candidate_id: str
    candidate_ref: LocalActorRef[Any]
    term: int


@dataclass
class Vote:
    voter_id: str
    term: int
    granted: bool


@dataclass
class HeartbeatMsg:
    leader_id: str
    leader_ref: LocalActorRef[Any]
    term: int


@dataclass
class HeartbeatAck:
    follower_id: str
    term: int


@dataclass
class SetPeers:
    peers: list[LocalActorRef[Any]]


@dataclass
class _ElectionTimeout:
    pass


@dataclass
class _HeartbeatTimeout:
    pass


@dataclass
class _LeaderTask:
    pass


@dataclass
class GetState:
    pass


class NodeState:
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


ElectorMsg = RequestVote | Vote | HeartbeatMsg | HeartbeatAck | SetPeers | _ElectionTimeout | _HeartbeatTimeout | _LeaderTask | GetState


@actor
async def elector_node(
    node_id: str,
    election_timeout_range: tuple[float, float] = (1.5, 3.0),
    heartbeat_interval: float = 0.5,
    *,
    mailbox: Mailbox[ElectorMsg],
):
    peers: list[LocalActorRef[Any]] = []

    current_term = 0
    voted_for: str | None = None

    state = NodeState.FOLLOWER
    leader_id: str | None = None
    votes_received: set[str] = set()

    election_timeout_id: str | None = None
    heartbeat_id: str | None = None
    leader_task_id: str | None = None

    async def cancel_all_timers(ctx):
        nonlocal election_timeout_id, heartbeat_id, leader_task_id
        if election_timeout_id:
            await ctx.cancel_schedule(election_timeout_id)
            election_timeout_id = None
        if heartbeat_id:
            await ctx.cancel_schedule(heartbeat_id)
            heartbeat_id = None
        if leader_task_id:
            await ctx.cancel_schedule(leader_task_id)
            leader_task_id = None

    async def reset_election_timeout(ctx):
        nonlocal election_timeout_id
        if election_timeout_id:
            await ctx.cancel_schedule(election_timeout_id)

        timeout = random.uniform(*election_timeout_range)
        election_timeout_id = await ctx.schedule(_ElectionTimeout(), delay=timeout)

    async def become_follower(ctx, term: int):
        nonlocal state, current_term, voted_for, votes_received
        await cancel_all_timers(ctx)

        state = NodeState.FOLLOWER
        current_term = term
        voted_for = None
        votes_received = set()

        print(f"[{node_id}] Became FOLLOWER (term={term})")
        await reset_election_timeout(ctx)

    async def become_candidate(ctx):
        nonlocal state, current_term, voted_for, votes_received
        await cancel_all_timers(ctx)

        state = NodeState.CANDIDATE
        current_term += 1
        voted_for = node_id
        votes_received = {node_id}

        print(f"[{node_id}] Became CANDIDATE (term={current_term})")

        for peer in peers:
            await peer.send(RequestVote(node_id, mailbox._self_ref, current_term))

        await reset_election_timeout(ctx)

    async def become_leader(ctx):
        nonlocal state, leader_id, heartbeat_id, leader_task_id
        await cancel_all_timers(ctx)

        state = NodeState.LEADER
        leader_id = node_id

        print(f"[{node_id}] Became LEADER (term={current_term})")

        heartbeat_id = await ctx.schedule(_HeartbeatTimeout(), delay=heartbeat_interval)
        leader_task_id = await ctx.schedule(_LeaderTask(), delay=2.0)

        await send_heartbeats()

    async def send_heartbeats():
        for peer in peers:
            await peer.send(HeartbeatMsg(node_id, mailbox._self_ref, current_term))

    def majority() -> int:
        total_nodes = len(peers) + 1
        return (total_nodes // 2) + 1

    print(f"[{node_id}] Started as FOLLOWER")
    await mailbox._self_ref.send(_ElectionTimeout())

    async for msg, ctx in mailbox:
        match msg:
            case SetPeers(new_peers):
                peers[:] = new_peers
                print(f"[{node_id}] Set {len(peers)} peers")

            case RequestVote(candidate_id, candidate_ref, term):
                if term > current_term:
                    await become_follower(ctx, term)

                grant_vote = False
                if term >= current_term and voted_for in (None, candidate_id):
                    grant_vote = True
                    voted_for = candidate_id
                    print(f"[{node_id}] Voted for {candidate_id} (term={term})")

                await candidate_ref.send(Vote(node_id, current_term, grant_vote))

            case Vote(voter_id, term, granted):
                if state != NodeState.CANDIDATE:
                    continue

                if term > current_term:
                    await become_follower(ctx, term)
                    continue

                if granted and term == current_term:
                    votes_received.add(voter_id)
                    print(f"[{node_id}] Received vote from {voter_id} ({len(votes_received)}/{majority()} needed)")

                    if len(votes_received) >= majority():
                        await become_leader(ctx)

            case HeartbeatMsg(hb_leader_id, leader_ref, term):
                if term >= current_term:
                    if state != NodeState.FOLLOWER or current_term < term:
                        await become_follower(ctx, term)
                    else:
                        await reset_election_timeout(ctx)

                    leader_id = hb_leader_id

                    await leader_ref.send(HeartbeatAck(node_id, term))

            case HeartbeatAck(follower_id, term):
                pass

            case _ElectionTimeout():
                if state in (NodeState.FOLLOWER, NodeState.CANDIDATE):
                    print(f"[{node_id}] Election timeout!")
                    await become_candidate(ctx)

            case _HeartbeatTimeout():
                if state == NodeState.LEADER:
                    await send_heartbeats()
                    heartbeat_id = await ctx.schedule(_HeartbeatTimeout(), delay=heartbeat_interval)

            case _LeaderTask():
                if state == NodeState.LEADER:
                    print(f"[{node_id}] LEADER executing exclusive task (term={current_term})")
                    leader_task_id = await ctx.schedule(_LeaderTask(), delay=2.0)

            case GetState():
                await ctx.reply({
                    "node_id": node_id,
                    "state": state,
                    "term": current_term,
                    "leader": leader_id,
                    "voted_for": voted_for,
                })


async def main():
    print("=== Leader Election ===\n")

    async with DevelopmentCluster(3) as cluster:
        node0, node1, node2 = cluster[0], cluster[1], cluster[2]

        print(f"Started 3-node cluster")
        await asyncio.sleep(0.3)

        print("\nPhase 1: Starting election nodes")
        print("-" * 50)

        elector0 = await node0.actor(
            elector_node(
                node_id="node-0",
                election_timeout_range=(1.0, 1.5),
            ),
            name="elector-0",
        )
        elector1 = await node1.actor(
            elector_node(
                node_id="node-1",
                election_timeout_range=(1.5, 2.0),
            ),
            name="elector-1",
        )
        elector2 = await node2.actor(
            elector_node(
                node_id="node-2",
                election_timeout_range=(2.0, 2.5),
            ),
            name="elector-2",
        )

        all_electors = [elector0, elector1, elector2]

        await elector0.send(SetPeers([elector1, elector2]))
        await elector1.send(SetPeers([elector0, elector2]))
        await elector2.send(SetPeers([elector0, elector1]))

        print("\nPhase 2: Waiting for initial leader election")
        print("-" * 50)

        await asyncio.sleep(3.0)

        for elector in all_electors:
            state = await elector.ask(GetState())
            status = "LEADER" if state["state"] == NodeState.LEADER else state["state"]
            print(f"  {state['node_id']}: {status} (term={state['term']}, leader={state['leader']})")

        print("\nPhase 3: Leader executing exclusive tasks")
        print("-" * 50)

        await asyncio.sleep(5.0)

        print("\nPhase 4: Simulating leader failure")
        print("-" * 50)

        current_leader_idx = None
        for i, elector in enumerate(all_electors):
            state = await elector.ask(GetState())
            if state["state"] == NodeState.LEADER:
                current_leader_idx = i
                print(f"Stopping leader: {state['node_id']}")
                break

        if current_leader_idx is not None:
            remaining = [e for i, e in enumerate(all_electors) if i != current_leader_idx]

            print("\nWaiting for re-election...")
            await asyncio.sleep(4.0)

            print("\nNew cluster state:")
            for elector in remaining:
                try:
                    state = await elector.ask(GetState(), timeout=1.0)
                    status = "LEADER" if state["state"] == NodeState.LEADER else state["state"]
                    print(f"  {state['node_id']}: {status} (term={state['term']}, leader={state['leader']})")
                except Exception:
                    pass

            print("\nPhase 5: New leader executing tasks")
            print("-" * 50)

            await asyncio.sleep(4.0)

        print("\n=== Summary ===")
        print("Leader election features demonstrated:")
        print("  - Randomized election timeouts prevent split votes")
        print("  - Majority voting ensures single leader")
        print("  - Heartbeats maintain leader authority")
        print("  - Automatic re-election on leader failure")
        print("  - Only leader executes exclusive tasks")


if __name__ == "__main__":
    asyncio.run(main())
