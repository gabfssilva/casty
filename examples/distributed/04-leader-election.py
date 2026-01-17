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
from dataclasses import dataclass, field
from typing import Any

from casty import Actor, Context, LocalRef
from casty.cluster import DevelopmentCluster


# --- Election Messages ---

@dataclass
class RequestVote:
    """Candidate requests a vote in an election."""
    candidate_id: str
    candidate_ref: LocalRef[Any]
    term: int


@dataclass
class Vote:
    """Response to a vote request."""
    voter_id: str
    term: int
    granted: bool


@dataclass
class Heartbeat:
    """Leader heartbeat to maintain authority."""
    leader_id: str
    leader_ref: LocalRef[Any]
    term: int


@dataclass
class HeartbeatAck:
    """Acknowledgment of heartbeat."""
    follower_id: str
    term: int


@dataclass
class SetPeers:
    """Set the peer references for an elector node."""
    peers: list[LocalRef[Any]]


@dataclass
class _ElectionTimeout:
    """Internal: election timeout triggered."""
    pass


@dataclass
class _HeartbeatTimeout:
    """Internal: time to send heartbeat (leader only)."""
    pass


@dataclass
class _LeaderTask:
    """Internal: trigger leader's exclusive task."""
    pass


@dataclass
class GetState:
    """Query current state of the node."""
    pass


# --- Node States ---

class NodeState:
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


# --- Elector Node Actor ---

type ElectorMessage = RequestVote | Vote | Heartbeat | HeartbeatAck | SetPeers | _ElectionTimeout | _HeartbeatTimeout | _LeaderTask | GetState


class ElectorNode(Actor[ElectorMessage]):
    """A node participating in leader election using a simplified Raft-like protocol.

    States:
    - FOLLOWER: Waits for leader heartbeats, starts election if timeout
    - CANDIDATE: Requests votes, becomes leader if majority
    - LEADER: Sends heartbeats, executes exclusive tasks
    """

    def __init__(
        self,
        node_id: str,
        election_timeout_range: tuple[float, float] = (1.5, 3.0),
        heartbeat_interval: float = 0.5,
    ):
        self.node_id = node_id
        self.peers: list[LocalRef[Any]] = []
        self.election_timeout_range = election_timeout_range
        self.heartbeat_interval = heartbeat_interval

        # Persistent state
        self.current_term = 0
        self.voted_for: str | None = None

        # Volatile state
        self.state = NodeState.FOLLOWER
        self.leader_id: str | None = None
        self.votes_received: set[str] = set()

        # Task IDs for cancellation
        self._election_timeout_id: str | None = None
        self._heartbeat_id: str | None = None
        self._leader_task_id: str | None = None

    async def on_start(self):
        print(f"[{self.node_id}] Started as FOLLOWER")
        await self._reset_election_timeout()

    async def on_stop(self):
        await self._cancel_all_timers()

    async def _cancel_all_timers(self):
        """Cancel all active timers."""
        if self._election_timeout_id:
            await self._ctx.cancel_schedule(self._election_timeout_id)
            self._election_timeout_id = None
        if self._heartbeat_id:
            await self._ctx.cancel_tick(self._heartbeat_id)
            self._heartbeat_id = None
        if self._leader_task_id:
            await self._ctx.cancel_tick(self._leader_task_id)
            self._leader_task_id = None

    async def _reset_election_timeout(self):
        """Reset/start election timeout with randomized delay."""
        if self._election_timeout_id:
            await self._ctx.cancel_schedule(self._election_timeout_id)

        timeout = random.uniform(*self.election_timeout_range)
        self._election_timeout_id = await self._ctx.schedule(timeout, _ElectionTimeout())

    async def _become_follower(self, term: int):
        """Transition to follower state."""
        await self._cancel_all_timers()

        self.state = NodeState.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self.votes_received.clear()

        print(f"[{self.node_id}] Became FOLLOWER (term={term})")
        await self._reset_election_timeout()

    async def _become_candidate(self):
        """Transition to candidate state and start election."""
        await self._cancel_all_timers()

        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}

        print(f"[{self.node_id}] Became CANDIDATE (term={self.current_term})")

        # Request votes from all peers (include self ref so peers can respond)
        for peer in self.peers:
            await peer.send(RequestVote(self.node_id, self._ctx.self_ref, self.current_term))

        # Set election timeout for retry
        await self._reset_election_timeout()

    async def _become_leader(self):
        """Transition to leader state."""
        await self._cancel_all_timers()

        self.state = NodeState.LEADER
        self.leader_id = self.node_id

        print(f"[{self.node_id}] Became LEADER (term={self.current_term})")

        # Start sending heartbeats
        self._heartbeat_id = await self._ctx.tick(_HeartbeatTimeout(), interval=self.heartbeat_interval)

        # Start leader-exclusive task
        self._leader_task_id = await self._ctx.tick(_LeaderTask(), interval=2.0)

        # Send immediate heartbeat
        await self._send_heartbeats()

    async def _send_heartbeats(self):
        """Send heartbeat to all peers."""
        for peer in self.peers:
            await peer.send(Heartbeat(self.node_id, self._ctx.self_ref, self.current_term))

    def _majority(self) -> int:
        """Calculate majority threshold."""
        total_nodes = len(self.peers) + 1
        return (total_nodes // 2) + 1

    async def receive(self, msg: ElectorMessage, ctx: Context):
        match msg:
            case SetPeers(peers):
                self.peers = peers
                print(f"[{self.node_id}] Set {len(peers)} peers")

            case RequestVote(candidate_id, candidate_ref, term):
                if term > self.current_term:
                    await self._become_follower(term)

                grant_vote = False
                if term >= self.current_term and self.voted_for in (None, candidate_id):
                    grant_vote = True
                    self.voted_for = candidate_id
                    print(f"[{self.node_id}] Voted for {candidate_id} (term={term})")

                # Send vote directly to candidate
                await candidate_ref.send(Vote(self.node_id, self.current_term, grant_vote))

            case Vote(voter_id, term, granted):
                if self.state != NodeState.CANDIDATE:
                    return

                if term > self.current_term:
                    await self._become_follower(term)
                    return

                if granted and term == self.current_term:
                    self.votes_received.add(voter_id)
                    print(f"[{self.node_id}] Received vote from {voter_id} ({len(self.votes_received)}/{self._majority()} needed)")

                    if len(self.votes_received) >= self._majority():
                        await self._become_leader()

            case Heartbeat(leader_id, leader_ref, term):
                if term >= self.current_term:
                    if self.state != NodeState.FOLLOWER or self.current_term < term:
                        await self._become_follower(term)
                    else:
                        await self._reset_election_timeout()

                    self.leader_id = leader_id

                    # Acknowledge heartbeat
                    await leader_ref.send(HeartbeatAck(self.node_id, term))

            case HeartbeatAck(follower_id, term):
                pass

            case _ElectionTimeout():
                if self.state in (NodeState.FOLLOWER, NodeState.CANDIDATE):
                    print(f"[{self.node_id}] Election timeout!")
                    await self._become_candidate()

            case _HeartbeatTimeout():
                if self.state == NodeState.LEADER:
                    await self._send_heartbeats()

            case _LeaderTask():
                if self.state == NodeState.LEADER:
                    print(f"[{self.node_id}] LEADER executing exclusive task (term={self.current_term})")

            case GetState():
                await ctx.reply({
                    "node_id": self.node_id,
                    "state": self.state,
                    "term": self.current_term,
                    "leader": self.leader_id,
                    "voted_for": self.voted_for,
                })


async def main():
    print("=== Leader Election ===\n")

    async with DevelopmentCluster(3) as (node0, node1, node2):  # type: ignore[misc]
        print(f"Started 3-node cluster")
        await asyncio.sleep(0.3)

        print("\nPhase 1: Starting election nodes")
        print("-" * 50)

        # Create elector nodes with staggered timeouts
        # Lower timeout = more likely to become leader first
        elector0 = await node0.spawn(
            ElectorNode,
            node_id="node-0",
            election_timeout_range=(1.0, 1.5),
        )
        elector1 = await node1.spawn(
            ElectorNode,
            node_id="node-1",
            election_timeout_range=(1.5, 2.0),
        )
        elector2 = await node2.spawn(
            ElectorNode,
            node_id="node-2",
            election_timeout_range=(2.0, 2.5),
        )

        all_electors = [elector0, elector1, elector2]

        # Set peers for each node
        await elector0.send(SetPeers([elector1, elector2]))
        await elector1.send(SetPeers([elector0, elector2]))
        await elector2.send(SetPeers([elector0, elector1]))

        print("\nPhase 2: Waiting for initial leader election")
        print("-" * 50)

        # Wait for election to complete
        await asyncio.sleep(3.0)

        # Check states
        for elector in all_electors:
            state = await elector.ask(GetState())
            status = "LEADER" if state["state"] == NodeState.LEADER else state["state"]
            print(f"  {state['node_id']}: {status} (term={state['term']}, leader={state['leader']})")

        print("\nPhase 3: Leader executing exclusive tasks")
        print("-" * 50)

        # Let leader run some tasks
        await asyncio.sleep(5.0)

        print("\nPhase 4: Simulating leader failure")
        print("-" * 50)

        # Find current leader
        current_leader_idx = None
        for i, elector in enumerate(all_electors):
            state = await elector.ask(GetState())
            if state["state"] == NodeState.LEADER:
                current_leader_idx = i
                print(f"Stopping leader: {state['node_id']}")
                break

        if current_leader_idx is not None:
            # Stop the leader based on which system it's in
            if current_leader_idx == 0:
                await node0.stop(all_electors[0])
            elif current_leader_idx == 1:
                await node1.stop(all_electors[1])
            else:
                await node2.stop(all_electors[2])

            remaining = [e for i, e in enumerate(all_electors) if i != current_leader_idx]

            print("\nWaiting for re-election...")
            await asyncio.sleep(4.0)

            # Check new states
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
