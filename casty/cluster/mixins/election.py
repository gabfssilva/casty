"""Raft leader election mixin with full election restriction."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import TYPE_CHECKING

from ..serialize import deserialize, serialize
from ..transport import MessageType, Transport, WireMessage

if TYPE_CHECKING:
    from ..distributed import ClusterState

log = logging.getLogger(__name__)


class ElectionMixin:
    """Raft leader election with log-based election restriction."""

    _cluster: ClusterState
    _running: bool

    def _has_quorum(self) -> bool:
        """Check if we can see enough nodes for quorum."""
        visible = len(self._cluster.known_nodes) + 1  # +1 for self
        return visible >= self._cluster.quorum_size

    def _reset_election_timeout(self) -> None:
        """Reset election timeout with random jitter."""
        self._cluster.last_heartbeat = time.time()
        self._cluster.election_timeout = random.uniform(
            self._cluster.election_timeout_min,
            self._cluster.election_timeout_max,
        )

    async def _election_loop(self) -> None:
        """Main election coordinator running in background."""
        while self._running:
            await asyncio.sleep(0.05)  # Fast loop for responsive heartbeats

            # Read state atomically to decide action
            async with self._cluster.election_lock:
                current_state = self._cluster.state
                should_send_heartbeat = False
                should_start_election = False

                if current_state == "leader":
                    now = time.time()
                    if now - self._cluster.last_heartbeat_sent >= self._cluster.heartbeat_interval:
                        should_send_heartbeat = True
                        self._cluster.last_heartbeat_sent = now
                elif current_state in ("follower", "candidate"):
                    if self._election_timed_out():
                        should_start_election = True

            # Perform actions outside lock to avoid blocking
            if should_send_heartbeat:
                await self._send_heartbeats()
            elif should_start_election:
                await self._start_election()

    def _election_timed_out(self) -> bool:
        """Check if election timeout has elapsed."""
        return time.time() - self._cluster.last_heartbeat > self._cluster.election_timeout

    def _cluster_fully_discovered(self) -> bool:
        """Check if we've discovered all expected cluster members."""
        discovered = len(self._cluster.known_nodes) + 1  # +1 for self
        return discovered >= self._cluster.expected_size

    async def _start_election(self) -> None:
        """Become candidate and request votes."""
        async with self._cluster.election_lock:
            # For first election (term 0), require full cluster discovery
            if self._cluster.term == 0 and not self._cluster_fully_discovered():
                log.debug(
                    f"Node {self._cluster.node_id[:8]}: Waiting for cluster discovery "
                    f"({len(self._cluster.known_nodes) + 1}/{self._cluster.expected_size})"
                )
                self._reset_election_timeout()
                return

            if not self._has_quorum():
                log.debug(f"Node {self._cluster.node_id[:8]}: No quorum, can't start election")
                self._reset_election_timeout()
                return

            self._cluster.term += 1
            self._cluster.state = "candidate"
            self._cluster.voted_for = self._cluster.node_id
            self._cluster.votes_received = {self._cluster.node_id}  # Vote for self
            self._reset_election_timeout()

            current_term = self._cluster.term
            node_id = self._cluster.node_id
            # Get log info for election restriction
            last_log_index = self._cluster.raft_log.last_log_index
            last_log_term = self._cluster.raft_log.last_log_term

        # Persist state before requesting votes
        await self._persist_state()

        log.info(f"Node {node_id[:8]}: Starting election for term {current_term}")

        # Request votes with log info for election restriction
        vote_data = serialize({
            "term": current_term,
            "candidate_id": node_id,
            "last_log_index": last_log_index,
            "last_log_term": last_log_term,
        })
        for peer_node_id, transport in list(self._cluster.known_nodes.items()):
            try:
                req = WireMessage(
                    msg_type=MessageType.VOTE_REQUEST,
                    target_name=node_id,
                    payload=vote_data,
                )
                await transport.send(req)
            except Exception as e:
                log.debug(f"Failed to request vote from {peer_node_id[:8]}: {e}")

        # Check if we already have majority (single node cluster)
        async with self._cluster.election_lock:
            became_leader = self._check_election_won()

        if became_leader:
            await self._on_become_leader()
            await self._send_heartbeats()

    def _is_candidate_log_up_to_date(
        self, candidate_last_index: int, candidate_last_term: int
    ) -> bool:
        """Check if candidate's log is at least as up-to-date as ours.

        Raft election restriction: compare by (term, index) tuple.
        """
        my_last = (self._cluster.raft_log.last_log_term, self._cluster.raft_log.last_log_index)
        candidate_last = (candidate_last_term, candidate_last_index)
        return candidate_last >= my_last

    async def _handle_vote_request(self, transport: Transport, msg: WireMessage) -> None:
        """Handle incoming vote request with election restriction."""
        data = deserialize(msg.payload, {})
        candidate_term = data["term"]
        candidate_id = data["candidate_id"]
        candidate_last_log_index = data.get("last_log_index", 0)
        candidate_last_log_term = data.get("last_log_term", 0)

        async with self._cluster.election_lock:
            vote_granted = False

            # If candidate's term is higher, update our term and become follower
            if candidate_term > self._cluster.term:
                self._cluster.term = candidate_term
                self._cluster.state = "follower"
                self._cluster.voted_for = None
                self._cluster.leader_id = None

            # Grant vote if:
            # 1. Candidate's term is at least as high as ours
            # 2. We haven't voted yet in this term (or already voted for this candidate)
            # 3. Candidate's log is at least as up-to-date as ours (election restriction)
            log_ok = self._is_candidate_log_up_to_date(
                candidate_last_log_index, candidate_last_log_term
            )

            if (candidate_term >= self._cluster.term and
                self._cluster.voted_for in (None, candidate_id) and
                log_ok):
                vote_granted = True
                self._cluster.voted_for = candidate_id
                self._reset_election_timeout()
                log.debug(
                    f"Node {self._cluster.node_id[:8]}: Voted for {candidate_id[:8]} "
                    f"in term {candidate_term}"
                )
            elif not log_ok:
                log.debug(
                    f"Node {self._cluster.node_id[:8]}: Rejected vote for {candidate_id[:8]} "
                    f"(log not up-to-date)"
                )

            response_data = serialize({
                "term": self._cluster.term,
                "vote_granted": vote_granted,
                "voter_id": self._cluster.node_id,
            })

        # Persist state before responding
        await self._persist_state()

        response = WireMessage(
            msg_type=MessageType.VOTE_RESPONSE,
            target_name=candidate_id,
            payload=response_data,
        )
        await transport.send(response)

    async def _handle_vote_response(self, msg: WireMessage) -> None:
        """Handle incoming vote response."""
        data = deserialize(msg.payload, {})
        term = data["term"]
        vote_granted = data["vote_granted"]
        voter_id = data["voter_id"]

        became_leader = False
        async with self._cluster.election_lock:
            if self._cluster.state != "candidate":
                return

            # If response has higher term, step down
            if term > self._cluster.term:
                self._cluster.term = term
                self._cluster.state = "follower"
                self._cluster.voted_for = None
                self._cluster.leader_id = None
                return

            if vote_granted and term == self._cluster.term:
                self._cluster.votes_received.add(voter_id)
                log.debug(
                    f"Node {self._cluster.node_id[:8]}: Got vote from {voter_id[:8]}, "
                    f"total: {len(self._cluster.votes_received)}"
                )
                became_leader = self._check_election_won()

        if became_leader:
            await self._on_become_leader()
            await self._send_heartbeats()

    def _check_election_won(self) -> bool:
        """Check if we have won the election. Returns True if just became leader."""
        if self._cluster.state != "candidate":
            return False

        if len(self._cluster.votes_received) >= self._cluster.quorum_size:
            self._cluster.state = "leader"
            self._cluster.leader_id = self._cluster.node_id
            log.info(
                f"Node {self._cluster.node_id[:8]}: Became LEADER with "
                f"{len(self._cluster.votes_received)}/{self._cluster.expected_size} votes (term {self._cluster.term})"
            )
            return True
        return False

    async def _on_become_leader(self) -> None:
        """Initialize leader state when becoming leader."""
        # Initialize next_index and match_index for all peers
        last_log_index = self._cluster.raft_log.last_log_index
        for peer_id in self._cluster.known_nodes:
            self._cluster.next_index[peer_id] = last_log_index + 1
            self._cluster.match_index[peer_id] = 0

    async def _send_heartbeats(self) -> None:
        """Leader sends heartbeats (empty AppendEntries) to all followers."""
        async with self._cluster.election_lock:
            if self._cluster.state != "leader":
                return

            term = self._cluster.term
            node_id = self._cluster.node_id
            commit_index = self._cluster.raft_log.commit_index
            peers_snapshot = list(self._cluster.known_nodes.items())

        node_id_short = node_id[:8]

        # Send AppendEntries as heartbeats
        for peer_node_id, transport in peers_snapshot:
            # Stop if shutting down
            if not self._running:
                return
            async with self._cluster.election_lock:
                next_idx = self._cluster.next_index.get(peer_node_id, 1)
                prev_log_index = next_idx - 1
                prev_log_term = self._cluster.raft_log.get_term_at(prev_log_index)
                entries = self._cluster.raft_log.get_entries_from(next_idx)

            # Check if peer needs snapshot instead
            if hasattr(self, '_should_send_snapshot') and self._should_send_snapshot(peer_node_id):
                if hasattr(self, '_send_install_snapshot'):
                    await self._send_install_snapshot(peer_node_id, transport)
                continue

            heartbeat_data = serialize({
                "term": term,
                "leader_id": node_id,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": [
                    {"term": e.term, "index": e.index, "command": e.command}
                    for e in entries
                ],
                "leader_commit": commit_index,
            })

            try:
                msg = WireMessage(
                    msg_type=MessageType.APPEND_ENTRIES_REQ,
                    target_name=node_id,
                    payload=heartbeat_data,
                )
                await transport.send(msg)
                log.debug(f"Node {node_id_short}: Sent heartbeat to {peer_node_id[:8]}")
            except Exception as e:
                log.debug(f"Failed to send heartbeat to {peer_node_id[:8]}: {e}")

    async def _handle_heartbeat(self, transport: Transport, msg: WireMessage) -> None:  # noqa: ARG002
        """Handle legacy heartbeat from leader (for backward compatibility)."""
        data = deserialize(msg.payload, {})
        term = data["term"]
        leader_id = data["leader_id"]

        async with self._cluster.election_lock:
            log.debug(f"Node {self._cluster.node_id[:8]}: Got heartbeat from {leader_id[:8]} (term {term})")

            if term >= self._cluster.term:
                if self._cluster.state != "follower" or self._cluster.term < term:
                    log.info(f"Node {self._cluster.node_id[:8]}: Accepting {leader_id[:8]} as leader (term {term})")
                if term > self._cluster.term:
                    self._cluster.voted_for = None
                self._cluster.term = term
                self._cluster.state = "follower"
                self._cluster.leader_id = leader_id
                self._reset_election_timeout()

