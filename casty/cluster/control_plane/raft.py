"""Raft consensus manager for control plane.

Implements a simplified Raft consensus algorithm for replicating
control plane commands across the cluster.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, Callable, Awaitable

from .commands import ControlPlaneCommand
from .state import ClusterState

if TYPE_CHECKING:
    from ..transport import Transport

logger = logging.getLogger(__name__)


class RaftRole(Enum):
    """Role of a node in Raft consensus."""

    FOLLOWER = auto()
    CANDIDATE = auto()
    LEADER = auto()


@dataclass
class RaftConfig:
    """Configuration for Raft consensus.

    Attributes:
        heartbeat_interval: Time between leader heartbeats (seconds)
        election_timeout_min: Minimum election timeout (seconds)
        election_timeout_max: Maximum election timeout (seconds)
        max_entries_per_append: Max log entries per AppendEntries RPC
    """

    heartbeat_interval: float = 0.3
    election_timeout_min: float = 1.0
    election_timeout_max: float = 2.0
    max_entries_per_append: int = 100


@dataclass
class LogEntry:
    """Entry in the Raft log.

    Attributes:
        term: Term when entry was created
        index: Position in the log (1-indexed)
        command: The command to apply
    """

    term: int
    index: int
    command: ControlPlaneCommand


@dataclass
class PersistentState:
    """Persistent Raft state that must survive restarts.

    Attributes:
        current_term: Latest term server has seen
        voted_for: Candidate that received vote in current term
        log: Log entries
    """

    current_term: int = 0
    voted_for: str | None = None
    log: list[LogEntry] = field(default_factory=list)


@dataclass
class VolatileState:
    """Volatile state on all servers.

    Attributes:
        commit_index: Index of highest log entry known to be committed
        last_applied: Index of highest log entry applied to state machine
    """

    commit_index: int = 0
    last_applied: int = 0


@dataclass
class LeaderState:
    """Volatile state on leaders.

    Attributes:
        next_index: For each server, index of next log entry to send
        match_index: For each server, index of highest log entry replicated
    """

    next_index: dict[str, int] = field(default_factory=dict)
    match_index: dict[str, int] = field(default_factory=dict)


# Raft RPC Messages


@dataclass
class VoteRequest:
    """RequestVote RPC request.

    Attributes:
        term: Candidate's term
        candidate_id: Candidate requesting vote
        last_log_index: Index of candidate's last log entry
        last_log_term: Term of candidate's last log entry
    """

    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class VoteResponse:
    """RequestVote RPC response.

    Attributes:
        term: Current term, for candidate to update itself
        vote_granted: True if candidate received vote
    """

    term: int
    vote_granted: bool


@dataclass
class AppendEntriesRequest:
    """AppendEntries RPC request (also used as heartbeat).

    Attributes:
        term: Leader's term
        leader_id: Leader's node ID
        prev_log_index: Index of log entry immediately preceding new ones
        prev_log_term: Term of prev_log_index entry
        entries: Log entries to store (empty for heartbeat)
        leader_commit: Leader's commit_index
    """

    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry]
    leader_commit: int


@dataclass
class AppendEntriesResponse:
    """AppendEntries RPC response.

    Attributes:
        term: Current term, for leader to update itself
        success: True if follower contained entry matching prev_log_*
        match_index: Index of highest log entry replicated (for optimization)
    """

    term: int
    success: bool
    match_index: int = 0


class RaftManager:
    """Raft consensus manager for the control plane.

    Implements the core Raft algorithm:
    - Leader election with randomized timeouts
    - Log replication
    - Safety guarantees (election restriction)

    Usage:
        raft = RaftManager(node_id="node-1", peers=["node-2", "node-3"])
        raft.set_transport(transport)
        await raft.start()

        # Append a command (only works on leader)
        success = await raft.append_command(JoinCluster(...))

        # Get current state
        state = raft.state

        await raft.stop()
    """

    def __init__(
        self,
        node_id: str,
        peers: list[str] | None = None,
        config: RaftConfig | None = None,
    ):
        self._node_id = node_id
        self._peers = set(peers or [])
        self._config = config or RaftConfig()

        # State
        self._role = RaftRole.FOLLOWER
        self._persistent = PersistentState()
        self._volatile = VolatileState()
        self._leader_state = LeaderState()

        # Cluster state machine
        self._state = ClusterState()

        # Current leader (known)
        self._leader_id: str | None = None

        # Timing
        self._last_heartbeat = time.time()
        self._election_timeout = self._random_election_timeout()

        # Transport
        self._transport: Transport | None = None

        # Tasks
        self._running = False
        self._ticker_task: asyncio.Task | None = None

        # Pending command futures (for async append)
        self._pending_commands: dict[int, asyncio.Future[bool]] = {}

        # Callbacks
        self.on_leadership_change: Callable[[bool], Awaitable[None]] | None = None
        self.on_state_change: Callable[[ClusterState], Awaitable[None]] | None = None

    @property
    def node_id(self) -> str:
        """Get this node's ID."""
        return self._node_id

    @property
    def is_leader(self) -> bool:
        """Check if this node is the leader."""
        return self._role == RaftRole.LEADER

    @property
    def leader_id(self) -> str | None:
        """Get the current leader's ID."""
        return self._leader_id

    @property
    def current_term(self) -> int:
        """Get the current term."""
        return self._persistent.current_term

    @property
    def state(self) -> ClusterState:
        """Get the cluster state machine."""
        return self._state

    @property
    def commit_index(self) -> int:
        """Get the current commit index."""
        return self._volatile.commit_index

    def set_transport(self, transport: Transport) -> None:
        """Set the transport for sending RPCs.

        Args:
            transport: Transport instance
        """
        self._transport = transport

    def add_peer(self, peer_id: str) -> None:
        """Add a peer to the cluster.

        Args:
            peer_id: Peer's node ID
        """
        if peer_id != self._node_id:
            self._peers.add(peer_id)
            if self._role == RaftRole.LEADER:
                # Initialize leader state for new peer
                last_index = len(self._persistent.log)
                self._leader_state.next_index[peer_id] = last_index + 1
                self._leader_state.match_index[peer_id] = 0

    def remove_peer(self, peer_id: str) -> None:
        """Remove a peer from the cluster.

        Args:
            peer_id: Peer's node ID
        """
        self._peers.discard(peer_id)
        self._leader_state.next_index.pop(peer_id, None)
        self._leader_state.match_index.pop(peer_id, None)

    async def start(self) -> None:
        """Start the Raft manager."""
        if self._running:
            return

        self._running = True
        self._last_heartbeat = time.time()
        self._election_timeout = self._random_election_timeout()
        self._ticker_task = asyncio.create_task(self._ticker())
        logger.info(f"Raft started for {self._node_id}")

    async def stop(self) -> None:
        """Stop the Raft manager."""
        self._running = False

        if self._ticker_task:
            self._ticker_task.cancel()
            try:
                await self._ticker_task
            except asyncio.CancelledError:
                pass
            self._ticker_task = None

        # Cancel pending commands
        for fut in self._pending_commands.values():
            fut.cancel()
        self._pending_commands.clear()

        logger.info(f"Raft stopped for {self._node_id}")

    async def append_command(
        self,
        command: ControlPlaneCommand,
        *,
        timeout: float = 5.0,
    ) -> bool:
        """Append a command to the log.

        Only works if this node is the leader.

        Args:
            command: Command to append
            timeout: Timeout for replication

        Returns:
            True if command was committed, False otherwise
        """
        if not self.is_leader:
            return False

        # Create log entry
        index = len(self._persistent.log) + 1
        entry = LogEntry(
            term=self._persistent.current_term,
            index=index,
            command=command,
        )
        self._persistent.log.append(entry)

        # Create future for this command
        fut: asyncio.Future[bool] = asyncio.get_event_loop().create_future()
        self._pending_commands[index] = fut

        try:
            # Wait for commit or timeout
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            return False
        finally:
            self._pending_commands.pop(index, None)

    # RPC Handlers

    async def handle_vote_request(self, request: VoteRequest) -> VoteResponse:
        """Handle a RequestVote RPC.

        Args:
            request: The vote request

        Returns:
            Vote response
        """
        logger.debug(
            f"{self._node_id} received vote request from {request.candidate_id} "
            f"(term={request.term})"
        )

        # Check if request term is newer
        if request.term > self._persistent.current_term:
            self._become_follower(request.term)

        # Decide whether to grant vote
        vote_granted = False

        if request.term >= self._persistent.current_term:
            # Check if we haven't voted or already voted for this candidate
            can_vote = (
                self._persistent.voted_for is None
                or self._persistent.voted_for == request.candidate_id
            )

            # Check log restriction: candidate's log must be at least as up-to-date
            if can_vote and self._is_log_up_to_date(
                request.last_log_index, request.last_log_term
            ):
                self._persistent.voted_for = request.candidate_id
                self._last_heartbeat = time.time()  # Reset election timer
                vote_granted = True
                logger.debug(f"{self._node_id} granted vote to {request.candidate_id}")

        return VoteResponse(
            term=self._persistent.current_term,
            vote_granted=vote_granted,
        )

    async def handle_append_entries(
        self, request: AppendEntriesRequest
    ) -> AppendEntriesResponse:
        """Handle an AppendEntries RPC.

        Args:
            request: The append entries request

        Returns:
            Append entries response
        """
        # Check term
        if request.term > self._persistent.current_term:
            self._become_follower(request.term)

        if request.term < self._persistent.current_term:
            return AppendEntriesResponse(
                term=self._persistent.current_term,
                success=False,
            )

        # Valid leader - reset election timeout
        self._last_heartbeat = time.time()
        self._leader_id = request.leader_id

        if self._role != RaftRole.FOLLOWER:
            self._become_follower(request.term)

        # Check log consistency
        if request.prev_log_index > 0:
            if request.prev_log_index > len(self._persistent.log):
                # Missing entries
                return AppendEntriesResponse(
                    term=self._persistent.current_term,
                    success=False,
                    match_index=len(self._persistent.log),
                )

            prev_entry = self._persistent.log[request.prev_log_index - 1]
            if prev_entry.term != request.prev_log_term:
                # Term mismatch - delete conflicting entries
                self._persistent.log = self._persistent.log[: request.prev_log_index - 1]
                return AppendEntriesResponse(
                    term=self._persistent.current_term,
                    success=False,
                    match_index=len(self._persistent.log),
                )

        # Append new entries
        for entry in request.entries:
            if entry.index <= len(self._persistent.log):
                # Check for conflict
                existing = self._persistent.log[entry.index - 1]
                if existing.term != entry.term:
                    # Delete conflicting entry and all that follow
                    self._persistent.log = self._persistent.log[: entry.index - 1]
                    self._persistent.log.append(entry)
            else:
                self._persistent.log.append(entry)

        # Update commit index
        if request.leader_commit > self._volatile.commit_index:
            new_commit = min(request.leader_commit, len(self._persistent.log))
            if new_commit > self._volatile.commit_index:
                self._volatile.commit_index = new_commit
                await self._apply_committed_entries()

        return AppendEntriesResponse(
            term=self._persistent.current_term,
            success=True,
            match_index=len(self._persistent.log),
        )

    # Internal methods

    async def _ticker(self) -> None:
        """Main ticker loop - handles timeouts and heartbeats."""
        while self._running:
            try:
                await asyncio.sleep(0.05)  # 50ms tick

                if self._role == RaftRole.LEADER:
                    # Send heartbeats periodically
                    elapsed = time.time() - self._last_heartbeat
                    if elapsed >= self._config.heartbeat_interval:
                        await self._send_heartbeats()
                        self._last_heartbeat = time.time()
                else:
                    # Check for election timeout
                    elapsed = time.time() - self._last_heartbeat
                    if elapsed >= self._election_timeout:
                        await self._start_election()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in Raft ticker: {e}")

    async def _start_election(self) -> None:
        """Start a leader election."""
        logger.info(f"{self._node_id} starting election for term {self._persistent.current_term + 1}")

        # Increment term and become candidate
        self._persistent.current_term += 1
        self._role = RaftRole.CANDIDATE
        self._persistent.voted_for = self._node_id
        self._last_heartbeat = time.time()
        self._election_timeout = self._random_election_timeout()

        # Vote for self
        votes_received = 1
        votes_needed = (len(self._peers) + 1) // 2 + 1

        if len(self._peers) == 0:
            # Single node cluster - become leader immediately
            self._become_leader()
            return

        if not self._transport:
            return

        # Request votes from all peers
        last_log_index = len(self._persistent.log)
        last_log_term = (
            self._persistent.log[-1].term if self._persistent.log else 0
        )

        request = VoteRequest(
            term=self._persistent.current_term,
            candidate_id=self._node_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term,
        )

        # Send vote requests in parallel
        tasks = []
        for peer_id in self._peers:
            tasks.append(self._request_vote(peer_id, request))

        responses = await asyncio.gather(*tasks, return_exceptions=True)

        for response in responses:
            if isinstance(response, VoteResponse):
                if response.term > self._persistent.current_term:
                    self._become_follower(response.term)
                    return
                if response.vote_granted:
                    votes_received += 1

        # Check if we won
        if (
            self._role == RaftRole.CANDIDATE
            and votes_received >= votes_needed
        ):
            self._become_leader()

    async def _request_vote(
        self,
        peer_id: str,
        request: VoteRequest,
    ) -> VoteResponse | None:
        """Send vote request to a peer."""
        if not self._transport:
            return None

        try:
            return await self._transport.send_vote_request(peer_id, request)
        except Exception as e:
            logger.debug(f"Failed to request vote from {peer_id}: {e}")
            return None

    def _become_leader(self) -> None:
        """Transition to leader role."""
        logger.info(f"{self._node_id} became leader for term {self._persistent.current_term}")

        self._role = RaftRole.LEADER
        self._leader_id = self._node_id

        # Initialize leader state
        last_index = len(self._persistent.log)
        for peer_id in self._peers:
            self._leader_state.next_index[peer_id] = last_index + 1
            self._leader_state.match_index[peer_id] = 0

        # Notify callback
        if self.on_leadership_change:
            asyncio.create_task(self.on_leadership_change(True))

    def _become_follower(self, term: int) -> None:
        """Transition to follower role."""
        was_leader = self._role == RaftRole.LEADER
        self._role = RaftRole.FOLLOWER
        self._persistent.current_term = term
        self._persistent.voted_for = None
        self._election_timeout = self._random_election_timeout()

        if was_leader:
            logger.info(f"{self._node_id} stepped down from leader (term {term})")
            if self.on_leadership_change:
                asyncio.create_task(self.on_leadership_change(False))

    async def _send_heartbeats(self) -> None:
        """Send heartbeats (AppendEntries) to all peers."""
        if not self._transport or not self.is_leader:
            return

        tasks = []
        for peer_id in self._peers:
            tasks.append(self._send_append_entries(peer_id))

        await asyncio.gather(*tasks, return_exceptions=True)

        # Check if we can advance commit index
        await self._try_advance_commit_index()

    async def _send_append_entries(self, peer_id: str) -> None:
        """Send AppendEntries to a single peer."""
        if not self._transport:
            return

        next_index = self._leader_state.next_index.get(peer_id, 1)

        # Get entries to send
        entries = []
        if next_index <= len(self._persistent.log):
            entries = self._persistent.log[
                next_index - 1 : next_index - 1 + self._config.max_entries_per_append
            ]

        # Get prev log info
        prev_log_index = next_index - 1
        prev_log_term = 0
        if prev_log_index > 0 and prev_log_index <= len(self._persistent.log):
            prev_log_term = self._persistent.log[prev_log_index - 1].term

        request = AppendEntriesRequest(
            term=self._persistent.current_term,
            leader_id=self._node_id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=self._volatile.commit_index,
        )

        try:
            response = await self._transport.send_append_entries(peer_id, request)

            if response.term > self._persistent.current_term:
                self._become_follower(response.term)
                return

            if response.success:
                # Update next_index and match_index
                if entries:
                    new_match = entries[-1].index
                    self._leader_state.match_index[peer_id] = new_match
                    self._leader_state.next_index[peer_id] = new_match + 1
            else:
                # Decrement next_index and retry
                if response.match_index > 0:
                    self._leader_state.next_index[peer_id] = response.match_index + 1
                else:
                    self._leader_state.next_index[peer_id] = max(
                        1, self._leader_state.next_index.get(peer_id, 1) - 1
                    )

        except Exception as e:
            logger.debug(f"Failed to send AppendEntries to {peer_id}: {e}")

    async def _try_advance_commit_index(self) -> None:
        """Try to advance commit index based on replication."""
        if not self.is_leader:
            return

        # Find the highest N such that a majority has match_index >= N
        for n in range(len(self._persistent.log), self._volatile.commit_index, -1):
            if self._persistent.log[n - 1].term != self._persistent.current_term:
                # Can only commit entries from current term
                continue

            # Count replicas
            count = 1  # Leader has it
            for match in self._leader_state.match_index.values():
                if match >= n:
                    count += 1

            majority = (len(self._peers) + 1) // 2 + 1
            if count >= majority:
                self._volatile.commit_index = n
                await self._apply_committed_entries()
                break

    async def _apply_committed_entries(self) -> None:
        """Apply committed entries to the state machine."""
        while self._volatile.last_applied < self._volatile.commit_index:
            self._volatile.last_applied += 1
            entry = self._persistent.log[self._volatile.last_applied - 1]

            # Apply to state machine
            self._state.apply(entry.command)
            logger.debug(f"Applied entry {entry.index}: {type(entry.command).__name__}")

            # Resolve pending command future
            if entry.index in self._pending_commands:
                fut = self._pending_commands[entry.index]
                if not fut.done():
                    fut.set_result(True)

            # Notify callback
            if self.on_state_change:
                asyncio.create_task(self.on_state_change(self._state))

    def _is_log_up_to_date(self, last_index: int, last_term: int) -> bool:
        """Check if candidate's log is at least as up-to-date as ours."""
        our_last_term = (
            self._persistent.log[-1].term if self._persistent.log else 0
        )
        our_last_index = len(self._persistent.log)

        if last_term != our_last_term:
            return last_term > our_last_term
        return last_index >= our_last_index

    def _random_election_timeout(self) -> float:
        """Generate a random election timeout."""
        return random.uniform(
            self._config.election_timeout_min,
            self._config.election_timeout_max,
        )

    def __repr__(self) -> str:
        return (
            f"RaftManager(node={self._node_id}, "
            f"role={self._role.name}, "
            f"term={self._persistent.current_term}, "
            f"commit={self._volatile.commit_index})"
        )
