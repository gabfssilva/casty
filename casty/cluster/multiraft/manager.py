"""Multi-Raft manager for coordinating multiple Raft groups."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Coroutine

from ..raft_types import SingletonCommand
from ..serialize import serialize
from ..transport import MessageType, WireMessage
from .batching import BatchedMessage, MessageBatcher
from .group import RaftGroup

if TYPE_CHECKING:
    from ..transport import Transport

log = logging.getLogger(__name__)

# System group ID (for singletons, registry sync, cluster membership)
SYSTEM_GROUP_ID = 0


class MultiRaftManager:
    """Manages multiple Raft groups with optimized batching.

    Features:
    - Single event loop processes all groups (tick())
    - Batched heartbeats: 1 message per node pair containing all groups
    - Batched AppendEntries for efficiency
    - Group 0 is the "system group" for singletons and cluster coordination
    """

    def __init__(
        self,
        node_id: str,
        expected_size: int = 3,
        *,
        heartbeat_interval: float = 0.3,
        election_timeout_min: float = 1.0,
        election_timeout_max: float = 2.0,
        persistence_dir: str | None = None,
    ) -> None:
        self._node_id = node_id
        self._expected_size = expected_size
        self._heartbeat_interval = heartbeat_interval
        self._election_timeout_min = election_timeout_min
        self._election_timeout_max = election_timeout_max
        self._persistence_dir = Path(persistence_dir) if persistence_dir else None
        if self._persistence_dir:
            self._persistence_dir.mkdir(parents=True, exist_ok=True)

        self._groups: dict[int, RaftGroup] = {}
        self._batcher = MessageBatcher()
        self._lock = asyncio.Lock()
        self._running = True
        self._persistence_lock = asyncio.Lock()

        # Create system group (group 0)
        self._create_group_internal(SYSTEM_GROUP_ID)

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def system_group(self) -> RaftGroup:
        """Get the system group (group 0)."""
        return self._groups[SYSTEM_GROUP_ID]

    @property
    def groups(self) -> dict[int, RaftGroup]:
        """Get all groups."""
        return self._groups

    def _create_group_internal(self, group_id: int) -> RaftGroup:
        """Internal group creation without lock."""
        group = RaftGroup(
            group_id=group_id,
            node_id=self._node_id,
            expected_size=self._expected_size,
            heartbeat_interval=self._heartbeat_interval,
            election_timeout_min=self._election_timeout_min,
            election_timeout_max=self._election_timeout_max,
        )
        self._groups[group_id] = group
        return group

    async def create_group(self, group_id: int) -> RaftGroup:
        """Create a new Raft group."""
        async with self._lock:
            if group_id in self._groups:
                return self._groups[group_id]
            return self._create_group_internal(group_id)

    def get_group(self, group_id: int) -> RaftGroup | None:
        """Get a Raft group by ID."""
        return self._groups.get(group_id)

    def stop(self) -> None:
        """Stop all groups."""
        self._running = False
        for group in self._groups.values():
            group.stop()

    def remove_peer(self, peer_id: str) -> None:
        """Remove a peer from all groups."""
        for group in self._groups.values():
            group.remove_peer(peer_id)

    # ---- Tick and Election Loop ----

    async def tick(
        self,
        known_nodes: dict[str, "Transport"],
        send_callback: Callable[[str, "Transport", WireMessage], Coroutine[Any, Any, None]],
    ) -> None:
        """Process all groups in a single tick.

        This batches heartbeats across groups for efficiency.
        """
        if not self._running:
            return

        peer_ids = list(known_nodes.keys())

        # Collect actions needed for each group
        elections_to_start: list[RaftGroup] = []
        groups_needing_heartbeat: list[RaftGroup] = []

        for group in self._groups.values():
            if group.should_send_heartbeat():
                groups_needing_heartbeat.append(group)
            elif group.should_start_election():
                elections_to_start.append(group)

        # Start elections
        for group in elections_to_start:
            vote_data = await group.start_election(len(known_nodes))
            if vote_data:
                await self._persist_group_state(group)
                # Send vote requests
                for peer_id, transport in known_nodes.items():
                    try:
                        msg = WireMessage(
                            msg_type=MessageType.VOTE_REQUEST,
                            target_name=self._node_id,
                            payload=serialize(vote_data),
                        )
                        await send_callback(peer_id, transport, msg)
                    except Exception as e:
                        log.debug(f"Failed to request vote from {peer_id[:8]}: {e}")

                # Check if already won (single node cluster)
                won = False
                async with group.lock:
                    won = group._check_election_won()
                if won:
                    await group.on_become_leader(peer_ids)
                    groups_needing_heartbeat.append(group)

        # Batch and send heartbeats
        if groups_needing_heartbeat:
            await self._send_batched_heartbeats(
                groups_needing_heartbeat, known_nodes, send_callback
            )

    async def _send_batched_heartbeats(
        self,
        groups: list[RaftGroup],
        known_nodes: dict[str, "Transport"],
        send_callback: Callable[[str, "Transport", WireMessage], Coroutine[Any, Any, None]],
    ) -> None:
        """Send batched heartbeats for multiple groups."""
        if not groups:
            return

        # Collect heartbeat data per peer
        for peer_id in known_nodes:
            for group in groups:
                # Check if need snapshot instead
                if group.should_send_snapshot(peer_id):
                    snapshot_data = group.get_install_snapshot_data()
                    if snapshot_data:
                        self._batcher.add_heartbeat(peer_id, group.group_id, {
                            "_snapshot": True,
                            **snapshot_data,
                        })
                    continue

                data = group.get_append_entries_for_peer(peer_id)
                if data:
                    self._batcher.add_heartbeat(peer_id, group.group_id, data)

        # Send batched messages
        batched = self._batcher.get_all_heartbeats()
        for peer_id, messages in batched.items():
            if not messages:
                continue

            transport = known_nodes.get(peer_id)
            if not transport:
                continue

            # If only one group, send unbatched for backwards compatibility
            if len(messages) == 1 and len(self._groups) == 1:
                msg_data = messages[0].data
                if msg_data.get("_snapshot"):
                    del msg_data["_snapshot"]
                    msg = WireMessage(
                        msg_type=MessageType.INSTALL_SNAPSHOT_REQ,
                        target_name=self._node_id,
                        payload=serialize(msg_data),
                    )
                else:
                    msg = WireMessage(
                        msg_type=MessageType.APPEND_ENTRIES_REQ,
                        target_name=self._node_id,
                        payload=serialize(msg_data),
                    )
            else:
                # Send as batched message
                batch_data = MessageBatcher.serialize_batch(messages)
                msg = WireMessage(
                    msg_type=MessageType.BATCHED_RAFT_MSG,
                    target_name=self._node_id,
                    payload=serialize({"messages": batch_data}),
                )

            try:
                await send_callback(peer_id, transport, msg)
            except Exception as e:
                log.debug(f"Failed to send heartbeat to {peer_id[:8]}: {e}")

    # ---- Message Handlers ----

    async def handle_vote_request(
        self,
        data: dict[str, Any],
        transport: "Transport",
        send_callback: Callable[["Transport", WireMessage], Coroutine[Any, Any, None]],
    ) -> None:
        """Handle vote request for a group."""
        group_id = data.get("group_id", SYSTEM_GROUP_ID)
        group = self._groups.get(group_id)
        if not group:
            return

        response = await group.handle_vote_request(data)
        await self._persist_group_state(group)

        msg = WireMessage(
            msg_type=MessageType.VOTE_RESPONSE,
            target_name=data["candidate_id"],
            payload=serialize(response),
        )
        await send_callback(transport, msg)

    async def handle_vote_response(
        self,
        data: dict[str, Any],
        known_nodes: dict[str, "Transport"],
    ) -> None:
        """Handle vote response for a group."""
        group_id = data.get("group_id", SYSTEM_GROUP_ID)
        group = self._groups.get(group_id)
        if not group:
            return

        became_leader = await group.handle_vote_response(data)
        if became_leader:
            await group.on_become_leader(list(known_nodes.keys()))

    async def handle_append_entries_request(
        self,
        data: dict[str, Any],
        transport: "Transport",
        send_callback: Callable[["Transport", WireMessage], Coroutine[Any, Any, None]],
    ) -> None:
        """Handle AppendEntries request for a group."""
        group_id = data.get("group_id", SYSTEM_GROUP_ID)
        group = self._groups.get(group_id)
        if not group:
            # Auto-create group if it doesn't exist (for multi-raft scenarios)
            group = await self.create_group(group_id)

        response = await group.handle_append_entries_request(data)
        await self._persist_group_state(group)
        await self._persist_group_log(group)
        await group.apply_committed_entries()

        msg = WireMessage(
            msg_type=MessageType.APPEND_ENTRIES_RES,
            target_name=response["follower_id"],
            payload=serialize(response),
        )
        await send_callback(transport, msg)

    async def handle_append_entries_response(
        self,
        data: dict[str, Any],
        peer_id: str,
        transport: "Transport",
        send_callback: Callable[["Transport", WireMessage], Coroutine[Any, Any, None]],
    ) -> None:
        """Handle AppendEntries response for a group."""
        group_id = data.get("group_id", SYSTEM_GROUP_ID)
        group = self._groups.get(group_id)
        if not group:
            return

        needs_retry = await group.handle_append_entries_response(data)
        await self._persist_group_state(group)
        await group.apply_committed_entries()

        # Retry if needed
        if needs_retry:
            retry_data = group.get_append_entries_for_peer(peer_id)
            if retry_data:
                msg = WireMessage(
                    msg_type=MessageType.APPEND_ENTRIES_REQ,
                    target_name=self._node_id,
                    payload=serialize(retry_data),
                )
                try:
                    await send_callback(transport, msg)
                except Exception as e:
                    log.debug(f"Failed to retry AppendEntries to {peer_id[:8]}: {e}")

    async def handle_install_snapshot_request(
        self,
        data: dict[str, Any],
        transport: "Transport",
        send_callback: Callable[["Transport", WireMessage], Coroutine[Any, Any, None]],
    ) -> None:
        """Handle InstallSnapshot request for a group."""
        group_id = data.get("group_id", SYSTEM_GROUP_ID)
        group = self._groups.get(group_id)
        if not group:
            group = await self.create_group(group_id)

        response = await group.handle_install_snapshot_request(data)
        await self._persist_group_snapshot(group)
        await self._persist_group_state(group)
        await self._persist_group_log(group)

        msg = WireMessage(
            msg_type=MessageType.INSTALL_SNAPSHOT_RES,
            target_name=response["follower_id"],
            payload=serialize(response),
        )
        await send_callback(transport, msg)

    async def handle_install_snapshot_response(
        self,
        data: dict[str, Any],
    ) -> None:
        """Handle InstallSnapshot response for a group."""
        group_id = data.get("group_id", SYSTEM_GROUP_ID)
        group = self._groups.get(group_id)
        if not group:
            return

        await group.handle_install_snapshot_response(data)

    async def handle_batched_message(
        self,
        data: dict[str, Any],
        transport: "Transport",
        send_callback: Callable[["Transport", WireMessage], Coroutine[Any, Any, None]],
        peer_id: str,
        known_nodes: dict[str, "Transport"],
    ) -> None:
        """Handle a batched Raft message containing multiple group messages."""
        messages = MessageBatcher.deserialize_batch(data.get("messages", []))

        responses: list[dict[str, Any]] = []

        for msg in messages:
            if msg.msg_type == "append_entries_req":
                if msg.data.get("_snapshot"):
                    # Handle as snapshot request
                    del msg.data["_snapshot"]
                    group_id = msg.data.get("group_id", SYSTEM_GROUP_ID)
                    group = self._groups.get(group_id)
                    if not group:
                        group = await self.create_group(group_id)
                    response = await group.handle_install_snapshot_request(msg.data)
                    await self._persist_group_snapshot(group)
                    await self._persist_group_state(group)
                    await self._persist_group_log(group)
                    responses.append({
                        "group_id": group_id,
                        "msg_type": "install_snapshot_res",
                        "data": response,
                    })
                else:
                    group_id = msg.data.get("group_id", SYSTEM_GROUP_ID)
                    group = self._groups.get(group_id)
                    if not group:
                        group = await self.create_group(group_id)
                    response = await group.handle_append_entries_request(msg.data)
                    await self._persist_group_state(group)
                    await self._persist_group_log(group)
                    await group.apply_committed_entries()
                    responses.append({
                        "group_id": group_id,
                        "msg_type": "append_entries_res",
                        "data": response,
                    })

            elif msg.msg_type == "vote_request":
                group_id = msg.data.get("group_id", SYSTEM_GROUP_ID)
                group = self._groups.get(group_id)
                if group:
                    response = await group.handle_vote_request(msg.data)
                    await self._persist_group_state(group)
                    responses.append({
                        "group_id": group_id,
                        "msg_type": "vote_response",
                        "data": response,
                    })

            elif msg.msg_type == "vote_response":
                await self.handle_vote_response(msg.data, known_nodes)

            elif msg.msg_type == "append_entries_res":
                await self.handle_append_entries_response(
                    msg.data, peer_id, transport, send_callback
                )

            elif msg.msg_type == "install_snapshot_res":
                await self.handle_install_snapshot_response(msg.data)

        # Send batched responses
        if responses:
            batch_data = responses
            response_msg = WireMessage(
                msg_type=MessageType.BATCHED_RAFT_MSG,
                target_name=self._node_id,
                payload=serialize({"messages": batch_data}),
            )
            try:
                await send_callback(transport, response_msg)
            except Exception as e:
                log.debug(f"Failed to send batched response: {e}")

    # ---- Command Appending ----

    async def append_command(
        self,
        command: Any,
        group_id: int = SYSTEM_GROUP_ID,
        known_nodes: dict[str, "Transport"] | None = None,
        send_callback: Callable[[str, "Transport", WireMessage], Coroutine[Any, Any, None]] | None = None,
        timeout: float = 5.0,
    ) -> bool:
        """Append a command to a group's log.

        Returns True if successfully replicated to majority.
        """
        group = self._groups.get(group_id)
        if not group:
            return False

        index = await group.append_command(command)
        if index is None:
            return False

        await self._persist_group_log(group)

        # Trigger immediate replication if we have peers
        if known_nodes and send_callback:
            for peer_id, transport in known_nodes.items():
                data = group.get_append_entries_for_peer(peer_id)
                if data:
                    try:
                        msg = WireMessage(
                            msg_type=MessageType.APPEND_ENTRIES_REQ,
                            target_name=self._node_id,
                            payload=serialize(data),
                        )
                        await send_callback(peer_id, transport, msg)
                    except Exception as e:
                        log.debug(f"Failed to replicate to {peer_id[:8]}: {e}")

        return await group.wait_for_commit(index, timeout)

    # ---- Callbacks ----

    def set_apply_callback(
        self,
        callback: Callable[[Any], Coroutine[Any, Any, None]],
        group_id: int = SYSTEM_GROUP_ID,
    ) -> None:
        """Set apply callback for a group."""
        group = self._groups.get(group_id)
        if group:
            group.set_apply_callback(callback)

    def set_singleton_apply_callback(
        self,
        callback: Callable[[SingletonCommand], Coroutine[Any, Any, None]],
        group_id: int = SYSTEM_GROUP_ID,
    ) -> None:
        """Set singleton apply callback for a group."""
        group = self._groups.get(group_id)
        if group:
            group.set_singleton_apply_callback(callback)

    def set_snapshot_callbacks(
        self,
        snapshot_cb: Callable[[], Coroutine[Any, Any, bytes]],
        restore_cb: Callable[[bytes], Coroutine[Any, Any, None]],
        group_id: int = SYSTEM_GROUP_ID,
    ) -> None:
        """Set snapshot callbacks for a group."""
        group = self._groups.get(group_id)
        if group:
            group.set_snapshot_callbacks(snapshot_cb, restore_cb)

    # ---- Persistence ----

    def _get_persistence_path(self, kind: str, group_id: int) -> Path | None:
        """Get path to a persistence file."""
        if not self._persistence_dir:
            return None
        return self._persistence_dir / f"raft_{kind}_g{group_id}_{self._node_id[:8]}.json"

    async def _persist_group_state(self, group: RaftGroup) -> None:
        """Persist group state."""
        path = self._get_persistence_path("state", group.group_id)
        if not path:
            return

        data = group.get_persisted_state()
        async with self._persistence_lock:
            await asyncio.to_thread(self._write_json_sync, path, data)

    async def _persist_group_log(self, group: RaftGroup) -> None:
        """Persist group log."""
        path = self._get_persistence_path("log", group.group_id)
        if not path:
            return

        data = group.get_persisted_log()
        async with self._persistence_lock:
            await asyncio.to_thread(self._write_json_sync, path, data)

    async def _persist_group_snapshot(self, group: RaftGroup) -> None:
        """Persist group snapshot."""
        data = group.get_persisted_snapshot()
        if not data:
            return

        path = self._get_persistence_path("snapshot", group.group_id)
        if not path:
            return

        async with self._persistence_lock:
            await asyncio.to_thread(self._write_json_sync, path, data)

    def _write_json_sync(self, path: Path, data: dict[str, Any]) -> None:
        """Synchronously write JSON with fsync for durability."""
        temp_path = path.with_suffix(".tmp")
        with open(temp_path, "w") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        temp_path.rename(path)

    async def load_persisted_state(self) -> None:
        """Load all persisted state."""
        if not self._persistence_dir:
            return

        # Find all state files
        for path in self._persistence_dir.glob("raft_state_g*_*.json"):
            try:
                data = await asyncio.to_thread(self._read_json_sync, path)
                group_id = data.get("group_id", SYSTEM_GROUP_ID)
                group = self._groups.get(group_id)
                if not group:
                    group = await self.create_group(group_id)
                group.restore_state(data)
                log.info(f"Loaded persisted state for group {group_id}")
            except Exception as e:
                log.warning(f"Failed to load state from {path}: {e}")

    async def load_persisted_log(self) -> None:
        """Load all persisted logs."""
        if not self._persistence_dir:
            return

        for path in self._persistence_dir.glob("raft_log_g*_*.json"):
            try:
                data = await asyncio.to_thread(self._read_json_sync, path)
                group_id = data.get("group_id", SYSTEM_GROUP_ID)
                group = self._groups.get(group_id)
                if not group:
                    group = await self.create_group(group_id)
                group.restore_log(data)
                log.info(f"Loaded persisted log for group {group_id}")
            except Exception as e:
                log.warning(f"Failed to load log from {path}: {e}")

    async def load_persisted_snapshots(self) -> None:
        """Load all persisted snapshots."""
        if not self._persistence_dir:
            return

        for path in self._persistence_dir.glob("raft_snapshot_g*_*.json"):
            try:
                data = await asyncio.to_thread(self._read_json_sync, path)
                group_id = data.get("group_id", SYSTEM_GROUP_ID)
                group = self._groups.get(group_id)
                if not group:
                    group = await self.create_group(group_id)
                group.restore_snapshot(data)
                log.info(f"Loaded persisted snapshot for group {group_id}")
            except Exception as e:
                log.warning(f"Failed to load snapshot from {path}: {e}")

    def _read_json_sync(self, path: Path) -> dict[str, Any]:
        """Synchronously read JSON file."""
        with open(path) as f:
            return json.load(f)

    # ---- Legacy Compatibility ----

    async def persist_state(self) -> None:
        """Persist state for all groups (legacy compatibility)."""
        for group in self._groups.values():
            await self._persist_group_state(group)

    async def persist_log(self) -> None:
        """Persist log for all groups (legacy compatibility)."""
        for group in self._groups.values():
            await self._persist_group_log(group)

    # ---- Snapshot Creation ----

    async def create_snapshot(self, group_id: int = SYSTEM_GROUP_ID) -> Any:
        """Create a snapshot for a group."""
        group = self._groups.get(group_id)
        if not group:
            return None

        snapshot = await group.create_snapshot()
        if snapshot:
            await self._persist_group_snapshot(group)
            await self._persist_group_log(group)
        return snapshot
