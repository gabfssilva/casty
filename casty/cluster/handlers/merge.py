"""Three-way merge handler mixin for conflict-free distributed updates."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import msgpack

from casty import Context
from casty.actor import on
from casty.merge import MergeableActor
from casty.transport import Send, ProtocolType
from ..codec import ActorCodec, MergeRequest, MergeState, MergeComplete

if TYPE_CHECKING:
    from ..state import ClusterState

log = logging.getLogger(__name__)


class MergeHandlers:
    """Handles three-way merge protocol for conflict resolution."""

    state: ClusterState

    @on(MergeRequest)
    async def handle_merge_request(
        self,
        msg: MergeRequest,
        ctx: Context,
    ) -> None:
        """Handle MergeRequest - send our state back."""
        request_id = msg.request_id
        entity_type = msg.entity_type
        entity_id = msg.entity_id

        wrapper = self.state.entity_wrappers.get(entity_type, {}).get(entity_id)
        if not wrapper:
            log.warning(f"MergeRequest for unknown entity: {entity_type}:{entity_id}")
            return

        version, base_version, state, base_state = wrapper.get_merge_info()

        wire_msg = MergeState(
            request_id=request_id,
            entity_type=entity_type,
            entity_id=entity_id,
            version=version,
            base_version=base_version,
            state=msgpack.packb(state, use_bin_type=True),
            base_state=msgpack.packb(base_state, use_bin_type=True) if base_state else b"",
        )

        if self.state.mux:
            data = ActorCodec.encode(wire_msg)
            # Get from_node from context (need to add to handler signature or state)
            # For now, we'll need to refactor this
            log.debug(f"Sent MergeState {request_id}")

    @on(MergeState)
    async def handle_merge_state(
        self,
        msg: MergeState,
        ctx: Context,
    ) -> None:
        """Handle MergeState - execute merge locally."""
        request_id = msg.request_id
        entity_type = msg.entity_type
        entity_id = msg.entity_id
        state_bytes = msg.state
        base_state_bytes = msg.base_state

        wrapper = self.state.entity_wrappers.get(entity_type, {}).get(entity_id)
        if not wrapper:
            log.warning(f"MergeState for unknown entity: {entity_type}:{entity_id}")
            return

        their_state = msgpack.unpackb(state_bytes, raw=False)
        their_base_state = msgpack.unpackb(base_state_bytes, raw=False) if base_state_bytes else None

        # Execute three-way merge
        wrapper.execute_merge(their_base_state, their_state, msg.version)

        # Take snapshot after merge
        wrapper.take_snapshot()

        # Publish new version to gossip
        await self._publish_version(entity_type, entity_id, wrapper)

        # Send completion acknowledgment
        wire_msg = MergeComplete(
            request_id=request_id,
            entity_type=entity_type,
            entity_id=entity_id,
            new_version=wrapper.version,
        )

        if self.state.mux:
            data = ActorCodec.encode(wire_msg)
            # Need from_node - requires refactoring
            log.debug(f"Merge complete for {entity_type}:{entity_id}, new version={wrapper.version}")

    @on(MergeComplete)
    async def handle_merge_complete(
        self,
        msg: MergeComplete,
        ctx: Context,
    ) -> None:
        """Handle MergeComplete - peer has merged."""
        # Log completion
        log.debug(f"Merge protocol complete: {msg.request_id}, entity={msg.entity_type}:{msg.entity_id}")

    async def _handle_merge_version_change(
        self,
        key: str,
        value: bytes | None,
        ctx: Context,
    ) -> None:
        """Handle version update from gossip for three-way merge detection."""
        if value is None:
            return  # Deletion - ignore

        # Parse key: "__merge/entity_type:entity_id"
        entity_key = key[9:]  # Strip "__merge/"
        parts = entity_key.split(":", 1)
        if len(parts) != 2:
            return

        entity_type, entity_id = parts

        # Parse value
        value_dict = msgpack.unpackb(value, raw=False)
        remote_version = value_dict.get("version", 0)
        remote_node_id = value_dict.get("node_id", "")

        # Skip our own updates
        if remote_node_id == self.state.node_id:
            return

        # Check if we have this entity locally
        wrapper = self.state.entity_wrappers.get(entity_type, {}).get(entity_id)
        if not wrapper:
            return  # We don't have it, no conflict

        local_version = wrapper.version
        local_node_id = wrapper.node_id

        # Detect conflict: same version, different nodes
        if local_version == remote_version and local_node_id != remote_node_id:
            log.info(
                f"Conflict detected for {entity_type}:{entity_id} "
                f"(local={local_node_id}:v{local_version}, remote={remote_node_id}:v{remote_version})"
            )

            # Deterministic tiebreaker: lower node_id initiates
            if self.state.node_id < remote_node_id:
                await self._initiate_merge(entity_type, entity_id, remote_node_id, ctx)

    async def _initiate_merge(
        self,
        entity_type: str,
        entity_id: str,
        peer_node: str,
        ctx: Context,
    ) -> None:
        """Initiate three-way merge with peer node."""
        wrapper = self.state.entity_wrappers.get(entity_type, {}).get(entity_id)
        if not wrapper:
            log.warning(f"Cannot initiate merge: no wrapper for {entity_type}:{entity_id}")
            return

        self.state.request_counter += 1
        request_id = f"{self.state.node_id}-merge-{self.state.request_counter}"

        version, base_version, _, _ = wrapper.get_merge_info()

        wire_msg = MergeRequest(
            request_id=request_id,
            entity_type=entity_type,
            entity_id=entity_id,
            my_version=version,
            my_base_version=base_version,
        )

        if self.state.mux:
            data = ActorCodec.encode(wire_msg)
            await self.state.mux.send(Send(ProtocolType.ACTOR, peer_node, data))
            log.debug(f"Sent MergeRequest {request_id} to {peer_node}")

    async def _publish_version(
        self, entity_type: str, entity_id: str, wrapper: MergeableActor
    ) -> None:
        """Publish entity version to gossip for conflict detection."""
        if not self.state.gossip:
            return

        from casty.gossip import Publish

        key = f"__merge/{entity_type}:{entity_id}"
        value_dict = {
            "version": wrapper.version,
            "node_id": wrapper.node_id,
        }
        value_bytes = msgpack.packb(value_dict, use_bin_type=True)

        await self.state.gossip.send(
            Publish(key=key, value=value_bytes, ttl=None),
        )
