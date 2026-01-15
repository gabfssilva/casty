"""ReplicationCoordinator - Orchestrates replicated writes and manages metadata.

This actor coordinates replicated writes across multiple replica nodes,
manages hinted handoff for offline replicas, and reacts to node failures.
"""

from __future__ import annotations

import asyncio
import logging
import msgpack
from typing import TYPE_CHECKING, Any

from casty import Actor, Context, LocalRef

from .metadata import ReplicationMetadata, WriteCoordination, WriteHint

if TYPE_CHECKING:
    from casty.cluster.consistency import ShardConsistency

log = logging.getLogger(__name__)


class ReplicationCoordinator(Actor[Any]):
    """Coordinates replicated writes and tracks replication metadata.

    Responsibilities:
    - Track replication metadata (replication_factor, consistency)
    - Coordinate writes to replicas (send + wait for acks)
    - Handle hinted handoff (store writes for offline replicas)
    - React to SWIM failures (update metadata on node changes)

    This actor is spawned as a child of Cluster and delegates
    coordination logic to keep Cluster focused on routing.
    """

    def __init__(
        self,
        node_id: str,
        cluster: LocalRef[Any],
        mux: LocalRef[Any],
    ) -> None:
        """Initialize ReplicationCoordinator.

        Args:
            node_id: This node's unique identifier
            cluster: Reference to parent Cluster actor
            mux: Reference to TransportMux for network communication
        """
        self.node_id = node_id
        self.cluster = cluster
        self.mux = mux
        self._ctx: Context[Any] | None = None  # Set during first receive

        # Replication metadata (type_name → ReplicationMetadata)
        self._replicated_types: dict[str, ReplicationMetadata] = {}

        # In-flight write coordination (request_id → WriteCoordination)
        self._pending_writes: dict[str, WriteCoordination] = {}
        self._request_counter = 0

        # Hinted handoff (node_id → list[WriteHint])
        # Hints are ordered FIFO, oldest hints are dropped if exceeding max
        self._hints: dict[str, list[WriteHint]] = {}
        self._max_hints_per_node = 1000  # Prevent unbounded growth

    async def receive(self, msg: Any, ctx: Context[Any]) -> None:
        """Process incoming messages."""
        # Store context for use in async tasks
        if not self._ctx:
            self._ctx = ctx

        from casty.cluster.messages import (
            RegisterReplicatedType,
            CoordinateReplicatedWrite,
            CoordinateWALReplication,
            NodeFailed,
            NodeJoined,
        )
        from casty.cluster.codec import ReplicateWriteAck, ReplicateWALAck

        match msg:
            case RegisterReplicatedType(type_name, actor_cls, actor_type, rf, wc):
                self._handle_register_replicated_type(type_name, actor_cls, actor_type, rf, wc)

            case CoordinateReplicatedWrite(et, eid, payload, pref_list, consistency, actor_type):
                await self._handle_coordinate_write(et, eid, payload, pref_list, consistency, ctx)

            case CoordinateWALReplication(wal_entry, pref_list, consistency):
                await self._handle_coordinate_wal_replication(wal_entry, pref_list, consistency, ctx)

            case ReplicateWriteAck(request_id, entity_type, entity_id, node_id, success, error):
                log.info(f"[ReplicationCoordinator] Received ack: request_id={request_id}, node_id={node_id}, success={success}")
                self._handle_write_ack(request_id, node_id, success, error)

            case ReplicateWALAck(request_id, entity_type, entity_id, node_id, sequence, success, error):
                log.info(f"[ReplicationCoordinator] Received WAL ack: request_id={request_id}, node_id={node_id}, seq={sequence}, success={success}")
                self._handle_write_ack(request_id, node_id, success, error)

            case NodeFailed(node_id):
                self._handle_node_failed(node_id)

            case NodeJoined(node_id, _):
                await self._handle_node_joined(node_id)

    def _handle_register_replicated_type(
        self,
        type_name: str,
        actor_cls: type[Any],
        actor_type: str,
        replication_factor: int,
        write_consistency: ShardConsistency,
    ) -> None:
        """Register a replicated actor type with its metadata."""
        self._replicated_types[type_name] = ReplicationMetadata(
            type_name=type_name,
            actor_cls=actor_cls,
            actor_type=actor_type,
            replication_factor=replication_factor,
            write_consistency=write_consistency,
        )
        log.info(
            f"Registered replicated type: {type_name} "
            f"(rf={replication_factor}, {write_consistency})"
        )

    async def _handle_coordinate_write(
        self,
        entity_type: str,
        entity_id: str,
        payload: Any,
        preference_list: list[str],
        consistency: ShardConsistency,
        ctx: Context[Any],
    ) -> None:
        """Coordinate replicated write to all replicas."""
        from casty.cluster.codec import ReplicateWrite
        from casty.cluster.messages import ProtocolType
        from casty.cluster.codec import ActorCodec
        from casty.transport import Send

        metadata = self._replicated_types.get(entity_type)
        if not metadata:
            log.warning(f"Coordinate write for unregistered type: {entity_type}")
            ctx.reply(False)
            return

        # Generate request ID
        self._request_counter += 1
        request_id = f"{self.node_id}-rw-{self._request_counter}"

        # Calculate required acks
        required_acks = consistency.required_acks(len(preference_list))
        log.info(f"[CoordinateWrite] request_id={request_id}, entity={entity_type}:{entity_id}, pref_list={preference_list} (len={len(preference_list)}), required_acks={required_acks}")

        # Create coordination state
        loop = asyncio.get_running_loop()
        future: asyncio.Future[bool] = loop.create_future()

        coordination = WriteCoordination(
            request_id=request_id,
            entity_type=entity_type,
            entity_id=entity_id,
            payload=payload,
            target_replicas=preference_list,
            acks_received=set(),
            required_acks=required_acks,
            reply_future=future,
            started_at=loop.time(),
        )

        self._pending_writes[request_id] = coordination

        # Serialize payload
        payload_type = f"{type(payload).__module__}.{type(payload).__qualname__}"
        try:
            payload_dict = payload.__dict__ if hasattr(payload, "__dict__") else vars(payload)
        except Exception:
            payload_dict = {}
        payload_bytes = msgpack.packb(payload_dict, use_bin_type=True)
        actor_cls_fqn = f"{metadata.actor_cls.__module__}.{metadata.actor_cls.__qualname__}"

        # Send to all replicas in parallel
        log.info(f"[CoordinateWrite] Sending writes to {len(preference_list)} replicas: {preference_list}")
        for i, node_id in enumerate(preference_list):
            if node_id == self.node_id:
                # Local write - deliver directly
                log.info(f"[Coordinator] ({i+1}/{len(preference_list)}) Local replica write for {request_id} to local node {node_id}")
                asyncio.create_task(
                    self._local_replicated_write(
                        request_id, entity_type, entity_id, payload, actor_cls_fqn
                    )
                )
            else:
                # Remote write - send via network
                log.info(f"[Coordinator] ({i+1}/{len(preference_list)}) Sending ReplicateWrite for {request_id} to remote node {node_id}")
                wire_msg = ReplicateWrite(
                    entity_type=entity_type,
                    entity_id=entity_id,
                    payload_type=payload_type,
                    payload=payload_bytes,
                    coordinator_id=self.node_id,
                    request_id=request_id,
                    actor_cls_fqn=actor_cls_fqn,
                )
                data = ActorCodec.encode(wire_msg)
                try:
                    await self.mux.send(Send(ProtocolType.ACTOR, node_id, data))
                    log.info(f"[Coordinator] Successfully sent ReplicateWrite to {node_id}")
                except Exception as e:
                    log.error(f"[Coordinator] Failed to send ReplicateWrite to {node_id}: {e}")

        # Set timeout
        timeout = 5.0
        timeout_task = asyncio.create_task(self._write_timeout(request_id, timeout))
        coordination.timeout_task = timeout_task

        # Wait for required acks
        # Capture reply_envelope directly since ctx._current_envelope is cleared after handler returns
        reply_envelope = ctx._current_envelope

        async def wait_for_acks() -> None:
            try:
                success = await asyncio.wait_for(future, timeout=timeout)
                log.info(f"[wait_for_acks] {request_id} completed successfully with {len(coordination.acks_received)} acks")
                if reply_envelope and reply_envelope.reply_to and not reply_envelope.reply_to.done():
                    reply_envelope.reply_to.set_result(success)
            except asyncio.TimeoutError:
                log.warning(f"[wait_for_acks] {request_id} timeout! Got {len(coordination.acks_received)}/{coordination.required_acks} acks")
                if reply_envelope and reply_envelope.reply_to and not reply_envelope.reply_to.done():
                    reply_envelope.reply_to.set_result(False)
            finally:
                self._pending_writes.pop(request_id, None)
                log.info(f"[wait_for_acks] {request_id} removed from pending_writes")
                if timeout_task and not timeout_task.done():
                    timeout_task.cancel()

        asyncio.create_task(wait_for_acks())

    async def _handle_coordinate_wal_replication(
        self,
        wal_entry: Any,  # ReplicationWALEntry
        preference_list: list[str],
        consistency: ShardConsistency,
        ctx: Context[Any],
    ) -> None:
        """Coordinate WAL-based replication to all replicas.

        Instead of sending the original message, we send the resulting state
        (WAL entry) to replicas. Replicas apply the state directly via set_state().
        """
        from casty.cluster.codec import ActorCodec, ReplicateWALEntry, ReplicateWALAck
        from casty.cluster.messages import ProtocolType
        from casty.transport import Send

        # Generate request ID
        self._request_counter += 1
        request_id = f"{self.node_id}-wal-{self._request_counter}"

        # Calculate required acks
        required_acks = consistency.required_acks(len(preference_list))
        log.info(
            f"[CoordinateWAL] request_id={request_id}, "
            f"entity={wal_entry.entity_type}:{wal_entry.entity_id}, "
            f"seq={wal_entry.sequence}, "
            f"pref_list={preference_list} (len={len(preference_list)}), "
            f"required_acks={required_acks}"
        )

        # Create coordination state
        loop = asyncio.get_running_loop()
        future: asyncio.Future[bool] = loop.create_future()

        coordination = WriteCoordination(
            request_id=request_id,
            entity_type=wal_entry.entity_type,
            entity_id=wal_entry.entity_id,
            payload=wal_entry,  # Store WAL entry for potential hinted handoff
            target_replicas=preference_list,
            acks_received=set(),
            required_acks=required_acks,
            reply_future=future,
            started_at=loop.time(),
        )

        self._pending_writes[request_id] = coordination

        # Send to all replicas in parallel
        log.info(f"[CoordinateWAL] Sending to {len(preference_list)} replicas: {preference_list}")
        for i, node_id in enumerate(preference_list):
            if node_id == self.node_id:
                # Local: already processed by primary, just ack
                log.info(f"[CoordinateWAL] ({i+1}/{len(preference_list)}) Local node (already applied)")
                coordination.acks_received.add(node_id)
            else:
                # Remote: send WAL entry
                log.info(f"[CoordinateWAL] ({i+1}/{len(preference_list)}) Sending to remote {node_id}")
                wire_msg = ReplicateWALEntry(
                    request_id=request_id,
                    coordinator_id=self.node_id,
                    sequence=wal_entry.sequence,
                    entity_type=wal_entry.entity_type,
                    entity_id=wal_entry.entity_id,
                    state=wal_entry.state,
                    message_type=wal_entry.message_type,
                    primary_node=wal_entry.node_id,
                    timestamp=wal_entry.timestamp,
                    checksum=wal_entry.checksum,
                )
                data = ActorCodec.encode(wire_msg)
                try:
                    await self.mux.send(Send(ProtocolType.ACTOR, node_id, data))
                    log.info(f"[CoordinateWAL] Successfully sent to {node_id}")
                except Exception as e:
                    log.error(f"[CoordinateWAL] Failed to send to {node_id}: {e}")

        # Check if we already have enough acks (local only case)
        if len(coordination.acks_received) >= coordination.required_acks:
            if not coordination.reply_future.done():
                coordination.reply_future.set_result(True)
                log.info(f"[CoordinateWAL] Immediate quorum (local only)")

        # Set timeout
        timeout = 5.0
        timeout_task = asyncio.create_task(self._write_timeout(request_id, timeout))
        coordination.timeout_task = timeout_task

        # Wait for required acks
        reply_envelope = ctx._current_envelope

        async def wait_for_acks() -> None:
            try:
                success = await asyncio.wait_for(future, timeout=timeout)
                log.info(f"[wait_for_acks] {request_id} WAL replication completed: {success}")
                if reply_envelope and reply_envelope.reply_to and not reply_envelope.reply_to.done():
                    reply_envelope.reply_to.set_result(success)
            except asyncio.TimeoutError:
                log.warning(
                    f"[wait_for_acks] {request_id} WAL replication timeout! "
                    f"Got {len(coordination.acks_received)}/{coordination.required_acks} acks"
                )
                if reply_envelope and reply_envelope.reply_to and not reply_envelope.reply_to.done():
                    reply_envelope.reply_to.set_result(False)
            finally:
                self._pending_writes.pop(request_id, None)
                if timeout_task and not timeout_task.done():
                    timeout_task.cancel()

        asyncio.create_task(wait_for_acks())

    async def _local_replicated_write(
        self,
        request_id: str,
        entity_type: str,
        entity_id: str,
        payload: Any,
        actor_cls_fqn: str,
    ) -> None:
        """Handle local replica write."""
        from casty.cluster.messages import LocalReplicaWrite
        from casty.cluster.codec import ReplicateWriteAck

        try:
            # Forward to Cluster to deliver to local entity
            await self.cluster.send(
                LocalReplicaWrite(
                    entity_type=entity_type,
                    entity_id=entity_id,
                    payload=payload,
                    request_id=request_id,
                    actor_cls_fqn=actor_cls_fqn,
                )
            )

            # Send ack back to self
            await self._ctx.self_ref.send(
                ReplicateWriteAck(
                    request_id=request_id,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    node_id=self.node_id,
                    success=True,
                )
            )
        except Exception as e:
            log.error(f"Local replica write failed: {e}")
            await self._ctx.self_ref.send(
                ReplicateWriteAck(
                    request_id=request_id,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    node_id=self.node_id,
                    success=False,
                    error=str(e),
                )
            )

    def _handle_write_ack(
        self,
        request_id: str,
        node_id: str,
        success: bool,
        error: str | None,
    ) -> None:
        """Handle write acknowledgment from replica."""
        log.info(f"[_handle_write_ack] Processing ack: request_id={request_id}, node={node_id}, success={success}")
        coordination = self._pending_writes.get(request_id)
        if not coordination:
            log.debug(f"[_handle_write_ack] Late ack for request_id={request_id} (already completed)")
            return

        if not success:
            # Replica failed - store hint for later
            hint = WriteHint(
                entity_type=coordination.entity_type,
                entity_id=coordination.entity_id,
                payload_type=f"{type(coordination.payload).__module__}.{type(coordination.payload).__qualname__}",
                payload=msgpack.packb(
                    coordination.payload.__dict__
                    if hasattr(coordination.payload, "__dict__")
                    else vars(coordination.payload),
                    use_bin_type=True,
                ),
                timestamp=coordination.started_at,
                actor_cls_fqn="",  # TODO: extract from metadata
            )
            self._store_hint(node_id, hint)
            log.debug(f"Stored hint for failed replica {node_id}: {coordination.entity_type}:{coordination.entity_id}")
        else:
            coordination.acks_received.add(node_id)
            log.info(f"[_handle_write_ack] Ack count: {len(coordination.acks_received)}/{coordination.required_acks}")

        # Check if we have enough acks
        if len(coordination.acks_received) >= coordination.required_acks:
            if not coordination.reply_future.done():
                coordination.reply_future.set_result(True)
                log.info(
                    f"[_handle_write_ack] Quorum reached for write {request_id}: "
                    f"{len(coordination.acks_received)}/{coordination.required_acks} acks"
                )

    async def _write_timeout(self, request_id: str, timeout: float) -> None:
        """Handle write timeout."""
        await asyncio.sleep(timeout)
        coordination = self._pending_writes.get(request_id)
        if coordination and not coordination.reply_future.done():
            log.warning(
                f"Write timeout: {request_id} "
                f"(got {len(coordination.acks_received)}/{coordination.required_acks} acks)"
            )
            coordination.reply_future.set_result(False)

    def _store_hint(self, node_id: str, hint: WriteHint) -> None:
        """Store hinted handoff for offline node."""
        if node_id not in self._hints:
            self._hints[node_id] = []

        hints = self._hints[node_id]
        if len(hints) >= self._max_hints_per_node:
            # Drop oldest hint to prevent unbounded growth
            dropped = hints.pop(0)
            log.debug(
                f"Dropped oldest hint for node {node_id}: "
                f"{dropped.entity_type}:{dropped.entity_id}"
            )

        hints.append(hint)
        log.info(f"Stored hint for node {node_id} ({len(hints)} total hints)")

    async def _handle_node_joined(self, node_id: str) -> None:
        """Replay hints when node recovers."""
        from casty.cluster.codec import ActorCodec, HintedHandoff
        from casty.cluster.messages import ProtocolType
        from casty.transport import Send

        hints = self._hints.pop(node_id, [])
        if not hints:
            return

        log.info(f"Replaying {len(hints)} hints to recovered node {node_id}")

        for hint in hints:
            wire_msg = HintedHandoff(
                entity_type=hint.entity_type,
                entity_id=hint.entity_id,
                payload_type=hint.payload_type,
                payload=hint.payload,
                original_timestamp=hint.timestamp,
                actor_cls_fqn=hint.actor_cls_fqn,
            )
            data = ActorCodec.encode(wire_msg)
            await self.mux.send(Send(ProtocolType.ACTOR, node_id, data))

    def _handle_node_failed(self, node_id: str) -> None:
        """Handle node failure - clean up pending writes."""
        # Check pending writes that involve this node
        cleaned_count = 0
        for request_id, coordination in list(self._pending_writes.items()):
            if node_id in coordination.target_replicas:
                # Node failed, won't get ack from it
                if node_id not in coordination.acks_received:
                    # Store hint for when it recovers
                    hint = WriteHint(
                        entity_type=coordination.entity_type,
                        entity_id=coordination.entity_id,
                        payload_type=f"{type(coordination.payload).__module__}.{type(coordination.payload).__qualname__}",
                        payload=msgpack.packb(
                            coordination.payload.__dict__
                            if hasattr(coordination.payload, "__dict__")
                            else vars(coordination.payload),
                            use_bin_type=True,
                        ),
                        timestamp=coordination.started_at,
                        actor_cls_fqn="",
                    )
                    self._store_hint(node_id, hint)
                    cleaned_count += 1

        log.info(f"Handled failure of node {node_id} (stored {cleaned_count} hints)")
