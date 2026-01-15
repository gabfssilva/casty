"""Sharded entities (distributed actors) handler mixin."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

import msgpack

from casty import Actor, Context, LocalRef
from casty.actor import on
from casty.merge import MergeableActor, is_mergeable
from casty.transport import Send, ProtocolType
from casty.replication.wal_entry import ReplicationWALEntry, WALGapError
from casty.replication.replicated_actor import ReplicatedActorWrapper
from ..codec import (
    ActorCodec,
    EntitySend,
    EntityAskRequest,
    EntityAskResponse,
    ShardedTypeRegister,
    ShardedTypeAck,
    ReplicateWALEntry,
    ReplicateWALAck,
    RequestCatchUp,
    CatchUpEntries,
)
from ..consistency import ShardConsistency
from ..messages import (
    RegisterShardedType,
    RemoteEntitySend,
    RemoteEntityAsk,
)
from ..state import PendingAsk, PendingRegistration, _RegistrationTimeout

if TYPE_CHECKING:
    from ..state import ClusterState

log = logging.getLogger(__name__)


class EntityHandlers:
    """Handles sharded actor registration and message routing."""

    state: ClusterState

    @on(RegisterShardedType)
    async def handle_register_sharded_type(
        self,
        msg: RegisterShardedType,
        ctx: Context,
    ) -> None:
        """Register a sharded actor type and propagate to cluster."""
        from ..consistency import Quorum

        entity_type = msg.entity_type
        actor_cls = msg.actor_cls
        consistency = msg.consistency

        effective_consistency: ShardConsistency = consistency if consistency is not None else Quorum()

        self.state.sharded_types[entity_type] = actor_cls
        if entity_type not in self.state.local_entities:
            self.state.local_entities[entity_type] = {}
        log.debug(f"Registered sharded type: {entity_type} -> {actor_cls.__name__}")

        # Get fully qualified class name for remote nodes to import
        class_fqn = f"{actor_cls.__module__}.{actor_cls.__qualname__}"

        # Propagate to cluster via Gossip (for discovery by nodes that join later)
        if self.state.gossip:
            from casty.gossip import Publish

            await self.state.gossip.send(
                Publish(key=f"_sharded/{entity_type}", value=class_fqn.encode("utf-8"))
            )

        # If consistency requires waiting, send wire protocol registration requests
        if effective_consistency.should_wait():
            total_nodes = self.state.hash_ring.node_count
            required_acks = effective_consistency.required_acks(total_nodes)

            # Generate request ID for tracking
            self.state.request_counter += 1
            request_id = f"{self.state.node_id}-sharded-reg-{self.state.request_counter}"

            # Capture the reply future so we can reply later when acks arrive
            reply_future = ctx._current_envelope.reply_to if ctx._current_envelope else None

            pending = PendingRegistration(
                entity_type=entity_type,
                required_acks=required_acks,
                received_acks={self.state.node_id},  # Count self
                reply_future=reply_future,
            )
            self.state.pending_registrations[request_id] = pending

            log.debug(
                f"Waiting for {required_acks} acks for {entity_type} "
                f"(consistency={effective_consistency}, request_id={request_id})"
            )

            # Send wire protocol registration requests to all other nodes
            if self.state.mux:
                wire_msg = ShardedTypeRegister(
                    request_id=request_id,
                    entity_type=entity_type,
                    actor_cls_fqn=class_fqn,
                )
                data = ActorCodec.encode(wire_msg)

                # Send to all known nodes (from hash ring)
                for node_id in self.state.hash_ring.nodes:
                    if node_id != self.state.node_id:
                        log.debug(f"Sending ShardedTypeRegister to {node_id}")
                        await self.state.mux.send(Send(ProtocolType.ACTOR, node_id, data))

            # Check if we already have enough (single node cluster)
            if len(pending.received_acks) >= required_acks:
                log.debug(f"Already have enough acks for {entity_type}, replying immediately")
                self.state.pending_registrations.pop(request_id, None)
                ctx.reply(None)
            else:
                # Schedule timeout via scheduler (proper actor pattern)
                if self.state.scheduler:
                    from casty.scheduler import Schedule

                    await self.state.scheduler.send(
                        Schedule(
                            timeout=self.state.config.ask_timeout,
                            listener=ctx.self_ref,
                            message=_RegistrationTimeout(request_id),
                        )
                    )
                # Don't reply here - reply will happen when acks arrive or timeout
        else:
            ctx.reply(None)  # Eventual consistency - reply immediately

    @on(RemoteEntitySend)
    async def handle_remote_entity_send(self, msg: RemoteEntitySend, ctx: Context) -> None:
        """Handle entity send request with hash ring routing.

        For replicated entities, uses WAL-based replication:
        1. Process message on primary (this node)
        2. Detect state change via get_state() comparison
        3. If state changed: create WAL entry and replicate
        4. If read-only: no replication needed
        """
        entity_type = msg.entity_type
        entity_id = msg.entity_id
        payload = msg.payload

        # Check if this entity type is replicated
        metadata = self.state.replicated_types.get(entity_type)
        log.info(
            f"[Cluster.handle_remote_entity_send] entity_type={entity_type}, "
            f"is_replicated={metadata is not None}, "
            f"replicated_types={list(self.state.replicated_types.keys())}"
        )

        if metadata and metadata.replication_factor > 1:
            # WAL-based replication: process locally first, then replicate state
            key = f"{entity_type}:{entity_id}"
            preference_list = self.state.hash_ring.get_preference_list(
                key, metadata.replication_factor
            )

            # Only process locally if we're in the preference list (replica)
            if self.state.node_id not in preference_list:
                log.warning(
                    f"[WAL] Node {self.state.node_id} not in preference list for "
                    f"{entity_type}:{entity_id}, routing to primary"
                )
                # Forward to primary (first in preference list)
                # This is handled below in the non-local case
                pass
            else:
                # Process locally using the wrapper
                try:
                    wrapper = await self._get_or_create_replicated_wrapper(
                        entity_type, entity_id, ctx, is_primary=True
                    )

                    # Process message and detect state change
                    wal_entry = await wrapper.process_message(payload, ctx)

                    if wal_entry:
                        # State changed: replicate WAL entry to other replicas
                        log.info(
                            f"[WAL] State changed for {entity_type}:{entity_id}, "
                            f"seq={wal_entry.sequence}, replicating..."
                        )

                        if metadata.write_consistency.should_wait():
                            # Strong/Quorum: wait for acks
                            reply_envelope = ctx._current_envelope

                            async def wait_for_wal_replication() -> None:
                                try:
                                    success = await self._coordinate_wal_replication(
                                        wal_entry, preference_list, metadata.write_consistency, ctx
                                    )
                                    if reply_envelope and reply_envelope.reply_to:
                                        reply_envelope.reply_to.set_result(success)
                                except Exception as e:
                                    log.error(f"[WAL] Replication failed: {e}")
                                    if reply_envelope and reply_envelope.reply_to and not reply_envelope.reply_to.done():
                                        reply_envelope.reply_to.set_result(False)

                            asyncio.create_task(wait_for_wal_replication())
                        else:
                            # Eventual: fire-and-forget
                            await self._coordinate_wal_replication(
                                wal_entry, preference_list, metadata.write_consistency, ctx
                            )
                            ctx.reply(True)
                    else:
                        # Read-only: no replication needed, reply immediately
                        log.debug(
                            f"[WAL] No state change for {entity_type}:{entity_id} "
                            f"(read-only message)"
                        )
                        ctx.reply(True)
                    return

                except Exception as e:
                    log.error(f"[WAL] Failed to process message: {e}")
                    ctx.reply(False)
                    return

        # Non-replicated entity - use existing logic
        key = f"{entity_type}:{entity_id}"
        target_node = self.state.hash_ring.get_node(key)

        if target_node is None:
            log.warning(f"Cannot route entity {key}: empty hash ring")
            return

        if target_node == self.state.node_id:
            # Local delivery
            try:
                ref = await self._get_or_create_entity(entity_type, entity_id, ctx)
                await ref.send(payload)

                # Increment version for mergeable entities
                wrapper = self.state.entity_wrappers.get(entity_type, {}).get(entity_id)
                if wrapper:
                    wrapper.increment_version()
                    await self._publish_version(entity_type, entity_id, wrapper)
            except Exception as e:
                log.error(f"Failed to deliver to entity {key}: {e}")
        else:
            # Forward to remote node
            payload_type = f"{type(payload).__module__}.{type(payload).__qualname__}"
            payload_bytes = msgpack.packb(payload.__dict__, use_bin_type=True)

            # Include actor class FQN for auto-registration on remote
            actor_cls = self.state.sharded_types.get(entity_type)
            actor_cls_fqn = f"{actor_cls.__module__}.{actor_cls.__qualname__}" if actor_cls else ""

            wire_msg = EntitySend(
                entity_type=entity_type,
                entity_id=entity_id,
                payload_type=payload_type,
                payload=payload_bytes,
                actor_cls_fqn=actor_cls_fqn,
            )
            data = ActorCodec.encode(wire_msg)
            if self.state.mux:
                await self.state.mux.send(Send(ProtocolType.ACTOR, target_node, data))

    @on(RemoteEntityAsk)
    async def handle_remote_entity_ask(self, msg: RemoteEntityAsk, ctx: Context) -> None:
        """Handle entity ask request with hash ring routing."""
        entity_type = msg.entity_type
        entity_id = msg.entity_id
        payload = msg.payload

        key = f"{entity_type}:{entity_id}"

        # Check if local node has a replica WITH DATA - read locally if so
        metadata = self.state.replicated_types.get(entity_type)
        if metadata and metadata.replication_factor > 1:
            preference_list = self.state.hash_ring.get_preference_list(
                key, metadata.replication_factor
            )
            if self.state.node_id in preference_list:
                # Check if we have RECEIVED replica data (not just in preference list)
                wrappers = self.state.replicated_wrappers.get(entity_type, {})
                wrapper = wrappers.get(entity_id)

                if wrapper and wrapper.sequence > 0:
                    # We have replicated data - safe to read locally (NO NETWORK!)
                    try:
                        ref = await self._get_or_create_entity(entity_type, entity_id, ctx)
                        result = await ref.ask(payload, timeout=self.state.config.ask_timeout)
                        ctx.reply(result)
                        return
                    except Exception as e:
                        log.error(f"Failed to ask local replica {key}: {e}")
                        ctx.reply(None)
                        return
                # No replicated data yet - fall through to route to primary

        # No local replica - route to primary
        target_node = self.state.hash_ring.get_node(key)

        if target_node is None:
            log.warning(f"Cannot route entity {key}: empty hash ring")
            ctx.reply(None)
            return

        if target_node == self.state.node_id:
            # Local delivery
            try:
                ref = await self._get_or_create_entity(entity_type, entity_id, ctx)
                result = await ref.ask(payload, timeout=self.state.config.ask_timeout)

                # Increment version for mergeable entities
                wrapper = self.state.entity_wrappers.get(entity_type, {}).get(entity_id)
                if wrapper:
                    wrapper.increment_version()
                    await self._publish_version(entity_type, entity_id, wrapper)

                ctx.reply(result)
            except Exception as e:
                log.error(f"Failed to ask entity {key}: {e}")
                ctx.reply(None)
        else:
            # Forward to remote node
            self.state.request_counter += 1
            request_id = f"{self.state.node_id}-entity-{self.state.request_counter}"

            loop = asyncio.get_running_loop()
            future: asyncio.Future[Any] = loop.create_future()

            # Capture reply envelope so we can reply asynchronously
            reply_envelope = ctx._current_envelope

            self.state.pending_asks[request_id] = PendingAsk(
                request_id=request_id,
                target_actor=f"{entity_type}:{entity_id}",
                target_node=target_node,
                future=future,
            )

            payload_type = f"{type(payload).__module__}.{type(payload).__qualname__}"
            payload_bytes = msgpack.packb(payload.__dict__, use_bin_type=True)

            # Include actor class FQN for auto-registration on remote
            actor_cls = self.state.sharded_types.get(entity_type)
            actor_cls_fqn = f"{actor_cls.__module__}.{actor_cls.__qualname__}" if actor_cls else ""

            wire_msg = EntityAskRequest(
                request_id=request_id,
                entity_type=entity_type,
                entity_id=entity_id,
                payload_type=payload_type,
                payload=payload_bytes,
                actor_cls_fqn=actor_cls_fqn,
            )
            data = ActorCodec.encode(wire_msg)
            if self.state.mux:
                await self.state.mux.send(Send(ProtocolType.ACTOR, target_node, data))

            # Spawn a task to wait for response (non-blocking)
            # This allows receive() to continue processing other messages
            async def wait_for_response() -> None:
                try:
                    result = await asyncio.wait_for(future, timeout=self.state.config.ask_timeout)
                    if reply_envelope and reply_envelope.reply_to:
                        reply_envelope.reply_to.set_result(result)
                except asyncio.TimeoutError:
                    self.state.pending_asks.pop(request_id, None)
                    if reply_envelope and reply_envelope.reply_to:
                        reply_envelope.reply_to.set_exception(
                            TimeoutError(f"Entity ask timed out: {entity_type}:{entity_id}")
                        )
                except Exception as e:
                    if reply_envelope and reply_envelope.reply_to and not reply_envelope.reply_to.done():
                        reply_envelope.reply_to.set_exception(e)

            asyncio.create_task(wait_for_response())

    async def _get_or_create_entity(
        self, entity_type: str, entity_id: str, ctx: Context, actor_cls_fqn: str = ""
    ) -> LocalRef:
        """Get or create a local entity actor."""
        # Auto-register type if FQN provided and not already registered
        if actor_cls_fqn and entity_type not in self.state.sharded_types:
            try:
                from ..cluster import _import_class

                actor_cls = _import_class(actor_cls_fqn)
                self.state.sharded_types[entity_type] = actor_cls
                self.state.local_entities[entity_type] = {}
                log.debug(f"Auto-registered sharded type: {entity_type} from {actor_cls_fqn}")
            except Exception as e:
                log.error(f"Failed to auto-register sharded type {entity_type}: {e}")
                raise ValueError(f"Failed to import actor class: {actor_cls_fqn}")

        entities = self.state.local_entities.get(entity_type)
        if entities is None:
            raise ValueError(f"Unknown entity type: {entity_type}")

        if entity_id not in entities:
            actor_cls = self.state.sharded_types.get(entity_type)
            if actor_cls is None:
                raise ValueError(f"Unknown sharded type: {entity_type}")

            # Spawn the actor using the provided context, passing entity_id
            ref = await ctx.spawn(actor_cls, entity_id=entity_id)
            entities[entity_id] = ref
            log.debug(f"Spawned entity {entity_type}:{entity_id}")

        return entities[entity_id]

    async def _publish_version(
        self, entity_type: str, entity_id: str, wrapper: MergeableActor
    ) -> None:
        """Publish entity version to gossip for conflict detection."""
        # Implemented in merge.py mixin
        pass

    async def _handle_registration_timeout(self, request_id: str) -> None:
        """Handle timeout for sharded type registration."""
        pending = self.state.pending_registrations.pop(request_id, None)
        if pending and pending.reply_future and not pending.reply_future.done():
            log.warning(
                f"Timeout waiting for acks for {pending.entity_type}: "
                f"got {len(pending.received_acks)}/{pending.required_acks}"
            )
            # Reply with None on timeout (registration still happened locally)
            pending.reply_future.set_result(None)

    # =========================================================================
    # WAL-Based Replication
    # =========================================================================

    async def _get_or_create_replicated_wrapper(
        self,
        entity_type: str,
        entity_id: str,
        ctx: Context,
        is_primary: bool = True,
    ) -> ReplicatedActorWrapper:
        """Get or create a ReplicatedActorWrapper for an entity.

        The wrapper tracks state changes and creates WAL entries for replication.
        """
        # Get or create the entity actor first
        ref = await self._get_or_create_entity(entity_type, entity_id, ctx)

        # Get or create wrapper
        if entity_type not in self.state.replicated_wrappers:
            self.state.replicated_wrappers[entity_type] = {}

        wrappers = self.state.replicated_wrappers[entity_type]

        if entity_id not in wrappers:
            # Get the actual actor instance from supervision tree
            node = ctx.system._supervision_tree.get_node(ref.id)
            if not node:
                raise RuntimeError(
                    f"Could not find supervision node for {entity_type}:{entity_id}"
                )
            actor = node.actor_instance

            wrapper = ReplicatedActorWrapper(
                actor=actor,
                entity_type=entity_type,
                entity_id=entity_id,
                node_id=self.state.node_id,
                is_primary=is_primary,
            )
            wrappers[entity_id] = wrapper
            log.debug(
                f"[WAL] Created wrapper for {entity_type}:{entity_id} "
                f"(primary={is_primary})"
            )

        return wrappers[entity_id]

    async def _coordinate_wal_replication(
        self,
        wal_entry: ReplicationWALEntry,
        preference_list: list[str],
        consistency: ShardConsistency,
        ctx: Context,
    ) -> bool:
        """Coordinate WAL entry replication to other replicas.

        This sends the WAL entry (containing the resulting state) to all
        replicas in the preference list. Replicas apply the state directly
        via set_state() instead of re-processing the original message.
        """
        from ..messages import CoordinateWALReplication

        if not self.state.replication_coordinator:
            log.warning("[WAL] No replication coordinator available")
            return False

        coord_msg = CoordinateWALReplication(
            wal_entry=wal_entry,
            preference_list=preference_list,
            consistency=consistency,
        )

        if consistency.should_wait():
            # Strong/Quorum: wait for acks
            try:
                success = await self.state.replication_coordinator.ask(
                    coord_msg, timeout=10.0
                )
                return success
            except asyncio.TimeoutError:
                log.warning(
                    f"[WAL] Replication timeout for "
                    f"{wal_entry.entity_type}:{wal_entry.entity_id}"
                )
                return False
            except Exception as e:
                log.error(f"[WAL] Replication failed: {e}")
                return False
        else:
            # Eventual: fire-and-forget
            await self.state.replication_coordinator.send(coord_msg)
            return True

    @on(ReplicateWALEntry)
    async def handle_replicate_wal_entry(
        self, msg: ReplicateWALEntry, ctx: Context
    ) -> None:
        """Handle WAL entry replication from primary.

        Instead of processing the original message, we apply the resulting
        state directly via set_state(). This ensures deterministic state
        across replicas.
        """
        entity_type = msg.entity_type
        entity_id = msg.entity_id

        log.debug(
            f"[WAL] Received entry for {entity_type}:{entity_id} "
            f"seq={msg.sequence} from {msg.primary_node}"
        )

        try:
            # Get or create wrapper (as replica)
            wrapper = await self._get_or_create_replicated_wrapper(
                entity_type, entity_id, ctx, is_primary=False
            )

            # Create WAL entry from wire message
            entry = ReplicationWALEntry(
                sequence=msg.sequence,
                entity_type=msg.entity_type,
                entity_id=msg.entity_id,
                state=msg.state,
                message_type=msg.message_type,
                node_id=msg.primary_node,
                timestamp=msg.timestamp,
                checksum=msg.checksum,
            )

            # Apply state directly
            wrapper.apply_wal_entry(entry)

            # Send ack
            ack = ReplicateWALAck(
                request_id=msg.request_id,
                entity_type=entity_type,
                entity_id=entity_id,
                node_id=self.state.node_id,
                sequence=msg.sequence,
                success=True,
            )

            # Send ack back to coordinator
            if msg.coordinator_id == self.state.node_id:
                # Local coordinator
                if self.state.replication_coordinator:
                    await self.state.replication_coordinator.send(ack)
            else:
                # Remote coordinator
                data = ActorCodec.encode(ack)
                if self.state.mux:
                    await self.state.mux.send(
                        Send(ProtocolType.ACTOR, msg.coordinator_id, data)
                    )

        except WALGapError as e:
            log.warning(
                f"[WAL] Gap detected for {entity_type}:{entity_id}: "
                f"expected={e.expected}, got={e.received}"
            )
            # Request catch-up
            catch_up_msg = RequestCatchUp(
                entity_type=entity_type,
                entity_id=entity_id,
                since_sequence=e.expected - 1,
                requesting_node=self.state.node_id,
            )
            data = ActorCodec.encode(catch_up_msg)
            if self.state.mux:
                await self.state.mux.send(
                    Send(ProtocolType.ACTOR, msg.primary_node, data)
                )

        except Exception as e:
            log.error(
                f"[WAL] Failed to apply entry for {entity_type}:{entity_id}: {e}"
            )
            # Send failure ack
            ack = ReplicateWALAck(
                request_id=msg.request_id,
                entity_type=entity_type,
                entity_id=entity_id,
                node_id=self.state.node_id,
                sequence=msg.sequence,
                success=False,
                error=str(e),
            )
            if msg.coordinator_id == self.state.node_id:
                if self.state.replication_coordinator:
                    await self.state.replication_coordinator.send(ack)
            else:
                data = ActorCodec.encode(ack)
                if self.state.mux:
                    await self.state.mux.send(
                        Send(ProtocolType.ACTOR, msg.coordinator_id, data)
                    )

    @on(RequestCatchUp)
    async def handle_request_catch_up(
        self, msg: RequestCatchUp, ctx: Context
    ) -> None:
        """Handle catch-up request from a replica.

        Send missing WAL entries to the requesting replica.
        """
        entity_type = msg.entity_type
        entity_id = msg.entity_id

        log.info(
            f"[WAL] Catch-up request for {entity_type}:{entity_id} "
            f"since={msg.since_sequence} from {msg.requesting_node}"
        )

        # Get the local wrapper
        wrappers = self.state.replicated_wrappers.get(entity_type, {})
        wrapper = wrappers.get(entity_id)

        if not wrapper:
            log.warning(
                f"[WAL] No wrapper for catch-up: {entity_type}:{entity_id}"
            )
            return

        # For now, send current state as a single entry
        # In a full implementation, we'd read from WAL storage
        if wrapper.last_entry and wrapper.last_entry.sequence > msg.since_sequence:
            entries = [wrapper.last_entry.to_bytes()]
            from_seq = wrapper.last_entry.sequence
            to_seq = wrapper.last_entry.sequence
        else:
            entries = []
            from_seq = msg.since_sequence
            to_seq = msg.since_sequence

        catch_up = CatchUpEntries(
            entity_type=entity_type,
            entity_id=entity_id,
            entries=entries,
            from_sequence=from_seq,
            to_sequence=to_seq,
        )

        data = ActorCodec.encode(catch_up)
        if self.state.mux:
            await self.state.mux.send(
                Send(ProtocolType.ACTOR, msg.requesting_node, data)
            )

    @on(CatchUpEntries)
    async def handle_catch_up_entries(
        self, msg: CatchUpEntries, ctx: Context
    ) -> None:
        """Handle catch-up entries from primary.

        Apply missing WAL entries in order.
        """
        entity_type = msg.entity_type
        entity_id = msg.entity_id

        log.info(
            f"[WAL] Received catch-up for {entity_type}:{entity_id} "
            f"entries={len(msg.entries)} range={msg.from_sequence}-{msg.to_sequence}"
        )

        if not msg.entries:
            return

        # Get wrapper
        wrappers = self.state.replicated_wrappers.get(entity_type, {})
        wrapper = wrappers.get(entity_id)

        if not wrapper:
            log.warning(
                f"[WAL] No wrapper for catch-up entries: {entity_type}:{entity_id}"
            )
            return

        # Deserialize and apply entries
        entries = [ReplicationWALEntry.from_bytes(e) for e in msg.entries]
        applied = wrapper.apply_catch_up_entries(entries)

        log.info(
            f"[WAL] Applied {applied} catch-up entries for "
            f"{entity_type}:{entity_id}"
        )
