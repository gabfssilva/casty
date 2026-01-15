"""Replication coordinator handler mixin for replicated entities."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from casty import Context
from casty.actor import on
from casty.transport import ProtocolType
from ..codec import (
    ReplicateWrite,
    ReplicateWriteAck,
    HintedHandoff,
    ShardedTypeRegister,
)
from ..messages import (
    RegisterReplicatedType,
    LocalReplicaWrite,
)

if TYPE_CHECKING:
    from ..state import ClusterState

log = logging.getLogger(__name__)


class ReplicationHandlers:
    """Handles replicated actor type registration and replication coordination."""

    state: ClusterState

    @on(RegisterReplicatedType)
    async def handle_register_replicated_type(
        self,
        msg: RegisterReplicatedType,
        ctx: Context,
    ) -> None:
        """Register a replicated actor type."""
        from casty.replication import ReplicationMetadata

        type_name = msg.type_name
        actor_cls = msg.actor_cls
        actor_type = msg.actor_type
        replication_factor = msg.replication_factor
        write_consistency = msg.write_consistency

        metadata = ReplicationMetadata(
            type_name=type_name,
            actor_cls=actor_cls,
            actor_type=actor_type,
            replication_factor=replication_factor,
            write_consistency=write_consistency,
        )
        self.state.replicated_types[type_name] = metadata
        log.info(
            f"Registered replicated type: {type_name} "
            f"(type={actor_type}, rf={replication_factor}, {write_consistency})"
        )

        # Also register with ReplicationCoordinator
        if self.state.replication_coordinator:
            await self.state.replication_coordinator.send(msg)

    @on(ShardedTypeRegister)
    async def handle_sharded_type_register(
        self,
        msg: ShardedTypeRegister,
        ctx: Context,
    ) -> None:
        """Handle sharded type registration from remote coordinator."""
        entity_type = msg.entity_type
        actor_cls_fqn = msg.actor_cls_fqn

        try:
            from ..cluster import _import_class

            actor_cls = _import_class(actor_cls_fqn)
            self.state.sharded_types[entity_type] = actor_cls
            if entity_type not in self.state.local_entities:
                self.state.local_entities[entity_type] = {}
            log.debug(f"Registered sharded type from wire: {entity_type} -> {actor_cls.__name__}")
            ctx.reply(True)
        except Exception as e:
            log.error(f"Failed to register sharded type {entity_type}: {e}")
            ctx.reply(False)

    @on(LocalReplicaWrite)
    async def handle_local_replica_write(
        self,
        msg: LocalReplicaWrite,
        ctx: Context,
    ) -> None:
        """Handle local replica write from ReplicationCoordinator."""
        entity_type = msg.entity_type
        entity_id = msg.entity_id
        payload = msg.payload
        request_id = msg.request_id
        actor_cls_fqn = msg.actor_cls_fqn

        try:
            ref = await self._get_or_create_entity(entity_type, entity_id, ctx, actor_cls_fqn)
            await ref.send(payload)

            # Increment version for mergeable entities
            wrapper = self.state.entity_wrappers.get(entity_type, {}).get(entity_id)
            if wrapper:
                wrapper.increment_version()
                await self._publish_version(entity_type, entity_id, wrapper)

            # Send ack back to coordinator
            if self.state.replication_coordinator:
                await self.state.replication_coordinator.send(
                    ReplicateWriteAck(
                        request_id=request_id,
                        entity_type=entity_type,
                        entity_id=entity_id,
                        node_id=self.state.node_id,
                        success=True,
                    )
                )
        except Exception as e:
            log.error(f"Local replica write failed: {e}")
            if self.state.replication_coordinator:
                await self.state.replication_coordinator.send(
                    ReplicateWriteAck(
                        request_id=request_id,
                        entity_type=entity_type,
                        entity_id=entity_id,
                        node_id=self.state.node_id,
                        success=False,
                        error=str(e),
                    )
                )

    @on(ReplicateWrite)
    async def handle_replicate_write(
        self,
        msg: ReplicateWrite,
        ctx: Context,
    ) -> None:
        """Handle replicate write request from remote coordinator."""
        entity_type = msg.entity_type
        entity_id = msg.entity_id
        payload_type = msg.payload_type
        coordinator_id = msg.coordinator_id
        request_id = msg.request_id
        actor_cls_fqn = msg.actor_cls_fqn

        try:
            import msgpack

            from ..cluster import deserialize_message

            ref = await self._get_or_create_entity(entity_type, entity_id, ctx, actor_cls_fqn)
            payload_dict = msgpack.unpackb(msg.payload, raw=False)
            payload = deserialize_message(payload_type, payload_dict)
            await ref.send(payload)

            # Increment version for mergeable entities
            wrapper = self.state.entity_wrappers.get(entity_type, {}).get(entity_id)
            if wrapper:
                wrapper.increment_version()
                await self._publish_version(entity_type, entity_id, wrapper)

            # Send ack back to coordinator
            if self.state.mux:
                from casty.transport import Send
                from casty.cluster.messages import ReplicateWriteAck as RemoteReplicateWriteAck
                from ..codec import ActorCodec

                ack_msg = RemoteReplicateWriteAck(
                    request_id=request_id,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    node_id=self.state.node_id,
                    success=True,
                )
                data = ActorCodec.encode(ack_msg)
                await self.state.mux.send(Send(ProtocolType.ACTOR, coordinator_id, data))
        except Exception as e:
            log.error(f"Remote replicate write failed: {e}")
            if self.state.mux:
                from casty.transport import Send
                from casty.cluster.messages import ReplicateWriteAck as RemoteReplicateWriteAck
                from ..codec import ActorCodec

                ack_msg = RemoteReplicateWriteAck(
                    request_id=request_id,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    node_id=self.state.node_id,
                    success=False,
                    error=str(e),
                )
                data = ActorCodec.encode(ack_msg)
                await self.state.mux.send(Send(ProtocolType.ACTOR, coordinator_id, data))

    @on(HintedHandoff)
    async def handle_hinted_handoff(
        self,
        msg: HintedHandoff,
        ctx: Context,
    ) -> None:
        """Handle hinted handoff message."""
        entity_type = msg.entity_type
        entity_id = msg.entity_id
        payload_type = msg.payload_type
        actor_cls_fqn = msg.actor_cls_fqn

        try:
            import msgpack

            from ..cluster import deserialize_message

            ref = await self._get_or_create_entity(entity_type, entity_id, ctx, actor_cls_fqn)
            payload_dict = msgpack.unpackb(msg.payload, raw=False)
            payload = deserialize_message(payload_type, payload_dict)
            await ref.send(payload)

            # Increment version
            wrapper = self.state.entity_wrappers.get(entity_type, {}).get(entity_id)
            if wrapper:
                wrapper.increment_version()
                await self._publish_version(entity_type, entity_id, wrapper)

            log.debug(f"Applied hinted handoff for {entity_type}:{entity_id}")
        except Exception as e:
            log.error(f"Failed to apply hinted handoff: {e}")

    async def _get_or_create_entity(
        self, entity_type: str, entity_id: str, ctx: Context, actor_cls_fqn: str = ""
    ) -> Any:
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
        self, entity_type: str, entity_id: str, wrapper: Any
    ) -> None:
        """Publish entity version to gossip for conflict detection."""
        # Implemented in merge.py mixin
        pass
