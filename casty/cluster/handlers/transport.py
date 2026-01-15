"""Transport/TransportMux handler mixin."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import msgpack

from casty import Context
from casty.actor import on
from casty.transport import (
    Listening,
    PeerConnected,
    PeerDisconnected,
    Incoming,
    RegisterHandler,
    ConnectTo,
    Send,
    ProtocolType,
)
from casty.swim import SwimCoordinator, Subscribe as SwimSubscribe
from casty.gossip import GossipManager, GossipConfig, Subscribe as GossipSubscribe
from casty.replication import ReplicationCoordinator
from ..codec import (
    ActorCodec,
    AskRequest,
    ActorMessage,
    AskResponse,
    EntitySend,
    EntityAskRequest,
    EntityAskResponse,
    SingletonSend,
    SingletonAskRequest,
    SingletonAskResponse,
    ShardedTypeRegister,
    ShardedTypeAck,
    ReplicateWrite,
    ReplicateWriteAck,
    MergeRequest,
    MergeState,
    MergeComplete,
    ReplicateWALEntry,
    ReplicateWALAck,
    RequestCatchUp,
    CatchUpEntries,
)

if TYPE_CHECKING:
    from ..state import ClusterState

log = logging.getLogger(__name__)


class TransportHandlers:
    """Handles TransportMux events (Listening, PeerConnected/Disconnected, Incoming)."""

    state: ClusterState

    @on(Listening)
    async def handle_listening(self, msg: Listening, ctx: Context) -> None:
        """Handle TransportMux bound event."""
        self.state.bound_address = msg.address
        log.info(f"Cluster {self.state.node_id} listening on {msg.address[0]}:{msg.address[1]}")

        # Spawn SWIM coordinator
        swim_config = self.state.config.swim_config.with_bind_address(msg.address[0], msg.address[1])
        self.state.swim = await ctx.spawn(
            SwimCoordinator,
            node_id=self.state.node_id,
            config=swim_config,
            transport=self.state.mux,  # Use TransportMux for connections
        )
        await self.state.swim.send(SwimSubscribe(ctx.self_ref))

        # Spawn Gossip manager (embedded mode - no TCP, uses TransportMux)
        gossip_config = GossipConfig(gossip_interval=0.5, fanout=3)
        self.state.gossip = await ctx.spawn(
            GossipManager,
            node_id=self.state.node_id,
            bind_address=msg.address,
            config=gossip_config,
            transport=self.state.mux,  # Use TransportMux for sending
        )
        await self.state.gossip.send(GossipSubscribe("_actors/*", ctx.self_ref))
        await self.state.gossip.send(GossipSubscribe("_sharded/*", ctx.self_ref))
        await self.state.gossip.send(GossipSubscribe("_singletons/*", ctx.self_ref))
        await self.state.gossip.send(GossipSubscribe("_singleton_meta/*", ctx.self_ref))
        await self.state.gossip.send(GossipSubscribe("__merge/*", ctx.self_ref))  # For three-way merge

        # Spawn ReplicationCoordinator for managing replicated writes
        self.state.replication_coordinator = await ctx.spawn(
            ReplicationCoordinator,
            node_id=self.state.node_id,
            cluster=ctx.self_ref,
            mux=self.state.mux,
        )

        # Register protocol handlers with TransportMux
        await self.state.mux.send(RegisterHandler(ProtocolType.SWIM, self.state.swim))
        await self.state.mux.send(RegisterHandler(ProtocolType.GOSSIP, self.state.gossip))
        log.info(f"[{self.state.node_id}] Registering ACTOR handler: {ctx.self_ref}")
        await self.state.mux.send(RegisterHandler(ProtocolType.ACTOR, ctx.self_ref))

    @on(PeerConnected)
    async def handle_peer_connected(self, msg: PeerConnected, ctx: Context) -> None:
        """Handle peer connected event from TransportMux."""
        log.info(f"Peer connected: {msg.node_id} at {msg.address}")
        # Hash ring is updated when SWIM reports MemberJoined

    @on(PeerDisconnected)
    async def handle_peer_disconnected(self, msg: PeerDisconnected, ctx: Context) -> None:
        """Handle peer disconnected event from TransportMux."""
        log.info(f"Peer disconnected: {msg.node_id} ({msg.reason})")
        # Cleanup is handled when SWIM reports MemberFailed

    @on(Incoming)
    async def handle_incoming(self, msg: Incoming, ctx: Context) -> None:
        """Handle incoming ACTOR protocol data."""
        # Decode actor message
        try:
            decoded_msg = ActorCodec.decode(msg.data)
        except Exception as e:
            log.error(f"[{self.state.node_id}] Exception decoding actor message from {msg.from_node}: {e}", exc_info=True)
            return
        if decoded_msg is None:
            log.warning(f"[{self.state.node_id}] Failed to decode actor message from {msg.from_node}")
            return

        await self._process_actor_message(msg.from_node, decoded_msg, ctx)

    async def _process_actor_message(
        self, from_node: str, msg: Any, ctx: Context
    ) -> None:
        """Process an actor protocol message."""
        match msg:
            case ActorMessage(target_actor, payload_type, payload):
                local_ref = self.state.local_actors.get(target_actor)
                if local_ref:
                    try:
                        payload_dict = msgpack.unpackb(payload, raw=False)
                        message = self._deserialize_message(payload_type, payload_dict)
                        await local_ref.send(message)
                    except Exception as e:
                        log.error(f"Failed to deliver to {target_actor}: {e}")
                else:
                    log.warning(f"Actor not found: {target_actor}")

            case AskRequest(request_id, target_actor, payload_type, payload):
                local_ref = self.state.local_actors.get(target_actor)

                if not local_ref:
                    response = AskResponse(
                        request_id=request_id,
                        success=False,
                        payload_type=None,
                        payload=b"Actor not found",
                    )
                    data = ActorCodec.encode(response)
                    await self.state.mux.send(Send(ProtocolType.ACTOR, from_node, data))
                    return

                try:
                    payload_dict = msgpack.unpackb(payload, raw=False)
                    message = self._deserialize_message(payload_type, payload_dict)
                    result = await local_ref.ask(message, timeout=self.state.config.ask_timeout)

                    result_type = f"{type(result).__module__}.{type(result).__qualname__}"
                    if hasattr(result, "__dict__"):
                        result_bytes = msgpack.packb(result.__dict__, use_bin_type=True)
                    else:
                        result_bytes = msgpack.packb(result, use_bin_type=True)

                    response = AskResponse(
                        request_id=request_id,
                        success=True,
                        payload_type=result_type,
                        payload=result_bytes,
                    )
                except Exception as e:
                    response = AskResponse(
                        request_id=request_id,
                        success=False,
                        payload_type=None,
                        payload=str(e).encode("utf-8"),
                    )

                data = ActorCodec.encode(response)
                await self.state.mux.send(Send(ProtocolType.ACTOR, from_node, data))

            case AskResponse(request_id, success, payload_type, payload):
                from ..remote_ref import RemoteActorError
                pending = self.state.pending_asks.pop(request_id, None)
                if pending and not pending.future.done():
                    if success:
                        try:
                            result = msgpack.unpackb(payload, raw=False)
                            pending.future.set_result(result)
                        except Exception as e:
                            pending.future.set_exception(e)
                    else:
                        error_msg = payload.decode("utf-8") if payload else "Unknown error"
                        pending.future.set_exception(
                            RemoteActorError(pending.target_actor, pending.target_node, error_msg)
                        )

            case ShardedTypeRegister(request_id, entity_type, actor_cls_fqn):
                try:
                    from ..cluster import _import_class
                    actor_cls = _import_class(actor_cls_fqn)
                    self.state.sharded_types[entity_type] = actor_cls
                    if entity_type not in self.state.local_entities:
                        self.state.local_entities[entity_type] = {}
                    log.debug(f"Registered sharded type from wire: {entity_type}")

                    # Send ACK back
                    ack = ShardedTypeAck(request_id=request_id, entity_type=entity_type, success=True)
                    data = ActorCodec.encode(ack)
                    await self.state.mux.send(Send(ProtocolType.ACTOR, from_node, data))
                except Exception as e:
                    log.error(f"Failed to register sharded type {entity_type}: {e}")
                    ack = ShardedTypeAck(request_id=request_id, entity_type=entity_type, success=False)
                    data = ActorCodec.encode(ack)
                    await self.state.mux.send(Send(ProtocolType.ACTOR, from_node, data))

            case ShardedTypeAck(request_id, entity_type, success, error):
                pending = self.state.pending_registrations.get(request_id)
                if pending:
                    if success:
                        pending.received_acks.add(from_node)
                        log.debug(f"[ShardedTypeAck] {request_id}: ack from {from_node}, count={len(pending.received_acks)}/{pending.required_acks}")

                        # Check if we have enough acks
                        if len(pending.received_acks) >= pending.required_acks:
                            log.info(f"[ShardedTypeAck] Quorum reached for {request_id}: {len(pending.received_acks)}/{pending.required_acks} acks")
                            if pending.reply_future:
                                if not pending.reply_future.done():
                                    pending.reply_future.set_result(True)
                            self.state.pending_registrations.pop(request_id, None)
                    else:
                        log.warning(f"[ShardedTypeAck] Registration failed for {entity_type} on {from_node}: {error}")
                else:
                    log.debug(f"[ShardedTypeAck] Late ack for {request_id} (already completed)")

            case ReplicateWrite(entity_type, entity_id, payload_type, payload, coordinator_id, request_id, actor_cls_fqn):
                # Handle replicate write from coordinator
                try:
                    from ..cluster import deserialize_message, _import_class

                    # Get or create the entity
                    if entity_type not in self.state.sharded_types:
                        actor_cls = _import_class(actor_cls_fqn)
                        self.state.sharded_types[entity_type] = actor_cls
                        if entity_type not in self.state.local_entities:
                            self.state.local_entities[entity_type] = {}

                    # Deserialize payload
                    payload_dict = msgpack.unpackb(payload, raw=False)
                    message = deserialize_message(payload_type, payload_dict)

                    # Get or create entity actor
                    entities = self.state.local_entities.get(entity_type)
                    if entities is None:
                        raise ValueError(f"Entity type not registered: {entity_type}")

                    if entity_id not in entities:
                        actor_cls = self.state.sharded_types.get(entity_type)
                        if not actor_cls:
                            raise ValueError(f"Actor class not found for {entity_type}")
                        ref = await ctx.spawn(actor_cls, entity_id=entity_id)
                        entities[entity_id] = ref
                    else:
                        ref = entities[entity_id]

                    # Deliver message
                    await ref.send(message)
                    log.debug(f"[ReplicateWrite] Delivered to {entity_type}:{entity_id}")

                    # Send ack back
                    ack_msg = ReplicateWriteAck(
                        request_id=request_id,
                        entity_type=entity_type,
                        entity_id=entity_id,
                        node_id=self.state.node_id,
                        success=True,
                    )
                    if coordinator_id == self.state.node_id:
                        # Local coordinator - deliver directly
                        if self.state.replication_coordinator:
                            await self.state.replication_coordinator.send(ack_msg)
                    else:
                        # Remote coordinator - send via network
                        data = ActorCodec.encode(ack_msg)
                        await self.state.mux.send(Send(ProtocolType.ACTOR, coordinator_id, data))
                except Exception as e:
                    log.error(f"ReplicateWrite failed: {e}", exc_info=True)
                    ack_msg = ReplicateWriteAck(
                        request_id=request_id,
                        entity_type=entity_type,
                        entity_id=entity_id,
                        node_id=self.state.node_id,
                        success=False,
                        error=str(e),
                    )
                    log.info(f"[ReplicateWrite] Sending failure ack to coordinator {coordinator_id} for {request_id}")
                    if coordinator_id == self.state.node_id:
                        # Local coordinator - deliver directly
                        if self.state.replication_coordinator:
                            await self.state.replication_coordinator.send(ack_msg)
                    else:
                        # Remote coordinator - send via network
                        data = ActorCodec.encode(ack_msg)
                        await self.state.mux.send(Send(ProtocolType.ACTOR, coordinator_id, data))

            case ReplicateWriteAck(request_id, entity_type, entity_id, node_id, success, error):
                # Route to local ReplicationCoordinator
                log.info(f"[ReplicateWriteAck] Received ack: request_id={request_id}, from {node_id}, success={success}")
                if self.state.replication_coordinator:
                    await self.state.replication_coordinator.send(msg)
                else:
                    log.warning(f"[ReplicateWriteAck] No ReplicationCoordinator available")

            case EntitySend(entity_type, entity_id, payload_type, payload, actor_cls_fqn):
                # Handle entity send from remote node
                try:
                    from ..cluster import deserialize_message, _import_class

                    # Auto-register type if needed
                    if actor_cls_fqn and entity_type not in self.state.sharded_types:
                        actor_cls = _import_class(actor_cls_fqn)
                        self.state.sharded_types[entity_type] = actor_cls
                        self.state.local_entities[entity_type] = {}

                    # Get or create entity
                    entities = self.state.local_entities.get(entity_type)
                    if entities is None:
                        log.warning(f"[EntitySend] Unknown entity type: {entity_type}")
                        return

                    if entity_id not in entities:
                        actor_cls = self.state.sharded_types.get(entity_type)
                        if actor_cls:
                            ref = await ctx.spawn(actor_cls, entity_id=entity_id)
                            entities[entity_id] = ref
                        else:
                            log.warning(f"[EntitySend] No actor class for {entity_type}")
                            return
                    else:
                        ref = entities[entity_id]

                    # Deserialize and deliver
                    payload_dict = msgpack.unpackb(payload, raw=False)
                    message = deserialize_message(payload_type, payload_dict)
                    await ref.send(message)
                    log.debug(f"[EntitySend] Delivered to {entity_type}:{entity_id}")
                except Exception as e:
                    log.error(f"[EntitySend] Failed: {e}", exc_info=True)

            case EntityAskRequest(request_id, entity_type, entity_id, payload_type, payload, actor_cls_fqn):
                # Handle entity ask from remote node
                try:
                    from ..cluster import deserialize_message, _import_class

                    # Auto-register type if needed
                    if actor_cls_fqn and entity_type not in self.state.sharded_types:
                        actor_cls = _import_class(actor_cls_fqn)
                        self.state.sharded_types[entity_type] = actor_cls
                        self.state.local_entities[entity_type] = {}

                    # Get or create entity
                    entities = self.state.local_entities.get(entity_type)
                    if entities is None:
                        response = EntityAskResponse(
                            request_id=request_id,
                            success=False,
                            payload_type=None,
                            payload=f"Unknown entity type: {entity_type}".encode(),
                        )
                        data = ActorCodec.encode(response)
                        await self.state.mux.send(Send(ProtocolType.ACTOR, from_node, data))
                        return

                    if entity_id not in entities:
                        actor_cls = self.state.sharded_types.get(entity_type)
                        if actor_cls:
                            ref = await ctx.spawn(actor_cls, entity_id=entity_id)
                            entities[entity_id] = ref
                        else:
                            response = EntityAskResponse(
                                request_id=request_id,
                                success=False,
                                payload_type=None,
                                payload=f"No actor class for {entity_type}".encode(),
                            )
                            data = ActorCodec.encode(response)
                            await self.state.mux.send(Send(ProtocolType.ACTOR, from_node, data))
                            return
                    else:
                        ref = entities[entity_id]

                    # Deserialize, ask and respond
                    payload_dict = msgpack.unpackb(payload, raw=False)
                    message = deserialize_message(payload_type, payload_dict)
                    result = await ref.ask(message, timeout=self.state.config.ask_timeout)

                    result_type = f"{type(result).__module__}.{type(result).__qualname__}"
                    if hasattr(result, "__dict__"):
                        result_bytes = msgpack.packb(result.__dict__, use_bin_type=True)
                    else:
                        result_bytes = msgpack.packb(result, use_bin_type=True)

                    response = EntityAskResponse(
                        request_id=request_id,
                        success=True,
                        payload_type=result_type,
                        payload=result_bytes,
                    )
                    log.debug(f"[EntityAskRequest] Responded to {entity_type}:{entity_id}")
                except Exception as e:
                    log.error(f"[EntityAskRequest] Failed: {e}", exc_info=True)
                    response = EntityAskResponse(
                        request_id=request_id,
                        success=False,
                        payload_type=None,
                        payload=str(e).encode("utf-8"),
                    )

                data = ActorCodec.encode(response)
                await self.state.mux.send(Send(ProtocolType.ACTOR, from_node, data))

            case EntityAskResponse(request_id, success, payload_type, payload):
                # Handle entity ask response
                from ..remote_ref import RemoteActorError
                pending = self.state.pending_asks.pop(request_id, None)
                if pending and not pending.future.done():
                    if success:
                        try:
                            result = msgpack.unpackb(payload, raw=False)
                            pending.future.set_result(result)
                        except Exception as e:
                            pending.future.set_exception(e)
                    else:
                        error_msg = payload.decode("utf-8") if payload else "Unknown error"
                        pending.future.set_exception(
                            RemoteActorError(pending.target_actor, pending.target_node, error_msg)
                        )

            case SingletonSend(singleton_name, payload_type, payload, actor_cls_fqn):
                # Handle singleton send from remote node
                try:
                    from ..cluster import deserialize_message, _import_class

                    # Auto-register type if needed
                    if actor_cls_fqn and singleton_name not in self.state.singleton_types:
                        actor_cls = _import_class(actor_cls_fqn)
                        self.state.singleton_types[singleton_name] = actor_cls
                        # If we're the owner but haven't spawned yet, spawn now
                        owner = self.state.hash_ring.get_node(f"_singleton:{singleton_name}")
                        if owner == self.state.node_id and singleton_name not in self.state.local_singletons:
                            ref = await ctx.spawn(actor_cls)
                            self.state.local_singletons[singleton_name] = ref
                            self.state.singleton_locations[singleton_name] = self.state.node_id
                            log.info(f"Auto-spawned singleton {singleton_name}")

                    ref = self.state.local_singletons.get(singleton_name)
                    if ref:
                        payload_dict = msgpack.unpackb(payload, raw=False)
                        message = deserialize_message(payload_type, payload_dict)
                        await ref.send(message)
                        log.debug(f"[SingletonSend] Delivered to {singleton_name}")
                    else:
                        log.warning(f"[SingletonSend] Singleton {singleton_name} not found locally")
                except Exception as e:
                    log.error(f"[SingletonSend] Failed: {e}", exc_info=True)

            case SingletonAskRequest(request_id, singleton_name, payload_type, payload, actor_cls_fqn):
                # Handle singleton ask from remote node
                try:
                    from ..cluster import deserialize_message, _import_class

                    # Auto-register type if needed
                    if actor_cls_fqn and singleton_name not in self.state.singleton_types:
                        actor_cls = _import_class(actor_cls_fqn)
                        self.state.singleton_types[singleton_name] = actor_cls
                        # If we're the owner but haven't spawned yet, spawn now
                        owner = self.state.hash_ring.get_node(f"_singleton:{singleton_name}")
                        if owner == self.state.node_id and singleton_name not in self.state.local_singletons:
                            ref = await ctx.spawn(actor_cls)
                            self.state.local_singletons[singleton_name] = ref
                            self.state.singleton_locations[singleton_name] = self.state.node_id
                            log.info(f"Auto-spawned singleton {singleton_name}")

                    ref = self.state.local_singletons.get(singleton_name)
                    if ref:
                        payload_dict = msgpack.unpackb(payload, raw=False)
                        message = deserialize_message(payload_type, payload_dict)
                        result = await ref.ask(message, timeout=self.state.config.ask_timeout)

                        # Serialize result
                        result_bytes = msgpack.packb(result, use_bin_type=True)
                        response = SingletonAskResponse(
                            request_id=request_id,
                            success=True,
                            payload_type=type(result).__name__ if result is not None else None,
                            payload=result_bytes,
                        )
                    else:
                        response = SingletonAskResponse(
                            request_id=request_id,
                            success=False,
                            payload_type=None,
                            payload=f"Singleton {singleton_name} not found".encode(),
                        )

                    data = ActorCodec.encode(response)
                    await self.state.mux.send(Send(ProtocolType.ACTOR, from_node, data))
                except Exception as e:
                    log.error(f"[SingletonAskRequest] Failed: {e}", exc_info=True)
                    response = SingletonAskResponse(
                        request_id=request_id,
                        success=False,
                        payload_type=None,
                        payload=str(e).encode("utf-8"),
                    )
                    data = ActorCodec.encode(response)
                    await self.state.mux.send(Send(ProtocolType.ACTOR, from_node, data))

            case SingletonAskResponse(request_id, success, payload_type, payload):
                # Handle singleton ask response
                from ..remote_ref import RemoteActorError
                pending = self.state.pending_asks.pop(request_id, None)
                if pending and not pending.future.done():
                    if success:
                        try:
                            result = msgpack.unpackb(payload, raw=False)
                            pending.future.set_result(result)
                        except Exception as e:
                            pending.future.set_exception(e)
                    else:
                        error_msg = payload.decode("utf-8") if payload else "Unknown error"
                        pending.future.set_exception(
                            RemoteActorError(pending.target_actor, pending.target_node, error_msg)
                        )

            # =========================================================================
            # WAL-Based Replication Messages
            # =========================================================================

            case ReplicateWALEntry() as wal_msg:
                # Delegate to entity handler via @on decorator
                # This is processed by EntityHandlers.handle_replicate_wal_entry
                await self.handle_replicate_wal_entry(wal_msg, ctx)

            case ReplicateWALAck() as wal_ack:
                # Route to ReplicationCoordinator
                if self.state.replication_coordinator:
                    await self.state.replication_coordinator.send(wal_ack)
                else:
                    log.warning(f"[ReplicateWALAck] No ReplicationCoordinator available")

            case RequestCatchUp() as catch_up_req:
                # Delegate to entity handler
                await self.handle_request_catch_up(catch_up_req, ctx)

            case CatchUpEntries() as catch_up_entries:
                # Delegate to entity handler
                await self.handle_catch_up_entries(catch_up_entries, ctx)

            case _:
                log.warning(f"[{self.state.node_id}] Unhandled actor message type: {type(msg).__name__}: {msg}")

    def _deserialize_message(self, payload_type: str, payload_dict: dict) -> Any:
        """Deserialize a message from its type name and dict representation."""
        from ..cluster import deserialize_message
        return deserialize_message(payload_type, payload_dict)
