from __future__ import annotations

import logging
from typing import Any, cast

from casty.actor import Behavior, Behaviors
from casty.cluster.state import ClusterState, MemberStatus, NodeAddress
from casty.cluster.topology import SubscribeTopology, TopologySnapshot
from casty.ref import ActorRef
from casty.core.address import ActorAddress
from casty.remote.tcp_transport import RemoteTransport
from casty.cluster.envelope import ShardEnvelope, entity_shard
from casty.cluster.coordinator import (
    CoordinatorMsg,
    GetShardLocation,
    ShardLocation,
)


def shard_proxy_behavior(
    *,
    coordinator: ActorRef[CoordinatorMsg],
    local_region: ActorRef[Any],
    self_node: NodeAddress,
    shard_name: str,
    num_shards: int,
    remote_transport: RemoteTransport | None,
    system_name: str,
    logger: logging.Logger | None = None,
    topology_ref: ActorRef[Any] | None = None,
) -> Behavior[Any]:
    """Proxy actor that routes ShardEnvelopes to the correct region.

    - Computes shard_id from entity_id
    - Caches shard->node mappings
    - Buffers messages while waiting for coordinator response
    """
    log = logger or logging.getLogger(f"casty.shard_proxy.{system_name}")

    def active(
        shard_cache: dict[int, NodeAddress],
        buffer: dict[int, tuple[ShardEnvelope[Any], ...]],
    ) -> Behavior[Any]:
        async def receive(ctx: Any, msg: Any) -> Any:
            match msg:
                case TopologySnapshot() as snapshot:
                    evicted = {
                        sid: n
                        for sid, n in shard_cache.items()
                        if n not in snapshot.unreachable
                    }
                    if len(evicted) < len(shard_cache):
                        log.info(
                            "Evicted %d cached shards (unreachable nodes)",
                            len(shard_cache) - len(evicted),
                        )
                    return active(evicted, buffer)

                case ShardEnvelope():
                    envelope = cast(ShardEnvelope[Any], msg)
                    shard_id = entity_shard(envelope.entity_id, num_shards)

                    if shard_id in shard_cache:
                        node = shard_cache[shard_id]
                        if node == self_node:
                            local_region.tell(envelope)
                        elif remote_transport is not None:
                            remote_addr = ActorAddress(
                                system=system_name,
                                path=f"/_region-{shard_name}",
                                host=node.host,
                                port=node.port,
                            )
                            remote_ref: ActorRef[Any] = remote_transport.make_ref(
                                remote_addr
                            )
                            remote_ref.tell(envelope)
                        return Behaviors.same()

                    existing_buf = buffer.get(shard_id, ())
                    new_buf = (*existing_buf, envelope)
                    new_buffer = {**buffer, shard_id: new_buf}
                    if not existing_buf:
                        log.debug(
                            "Shard %d: requesting location (entity=%s)",
                            shard_id,
                            envelope.entity_id,
                        )
                        coordinator.tell(
                            GetShardLocation(shard_id=shard_id, reply_to=ctx.self)
                        )
                    return active(shard_cache, new_buffer)

                case ShardLocation(shard_id=sid, node=node):
                    new_cache = {**shard_cache, sid: node}
                    buffered = buffer.get(sid, ())
                    log.debug(
                        "Shard %d -> %s:%d (flushed %d)",
                        sid,
                        node.host,
                        node.port,
                        len(buffered),
                    )
                    new_buffer = {k: v for k, v in buffer.items() if k != sid}
                    for envelope in buffered:
                        if node == self_node:
                            local_region.tell(envelope)
                        elif remote_transport is not None:
                            remote_addr = ActorAddress(
                                system=system_name,
                                path=f"/_region-{shard_name}",
                                host=node.host,
                                port=node.port,
                            )
                            remote_ref: ActorRef[Any] = remote_transport.make_ref(
                                remote_addr
                            )
                            remote_ref.tell(envelope)
                    return active(new_cache, new_buffer)

                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    if topology_ref is not None:

        async def setup(ctx: Any) -> Any:
            topology_ref.tell(SubscribeTopology(reply_to=ctx.self))
            return active({}, {})

        return Behaviors.setup(setup)

    return active({}, {})


def broadcast_proxy_behavior(
    *,
    local_ref: ActorRef[Any],
    self_node: NodeAddress,
    bcast_name: str,
    remote_transport: RemoteTransport | None,
    system_name: str,
    topology_ref: ActorRef[Any] | None = None,
) -> Behavior[Any]:
    """Proxy that fans out every message to all cluster members.

    Receives ``ClusterState`` or ``TopologySnapshot`` to track membership.
    Any other message is forwarded to every ``up`` member — locally via
    ``local_ref.tell()`` and remotely via ``remote_transport.make_ref()``.
    """

    def active(members: frozenset[NodeAddress]) -> Behavior[Any]:
        async def receive(ctx: Any, msg: Any) -> Behavior[Any]:
            match msg:
                case TopologySnapshot() as snapshot:
                    up = frozenset(
                        m.address
                        for m in snapshot.members
                        if m.status == MemberStatus.up
                    )
                    return active(up)

                case ClusterState() as state:
                    up = frozenset(
                        m.address for m in state.members if m.status == MemberStatus.up
                    )
                    return active(up)
                case _:
                    for node in members:
                        if node == self_node:
                            local_ref.tell(msg)
                        elif remote_transport is not None:
                            addr = ActorAddress(
                                system=system_name,
                                path=f"/_bcast-{bcast_name}",
                                host=node.host,
                                port=node.port,
                            )
                            remote_ref: ActorRef[Any] = remote_transport.make_ref(addr)
                            remote_ref.tell(msg)
                    return Behaviors.same()

        return Behaviors.receive(receive)

    if topology_ref is not None:

        async def setup(ctx: Any) -> Any:
            topology_ref.tell(SubscribeTopology(reply_to=ctx.self))
            return active(frozenset({self_node}))

        return Behaviors.setup(setup)

    return active(frozenset({self_node}))
