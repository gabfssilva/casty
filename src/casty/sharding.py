# src/casty/sharding.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, cast, overload

from casty.actor import Behavior, Behaviors, ShardedBehavior
from casty.cluster_state import NodeAddress
from casty.mailbox import Mailbox
from casty.ref import ActorRef
from casty.system import ActorSystem
from casty._shard_coordinator_actor import (
    CoordinatorMsg,
    GetShardLocation,
    LeastShardStrategy,
    ShardLocation,
    shard_coordinator_actor,
)
@dataclass(frozen=True)
class ShardEnvelope[M]:
    entity_id: str
    message: M


# ---------------------------------------------------------------------------
# Shard proxy behavior — routes ShardEnvelope to the right region
# ---------------------------------------------------------------------------


def _shard_proxy_behavior(
    *,
    coordinator: ActorRef[CoordinatorMsg],
    local_region: ActorRef[Any],
    self_node: NodeAddress,
    shard_name: str,
    num_shards: int,
    peer_regions: dict[tuple[str, NodeAddress], ActorRef[Any]],
) -> Behavior[Any]:
    """Proxy actor that routes ShardEnvelopes to the correct region.

    - Computes shard_id from entity_id
    - Caches shard→node mappings
    - Buffers messages while waiting for coordinator response
    """

    async def setup(_ctx: Any) -> Any:
        shard_cache: dict[int, NodeAddress] = {}
        buffer: dict[int, list[ShardEnvelope[Any]]] = {}

        def _route(envelope: ShardEnvelope[Any], node: NodeAddress) -> None:
            if node == self_node:
                local_region.tell(envelope)
            else:
                peer = peer_regions.get((shard_name, node))
                if peer is not None:
                    peer.tell(envelope)

        async def receive(ctx: Any, msg: Any) -> Any:
            if isinstance(msg, ShardEnvelope):
                envelope = cast(ShardEnvelope[Any], msg)
                shard_id = hash(envelope.entity_id) % num_shards

                if shard_id in shard_cache:
                    _route(envelope, shard_cache[shard_id])
                    return Behaviors.same()

                # Buffer and ask coordinator
                buf = buffer.setdefault(shard_id, [])
                buf.append(envelope)
                if len(buf) == 1:
                    # First message for this shard — ask coordinator
                    coordinator.tell(
                        GetShardLocation(shard_id=shard_id, reply_to=ctx.self)
                    )
                return Behaviors.same()

            if isinstance(msg, ShardLocation):
                location = msg
                shard_cache[location.shard_id] = location.node
                # Drain buffered messages
                buffered = buffer.pop(location.shard_id, [])
                for envelope in buffered:
                    _route(envelope, location.node)
                return Behaviors.same()

            return Behaviors.same()

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)


# ---------------------------------------------------------------------------
# ClusteredActorSystem
# ---------------------------------------------------------------------------


class ClusteredActorSystem(ActorSystem):
    _peer_regions: ClassVar[dict[tuple[str, NodeAddress], ActorRef[Any]]] = {}
    _coordinators: ClassVar[dict[str, ActorRef[CoordinatorMsg]]] = {}

    def __init__(
        self,
        *,
        name: str,
        host: str,
        port: int,
        seed_nodes: list[tuple[str, int]] | None = None,
    ) -> None:
        super().__init__(name=name)
        self._host = host
        self._port = port
        self._seed_nodes = seed_nodes or []
        self._self_node = NodeAddress(host=host, port=port)
        self._shard_names: list[str] = []

    @property
    def self_node(self) -> NodeAddress:
        return self._self_node

    async def __aenter__(self) -> ClusteredActorSystem:
        await super().__aenter__()
        return self

    @overload
    def spawn[M](
        self, behavior: ShardedBehavior[M], name: str
    ) -> ActorRef[ShardEnvelope[M]]: ...

    @overload
    def spawn[M](
        self,
        behavior: Behavior[M],
        name: str,
        *,
        mailbox: Mailbox[M] | None = None,
    ) -> ActorRef[M]: ...

    def spawn[M](
        self,
        behavior: ShardedBehavior[M] | Behavior[M],
        name: str,
        *,
        mailbox: Mailbox[M] | None = None,
    ) -> ActorRef[ShardEnvelope[M]] | ActorRef[M]:
        if isinstance(behavior, ShardedBehavior):
            return self._spawn_sharded(behavior, name)
        return super().spawn(behavior, name, mailbox=mailbox)

    def _spawn_sharded[M](
        self, sharded: ShardedBehavior[M], name: str
    ) -> ActorRef[ShardEnvelope[M]]:
        from casty._shard_region_actor import shard_region_actor

        # Build available nodes from self + seed_nodes
        available_nodes = frozenset(
            {self._self_node}
            | {NodeAddress(host=h, port=p) for h, p in self._seed_nodes}
        )

        # Coordinator: shared across all systems for this shard group
        if name not in ClusteredActorSystem._coordinators:
            coord_ref = super().spawn(
                shard_coordinator_actor(
                    strategy=LeastShardStrategy(),
                    available_nodes=available_nodes,
                ),
                f"_coord-{name}",
            )
            ClusteredActorSystem._coordinators[name] = coord_ref
        coordinator = ClusteredActorSystem._coordinators[name]

        # Local shard region
        region_ref = super().spawn(
            shard_region_actor(
                self_node=self._self_node,
                coordinator=coordinator,
                entity_factory=sharded.entity_factory,
                num_shards=sharded.num_shards,
            ),
            f"_region-{name}",
        )

        # Register for peer discovery
        ClusteredActorSystem._peer_regions[(name, self._self_node)] = region_ref
        self._shard_names.append(name)

        # Proxy — the ref users interact with
        proxy_ref: ActorRef[Any] = super().spawn(
            _shard_proxy_behavior(
                coordinator=coordinator,
                local_region=region_ref,
                self_node=self._self_node,
                shard_name=name,
                num_shards=sharded.num_shards,
                peer_regions=ClusteredActorSystem._peer_regions,
            ),
            name,
        )

        return cast(ActorRef[ShardEnvelope[M]], proxy_ref)

    async def shutdown(self) -> None:
        # Cleanup peer region entries for this node
        for shard_name in self._shard_names:
            ClusteredActorSystem._peer_regions.pop(
                (shard_name, self._self_node), None
            )
            # If no more peers for this shard group, remove coordinator
            has_peers = any(
                k[0] == shard_name for k in ClusteredActorSystem._peer_regions
            )
            if not has_peers:
                ClusteredActorSystem._coordinators.pop(shard_name, None)
        self._shard_names.clear()
        await super().shutdown()
