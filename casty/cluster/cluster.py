from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from casty import actor, Mailbox
from casty.actor import Behavior
from .. import logger
from casty.envelope import Envelope
from casty.protocols import System
from casty.remote import remote, Listen, Connect, Expose, Lookup
from casty.serializable import serializable

from .membership import membership_actor, MemberInfo, MemberState
from .swim import swim_actor
from .gossip import gossip_actor
from .messages import Join, SetLocalAddress, GetAliveMembers, GetAddress
from casty.actor import register_behavior, get_registered_actor
from .replication import replication_filter, leadership_filter
from .hash_ring import HashRing
from .shard import ShardCoordinator
from .sharded_ref import ShardedActorRef, ClusterShardResolver
from .states import states, StoreState, GetState

if TYPE_CHECKING:
    from casty.ref import ActorRef


@dataclass
class CreateActor:
    behavior: Behavior
    name: str


@serializable
@dataclass
class _RemoteCreateActor:
    behavior_name: str
    actor_name: str
    initial_state: bytes | None = None


@dataclass
class GetClusterAddress:
    pass


@dataclass
class WaitFor:
    nodes: int


type ClusterMessage = CreateActor | GetClusterAddress | WaitFor | _RemoteCreateActor


class _MembershipAdapter:
    def __init__(self, alive_members: dict[str, MemberInfo]):
        self._alive = set(alive_members.keys())

    def is_alive(self, node_id: str) -> bool:
        return node_id in self._alive


@actor
async def cluster(
    node_id: str,
    host: str = "127.0.0.1",
    port: int = 0,
    seeds: list[tuple[str, str]] | None = None,
    *,
    mailbox: Mailbox[ClusterMessage],
    system: System,
):
    remote_ref = await system.actor(remote(), name="remote")
    result = await remote_ref.ask(Listen(port=port, host=host))
    local_address = f"{host}:{result.address[1]}"

    states_ref = await system.actor(states(), name="states")

    initial_members: dict[str, MemberInfo] = {}
    for seed_node_id, seed_address in (seeds or []):
        initial_members[seed_node_id] = MemberInfo(
            node_id=seed_node_id,
            address=seed_address,
            state=MemberState.ALIVE,
            incarnation=0,
        )

    membership_ref = await system.actor(
        membership_actor(node_id, initial_members),
        name="membership"
    )
    await membership_ref.send(SetLocalAddress(local_address))

    swim_ref = await system.actor(swim_actor(node_id), name="swim")
    gossip_ref = await system.actor(gossip_actor(node_id), name="gossip")

    await remote_ref.ask(Expose(ref=swim_ref, name="swim"))
    await remote_ref.ask(Expose(ref=gossip_ref, name="gossip"))
    await remote_ref.ask(Expose(ref=membership_ref, name="membership"))
    await remote_ref.ask(Expose(ref=mailbox.ref(), name="cluster"))
    await remote_ref.ask(Expose(ref=states_ref, name="states"))

    for seed_node_id, seed_address in (seeds or []):
        asyncio.create_task(_connect_to_seed(
            remote_ref, membership_ref, node_id, local_address, seed_address
        ))

    async for msg, ctx in mailbox:
        match msg:
            case GetClusterAddress():
                await ctx.reply(local_address)

            case CreateActor(behavior, name):
                func_name = behavior.func.__name__
                replication_config = behavior.__replication_config__
                replicas = replication_config.replicas if replication_config else None

                logger.debug("CreateActor received", actor=name, replicas=replicas)

                logger.debug("Getting alive members")
                members = await membership_ref.ask(GetAliveMembers())
                logger.debug("Got alive members", count=len(members))

                local_ref = await system.actor(name=name)
                if local_ref:
                    logger.debug("Actor already exists locally", actor=name)
                    await ctx.reply(local_ref)
                    continue

                if replication_config:
                    effective_replicas = replicas if replicas else 1
                    hash_ring = HashRing()
                    hash_ring.add_node(node_id)
                    for member_id in members:
                        hash_ring.add_node(member_id)

                    membership_adapter = _MembershipAdapter(members)
                    membership_adapter._alive.add(node_id)

                    shard_coordinator = ShardCoordinator(
                        node_id=node_id,
                        hash_ring=hash_ring,
                        membership=membership_adapter,
                    )

                    responsible_nodes = shard_coordinator.get_responsible_nodes(name, effective_replicas)
                    is_leader = shard_coordinator.is_leader(name, effective_replicas)

                    logger.debug("Shard calculated", actor=name, is_leader=is_leader, responsible=responsible_nodes)

                    if is_leader:
                        logger.debug("I am leader, creating actor", actor=name)
                        register_behavior(func_name, behavior)

                        other_replicas = shard_coordinator.get_replica_ids(name, effective_replicas)
                        states_refs: list[ActorRef] = []

                        for resp_node in other_replicas:
                            if resp_node in members:
                                member_info = members[resp_node]
                                host_part, port_str = member_info.address.rsplit(":", 1)
                                try:
                                    await remote_ref.ask(Connect(host=host_part, port=int(port_str)), timeout=2.0)
                                    result = await remote_ref.ask(Lookup("states", peer=member_info.address), timeout=2.0)
                                    if result and result.ref:
                                        states_refs.append(result.ref)
                                except (TimeoutError, Exception):
                                    pass

                        write_quorum = replication_config.write_quorum if replication_config else 1
                        quorum_count = 1
                        match write_quorum:
                            case "async":
                                quorum_count = 0
                            case "all":
                                quorum_count = len(states_refs)
                            case "quorum":
                                quorum_count = (len(states_refs) // 2) + 1
                            case int(n):
                                quorum_count = min(n, len(states_refs))

                        rep_filter = replication_filter(states_refs, write_quorum=quorum_count)
                        lead_filter = leadership_filter(shard_coordinator, name, effective_replicas)

                        logger.debug("Spawning actor locally", actor=name)
                        local_ref = await system.actor(behavior, name=name, filters=[lead_filter, rep_filter])
                        logger.debug("Actor spawned, exposing", actor=name)
                        await remote_ref.ask(Expose(ref=local_ref, name=name))
                        logger.debug("Actor exposed", actor=name)

                        all_members = dict(members)
                        all_members[node_id] = MemberInfo(
                            node_id=node_id,
                            address=local_address,
                            state=MemberState.ALIVE,
                            incarnation=0,
                        )

                        resolver = ClusterShardResolver(
                            shard_coordinator=shard_coordinator,
                            members=all_members,
                            replicas=effective_replicas,
                        )

                        async def send_fn(address: str, actor_id: str, msg: Any) -> None:
                            logger.debug("ShardedRef send_fn called", target=address, actor=actor_id)
                            host, port_str = address.rsplit(":", 1)
                            if address == local_address:
                                await local_ref.send(msg)
                                return
                            try:
                                await remote_ref.ask(Connect(host=host, port=int(port_str)), timeout=2.0)
                                result = await remote_ref.ask(Lookup(actor_id, peer=address), timeout=2.0)
                                if result and result.ref:
                                    await result.ref.send(msg)
                            except (TimeoutError, Exception) as e:
                                logger.warn("ShardedRef send_fn failed", error=str(e))

                        async def ask_fn(address: str, actor_id: str, msg: Any) -> Any:
                            logger.debug("ShardedRef ask_fn called", target=address, actor=actor_id)
                            host, port_str = address.rsplit(":", 1)
                            if address == local_address:
                                return await local_ref.ask(msg)
                            try:
                                await remote_ref.ask(Connect(host=host, port=int(port_str)), timeout=2.0)
                                result = await remote_ref.ask(Lookup(actor_id, peer=address), timeout=2.0)
                                if result and result.ref:
                                    return await result.ref.ask(msg)
                            except (TimeoutError, Exception) as e:
                                logger.warn("ShardedRef ask_fn failed", error=str(e))
                            return None

                        sharded_ref = ShardedActorRef(
                            actor_id=name,
                            resolver=resolver,
                            send_fn=send_fn,
                            ask_fn=ask_fn,
                            known_leader_id=node_id,
                        )
                        logger.debug("Replying with sharded ref (leader)", actor=name)
                        await ctx.reply(sharded_ref)
                    else:
                        leader_id = shard_coordinator.get_leader_id(name, effective_replicas)
                        leader_address = members[leader_id].address if leader_id in members else None
                        logger.debug("I am NOT leader, forwarding to leader", actor=name, leader=leader_id, leader_address=leader_address)

                        if leader_address:
                            logger.debug("Preparing to forward to leader", leader=leader_id, remote_ref_ok=remote_ref is not None)
                            register_behavior(func_name, behavior)
                            host, port_str = leader_address.rsplit(":", 1)
                            initial_state_bytes = None
                            if behavior.state_initial is not None:
                                from casty.serializable import serialize
                                logger.debug("Serializing initial state")
                                initial_state_bytes = serialize(behavior.state_initial)
                            logger.debug("About to connect to leader", leader=leader_id, host=host, port=port_str)
                            try:
                                logger.debug("Connecting to leader", leader=leader_id)
                                await remote_ref.ask(Connect(host=host, port=int(port_str)), timeout=2.0)
                                logger.debug("Looking up cluster on leader", leader=leader_id)
                                result = await remote_ref.ask(Lookup("cluster", peer=leader_address), timeout=2.0)
                                if result and result.ref:
                                    logger.debug("Sending _RemoteCreateActor to leader", actor=name, leader=leader_id)
                                    await result.ref.ask(
                                        _RemoteCreateActor(
                                            behavior_name=func_name,
                                            actor_name=name,
                                            initial_state=initial_state_bytes,
                                        ),
                                        timeout=5.0
                                    )
                                    logger.debug("_RemoteCreateActor completed", actor=name)
                            except (TimeoutError, Exception) as e:
                                logger.warn("Failed to forward to leader", actor=name, error=str(e))

                        all_members = dict(members)
                        all_members[node_id] = MemberInfo(
                            node_id=node_id,
                            address=local_address,
                            state=MemberState.ALIVE,
                            incarnation=0,
                        )

                        resolver = ClusterShardResolver(
                            shard_coordinator=shard_coordinator,
                            members=all_members,
                            replicas=effective_replicas,
                        )

                        async def send_fn(address: str, actor_id: str, msg: Any) -> None:
                            host, port_str = address.rsplit(":", 1)
                            try:
                                await remote_ref.ask(Connect(host=host, port=int(port_str)), timeout=2.0)
                                result = await remote_ref.ask(Lookup(actor_id, peer=address), timeout=2.0)
                                if result and result.ref:
                                    await result.ref.send(msg)
                            except (TimeoutError, Exception):
                                pass

                        async def ask_fn(address: str, actor_id: str, msg: Any) -> Any:
                            host, port_str = address.rsplit(":", 1)
                            try:
                                await remote_ref.ask(Connect(host=host, port=int(port_str)), timeout=2.0)
                                result = await remote_ref.ask(Lookup(actor_id, peer=address), timeout=2.0)
                                if result and result.ref:
                                    return await result.ref.ask(msg)
                            except (TimeoutError, Exception):
                                pass
                            return None

                        sharded_ref = ShardedActorRef(
                            actor_id=name,
                            resolver=resolver,
                            send_fn=send_fn,
                            ask_fn=ask_fn,
                            known_leader_id=leader_id,
                        )
                        logger.debug("Replying with sharded ref (non-leader)", actor=name)
                        await ctx.reply(sharded_ref)
                else:
                    logger.debug("Non-replicated actor, looking up or creating", actor=name)
                    for _, member_info in members.items():
                        try:
                            result = await remote_ref.ask(Lookup(name, peer=member_info.address), timeout=2.0)
                            if result and result.ref:
                                await ctx.reply(result.ref)
                                break
                        except (TimeoutError, Exception):
                            continue
                    else:
                        ref = await system.actor(behavior, name=name)
                        await remote_ref.ask(Expose(ref=ref, name=name))
                        await ctx.reply(ref)

            case _RemoteCreateActor(behavior_name, actor_name, initial_state):
                logger.debug("_RemoteCreateActor received", actor=actor_name, behavior=behavior_name)
                behavior = get_registered_actor(behavior_name)
                if behavior:
                    if initial_state is not None:
                        from casty.serializable import deserialize
                        state = deserialize(initial_state)
                        configured_behavior = behavior(state)
                    else:
                        configured_behavior = behavior()

                    local_ref = await system.actor(name=actor_name)
                    if local_ref:
                        logger.debug("Actor already exists for _RemoteCreateActor", actor=actor_name)
                        await ctx.reply(True)
                        continue

                    logger.debug("Creating actor directly for _RemoteCreateActor", actor=actor_name)
                    local_ref = await system.actor(configured_behavior, name=actor_name)
                    await remote_ref.ask(Expose(ref=local_ref, name=actor_name))
                    logger.debug("Actor created and exposed for _RemoteCreateActor", actor=actor_name)
                    await ctx.reply(True)
                else:
                    logger.warn("Behavior not found for _RemoteCreateActor", behavior=behavior_name)
                    await ctx.reply(None)

            case WaitFor(nodes):
                members = await membership_ref.ask(GetAliveMembers())
                count = len(members)

                if count >= nodes:
                    await ctx.reply(True)
                else:
                    await mailbox.schedule(
                        WaitFor(nodes),
                        delay=0.2,
                        sender=ctx.sender,
                    )


async def _connect_to_seed(
    remote_ref: "ActorRef",
    membership_ref: "ActorRef",
    node_id: str,
    local_address: str,
    seed_address: str,
):
    try:
        host, port_str = seed_address.rsplit(":", 1)
        await remote_ref.ask(Connect(host=host, port=int(port_str)), timeout=2.0)

        result = await remote_ref.ask(Lookup("membership", peer=seed_address), timeout=2.0)
        if result.ref:
            response = await result.ref.ask(Join(node_id=node_id, address=local_address), timeout=2.0)
            match response:
                case Join(node_id=resp_node_id, address=resp_address):
                    await membership_ref.send(Join(node_id=resp_node_id, address=resp_address))
    except (TimeoutError, Exception):
        pass
