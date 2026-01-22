from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from casty import actor, Mailbox
from casty.actor import Behavior
from casty.actor_config import Routing
from casty.protocols import System
from casty.remote import remote, Listen, Connect, Expose, Lookup
from casty.remote.session import SendReplicate

from .membership import membership_actor, MemberInfo, MemberState
from .swim import swim_actor
from .gossip import gossip_actor
from .messages import Join, SetLocalAddress, GetAliveMembers, GetResponsibleNodes, GetAddress
from .replica_manager import ReplicaManager
from .replicated_ref import ReplicatedActorRef
from casty.actor import register_behavior
from .replication import Replicator, ReplicationConfig, replication_filter

if TYPE_CHECKING:
    from casty.ref import ActorRef


@dataclass
class CreateActor:
    behavior: Behavior
    name: str


@dataclass
class GetClusterAddress:
    pass


@dataclass
class WaitFor:
    nodes: int


type ClusterMessage = CreateActor | GetClusterAddress | WaitFor


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
    # 1. Create remote and listen
    remote_ref = await system.actor(remote(), name="remote")
    result = await remote_ref.ask(Listen(port=port, host=host))
    local_address = f"{host}:{result.address[1]}"

    # 2. Create membership with seeds as initial members
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

    # 3. Create SWIM and gossip
    swim_ref = await system.actor(swim_actor(node_id), name="swim")
    gossip_ref = await system.actor(gossip_actor(node_id), name="gossip")

    # 4. Create replica manager
    replica_manager = ReplicaManager()

    # 5. Expose for other nodes to find
    await remote_ref.ask(Expose(ref=swim_ref, name="swim"))
    await remote_ref.ask(Expose(ref=gossip_ref, name="gossip"))
    await remote_ref.ask(Expose(ref=membership_ref, name="membership"))

    # 6. Connect to seeds
    for seed_node_id, seed_address in (seeds or []):
        asyncio.create_task(_connect_to_seed(
            remote_ref, membership_ref, node_id, local_address, seed_address
        ))

    # 7. Process cluster messages
    async for msg, ctx in mailbox:
        match msg:
            case GetClusterAddress():
                await ctx.reply(local_address)

            case CreateActor(behavior, name):
                func_name = behavior.func.__name__
                actor_id = f"{func_name}/{name}"
                replication_config = behavior.__replication_config__
                replicated = replication_config.replicated if replication_config else None

                # Check if already exists locally
                local_ref = await system.actor(name=actor_id)
                if local_ref:
                    info = replica_manager.get(actor_id)
                    if info and replication_config:
                        await ctx.reply(ReplicatedActorRef(
                            actor_id=actor_id,
                            manager=replica_manager,
                            node_refs={node_id: local_ref},
                            routing=replication_config.routing,
                            local_node=node_id,
                        ))
                    else:
                        await ctx.reply(local_ref)
                    continue

                members = await membership_ref.ask(GetAliveMembers())

                if replicated and replicated > 1:
                    # Get responsible nodes via HashRing in membership
                    responsible_nodes = await membership_ref.ask(
                        GetResponsibleNodes(actor_id, replicated)
                    )

                    if node_id in responsible_nodes:
                        # Register behavior for lazy replication on other nodes
                        register_behavior(func_name, behavior)

                        # Get other replica nodes (excluding self)
                        other_replicas = [n for n in responsible_nodes if n != node_id]

                        # Create send function that uses remote module
                        sessions: dict[str, ActorRef] = {}

                        async def send_to_node(target_node: str, msg: Any) -> None:
                            if target_node not in sessions:
                                address = await membership_ref.ask(GetAddress(target_node))
                                if address:
                                    host, port_str = address.rsplit(":", 1)
                                    try:
                                        await remote_ref.ask(Connect(host=host, port=int(port_str)), timeout=2.0)
                                        result = await remote_ref.ask(Lookup(actor_id, peer=address), timeout=2.0)
                                        if result and result.ref and hasattr(result.ref, '_session'):
                                            sessions[target_node] = result.ref._session
                                    except (TimeoutError, Exception):
                                        pass

                            session = sessions.get(target_node)
                            if session:
                                await session.send(SendReplicate(
                                    actor_id=actor_id,
                                    snapshot=msg.snapshot,
                                    version=msg.version,
                                ))

                        # Create replicator and filter with fire-and-forget replication
                        rep_config = ReplicationConfig(
                            factor=replicated,
                            write_quorum=1,  # No ack required for now
                            routing=Routing.LEADER,
                        )
                        replicator = Replicator(
                            actor_id=actor_id,
                            config=rep_config,
                            replica_nodes=other_replicas,
                            send_fn=send_to_node,
                            node_id=node_id,
                        )
                        rep_filter = replication_filter(replicator)

                        # Create local replica with replication filter
                        local_ref = await system.actor(behavior, name=name, filters=[rep_filter])
                        await remote_ref.ask(Expose(ref=local_ref, name=actor_id))

                        # Build refs for all responsible nodes (replicas created lazily)
                        node_refs: dict[str, Any] = {node_id: local_ref}

                        for resp_node in responsible_nodes:
                            if resp_node != node_id and resp_node in members:
                                member_info = members[resp_node]
                                host, port_str = member_info.address.rsplit(":", 1)
                                try:
                                    await remote_ref.ask(Connect(host=host, port=int(port_str)), timeout=2.0)
                                    result = await remote_ref.ask(Lookup(actor_id, peer=member_info.address), timeout=2.0)
                                    if result and result.ref:
                                        node_refs[resp_node] = result.ref
                                except (TimeoutError, Exception):
                                    pass

                        replica_manager.register(actor_id, list(node_refs.keys()), node_id)
                        await ctx.reply(ReplicatedActorRef(
                            actor_id=actor_id,
                            manager=replica_manager,
                            node_refs=node_refs,
                            routing=replication_config.routing,
                            local_node=node_id,
                        ))
                    else:
                        # Not responsible - check if already exists on responsible node
                        found_ref = None
                        for resp_node in responsible_nodes:
                            if resp_node in members:
                                member_info = members[resp_node]
                                host, port_str = member_info.address.rsplit(":", 1)
                                try:
                                    await remote_ref.ask(Connect(host=host, port=int(port_str)), timeout=2.0)
                                    result = await remote_ref.ask(Lookup(
                                        actor_id,
                                        peer=member_info.address,
                                    ), timeout=2.0)
                                    if result and result.ref:
                                        found_ref = result.ref
                                        break
                                except (TimeoutError, Exception):
                                    continue

                        if found_ref:
                            await ctx.reply(found_ref)
                        else:
                            # Create locally with full behavior (includes initial state)
                            # Replication will sync to responsible nodes
                            local_ref = await system.actor(behavior, name=name)
                            await remote_ref.ask(Expose(ref=local_ref, name=actor_id))
                            await ctx.reply(local_ref)
                else:
                    # Non-replicated: global lookup then create
                    for _, member_info in members.items():
                        try:
                            result = await remote_ref.ask(Lookup(actor_id, peer=member_info.address), timeout=2.0)
                            if result and result.ref:
                                await ctx.reply(result.ref)
                                break
                        except (TimeoutError, Exception):
                            continue
                    else:
                        ref = await system.actor(behavior, name=name)
                        await remote_ref.ask(Expose(ref=ref, name=actor_id))
                        await ctx.reply(ref)

            case WaitFor(nodes):
                members = await membership_ref.ask(GetAliveMembers())
                count = len(members) + 1  # +1 for self

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
            if isinstance(response, Join):
                await membership_ref.send(Join(node_id=response.node_id, address=response.address))
    except (TimeoutError, Exception):
        pass
