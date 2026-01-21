from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

from casty import actor, Mailbox
from casty.actor import Behavior
from casty.protocols import System
from casty.remote import remote, Listen, Connect, Expose, Lookup

from .membership import membership_actor, MemberInfo, MemberState
from .swim import swim_actor
from .gossip import gossip_actor
from .messages import Join, SetLocalAddress, GetAliveMembers

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

    # 4. Expose for other nodes to find
    await remote_ref.ask(Expose(ref=swim_ref, name="swim"))
    await remote_ref.ask(Expose(ref=gossip_ref, name="gossip"))
    await remote_ref.ask(Expose(ref=membership_ref, name="membership"))

    # 5. Connect to seeds
    for seed_node_id, seed_address in (seeds or []):
        asyncio.create_task(_connect_to_seed(
            remote_ref, membership_ref, node_id, local_address, seed_address
        ))

    # 6. Process cluster messages
    async for msg, ctx in mailbox:
        match msg:
            case GetClusterAddress():
                await ctx.reply(local_address)

            case CreateActor(behavior, name):
                ref = await system.actor(behavior, name=name)
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
        await remote_ref.ask(Connect(host=host, port=int(port_str)))

        result = await remote_ref.ask(Lookup("membership", peer=seed_address))
        if result.ref:
            response = await result.ref.ask(Join(node_id=node_id, address=local_address))
            if isinstance(response, Join):
                await membership_ref.send(Join(node_id=response.node_id, address=response.address))
    except Exception:
        pass
