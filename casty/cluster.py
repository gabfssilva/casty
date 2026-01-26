from __future__ import annotations

import asyncio
import bisect
import hashlib
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Coroutine, Protocol, TYPE_CHECKING

from .core import actor, Mailbox, Behavior, ActorRef, Reply, MessageStream, Filter, System, register_behavior, get_registered_actor
from .serializable import serializable, serialize, deserialize
from .state import Stateful
from . import logger

if TYPE_CHECKING:
    from .core import Envelope

REMOTE_ACTOR_ID = "remote"
MEMBERSHIP_ACTOR_ID = "membership"
SWIM_NAME = "swim"
GOSSIP_NAME = "gossip"


class HashRing:
    def __init__(self, virtual_nodes: int = 150) -> None:
        self._ring: dict[int, str] = {}
        self._sorted_keys: list[int] = []
        self._virtual_nodes = virtual_nodes

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node: str) -> None:
        for i in range(self._virtual_nodes):
            key = self._hash(f"{node}:{i}")
            if key not in self._ring:
                self._ring[key] = node
                bisect.insort(self._sorted_keys, key)
        logger.debug("HashRing node added", node=node, total_nodes=len(self.nodes))

    def remove_node(self, node: str) -> None:
        for i in range(self._virtual_nodes):
            key = self._hash(f"{node}:{i}")
            if key in self._ring:
                del self._ring[key]
                idx = bisect.bisect_left(self._sorted_keys, key)
                if idx < len(self._sorted_keys) and self._sorted_keys[idx] == key:
                    self._sorted_keys.pop(idx)
        logger.debug("HashRing node removed", node=node, total_nodes=len(self.nodes))

    def get_node(self, actor_id: str) -> str:
        if not self._ring:
            raise RuntimeError("HashRing is empty")
        key = self._hash(actor_id)
        idx = bisect.bisect_left(self._sorted_keys, key)
        if idx == len(self._sorted_keys):
            idx = 0
        return self._ring[self._sorted_keys[idx]]

    @property
    def nodes(self) -> set[str]:
        return set(self._ring.values())

    def get_n_nodes(self, key: str, n: int) -> list[str]:
        if not self._ring or n <= 0:
            return []
        key_hash = self._hash(key)
        idx = bisect.bisect_left(self._sorted_keys, key_hash)
        if idx >= len(self._sorted_keys):
            idx = 0
        result: list[str] = []
        seen: set[str] = set()
        for i in range(len(self._sorted_keys)):
            pos = (idx + i) % len(self._sorted_keys)
            node = self._ring[self._sorted_keys[pos]]
            if node not in seen:
                result.append(node)
                seen.add(node)
            if len(result) == n:
                break
        return result

    def get_nodes(self, actor_id: str, n: int) -> list[str]:
        return self.get_n_nodes(actor_id, n)


class MemberState(Enum):
    ALIVE = "alive"
    DOWN = "down"


@dataclass
class MemberInfo:
    node_id: str
    address: str
    state: MemberState
    incarnation: int


from .messages import (
    MemberSnapshot, Ping, Ack, PingReq, PingReqAck,
    Join, SetLocalAddress, GetAliveMembers, GetAllMembers,
    GetResponsibleNodes, GetAddress, MergeMembership, MarkDown, MarkAlive,
    GetLeaderId, IsLeader, GetReplicaIds,
    SwimTick, ProbeTimeout, PingReqTimeout, IndirectProbeTimeout,
    GossipPut, GossipGet, GossipTick, GossipMessage,
    StoreState, StoreAck, StatesMessage,
    Subscribe, Unsubscribe, Forward, MembershipChanged,
)


type MembershipMessage = (
    Join | MergeMembership | MarkDown | MarkAlive |
    GetAliveMembers | GetAllMembers | GetResponsibleNodes |
    SetLocalAddress | GetAddress |
    GetLeaderId | IsLeader | GetReplicaIds |
    MembershipChanged
)


@actor
async def membership_actor(
    node_id: str,
    initial_members: dict[str, MemberInfo] | None = None,
    *,
    mailbox: Mailbox[MembershipMessage],
    system: System,
):
    logger.debug("membership_actor starting", node_id=node_id, initial_members=len(initial_members or {}))
    members = dict(initial_members or {})
    hash_ring = HashRing()
    hash_ring.add_node(node_id)
    local_address = ""

    for member_id, info in members.items():
        if info.state == MemberState.ALIVE:
            hash_ring.add_node(member_id)
            logger.debug("membership_actor initialized member", member_id=member_id, address=info.address)

    async def publish_membership():
        gossip_ref = await system.actor(name=GOSSIP_NAME)
        if gossip_ref:
            all_members = dict(members)
            if local_address:
                all_members[node_id] = MemberInfo(node_id=node_id, address=local_address, state=MemberState.ALIVE, incarnation=0)
            addresses = {m: info.address for m, info in all_members.items() if info.state == MemberState.ALIVE}
            await gossip_ref.send(GossipPut(key="membership", value=serialize(addresses), version=0))

    gossip_ref = await system.actor(name=GOSSIP_NAME)
    if gossip_ref:
        await gossip_ref.send(Subscribe(pattern="membership", subscriber=mailbox.ref()))
        logger.debug("membership_actor subscribed to gossip", node_id=node_id)

    async for msg, ctx in mailbox:
        logger.debug("membership_actor received", msg_type=type(msg).__name__)
        match msg:
            case SetLocalAddress(addr):
                local_address = addr
                logger.debug("membership_actor local address set", address=addr)

            case Join(joined_node_id, address):
                is_new = joined_node_id not in members
                members[joined_node_id] = MemberInfo(
                    node_id=joined_node_id,
                    address=address,
                    state=MemberState.ALIVE,
                    incarnation=0,
                )
                hash_ring.add_node(joined_node_id)
                if is_new:
                    logger.info("Member joined", member=joined_node_id, address=address)
                    if local_address:
                        await ctx.reply(Join(node_id=node_id, address=local_address))
                    await publish_membership()

            case MergeMembership(remote_members):
                logger.debug("membership_actor merging", count=len(remote_members))
                for snapshot in remote_members:
                    if snapshot.node_id == node_id:
                        continue
                    if snapshot.node_id not in members:
                        members[snapshot.node_id] = MemberInfo(
                            node_id=snapshot.node_id,
                            address=snapshot.address,
                            state=MemberState(snapshot.state),
                            incarnation=snapshot.incarnation,
                        )
                        if snapshot.state == "alive":
                            hash_ring.add_node(snapshot.node_id)
                        logger.debug("membership_actor new member from merge", member=snapshot.node_id, state=snapshot.state)
                    else:
                        local = members[snapshot.node_id]
                        if snapshot.incarnation > local.incarnation:
                            old_state = local.state
                            local.state = MemberState(snapshot.state)
                            local.incarnation = snapshot.incarnation
                            local.address = snapshot.address
                            logger.debug("membership_actor updated member", member=snapshot.node_id, old_state=old_state.value, new_state=snapshot.state)
                            match (local.state, old_state):
                                case (MemberState.DOWN, MemberState.ALIVE):
                                    hash_ring.remove_node(snapshot.node_id)
                                case (MemberState.ALIVE, MemberState.DOWN):
                                    hash_ring.add_node(snapshot.node_id)

            case MarkDown(target_node_id):
                if target_node_id in members:
                    member = members[target_node_id]
                    if member.state == MemberState.ALIVE:
                        member.state = MemberState.DOWN
                        member.incarnation += 1
                        hash_ring.remove_node(target_node_id)
                        logger.warn("Member marked down", member=target_node_id)
                        await publish_membership()

            case MarkAlive(target_node_id, address):
                if target_node_id in members:
                    member = members[target_node_id]
                    if member.state == MemberState.DOWN:
                        member.state = MemberState.ALIVE
                        member.incarnation += 1
                        member.address = address
                        hash_ring.add_node(target_node_id)
                        logger.info("Member recovered", member=target_node_id)
                        await publish_membership()
                else:
                    members[target_node_id] = MemberInfo(
                        node_id=target_node_id,
                        address=address,
                        state=MemberState.ALIVE,
                        incarnation=0,
                    )
                    hash_ring.add_node(target_node_id)
                    logger.info("New member discovered", member=target_node_id, address=address)
                    await publish_membership()

            case GetAliveMembers():
                result = {mid: info for mid, info in members.items() if info.state == MemberState.ALIVE}
                if local_address:
                    result[node_id] = MemberInfo(node_id=node_id, address=local_address, state=MemberState.ALIVE, incarnation=0)
                logger.debug("membership_actor GetAliveMembers", count=len(result))
                await ctx.reply(result)

            case GetAllMembers():
                result = dict(members)
                if local_address:
                    result[node_id] = MemberInfo(node_id=node_id, address=local_address, state=MemberState.ALIVE, incarnation=0)
                logger.debug("membership_actor GetAllMembers", count=len(result))
                await ctx.reply(result)

            case GetResponsibleNodes(actor_id=aid, count=cnt):
                try:
                    nodes = hash_ring.get_nodes(aid, cnt)
                    logger.debug("membership_actor GetResponsibleNodes", actor_id=aid, count=cnt, nodes=nodes)
                    await ctx.reply(nodes)
                except RuntimeError:
                    logger.warn("membership_actor GetResponsibleNodes failed - empty ring", actor_id=aid)
                    await ctx.reply([])

            case GetAddress(node_id=target_node_id):
                if target_node_id in members:
                    logger.debug("membership_actor GetAddress", target=target_node_id, address=members[target_node_id].address)
                    await ctx.reply(members[target_node_id].address)
                else:
                    logger.debug("membership_actor GetAddress not found", target=target_node_id)
                    await ctx.reply(None)

            case GetLeaderId(actor_id=aid, replicas=reps):
                nodes = hash_ring.get_n_nodes(aid, reps)
                alive = [n for n in nodes if n == node_id or (n in members and members[n].state == MemberState.ALIVE)]
                leader = alive[0] if alive else None
                logger.debug("membership_actor GetLeaderId", actor_id=aid, replicas=reps, leader=leader)
                await ctx.reply(leader)

            case IsLeader(actor_id=aid, replicas=reps):
                nodes = hash_ring.get_n_nodes(aid, reps)
                alive = [n for n in nodes if n == node_id or (n in members and members[n].state == MemberState.ALIVE)]
                leader = alive[0] if alive else None
                is_leader = leader == node_id
                logger.debug("membership_actor IsLeader", actor_id=aid, replicas=reps, is_leader=is_leader)
                await ctx.reply(is_leader)

            case GetReplicaIds(actor_id=aid, replicas=reps):
                nodes = hash_ring.get_n_nodes(aid, reps)
                alive = [n for n in nodes if n == node_id or (n in members and members[n].state == MemberState.ALIVE)]
                leader = alive[0] if alive else None
                replicas_list = [n for n in alive if n != leader]
                logger.debug("membership_actor GetReplicaIds", actor_id=aid, replicas=reps, replicas_list=replicas_list)
                await ctx.reply(replicas_list)

            case MembershipChanged(addresses=remote_addresses):
                added = 0
                for remote_node_id, address in remote_addresses.items():
                    if remote_node_id == node_id:
                        continue
                    if remote_node_id not in members:
                        members[remote_node_id] = MemberInfo(
                            node_id=remote_node_id,
                            address=address,
                            state=MemberState.ALIVE,
                            incarnation=0,
                        )
                        hash_ring.add_node(remote_node_id)
                        added += 1
                        logger.info("Member discovered via gossip", member=remote_node_id, address=address)
                if added > 0:
                    logger.debug("membership_actor MembershipChanged", added=added, total=len(members))


def to_snapshots(members: dict[str, MemberInfo]) -> list[MemberSnapshot]:
    return [MemberSnapshot(m.node_id, m.address, m.state.value, m.incarnation) for m in members.values()]


@dataclass
class _ProbeState:
    start_time: float
    cancel_timeout: Callable[[], Coroutine[Any, Any, None]] | None = None
    cancel_ping_req_timeout: Callable[[], Coroutine[Any, Any, None]] | None = None


@dataclass
class _PendingSend:
    address: str
    actor_name: str
    message: Any


@dataclass
class _IndirectProbeState:
    target: str
    requester: str
    requester_address: str | None
    reply_to: "ActorRef | None"
    cancel_timeout: Callable[[], Coroutine[Any, Any, None]] | None = None


from .messages import RemoteConnected, ConnectFailed, LookupResult


@actor
async def swim_actor(
    node_id: str,
    probe_interval: float = 2.0,
    probe_timeout: float = 5.0,
    ping_req_fanout: int = 3,
    *,
    mailbox: Mailbox[SwimTick | Ping | Ack | PingReq | PingReqAck | ProbeTimeout | PingReqTimeout | IndirectProbeTimeout | Reply],
    system: System,
):
    from .messages import Connect, Lookup

    logger.debug("swim_actor starting", node_id=node_id, probe_interval=probe_interval, probe_timeout=probe_timeout)

    pending_probes: dict[str, _ProbeState] = {}
    pending_connects: dict[str, list[_PendingSend]] = {}
    pending_lookups: dict[str, list[_PendingSend]] = {}
    indirect_probes: dict[str, _IndirectProbeState] = {}

    membership_ref = await system.actor(name=MEMBERSHIP_ACTOR_ID)
    remote_ref = await system.actor(name=REMOTE_ACTOR_ID)

    if membership_ref is None or remote_ref is None:
        logger.error("swim_actor failed to get required refs", membership_ref=membership_ref is not None, remote_ref=remote_ref is not None)
        return

    await mailbox.schedule(SwimTick(), every=probe_interval)
    logger.info("swim_actor started", node_id=node_id)

    async def get_member_snapshots() -> list[MemberSnapshot]:
        all_members = await membership_ref.ask(GetAllMembers())
        return to_snapshots(all_members)

    async def initiate_send(address: str, actor_name: str, message: Any) -> None:
        if remote_ref is None:
            return
        pending = _PendingSend(address=address, actor_name=actor_name, message=message)
        host, port = address.rsplit(":", 1)
        if address in pending_connects:
            pending_connects[address].append(pending)
        else:
            pending_connects[address] = [pending]
            await remote_ref.send(Connect(host=host, port=int(port)), sender=mailbox.ref())
        logger.debug("swim_actor queued send", address=address, actor=actor_name)

    async for msg, ctx in mailbox:
        match msg:
            case SwimTick():
                members = await membership_ref.ask(GetAliveMembers())
                other_members = [m for m in members if m != node_id]
                if not other_members:
                    logger.debug("swim_actor tick - no other members")
                    continue
                target = random.choice(other_members)
                target_info = members[target]
                cancel_fn = await ctx.schedule(ProbeTimeout(target), delay=probe_timeout)
                pending_probes[target] = _ProbeState(start_time=time.time(), cancel_timeout=cancel_fn)
                snapshots = await get_member_snapshots()
                logger.debug("swim_actor probing", target=target, address=target_info.address)
                await initiate_send(target_info.address, SWIM_NAME, Ping(sender=node_id, members=snapshots))

            case Ping(sender=ping_sender, members=remote_members):
                logger.debug("swim_actor received Ping", sender=ping_sender, members_count=len(remote_members))
                for m in remote_members:
                    if m.node_id == ping_sender and m.state == "alive":
                        await membership_ref.send(MarkAlive(ping_sender, m.address))
                        break
                await membership_ref.send(MergeMembership(remote_members))
                snapshots = await get_member_snapshots()
                ack = Ack(sender=node_id, members=snapshots)
                if ctx.reply_to is not None or ctx.sender is not None:
                    logger.debug("swim_actor replying Ack", to=ping_sender)
                    await ctx.reply(ack)
                else:
                    members = await membership_ref.ask(GetAliveMembers())
                    if ping_sender in members:
                        sender_info = members[ping_sender]
                        logger.debug("swim_actor sending Ack via initiate_send", to=ping_sender)
                        await initiate_send(sender_info.address, SWIM_NAME, ack)

            case Ack(sender=ack_sender, members=remote_members):
                logger.debug("swim_actor received Ack", sender=ack_sender)

                if ack_sender in indirect_probes:
                    state = indirect_probes.pop(ack_sender)
                    if state.cancel_timeout:
                        await state.cancel_timeout()
                    snapshots = await get_member_snapshots()
                    ack = PingReqAck(sender=node_id, target=ack_sender, success=True, members=snapshots)
                    if state.reply_to:
                        logger.debug("swim_actor sending PingReqAck success via reply", target=ack_sender, to=state.requester)
                        await state.reply_to.send(ack)
                    elif state.requester_address:
                        logger.debug("swim_actor sending PingReqAck success via initiate_send", target=ack_sender, to=state.requester)
                        await initiate_send(state.requester_address, SWIM_NAME, ack)

                if ack_sender in pending_probes:
                    probe_state = pending_probes.pop(ack_sender)
                    if probe_state.cancel_timeout:
                        await probe_state.cancel_timeout()
                    if probe_state.cancel_ping_req_timeout:
                        await probe_state.cancel_ping_req_timeout()
                    logger.debug("swim_actor probe succeeded", target=ack_sender)

                for m in remote_members:
                    if m.node_id == ack_sender and m.state == "alive":
                        await membership_ref.send(MarkAlive(ack_sender, m.address))
                        break
                await membership_ref.send(MergeMembership(remote_members))

            case ProbeTimeout(target):
                if target not in pending_probes:
                    logger.debug("swim_actor ProbeTimeout - already resolved", target=target)
                    continue
                logger.debug("swim_actor ProbeTimeout", target=target)
                members = await membership_ref.ask(GetAliveMembers())
                other_members = [m for m in members if m != node_id and m != target]
                if not other_members:
                    del pending_probes[target]
                    logger.warn("swim_actor marking node down (no probers)", target=target)
                    await membership_ref.send(MarkDown(target))
                    continue
                probers = random.sample(other_members, min(ping_req_fanout, len(other_members)))
                logger.debug("swim_actor sending PingReq", target=target, probers=probers)
                snapshots = await get_member_snapshots()
                for prober in probers:
                    prober_info = members[prober]
                    await initiate_send(prober_info.address, SWIM_NAME, PingReq(sender=node_id, target=target, members=snapshots))
                cancel_fn = await ctx.schedule(PingReqTimeout(target), delay=probe_timeout)
                pending_probes[target].cancel_ping_req_timeout = cancel_fn

            case PingReq(sender=req_sender, target=req_target, members=remote_members):
                logger.debug("swim_actor received PingReq", sender=req_sender, target=req_target)
                for m in remote_members:
                    if m.node_id == req_sender and m.state == "alive":
                        await membership_ref.send(MarkAlive(req_sender, m.address))
                        break
                await membership_ref.send(MergeMembership(remote_members))
                members = await membership_ref.ask(GetAliveMembers())

                req_sender_address = members[req_sender].address if req_sender in members else None

                if req_target in indirect_probes:
                    logger.debug("swim_actor already probing target for PingReq", target=req_target)
                    continue

                cancel_fn = await ctx.schedule(
                    IndirectProbeTimeout(target=req_target, requester=req_sender),
                    delay=probe_timeout
                )

                indirect_probes[req_target] = _IndirectProbeState(
                    target=req_target,
                    requester=req_sender,
                    requester_address=req_sender_address,
                    reply_to=ctx.reply_to or ctx.sender,
                    cancel_timeout=cancel_fn,
                )

                if req_target in members:
                    target_info = members[req_target]
                    snapshots = await get_member_snapshots()
                    logger.debug("swim_actor forwarding Ping for PingReq", target=req_target)
                    await initiate_send(target_info.address, SWIM_NAME, Ping(sender=node_id, members=snapshots))
                else:
                    logger.debug("swim_actor PingReq target not in members", target=req_target)
                    snapshots = await get_member_snapshots()
                    ack = PingReqAck(sender=node_id, target=req_target, success=False, members=snapshots)
                    state = indirect_probes.pop(req_target)
                    if state.cancel_timeout:
                        await state.cancel_timeout()
                    if state.reply_to:
                        await state.reply_to.send(ack)
                    elif req_sender_address:
                        await initiate_send(req_sender_address, SWIM_NAME, ack)

            case PingReqAck(sender=_, target=ack_target, success=success, members=remote_members):
                logger.debug("swim_actor received PingReqAck", target=ack_target, success=success)
                if success and ack_target in pending_probes:
                    probe_state = pending_probes.pop(ack_target)
                    if probe_state.cancel_ping_req_timeout:
                        await probe_state.cancel_ping_req_timeout()
                    logger.debug("swim_actor PingReq succeeded", target=ack_target)
                    for m in remote_members:
                        if m.node_id == ack_target and m.state == "alive":
                            await membership_ref.send(MarkAlive(ack_target, m.address))
                            break
                await membership_ref.send(MergeMembership(remote_members))

            case PingReqTimeout(target):
                if target in pending_probes:
                    pending_probes.pop(target)
                    logger.warn("swim_actor marking node down after ping-req timeout", target=target)
                    await membership_ref.send(MarkDown(target))
                else:
                    logger.debug("swim_actor PingReqTimeout - already resolved", target=target)

            case IndirectProbeTimeout(target=target, requester=requester):
                if target in indirect_probes:
                    state = indirect_probes.pop(target)
                    logger.debug("swim_actor IndirectProbeTimeout, sending failure", target=target, to=requester)
                    snapshots = await get_member_snapshots()
                    ack = PingReqAck(sender=node_id, target=target, success=False, members=snapshots)
                    if state.reply_to:
                        await state.reply_to.send(ack)
                    elif state.requester_address:
                        await initiate_send(state.requester_address, SWIM_NAME, ack)
                else:
                    logger.debug("swim_actor IndirectProbeTimeout - already resolved", target=target)

            case Reply(result=Ack() as ack_msg):
                ack_sender = ack_msg.sender
                remote_members = ack_msg.members
                logger.debug("swim_actor received Ack via Reply", sender=ack_sender)

                if ack_sender in indirect_probes:
                    state = indirect_probes.pop(ack_sender)
                    if state.cancel_timeout:
                        await state.cancel_timeout()
                    snapshots = await get_member_snapshots()
                    ping_req_ack = PingReqAck(sender=node_id, target=ack_sender, success=True, members=snapshots)
                    if state.reply_to:
                        logger.debug("swim_actor sending PingReqAck success via Reply path", target=ack_sender, to=state.requester)
                        await state.reply_to.send(ping_req_ack)
                    elif state.requester_address:
                        logger.debug("swim_actor sending PingReqAck success via initiate_send (Reply path)", target=ack_sender, to=state.requester)
                        await initiate_send(state.requester_address, SWIM_NAME, ping_req_ack)

                if ack_sender in pending_probes:
                    probe_state = pending_probes.pop(ack_sender)
                    if probe_state.cancel_timeout:
                        await probe_state.cancel_timeout()
                    if probe_state.cancel_ping_req_timeout:
                        await probe_state.cancel_ping_req_timeout()
                    logger.debug("swim_actor probe succeeded via Reply", target=ack_sender)

                for m in remote_members:
                    if m.node_id == ack_sender and m.state == "alive":
                        await membership_ref.send(MarkAlive(ack_sender, m.address))
                        break
                await membership_ref.send(MergeMembership(remote_members))

            case Reply(result=PingReqAck() as ack_msg):
                ack_target = ack_msg.target
                success = ack_msg.success
                remote_members = ack_msg.members
                logger.debug("swim_actor received PingReqAck via Reply", target=ack_target, success=success)
                if success and ack_target in pending_probes:
                    probe_state = pending_probes.pop(ack_target)
                    if probe_state.cancel_ping_req_timeout:
                        await probe_state.cancel_ping_req_timeout()
                    logger.debug("swim_actor PingReq succeeded via Reply", target=ack_target)
                    for m in remote_members:
                        if m.node_id == ack_target and m.state == "alive":
                            await membership_ref.send(MarkAlive(ack_target, m.address))
                            break
                await membership_ref.send(MergeMembership(remote_members))

            case Reply(result=RemoteConnected(peer_id=peer)):
                address = peer
                if address in pending_connects:
                    pending_list = pending_connects.pop(address)
                    for pending in pending_list:
                        if address in pending_lookups:
                            pending_lookups[address].append(pending)
                        else:
                            pending_lookups[address] = [pending]
                        await remote_ref.send(Lookup(pending.actor_name, peer=address), sender=mailbox.ref())
                    logger.debug("swim_actor RemoteConnected, sent lookups", peer=peer, count=len(pending_list))

            case Reply(result=ConnectFailed(reason=reason, peer=peer)):
                if peer and peer in pending_connects:
                    pending_connects.pop(peer)
                    logger.debug("swim_actor ConnectFailed, cleared pending", peer=peer, reason=reason)
                else:
                    logger.debug("swim_actor ConnectFailed", reason=reason)

            case Reply(result=LookupResult(ref=lookup_ref, peer=peer)) if lookup_ref is not None:
                if peer and peer in pending_lookups:
                    pending_list = pending_lookups.pop(peer)
                    for pending in pending_list:
                        await lookup_ref.send(pending.message, sender=mailbox.ref())
                    logger.debug("swim_actor LookupResult, sent messages", peer=peer, count=len(pending_list))

            case Reply(result=LookupResult(ref=None, error=err, peer=peer)):
                if peer and peer in pending_lookups:
                    pending_lookups.pop(peer)
                logger.debug("swim_actor LookupResult failed", error=err, peer=peer)

            case Reply():
                logger.debug("swim_actor received Reply")


@actor
async def gossip_actor(
    node_id: str,
    fanout: int = 3,
    tick_interval: float = 0.5,
    *,
    mailbox: Mailbox[GossipMessage | Subscribe | Unsubscribe | Reply],
    system: System,
):
    from .messages import Connect, Lookup

    logger.debug("gossip_actor starting", node_id=node_id, fanout=fanout, tick_interval=tick_interval)

    store: dict[str, tuple[bytes, int]] = {}
    subscribers: dict[str, list[ActorRef]] = {}
    pending_connects: set[str] = set()
    pending_lookups: set[str] = set()

    membership_ref = await system.actor(name=MEMBERSHIP_ACTOR_ID)
    remote_ref = await system.actor(name=REMOTE_ACTOR_ID)

    await mailbox.schedule(GossipTick(), every=tick_interval)
    logger.info("gossip_actor started", node_id=node_id)

    async for msg, ctx in mailbox:
        match msg:
            case Subscribe(pattern=pattern, subscriber=subscriber):
                if pattern not in subscribers:
                    subscribers[pattern] = []
                if subscriber not in subscribers[pattern]:
                    subscribers[pattern].append(subscriber)
                    logger.debug("gossip_actor subscribed", pattern=pattern, subscriber=subscriber.actor_id)

            case Unsubscribe(pattern=pattern, subscriber=subscriber):
                if pattern in subscribers:
                    subscribers[pattern] = [s for s in subscribers[pattern] if s != subscriber]
                    logger.debug("gossip_actor unsubscribed", pattern=pattern)

            case GossipTick():
                if not store or membership_ref is None or remote_ref is None:
                    continue
                members = await membership_ref.ask(GetAliveMembers())
                other_members = [m for m in members if m != node_id]
                if not other_members:
                    continue
                targets = random.sample(other_members, min(fanout, len(other_members)))
                logger.debug("gossip_actor spreading to targets", targets=targets)
                for t in targets:
                    addr = members[t].address
                    if addr not in pending_connects and addr not in pending_lookups:
                        pending_connects.add(addr)
                        host, port = addr.rsplit(":", 1)
                        await remote_ref.send(Connect(host=host, port=int(port)), sender=mailbox.ref())

            case GossipPut(key, value, version):
                current = store.get(key)
                current_version = current[1] if current else 0
                updated = False
                if version == 0:
                    new_version = current_version + 1
                    store[key] = (value, new_version)
                    logger.debug("gossip_actor put (new)", key=key, version=new_version)
                    updated = True
                elif version > current_version:
                    store[key] = (value, version)
                    logger.debug("gossip_actor put (replicated)", key=key, version=version)
                    updated = True
                else:
                    logger.debug("gossip_actor put (stale, ignored)", key=key, version=version, current=current_version)

                if updated:
                    for pattern, subs in list(subscribers.items()):
                        if pattern == key or pattern == "*":
                            addresses = deserialize(value) if key == "membership" else {}
                            logger.debug("gossip_actor notifying subscribers", pattern=pattern, count=len(subs))
                            for sub in subs:
                                await sub.send(MembershipChanged(
                                    actor_id=key,
                                    leader_id=None,
                                    replica_nodes=[],
                                    addresses=addresses,
                                ))

            case GossipGet(key):
                entry = store.get(key)
                logger.debug("gossip_actor get", key=key, found=entry is not None)
                await ctx.reply(entry[0] if entry else None)

            case Reply(result=RemoteConnected(peer_id=peer)):
                address = peer
                if address in pending_connects:
                    pending_connects.remove(address)
                    pending_lookups.add(address)
                    await remote_ref.send(Lookup(GOSSIP_NAME, peer=address), sender=mailbox.ref())
                    logger.debug("gossip_actor RemoteConnected, sent lookup", peer=peer)

            case Reply(result=ConnectFailed(reason=reason, peer=peer)):
                if peer and peer in pending_connects:
                    pending_connects.remove(peer)
                    logger.debug("gossip_actor ConnectFailed, cleared pending", peer=peer, reason=reason)
                else:
                    logger.debug("gossip_actor ConnectFailed", reason=reason)

            case Reply(result=LookupResult(ref=lookup_ref, peer=peer)) if lookup_ref is not None:
                if peer and peer in pending_lookups:
                    pending_lookups.remove(peer)
                    logger.debug("gossip_actor LookupResult, sending store", peer=peer, keys=list(store.keys()))
                    for key, (value, version) in store.items():
                        await lookup_ref.send(GossipPut(key, value, version))

            case Reply(result=LookupResult(ref=None, error=err, peer=peer)):
                if peer and peer in pending_lookups:
                    pending_lookups.remove(peer)
                logger.debug("gossip_actor LookupResult failed", error=err, peer=peer)

            case Reply():
                logger.debug("gossip_actor received Reply")


class ShardResolver(Protocol):
    async def resolve_address(self, node_id: str) -> str: ...
    async def get_leader_id(self, actor_id: str) -> str: ...


@dataclass
class MembershipShardResolver:
    membership_ref: ActorRef
    members: dict[str, MemberInfo]
    replicas: int

    async def resolve_address(self, node_id: str) -> str:
        if node_id in self.members:
            return self.members[node_id].address
        address = await self.membership_ref.ask(GetAddress(node_id))
        if address:
            return address
        raise RuntimeError(f"Unknown node: {node_id}")

    async def get_leader_id(self, actor_id: str) -> str:
        return await self.membership_ref.ask(GetLeaderId(actor_id, self.replicas))


@dataclass
class ShardedActorRef[M](ActorRef[M]):
    actor_id: str
    resolver: ShardResolver
    send_fn: Callable[[str, str, M], Awaitable[None]]
    ask_fn: Callable[[str, str, M, float], Awaitable[Any]]
    known_leader_id: str | None = None

    async def _get_target_address(self) -> str:
        if self.known_leader_id is None:
            self.known_leader_id = await self.resolver.get_leader_id(self.actor_id)
        return await self.resolver.resolve_address(self.known_leader_id)

    def _invalidate_leader(self) -> None:
        self.known_leader_id = None

    async def send(self, msg: M, *, sender: "ActorRef[Any] | None" = None) -> None:
        for attempt in range(3):
            try:
                address = await self._get_target_address()
                await self.send_fn(address, self.actor_id, msg)
                return
            except (TimeoutError, ConnectionError, OSError) as e:
                logger.debug("ShardedActorRef send failed, retrying", actor_id=self.actor_id, attempt=attempt, error=str(e))
                self._invalidate_leader()
                if attempt == 2:
                    raise

    async def send_envelope(self, envelope: "Envelope[M]") -> None:
        await self.send(envelope.payload, sender=envelope.sender)

    async def ask[R](self, msg: M, timeout: float = 10.0) -> R:
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                address = await self._get_target_address()
                return await self.ask_fn(address, self.actor_id, msg, timeout)
            except (TimeoutError, ConnectionError, OSError) as e:
                logger.debug("ShardedActorRef ask failed, retrying", actor_id=self.actor_id, attempt=attempt, error=str(e))
                self._invalidate_leader()
                last_error = e
        raise last_error or TimeoutError("All retry attempts failed")

    def __rshift__(self, msg: M) -> Awaitable[None]:
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        return self.ask(msg)


@dataclass
class ClusteredActorRef[M](ActorRef[M]):
    actor_id: str
    _clustered_ref: ActorRef = field(repr=False)

    async def send(self, msg: M, *, sender: ActorRef[Any] | None = None) -> None:
        await self._clustered_ref.send(msg, sender=sender)

    async def send_envelope(self, envelope: "Envelope[M]") -> None:
        await self._clustered_ref.send_envelope(envelope)

    async def ask[R](self, msg: M, timeout: float = 10.0) -> R:
        return await self._clustered_ref.ask(msg, timeout=timeout)

    def __rshift__(self, msg: M) -> Awaitable[None]:
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        return self.ask(msg)


@dataclass
class _ClusteredState:
    actor_id: str
    behavior: Behavior
    initial_state_bytes: bytes | None = None
    local_ref: ActorRef | None = None
    leader_id: str | None = None
    replica_nodes: list[str] = field(default_factory=list)
    node_addresses: dict[str, str] = field(default_factory=dict)
    pending: list[Any] = field(default_factory=list)
    ready: bool = False


@dataclass
class _PendingForward:
    envelope: Any
    leader_addr: str


@actor
async def clustered_actor(
    actor_id: str,
    behavior: Behavior,
    replicas: int = 1,
    *,
    mailbox: Mailbox[MembershipChanged | Forward | Reply | Any],
    system: System,
):
    from .messages import Expose, Connect, Lookup

    logger.debug("clustered_actor starting", actor_id=actor_id, replicas=replicas)

    initial_state_bytes = None
    if behavior.state_initial is not None:
        initial_state_bytes = serialize(behavior.state_initial)

    state = _ClusteredState(actor_id=actor_id, behavior=behavior, initial_state_bytes=initial_state_bytes)
    node_id = system.node_id

    pending_forward_connects: dict[str, list[_PendingForward]] = {}
    pending_forward_lookups: dict[str, list[_PendingForward]] = {}
    leader_refs: dict[str, ActorRef] = {}

    gossip_ref = await system.actor(name=GOSSIP_NAME)
    remote_ref = await system.actor(name=REMOTE_ACTOR_ID)
    membership_ref = await system.actor(name=MEMBERSHIP_ACTOR_ID)

    async def forward_to_leader(envelope: Any, leader_addr: str) -> None:
        if leader_addr in leader_refs:
            await leader_refs[leader_addr].send(envelope.payload, sender=envelope.sender)
            return
        pending = _PendingForward(envelope=envelope, leader_addr=leader_addr)
        if leader_addr in pending_forward_connects or leader_addr in pending_forward_lookups:
            if leader_addr in pending_forward_connects:
                pending_forward_connects[leader_addr].append(pending)
            else:
                pending_forward_lookups[leader_addr].append(pending)
        else:
            pending_forward_connects[leader_addr] = [pending]
            host, port = leader_addr.rsplit(":", 1)
            await remote_ref.send(Connect(host=host, port=int(port)), sender=mailbox.ref())
            logger.debug("clustered_actor initiating connect to leader", actor_id=actor_id, leader_addr=leader_addr)

    if gossip_ref:
        await gossip_ref.send(Subscribe(pattern="membership", subscriber=mailbox.ref()))

    if membership_ref:
        members = await membership_ref.ask(GetAliveMembers())
        responsible = await membership_ref.ask(GetResponsibleNodes(actor_id=actor_id, count=replicas))
        leader = await membership_ref.ask(GetLeaderId(actor_id=actor_id, replicas=replicas))

        state.leader_id = leader
        state.replica_nodes = responsible
        state.node_addresses = {m: info.address for m, info in members.items()}
        logger.debug("clustered_actor initialized", actor_id=actor_id, leader=leader, responsible=responsible)

        if leader == node_id:
            logger.info("clustered_actor is leader", actor_id=actor_id)
            state.local_ref = await system.actor(behavior, name=actor_id)
            if remote_ref and state.local_ref:
                await remote_ref.send(Expose(ref=state.local_ref, name=actor_id))
            state.ready = True
            for env in state.pending:
                await state.local_ref.send_envelope(env)
            state.pending.clear()
        else:
            logger.debug("clustered_actor is not leader", actor_id=actor_id, leader=leader)

    async for msg, ctx in mailbox:
        logger.debug("clustered_actor received", actor_id=actor_id, msg_type=type(msg).__name__)
        match msg:
            case MembershipChanged(actor_id=aid, leader_id=lid, replica_nodes=rn, addresses=addrs) if aid == actor_id or aid == "membership":
                old_leader = state.leader_id
                if lid:
                    state.leader_id = lid
                if rn:
                    state.replica_nodes = rn
                state.node_addresses.update(addrs)
                logger.debug("clustered_actor membership changed", actor_id=actor_id, old_leader=old_leader, new_leader=state.leader_id)

                if state.leader_id == node_id and old_leader != node_id and not state.local_ref:
                    logger.info("clustered_actor became leader", actor_id=actor_id)
                    state.local_ref = await system.actor(behavior, name=actor_id)
                    if remote_ref and state.local_ref:
                        await remote_ref.send(Expose(ref=state.local_ref, name=actor_id))
                    state.ready = True
                    if state.pending:
                        logger.debug("clustered_actor flushing pending messages", actor_id=actor_id, count=len(state.pending))
                    for env in state.pending:
                        await state.local_ref.send_envelope(env)
                    state.pending.clear()
                elif state.leader_id and state.leader_id != node_id and state.leader_id in state.node_addresses and state.pending:
                    leader_addr = state.node_addresses[state.leader_id]
                    if remote_ref:
                        logger.debug("clustered_actor forwarding pending to new leader", actor_id=actor_id, leader=state.leader_id, count=len(state.pending))
                        for env in state.pending:
                            await forward_to_leader(env, leader_addr)
                        state.pending.clear()

            case Forward(payload=payload, original_sender_id=_):
                if state.leader_id == node_id and state.ready and state.local_ref:
                    decoded = deserialize(payload)
                    logger.debug("clustered_actor forwarding to local ref", actor_id=actor_id)
                    await state.local_ref.send(decoded)
                else:
                    logger.debug("clustered_actor ignoring forward (not leader)", actor_id=actor_id)

            case _ if state.leader_id == node_id:
                if state.ready and state.local_ref:
                    logger.debug("clustered_actor delivering to local actor", actor_id=actor_id, msg_type=type(msg).__name__)
                    await state.local_ref.send(msg, sender=ctx.sender)
                else:
                    logger.debug("clustered_actor queueing message (not ready)", actor_id=actor_id, msg_type=type(msg).__name__)
                    from .core import Envelope
                    state.pending.append(Envelope(payload=msg, sender=ctx.sender))

            case Reply(result=RemoteConnected(peer_id=peer)):
                address = peer
                if address in pending_forward_connects:
                    pending_list = pending_forward_connects.pop(address)
                    pending_forward_lookups[address] = pending_list
                    await remote_ref.send(Lookup(
                        actor_id,
                        peer=address,
                        ensure=True,
                        behavior=behavior.func.__name__,
                        initial_state=state.initial_state_bytes
                    ), sender=mailbox.ref())
                    logger.debug("clustered_actor RemoteConnected, sent lookup", actor_id=actor_id, peer=peer)

            case Reply(result=ConnectFailed(reason=reason, peer=peer)):
                logger.debug("clustered_actor ConnectFailed", actor_id=actor_id, reason=reason, peer=peer)
                if peer and peer in pending_forward_connects:
                    pending_list = pending_forward_connects.pop(peer)
                    for pending in pending_list:
                        state.pending.append(pending.envelope)
                else:
                    for pending_list in pending_forward_connects.values():
                        for pending in pending_list:
                            state.pending.append(pending.envelope)
                    pending_forward_connects.clear()

            case Reply(result=LookupResult(ref=lookup_ref, peer=peer)) if lookup_ref is not None:
                if peer and peer in pending_forward_lookups:
                    pending_list = pending_forward_lookups.pop(peer)
                    leader_refs[peer] = lookup_ref
                    logger.debug("clustered_actor LookupResult, forwarding messages", actor_id=actor_id, peer=peer, count=len(pending_list))
                    for pending in pending_list:
                        await lookup_ref.send(pending.envelope.payload, sender=pending.envelope.sender)

            case Reply(result=LookupResult(ref=None, error=err, peer=peer)):
                if peer and peer in pending_forward_lookups:
                    pending_list = pending_forward_lookups.pop(peer)
                    for pending in pending_list:
                        state.pending.append(pending.envelope)
                logger.debug("clustered_actor LookupResult failed", actor_id=actor_id, error=err, peer=peer)

            case Reply():
                logger.debug("clustered_actor received Reply")

            case _:
                if state.leader_id and state.leader_id in state.node_addresses:
                    leader_addr = state.node_addresses[state.leader_id]
                    if remote_ref:
                        from .core import Envelope
                        logger.debug("clustered_actor forwarding to leader", actor_id=actor_id, leader=state.leader_id, msg_type=type(msg).__name__)
                        env = Envelope(payload=msg, sender=ctx.sender)
                        await forward_to_leader(env, leader_addr)
                else:
                    logger.debug("clustered_actor queueing (leader unknown)", actor_id=actor_id, leader=state.leader_id)
                    from .core import Envelope
                    state.pending.append(Envelope(payload=msg, sender=ctx.sender))


def leadership_filter[M](
    membership_ref: ActorRef,
    actor_id: str,
    replicas: int,
) -> Filter[M]:
    async def apply(_state: Any, inner: MessageStream[M]) -> MessageStream[M]:
        async for msg, ctx in inner:
            is_leader = await membership_ref.ask(IsLeader(actor_id=actor_id, replicas=replicas))
            if not is_leader:
                continue
            yield msg, ctx
    return apply


class ReplicationQuorumError(Exception):
    pass


def replication_filter[M](
    states_refs: list[ActorRef],
    write_quorum: int = 1,
) -> Filter[M]:
    from .state import State

    async def apply(state: Any, inner: MessageStream[M]) -> MessageStream[M]:
        async for msg, ctx in inner:
            yield msg, ctx

            if states_refs and state is not None:
                if isinstance(state, State):
                    snapshot = {
                        "value": serialize(state.value),
                        "version": state.version,
                    }
                elif isinstance(state, Stateful):
                    snapshot = state.snapshot()
                elif hasattr(state, 'snapshot'):
                    snapshot = state.snapshot()
                else:
                    snapshot = vars(state) if hasattr(state, '__dict__') else {}

                actor_id = ctx.self_id
                logger.debug("replication_filter storing state", actor_id=actor_id, version=snapshot.get("version"))
                tasks = [states_ref.ask(StoreState(actor_id, snapshot)) for states_ref in states_refs]

                done = 0
                for coro in asyncio.as_completed(tasks):
                    try:
                        ack = await coro
                        if isinstance(ack, StoreAck) and ack.success:
                            done += 1
                            if done >= write_quorum:
                                break
                    except Exception:
                        pass

                if done < write_quorum:
                    raise ReplicationQuorumError(f"Failed to replicate {actor_id}: got {done}/{write_quorum} acks")
    return apply


@dataclass
class GetClusterAddress:
    pass


@dataclass
class WaitFor:
    nodes: int


type ClusterMessage = GetClusterAddress | WaitFor


@actor
async def cluster_actor(
    node_id: str,
    host: str = "127.0.0.1",
    port: int = 0,
    seeds: list[tuple[str, str]] | None = None,
    probe_interval: float = 2.0,
    probe_timeout: float = 5.0,
    *,
    mailbox: Mailbox[ClusterMessage],
    system: System,
):
    from .remote import remote_actor, Listen, Expose
    from .state import states

    logger.debug("cluster_actor starting", node_id=node_id, host=host, port=port, seeds=len(seeds or []))

    remote_ref = await system.actor(remote_actor(), name="remote")
    result = await remote_ref.ask(Listen(port=port, host=host))
    local_address = f"{host}:{result.address[1]}"
    logger.info("cluster_actor listening", node_id=node_id, address=local_address)

    states_ref = await system.actor(states(), name="states")

    initial_members: dict[str, MemberInfo] = {}
    for seed_node_id, seed_address in (seeds or []):
        initial_members[seed_node_id] = MemberInfo(
            node_id=seed_node_id,
            address=seed_address,
            state=MemberState.ALIVE,
            incarnation=0,
        )

    membership_ref = await system.actor(membership_actor(node_id, initial_members), name="membership")
    await membership_ref.send(SetLocalAddress(local_address))

    swim_ref = await system.actor(swim_actor(node_id, probe_interval, probe_timeout), name="swim")
    gossip_ref = await system.actor(gossip_actor(node_id), name="gossip")

    await remote_ref.ask(Expose(ref=swim_ref, name="swim"))
    await remote_ref.ask(Expose(ref=gossip_ref, name="gossip"))
    await remote_ref.ask(Expose(ref=membership_ref, name="membership"))
    await remote_ref.ask(Expose(ref=mailbox.ref(), name="cluster"))
    await remote_ref.ask(Expose(ref=states_ref, name="states"))
    logger.debug("cluster_actor actors exposed", node_id=node_id)

    for seed_node_id, seed_address in (seeds or []):
        logger.debug("cluster_actor connecting to seed", node_id=node_id, seed=seed_node_id, address=seed_address)
        asyncio.create_task(_connect_to_seed(remote_ref, membership_ref, node_id, local_address, seed_address))

    logger.info("cluster_actor ready", node_id=node_id)

    async for msg, ctx in mailbox:
        match msg:
            case GetClusterAddress():
                logger.debug("cluster_actor GetClusterAddress", address=local_address)
                await ctx.reply(local_address)

            case WaitFor(nodes):
                members = await membership_ref.ask(GetAliveMembers())
                logger.debug("cluster_actor WaitFor", requested=nodes, current=len(members))
                if len(members) >= nodes:
                    await ctx.reply(True)
                else:
                    await mailbox.schedule(WaitFor(nodes), delay=0.2, sender=ctx.sender)


async def _connect_to_seed(remote_ref: ActorRef, membership_ref: ActorRef, node_id: str, local_address: str, seed_address: str):
    from .messages import Connect, Lookup

    logger.debug("connecting to seed", node_id=node_id, seed=seed_address)
    try:
        host, port_str = seed_address.rsplit(":", 1)
        await remote_ref.ask(Connect(host=host, port=int(port_str)), timeout=2.0)
        result = await remote_ref.ask(Lookup("membership", peer=seed_address), timeout=2.0)
        if result.ref:
            response = await result.ref.ask(Join(node_id=node_id, address=local_address), timeout=2.0)
            match response:
                case Join(node_id=resp_node_id, address=resp_address):
                    logger.info("joined cluster via seed", node_id=node_id, seed=resp_node_id)
                    await membership_ref.send(Join(node_id=resp_node_id, address=resp_address))
        else:
            logger.warn("seed lookup failed", node_id=node_id, seed=seed_address)
    except TimeoutError:
        logger.warn("seed connection timeout", node_id=node_id, seed=seed_address)
    except Exception as e:
        logger.error("seed connection failed", node_id=node_id, seed=seed_address, error=str(e))


class ClusteredActorSystem(System):
    def __init__(
        self,
        node_id: str,
        host: str = "127.0.0.1",
        port: int = 0,
        start_timeout: float = 10.0,
        seeds: list[tuple[str, str]] | None = None,
        debug_filter: Any = None,
    ) -> None:
        from .system import LocalActorSystem
        self._system = LocalActorSystem(node_id=node_id, debug_filter=debug_filter)
        self._node_id = node_id
        self._host = host
        self._port = port
        self._seeds = seeds or []
        self._cluster_ref: ActorRef | None = None
        self._address: str | None = None
        self._start_timeout = start_timeout

    @property
    def node_id(self) -> str:
        return self._node_id

    async def address(self) -> str:
        if self._address:
            return self._address
        return f"{self._host}:{self._port}"

    async def start(self) -> None:
        from .messages import Lookup

        logger.info("starting clustered system", host=self._host, port=self._port)
        async with asyncio.timeout(self._start_timeout):
            self._cluster_ref = await self._system.actor(
                cluster_actor(self._node_id, self._host, self._port, self._seeds),
                name="cluster"
            )
            while True:
                remote_ref = await self._system.actor(name=REMOTE_ACTOR_ID)
                if remote_ref:
                    break
                await asyncio.sleep(0.01)
            self._address = await self._cluster_ref.ask(GetClusterAddress())
            logger.info("clustered system started", address=self._address)

    async def actor[M](
        self,
        behavior: Behavior | None = None,
        *,
        name: str,
        filters: list[Filter] | None = None,
        node_id: str | None = None,
        replicas: int | None = None,
        write_quorum: int | None = None,
    ) -> ActorRef[M] | None:
        from .messages import Lookup

        if behavior is None:
            if node_id:
                logger.debug("ClusteredActorSystem looking up remote actor", name=name, node_id=node_id)
                return await self._lookup_remote(name, node_id)
            return await self._system.actor(name=name)

        logger.debug("ClusteredActorSystem creating actor", name=name)

        if self._cluster_ref is None:
            raise RuntimeError("ClusteredActorSystem not started")

        replication_config = behavior.__replication_config__
        if replication_config:
            effective_replicas = replication_config.replicas or 1
            func_name = behavior.func.__name__
            register_behavior(func_name, behavior)
            logger.debug("ClusteredActorSystem creating clustered actor", name=name, replicas=effective_replicas)

            membership_ref = await self._system.actor(name=MEMBERSHIP_ACTOR_ID)
            remote_ref = await self._system.actor(name=REMOTE_ACTOR_ID)

            if not membership_ref or not remote_ref:
                logger.error("ClusteredActorSystem cluster not ready")
                raise RuntimeError("Cluster not ready")

            is_leader = await membership_ref.ask(IsLeader(actor_id=name, replicas=effective_replicas))
            members = await membership_ref.ask(GetAliveMembers())
            logger.debug("ClusteredActorSystem leadership check", name=name, is_leader=is_leader)

            initial_state_bytes = None
            if behavior.state_initial is not None:
                initial_state_bytes = serialize(behavior.state_initial)

            if is_leader:
                logger.info("ClusteredActorSystem spawning as leader", name=name)

                states_refs: list[ActorRef] = []

                local_states = await self._system.actor(name="states")
                if local_states:
                    states_refs.append(local_states)

                replica_nodes = await membership_ref.ask(GetReplicaIds(actor_id=name, replicas=effective_replicas))
                for replica_node_id in replica_nodes:
                    if replica_node_id in members:
                        replica_addr = members[replica_node_id].address
                        try:
                            host, port = replica_addr.rsplit(":", 1)
                            from .messages import Connect, Lookup
                            await remote_ref.ask(Connect(host=host, port=int(port)), timeout=2.0)
                            result = await remote_ref.ask(Lookup("states", peer=replica_addr), timeout=2.0)
                            if result and result.ref:
                                states_refs.append(result.ref)
                                logger.debug("ClusteredActorSystem got states ref", name=name, replica=replica_node_id)
                        except (TimeoutError, Exception) as e:
                            logger.debug("ClusteredActorSystem failed to get states ref", name=name, replica=replica_node_id, error=str(e))

                recovered_state = await self._try_recover_state(name, members, remote_ref)
                if recovered_state is not None and behavior.state_initial is not None:
                    from .state import State
                    if isinstance(behavior.state_initial, State):
                        behavior.state_initial.value = recovered_state["value"]
                        behavior.state_initial._version = recovered_state["version"]
                        logger.info("ClusteredActorSystem recovered state", name=name, version=recovered_state["version"])

                write_quorum_value = replication_config.write_quorum or 1
                if isinstance(write_quorum_value, str):
                    if write_quorum_value == "all":
                        write_quorum_value = len(states_refs)
                    elif write_quorum_value == "quorum":
                        write_quorum_value = (len(states_refs) // 2) + 1
                    else:
                        write_quorum_value = 1

                actor_filters = list(filters) if filters else []
                if states_refs:
                    actor_filters.append(replication_filter(states_refs, write_quorum_value))

                local_ref = await self._system.actor(behavior, name=name, filters=actor_filters if actor_filters else None)
                from .messages import Expose
                await remote_ref.send(Expose(ref=local_ref, name=name))

                all_members = dict(members)
                all_members[self._node_id] = MemberInfo(
                    node_id=self._node_id,
                    address=self._address,
                    state=MemberState.ALIVE,
                    incarnation=0
                )
                resolver = MembershipShardResolver(
                    membership_ref=membership_ref,
                    members=all_members,
                    replicas=effective_replicas
                )

                async def send_fn(address: str, actor_id: str, msg: Any) -> None:
                    if address == self._address:
                        ref = await self._system.actor(name=actor_id)
                        if ref:
                            await ref.send(msg)
                    else:
                        host, port = address.rsplit(":", 1)
                        from .messages import Connect, Lookup
                        await remote_ref.ask(Connect(host=host, port=int(port)), timeout=2.0)
                        result = await remote_ref.ask(Lookup(actor_id, peer=address), timeout=2.0)
                        if result and result.ref:
                            await result.ref.send(msg)

                async def ask_fn(address: str, actor_id: str, msg: Any, timeout: float = 10.0) -> Any:
                    if address == self._address:
                        ref = await self._system.actor(name=actor_id)
                        if ref:
                            return await ref.ask(msg, timeout)
                    else:
                        host, port = address.rsplit(":", 1)
                        from .messages import Connect, Lookup
                        await remote_ref.ask(Connect(host=host, port=int(port)), timeout=2.0)
                        result = await remote_ref.ask(Lookup(actor_id, peer=address), timeout=2.0)
                        if result and result.ref:
                            return await result.ref.ask(msg, timeout)
                    return None

                return ShardedActorRef(
                    actor_id=name,
                    resolver=resolver,
                    send_fn=send_fn,
                    ask_fn=ask_fn,
                    known_leader_id=self._node_id
                )
            else:
                leader_id = await membership_ref.ask(GetLeaderId(actor_id=name, replicas=effective_replicas))
                logger.debug("ClusteredActorSystem not leader, using sharded ref", name=name, leader=leader_id)
                if leader_id and leader_id in members:
                    leader_addr = members[leader_id].address
                    host, port = leader_addr.rsplit(":", 1)
                    from .messages import Connect, Lookup
                    await remote_ref.ask(Connect(host=host, port=int(port)), timeout=5.0)
                    await remote_ref.ask(Lookup(
                        name,
                        peer=leader_addr,
                        ensure=True,
                        behavior=func_name,
                        initial_state=initial_state_bytes
                    ), timeout=5.0)

                all_members = dict(members)
                all_members[self._node_id] = MemberInfo(
                    node_id=self._node_id,
                    address=self._address,
                    state=MemberState.ALIVE,
                    incarnation=0
                )
                resolver = MembershipShardResolver(
                    membership_ref=membership_ref,
                    members=all_members,
                    replicas=effective_replicas
                )

                async def send_fn(address: str, actor_id: str, msg: Any) -> None:
                    if address == self._address:
                        ref = await self._system.actor(name=actor_id)
                        if ref:
                            await ref.send(msg)
                    else:
                        host, port = address.rsplit(":", 1)
                        from .messages import Connect, Lookup
                        await remote_ref.ask(Connect(host=host, port=int(port)), timeout=2.0)
                        result = await remote_ref.ask(Lookup(actor_id, peer=address), timeout=2.0)
                        if result and result.ref:
                            await result.ref.send(msg)

                async def ask_fn(address: str, actor_id: str, msg: Any, timeout: float = 10.0) -> Any:
                    if address == self._address:
                        ref = await self._system.actor(name=actor_id)
                        if ref:
                            return await ref.ask(msg, timeout)
                    else:
                        host, port = address.rsplit(":", 1)
                        from .messages import Connect, Lookup
                        await remote_ref.ask(Connect(host=host, port=int(port)), timeout=2.0)
                        result = await remote_ref.ask(Lookup(actor_id, peer=address), timeout=2.0)
                        if result and result.ref:
                            return await result.ref.ask(msg, timeout)
                    return None

                return ShardedActorRef(
                    actor_id=name,
                    resolver=resolver,
                    send_fn=send_fn,
                    ask_fn=ask_fn,
                    known_leader_id=leader_id
                )
        else:
            return await self._system.actor(behavior, name=name, filters=filters)

    async def _try_recover_state(
        self,
        actor_name: str,
        members: dict[str, MemberInfo],
        remote_ref: ActorRef,
    ) -> dict[str, Any] | None:
        from .messages import Connect, Lookup, GetState

        best_state: dict[str, Any] | None = None
        best_version = -1

        local_states = await self._system.actor(name="states")
        if local_states:
            try:
                local_result = await local_states.ask(GetState(actor_name), timeout=2.0)
                if local_result and isinstance(local_result, dict) and "version" in local_result:
                    if local_result["version"] > best_version:
                        best_state = local_result
                        best_version = local_result["version"]
                        logger.debug("ClusteredActorSystem found local state", name=actor_name, version=best_version)
            except (TimeoutError, Exception):
                pass

        for member_id, member_info in members.items():
            if member_id == self._node_id:
                continue
            try:
                host, port = member_info.address.rsplit(":", 1)
                await remote_ref.ask(Connect(host=host, port=int(port)), timeout=2.0)
                result = await remote_ref.ask(Lookup("states", peer=member_info.address), timeout=2.0)
                if result and result.ref:
                    state_result = await result.ref.ask(GetState(actor_name), timeout=2.0)
                    if state_result and isinstance(state_result, dict) and "version" in state_result:
                        if state_result["version"] > best_version:
                            best_state = state_result
                            best_version = state_result["version"]
                            logger.debug("ClusteredActorSystem found remote state", name=actor_name, node=member_id, version=best_version)
            except (TimeoutError, Exception) as e:
                logger.debug("ClusteredActorSystem failed to get state from node", name=actor_name, node=member_id, error=str(e))

        if best_state:
            best_state["value"] = deserialize(best_state["value"])

        return best_state

    async def _lookup_remote[M](self, name: str, node_id: str) -> ActorRef[M] | None:
        membership_ref = await self._system.actor(name=MEMBERSHIP_ACTOR_ID)
        if not membership_ref:
            return None
        address = await membership_ref.ask(GetAddress(node_id))
        if not address:
            return None
        remote_ref = await self._system.actor(name=REMOTE_ACTOR_ID)
        if not remote_ref:
            return None
        from .messages import Lookup
        result = await remote_ref.ask(Lookup(name, peer=address))
        return result.ref if result else None

    async def ask[M, R](self, ref: ActorRef[M], msg: M, timeout: float = 30.0) -> R:
        return await self._system.ask(ref, msg, timeout)

    async def schedule[M](
        self,
        msg: M,
        *,
        to: ActorRef[M] | None = None,
        delay: float | None = None,
        every: float | None = None,
        sender: ActorRef | None = None,
    ) -> Callable[[], Coroutine[Any, Any, None]] | None:
        return await self._system.schedule(msg, to=to, delay=delay, every=every, sender=sender)

    async def shutdown(self) -> None:
        logger.info("shutting down clustered system")
        await self._system.shutdown()

    async def __aenter__(self) -> "ClusteredActorSystem":
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.shutdown()


def debug_filter[M](node_id: str) -> Filter[M]:
    def filter_fn(state: Any, stream: MessageStream[M], meta: Any = None) -> MessageStream[M]:
        async def filtered() -> MessageStream[M]:
            async for msg, ctx in stream:
                print(f"[{node_id}] {ctx.self_id} <- {type(msg).__name__}: {msg}", flush=True)
                yield msg, ctx
        return filtered()
    return filter_fn


class DistributionStrategy(Enum):
    RANDOM = "random"
    ROUND_ROBIN = "round-robin"
    CONSISTENT = "consistent"


class ClusteredDevelopmentActorRef[M](ActorRef[M]):
    def __init__(self, cluster: DevelopmentCluster, behavior: Behavior, *, name: str) -> None:
        self._cluster = cluster
        self.actor_id = name
        self._behavior = behavior

    async def _get_ref(self) -> ActorRef[M]:
        node = self._cluster._next_node(name=self.actor_id)
        ref = await node.actor(behavior=self._behavior, name=self.actor_id)
        return ref

    async def send(self, msg: M, *, sender: ActorRef[Any] | None = None) -> None:
        ref = await self._get_ref()
        await ref.send(msg=msg, sender=sender)

    async def send_envelope(self, envelope: "Envelope[M]") -> None:
        ref = await self._get_ref()
        await ref.send_envelope(envelope)

    async def ask(self, msg: M, timeout: float = 10.0) -> Any:
        ref = await self._get_ref()
        return await ref.ask(msg=msg, timeout=timeout)

    def __rshift__(self, msg: M) -> Awaitable[None]:
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        return self.ask(msg)


class DevelopmentCluster:
    def __init__(self, nodes: int = 3, *, strategy: DistributionStrategy = DistributionStrategy.RANDOM, debug: bool = False) -> None:
        self._node_count = nodes
        self._strategy = strategy
        self._systems: list[ClusteredActorSystem] = []
        self._round_robin_index = 0
        self._debug = debug

    def __getitem__(self, index: int) -> ClusteredActorSystem:
        return self._systems[index]

    def __len__(self) -> int:
        return len(self._systems)

    def __iter__(self):
        return iter(self._systems)

    @property
    def nodes(self) -> list[ClusteredActorSystem]:
        return list(self._systems)

    def _next_node(self, name: str | None = None) -> ClusteredActorSystem:
        match self._strategy:
            case DistributionStrategy.RANDOM:
                return random.choice(self._systems)
            case DistributionStrategy.ROUND_ROBIN:
                node = self._systems[self._round_robin_index]
                self._round_robin_index = (self._round_robin_index + 1) % len(self._systems)
                return node
            case DistributionStrategy.CONSISTENT:
                index = hash(name) % len(self._systems)
                return self._systems[index]

    async def actor[M](self, behavior: Behavior, *, name: str) -> ActorRef[M] | None:
        return ClusteredDevelopmentActorRef(cluster=self, behavior=behavior, name=name)

    async def start(self) -> None:
        logger.info("DevelopmentCluster starting", nodes=self._node_count, strategy=self._strategy.value)

        first_system = ClusteredActorSystem(
            node_id="node-0",
            host="127.0.0.1",
            port=0,
            debug_filter=debug_filter("node-0") if self._debug else None,
        )
        await first_system.start()
        self._systems.append(first_system)

        first_address = await first_system.address()
        logger.debug("DevelopmentCluster first node started", address=first_address)

        for i in range(1, self._node_count):
            node_id = f"node-{i}"
            system = ClusteredActorSystem(
                node_id=node_id,
                host="127.0.0.1",
                port=0,
                seeds=[("node-0", first_address)],
                debug_filter=debug_filter(node_id) if self._debug else None,
            )
            await system.start()
            self._systems.append(system)
            logger.debug("DevelopmentCluster node started", node_id=node_id)

        logger.debug("DevelopmentCluster waiting for all nodes")
        await self.wait_for(self._node_count)
        logger.info("DevelopmentCluster started", nodes=len(self._systems))

    async def shutdown(self) -> None:
        logger.info("DevelopmentCluster shutting down", nodes=len(self._systems))
        await asyncio.gather(*[ s.shutdown() for s in self._systems ])
        self._systems.clear()
        logger.debug("DevelopmentCluster shutdown complete")

    async def __aenter__(self) -> "DevelopmentCluster":
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.shutdown()

    async def wait_for(self, nodes: int):
        logger.debug("DevelopmentCluster wait_for", nodes=nodes)
        clusters = [await s.actor(name="cluster") for s in self._systems]
        await asyncio.gather(*[c.ask(WaitFor(nodes=nodes)) for c in clusters])
        logger.debug("DevelopmentCluster wait_for complete", nodes=nodes)

    async def gossip(self) -> ActorRef:
        node = self._next_node()
        return await node._system.actor(name="gossip")

    async def decluster(self, node_id: str) -> None:
        system = next((s for s in self._systems if s.node_id == node_id), None)
        if not system:
            raise ValueError(f"Node {node_id} not found")

        await system.shutdown()
        self._systems.remove(system)

    async def add_node(self, node_id: str | None = None) -> ClusteredActorSystem:
        if node_id is None:
            existing_ids = {s.node_id for s in self._systems}
            i = 0
            while f"node-{i}" in existing_ids:
                i += 1
            node_id = f"node-{i}"

        if not self._systems:
            raise RuntimeError("No nodes in cluster to use as seed")

        seed_system = self._systems[0]
        seed_address = await seed_system.address()

        system = ClusteredActorSystem(
            node_id=node_id,
            host="127.0.0.1",
            port=0,
            seeds=[(seed_system.node_id, seed_address)],
            debug_filter=debug_filter(node_id) if self._debug else None,
        )

        await system.start()
        self._systems.append(system)

        return system
