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

    def remove_node(self, node: str) -> None:
        for i in range(self._virtual_nodes):
            key = self._hash(f"{node}:{i}")
            if key in self._ring:
                del self._ring[key]
                idx = bisect.bisect_left(self._sorted_keys, key)
                if idx < len(self._sorted_keys) and self._sorted_keys[idx] == key:
                    self._sorted_keys.pop(idx)

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
    SwimTick, ProbeTimeout, PingReqTimeout,
    GossipPut, GossipGet, GossipTick, GossipMessage,
    StoreState, StoreAck, StatesMessage,
)


type MembershipMessage = (
    Join | MergeMembership | MarkDown | MarkAlive |
    GetAliveMembers | GetAllMembers | GetResponsibleNodes |
    SetLocalAddress | GetAddress |
    GetLeaderId | IsLeader | GetReplicaIds
)


@actor
async def membership_actor(
    node_id: str,
    initial_members: dict[str, MemberInfo] | None = None,
    *,
    mailbox: Mailbox[MembershipMessage],
):
    members = dict(initial_members or {})
    hash_ring = HashRing()
    hash_ring.add_node(node_id)
    local_address = ""

    for member_id, info in members.items():
        if info.state == MemberState.ALIVE:
            hash_ring.add_node(member_id)

    async for msg, ctx in mailbox:
        match msg:
            case SetLocalAddress(addr):
                local_address = addr

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

            case MergeMembership(remote_members):
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
                    else:
                        local = members[snapshot.node_id]
                        if snapshot.incarnation > local.incarnation:
                            old_state = local.state
                            local.state = MemberState(snapshot.state)
                            local.incarnation = snapshot.incarnation
                            local.address = snapshot.address
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

            case MarkAlive(target_node_id, address):
                if target_node_id in members:
                    member = members[target_node_id]
                    if member.state == MemberState.DOWN:
                        member.state = MemberState.ALIVE
                        member.incarnation += 1
                        member.address = address
                        hash_ring.add_node(target_node_id)
                        logger.info("Member recovered", member=target_node_id)
                else:
                    members[target_node_id] = MemberInfo(
                        node_id=target_node_id,
                        address=address,
                        state=MemberState.ALIVE,
                        incarnation=0,
                    )
                    hash_ring.add_node(target_node_id)
                    logger.info("New member discovered", member=target_node_id, address=address)

            case GetAliveMembers():
                result = {mid: info for mid, info in members.items() if info.state == MemberState.ALIVE}
                if local_address:
                    result[node_id] = MemberInfo(node_id=node_id, address=local_address, state=MemberState.ALIVE, incarnation=0)
                await ctx.reply(result)

            case GetAllMembers():
                result = dict(members)
                if local_address:
                    result[node_id] = MemberInfo(node_id=node_id, address=local_address, state=MemberState.ALIVE, incarnation=0)
                await ctx.reply(result)

            case GetResponsibleNodes(actor_id=aid, count=cnt):
                try:
                    await ctx.reply(hash_ring.get_nodes(aid, cnt))
                except RuntimeError:
                    await ctx.reply([])

            case GetAddress(node_id=target_node_id):
                if target_node_id in members:
                    await ctx.reply(members[target_node_id].address)
                else:
                    await ctx.reply(None)

            case GetLeaderId(actor_id=aid, replicas=reps):
                nodes = hash_ring.get_n_nodes(aid, reps)
                alive = [n for n in nodes if n == node_id or (n in members and members[n].state == MemberState.ALIVE)]
                await ctx.reply(alive[0] if alive else None)

            case IsLeader(actor_id=aid, replicas=reps):
                nodes = hash_ring.get_n_nodes(aid, reps)
                alive = [n for n in nodes if n == node_id or (n in members and members[n].state == MemberState.ALIVE)]
                leader = alive[0] if alive else None
                await ctx.reply(leader == node_id)

            case GetReplicaIds(actor_id=aid, replicas=reps):
                nodes = hash_ring.get_n_nodes(aid, reps)
                alive = [n for n in nodes if n == node_id or (n in members and members[n].state == MemberState.ALIVE)]
                leader = alive[0] if alive else None
                await ctx.reply([n for n in alive if n != leader])


def to_snapshots(members: dict[str, MemberInfo]) -> list[MemberSnapshot]:
    return [MemberSnapshot(m.node_id, m.address, m.state.value, m.incarnation) for m in members.values()]


@dataclass
class _PendingSend:
    message: Any


@actor
async def swim_actor(
    node_id: str,
    probe_interval: float = 1.0,
    probe_timeout: float = 2.0,
    ping_req_fanout: int = 3,
    *,
    mailbox: Mailbox[SwimTick | Ping | Ack | PingReq | PingReqAck | ProbeTimeout | PingReqTimeout | Reply],
    system: System,
):
    from .messages import Connect, Lookup, LookupResult
    from .remote import RemoteConnected

    pending_probes: dict[str, float] = {}
    pending_sends: dict[str, _PendingSend] = {}

    membership_ref = await system.actor(name=MEMBERSHIP_ACTOR_ID)
    remote_ref = await system.actor(name=REMOTE_ACTOR_ID)

    if membership_ref is None or remote_ref is None:
        return

    await mailbox.schedule(SwimTick(), every=probe_interval)

    async def get_member_snapshots() -> list[MemberSnapshot]:
        all_members = await membership_ref.ask(GetAllMembers())
        return to_snapshots(all_members)

    async def initiate_send(address: str, actor_name: str, message: Any) -> None:
        if remote_ref is None:
            return
        pending_sends[address] = _PendingSend(message=message)
        host, port = address.rsplit(":", 1)
        await remote_ref.send(Connect(host=host, port=int(port)), sender=mailbox.ref())
        await remote_ref.send(Lookup(actor_name, peer=address), sender=mailbox.ref())

    async for msg, ctx in mailbox:
        match msg:
            case SwimTick():
                members = await membership_ref.ask(GetAliveMembers())
                other_members = [m for m in members if m != node_id]
                if not other_members:
                    continue
                target = random.choice(other_members)
                target_info = members[target]
                pending_probes[target] = time.time()
                snapshots = await get_member_snapshots()
                await initiate_send(target_info.address, SWIM_NAME, Ping(sender=node_id, members=snapshots))
                await ctx.schedule(ProbeTimeout(target), delay=probe_timeout)

            case Ping(sender=ping_sender, members=remote_members):
                for m in remote_members:
                    if m.node_id == ping_sender and m.state == "alive":
                        await membership_ref.send(MarkAlive(ping_sender, m.address))
                        break
                await membership_ref.send(MergeMembership(remote_members))
                snapshots = await get_member_snapshots()
                ack = Ack(sender=node_id, members=snapshots)
                if ctx.reply_to is not None or ctx.sender is not None:
                    await ctx.reply(ack)
                else:
                    members = await membership_ref.ask(GetAliveMembers())
                    if ping_sender in members:
                        sender_info = members[ping_sender]
                        await initiate_send(sender_info.address, SWIM_NAME, ack)

            case Ack(sender=ack_sender, members=remote_members):
                if ack_sender in pending_probes:
                    del pending_probes[ack_sender]
                for m in remote_members:
                    if m.node_id == ack_sender and m.state == "alive":
                        await membership_ref.send(MarkAlive(ack_sender, m.address))
                        break
                await membership_ref.send(MergeMembership(remote_members))

            case ProbeTimeout(target):
                if target not in pending_probes:
                    continue
                logger.debug("Probe timeout", target=target)
                members = await membership_ref.ask(GetAliveMembers())
                other_members = [m for m in members if m != node_id and m != target]
                if not other_members:
                    del pending_probes[target]
                    logger.warn("Marking node down", target=target)
                    await membership_ref.send(MarkDown(target))
                    continue
                probers = random.sample(other_members, min(ping_req_fanout, len(other_members)))
                snapshots = await get_member_snapshots()
                for prober in probers:
                    prober_info = members[prober]
                    await initiate_send(prober_info.address, SWIM_NAME, PingReq(sender=node_id, target=target, members=snapshots))
                await ctx.schedule(PingReqTimeout(target), delay=probe_timeout)

            case PingReq(sender=req_sender, target=req_target, members=remote_members):
                for m in remote_members:
                    if m.node_id == req_sender and m.state == "alive":
                        await membership_ref.send(MarkAlive(req_sender, m.address))
                        break
                await membership_ref.send(MergeMembership(remote_members))
                members = await membership_ref.ask(GetAliveMembers())
                if req_target in members:
                    target_info = members[req_target]
                    snapshots = await get_member_snapshots()
                    await initiate_send(target_info.address, SWIM_NAME, Ping(sender=node_id, members=snapshots))
                snapshots = await get_member_snapshots()
                ack = PingReqAck(sender=node_id, target=req_target, success=True, members=snapshots)
                if ctx.reply_to is not None or ctx.sender is not None:
                    await ctx.reply(ack)
                elif req_sender in members:
                    sender_info = members[req_sender]
                    await initiate_send(sender_info.address, SWIM_NAME, ack)

            case PingReqAck(sender=_, target=ack_target, success=success, members=remote_members):
                if success and ack_target in pending_probes:
                    del pending_probes[ack_target]
                    for m in remote_members:
                        if m.node_id == ack_target and m.state == "alive":
                            await membership_ref.send(MarkAlive(ack_target, m.address))
                            break
                await membership_ref.send(MergeMembership(remote_members))

            case PingReqTimeout(target):
                if target in pending_probes:
                    del pending_probes[target]
                    logger.warn("Marking node down after ping-req timeout", target=target)
                    await membership_ref.send(MarkDown(target))

            case Reply(result=RemoteConnected()):
                pass

            case Reply(result=Ack() as ack_msg):
                ack_sender = ack_msg.sender
                remote_members = ack_msg.members
                if ack_sender in pending_probes:
                    del pending_probes[ack_sender]
                for m in remote_members:
                    if m.node_id == ack_sender and m.state == "alive":
                        await membership_ref.send(MarkAlive(ack_sender, m.address))
                        break
                await membership_ref.send(MergeMembership(remote_members))

            case Reply(result=PingReqAck() as ack_msg):
                ack_target = ack_msg.target
                success = ack_msg.success
                remote_members = ack_msg.members
                if success and ack_target in pending_probes:
                    del pending_probes[ack_target]
                    for m in remote_members:
                        if m.node_id == ack_target and m.state == "alive":
                            await membership_ref.send(MarkAlive(ack_target, m.address))
                            break
                await membership_ref.send(MergeMembership(remote_members))

            case Reply(result=LookupResult(ref=ref, peer=peer)) if ref is not None and peer is not None:
                pending = pending_sends.pop(peer, None)
                if pending:
                    await ref.send(pending.message, sender=mailbox.ref())

            case Reply(result=LookupResult(ref=None, peer=peer)) if peer is not None:
                pending_sends.pop(peer, None)

            case Reply():
                pass


@actor
async def gossip_actor(
    node_id: str,
    fanout: int = 3,
    tick_interval: float = 0.5,
    *,
    mailbox: Mailbox[GossipMessage | Reply[Any]],
    system: System,
):
    from .messages import Connect, Lookup, LookupResult
    from .remote import RemoteConnected

    store: dict[str, tuple[bytes, int]] = {}

    membership_ref = await system.actor(name=MEMBERSHIP_ACTOR_ID)
    remote_ref = await system.actor(name=REMOTE_ACTOR_ID)

    await mailbox.schedule(GossipTick(), every=tick_interval)

    async for msg, ctx in mailbox:
        match msg:
            case GossipTick():
                if not store or membership_ref is None:
                    continue
                await membership_ref.send(GetAliveMembers(), sender=mailbox.ref())

            case Reply(result=members) if isinstance(members, dict):
                if not store or remote_ref is None:
                    continue
                other_members = [m for m in members if m != node_id]
                if not other_members:
                    continue
                targets = random.sample(other_members, min(fanout, len(other_members)))
                for t in targets:
                    addr = members[t].address
                    host, port = addr.rsplit(":", 1)
                    await remote_ref.send(Connect(host=host, port=int(port)), sender=mailbox.ref())
                    await remote_ref.send(Lookup(GOSSIP_NAME, peer=addr), sender=mailbox.ref())

            case Reply(result=RemoteConnected()):
                pass

            case Reply(result=LookupResult(ref=ref)) if ref is not None:
                for key, (value, version) in store.items():
                    await ref.send(GossipPut(key, value, version))

            case Reply(result=LookupResult(ref=None)):
                pass

            case GossipPut(key, value, version):
                current = store.get(key)
                current_version = current[1] if current else 0
                if version == 0:
                    new_version = current_version + 1
                    store[key] = (value, new_version)
                    logger.debug("Gossip put", key=key, version=new_version)
                elif version > current_version:
                    store[key] = (value, version)
                    logger.debug("Gossip replicated", key=key, version=version)

            case GossipGet(key):
                entry = store.get(key)
                await ctx.reply(entry[0] if entry else None)


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

    async def send(self, msg: M, *, sender: "ActorRef[Any] | None" = None) -> None:
        address = await self._get_target_address()
        await self.send_fn(address, self.actor_id, msg)

    async def send_envelope(self, envelope: "Envelope[M]") -> None:
        await self.send(envelope.payload, sender=envelope.sender)

    async def ask[R](self, msg: M, timeout: float = 10.0) -> R:
        address = await self._get_target_address()
        return await self.ask_fn(address, self.actor_id, msg, timeout)

    def __rshift__(self, msg: M) -> Awaitable[None]:
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        return self.ask(msg)


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
    async def apply(state: Any, inner: MessageStream[M]) -> MessageStream[M]:
        async for msg, ctx in inner:
            yield msg, ctx

            if states_refs and state is not None:
                if isinstance(state, Stateful):
                    snapshot = state.snapshot()
                elif hasattr(state, 'snapshot'):
                    snapshot = state.snapshot()
                else:
                    snapshot = vars(state) if hasattr(state, '__dict__') else {}

                actor_id = ctx.self_id
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


@actor
async def cluster_actor(
    node_id: str,
    host: str = "127.0.0.1",
    port: int = 0,
    seeds: list[tuple[str, str]] | None = None,
    *,
    mailbox: Mailbox[ClusterMessage],
    system: System,
):
    from .remote import remote_actor, Listen, Connect, Expose, Lookup
    from .state import states

    remote_ref = await system.actor(remote_actor(), name="remote")
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

    membership_ref = await system.actor(membership_actor(node_id, initial_members), name="membership")
    await membership_ref.send(SetLocalAddress(local_address))

    swim_ref = await system.actor(swim_actor(node_id), name="swim")
    gossip_ref = await system.actor(gossip_actor(node_id), name="gossip")

    await remote_ref.ask(Expose(ref=swim_ref, name="swim"))
    await remote_ref.ask(Expose(ref=gossip_ref, name="gossip"))
    await remote_ref.ask(Expose(ref=membership_ref, name="membership"))
    await remote_ref.ask(Expose(ref=mailbox.ref(), name="cluster"))
    await remote_ref.ask(Expose(ref=states_ref, name="states"))

    for seed_node_id, seed_address in (seeds or []):
        asyncio.create_task(_connect_to_seed(remote_ref, membership_ref, node_id, local_address, seed_address))

    async def route_message(address: str, actor_id: str, msg: Any, ask: bool = False, timeout: float = 10.0) -> Any:
        host_part, port_str = address.rsplit(":", 1)
        if address == local_address:
            local_ref = await system.actor(name=actor_id)
            if local_ref:
                if ask:
                    return await local_ref.ask(msg, timeout)
                await local_ref.send(msg)
                return None
        try:
            await remote_ref.ask(Connect(host=host_part, port=int(port_str)), timeout=2.0)
            result = await remote_ref.ask(Lookup(actor_id, peer=address), timeout=2.0)
            if result and result.ref:
                if ask:
                    return await result.ref.ask(msg, timeout)
                await result.ref.send(msg)
        except (TimeoutError, Exception) as e:
            logger.warn("route_message failed", error=str(e))
        return None

    async for msg, ctx in mailbox:
        match msg:
            case GetClusterAddress():
                await ctx.reply(local_address)

            case CreateActor(behavior, name):
                func_name = behavior.func.__name__
                replication_config = behavior.__replication_config__
                replicas = replication_config.replicas if replication_config else None

                logger.debug("CreateActor received", actor=name, replicas=replicas)

                members = await membership_ref.ask(GetAliveMembers())

                local_ref = await system.actor(name=name)
                if local_ref:
                    logger.debug("Actor already exists locally", actor=name)
                    await ctx.reply(local_ref)
                    continue

                if replication_config:
                    effective_replicas = replicas if replicas else 1
                    responsible_nodes = await membership_ref.ask(GetResponsibleNodes(actor_id=name, count=effective_replicas))
                    is_leader = await membership_ref.ask(IsLeader(actor_id=name, replicas=effective_replicas))

                    logger.debug("Shard calculated", actor=name, is_leader=is_leader, responsible=responsible_nodes)

                    if is_leader:
                        logger.debug("I am leader, creating actor", actor=name)
                        register_behavior(func_name, behavior)

                        other_replicas = await membership_ref.ask(GetReplicaIds(actor_id=name, replicas=effective_replicas))
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
                        lead_filter = leadership_filter(membership_ref, name, effective_replicas)

                        logger.debug("Spawning actor locally", actor=name)
                        local_ref = await system.actor(behavior, name=name, filters=[lead_filter, rep_filter])
                        logger.debug("Actor spawned, exposing", actor=name)
                        await remote_ref.ask(Expose(ref=local_ref, name=name))

                        all_members = dict(members)
                        all_members[node_id] = MemberInfo(node_id=node_id, address=local_address, state=MemberState.ALIVE, incarnation=0)

                        resolver = MembershipShardResolver(membership_ref=membership_ref, members=all_members, replicas=effective_replicas)

                        async def send_fn(address: str, actor_id: str, msg: Any) -> None:
                            await route_message(address, actor_id, msg, ask=False)

                        async def ask_fn(address: str, actor_id: str, msg: Any, timeout: float = 10.0) -> Any:
                            return await route_message(address, actor_id, msg, ask=True, timeout=timeout)

                        sharded_ref = ShardedActorRef(actor_id=name, resolver=resolver, send_fn=send_fn, ask_fn=ask_fn, known_leader_id=node_id)
                        logger.debug("Replying with sharded ref (leader)", actor=name)
                        await ctx.reply(sharded_ref)
                    else:
                        leader_id = await membership_ref.ask(GetLeaderId(actor_id=name, replicas=effective_replicas))
                        leader_address = members[leader_id].address if leader_id in members else None
                        logger.debug("I am NOT leader, forwarding to leader", actor=name, leader=leader_id)

                        if leader_address:
                            register_behavior(func_name, behavior)
                            host_part, port_str = leader_address.rsplit(":", 1)
                            initial_state_bytes = None
                            if behavior.state_initial is not None:
                                logger.debug("Serializing initial state")
                                initial_state_bytes = serialize(behavior.state_initial)
                            try:
                                await remote_ref.ask(Connect(host=host_part, port=int(port_str)), timeout=2.0)
                                result = await remote_ref.ask(Lookup("cluster", peer=leader_address), timeout=2.0)
                                if result and result.ref:
                                    await result.ref.ask(_RemoteCreateActor(behavior_name=func_name, actor_name=name, initial_state=initial_state_bytes), timeout=5.0)
                            except (TimeoutError, Exception) as e:
                                logger.warn("Failed to forward to leader", actor=name, error=str(e))

                        all_members = dict(members)
                        all_members[node_id] = MemberInfo(node_id=node_id, address=local_address, state=MemberState.ALIVE, incarnation=0)

                        resolver = MembershipShardResolver(membership_ref=membership_ref, members=all_members, replicas=effective_replicas)

                        async def send_fn(address: str, actor_id: str, msg: Any) -> None:
                            await route_message(address, actor_id, msg, ask=False)

                        async def ask_fn(address: str, actor_id: str, msg: Any, timeout: float = 10.0) -> Any:
                            return await route_message(address, actor_id, msg, ask=True, timeout=timeout)

                        sharded_ref = ShardedActorRef(actor_id=name, resolver=resolver, send_fn=send_fn, ask_fn=ask_fn, known_leader_id=leader_id)
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
                    await mailbox.schedule(WaitFor(nodes), delay=0.2, sender=ctx.sender)


async def _connect_to_seed(remote_ref: ActorRef, membership_ref: ActorRef, node_id: str, local_address: str, seed_address: str):
    from .messages import Connect, Lookup

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
                return await self._lookup_remote(name, node_id)
            return await self._system.actor(name=name)

        if self._cluster_ref is None:
            raise RuntimeError("ClusteredActorSystem not started")

        return await self._cluster_ref.ask(CreateActor(behavior, name))

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
        first_system = ClusteredActorSystem(
            node_id="node-0",
            host="127.0.0.1",
            port=0,
            debug_filter=debug_filter("node-0") if self._debug else None,
        )
        await first_system.start()
        self._systems.append(first_system)

        first_address = await first_system.address()

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

        await self.wait_for(self._node_count)

    async def shutdown(self) -> None:
        for system in reversed(self._systems):
            await system.shutdown()
        self._systems.clear()

    async def __aenter__(self) -> "DevelopmentCluster":
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.shutdown()

    async def wait_for(self, nodes: int):
        clusters = [await s.actor(name="cluster") for s in self._systems]
        await asyncio.gather(*[c.ask(WaitFor(nodes=nodes)) for c in clusters])

    async def gossip(self) -> ActorRef:
        node = self._next_node()
        return await node._system.actor(name="gossip")
