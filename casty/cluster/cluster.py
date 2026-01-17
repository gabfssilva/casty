from __future__ import annotations

import asyncio
import copy
import importlib
from dataclasses import dataclass, field
from enum import Enum
from time import time
from typing import Any
from uuid import uuid4

import msgpack

from casty import Actor, Context, LocalRef

from .config import ClusterConfig
from .hash_ring import HashRing
from .messages import (
    Send,
    Node,
    Shard,
    All,
    GetMembers,
    GetNodeForKey,
    Subscribe,
    Unsubscribe,
    ClusterEvent,
    NodeJoined,
    NodeLeft,
    NodeFailed,
    TransportSend,
    TransportReceived,
    TransportConnected,
    TransportDisconnected,
    TransportEvent,
    Ping,
    Ack,
    PingReq,
    Suspect,
    Alive,
    Dead,
    RegisterClusteredActor,
    GetClusteredActor,
    ClusteredSend,
    ClusteredSendAck,
    ClusteredAsk,
    ClusteredAskResponse,
    ActorRegistered,
)
from .transport import Connect
from .tcp import TcpTransport


class MemberState(Enum):
    ALIVE = "alive"
    SUSPECTED = "suspected"
    DEAD = "dead"


@dataclass
class MemberInfo:
    node_id: str
    address: tuple[str, int]
    state: MemberState = MemberState.ALIVE
    incarnation: int = 0
    last_seen: float = field(default_factory=time)


@dataclass
class PendingProbe:
    target: str
    sequence: int
    started_at: float
    indirect_targets: list[str] = field(default_factory=list)
    indirect_acks: int = 0
    timeout_task: asyncio.Task[None] | None = None


@dataclass(frozen=True, slots=True)
class _SwimTick:
    pass


@dataclass(frozen=True, slots=True)
class _ProbeTimeout:
    sequence: int
    target: str


@dataclass(frozen=True, slots=True)
class _SuspicionTimeout:
    node_id: str


@dataclass
class _ClusteredActorInfo:
    actor_id: str
    actor_cls: type | None
    actor_cls_name: str
    replication: int
    singleton: bool
    owner_node: str
    version: int = 0


type ClusterMessage = (
    Send
    | GetMembers
    | GetNodeForKey
    | Subscribe
    | Unsubscribe
    | TransportEvent
    | Ping
    | Ack
    | PingReq
    | Suspect
    | Alive
    | Dead
    | _SwimTick
    | _ProbeTimeout
    | _SuspicionTimeout
    | RegisterClusteredActor
    | GetClusteredActor
    | ClusteredSend
    | ClusteredAsk
)


class Cluster(Actor[ClusterMessage]):

    def __init__(self, config: ClusterConfig | None = None):
        self._config = config or ClusterConfig()
        self._node_id = self._config.node_id or f"node-{uuid4().hex[:8]}"
        self._members: dict[str, MemberInfo] = {}
        self._hash_ring = HashRing(self._config.virtual_nodes)
        self._subscribers: set[LocalRef[ClusterEvent]] = set()
        self._transport: LocalRef[TransportSend | Connect] | None = None
        self._incarnation = 0
        self._sequence = 0
        self._pending_probes: dict[int, PendingProbe] = {}
        self._probe_index = 0
        self._member_order: list[str] = []
        self._suspicion_timers: dict[str, str] = {}
        self._tick_id: str | None = None
        self._clustered_actors: dict[str, _ClusteredActorInfo] = {}
        self._local_actors: dict[str, LocalRef[Any]] = {}
        self._pending_asks: dict[str, asyncio.Future[Any]] = {}
        self._pending_sends: dict[str, tuple[int, int, Context]] = {}

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def members(self) -> dict[str, MemberInfo]:
        return dict(self._members)

    async def on_start(self) -> None:
        advertise_host = self._config.advertise_host or self._config.bind_host
        advertise_port = self._config.advertise_port or self._config.bind_port
        address = (advertise_host, advertise_port)

        self._members[self._node_id] = MemberInfo(
            node_id=self._node_id,
            address=address,
            state=MemberState.ALIVE,
            incarnation=self._incarnation,
        )
        self._hash_ring.add_node(self._node_id)
        self._member_order.append(self._node_id)

        self._transport = await self._ctx.spawn(
            TcpTransport,
            cluster=self._ctx.self_ref,
            bind_address=(self._config.bind_host, self._config.bind_port),
            node_id=self._node_id,
        )

        self._tick_id = await self._ctx.tick(_SwimTick(), interval=self._config.protocol_period)

        for seed in self._config.seeds:
            parts = seed.rsplit(":", 1)
            if len(parts) == 2:
                host = parts[0]
                port = int(parts[1])
                await self._transport.send(Connect(node_id=seed, address=(host, port)))

    async def on_stop(self) -> None:
        if self._tick_id:
            await self._ctx.cancel_tick(self._tick_id)

        for probe in self._pending_probes.values():
            if probe.timeout_task and not probe.timeout_task.done():
                probe.timeout_task.cancel()
        self._pending_probes.clear()

        for task_id in self._suspicion_timers.values():
            await self._ctx.cancel_schedule(task_id)
        self._suspicion_timers.clear()

    async def receive(self, msg: ClusterMessage, ctx: Context) -> None:
        match msg:
            case Send(payload=payload, to=route):
                await self._handle_send(payload, route)
            case GetMembers():
                ctx.reply(self.members)
            case GetNodeForKey(key=key):
                ctx.reply(self._hash_ring.get_node(key))
            case Subscribe(subscriber=subscriber):
                self._subscribers.add(subscriber)
            case Unsubscribe(subscriber=subscriber):
                self._subscribers.discard(subscriber)
            case TransportReceived(node_id=node_id, payload=payload):
                await self._handle_transport_received(node_id, payload)
            case TransportConnected(node_id=node_id, address=address):
                await self._handle_transport_connected(node_id, address)
            case TransportDisconnected(node_id=node_id):
                await self._handle_transport_disconnected(node_id)
            case _SwimTick():
                await self._handle_swim_tick()
            case _ProbeTimeout(sequence=sequence, target=target):
                await self._handle_probe_timeout(sequence, target)
            case _SuspicionTimeout(node_id=node_id):
                await self._handle_suspicion_timeout(node_id)
            case RegisterClusteredActor(actor_id=actor_id, actor_cls=actor_cls, replication=replication, singleton=singleton):
                await self._handle_register_clustered_actor(actor_id, actor_cls, replication, singleton, ctx)
            case GetClusteredActor(actor_id=actor_id):
                ctx.reply(self._clustered_actors.get(actor_id))
            case ClusteredSend():
                await self._handle_clustered_send(msg, ctx)
            case ClusteredAsk(actor_id=actor_id, request_id=request_id, payload_type=payload_type, payload=payload, consistency=consistency):
                await self._handle_clustered_ask(actor_id, request_id, payload_type, payload, consistency, ctx)
            case Ping() | Ack() | PingReq() | Suspect() | Alive() | Dead():
                pass

    async def _handle_send(self, payload: Any, route: Node | Shard | All) -> None:
        match route:
            case Node(id=node_id):
                await self._send_to_node(node_id, payload)
            case Shard(key=key):
                node_id = self._hash_ring.get_node(key)
                if node_id:
                    await self._send_to_node(node_id, payload)
            case All():
                await self._broadcast(payload)

    async def _handle_transport_received(self, node_id: str, payload: Any) -> None:
        match payload:
            case Ping():
                await self._handle_ping(node_id, payload)
            case Ack():
                await self._handle_ack(node_id, payload)
            case PingReq():
                await self._handle_ping_req(node_id, payload)
            case Suspect():
                await self._handle_suspect(payload)
            case Alive():
                await self._handle_alive(payload)
            case Dead():
                await self._handle_dead(payload)
            case ActorRegistered():
                await self._handle_actor_registered(payload)
            case ClusteredSend():
                await self._handle_remote_clustered_send(node_id, payload)
            case ClusteredSendAck():
                await self._handle_clustered_send_ack(payload)
            case ClusteredAsk():
                await self._handle_remote_clustered_ask(node_id, payload)
            case ClusteredAskResponse():
                await self._handle_clustered_ask_response(payload)
            case _:
                await self._broadcast_to_subscribers(payload)

    async def _handle_transport_connected(self, node_id: str, address: tuple[str, int]) -> None:
        if node_id in self._members:
            member = self._members[node_id]
            member.address = address
            member.last_seen = time()
            if member.state == MemberState.SUSPECTED:
                member.state = MemberState.ALIVE
                if node_id in self._suspicion_timers:
                    await self._ctx.cancel_schedule(self._suspicion_timers.pop(node_id))
            return

        self._members[node_id] = MemberInfo(
            node_id=node_id,
            address=address,
            state=MemberState.ALIVE,
        )
        self._hash_ring.add_node(node_id)
        self._member_order.append(node_id)

        await self._broadcast_event(NodeJoined(node_id=node_id, address=address))

    async def _handle_transport_disconnected(self, node_id: str) -> None:
        if node_id not in self._members:
            return

        member = self._members[node_id]
        if member.state == MemberState.ALIVE:
            await self._start_suspicion(node_id)

    async def _handle_swim_tick(self) -> None:
        alive_members = [
            nid
            for nid, member in self._members.items()
            if nid != self._node_id and member.state == MemberState.ALIVE
        ]

        if not alive_members:
            return

        self._probe_index = self._probe_index % len(alive_members)
        target = alive_members[self._probe_index]
        self._probe_index += 1

        self._sequence += 1
        seq = self._sequence

        self._pending_probes[seq] = PendingProbe(
            target=target,
            sequence=seq,
            started_at=time(),
        )

        await self._send_to_node(target, Ping(sequence=seq))
        await self._ctx.schedule(self._config.ping_timeout, _ProbeTimeout(sequence=seq, target=target))

    async def _handle_probe_timeout(self, sequence: int, target: str) -> None:
        probe = self._pending_probes.get(sequence)
        if probe is None:
            return

        if probe.indirect_targets:
            if probe.indirect_acks > 0:
                del self._pending_probes[sequence]
                member = self._members.get(target)
                if member:
                    member.last_seen = time()
            else:
                del self._pending_probes[sequence]
                await self._start_suspicion(target)
            return

        alive_members = [
            nid
            for nid, member in self._members.items()
            if nid != self._node_id and nid != target and member.state == MemberState.ALIVE
        ]

        if not alive_members:
            del self._pending_probes[sequence]
            await self._start_suspicion(target)
            return

        indirect_targets = alive_members[: self._config.ping_req_fanout]
        probe.indirect_targets = indirect_targets

        for indirect_node in indirect_targets:
            await self._send_to_node(indirect_node, PingReq(target=target, sequence=sequence))

        await self._ctx.schedule(
            self._config.ping_req_timeout,
            _ProbeTimeout(sequence=sequence, target=target),
        )

    async def _handle_suspicion_timeout(self, node_id: str) -> None:
        self._suspicion_timers.pop(node_id, None)
        await self._declare_dead(node_id)

    async def _handle_ping(self, from_node: str, ping: Ping) -> None:
        await self._send_to_node(from_node, Ack(sequence=ping.sequence))

        if from_node in self._members:
            self._members[from_node].last_seen = time()

    async def _handle_ack(self, from_node: str, ack: Ack) -> None:
        probe = self._pending_probes.get(ack.sequence)
        if probe is None:
            return

        if from_node == probe.target:
            del self._pending_probes[ack.sequence]
            if probe.target in self._members:
                member = self._members[probe.target]
                member.last_seen = time()
                if member.state == MemberState.SUSPECTED:
                    member.state = MemberState.ALIVE
                    if probe.target in self._suspicion_timers:
                        await self._ctx.cancel_schedule(self._suspicion_timers.pop(probe.target))
        elif from_node in probe.indirect_targets:
            probe.indirect_acks += 1

    async def _handle_ping_req(self, from_node: str, ping_req: PingReq) -> None:
        target = ping_req.target

        if target == self._node_id:
            await self._send_to_node(from_node, Ack(sequence=ping_req.sequence))
            return

        if target not in self._members:
            return

        member = self._members[target]
        if member.state != MemberState.ALIVE:
            return

        async def forward_and_respond() -> None:
            try:
                self._sequence += 1
                local_seq = self._sequence
                await self._send_to_node(target, Ping(sequence=local_seq))
                await asyncio.sleep(self._config.ping_timeout)
            except Exception:
                pass

        self._ctx.detach(forward_and_respond()).discard()
        await self._send_to_node(target, Ping(sequence=ping_req.sequence))

    async def _handle_suspect(self, suspect: Suspect) -> None:
        if suspect.node_id == self._node_id:
            if suspect.incarnation >= self._incarnation:
                self._incarnation = suspect.incarnation + 1
                address = self._members[self._node_id].address
                await self._broadcast(
                    Alive(
                        node_id=self._node_id,
                        incarnation=self._incarnation,
                        address=address,
                    )
                )
            return

        if suspect.node_id not in self._members:
            return

        member = self._members[suspect.node_id]
        if suspect.incarnation > member.incarnation:
            member.incarnation = suspect.incarnation
            if member.state == MemberState.ALIVE:
                await self._start_suspicion(suspect.node_id)

    async def _handle_alive(self, alive: Alive) -> None:
        if alive.node_id == self._node_id:
            return

        if alive.node_id not in self._members:
            self._members[alive.node_id] = MemberInfo(
                node_id=alive.node_id,
                address=alive.address,
                state=MemberState.ALIVE,
                incarnation=alive.incarnation,
            )
            self._hash_ring.add_node(alive.node_id)
            self._member_order.append(alive.node_id)
            await self._broadcast_event(NodeJoined(node_id=alive.node_id, address=alive.address))
            return

        member = self._members[alive.node_id]
        if alive.incarnation > member.incarnation:
            member.incarnation = alive.incarnation
            member.address = alive.address
            member.last_seen = time()

            if member.state == MemberState.SUSPECTED:
                member.state = MemberState.ALIVE
                if alive.node_id in self._suspicion_timers:
                    await self._ctx.cancel_schedule(self._suspicion_timers.pop(alive.node_id))

    async def _handle_dead(self, dead: Dead) -> None:
        if dead.node_id == self._node_id:
            return

        if dead.node_id in self._members:
            await self._declare_dead(dead.node_id)

    async def _start_suspicion(self, node_id: str) -> None:
        if node_id not in self._members:
            return

        if node_id == self._node_id:
            return

        member = self._members[node_id]
        if member.state == MemberState.DEAD:
            return

        if member.state == MemberState.SUSPECTED:
            return

        member.state = MemberState.SUSPECTED

        await self._broadcast(Suspect(node_id=node_id, incarnation=member.incarnation))

        suspicion_timeout = self._config.protocol_period * self._config.suspicion_mult
        task_id = await self._ctx.schedule(suspicion_timeout, _SuspicionTimeout(node_id=node_id))
        self._suspicion_timers[node_id] = task_id

    async def _declare_dead(self, node_id: str) -> None:
        if node_id not in self._members:
            return

        if node_id == self._node_id:
            return

        member = self._members.pop(node_id)
        self._hash_ring.remove_node(node_id)
        if node_id in self._member_order:
            self._member_order.remove(node_id)

        if node_id in self._suspicion_timers:
            await self._ctx.cancel_schedule(self._suspicion_timers.pop(node_id))

        await self._broadcast(Dead(node_id=node_id))

        if member.state == MemberState.SUSPECTED:
            await self._broadcast_event(NodeFailed(node_id=node_id))
        else:
            await self._broadcast_event(NodeLeft(node_id=node_id))

    async def _send_to_node(self, node_id: str, payload: Any) -> None:
        if self._transport and node_id in self._members:
            await self._transport.send(TransportSend(node_id=node_id, payload=payload))

    async def _broadcast(self, payload: Any) -> None:
        for node_id in self._members:
            if node_id != self._node_id:
                await self._send_to_node(node_id, payload)

    async def _broadcast_event(self, event: ClusterEvent) -> None:
        for subscriber in self._subscribers:
            await subscriber.send(event)

    async def _broadcast_to_subscribers(self, payload: Any) -> None:
        for subscriber in self._subscribers:
            await subscriber.send(payload)

    async def _handle_register_clustered_actor(
        self,
        actor_id: str,
        actor_cls: type,
        replication: int,
        singleton: bool,
        ctx: Context,
    ) -> None:
        from .consistency import resolve_replication

        total_nodes = len(self._members)
        effective_replication = resolve_replication(replication, total_nodes)

        actor_cls_name = f"{actor_cls.__module__}.{actor_cls.__qualname__}"

        info = _ClusteredActorInfo(
            actor_id=actor_id,
            actor_cls=actor_cls,
            actor_cls_name=actor_cls_name,
            replication=effective_replication,
            singleton=singleton,
            owner_node=self._node_id,
        )
        self._clustered_actors[actor_id] = info

        preference_list = self._hash_ring.get_preference_list(actor_id, effective_replication)

        if self._node_id in preference_list:
            ref = await ctx.spawn(actor_cls)
            self._local_actors[actor_id] = ref

        await self._broadcast(ActorRegistered(
            actor_id=actor_id,
            actor_cls_name=actor_cls_name,
            replication=effective_replication,
            singleton=singleton,
            owner_node=self._node_id,
        ))

    async def _handle_actor_registered(self, msg: ActorRegistered) -> None:
        if msg.actor_id in self._clustered_actors:
            return

        actor_cls = self._resolve_actor_class(msg.actor_cls_name)

        info = _ClusteredActorInfo(
            actor_id=msg.actor_id,
            actor_cls=actor_cls,
            actor_cls_name=msg.actor_cls_name,
            replication=msg.replication,
            singleton=msg.singleton,
            owner_node=msg.owner_node,
        )
        self._clustered_actors[msg.actor_id] = info

        if actor_cls is not None:
            preference_list = self._hash_ring.get_preference_list(msg.actor_id, msg.replication)
            if self._node_id in preference_list:
                ref = await self._ctx.spawn(actor_cls)
                self._local_actors[msg.actor_id] = ref

    def _resolve_actor_class(self, fqn: str) -> type | None:
        try:
            parts = fqn.rsplit(".", 1)
            if len(parts) != 2:
                return None
            module_path, class_name = parts
            module = importlib.import_module(module_path)
            return getattr(module, class_name, None)
        except Exception:
            return None

    async def _handle_clustered_send(
        self,
        send: ClusteredSend,
        ctx: Context,
    ) -> None:
        info = self._clustered_actors.get(send.actor_id)
        if info is None:
            ctx.reply(None)
            return

        replication = info.replication
        preference_list = self._hash_ring.get_preference_list(send.actor_id, replication)

        required_acks = send.consistency
        if required_acks == -1:
            required_acks = len(preference_list)
        elif required_acks == -2:
            required_acks = len(preference_list) // 2 + 1

        if required_acks <= 1:
            for target_node in preference_list:
                if target_node == self._node_id:
                    await self._process_local_send(send)
                else:
                    await self._send_to_node(target_node, send)
            ctx.reply(None)
            return

        self._pending_sends[send.request_id] = (required_acks, 0, ctx)

        local_processed = False
        for target_node in preference_list:
            if target_node == self._node_id:
                await self._process_local_send(send)
                local_processed = True
            else:
                await self._send_to_node(target_node, send)

        if local_processed:
            await self._record_send_ack(send.request_id)

    async def _handle_remote_clustered_send(self, from_node: str, send: ClusteredSend) -> None:
        await self._process_local_send(send)
        await self._send_to_node(from_node, ClusteredSendAck(request_id=send.request_id, success=True))

    async def _process_local_send(self, send: ClusteredSend) -> None:
        local_ref = self._local_actors.get(send.actor_id)
        if local_ref is None:
            return

        info = self._clustered_actors.get(send.actor_id)
        node = self._ctx.system._supervision_tree.get_node(local_ref.id)

        state_before = None
        if info and node:
            actor = node.actor_instance
            state_before = copy.deepcopy(actor.get_state())

        msg = self._deserialize_payload(send.payload_type, send.payload)
        await local_ref.send(msg)
        await asyncio.sleep(0)

        if info and node and state_before is not None:
            actor = node.actor_instance
            state_after = actor.get_state()
            if state_before != state_after:
                info.version += 1

    async def _handle_clustered_send_ack(self, ack: ClusteredSendAck) -> None:
        await self._record_send_ack(ack.request_id)

    async def _record_send_ack(self, request_id: str) -> None:
        pending = self._pending_sends.get(request_id)
        if pending is None:
            return

        required, received, ctx = pending
        received += 1

        if received >= required:
            self._pending_sends.pop(request_id, None)
            ctx.reply(None)
        else:
            self._pending_sends[request_id] = (required, received, ctx)

    async def _handle_clustered_ask(
        self,
        actor_id: str,
        request_id: str,
        payload_type: str,
        payload: bytes,
        consistency: int,
        ctx: Context,
    ) -> None:
        info = self._clustered_actors.get(actor_id)
        if info is None:
            ctx.reply(None)
            return

        local_ref = self._local_actors.get(actor_id)
        if local_ref is None:
            target_node = self._hash_ring.get_node(actor_id)
            if target_node and target_node != self._node_id and target_node in self._members:
                result = await self._forward_ask_to_node(
                    target_node,
                    ClusteredAsk(
                        actor_id=actor_id,
                        request_id=request_id,
                        payload_type=payload_type,
                        payload=payload,
                        consistency=consistency,
                    ),
                )
                ctx.reply(result)
            else:
                ctx.reply(None)
            return

        msg = self._deserialize_payload(payload_type, payload)
        result = await local_ref.ask(msg)
        ctx.reply(result)

    async def _handle_remote_clustered_ask(self, from_node: str, ask: ClusteredAsk) -> None:
        local_ref = self._local_actors.get(ask.actor_id)
        if local_ref is None:
            await self._send_to_node(
                from_node,
                ClusteredAskResponse(
                    request_id=ask.request_id,
                    payload_type="",
                    payload=b"",
                    success=False,
                ),
            )
            return

        msg = self._deserialize_payload(ask.payload_type, ask.payload)
        result = await local_ref.ask(msg)

        result_type = f"{type(result).__module__}.{type(result).__qualname__}"
        result_bytes = msgpack.packb(result if isinstance(result, (int, float, str, bool, type(None))) else result.__dict__, use_bin_type=True)

        await self._send_to_node(
            from_node,
            ClusteredAskResponse(
                request_id=ask.request_id,
                payload_type=result_type,
                payload=result_bytes,
                success=True,
            ),
        )

    async def _forward_ask_to_node(self, target_node: str, ask: ClusteredAsk) -> Any:
        request_id = f"{self._node_id}-{uuid4().hex[:8]}"
        ask_with_id = ClusteredAsk(
            actor_id=ask.actor_id,
            request_id=request_id,
            payload_type=ask.payload_type,
            payload=ask.payload,
            consistency=ask.consistency,
        )

        future: asyncio.Future[Any] = asyncio.Future()
        self._pending_asks[request_id] = future

        await self._send_to_node(target_node, ask_with_id)

        try:
            return await asyncio.wait_for(future, timeout=5.0)
        except asyncio.TimeoutError:
            return None
        finally:
            self._pending_asks.pop(request_id, None)

    async def _handle_clustered_ask_response(self, response: ClusteredAskResponse) -> None:
        future = self._pending_asks.get(response.request_id)
        if future is None:
            return

        if not response.success:
            future.set_result(None)
            return

        if response.payload_type in ("builtins.int", "builtins.float", "builtins.str", "builtins.bool", "builtins.NoneType"):
            result = msgpack.unpackb(response.payload, raw=False)
        else:
            result = self._deserialize_payload(response.payload_type, response.payload)

        future.set_result(result)

    def _deserialize_payload(self, payload_type: str, payload: bytes) -> Any:
        parts = payload_type.rsplit(".", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid payload type: {payload_type}")

        module_path, class_name = parts
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)

        data = msgpack.unpackb(payload, raw=False)
        return cls(**data)
