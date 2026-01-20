from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any

from casty.system import LocalActorSystem
from casty.actor import Behavior
from casty.protocols import System
from casty.ref import ActorRef, LocalActorRef
from casty.envelope import Envelope
from casty.mailbox import ActorMailbox
from casty.serializable import serialize
from casty.state import State
from .replication import ReplicationConfig, Routing, Replicator, replication_filter

from .router import router_actor, RegisterPending, SetReferences
from .inbound import inbound_actor, GetPort
from .outbound import outbound_actor
from .membership import membership_actor
from .swim import swim_actor
from .gossip import gossip_actor
from .transport_messages import Register, Connect, Transmit, RegisterReplication, SpawnReplica, RegisterReplicators
from .messages import Join, GetAliveMembers, GetResponsibleNodes, SetLocalAddress
from .registry import register_behavior


class ClusteredActorSystem(System):
    def __init__(
        self,
        node_id: str,
        host: str = "127.0.0.1",
        port: int = 0,
        seeds: list[str] | None = None,
        debug_filter: Any = None,
    ) -> None:
        self._node_id = node_id
        self._host = host
        self._port = port
        self._seeds = seeds or []
        self._debug_filter = debug_filter
        self._system = LocalActorSystem(node_id=node_id, debug_filter=debug_filter)
        self._router_ref: ActorRef | None = None
        self._inbound_ref: ActorRef | None = None
        self._outbound_ref: ActorRef | None = None
        self._membership_ref: ActorRef | None = None
        self._swim_ref: ActorRef | None = None
        self._gossip_ref: ActorRef | None = None
        self._actual_port: int = 0
        self._pending_asks: dict[str, asyncio.Future] = {}
        self._replicators: dict[str, Replicator] = {}

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def port(self) -> int:
        return self._actual_port

    @property
    def address(self) -> str:
        return f"{self._host}:{self._actual_port}"

    async def start(self) -> None:
        self._membership_ref = await self._system.actor(
            membership_actor(self._node_id), name="membership"
        )

        self._router_ref = await self._system.actor(
            router_actor(), name="router"
        )

        await self._router_ref.send(RegisterPending(pending=self._pending_asks))
        await self._router_ref.send(RegisterReplicators(replicators=self._replicators))

        self._inbound_ref = await self._system.actor(
            inbound_actor(self._host, self._port, self._router_ref),
            name="inbound"
        )
        self._actual_port = await self._inbound_ref.ask(GetPort())

        await self._membership_ref.send(SetLocalAddress(f"{self._host}:{self._actual_port}"))

        self._outbound_ref = await self._system.actor(
            outbound_actor(self._router_ref),
            name="outbound"
        )

        await self._router_ref.send(SetReferences(
            membership_ref=self._membership_ref,
            outbound_ref=self._outbound_ref,
        ))

        await self._router_ref.send(Register(ref=self._membership_ref))

        self._swim_ref = await self._system.actor(
            swim_actor(
                node_id=self._node_id,
                membership_ref=self._membership_ref,
                outbound_ref=self._outbound_ref,
                probe_interval=1.0,
                probe_timeout=0.5,
                ping_req_fanout=3,
            ),
            name="swim"
        )
        await self._router_ref.send(Register(ref=self._swim_ref))

        self._gossip_ref = await self._system.actor(
            gossip_actor(
                node_id=self._node_id,
                membership_ref=self._membership_ref,
                outbound_ref=self._outbound_ref,
                fanout=3,
            ),
            name="gossip"
        )
        await self._router_ref.send(Register(ref=self._gossip_ref))

        for seed in self._seeds:
            await self.connect_to(seed)

    async def connect_to(self, address: str, *, node_id: str | None = None) -> None:
        temp_node_id = node_id
        if temp_node_id is None:
            host, port_str = address.rsplit(":", 1)
            temp_node_id = f"node-{host}:{port_str}"

        conn = await self._outbound_ref.ask(Connect(
            node_id=temp_node_id,
            address=address,
        ))

        if conn:
            import uuid
            correlation_id = uuid.uuid4().hex
            future: asyncio.Future[Join] = asyncio.Future()
            self._pending_asks[correlation_id] = future

            envelope = Envelope(
                payload=Join(node_id=self._node_id, address=f"{self._host}:{self._actual_port}"),
                target="membership_actor/membership",
                correlation_id=correlation_id,
            )
            await conn.send(Transmit(data=serialize(envelope)))

            try:
                response = await asyncio.wait_for(future, timeout=5.0)
                if isinstance(response, Join):
                    await self._membership_ref.send(response)
            except asyncio.TimeoutError:
                if node_id is not None:
                    await self._membership_ref.send(Join(node_id=node_id, address=address))
            finally:
                self._pending_asks.pop(correlation_id, None)

    async def shutdown(self) -> None:
        await self._system.shutdown()

    async def actor[M](
        self,
        behavior: Behavior,
        *,
        name: str,
        replicas: int | None = None,
        write_quorum: int | None = None,
        routing: Routing | None = None,
    ) -> ActorRef[M]:
        base_config = getattr(behavior.func, "__replication__", None)
        actor_id = f"{behavior.func.__name__}/{name}"

        if base_config or replicas is not None:
            config = base_config or ReplicationConfig()
            if replicas is not None:
                config = replace(config, factor=replicas)

            replica_nodes = await self._membership_ref.ask(GetResponsibleNodes(actor_id, config.factor))
            is_leader = len(replica_nodes) > 0 and replica_nodes[0] == self._node_id

            if actor_id in self._system._actors:
                if is_leader:
                    return self._system._actors[actor_id]
                else:
                    from .remote_ref import RemoteActorRef
                    return RemoteActorRef(
                        actor_id=actor_id,
                        node_id=replica_nodes[0],
                        connection=None,
                        outbound_ref=self._outbound_ref,
                        membership_ref=self._membership_ref,
                        pending_asks=self._pending_asks,
                    )

            if write_quorum is not None:
                config = replace(config, write_quorum=write_quorum)
            if routing is not None:
                config = replace(config, routing=routing)

            behavior_name = f"{behavior.func.__module__}.{behavior.func.__name__}"
            register_behavior(behavior_name, behavior)

            other_replicas = [n for n in replica_nodes if n != self._node_id]

            if not is_leader:
                leader_node = replica_nodes[0]
                await self._send_to_node(leader_node, SpawnReplica(
                    actor_id=actor_id,
                    behavior_name=behavior_name,
                    initial_args=behavior.initial_args,
                    initial_kwargs=behavior.initial_kwargs,
                    config=config,
                    is_leader=True,
                ))

                from .remote_ref import RemoteActorRef
                return RemoteActorRef(
                    actor_id=actor_id,
                    node_id=leader_node,
                    connection=None,
                    outbound_ref=self._outbound_ref,
                    membership_ref=self._membership_ref,
                    pending_asks=self._pending_asks,
                )

            for node_id in other_replicas:
                await self._send_to_node(node_id, SpawnReplica(
                    actor_id=actor_id,
                    behavior_name=behavior_name,
                    initial_args=behavior.initial_args,
                    initial_kwargs=behavior.initial_kwargs,
                    config=config,
                    is_leader=False,
                ))

            state: State[Any] | None = None
            if behavior.state_param is not None:
                state = State(behavior.state_initial)

            filters = []
            if self._debug_filter:
                filters.append(self._debug_filter)
            if is_leader and other_replicas:
                replicator = Replicator(
                    actor_id=actor_id,
                    config=config,
                    replica_nodes=other_replicas,
                    send_fn=self._send_replicate,
                    node_id=self._node_id,
                )
                self._replicators[actor_id] = replicator
                filters.append(replication_filter(replicator))

            mailbox: ActorMailbox[M] = ActorMailbox(
                state=state,
                filters=filters,
                self_id=actor_id,
                node_id=self._node_id,
                is_leader=is_leader,
                system=self._system,
            )

            ref: LocalActorRef[M] = LocalActorRef(actor_id=actor_id, mailbox=mailbox)
            mailbox.set_self_ref(ref)

            task = asyncio.create_task(
                self._run_actor(behavior, mailbox, state)
            )

            self._system._actors[actor_id] = ref
            self._system._tasks[actor_id] = task
            self._system._mailboxes[actor_id] = mailbox

            await self._router_ref.send(Register(ref=ref))
            await self._router_ref.send(RegisterReplication(
                actor_id=actor_id,
                config=config,
            ))

            return ref
        else:
            ref = await self._system.actor(behavior, name=name)
            await self._router_ref.send(Register(ref=ref))
            return ref

    async def _run_actor(
        self,
        behavior: Behavior,
        mailbox: ActorMailbox[Any],
        state: State[Any] | None,
    ) -> None:
        kwargs = dict(behavior.initial_kwargs)
        kwargs["mailbox"] = mailbox

        if behavior.state_param is not None and state is not None:
            kwargs[behavior.state_param] = state

        try:
            await behavior.func(*behavior.initial_args, **kwargs)
        except StopAsyncIteration:
            pass

    async def _send_to_node(self, node_id: str, msg: Any, target: str = "router_actor/router") -> None:
        members = await self._membership_ref.ask(GetAliveMembers())
        if node_id not in members:
            return

        address = members[node_id].address
        conn = await self._outbound_ref.ask(Connect(node_id=node_id, address=address))
        if conn:
            envelope = Envelope(payload=msg, target=target)
            await conn.send(Transmit(data=serialize(envelope)))

    async def _send_replicate(self, node_id: str, msg: Any) -> None:
        await self._send_to_node(node_id, msg)

    async def __aenter__(self) -> "ClusteredActorSystem":
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.shutdown()
