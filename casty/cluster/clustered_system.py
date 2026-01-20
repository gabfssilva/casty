from __future__ import annotations

import asyncio
from typing import Any

from casty.system import LocalActorSystem
from casty.actor import Behavior
from casty.protocols import System
from casty.ref import ActorRef
from casty.envelope import Envelope
from casty.serializable import serialize

from .router import router_actor, RegisterPending
from .inbound import inbound_actor, GetPort
from .outbound import outbound_actor
from .membership import membership_actor
from .swim import swim_actor
from .gossip import gossip_actor
from .transport_messages import Register, Connect, Transmit
from .messages import Join


class ClusteredActorSystem(System):
    def __init__(
        self,
        node_id: str,
        host: str = "127.0.0.1",
        port: int = 0,
        seeds: list[str] | None = None,
    ) -> None:
        self._node_id = node_id
        self._host = host
        self._port = port
        self._seeds = seeds or []
        self._system = LocalActorSystem(node_id=node_id)
        self._router_ref: ActorRef | None = None
        self._inbound_ref: ActorRef | None = None
        self._outbound_ref: ActorRef | None = None
        self._membership_ref: ActorRef | None = None
        self._swim_ref: ActorRef | None = None
        self._gossip_ref: ActorRef | None = None
        self._actual_port: int = 0
        self._pending_asks: dict[str, asyncio.Future] = {}

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
        self._router_ref = await self._system.actor(
            router_actor(), name="router"
        )

        await self._router_ref.send(RegisterPending(pending=self._pending_asks))

        self._membership_ref = await self._system.actor(
            membership_actor({}), name="membership"
        )

        self._inbound_ref = await self._system.actor(
            inbound_actor(self._host, self._port, self._router_ref),
            name="inbound"
        )
        self._actual_port = await self._inbound_ref.ask(GetPort())

        self._outbound_ref = await self._system.actor(
            outbound_actor(self._router_ref),
            name="outbound"
        )

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

        self._gossip_ref = await self._system.actor(
            gossip_actor(
                node_id=self._node_id,
                membership_ref=self._membership_ref,
                outbound_ref=self._outbound_ref,
                fanout=3,
            ),
            name="gossip"
        )

        for seed in self._seeds:
            await self.connect_to(seed)

    async def connect_to(self, address: str) -> None:
        host, port_str = address.rsplit(":", 1)
        node_id = f"node-{host}:{port_str}"

        conn = await self._outbound_ref.ask(Connect(
            node_id=node_id,
            address=address,
        ))

        if conn:
            envelope = Envelope(
                payload=Join(node_id=self._node_id, address=f"{self._host}:{self._actual_port}"),
                target="membership_actor/membership",
            )
            await conn.send(Transmit(data=serialize(envelope)))

            await self._membership_ref.send(Join(node_id=node_id, address=address))

    async def shutdown(self) -> None:
        await self._system.shutdown()

    async def actor[M](
        self,
        behavior: Behavior,
        *,
        name: str,
    ) -> ActorRef[M]:
        ref = await self._system.actor(behavior, name=name)

        await self._router_ref.send(Register(ref=ref))

        return ref

    async def __aenter__(self) -> "ClusteredActorSystem":
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.shutdown()
