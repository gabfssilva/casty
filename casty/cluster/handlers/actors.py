"""Local actor registry handler mixin."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

import msgpack

from casty import Context, LocalRef
from casty.actor import on
from casty.transport import Send, ProtocolType
from ..codec import ActorCodec, AskRequest, AskResponse
from ..messages import (
    RemoteSend,
    RemoteAsk,
    LocalRegister,
    LocalUnregister,
    GetRemoteRef,
    GetMembers,
    GetLocalActors,
    ActorRegistered,
    ActorUnregistered,
)
from ..remote_ref import RemoteRef, RemoteActorError
from ..state import PendingAsk

if TYPE_CHECKING:
    from ..state import ClusterState

log = logging.getLogger(__name__)


class ActorHandlers:
    """Handles local actor registration and remote actor routing."""

    state: ClusterState

    @on(LocalRegister)
    async def handle_local_register(self, msg: LocalRegister, ctx: Context) -> None:
        """Handle local actor registration."""
        self.state.local_actors[msg.actor_name] = msg.actor_ref
        self.state.actor_locations[msg.actor_name] = self.state.node_id

        if self.state.gossip:
            from casty.gossip import Publish

            await self.state.gossip.send(
                Publish(key=f"_actors/{msg.actor_name}", value=self.state.node_id.encode("utf-8"))
            )

        log.info(f"Registered local actor: {msg.actor_name}")
        await self._broadcast_event(ActorRegistered(msg.actor_name, self.state.node_id))

    @on(LocalUnregister)
    async def handle_local_unregister(self, msg: LocalUnregister, ctx: Context) -> None:
        """Handle local actor unregistration."""
        self.state.local_actors.pop(msg.actor_name, None)
        self.state.actor_locations.pop(msg.actor_name, None)

        if self.state.gossip:
            from casty.gossip import Delete

            await self.state.gossip.send(Delete(key=f"_actors/{msg.actor_name}"))

        log.info(f"Unregistered local actor: {msg.actor_name}")
        await self._broadcast_event(ActorUnregistered(msg.actor_name, self.state.node_id))

    @on(GetRemoteRef)
    async def handle_get_remote_ref(self, msg: GetRemoteRef, ctx: Context) -> None:
        """Handle get remote reference request."""
        node_id = msg.node_id
        if node_id is None:
            node_id = self.state.actor_locations.get(msg.actor_name)

        if node_id is None:
            ctx.reply(None)
            return

        remote_ref = RemoteRef(
            actor_name=msg.actor_name,
            node_id=node_id,
            cluster=ctx.self_ref,
        )
        ctx.reply(remote_ref)

    @on(GetMembers)
    async def handle_get_members(self, msg: GetMembers, ctx: Context) -> None:
        """Handle get members request."""
        ctx.reply(dict(self.state.actor_locations))

    @on(GetLocalActors)
    async def handle_get_local_actors(self, msg: GetLocalActors, ctx: Context) -> None:
        """Handle get local actors request."""
        ctx.reply(dict(self.state.local_actors))

    @on(RemoteSend)
    async def handle_remote_send(self, msg: RemoteSend, ctx: Context) -> None:
        """Handle remote send request."""
        payload_type = f"{type(msg.payload).__module__}.{type(msg.payload).__qualname__}"
        payload_bytes = msgpack.packb(msg.payload.__dict__, use_bin_type=True)

        wire_msg = AskRequest(
            target_actor=msg.target_actor,
            payload_type=payload_type,
            payload=payload_bytes,
        )
        data = ActorCodec.encode(wire_msg)
        if self.state.mux:
            await self.state.mux.send(Send(ProtocolType.ACTOR, msg.target_node, data))

    @on(RemoteAsk)
    async def handle_remote_ask(self, msg: RemoteAsk, ctx: Context) -> None:
        """Handle remote ask request."""
        self.state.request_counter += 1
        request_id = f"{self.state.node_id}-{self.state.request_counter}"

        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()

        self.state.pending_asks[request_id] = PendingAsk(
            request_id=request_id,
            target_actor=msg.target_actor,
            target_node=msg.target_node,
            future=future,
        )

        payload_type = f"{type(msg.payload).__module__}.{type(msg.payload).__qualname__}"
        payload_bytes = msgpack.packb(msg.payload.__dict__, use_bin_type=True)

        wire_msg = AskRequest(
            request_id=request_id,
            target_actor=msg.target_actor,
            payload_type=payload_type,
            payload=payload_bytes,
        )
        data = ActorCodec.encode(wire_msg)
        if self.state.mux:
            await self.state.mux.send(Send(ProtocolType.ACTOR, msg.target_node, data))

        try:
            result = await asyncio.wait_for(future, timeout=self.state.config.ask_timeout)
            ctx.reply(result)
        except asyncio.TimeoutError:
            self.state.pending_asks.pop(request_id, None)
            ctx.reply(None)
        except Exception as e:
            self.state.pending_asks.pop(request_id, None)
            raise e

    async def _broadcast_event(self, event) -> None:
        """Broadcast cluster event to subscribers."""
        # This is implemented in cluster.py
        pass
