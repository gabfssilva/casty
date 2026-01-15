"""Cluster singleton actor handler mixin."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

import msgpack

from casty import Actor, Context
from casty.actor import on
from casty.transport import Send, ProtocolType
from ..codec import (
    ActorCodec,
    SingletonSend,
    SingletonAskRequest,
    SingletonAskResponse,
)
from ..messages import (
    RegisterSingleton,
    RemoteSingletonSend,
    RemoteSingletonAsk,
)
from ..singleton_ref import SingletonRef, SingletonError
from ..state import PendingAsk

if TYPE_CHECKING:
    from ..state import ClusterState

log = logging.getLogger(__name__)


class SingletonHandlers:
    """Handles singleton actor registration and routing."""

    state: ClusterState

    @on(RegisterSingleton)
    async def handle_register_singleton(
        self,
        msg: RegisterSingleton,
        ctx: Context,
    ) -> None:
        """Register a singleton actor type."""
        singleton_name = msg.singleton_name
        actor_cls = msg.actor_cls

        self.state.singleton_types[singleton_name] = actor_cls
        class_fqn = f"{actor_cls.__module__}.{actor_cls.__qualname__}"

        # Determine owner via consistent hashing
        owner = self.state.hash_ring.get_node(f"_singleton:{singleton_name}")

        if owner == self.state.node_id:
            # We own this singleton - spawn locally
            ref = await ctx.spawn(actor_cls)
            self.state.local_singletons[singleton_name] = ref
            self.state.singleton_locations[singleton_name] = self.state.node_id

            # Publish our ownership to the cluster
            if self.state.gossip:
                from casty.gossip import Publish

                await self.state.gossip.send(
                    Publish(key=f"_singletons/{singleton_name}", value=self.state.node_id.encode())
                )
            log.info(f"Spawned singleton {singleton_name} locally")
        else:
            # Remote owner - publish metadata so they can spawn
            if self.state.gossip:
                from casty.gossip import Publish

                await self.state.gossip.send(
                    Publish(key=f"_singleton_meta/{singleton_name}", value=class_fqn.encode())
                )
            log.debug(f"Published singleton metadata for {singleton_name}, owner: {owner}")

        # Return SingletonRef
        singleton_ref: SingletonRef[Any] = SingletonRef(singleton_name, ctx.self_ref)
        ctx.reply(singleton_ref)

    @on(RemoteSingletonSend)
    async def handle_remote_singleton_send(
        self,
        msg: RemoteSingletonSend,
        ctx: Context,
    ) -> None:
        """Route send to singleton owner."""
        singleton_name = msg.singleton_name
        payload = msg.payload

        owner = self.state.singleton_locations.get(singleton_name)

        if owner is None:
            # Use hash ring to determine owner
            owner = self.state.hash_ring.get_node(f"_singleton:{singleton_name}")

        if owner is None:
            log.warning(f"Cannot route singleton {singleton_name}: no owner found")
            return

        if owner == self.state.node_id:
            # Local delivery
            ref = self.state.local_singletons.get(singleton_name)
            if ref:
                await ref.send(payload)
            else:
                log.warning(f"Singleton {singleton_name} not found locally")
        else:
            # Forward to remote owner
            payload_type = f"{type(payload).__module__}.{type(payload).__qualname__}"
            payload_bytes = msgpack.packb(payload.__dict__, use_bin_type=True)
            actor_cls = self.state.singleton_types.get(singleton_name)
            actor_cls_fqn = f"{actor_cls.__module__}.{actor_cls.__qualname__}" if actor_cls else ""

            wire_msg = SingletonSend(
                singleton_name=singleton_name,
                payload_type=payload_type,
                payload=payload_bytes,
                actor_cls_fqn=actor_cls_fqn,
            )
            data = ActorCodec.encode(wire_msg)
            if self.state.mux:
                await self.state.mux.send(Send(ProtocolType.ACTOR, owner, data))

    @on(RemoteSingletonAsk)
    async def handle_remote_singleton_ask(
        self,
        msg: RemoteSingletonAsk,
        ctx: Context,
    ) -> None:
        """Route ask to singleton owner."""
        singleton_name = msg.singleton_name
        payload = msg.payload

        owner = self.state.singleton_locations.get(singleton_name)

        if owner is None:
            owner = self.state.hash_ring.get_node(f"_singleton:{singleton_name}")

        if owner is None:
            log.warning(f"Cannot route singleton {singleton_name}: no owner found")
            ctx.reply(None)
            return

        if owner == self.state.node_id:
            # Local delivery
            ref = self.state.local_singletons.get(singleton_name)
            if ref:
                result = await ref.ask(payload, timeout=self.state.config.ask_timeout)
                ctx.reply(result)
            else:
                log.warning(f"Singleton {singleton_name} not found locally")
                ctx.reply(None)
        else:
            # Forward to remote owner
            self.state.request_counter += 1
            request_id = f"{self.state.node_id}-singleton-{self.state.request_counter}"

            loop = asyncio.get_running_loop()
            future: asyncio.Future[Any] = loop.create_future()

            # Capture reply envelope
            reply_envelope = ctx._current_envelope

            self.state.pending_asks[request_id] = PendingAsk(
                request_id=request_id,
                target_actor=f"singleton:{singleton_name}",
                target_node=owner,
                future=future,
            )

            payload_type = f"{type(payload).__module__}.{type(payload).__qualname__}"
            payload_bytes = msgpack.packb(payload.__dict__, use_bin_type=True)
            actor_cls = self.state.singleton_types.get(singleton_name)
            actor_cls_fqn = f"{actor_cls.__module__}.{actor_cls.__qualname__}" if actor_cls else ""

            wire_msg = SingletonAskRequest(
                request_id=request_id,
                singleton_name=singleton_name,
                payload_type=payload_type,
                payload=payload_bytes,
                actor_cls_fqn=actor_cls_fqn,
            )
            data = ActorCodec.encode(wire_msg)
            if self.state.mux:
                await self.state.mux.send(Send(ProtocolType.ACTOR, owner, data))

            # Wait for response asynchronously
            async def wait_for_response() -> None:
                try:
                    result = await asyncio.wait_for(future, timeout=self.state.config.ask_timeout)
                    if reply_envelope and reply_envelope.reply_to:
                        reply_envelope.reply_to.set_result(result)
                except asyncio.TimeoutError:
                    self.state.pending_asks.pop(request_id, None)
                    if reply_envelope and reply_envelope.reply_to:
                        reply_envelope.reply_to.set_exception(
                            TimeoutError(f"Singleton ask timed out: {singleton_name}")
                        )
                except Exception as e:
                    if reply_envelope and reply_envelope.reply_to and not reply_envelope.reply_to.done():
                        reply_envelope.reply_to.set_exception(e)

            asyncio.create_task(wait_for_response())

    async def _maybe_spawn_singleton(self, singleton_name: str, ctx: Context) -> None:
        """Check if we should spawn a singleton and do so if we're the owner."""
        # Don't spawn if already owned
        if singleton_name in self.state.singleton_locations:
            return

        # Check if we're the owner
        owner = self.state.hash_ring.get_node(f"_singleton:{singleton_name}")
        if owner != self.state.node_id:
            return

        # Spawn the singleton
        actor_cls = self.state.singleton_types.get(singleton_name)
        if actor_cls and singleton_name not in self.state.local_singletons:
            ref = await ctx.spawn(actor_cls)
            self.state.local_singletons[singleton_name] = ref
            self.state.singleton_locations[singleton_name] = self.state.node_id

            # Publish our ownership
            if self.state.gossip:
                from casty.gossip import Publish

                await self.state.gossip.send(
                    Publish(key=f"_singletons/{singleton_name}", value=self.state.node_id.encode())
                )
            log.info(f"Spawned singleton {singleton_name} (owner via hash ring)")

    async def _check_singleton_failover(self, failed_node: str, ctx: Context) -> None:
        """Check if we should take over any singletons from a failed node."""
        for singleton_name in list(self.state.singleton_locations.keys()):
            if self.state.singleton_locations.get(singleton_name) == failed_node:
                # This singleton needs failover
                # Remove stale location
                del self.state.singleton_locations[singleton_name]

                # Check if we're the new owner
                new_owner = self.state.hash_ring.get_node(f"_singleton:{singleton_name}")

                if new_owner == self.state.node_id:
                    # We're the new owner - spawn locally
                    actor_cls = self.state.singleton_types.get(singleton_name)
                    if actor_cls:
                        ref = await ctx.spawn(actor_cls)
                        self.state.local_singletons[singleton_name] = ref
                        self.state.singleton_locations[singleton_name] = self.state.node_id

                        # Publish new location
                        if self.state.gossip:
                            from casty.gossip import Publish

                            await self.state.gossip.send(
                                Publish(key=f"_singletons/{singleton_name}", value=self.state.node_id.encode())
                            )
                        log.info(f"Singleton {singleton_name} failed over to this node")
                    else:
                        log.warning(f"Cannot failover singleton {singleton_name}: class not registered")
