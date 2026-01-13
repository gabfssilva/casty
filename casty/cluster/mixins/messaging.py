"""Remote actor messaging mixin: lookup, ask, tell."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from ...actor import ActorRef
from ..remote import RemoteRef
from ..serialize import deserialize, serialize
from ..transport import MessageType, Transport, WireMessage

if TYPE_CHECKING:
    from ..distributed import ClusterState

log = logging.getLogger(__name__)


class MessagingMixin:
    """Remote actor messaging: lookup, ask, tell."""

    _cluster: ClusterState

    def _get_local_ref(self, name: str) -> ActorRef | None:
        """Get a local actor ref by name (checking both registries)."""
        if name in self._cluster.registry:
            return self._cluster.registry[name]
        if name in self._cluster.singleton_local_refs:
            return self._cluster.singleton_local_refs[name]
        return None

    async def _deliver_remote_message(self, msg: WireMessage) -> None:
        """Deliver a message from a remote peer to a local actor."""
        target_name = msg.target_name
        ref = self._get_local_ref(target_name)
        if ref is None:
            log.warning(f"Actor '{target_name}' not found for remote message")
            return
        try:
            payload = deserialize(msg.payload, self._cluster.type_registry)
            await ref.send(payload)
        except Exception as e:
            log.error(f"Failed to deliver remote message: {e}")

    async def _handle_lookup_request(
        self,
        transport: Transport,
        msg: WireMessage,
    ) -> None:
        """Handle a lookup request from a peer."""
        target_name = msg.target_name
        found = self._get_local_ref(target_name) is not None

        response = WireMessage(
            msg_type=MessageType.LOOKUP_RES,
            target_name=target_name,
            payload=b"\x01" if found else b"\x00",
        )
        await transport.send(response)

    def _handle_lookup_response(self, msg: WireMessage) -> None:
        """Handle a lookup response from a peer."""
        target_name = msg.target_name
        found = msg.payload == b"\x01"

        if target_name in self._cluster.pending_lookups:
            future = self._cluster.pending_lookups.pop(target_name)
            if not future.done():
                future.set_result(found)

    async def _handle_ask_request(
        self,
        transport: Transport,
        msg: WireMessage,
    ) -> None:
        """Handle an ask request from a peer."""
        target_name = msg.target_name

        # Payload format: request_id (36 bytes UUID) + message
        request_id = msg.payload[:36].decode("utf-8")
        message_data = msg.payload[36:]

        ref = self._get_local_ref(target_name)
        if ref is None:
            log.warning(f"Actor '{target_name}' not found for remote ask")
            response = WireMessage(
                msg_type=MessageType.ASK_RES,
                target_name=request_id,
                payload=b"\x00",
            )
            await transport.send(response)
            return
        try:
            payload = deserialize(message_data, self._cluster.type_registry)
            result = await ref.ask(payload)
            response_data = b"\x01" + serialize(result)
        except Exception as e:
            log.error(f"Failed to process remote ask: {e}")
            response_data = b"\x00"

        response = WireMessage(
            msg_type=MessageType.ASK_RES,
            target_name=request_id,
            payload=response_data,
        )
        await transport.send(response)

    def _handle_ask_response(self, msg: WireMessage) -> None:
        """Handle an ask response from a peer."""
        request_id = msg.target_name

        if request_id not in self._cluster.pending_asks:
            return

        future = self._cluster.pending_asks.pop(request_id)
        if future.done():
            return

        if msg.payload[:1] == b"\x00":
            future.set_exception(RuntimeError("Remote ask failed"))
        else:
            try:
                result = deserialize(msg.payload[1:], self._cluster.type_registry)
                future.set_result(result)
            except Exception as e:
                future.set_exception(e)

    async def lookup[T](self, name: str, *, singleton: bool = False) -> ActorRef[T]:
        """Look up an actor by name.

        Args:
            name: The actor name to look up.
            singleton: If True, looks up in the singleton registry (global).
                      If False, looks up in local registry first, then peers.
        """
        if singleton:
            return await self._lookup_singleton(name)  # type: ignore

        # Local first
        if name in self._cluster.registry:
            return self._cluster.registry[name]  # type: ignore

        # Search in peers
        for transport in self._cluster.peers.values():
            loop = asyncio.get_running_loop()
            future: asyncio.Future[bool] = loop.create_future()
            self._cluster.pending_lookups[name] = future

            req = WireMessage(
                msg_type=MessageType.LOOKUP_REQ,
                target_name=name,
                payload=b"",
            )
            await transport.send(req)

            try:
                found = await asyncio.wait_for(future, timeout=2.0)
                if found:
                    return RemoteRef(
                        name,
                        transport,
                        self._cluster.type_registry,
                        self._cluster.pending_asks,
                    )
            except asyncio.TimeoutError:
                self._cluster.pending_lookups.pop(name, None)
                continue

        raise KeyError(f"Actor '{name}' not found in cluster")

    # Registry sync
    async def _handle_registry_sync_request(self, transport: Transport, msg: WireMessage) -> None:
        """Leader sends registry to requesting follower."""
        if self._cluster.state != "leader":
            return

        names = list(self._cluster.registry.keys())
        response = WireMessage(
            msg_type=MessageType.REGISTRY_SYNC_RES,
            target_name=msg.target_name,
            payload=serialize(names),
        )
        await transport.send(response)

    def _handle_registry_sync_response(self, msg: WireMessage) -> None:
        """Follower receives registry names from leader."""
        # For now, we just use this to know what actors exist on leader
        # The actual actor lookup still goes through the normal lookup flow
        pass
