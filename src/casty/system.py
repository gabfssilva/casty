# src/casty/system.py
from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from casty.actor import Behavior
from casty.cell import ActorCell
from casty.address import ActorAddress
from casty.events import EventStream
from casty.mailbox import Mailbox
from casty.ref import ActorRef
from casty.transport import LocalTransport, MessageTransport


class CallbackTransport:
    """Lightweight transport for temporary ask refs."""

    def __init__(self, callback: Callable[[Any], None]) -> None:
        self._callback = callback

    def deliver(self, address: ActorAddress, msg: Any) -> None:
        self._callback(msg)


class ActorSystem:
    def __init__(self, name: str = "casty-system") -> None:
        self._name = name
        self._event_stream = EventStream()
        self._root_cells: dict[str, ActorCell[Any]] = {}
        self._local_transport = LocalTransport()

    @property
    def name(self) -> str:
        return self._name

    @property
    def event_stream(self) -> EventStream:
        return self._event_stream

    async def __aenter__(self) -> ActorSystem:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.shutdown()

    def spawn[M](
        self,
        behavior: Behavior[M],
        name: str,
        *,
        mailbox: Mailbox[M] | None = None,
    ) -> ActorRef[M]:
        if name in self._root_cells:
            raise ValueError(f"Root actor '{name}' already exists")

        cell: ActorCell[M] = ActorCell(
            behavior=behavior,
            name=name,
            parent=None,
            event_stream=self._event_stream,
            system_name=self._name,
            local_transport=self._local_transport,
            ref_transport=self._get_ref_transport(),
            ref_host=self._get_ref_host(),
            ref_port=self._get_ref_port(),
        )
        if mailbox is not None:
            cell.mailbox = mailbox
        self._root_cells[name] = cell
        asyncio.get_running_loop().create_task(cell.start())
        return cell.ref

    async def ask[M, R](
        self,
        ref: ActorRef[M],
        msg_factory: Callable[[ActorRef[R]], M],
        *,
        timeout: float,
    ) -> R:
        future: asyncio.Future[R] = asyncio.get_running_loop().create_future()

        def on_reply(msg: R) -> None:
            if not future.done():
                future.set_result(msg)

        temp_ref: ActorRef[R] = ActorRef(
            address=ActorAddress(system=self._name, path=f"/_temp/{id(future)}"),
            _transport=CallbackTransport(on_reply),
        )
        message = msg_factory(temp_ref)
        ref.tell(message)

        return await asyncio.wait_for(future, timeout=timeout)

    def resolve(self, address: ActorAddress) -> ActorRef[Any] | None:
        """Resolve an ActorAddress to a local ActorRef, or None if not found."""
        if address.is_local or address.host is None:
            return self.lookup(address.path)
        return None

    def lookup(self, path: str) -> ActorRef[Any] | None:
        # Strip leading slash
        parts = path.strip("/").split("/")
        if not parts:
            return None

        root_name = parts[0]
        cell = self._root_cells.get(root_name)
        if cell is None:
            return None

        # Navigate children for nested paths
        for part in parts[1:]:
            child = cell.children.get(part)
            if child is None:
                return None
            cell = child

        return cell.ref

    def _get_ref_transport(self) -> MessageTransport | None:
        return None

    def _get_ref_host(self) -> str | None:
        return None

    def _get_ref_port(self) -> int | None:
        return None

    async def shutdown(self) -> None:
        for cell in list(self._root_cells.values()):
            await cell.stop()
        self._root_cells.clear()
