from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .actor import Behavior
    from .ref import ActorRef


@dataclass
class Context:
    self_id: str
    sender: "ActorRef[Any] | None" = None
    node_id: str = "local"
    is_leader: bool = True
    _system: Any = field(default=None, repr=False)
    _self_ref: "ActorRef[Any] | None" = field(default=None, repr=False)
    reply_to: "asyncio.Future[Any] | None" = field(default=None, repr=False)

    async def reply(self, value: Any) -> None:
        if self.reply_to is not None:
            if not self.reply_to.done():
                self.reply_to.set_result(value)
            return

        from .reply import Reply
        if self.sender is not None:
            await self.sender.send(Reply(result=value))

    async def actor[M](
        self,
        behavior: "Behavior",
        *,
        name: str,
        replicas: int = 1,
    ) -> "ActorRef[M]":
        if self._system is None:
            raise RuntimeError("Context not bound to system")

        return await self._system._create_child(
            parent_id=self.self_id,
            behavior=behavior,
            name=name,
            replicas=replicas,
        )

    async def schedule[M](
        self,
        msg: M,
        *,
        to: "ActorRef[M] | None" = None,
        delay: float | None = None,
        every: float | None = None,
        sender: "ActorRef | None" = None,
    ) -> Any:
        if self._system is None:
            raise RuntimeError("Context not bound to system")

        target = to if to is not None else self._self_ref
        return await self._system.schedule(msg, to=target, delay=delay, every=every, sender=sender)
