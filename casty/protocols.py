from __future__ import annotations

from typing import Protocol, Any, TYPE_CHECKING, overload, List

if TYPE_CHECKING:
    from .actor import Behavior
    from .ref import ActorRef
    from .mailbox import Filter


class System(Protocol):
    @property
    def node_id(self) -> str: ...

    @overload
    async def actor[M](
        self,
        behavior: "Behavior",
        *,
        name: str,
        filters: "list[Filter] | None" = None,
    ) -> "ActorRef[M]": ...

    @overload
    async def actor[M](
        self,
        behavior: None = None,
        *,
        name: str,
        node_id: str | None = None,
    ) -> "ActorRef[M] | None": ...

    async def actor[M](
        self,
        behavior: "Behavior | None" = None,
        *,
        name: str,
        filters: "list[Filter] | None" = None,
        node_id: str | None = None,
    ) -> "ActorRef[M] | None": ...

    async def ask[M, R](
        self,
        ref: "ActorRef[M]",
        msg: M,
        timeout: float = 30.0,
        filters: List[Filter] = None
    ) -> R: ...

    async def shutdown(self) -> None: ...

    async def __aenter__(self) -> "System": ...

    async def __aexit__(self, *args: Any) -> None: ...
