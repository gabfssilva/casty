from __future__ import annotations

from typing import Protocol, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .actor import Behavior
    from .ref import ActorRef


class System(Protocol):
    @property
    def node_id(self) -> str: ...

    async def actor[M](
        self,
        behavior: "Behavior",
        *,
        name: str,
    ) -> "ActorRef[M]": ...

    async def shutdown(self) -> None: ...

    async def __aenter__(self) -> "System": ...

    async def __aexit__(self, *args: Any) -> None: ...
