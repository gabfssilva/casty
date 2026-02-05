from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from casty.mailbox import Mailbox
    from casty.ref import ActorRef


class ActorContext[M](Protocol):
    @property
    def self(self) -> ActorRef[M]: ...

    @property
    def log(self) -> logging.Logger: ...

    def spawn[C](
        self,
        behavior: Any,
        name: str,
        *,
        mailbox: Mailbox[C] | None = None,
    ) -> ActorRef[C]: ...

    def stop(self, ref: ActorRef[Any]) -> None: ...

    def watch(self, ref: ActorRef[Any]) -> None: ...

    def unwatch(self, ref: ActorRef[Any]) -> None: ...
