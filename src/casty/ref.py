from __future__ import annotations

from dataclasses import dataclass

from casty.address import ActorAddress
from casty.transport import MessageTransport


@dataclass(frozen=True)
class ActorRef[M]:
    address: ActorAddress
    _transport: MessageTransport

    def tell(self, msg: M) -> None:
        self._transport.deliver(self.address, msg)
