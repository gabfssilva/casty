from __future__ import annotations

import asyncio
from asyncio import Future
from dataclasses import dataclass
from typing import TYPE_CHECKING

from . import actor
from .envelope import Envelope
from .serializable import serializable

if TYPE_CHECKING:
    from .mailbox import Mailbox
    from .ref import ActorRef


@serializable
@dataclass
class Reply[R]:
    result: R | Exception


@serializable
@dataclass
class Cancel:
    reason: Exception | None


@actor
async def reply[M, R](
    content: M,
    to: "ActorRef[M]",
    promise: Future[R],
    timeout: float,
    *,
    mailbox: Mailbox[Reply[R] | Cancel]
):
    timeout_task = mailbox.schedule(Cancel(reason=TimeoutError()), delay=timeout)
    forward_task = to.send(content, sender=mailbox.ref())

    await asyncio.gather(timeout_task, forward_task)

    async for message, ctx in mailbox:
        match message:
            case Reply(result=e) if isinstance(e, Exception):
                promise.set_exception(e)

            case Reply(result=result):
                promise.set_result(result)

            case Cancel(reason=e):
                if not promise.done():
                    promise.set_exception(e or TimeoutError())

        return