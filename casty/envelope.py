from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from casty.serializable import serializable


@serializable
@dataclass
class Envelope[M]:
    payload: M
    sender: str | None = None
    target: str | None = None
    correlation_id: str | None = None
    reply_to: asyncio.Future[Any] | None = field(default=None, repr=False)
