"""Actor context protocol — re-exports from core.

Keeps ``Interceptor`` type alias for backward compatibility.
"""

from __future__ import annotations

from collections.abc import Callable

from casty.core.context import ActorContext

type Interceptor = Callable[[object], bool]

__all__ = [
    "ActorContext",
    "Interceptor",
]
