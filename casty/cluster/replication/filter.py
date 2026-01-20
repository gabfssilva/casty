from __future__ import annotations

from typing import TYPE_CHECKING, AsyncGenerator, Callable, Any

from .replicator import Replicator

if TYPE_CHECKING:
    from casty.state import State
    from casty.context import Context

type MessageStream[M] = AsyncGenerator[tuple[M, Context], None]
type Filter[M] = Callable[["State[Any] | None", MessageStream[M]], MessageStream[M]]


def replication_filter[M](replicator: Replicator) -> Filter[M]:
    """Creates a filter that replicates state after changes."""

    async def apply(
        state: "State[Any] | None",
        inner: MessageStream[M],
    ) -> MessageStream[M]:
        async for msg, ctx in inner:
            before: bytes | None = None
            if state is not None:
                before = state.snapshot()

            yield msg, ctx

            if state is not None and before is not None:
                after = state.snapshot()
                await replicator.replicate(before, after, state.version)

    return apply
