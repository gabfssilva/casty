"""High-level behavior factories built on the Behavior primitive.

Every factory here composes ``Behavior.receive``, ``Behavior.setup``,
and ``Behavior.same``/``Behavior.stopped`` — nothing else. Lifecycle,
supervision, and spy are emergent, not baked in.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any, TYPE_CHECKING

from casty.core.behavior import Behavior, Signal
from casty.core.spy import SpyEvent
from casty.core.spy import spy as spy_fn
from casty.core.supervision import supervise as supervise_fn

if TYPE_CHECKING:
    from casty.core.context import ActorContext
    from casty.core.journal import EventJournal
    from casty.core.ref import ActorRef
    from casty.core.replication import ReplicationConfig
    from casty.core.supervision import SupervisionStrategy
    from casty.cluster.receptionist import ServiceKey


class Behaviors:
    """Factory for composing behaviors from the Behavior primitive."""

    @staticmethod
    def receive[M](
        handler: Callable[[ActorContext[M], M], Awaitable[Behavior[M]]],
    ) -> Behavior[M]:
        return Behavior.receive(handler)

    @staticmethod
    def setup[M](
        factory: Callable[[ActorContext[M]], Awaitable[Behavior[M]]],
    ) -> Behavior[M]:
        return Behavior.setup(factory)

    @staticmethod
    def same() -> Behavior[Any]:
        return Behavior.same()

    @staticmethod
    def ignore[M]() -> Behavior[M]:
        async def receive(ctx: ActorContext[M], msg: M) -> Behavior[M]:
            return Behavior.same()
        return Behavior.receive(receive)

    @staticmethod
    def stopped() -> Behavior[Any]:
        return Behavior.stopped()

    @staticmethod
    def unhandled() -> Behavior[Any]:
        return Behavior.unhandled()

    @staticmethod
    def restart() -> Behavior[Any]:
        return Behavior.restart()

    @staticmethod
    def with_lifecycle[M](
        behavior: Behavior[M],
        *,
        pre_start: Callable[[ActorContext[M]], Awaitable[None]] | None = None,
        post_stop: Callable[[ActorContext[M]], Awaitable[None]] | None = None,
        pre_restart: Callable[[ActorContext[M], Exception], Awaitable[None]] | None = None,
        post_restart: Callable[[ActorContext[M]], Awaitable[None]] | None = None,
    ) -> Behavior[M]:
        async def setup(ctx: ActorContext[M]) -> Behavior[M]:
            if pre_start is not None:
                await pre_start(ctx)

            inner = behavior
            if inner.on_setup is not None:
                inner = await inner.on_setup(ctx)

            original = inner.on_receive
            if original is None:
                return inner

            async def receive(ctx: ActorContext[M], msg: M) -> Behavior[M]:
                try:
                    result = await original(ctx, msg)
                except Exception as exc:
                    if pre_restart is not None:
                        await pre_restart(ctx, exc)
                    raise

                match result.signal:
                    case Signal.stopped:
                        if post_stop is not None:
                            await post_stop(ctx)
                        return result
                    case Signal.restart:
                        if post_restart is not None:
                            await post_restart(ctx)
                        return result
                    case _:
                        pass

                if result.on_receive is not None:
                    return Behaviors.with_lifecycle(
                        result,
                        pre_start=None,
                        post_stop=None,
                        pre_restart=pre_restart,
                        post_restart=post_restart,
                    )

                return result

            return Behavior.receive(receive)

        return Behavior.setup(setup)

    @staticmethod
    def supervise[M](
        behavior: Behavior[M],
        strategy: SupervisionStrategy,
    ) -> Behavior[M]:
        return supervise_fn(behavior, strategy)

    @staticmethod
    def spy[M](
        behavior: Behavior[M],
        observer: ActorRef[SpyEvent[M]],
        *,
        spy_children: bool = False,
    ) -> Behavior[M]:
        return spy_fn(behavior, observer, spy_children=spy_children)

    @staticmethod
    def event_sourced[M, E, S](
        *,
        entity_id: str,
        journal: EventJournal,
        initial_state: S,
        on_event: Callable[[S, E], S],
        on_command: Callable[[ActorContext[M], S, M], Awaitable[Behavior[M]]],
        snapshot_policy: Any | None = None,
        replica_refs: tuple[ActorRef[Any], ...] | Sequence[ActorRef[Any]] | None = None,
        replication: ReplicationConfig | None = None,
    ) -> Behavior[Any]:
        from casty.core.event_sourcing import event_sourced_wrapper

        refs = tuple(replica_refs) if replica_refs is not None else ()
        return event_sourced_wrapper(
            entity_id=entity_id,
            journal=journal,
            initial_state=initial_state,
            on_event=on_event,
            on_command=on_command,
            snapshot_policy=snapshot_policy,
            replica_refs=refs,
            replication=replication,
        )

    @staticmethod
    def persisted[M, E](
        events: Sequence[E],
        then: Behavior[M] | None = None,
    ) -> Any:
        from casty.actor import PersistedBehavior

        return PersistedBehavior(
            events=tuple(events),
            then=then if then is not None else Behavior.same(),
        )

    @staticmethod
    def sharded[M](
        entity_factory: Callable[[str], Behavior[M]],
        *,
        num_shards: int = 100,
        replication: ReplicationConfig | None = None,
    ) -> Any:
        from casty.actor import ShardedBehavior

        return ShardedBehavior(
            entity_factory=entity_factory,
            num_shards=num_shards,
            replication=replication,
        )

    @staticmethod
    def singleton[M](
        factory: Callable[[], Behavior[M]],
    ) -> Any:
        from casty.actor import SingletonBehavior

        return SingletonBehavior(factory=factory)

    @staticmethod
    def discoverable[M](
        behavior: Behavior[M],
        *,
        key: ServiceKey[M],
    ) -> Behavior[M]:
        from casty.cluster.receptionist import Deregister, Register

        async def pre_start(ctx: ActorContext[M]) -> None:
            ref = ctx.system.lookup("/_cluster/_receptionist")
            if ref is not None:
                ref.tell(Register(key=key, ref=ctx.self))

        async def post_stop(ctx: ActorContext[M]) -> None:
            ref = ctx.system.lookup("/_cluster/_receptionist")
            if ref is not None:
                ref.tell(Deregister(key=key, ref=ctx.self))

        return Behaviors.with_lifecycle(behavior, pre_start=pre_start, post_stop=post_stop)

    @staticmethod
    def broadcasted[M](
        behavior: Behavior[M],
    ) -> Any:
        from casty.actor import BroadcastedBehavior

        return BroadcastedBehavior(behavior=behavior)
