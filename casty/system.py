from __future__ import annotations

import asyncio
import uuid
from typing import Any, Callable, Coroutine, overload

from .actor import Behavior
from .envelope import Envelope
from .mailbox import Mailbox, ActorMailbox, Stop, Filter
from .protocols import System
from .ref import ActorRef, LocalActorRef
from .reply import reply
from .state import Stateful
from .supervision import DecisionType


class LocalActorSystem(System):
    def __init__(
        self,
        node_id: str = "local",
        debug_filter: Any = None,
    ) -> None:
        self._node_id = node_id
        self._debug_filter = debug_filter
        self._actors: dict[str, ActorRef[Any]] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._mailboxes: dict[str, ActorMailbox[Any]] = {}
        self._scheduled_tasks: list[asyncio.Task[None]] = []

    @property
    def node_id(self) -> str:
        return self._node_id

    async def _spawn_actor[M](
        self,
        actor_id: str,
        behavior: Behavior,
        filters: list[Filter] | None = None,
    ) -> ActorRef[M]:
        from .transform import make_state_namespace

        if actor_id in self._actors:
            return self._actors[actor_id]

        state_ns = make_state_namespace(behavior.state_params, behavior.state_initials) if behavior.state_params else None

        all_filters = [self._debug_filter] if self._debug_filter else []
        if filters:
            all_filters.extend(filters)

        mailbox: ActorMailbox[M] = ActorMailbox(
            state=state_ns,
            self_id=actor_id,
            node_id=self._node_id,
            is_leader=True,
            system=self,
            filters=all_filters,
        )

        ref: LocalActorRef[M] = LocalActorRef(actor_id=actor_id, mailbox=mailbox, _system=self)
        mailbox.set_self_ref(ref)

        task = asyncio.create_task(
            self._run_supervised_actor(actor_id, behavior, mailbox, ref, state_ns)
        )

        self._actors[actor_id] = ref
        self._tasks[actor_id] = task
        self._mailboxes[actor_id] = mailbox

        return ref

    async def _run_supervised_actor[M](
        self,
        actor_id: str,
        behavior: Behavior,
        mailbox: ActorMailbox[M],
        ref: ActorRef[M],
        state_ns: Any = None,
    ) -> None:
        kwargs = dict(behavior.initial_kwargs)

        for name, value in zip(behavior.config_params, behavior.config_values):
            if name not in kwargs:
                kwargs[name] = value

        if behavior.system_param is not None:
            kwargs[behavior.system_param] = self

        retries = 0
        stateful = Stateful()
        mailbox._stateful = stateful

        while True:
            try:
                with stateful:
                    if state_ns is not None:
                        await behavior.func(
                            state_ns,
                            mailbox=mailbox,
                            **kwargs,
                        )
                    else:
                        await behavior.func(
                            mailbox=mailbox,
                            **kwargs,
                        )
                break
            except StopAsyncIteration:
                break
            except Exception as e:
                if behavior.supervision is None:
                    raise

                decision = behavior.supervision.strategy.decide(e, retries)

                match decision.type:
                    case DecisionType.RESTART:
                        retries += 1
                        continue
                    case DecisionType.STOP:
                        break
                    case _:
                        raise

    @overload
    async def actor[M](
        self,
        behavior: Behavior,
        *,
        name: str,
        filters: list[Filter] | None = None,
    ) -> ActorRef[M]: ...

    @overload
    async def actor[M](
        self,
        behavior: None = None,
        *,
        name: str,
        filters: list[Filter] | None = None,
        node_id: str | None = None,
    ) -> ActorRef[M] | None: ...

    async def actor[M](
        self,
        behavior: Behavior | None = None,
        *,
        name: str,
        filters: list[Filter] | None = None,
        node_id: str | None = None,
    ) -> ActorRef[M] | None:
        if behavior is None:
            if node_id is not None and node_id != self._node_id:
                return None
            return self._actors.get(name)

        return await self._spawn_actor(name, behavior, filters)

    async def _create_child[M](
        self,
        parent_id: str,
        behavior: Behavior,
        *,
        name: str,
    ) -> ActorRef[M]:
        actor_id = f"{parent_id}/{name}"
        return await self._spawn_actor(actor_id, behavior)

    async def ask[M, R](
        self,
        ref: ActorRef[M],
        msg: M,
        timeout: float = 30.0,
        filters: list[Filter] | None = None,
    ) -> R:
        promise: asyncio.Future[R] = asyncio.Future()
        await self.actor(
            reply(msg, ref, promise, timeout),
            name=uuid.uuid4().hex,
            filters=filters
        )
        return await promise

    async def schedule[M](
        self,
        msg: M,
        *,
        to: ActorRef[M] | None = None,
        delay: float | None = None,
        every: float | None = None,
        sender: ActorRef | None = None,
    ) -> Callable[[], Coroutine[Any, Any, None]] | None:
        if delay is not None and every is not None:
            raise ValueError("Cannot specify both delay and every")

        if to is None:
            raise ValueError("Must specify 'to' target")

        if delay is not None:
            async def delayed_send() -> None:
                await asyncio.sleep(delay)
                await to.send(msg, sender=sender)

            task = asyncio.create_task(delayed_send())
            self._scheduled_tasks.append(task)
            return None

        if every is not None:
            cancelled = False

            async def periodic_send() -> None:
                nonlocal cancelled
                while not cancelled:
                    await asyncio.sleep(every)
                    if not cancelled:
                        await to.send(msg, sender=sender)

            task = asyncio.create_task(periodic_send())
            self._scheduled_tasks.append(task)

            async def cancel() -> None:
                nonlocal cancelled
                cancelled = True
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            return cancel

        raise ValueError("Must specify either delay or every")

    async def shutdown(self) -> None:
        for task in self._scheduled_tasks:
            task.cancel()
        for task in self._scheduled_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._scheduled_tasks.clear()

        for mailbox in self._mailboxes.values():
            await mailbox.put(Envelope(Stop()))

        if self._tasks:
            try:
                async with asyncio.timeout(2.0):
                    await asyncio.gather(*self._tasks.values(), return_exceptions=True)
            except TimeoutError:
                for task in self._tasks.values():
                    task.cancel()
                await asyncio.gather(*self._tasks.values(), return_exceptions=True)

        self._actors.clear()
        self._tasks.clear()
        self._mailboxes.clear()

    async def __aenter__(self) -> "LocalActorSystem":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.shutdown()


class ActorSystem(System):
    def __init__(
        self,
        node_id: str = "local",
        filters: list[Filter] | None = None,
    ) -> None:
        self._inner: LocalActorSystem | Any = LocalActorSystem(node_id=node_id)
        self._filters = filters or []

    @classmethod
    def clustered(
        cls,
        node_id: str,
        host: str = "127.0.0.1",
        port: int = 0,
        seeds: list[str] | None = None,
    ) -> "ActorSystem":
        from .cluster.clustered_system import ClusteredActorSystem

        instance = cls.__new__(cls)
        instance._inner = ClusteredActorSystem(
            node_id=node_id,
            host=host,
            port=port,
            seeds=seeds,
        )
        return instance

    @property
    def node_id(self) -> str:
        return self._inner.node_id

    async def actor[M](
        self,
        behavior: Behavior | None = None,
        *,
        name: str,
        filters: list[Filter] | None = None,
        node_id: str | None = None,
    ) -> ActorRef[M] | None:
        return await self._inner.actor(behavior, name=name, filters=self._filters + (filters or []), node_id=node_id)

    async def ask[M, R](
        self,
        ref: ActorRef[M],
        msg: M,
        timeout: float = 30.0,
        filters: list[Filter] | None = None,
    ) -> R:
        return await self._inner.ask(ref, msg, timeout, filters=filters)

    async def schedule[M](
        self,
        msg: M,
        *,
        to: ActorRef[M] | None = None,
        delay: float | None = None,
        every: float | None = None,
        sender: ActorRef | None = None,
    ) -> Callable[[], Coroutine[Any, Any, None]] | None:
        return await self._inner.schedule(msg, to=to, delay=delay, every=every, sender=sender)

    async def shutdown(self) -> None:
        await self._inner.shutdown()

    async def __aenter__(self) -> "ActorSystem":
        if hasattr(self._inner, "start"):
            await self._inner.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.shutdown()
