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
from .supervision import Decision


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

    @property
    def node_id(self) -> str:
        return self._node_id

    def _build_actor_id(self, behavior: Behavior, name: str) -> str:
        func_name = behavior.func.__name__
        return f"{func_name}/{name}"

    async def _run_supervised_actor[M](
        self,
        actor_id: str,
        behavior: Behavior,
        mailbox: Mailbox[M],
        ref: ActorRef[M],
        state: Any = None,
    ) -> None:
        kwargs = dict(behavior.initial_kwargs)

        if behavior.state_param is not None and state is not None:
            kwargs[behavior.state_param] = state

        retries = 0
        if behavior.system_param is not None:
            kwargs[behavior.system_param] = self

        while True:
            try:
                await behavior.func(
                    *behavior.initial_args,
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

                if decision == Decision.restart():
                    retries += 1
                    continue
                elif decision == Decision.stop():
                    break
                else:
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
        from .state import State

        if behavior is None:
            if node_id is not None and node_id != self._node_id:
                return None
            return self._actors.get(name)

        actor_id = self._build_actor_id(behavior, name)

        if actor_id in self._actors:
            return self._actors[actor_id]

        state = None
        if behavior.state_param is not None:
            state = State(behavior.state_initial)

        all_filters = []
        if self._debug_filter:
            all_filters.append(self._debug_filter)
        if filters:
            all_filters.extend(filters)

        mailbox: ActorMailbox[M] = ActorMailbox(
            state=state,
            self_id=actor_id,
            node_id=self._node_id,
            is_leader=True,
            system=self,
            filters=all_filters,
        )
        ref: LocalActorRef[M] = LocalActorRef(actor_id=actor_id, mailbox=mailbox, _system=self)
        mailbox.set_self_ref(ref)

        task = asyncio.create_task(
            self._run_supervised_actor(actor_id, behavior, mailbox, ref, state)
        )

        self._actors[actor_id] = ref
        self._tasks[actor_id] = task
        self._mailboxes[actor_id] = mailbox

        return ref

    async def _create_child[M](
        self,
        parent_id: str,
        behavior: Behavior,
        *,
        name: str,
        replicas: int = 1,
    ) -> ActorRef[M]:
        from .state import State

        func_name = behavior.func.__name__
        actor_id = f"{parent_id}/{func_name}/{name}"

        if actor_id in self._actors:
            return self._actors[actor_id]

        state = None
        if behavior.state_param is not None:
            state = State(behavior.state_initial)

        filters = [self._debug_filter] if self._debug_filter else []
        mailbox: ActorMailbox[M] = ActorMailbox(
            state=state,
            self_id=actor_id,
            node_id=self._node_id,
            is_leader=True,
            system=self,
            filters=filters,
        )
        ref: LocalActorRef[M] = LocalActorRef(actor_id=actor_id, mailbox=mailbox, _system=self)
        mailbox.set_self_ref(ref)

        task = asyncio.create_task(
            self._run_supervised_actor(actor_id, behavior, mailbox, ref, state)
        )

        self._actors[actor_id] = ref
        self._tasks[actor_id] = task
        self._mailboxes[actor_id] = mailbox

        return ref

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
    ) -> Callable[[], Coroutine[Any, Any, None]] | None:
        if delay is not None and every is not None:
            raise ValueError("Cannot specify both delay and every")

        if to is None:
            raise ValueError("Must specify 'to' target")

        if delay is not None:
            async def delayed_send() -> None:
                await asyncio.sleep(delay)
                await to.send(msg)

            asyncio.create_task(delayed_send())
            return None

        if every is not None:
            cancelled = False

            async def periodic_send() -> None:
                nonlocal cancelled
                while not cancelled:
                    await asyncio.sleep(every)
                    if not cancelled:
                        await to.send(msg)

            task = asyncio.create_task(periodic_send())

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
        for mailbox in self._mailboxes.values():
            await mailbox.put(Envelope(Stop()))

        if self._tasks:
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
        return await self._inner.actor(behavior, name=name, filters=self._filters + (filters or []), node_id=node_id)

    async def ask[M, R](
        self,
        ref: ActorRef[M],
        msg: M,
        timeout: float = 30.0,
    ) -> R:
        return await self._inner.ask(ref, msg, timeout)

    async def schedule[M](
        self,
        msg: M,
        *,
        to: ActorRef[M] | None = None,
        delay: float | None = None,
        every: float | None = None,
    ) -> Callable[[], Coroutine[Any, Any, None]] | None:
        return await self._inner.schedule(msg, to=to, delay=delay, every=every)

    async def shutdown(self) -> None:
        await self._inner.shutdown()

    async def __aenter__(self) -> "ActorSystem":
        if hasattr(self._inner, "start"):
            await self._inner.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.shutdown()
