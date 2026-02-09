"""Actor system entry point for spawning and managing top-level actors.

Provides ``ActorSystem``, the main runtime container that owns root actors,
handles request-reply (``ask``), path-based lookup, scheduling, and
graceful shutdown.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from casty.config import CastyConfig

from casty.actor import Behavior
from casty.cell import ActorCell
from casty.address import ActorAddress
from casty.events import EventStream
from casty.mailbox import Mailbox
from casty.ref import ActorRef
from casty.scheduler import (
    CancelSchedule,
    ScheduleOnce,
    SchedulerMsg,
    ScheduleTick,
    scheduler as scheduler_behavior,
)
from casty.transport import LocalTransport, MessageTransport


class CallbackTransport:
    """Lightweight transport for temporary ask refs."""

    def __init__(self, callback: Callable[[Any], None]) -> None:
        self._callback = callback

    def deliver(self, address: ActorAddress, msg: Any) -> None:
        self._callback(msg)


class ActorSystem:
    """Main entry point for creating and managing actors.

    ``ActorSystem`` is the top-level container that owns root actors,
    provides request-reply via ``ask()``, path-based actor lookup, a
    built-in scheduler, and an event stream for system-wide pub/sub.

    Use as an async context manager for automatic shutdown.

    Parameters
    ----------
    name : str
        Name of the actor system, used in actor addresses.
    config : CastyConfig | None
        System-wide configuration for mailbox defaults, transport, etc.

    Examples
    --------
    >>> async with ActorSystem("my-system") as system:
    ...     ref = system.spawn(my_behavior(), "actor-1")
    ...     ref.tell("hello")
    """

    def __init__(self, name: str = "casty-system", *, config: CastyConfig | None = None) -> None:
        self._name = name
        self._config = config
        self._event_stream = EventStream()
        self._root_cells: dict[str, ActorCell[Any]] = {}
        max_pending = config.transport.max_pending_per_path if config is not None else 64
        self._local_transport = LocalTransport(max_pending_per_path=max_pending)
        self._scheduler: ActorRef[SchedulerMsg] | None = None
        self._system_logger = logging.getLogger(f"casty.system.{name}")

    @property
    def name(self) -> str:
        """The name of this actor system.

        Returns
        -------
        str
            System name as provided at construction time.

        Examples
        --------
        >>> system.name
        'my-system'
        """
        return self._name

    @property
    def event_stream(self) -> EventStream:
        """The system-wide event stream for pub/sub.

        Returns
        -------
        EventStream
            Event stream shared by all actors in this system.

        Examples
        --------
        >>> system.event_stream.subscribe(ActorStarted, lambda e: print(e.ref))
        """
        return self._event_stream

    @property
    def scheduler(self) -> ActorRef[SchedulerMsg]:
        """The built-in scheduler actor, lazily created on first access.

        Returns
        -------
        ActorRef[SchedulerMsg]
            Reference to the scheduler actor.

        Examples
        --------
        >>> system.scheduler.tell(ScheduleTick(key="k", target=ref, message="go", interval=1.0))
        """
        if self._scheduler is None:
            self._scheduler = self.spawn(scheduler_behavior(), "_scheduler")
        return self._scheduler

    def tick[M](self, key: str, target: ActorRef[M], message: M, interval: float) -> None:
        """Schedule a repeating message delivery.

        Parameters
        ----------
        key : str
            Unique key for this schedule.
        target : ActorRef[M]
            Actor to receive the message.
        message : M
            Message to deliver on each tick.
        interval : float
            Seconds between deliveries.

        Examples
        --------
        >>> system.tick("ping", worker_ref, "ping", interval=5.0)
        """
        self.scheduler.tell(ScheduleTick(key=key, target=target, message=message, interval=interval))

    def schedule[M](self, key: str, target: ActorRef[M], message: M, delay: float) -> None:
        """Schedule a one-shot delayed message delivery.

        Parameters
        ----------
        key : str
            Unique key for this schedule.
        target : ActorRef[M]
            Actor to receive the message.
        message : M
            Message to deliver after the delay.
        delay : float
            Seconds to wait before delivery.

        Examples
        --------
        >>> system.schedule("timeout", ref, "expired", delay=30.0)
        """
        self.scheduler.tell(ScheduleOnce(key=key, target=target, message=message, delay=delay))

    def cancel_schedule(self, key: str) -> None:
        """Cancel a previously registered schedule.

        Parameters
        ----------
        key : str
            The key of the schedule to cancel.

        Examples
        --------
        >>> system.cancel_schedule("ping")
        """
        self.scheduler.tell(CancelSchedule(key=key))

    async def __aenter__(self) -> ActorSystem:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.shutdown()

    def spawn[M](
        self,
        behavior: Behavior[M],
        name: str,
        *,
        mailbox: Mailbox[M] | None = None,
    ) -> ActorRef[M]:
        """Spawn a root-level actor in this system.

        Parameters
        ----------
        behavior : Behavior[M]
            The initial behavior of the actor.
        name : str
            Unique name for the root actor.
        mailbox : Mailbox[M] | None
            Custom mailbox. Uses config defaults if ``None``.

        Returns
        -------
        ActorRef[M]
            A typed reference to the newly spawned actor.

        Raises
        ------
        ValueError
            If a root actor with the same name already exists.

        Examples
        --------
        >>> ref = system.spawn(my_behavior(), "greeter")
        >>> ref.tell("hello")
        """
        if name in self._root_cells:
            raise ValueError(f"Root actor '{name}' already exists")

        if mailbox is None and self._config is not None and not name.startswith("_"):
            from casty.mailbox import MailboxOverflowStrategy

            resolved = self._config.resolve_actor(name)
            mailbox = Mailbox(
                capacity=resolved.mailbox.capacity,
                overflow=MailboxOverflowStrategy[resolved.mailbox.strategy],
            )

        cell: ActorCell[M] = ActorCell(
            behavior=behavior,
            name=name,
            parent=None,
            event_stream=self._event_stream,
            system_name=self._name,
            local_transport=self._local_transport,
            ref_transport=self._get_ref_transport(),
            ref_host=self._get_ref_host(),
            ref_port=self._get_ref_port(),
            ref_node_id=self._get_ref_node_id(),
        )
        if mailbox is not None:
            cell.mailbox = mailbox
        self._root_cells[name] = cell
        asyncio.get_running_loop().create_task(cell.start())
        self._system_logger.info("Spawning root actor: %s", name)
        return cell.ref

    async def ask[M, R](
        self,
        ref: ActorRef[M],
        msg_factory: Callable[[ActorRef[R]], M],
        *,
        timeout: float,
    ) -> R:
        """Send a message and wait for a reply (request-reply pattern).

        Creates a temporary ``ActorRef`` for receiving the reply and
        passes it to ``msg_factory`` to construct the outgoing message.

        Parameters
        ----------
        ref : ActorRef[M]
            The target actor.
        msg_factory : Callable[[ActorRef[R]], M]
            Factory that receives a reply-to ref and returns the message.
        timeout : float
            Maximum seconds to wait for a reply.

        Returns
        -------
        R
            The reply from the target actor.

        Raises
        ------
        asyncio.TimeoutError
            If no reply is received within ``timeout`` seconds.

        Examples
        --------
        >>> result = await system.ask(ref, lambda r: GetBalance(reply_to=r), timeout=5.0)
        """
        future: asyncio.Future[R] = asyncio.get_running_loop().create_future()

        def on_reply(msg: R) -> None:
            if not future.done():
                future.set_result(msg)

        temp_ref: ActorRef[R] = ActorRef(
            address=ActorAddress(system=self._name, path=f"/_temp/{id(future)}"),
            _transport=CallbackTransport(on_reply),
        )
        message = msg_factory(temp_ref)
        self._system_logger.debug(
            "ask %s -> %s (timeout=%.1fs)", type(message).__name__, ref.address.path, timeout
        )
        ref.tell(message)

        return await asyncio.wait_for(future, timeout=timeout)

    async def ask_or_none[M, R](
        self,
        ref: ActorRef[M],
        msg_factory: Callable[[ActorRef[R]], M],
        *,
        timeout: float,
    ) -> R | None:
        """Like ``ask()``, but returns ``None`` on timeout instead of raising.

        Parameters
        ----------
        ref : ActorRef[M]
            The target actor.
        msg_factory : Callable[[ActorRef[R]], M]
            Factory that receives a reply-to ref and returns the message.
        timeout : float
            Maximum seconds to wait for a reply.

        Returns
        -------
        R | None
            The reply, or ``None`` if the timeout was exceeded.

        Examples
        --------
        >>> result = await system.ask_or_none(ref, lambda r: GetBalance(reply_to=r), timeout=2.0)
        >>> if result is None:
        ...     print("Timed out")
        """
        try:
            return await self.ask(ref, msg_factory, timeout=timeout)
        except asyncio.TimeoutError:
            return None

    def resolve(self, address: ActorAddress) -> ActorRef[Any] | None:
        """Resolve an ``ActorAddress`` to a local ``ActorRef``.

        Parameters
        ----------
        address : ActorAddress
            The address to resolve.

        Returns
        -------
        ActorRef[Any] | None
            The actor reference, or ``None`` if not found locally.

        Examples
        --------
        >>> addr = ActorAddress(system="sys", path="/greeter")
        >>> ref = system.resolve(addr)
        """
        if address.is_local or address.host is None:
            return self.lookup(address.path)
        return None

    def lookup(self, path: str) -> ActorRef[Any] | None:
        """Look up an actor by its path in the actor tree.

        Navigates the hierarchy from root to leaf using ``/``-separated
        path segments.

        Parameters
        ----------
        path : str
            Actor path, e.g. ``"/parent/child"``.

        Returns
        -------
        ActorRef[Any] | None
            The actor reference, or ``None`` if no actor exists at that path.

        Examples
        --------
        >>> ref = system.lookup("/greeter")
        >>> ref = system.lookup("/parent/child")
        """
        parts = path.strip("/").split("/")
        if not parts:
            return None

        root_name = parts[0]
        cell = self._root_cells.get(root_name)
        if cell is None:
            return None

        for part in parts[1:]:
            child = cell.children.get(part)
            if child is None:
                return None
            cell = child

        return cell.ref

    def _get_ref_transport(self) -> MessageTransport | None:
        return None

    def _get_ref_host(self) -> str | None:
        return None

    def _get_ref_port(self) -> int | None:
        return None

    def _get_ref_node_id(self) -> str | None:
        return None

    async def shutdown(self) -> None:
        """Shut down the actor system, stopping all root actors.

        Stops each root actor gracefully and clears the actor tree.

        Examples
        --------
        >>> await system.shutdown()
        """
        self._system_logger.info("Shutting down (%d root actors)", len(self._root_cells))
        for cell in list(self._root_cells.values()):
            await cell.stop()
        self._root_cells.clear()
