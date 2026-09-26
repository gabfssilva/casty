"""Real nodes in one process and one event loop, with a TCP proxy per ordered pair to make failures real."""

import asyncio
import random
import socket
from collections.abc import AsyncGenerator, Callable, Coroutine, Iterable, Sequence
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from dataclasses import dataclass, replace
from datetime import timedelta

from casty import ActorDefinition, ActorSystem, Client, Cluster, Compression, Limits, Observer, Overlay, Runtime, Store
from tests.support import eventually


@dataclass(frozen=True)
class Timing:
    """Short periods that make convergence observable in a test, spread over `Cluster` and `ActorSystem`."""

    heartbeat: timedelta = timedelta(milliseconds=50)
    suspect_after: timedelta = timedelta(milliseconds=500)
    dead_after: timedelta = timedelta(milliseconds=500)
    remove_after: timedelta | None = timedelta(seconds=1)
    anti_entropy: timedelta = timedelta(milliseconds=250)
    shuffle_every: timedelta = timedelta(milliseconds=500)
    graft_after: timedelta = timedelta(milliseconds=100)
    idle_after: timedelta = timedelta(seconds=30)
    ask_timeout: timedelta = timedelta(seconds=5)
    write_timeout: timedelta = timedelta(milliseconds=500)
    sync_every: timedelta = timedelta(milliseconds=100)
    leave_timeout: timedelta = timedelta(seconds=5)


FAST = Timing()
OVERLAY = Overlay()
LIMITS = Limits()
NARROW = Limits(frame=64 * 1024, message=128 * 1024, window=64 * 1024)
"""The smallest `Limits.message` there is, which leaves 64 KiB of each message to the state it carries."""
COMPRESSION = Compression()
WITHIN = timedelta(seconds=10)
"""How long a test waits for the cluster to come to what it asserts."""


# Equal only to itself, as a running node is: a test collects nodes in sets and finds them by identity.
@dataclass(frozen=True, eq=False)
class Node:
    """A node of the harness: its system, the address it advertises and the task that keeps it running."""

    system: ActorSystem
    address: str
    id: int
    task: asyncio.Task[None]
    stop: asyncio.Event


class Harness:
    """A cluster of nodes in this process, each one reaching the others through proxies that a test can block."""

    @classmethod
    @asynccontextmanager
    async def start(
        cls,
        count: int,
        *,
        timing: Timing = FAST,
        overlay: Overlay = OVERLAY,
        limits: Limits = LIMITS,
        compression: Compression = COMPRESSION,
        observer: Callable[[int], Observer] | None = None,
        store: Store | None = None,
        runtime: Runtime | None = None,
    ) -> AsyncGenerator["Harness"]:
        """Start `count` nodes at once and wait until they all see each other.

        `observer` gives the observer of each node, from the `id` of the node, the ones added later included. `store`
        is the store of every node, the ones added later included. `runtime` carries the transport of every node and
        client, unless `add` gives a node another; without one, each starts its own.
        """
        async with asyncio.TaskGroup() as tasks:
            harness = cls(
                tasks,
                timing=timing,
                overlay=overlay,
                limits=limits,
                compression=compression,
                observer=observer,
                store=store,
                runtime=runtime,
            )
            try:
                addresses = tuple(harness._address() for _ in range(count))
                async with asyncio.TaskGroup() as starting:
                    for address in addresses:
                        seeds = _seeds(addresses, address)
                        harness._ids += 1
                        starting.create_task(harness._start(harness._ids, address, seeds, (), "casty", runtime))
                await harness._converged()
                yield harness
            finally:
                await harness._close()

    def __init__(
        self,
        tasks: asyncio.TaskGroup,
        /,
        *,
        timing: Timing,
        overlay: Overlay,
        limits: Limits,
        compression: Compression,
        observer: Callable[[int], Observer] | None = None,
        store: Store | None = None,
        runtime: Runtime | None = None,
    ) -> None:
        self._tasks = tasks
        self._timing = timing
        self._overlay = overlay
        self._limits = limits
        self._compression = compression
        self._observer = observer
        self._store = store
        self._runtime = runtime
        self._clients = AsyncExitStack()
        self._nodes: list[Node] = []
        self._proxies: dict[tuple[int, str], Proxy] = {}
        self._plumbing: set[asyncio.Task[None]] = set()
        self._blocked: set[tuple[int, str]] = set()
        self._port = random.randrange(20_000, 30_000)
        self._ids = 0

    @property
    def nodes(self) -> tuple[Node, ...]:
        """The nodes still running, in the order they were started."""
        return tuple(self._nodes)

    @property
    def forwarded(self) -> int:
        """Bytes every proxy of the harness has put on the wire so far, both ways."""
        return sum(proxy.forwarded for proxy in self._proxies.values())

    async def add(
        self,
        *,
        version: Sequence[ActorDefinition] = (),
        name: str = "casty",
        address: str | None = None,
        cut_off: Iterable[Node] = (),
        runtime: Runtime | None = None,
        joining: Callable[[ActorSystem], object] | None = None,
    ) -> Node:
        """Start one more node, seeded with the nodes that are running, on `address` or on a free port.

        It never exchanges bytes with `cut_off`, from before it starts: a node blocked only once it is up has already
        talked to everyone, which is too late for a test about what it does before it has heard from them. `runtime`
        carries its transport instead of the one of the harness. `joining` is called with its system once the join is
        under way and before the node has entered.
        """
        seeds = _seeds(tuple(node.address for node in self._nodes), address)
        where = address or self._address()
        self._ids += 1
        source = self._ids
        self._block(pair for other in cut_off for pair in ((other.id, where), (source, other.address)))
        return await self._start(source, where, seeds, version, name, runtime or self._runtime, joining)

    async def client(self, *, name: str = "casty") -> Client:
        """Start a client of the cluster, reaching it through proxies of its own, closed when the harness closes."""
        self._ids += 1
        source = self._ids
        client = Client(
            seeds=_seeds(tuple(node.address for node in self._nodes), None),
            name=name,
            address_map=lambda target: self._proxy(source, target).address,
            limits=self._limits,
            compression=self._compression,
            ask_timeout=self._timing.ask_timeout,
            sync_every=self._timing.sync_every,
            runtime=self._runtime,
        )
        # A client that never gets the table is a failed test, not a test that hangs.
        async with asyncio.timeout(_JOIN.total_seconds()):
            return await self._clients.enter_async_context(client)

    async def crash(self, node: Node) -> None:
        """Kill the process of `node`: its sockets close and nothing is drained."""
        node.task.cancel()
        await self._end(node)

    async def leave(self, node: Node) -> None:
        """Shut `node` down in an orderly way."""
        node.stop.set()
        await self._end(node)

    def partition(self, *groups: Iterable[Node]) -> None:
        """Stop every pair of nodes that are in different groups from exchanging bytes."""
        sides = [tuple(group) for group in groups]
        pairs: list[tuple[int, str]] = []
        for index, side in enumerate(sides):
            for across in sides[index + 1 :]:
                for node in side:
                    pairs += [pair for other in across for pair in self._both_ways(node, other)]
        self._block(pairs)

    def isolate(self, node: Node) -> None:
        """Make the machine of `node` disappear: it keeps running, and nothing reaches it or leaves it."""
        self._block(pair for other in self._nodes if other is not node for pair in self._both_ways(node, other))

    async def sever(self) -> None:
        """Reset every connection the proxies carry, as a network that drops them does, and keep accepting new ones."""
        for proxy in self._proxies.values():
            await proxy.close()

    def heal(self) -> None:
        """Let every pair exchange bytes again, delivering what the blocked proxies held."""
        self._blocked.clear()
        self._refresh()

    async def _start(
        self,
        source: int,
        address: str,
        seeds: tuple[str, ...],
        version: Sequence[ActorDefinition],
        name: str,
        runtime: Runtime | None,
        joining: Callable[[ActorSystem], object] | None = None,
    ) -> Node:
        cluster = Cluster(
            bind=address,
            seeds=seeds,
            name=name,
            address_map=lambda target: self._proxy(source, target).address,
            limits=self._limits,
            compression=self._compression,
            heartbeat=self._timing.heartbeat,
            suspect_after=self._timing.suspect_after,
            dead_after=self._timing.dead_after,
            remove_after=self._timing.remove_after,
            anti_entropy=self._timing.anti_entropy,
            overlay=replace(
                self._overlay, shuffle_every=self._timing.shuffle_every, graft_after=self._timing.graft_after
            ),
        )
        system = Versioned(
            version,
            cluster=cluster,
            idle_after=self._timing.idle_after,
            ask_timeout=self._timing.ask_timeout,
            write_timeout=self._timing.write_timeout,
            leave_timeout=self._timing.leave_timeout,
            observer=None if self._observer is None else self._observer(source),
            store=self._store,
            runtime=runtime,
        )
        started: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        stop = asyncio.Event()
        task = self._tasks.create_task(self._run(system, started, stop))
        if joining is not None:
            # One step of the task enters the system, which starts the join; the join ends only after the loop has
            # forwarded its bytes through the proxies, which it does not do before `joining` returns.
            await asyncio.sleep(0)
            joining(system)
        # A node that never enters the cluster is a failed test, not a test that hangs.
        async with asyncio.timeout(_JOIN.total_seconds()):
            await started
        node = Node(system, address, source, task, stop)
        self._nodes.append(node)
        return node

    async def _run(self, system: ActorSystem, started: asyncio.Future[None], stop: asyncio.Event) -> None:
        """Hold a node open until it is stopped; cancelling this task is the death of its process."""
        try:
            async with system:
                started.set_result(None)
                await stop.wait()
        except Exception as error:
            if started.done():
                raise
            started.set_exception(error)

    async def _end(self, node: Node) -> None:
        self._nodes.remove(node)
        await asyncio.wait([node.task])

    async def _converged(self) -> None:
        async def every_node_sees_every_other() -> None:
            nodes = {node.system.node for node in self._nodes}
            for node in self._nodes:
                alive = {member.node for member in node.system.members if member.status == "alive"}
                assert alive == nodes, f"{node.address} sees {len(alive)} of {len(nodes)} nodes"

        await eventually(every_node_sees_every_other, timedelta(seconds=10))

    async def _close(self) -> None:
        self.heal()
        await self._clients.aclose()
        tasks = [node.task for node in self._nodes]
        for node in self._nodes:
            node.stop.set()
        self._nodes.clear()
        if tasks:
            await asyncio.wait(tasks)
        plumbing = tuple(self._plumbing)
        for task in plumbing:
            task.cancel()
        if plumbing:
            await asyncio.wait(plumbing)
        # The listeners close first: a connection that detaches from a server already done closing raises in asyncio.
        for proxy in self._proxies.values():
            await proxy.close()

    def _address(self) -> str:
        while True:
            self._port += 1
            with socket.socket() as probe:
                try:
                    probe.bind(("127.0.0.1", self._port))
                except OSError:
                    continue
                return f"127.0.0.1:{self._port}"

    def _proxy(self, source: int, target: str) -> "Proxy":
        proxy = self._proxies.get((source, target))
        if proxy is None:
            proxy = self._proxies[source, target] = Proxy(self._spawn, target)
            proxy.block((source, target) in self._blocked)
            self._spawn(proxy.serve())
        return proxy

    def _block(self, pairs: Iterable[tuple[int, str]]) -> None:
        self._blocked |= set(pairs)
        self._refresh()

    def _spawn(self, work: Coroutine[None, None, None], /) -> asyncio.Task[None]:
        task = self._tasks.create_task(work)
        self._plumbing.add(task)
        task.add_done_callback(self._plumbing.discard)
        return task

    def _both_ways(self, node: Node, other: Node) -> tuple[tuple[int, str], tuple[int, str]]:
        return (node.id, other.address), (other.id, node.address)

    def _refresh(self) -> None:
        for pair, proxy in self._proxies.items():
            proxy.block(pair in self._blocked)


class Versioned(ActorSystem):
    """A node whose code has `version` of some types: what a process of another deploy is, inside this one.

    A node finds a type by importing it, and every node of a test imports from the same process. The ones given here
    are met before the cluster names them, so this node runs them and not what the import would bring.
    """

    def __init__(
        self,
        version: Sequence[ActorDefinition],
        /,
        *,
        cluster: Cluster,
        idle_after: timedelta,
        ask_timeout: timedelta,
        write_timeout: timedelta,
        leave_timeout: timedelta,
        observer: Observer | None = None,
        store: Store | None = None,
        runtime: Runtime | None = None,
    ) -> None:
        super().__init__(
            cluster=cluster,
            idle_after=idle_after,
            ask_timeout=ask_timeout,
            write_timeout=write_timeout,
            leave_timeout=leave_timeout,
            observer=observer,
            store=store,
            runtime=runtime,
        )
        for definition in version:
            self._learn(definition)


class Proxy:
    """The bytes of one ordered pair of addresses.

    Blocked, it stops forwarding without closing anything: the buffers fill up as in a network that drops packets, and
    what it held arrives after the healing, if the connection survives.
    """

    def __init__(self, spawn: Callable[[Coroutine[None, None, None]], asyncio.Task[None]], target: str, /) -> None:
        # asyncio sets TCP_NODELAY only on sockets whose proto is IPPROTO_TCP, and those `create_server` makes are not:
        # without it, Nagle holds every small write back until the delayed ACK of Linux, 40 ms later.
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        self.address = f"127.0.0.1:{listener.getsockname()[1]}"
        self._spawn = spawn
        self._target = target
        self._listener = listener
        self._writers: set[asyncio.StreamWriter] = set()
        self._open = asyncio.Event()
        self._open.set()
        self._delay = 0.0
        self.forwarded = 0
        """Bytes this pair put on the wire, both ways."""

    def block(self, blocked: bool, /) -> None:
        if blocked:
            self._open.clear()
        else:
            self._open.set()

    def slow(self, delay: timedelta, /) -> None:
        """Hold what each read brings for `delay` before forwarding it: a link that much longer, both ways."""
        self._delay = delay.total_seconds()

    async def close(self) -> None:
        """Close every connection and wait for it.

        A transport left to the garbage collector is collected after its server is gone, and asyncio raises inside
        `__del__`, which reaches the test as a warning about an exception nobody can catch.
        """
        for writer in self._writers:
            writer.close()
        for writer in self._writers:
            with suppress(OSError):
                await writer.wait_closed()
        self._writers.clear()

    async def serve(self) -> None:
        # The listener is bound before serving, so that a dial finds the port the moment `address_map` returns it.
        with self._listener:
            server = await asyncio.start_server(self._accept, sock=self._listener)
            try:
                await server.serve_forever()
            finally:
                server.close()

    def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._writers.add(writer)
        # The connection closes when its pair ends, so that no transport outlives its server: a cancelled `serve`
        # waits for every connection it accepted. A pair the harness cancels before its first step runs none of its
        # code, and its task still ends.
        self._spawn(self._forward(reader, writer)).add_done_callback(lambda _: writer.close())

    async def _forward(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        host, _, port = self._target.rpartition(":")
        # Blocked, the pair does not even reach the other side: a machine nobody can reach accepts no connection.
        await self._opened()
        # A node that died refuses the connection, and this pair ends before it starts.
        with suppress(OSError):
            upstream, answers = await asyncio.open_connection(host, int(port))
            self._writers.add(answers)
            try:
                async with asyncio.TaskGroup() as pumps:
                    pumps.create_task(self._pump(reader, answers))
                    pumps.create_task(self._pump(upstream, writer))
            finally:
                answers.close()

    async def _opened(self) -> None:
        # `set` wakes every waiter even when `clear` follows in the same step, as `heal` then `isolate` does: without
        # looking again, what a waiter held would cross a block that is already back.
        while not self._open.is_set():
            await self._open.wait()

    async def _pump(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        # A node that died breaks its pipe, which is how this half of the pair ends.
        with suppress(OSError):
            while data := await reader.read(_CHUNK):
                # The read that was already waiting would cross the partition; holding it is what makes the block whole.
                await self._opened()
                if self._delay:
                    await asyncio.sleep(self._delay)
                    await self._opened()
                self.forwarded += len(data)
                writer.write(data)
                await writer.drain()
        writer.close()


def _seeds(addresses: tuple[str, ...], mine: str | None) -> tuple[str, ...]:
    """The first few nodes, as an operator would configure them.

    The first node is seeded too, by the others, so that a node the cluster removed has somewhere to come back through.
    """
    return tuple(address for address in addresses[:_SEEDS] if address != mine)


_SEEDS = 3
_JOIN = timedelta(seconds=10)
_CHUNK = 64 * 1024
