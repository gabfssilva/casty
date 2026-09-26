"""The pods of a run: the nodes on its slots, named by the headless service of the run, the clients of a performance
run, and the PostgreSQL of the run; and the links between the nodes, cut and slowed by Chaos Mesh.

The slot `n` is the pod `slot-n`, which advertises `slot-n.casty:7400`: the same name in every life of the slot, and a
new IP in each. Every pod and chaos of a run belongs to the pod of its runner, so deleting that pod deletes them all.
What a pod says is followed from its log into the output directory, from its start, since the log goes with the pod.
The runner asks a pod things on its control port, at the IP of the pod.

The store of the durable types is a PostgreSQL of the run, in the pod `store`, which the nodes reach by its IP: a run
starts on an empty database, which goes with the run.

A crash is the process ending when told to, and then its pod going: deleting the pod alone sends SIGTERM, which is the
leave. A leave is the deletion of the pod with a grace period above the `leave_timeout` of the node. Of the network of a
machine, the node keeps its clock itself; its links are `NetworkChaos` on the packets its pod sends: a partition from
the slots it cannot reach, and a delay to the ones it is slow to.
"""

from __future__ import annotations

import asyncio
import json
import math
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from lightkube import ApiError, AsyncClient
from lightkube.core.resource import ApiInfo, ResourceDef
from lightkube.generic_resource import GenericNamespacedResource
from lightkube.models.core_v1 import (
    Container,
    ContainerPort,
    EnvVar,
    ExecAction,
    PodSpec,
    Probe,
    ResourceRequirements,
)
from lightkube.models.meta_v1 import ObjectMeta, OwnerReference
from lightkube.resources import core_v1

from reliability import records
from reliability.node import CONTROL, Launch, Network
from reliability.support import eventually

SERVICE = "casty"
"""The headless service of the runs, which `reliability/infra/__main__.py` creates."""
PORT = 7400
ROLE = "casty/role"
"""The label the service selects the pods of the nodes by."""
SLOT = "casty/slot"
STORE = "store"
"""The pod of the PostgreSQL of a run."""
POSTGRES = "postgres:17"


class Stuck(Exception):
    """The cluster did not get where it had to in time, or a pod went away by itself: a failure of liveness."""


class NetworkChaos(GenericNamespacedResource):
    """A fault of Chaos Mesh on the network of some pods, which lightkube knows only by its API."""

    _api_info = ApiInfo(
        resource=ResourceDef("chaos-mesh.org", "v1alpha1", "NetworkChaos"),
        plural="networkchaos",
        verbs=["delete", "get", "list", "post", "watch"],
    )


@dataclass(frozen=True)
class Shape:
    """The CPU and the memory of the pod of each node and client, as Kubernetes quantities, requested and limited
    alike. What is not given is not bounded."""

    cpu: str | None = None
    memory: str | None = None

    def resources(self) -> ResourceRequirements:
        quantities = {name: value for name, value in (("cpu", self.cpu), ("memory", self.memory)) if value}
        return ResourceRequirements(requests=quantities, limits=quantities)


type Links = dict[str, tuple[dict[str, object], tuple[int, ...]]]
"""The chaos on the packets of one slot, by name: its spec, and the life of each slot it targets, since Chaos Mesh
finds the pods of its targets once, when it injects."""


class Kubernetes:
    """The pods of a run in the namespace of the runner, which it reaches from the pod it runs in."""

    @classmethod
    @asynccontextmanager
    async def connected(
        cls, slots: int, /, *, image: str, shape: Shape, owner: OwnerReference
    ) -> AsyncGenerator[Kubernetes]:
        client = AsyncClient()
        try:
            site = cls(client, slots, image=image, shape=shape, owner=owner)
            site.store = await site._database()
            try:
                yield site
            finally:
                await site.delete(STORE, grace=0, within=_GONE)
        finally:
            await client.close()

    def __init__(self, client: AsyncClient, slots: int, /, *, image: str, shape: Shape, owner: OwnerReference) -> None:
        self.addresses = tuple(f"slot-{slot}.{SERVICE}:{PORT}" for slot in range(slots))
        self.client = client
        self._namespace = client.namespace
        self._image = image
        self._shape = shape
        self._owner = owner
        self._lives = [0] * slots
        self._links: dict[str, Links] = {}
        self.store: str | None = None

    async def node(self, slot: int, launch: Launch, log: Path, /) -> Pod:
        """Start the node of `launch` on `slot`."""
        self._lives[slot] += 1
        name = f"slot-{slot}"
        await self.client.create(
            self._pod(
                name,
                {ROLE: "node", SLOT: str(slot)},
                ["-m", "reliability.node", launch.encoded()],
                [ContainerPort(containerPort=PORT, name="casty"), ContainerPort(containerPort=CONTROL)],
                named=True,
            )
        )
        return Pod(self, name, log)

    async def load(self, index: int, arguments: list[str], log: Path, /) -> Pod:
        """Start the pod `load-<index>`, a client of a performance run, running `python` with `arguments`."""
        name = f"load-{index}"
        await self.client.create(
            self._pod(name, {ROLE: "load"}, arguments, [ContainerPort(containerPort=CONTROL)], named=False)
        )
        return Pod(self, name, log)

    async def link(self, slot: int, network: Network, /) -> None:
        """Put on the packets of `slot` the chaos `network` asks for, and take off what it no longer does."""
        wanted = self._wanted(slot, network)
        held = self._links.pop(f"slot-{slot}", {})
        await asyncio.gather(*(self._remove(name) for name, link in held.items() if wanted.get(name) != link))
        await asyncio.gather(*(self._inject(name, link[0]) for name, link in wanted.items() if held.get(name) != link))
        self._links[f"slot-{slot}"] = wanted

    async def unlink(self, pod: str, /) -> None:
        """Take off every chaos on the packets of the pod `pod`."""
        await asyncio.gather(*(self._remove(name) for name in self._links.pop(pod, {})))

    async def delete(self, name: str, /, *, grace: int, within: float) -> None:
        """Delete the pod `name`, and wait until it is gone, which frees its name for the next life of its slot."""
        try:
            await self.client.delete(core_v1.Pod, name, grace_period=grace)
        except ApiError as error:
            if not _gone(error):
                raise
        await _until(lambda: self._absent(core_v1.Pod, name), within, f"the pod {name} was not gone")

    async def _database(self) -> str:
        """Start the PostgreSQL of the run on an empty database, and answer its URL once it takes connections."""
        user = [EnvVar(name=name, value="casty") for name in ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB")]
        # Over TCP: the server that initializes the database listens on no port, and is not the one the nodes reach.
        ready = ["pg_isready", "--host=127.0.0.1", "--username=casty", "--dbname=casty"]
        await self.client.create(
            core_v1.Pod(
                metadata=ObjectMeta(name=STORE, labels={ROLE: STORE}, ownerReferences=[self._owner]),
                spec=PodSpec(
                    restartPolicy="Never",
                    enableServiceLinks=False,
                    containers=[
                        Container(
                            name="postgres",
                            image=POSTGRES,
                            env=user,
                            ports=[ContainerPort(containerPort=5432)],
                            readinessProbe=Probe(exec=ExecAction(command=ready), periodSeconds=1),
                        )
                    ],
                ),
            )
        )
        address: list[str] = []

        async def started() -> bool:
            status = (await self.client.get(core_v1.Pod, STORE)).status
            conditions = (status.conditions if status else None) or []
            if status and status.podIP and any(each.type == "Ready" and each.status == "True" for each in conditions):
                address.append(status.podIP)
            return bool(address)

        await _until(started, _STARTING, "the PostgreSQL of the run did not take connections")
        return f"postgres://casty:casty@{address[0]}:5432/casty"

    def _pod(
        self, name: str, labels: dict[str, str], arguments: list[str], ports: list[ContainerPort], /, *, named: bool
    ) -> core_v1.Pod:
        """The pod `name`; a `named` one also gets a name in the headless service, which it advertises."""
        return core_v1.Pod(
            metadata=ObjectMeta(name=name, labels=labels, ownerReferences=[self._owner]),
            spec=PodSpec(
                hostname=name if named else None,
                subdomain=SERVICE if named else None,
                restartPolicy="Never",
                enableServiceLinks=False,
                containers=[
                    Container(
                        name=labels[ROLE],
                        image=self._image,
                        args=arguments,
                        ports=ports,
                        resources=self._shape.resources(),
                    )
                ],
            ),
        )

    def _wanted(self, slot: int, network: Network, /) -> Links:
        wanted: Links = {}
        if network.blocked:
            wanted[f"slot-{slot}-cut"] = self._link(slot, "partition", network.blocked, {})
        slow: dict[int, list[str]] = {}
        for address, delay in network.delays.items():
            slow.setdefault(round(delay * 1000), []).append(address)
        for millis, addresses in slow.items():
            wanted[f"slot-{slot}-slow-{millis}"] = self._link(
                slot, "delay", addresses, {"delay": {"latency": f"{millis}ms"}}
            )
        return wanted

    def _link(
        self, slot: int, action: str, addresses: Iterable[str], extra: dict[str, object], /
    ) -> tuple[dict[str, object], tuple[int, ...]]:
        targets = sorted(self.addresses.index(address) for address in addresses)
        spec: dict[str, object] = {
            "action": action,
            "mode": "all",
            "selector": {"namespaces": [self._namespace], "labelSelectors": {ROLE: "node", SLOT: str(slot)}},
            "direction": "to",
            "target": {
                "mode": "all",
                "selector": {
                    "namespaces": [self._namespace],
                    "labelSelectors": {ROLE: "node"},
                    "expressionSelectors": [
                        {"key": SLOT, "operator": "In", "values": [str(target) for target in targets]}
                    ],
                },
            },
            **extra,
        }
        return spec, tuple(self._lives[target] for target in targets)

    async def _inject(self, name: str, spec: dict[str, object], /) -> None:
        chaos = NetworkChaos(metadata=ObjectMeta(name=name, ownerReferences=[self._owner]), spec=spec)
        await self.client.create(chaos)

        async def injected() -> bool:
            return _injected(await self.client.get(NetworkChaos, name))

        await _until(injected, _CHAOS, f"Chaos Mesh did not inject {name}")

    async def _remove(self, name: str, /) -> None:
        """Delete the chaos `name`, and wait until Chaos Mesh has taken it off and let it go."""
        try:
            await self.client.delete(NetworkChaos, name)
        except ApiError as error:
            if not _gone(error):
                raise
        await _until(lambda: self._absent(NetworkChaos, name), _CHAOS, f"Chaos Mesh did not recover from {name}")

    async def _absent(self, kind: type[core_v1.Pod] | type[NetworkChaos], name: str, /) -> bool:
        try:
            await self.client.get(kind, name)
        except ApiError as error:
            if _gone(error):
                return True
            raise
        return False


class Pod:
    """A pod of a run, a node or a client, from its start to its end.

    It is ready once its process says `{"ready": ...}`, and it left if it said `{"left": null}` before it ended.
    """

    def __init__(self, site: Kubernetes, name: str, log: Path, /) -> None:
        self.name = name
        self.log = log
        self._site = site
        self._address: str | None = None
        self._ready: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._left = False
        self._sink = log.open("a")
        self._following = asyncio.create_task(self._follow())

    @property
    def ended(self) -> bool:
        return self._following.done()

    async def ready(self, within: float, /) -> None:
        try:
            async with asyncio.timeout(within):
                await self._ready
        except TimeoutError:
            raise Stuck(f"{self.name} was not ready within {within:.0f}s; see {self.log}") from None

    async def connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """A connection to the control port of the pod."""
        if self._address is None:
            raise Stuck(f"{self.name} has no address; see {self.log}")
        return await asyncio.open_connection(self._address, CONTROL, limit=_LINE)

    async def ask(self, command: dict[str, object], within: float, /) -> object:
        """Send `command` on a connection of its own, and answer what the pod says back."""
        async with asyncio.timeout(within):
            reader, writer = await self.connect()
            try:
                writer.write((json.dumps(command) + "\n").encode())
                await writer.drain()
                line = await reader.readline()
            finally:
                writer.close()
        if not line:
            raise Stuck(f"{self.name} hung up; see {self.log}")
        return json.loads(line)

    async def kill(self) -> None:
        """End the process at once, draining nothing, and then the pod."""
        if self._address is not None:
            with suppress(OSError, TimeoutError):
                async with asyncio.timeout(_ASKING):
                    _, writer = await asyncio.open_connection(self._address, CONTROL)
                    writer.write(b'{"crash": null}\n')
                    await writer.drain()
                    writer.close()
                await asyncio.wait({self._following}, timeout=_ASKING)
        await self._site.unlink(self.name)
        await self._site.delete(self.name, grace=0, within=_GONE)
        await self._following

    async def leave(self, within: float, /) -> None:
        """Delete the pod with `within` seconds of grace, and wait until the process left and the pod is gone."""
        await self._site.unlink(self.name)
        await self._site.client.delete(core_v1.Pod, self.name, grace_period=math.ceil(within))
        await asyncio.wait({self._following}, timeout=within)
        if not self._following.done():
            await self.kill()
            raise Stuck(f"{self.name} did not finish leaving within {within:.0f}s; see {self.log}")
        await self._site.delete(self.name, grace=0, within=_GONE)
        if not self._left:
            raise Stuck(f"{self.name} ended without leaving; see {self.log}")

    async def _follow(self) -> None:
        try:
            if await self._wait():
                async for line in self._site.client.log(self.name, follow=True):
                    self._sink.write(line)
                    self._sink.flush()
                    self._heard(line)
        except Exception as error:
            # The log is how the runner knows the process ended, so however it breaks off, the pod counts as gone.
            self._sink.write(f"[runner] the log of {self.name} broke off: {error!r}\n")
        finally:
            self._sink.close()
            if not self._ready.done():
                self._ready.set_exception(Stuck(f"{self.name} exited; see {self.log}"))

    async def _wait(self) -> bool:
        """Wait until the container has started, noting in the log what it waits on; answer whether it did before
        its pod went."""
        said = None
        while True:
            try:
                pod = await self._site.client.get(core_v1.Pod, self.name)
            except ApiError as error:
                if _gone(error):
                    return False
                raise
            if pod.status and pod.status.podIP:
                self._address = pod.status.podIP
            waiting = _waiting(pod)
            if waiting is None:
                return True
            if waiting != said:
                self._sink.write(f"[runner] {self.name} waits: {waiting}\n")
                said = waiting
            await asyncio.sleep(_POLL)

    def _heard(self, line: str, /) -> None:
        try:
            said = records.record(json.loads(line))
        except ValueError:
            return  # a line of something else the process printed
        if "ready" in said and not self._ready.done():
            self._ready.set_result(None)
        elif "left" in said:
            self._left = True


async def _until(check: Callable[[], Awaitable[bool]], within: float, failure: str, /) -> None:
    """Ask `check` a few times a second until it says yes; raise `Stuck` saying `failure` if it has not in `within`
    seconds."""

    async def holds() -> None:
        await asyncio.sleep(_POLL)
        assert await check(), failure

    try:
        await eventually(holds, timedelta(seconds=within))
    except (AssertionError, TimeoutError):
        raise Stuck(f"{failure} within {within:.0f}s") from None


def _gone(error: ApiError, /) -> bool:
    return error.status.code == 404


def _waiting(pod: core_v1.Pod, /) -> str | None:
    """What the container of `pod` waits on, or `None` once it started."""
    statuses = (pod.status.containerStatuses if pod.status else None) or []
    state = statuses[0].state if statuses else None
    if state is None:
        return "scheduling"
    if state.running is not None or state.terminated is not None:
        return None
    return f"{state.waiting.reason}: {state.waiting.message}" if state.waiting else "scheduling"


def _injected(chaos: object, /) -> bool:
    status = records.record(chaos).get("status")
    if status is None:
        return False
    conditions = (records.record(each) for each in records.items(records.record(status).get("conditions", [])))
    return any(condition.get("type") == "AllInjected" and condition.get("status") == "True" for condition in conditions)


_ASKING = 5.0
"""Seconds a pod has to answer on its control port."""
_CHAOS = 60.0
"""Seconds Chaos Mesh has to put a chaos on or take it off."""
_GONE = 60.0
"""Seconds a deleted pod has to be gone."""
_STARTING = 180.0
"""Seconds the PostgreSQL of a run has to take connections, its image pulled included."""
_POLL = 0.2
_LINE = 1 << 24
"""The longest line a pod may answer, which is the sample of a client of a performance run."""
