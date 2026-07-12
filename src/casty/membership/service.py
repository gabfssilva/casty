from __future__ import annotations

import asyncio
import contextlib
import logging
import typing
import uuid
from collections.abc import Callable, Sequence

from casty.config import MembershipConfig
from casty.errors import CastyError, CastyTimeoutError
from casty.membership import messages, plumtree
from casty.membership.hyparview import Views
from casty.membership.table import Member, MemberTable, Status, ViewEvent
from casty.serde import codec
from casty.transport.connection import Connection
from casty.transport.pool import Pool

logger = logging.getLogger("casty.membership")


def _to_msg(member: Member) -> messages.MemberMsg:
    return messages.MemberMsg(node_id=member.node_id, addr=member.addr, role=member.role)


def _from_msg(msg: messages.MemberMsg) -> Member:
    return Member(node_id=msg.node_id, addr=msg.addr, role=msg.role)


class Membership:
    """Cluster membership: HyParView overlay + Plumtree broadcast + member table.
    Owns no sockets — the transport owner (Node, or the test host) wires
    `handle_connection` / `handle_control` / `handle_close` into Server and Pool,
    and assigns `pool` before `start()`."""

    pool: Pool  # assigned by the transport owner before start()

    def __init__(self, self_member: Member, config: MembershipConfig | None = None) -> None:
        self.self_member = self_member
        self._config = config or MembershipConfig()
        self.table = MemberTable(self_member)
        self.views = Views(
            self_member,
            active_size=self._config.active_view_size,
            passive_size=self._config.passive_view_size,
        )
        self.broadcaster = plumtree.Broadcaster(
            self_member.node_id,
            graft_timeout=self._config.graft_timeout,
            seen_ttl=self._config.seen_ttl,
        )
        self._links: dict[uuid.UUID, Connection] = {}
        self._pending_neighbor: dict[uuid.UUID, Member] = {}
        self._subscribers: list[Callable[[ViewEvent], None]] = []
        self._tasks: list[asyncio.Task[None]] = []
        self._spawned: set[asyncio.Task[None]] = set()
        self._promoting = False
        self._rejoining = False
        self._seeds: list[str] = []
        self._stopped = False

    # --- public API ------------------------------------------------------------

    def alive_members(self) -> frozenset[Member]:
        return self.table.alive_members()

    def subscribe(self, cb: Callable[[ViewEvent], None]) -> Callable[[], None]:
        self._subscribers.append(cb)

        def unsubscribe() -> None:
            with contextlib.suppress(ValueError):
                self._subscribers.remove(cb)

        return unsubscribe

    def start(self) -> None:
        self._tasks = [
            asyncio.create_task(self._sweep_loop()),
            asyncio.create_task(self._shuffle_loop()),
            asyncio.create_task(self._anti_entropy_loop()),
        ]

    async def join(self, seeds: Sequence[str]) -> None:
        self._seeds = list(seeds)
        last_error: Exception | None = None
        for seed in seeds:
            try:
                await asyncio.wait_for(self._join_seed(seed), self._config.join_timeout)
                return
            except TimeoutError as exc:
                last_error = CastyTimeoutError(f"join via {seed} timed out")
                last_error.__cause__ = exc
            except (CastyError, OSError) as exc:
                last_error = exc
        if last_error is not None:
            raise last_error

    async def leave(self) -> None:
        await self._broadcast(
            messages.NodeLeft(node_id=self.self_member.node_id, incarnation=self.table.incarnation)
        )
        for node_id, conn in list(self._links.items()):
            with contextlib.suppress(CastyError, OSError):
                await conn.send_control(
                    messages.DISCONNECT,
                    codec.encode(messages.Disconnect(node_id=self.self_member.node_id)),
                )
            self._drop_link(node_id, to_passive=True)
        await self.stop()

    async def stop(self) -> None:
        self._stopped = True
        for task in [*self._tasks, *self._spawned]:
            task.cancel()
        for task in [*self._tasks, *self._spawned]:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._tasks.clear()
        self._spawned.clear()

    # --- transport wiring ---------------------------------------------------------

    async def handle_connection(self, conn: Connection) -> None:
        self.pool.register(conn)

    def handle_close(self, conn: Connection, exc: Exception | None) -> None:
        peer = conn.peer
        if peer is None or self._stopped:
            return
        node_id = peer.node_id
        if self._links.get(node_id) is not conn:
            return
        member = next((m for m in self.views.active if m.node_id == node_id), None)
        self._drop_link(node_id, to_passive=False)
        if member is not None:
            self._spawn(self._on_link_failed(member))

    async def handle_control(self, conn: Connection, msg_type: int, body: bytes) -> None:
        peer = conn.peer
        if peer is None:
            return
        decoded = codec.decode(body)
        match decoded:
            case messages.Join(member=m):
                await self._on_join(conn, _from_msg(m))
            case messages.ForwardJoin(member=m, ttl=ttl):
                await self._on_forward_join(peer.node_id, _from_msg(m), ttl)
            case messages.Neighbor(member=m, high_priority=high):
                await self._on_neighbor(conn, _from_msg(m), high)
            case messages.NeighborReply(member=m, accepted=accepted):
                await self._on_neighbor_reply(conn, _from_msg(m), accepted)
            case messages.Disconnect(node_id=node_id):
                self._drop_link(node_id, to_passive=True)
            case messages.Shuffle(origin=origin, sample=sample, ttl=ttl):
                await self._on_shuffle(peer.node_id, _from_msg(origin), sample, ttl)
            case messages.ShuffleReply(sample=sample):
                self.views.integrate_shuffle([_from_msg(m) for m in sample])
            case messages.Gossip() as gossip:
                await self._on_gossip(peer.node_id, gossip)
            case messages.IHave(msg_ids=msg_ids):
                self.broadcaster.on_ihave(peer.node_id, msg_ids, self._now())
            case messages.Graft(msg_id=msg_id):
                await self._run_commands(self.broadcaster.on_graft(peer.node_id, msg_id))
            case messages.Prune():
                self.broadcaster.on_prune(peer.node_id)
            case messages.Sync(records=records):
                self._merge_records(records)
                await self._send_conn(
                    conn, messages.SYNC_REPLY, messages.SyncReply(records=self._sync_records())
                )
            case messages.SyncReply(records=records):
                self._merge_records(records)
            case _:
                logger.warning("unknown membership message 0x%x from %s", msg_type, peer.node_id)

    # --- HyParView ---------------------------------------------------------------

    async def _join_seed(self, seed: str) -> None:
        conn = await self.pool.get(seed)
        peer = conn.peer
        assert peer is not None
        await self._send_conn(conn, messages.JOIN, messages.Join(member=_to_msg(self.self_member)))
        member = Member(node_id=peer.node_id, addr=peer.listen_addr or seed, role=peer.role)
        await self._link_up(conn, member)
        self._apply(member, 0, Status.ALIVE)
        await self._broadcast(
            messages.NodeJoined(
                member=_to_msg(self.self_member), incarnation=self.table.incarnation
            )
        )

    async def _on_join(self, conn: Connection, joiner: Member) -> None:
        forward_targets = [m for m in self.views.active if m.node_id != joiner.node_id]
        await self._link_up(conn, joiner)
        self._apply(joiner, 0, Status.ALIVE)
        await self._broadcast(messages.NodeJoined(member=_to_msg(joiner), incarnation=0))
        fj = messages.ForwardJoin(member=_to_msg(joiner), ttl=self._config.active_rwl)
        for target in forward_targets:
            await self._send_member(target, messages.FORWARD_JOIN, fj)

    async def _on_forward_join(self, sender: uuid.UUID, joiner: Member, ttl: int) -> None:
        if joiner.node_id == self.self_member.node_id:
            return
        if ttl <= 0 or len(self._links) <= 1:
            await self._request_neighbor(joiner, high_priority=False)
            return
        if ttl == self._config.passive_rwl:
            self.views.add_passive(joiner)
        target = self.views.random_active(exclude=sender)
        if target is None or target.node_id == joiner.node_id:
            await self._request_neighbor(joiner, high_priority=False)
            return
        await self._send_member(
            target, messages.FORWARD_JOIN, messages.ForwardJoin(member=_to_msg(joiner), ttl=ttl - 1)
        )

    async def _request_neighbor(self, member: Member, *, high_priority: bool) -> None:
        if member.node_id in self._links or member.node_id == self.self_member.node_id:
            return
        try:
            conn = await self.pool.get(member.addr)
        except (CastyError, OSError):
            self.views.remove_passive(member.node_id)
            return
        peer = conn.peer
        assert peer is not None
        self._pending_neighbor[peer.node_id] = Member(peer.node_id, member.addr)
        await self._send_conn(
            conn,
            messages.NEIGHBOR,
            messages.Neighbor(member=_to_msg(self.self_member), high_priority=high_priority),
        )

    async def _on_neighbor(self, conn: Connection, member: Member, high_priority: bool) -> None:
        accepted = high_priority or not self.views.active_full()
        await self._send_conn(
            conn,
            messages.NEIGHBOR_REPLY,
            messages.NeighborReply(member=_to_msg(self.self_member), accepted=accepted),
        )
        if accepted:
            await self._link_up(conn, member)
            self._apply(member, 0, Status.ALIVE)

    async def _on_neighbor_reply(self, conn: Connection, member: Member, accepted: bool) -> None:
        pending = self._pending_neighbor.pop(member.node_id, None)
        if pending is None:
            return
        if accepted:
            await self._link_up(conn, member)
            self._apply(member, 0, Status.ALIVE)

    async def _on_shuffle(
        self, sender: uuid.UUID, origin: Member, sample: list[messages.MemberMsg], ttl: int
    ) -> None:
        if ttl > 0 and len(self._links) > 1:
            target = self.views.random_active(exclude=sender)
            if target is not None and target.node_id != origin.node_id:
                await self._send_member(
                    target,
                    messages.SHUFFLE,
                    messages.Shuffle(origin=_to_msg(origin), sample=sample, ttl=ttl - 1),
                )
                return
        members = [_from_msg(m) for m in sample]
        reply_sample = self.views.shuffle_sample(0, len(members))
        self.views.integrate_shuffle([origin, *members])
        if origin.node_id == self.self_member.node_id:
            return
        try:
            conn = await self.pool.get(origin.addr)
        except (CastyError, OSError):
            return
        await self._send_conn(
            conn,
            messages.SHUFFLE_REPLY,
            messages.ShuffleReply(sample=[_to_msg(m) for m in reply_sample]),
        )

    async def _link_up(self, conn: Connection, member: Member) -> None:
        if member.node_id == self.self_member.node_id:
            return
        existing = self._links.get(member.node_id)
        if existing is conn:
            return
        evicted = self.views.add_active(member)
        if evicted is not None:
            evicted_conn = self._links.pop(evicted.node_id, None)
            self.broadcaster.remove_peer(evicted.node_id)
            if evicted_conn is not None:
                with contextlib.suppress(CastyError, OSError):
                    await evicted_conn.send_control(
                        messages.DISCONNECT,
                        codec.encode(messages.Disconnect(node_id=self.self_member.node_id)),
                    )
        self._links[member.node_id] = conn
        self.broadcaster.add_peer(member.node_id)
        await self._send_conn(conn, messages.SYNC, messages.Sync(records=self._sync_records()))

    def _drop_link(self, node_id: uuid.UUID, *, to_passive: bool) -> None:
        self._links.pop(node_id, None)
        self.broadcaster.remove_peer(node_id)
        self.views.remove_active(node_id, to_passive=to_passive)

    async def _on_link_failed(self, member: Member) -> None:
        record = self.table.get(member.node_id)
        incarnation = record.incarnation if record is not None else 0
        self._apply(member, incarnation, Status.SUSPECT)
        await self._broadcast(
            messages.NodeSuspect(node_id=member.node_id, incarnation=incarnation)
        )
        await self._promote()

    async def _promote(self) -> None:
        if self._promoting or self._stopped:
            return
        self._promoting = True
        try:
            attempts = 0
            while len(self._links) < self._config.active_view_size and attempts < 3:
                candidate = self.views.promotion_candidate()
                if candidate is None or candidate.node_id in self._pending_neighbor:
                    return
                attempts += 1
                await self._request_neighbor(
                    candidate, high_priority=len(self._links) == 0
                )
        finally:
            self._promoting = False

    async def _rejoin(self) -> None:
        """Active view emptied out (isolated minority, mass failure): try to
        get back into the overlay through any address we ever knew. The JOIN +
        link-up SYNC resurrects us on the other side (refutation bumps our
        incarnation past any DEAD record)."""
        if self._rejoining or self._stopped:
            return
        self._rejoining = True
        try:
            known = [
                r.member.addr for r in self.table.records() if r.member.role == "member"
            ]
            candidates = [m.addr for m in self.views.passive] + known + self._seeds
            for addr in dict.fromkeys(candidates):
                if addr == self.self_member.addr:
                    continue
                try:
                    await self._join_seed(addr)
                    return
                except (CastyError, OSError):
                    continue
        finally:
            self._rejoining = False

    # --- gossip / table --------------------------------------------------------------

    async def _on_gossip(self, sender: uuid.UUID, gossip: messages.Gossip) -> None:
        deliver, commands = self.broadcaster.on_gossip(sender, gossip, self._now())
        if deliver:
            await self._deliver(gossip.event)
        await self._run_commands(commands)

    async def _deliver(self, event: messages.ClusterEvent) -> None:
        match event:
            case messages.NodeJoined(member=m, incarnation=inc):
                await self._observe(_from_msg(m), inc, Status.ALIVE)
            case messages.NodeAlive(member=m, incarnation=inc):
                await self._observe(_from_msg(m), inc, Status.ALIVE)
            case messages.NodeSuspect(node_id=node_id, incarnation=inc):
                await self._observe_by_id(node_id, inc, Status.SUSPECT)
            case messages.NodeDead(node_id=node_id, incarnation=inc):
                await self._observe_by_id(node_id, inc, Status.DEAD)
            case messages.NodeLeft(node_id=node_id, incarnation=inc):
                await self._observe_by_id(node_id, inc, Status.LEFT)
            case _:
                typing.assert_never(event)

    async def _observe(self, member: Member, incarnation: int, status: Status) -> None:
        if self.table.needs_refutation(member.node_id, incarnation, status):
            await self._refute(incarnation)
            return
        self._apply(member, incarnation, status)
        if status is Status.ALIVE:
            self.views.add_passive(member)

    async def _observe_by_id(self, node_id: uuid.UUID, incarnation: int, status: Status) -> None:
        if self.table.needs_refutation(node_id, incarnation, status):
            await self._refute(incarnation)
            return
        record = self.table.get(node_id)
        if record is None:
            return
        self._apply(record.member, incarnation, status)

    async def _refute(self, observed_incarnation: int) -> None:
        incarnation = self.table.refute(observed_incarnation)
        await self._broadcast(
            messages.NodeAlive(member=_to_msg(self.self_member), incarnation=incarnation)
        )

    def _apply(self, member: Member, incarnation: int, status: Status) -> None:
        event = self.table.merge(member, incarnation, status, self._now())
        if event is not None:
            self._emit(event)

    async def _broadcast(self, event: messages.ClusterEvent) -> None:
        await self._run_commands(self.broadcaster.broadcast(event, self._now()))

    def _merge_records(self, records: list[messages.SyncRecord]) -> None:
        for record in records:
            status = Status(record.status)
            if self.table.needs_refutation(record.member.node_id, record.incarnation, status):
                self._spawn(self._refute(record.incarnation))
                continue
            member = _from_msg(record.member)
            self._apply(member, record.incarnation, status)
            if status is Status.ALIVE:
                self.views.add_passive(member)

    def _sync_records(self) -> list[messages.SyncRecord]:
        records = [
            messages.SyncRecord(
                member=_to_msg(r.member), incarnation=r.incarnation, status=int(r.status)
            )
            for r in self.table.records()
        ]
        records.append(
            messages.SyncRecord(
                member=_to_msg(self.self_member),
                incarnation=self.table.incarnation,
                status=int(Status.ALIVE),
            )
        )
        return records

    # --- command execution -------------------------------------------------------------

    async def _run_commands(self, commands: list[plumtree.Command]) -> None:
        for command in commands:
            match command:
                case plumtree.SendGossip(to=to, gossip=gossip):
                    await self._send_node(to, messages.GOSSIP, gossip)
                case plumtree.SendIHave(to=to, msg_id=msg_id):
                    await self._send_node(to, messages.IHAVE, messages.IHave(msg_ids=[msg_id]))
                case plumtree.SendGraft(to=to, msg_id=msg_id):
                    await self._send_node(to, messages.GRAFT, messages.Graft(msg_id=msg_id))
                case plumtree.SendPrune(to=to):
                    await self._send_node(
                        to, messages.PRUNE, messages.Prune(node_id=self.self_member.node_id)
                    )
                case _:
                    typing.assert_never(command)

    async def _send_node(self, node_id: uuid.UUID, msg_type: int, msg: object) -> None:
        conn = self._links.get(node_id)
        if conn is None:
            return
        await self._send_conn(conn, msg_type, msg)

    async def _send_member(self, member: Member, msg_type: int, msg: object) -> None:
        await self._send_node(member.node_id, msg_type, msg)

    async def _send_conn(self, conn: Connection, msg_type: int, msg: object) -> None:
        with contextlib.suppress(CastyError, OSError):
            await conn.send_control(msg_type, codec.encode(msg))

    # --- timers -----------------------------------------------------------------------------

    async def _sweep_loop(self) -> None:
        cfg = self._config
        while True:
            await asyncio.sleep(cfg.sweep_interval)
            now = self._now()
            for record in self.table.suspects_older_than(now - cfg.suspicion_timeout):
                self._apply(record.member, record.incarnation, Status.DEAD)
                await self._broadcast(
                    messages.NodeDead(
                        node_id=record.member.node_id, incarnation=record.incarnation
                    )
                )
            self.table.sweep_tombstones(now - cfg.tombstone_ttl)
            await self._run_commands(self.broadcaster.expire_missing(now))
            self.broadcaster.sweep_seen(now)
            if len(self._links) < cfg.active_view_size and self.views.passive:
                await self._promote()
            if not self._links:
                await self._rejoin()

    async def _shuffle_loop(self) -> None:
        cfg = self._config
        while True:
            await asyncio.sleep(cfg.shuffle_interval)
            target = self.views.random_active()
            if target is None:
                continue
            sample = self.views.shuffle_sample(
                cfg.shuffle_active_sample, cfg.shuffle_passive_sample
            )
            await self._send_member(
                target,
                messages.SHUFFLE,
                messages.Shuffle(
                    origin=_to_msg(self.self_member),
                    sample=[_to_msg(m) for m in sample],
                    ttl=cfg.passive_rwl,
                ),
            )

    async def _anti_entropy_loop(self) -> None:
        cfg = self._config
        while True:
            await asyncio.sleep(cfg.anti_entropy_interval)
            target = self.views.random_active()
            if target is None:
                continue
            await self._send_member(
                target, messages.SYNC, messages.Sync(records=self._sync_records())
            )

    # --- misc ----------------------------------------------------------------------------------

    def _emit(self, event: ViewEvent) -> None:
        for cb in self._subscribers:
            try:
                cb(event)
            except Exception:
                logger.exception("membership subscriber raised")

    def _spawn(self, coro: typing.Coroutine[object, object, None]) -> None:
        if self._stopped:
            coro.close()
            return
        task = asyncio.create_task(coro)
        self._spawned.add(task)
        task.add_done_callback(self._spawned.discard)

    def _now(self) -> float:
        return asyncio.get_running_loop().time()
