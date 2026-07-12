from __future__ import annotations

import struct
import typing
import uuid
from dataclasses import dataclass, field

from casty.membership import messages

_SEQ = struct.Struct("!Q")


@dataclass(frozen=True, slots=True)
class SendGossip:
    to: uuid.UUID
    gossip: messages.Gossip


@dataclass(frozen=True, slots=True)
class SendIHave:
    to: uuid.UUID
    msg_id: bytes


@dataclass(frozen=True, slots=True)
class SendGraft:
    to: uuid.UUID
    msg_id: bytes


@dataclass(frozen=True, slots=True)
class SendPrune:
    to: uuid.UUID


type Command = SendGossip | SendIHave | SendGraft | SendPrune


class GossipResult(typing.NamedTuple):
    deliver: bool  # first sighting: apply the event to the member table
    commands: list[Command]


@dataclass(slots=True)
class _Missing:
    announcers: list[uuid.UUID] = field(default_factory=list)
    deadline: float = 0.0


class Broadcaster:
    """Plumtree (epidemic broadcast trees) over the active view. Pure: methods
    return commands; the service does the sending and drives timers via
    `expire_missing`."""

    def __init__(self, self_id: uuid.UUID, *, graft_timeout: float, seen_ttl: float) -> None:
        self._self_id = self_id
        self._graft_timeout = graft_timeout
        self._seen_ttl = seen_ttl
        self._seq = 0
        self._eager: set[uuid.UUID] = set()
        self._lazy: set[uuid.UUID] = set()
        self._seen: dict[bytes, tuple[messages.Gossip, float]] = {}
        self._missing: dict[bytes, _Missing] = {}

    # --- membership of the tree -----------------------------------------------

    def add_peer(self, node_id: uuid.UUID) -> None:
        if node_id not in self._lazy:
            self._eager.add(node_id)

    def remove_peer(self, node_id: uuid.UUID) -> None:
        self._eager.discard(node_id)
        self._lazy.discard(node_id)
        for missing in self._missing.values():
            if node_id in missing.announcers:
                missing.announcers.remove(node_id)

    @property
    def peers(self) -> set[uuid.UUID]:
        return self._eager | self._lazy

    # --- broadcast --------------------------------------------------------------

    def broadcast(self, event: messages.ClusterEvent, now: float) -> list[Command]:
        self._seq += 1
        msg_id = self._self_id.bytes + _SEQ.pack(self._seq)
        gossip = messages.Gossip(msg_id=msg_id, round=0, event=event)
        self._seen[msg_id] = (gossip, now)
        return self._push(gossip, exclude=None)

    def on_gossip(self, sender: uuid.UUID, gossip: messages.Gossip, now: float) -> GossipResult:
        """First sighting delivers and forwards; a duplicate prunes the link."""
        self._missing.pop(gossip.msg_id, None)
        if gossip.msg_id in self._seen:
            self._to_lazy(sender)
            return GossipResult(deliver=False, commands=[SendPrune(to=sender)])
        self._seen[gossip.msg_id] = (gossip, now)
        self._to_eager(sender)
        forwarded = messages.Gossip(
            msg_id=gossip.msg_id, round=gossip.round + 1, event=gossip.event
        )
        return GossipResult(deliver=True, commands=self._push(forwarded, exclude=sender))

    def on_ihave(self, sender: uuid.UUID, msg_ids: list[bytes], now: float) -> None:
        for msg_id in msg_ids:
            if msg_id in self._seen:
                continue
            missing = self._missing.setdefault(
                msg_id, _Missing(deadline=now + self._graft_timeout)
            )
            if sender not in missing.announcers:
                missing.announcers.append(sender)

    def on_graft(self, sender: uuid.UUID, msg_id: bytes) -> list[Command]:
        self._to_eager(sender)
        entry = self._seen.get(msg_id)
        if entry is None:
            return []
        return [SendGossip(to=sender, gossip=entry[0])]

    def on_prune(self, sender: uuid.UUID) -> None:
        self._to_lazy(sender)

    def expire_missing(self, now: float) -> list[Command]:
        """Graft timers: for every announced-but-never-received id past its
        deadline, ask one announcer for the payload over an eager link."""
        commands: list[Command] = []
        for msg_id, missing in list(self._missing.items()):
            if missing.deadline > now:
                continue
            if not missing.announcers:
                del self._missing[msg_id]
                continue
            announcer = missing.announcers.pop(0)
            missing.deadline = now + self._graft_timeout
            self._to_eager(announcer)
            commands.append(SendGraft(to=announcer, msg_id=msg_id))
        return commands

    def sweep_seen(self, now: float) -> None:
        expired = [
            msg_id for msg_id, (_, seen_at) in self._seen.items()
            if seen_at + self._seen_ttl <= now
        ]
        for msg_id in expired:
            del self._seen[msg_id]

    # --- internals ------------------------------------------------------------------

    def _push(self, gossip: messages.Gossip, exclude: uuid.UUID | None) -> list[Command]:
        commands: list[Command] = [
            SendGossip(to=peer, gossip=gossip) for peer in self._eager if peer != exclude
        ]
        commands.extend(
            SendIHave(to=peer, msg_id=gossip.msg_id) for peer in self._lazy if peer != exclude
        )
        return commands

    def _to_eager(self, node_id: uuid.UUID) -> None:
        if node_id in self._lazy:
            self._lazy.discard(node_id)
            self._eager.add(node_id)

    def _to_lazy(self, node_id: uuid.UUID) -> None:
        if node_id in self._eager:
            self._eager.discard(node_id)
            self._lazy.add(node_id)
