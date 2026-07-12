from __future__ import annotations

import random
import uuid

from casty.membership.table import Member


class Views:
    """HyParView active/passive view bookkeeping. Pure: decisions only, the
    service owns connections. Randomness injected for deterministic tests."""

    def __init__(
        self,
        self_member: Member,
        *,
        active_size: int,
        passive_size: int,
        rng: random.Random | None = None,
    ) -> None:
        self.self_member = self_member
        self._active_size = active_size
        self._passive_size = passive_size
        self._rng = rng or random.Random()
        self._active: dict[uuid.UUID, Member] = {}
        self._passive: dict[uuid.UUID, Member] = {}

    # --- active ------------------------------------------------------------

    @property
    def active(self) -> list[Member]:
        return list(self._active.values())

    def in_active(self, node_id: uuid.UUID) -> bool:
        return node_id in self._active

    def active_full(self) -> bool:
        return len(self._active) >= self._active_size

    def add_active(self, member: Member) -> Member | None:
        """Add to the active view. Returns a member evicted to make room (the
        service must DISCONNECT it), or None."""
        if member.node_id == self.self_member.node_id or member.node_id in self._active:
            return None
        self._passive.pop(member.node_id, None)
        evicted: Member | None = None
        if len(self._active) >= self._active_size:
            victim_id = self._rng.choice(list(self._active))
            evicted = self._active.pop(victim_id)
            self._add_passive(evicted)
        self._active[member.node_id] = member
        return evicted

    def remove_active(self, node_id: uuid.UUID, *, to_passive: bool) -> Member | None:
        member = self._active.pop(node_id, None)
        if member is not None and to_passive:
            self._add_passive(member)
        return member

    def random_active(self, exclude: uuid.UUID | None = None) -> Member | None:
        candidates = [m for nid, m in self._active.items() if nid != exclude]
        return self._rng.choice(candidates) if candidates else None

    # --- passive -----------------------------------------------------------

    @property
    def passive(self) -> list[Member]:
        return list(self._passive.values())

    def add_passive(self, member: Member) -> None:
        if member.node_id == self.self_member.node_id or member.node_id in self._active:
            return
        self._add_passive(member)

    def _add_passive(self, member: Member) -> None:
        if member.node_id in self._passive:
            return
        if len(self._passive) >= self._passive_size:
            victim = self._rng.choice(list(self._passive))
            del self._passive[victim]
        self._passive[member.node_id] = member

    def remove_passive(self, node_id: uuid.UUID) -> None:
        self._passive.pop(node_id, None)

    def promotion_candidate(self) -> Member | None:
        if not self._passive:
            return None
        return self._passive[self._rng.choice(list(self._passive))]

    # --- shuffle -------------------------------------------------------------

    def shuffle_sample(self, k_active: int, k_passive: int) -> list[Member]:
        active = self._rng.sample(self.active, min(k_active, len(self._active)))
        passive = self._rng.sample(self.passive, min(k_passive, len(self._passive)))
        return [self.self_member, *active, *passive]

    def integrate_shuffle(self, received: list[Member]) -> None:
        for member in received:
            self.add_passive(member)
