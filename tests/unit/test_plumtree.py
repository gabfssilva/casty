from __future__ import annotations

import uuid
from collections.abc import Sequence

from casty.membership import messages
from casty.membership.plumtree import (
    Broadcaster,
    SendGossip,
    SendGraft,
    SendIHave,
    SendPrune,
)


def nid(name: str) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_DNS, name)


def event(name: str = "x") -> messages.NodeDead:
    return messages.NodeDead(node_id=nid(name), incarnation=0)


def make(peers: list[str]) -> Broadcaster:
    b = Broadcaster(nid("self"), graft_timeout=0.5, seen_ttl=60.0)
    for peer in peers:
        b.add_peer(nid(peer))
    return b


def gossip_of(commands: Sequence[object]) -> messages.Gossip:
    sends = [c for c in commands if isinstance(c, SendGossip)]
    assert sends
    return sends[0].gossip


def test_broadcast_pushes_eager_to_all_peers() -> None:
    b = make(["a", "b"])
    commands = b.broadcast(event(), now=0.0)
    targets = {c.to for c in commands if isinstance(c, SendGossip)}
    assert targets == {nid("a"), nid("b")}


def test_first_gossip_delivers_and_forwards() -> None:
    b = make(["a", "b"])
    incoming = gossip_of(make(["x"]).broadcast(event(), now=0.0))
    deliver, commands = b.on_gossip(nid("a"), incoming, now=0.0)
    assert deliver
    forwarded = {c.to for c in commands if isinstance(c, SendGossip)}
    assert forwarded == {nid("b")}  # not back to the sender
    assert all(c.gossip.round == incoming.round + 1 for c in commands if isinstance(c, SendGossip))


def test_duplicate_gossip_prunes_sender() -> None:
    b = make(["a", "b"])
    incoming = gossip_of(make(["x"]).broadcast(event(), now=0.0))
    b.on_gossip(nid("a"), incoming, now=0.0)
    deliver, commands = b.on_gossip(nid("b"), incoming, now=0.0)
    assert not deliver
    assert commands == [SendPrune(to=nid("b"))]
    # pruned peer now gets IHAVE instead of GOSSIP
    commands = b.broadcast(event("y"), now=1.0)
    ihave_targets = {c.to for c in commands if isinstance(c, SendIHave)}
    assert nid("b") in ihave_targets


def test_ihave_then_timeout_grafts() -> None:
    b = make(["a"])
    b.on_prune(nid("a"))
    b.on_ihave(nid("a"), [b"some-id"], now=0.0)
    assert b.expire_missing(now=0.1) == []  # not due yet
    commands = b.expire_missing(now=1.0)
    assert commands == [SendGraft(to=nid("a"), msg_id=b"some-id")]


def test_ihave_for_seen_message_is_ignored() -> None:
    b = make(["a"])
    commands = b.broadcast(event(), now=0.0)
    msg_id = gossip_of(commands).msg_id
    b.on_ihave(nid("a"), [msg_id], now=0.0)
    assert b.expire_missing(now=10.0) == []


def test_graft_resends_payload_and_promotes() -> None:
    b = make(["a"])
    b.on_prune(nid("a"))
    commands = b.broadcast(event(), now=0.0)
    ihaves = [c for c in commands if isinstance(c, SendIHave)]
    assert len(ihaves) == 1  # peer is lazy, gets the announcement only
    replies = b.on_graft(nid("a"), ihaves[0].msg_id)
    assert len(replies) == 1 and isinstance(replies[0], SendGossip)
    # promoted back to eager: next broadcast goes as GOSSIP
    commands = b.broadcast(event("y"), now=1.0)
    assert any(isinstance(c, SendGossip) and c.to == nid("a") for c in commands)


def test_remove_peer_forgets_announcers() -> None:
    b = make(["a"])
    b.on_ihave(nid("a"), [b"id"], now=0.0)
    b.remove_peer(nid("a"))
    assert b.expire_missing(now=10.0) == []
