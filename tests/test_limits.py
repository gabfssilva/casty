from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, assert_never

import pytest

from casty import Client, Compression, Context, Limits, MessageTooLarge, Ref, Refused, actor
from tests.app import Locate
from tests.cluster import NARROW, Harness, Node

MIB = 1024 * 1024
OVER = 4 * MIB + 1
"""One byte over the default `Limits.message`, before what surrounds it."""


@dataclass(frozen=True)
class Echo:
    reply_to: Ref[bytes]
    payload: bytes


@dataclass(frozen=True)
class Grow:
    reply_to: Ref[bytes]
    size: int


@dataclass(frozen=True)
class Keep:
    payload: bytes


type EchoMsg = Echo | Grow | Keep | Locate


@actor(initial=b"")
async def echo(ctx: Context[bytes, EchoMsg]) -> None:
    """Answers what it was sent, or as many bytes as it is asked for, and says where it runs."""
    async for msg in ctx.inbox:
        match msg:
            case Echo(reply_to, payload):
                reply_to.tell(payload)
            case Grow(reply_to, size):
                reply_to.tell(bytes(size))
            case Keep():
                pass
            case Locate(reply_to):
                reply_to.tell(ctx.system.node)
            case _:
                assert_never(msg)


WIDE = "f" * (64 * 1024)
"""A field name that leaves no room for its data in a message of `NARROW`."""

if TYPE_CHECKING:

    @dataclass(frozen=True)
    class Wide:
        """A state of one field, named `WIDE`."""

else:
    from dataclasses import field, make_dataclass

    Wide = make_dataclass("Wide", [(WIDE, int, field(default=0))], frozen=True)


@dataclass(frozen=True)
class Widen:
    reply_to: Ref[str]


@actor
async def wide(ctx: Context[Wide, Widen]) -> None:
    """What `narrow` becomes, if the state it saves fits in a message."""
    async for msg in ctx.inbox:
        msg.reply_to.tell("widened")


@actor(initial=b"")
async def narrow(ctx: Context[bytes, Widen]) -> None:
    """Becomes `wide`, and answers why it could not."""
    async for msg in ctx.inbox:
        try:
            await ctx.become(wide, Wide())
        except MessageTooLarge as refused:
            msg.reply_to.tell(str(refused))


INVALID: list[tuple[Callable[[], object], str]] = [
    (lambda: Limits(frame=0), "limits.frame"),
    (lambda: Limits(window=-1), "limits.window"),
    (lambda: Limits(message=4 * 1024 * MIB), "limits.message"),
    (lambda: Limits(frame=16 * 1024, message=64 * 1024, window=16 * 1024), "limits.message"),
    (lambda: Limits(frame=8 * MIB), "limits.frame"),
    (lambda: Limits(window=64 * 1024), "limits.window"),
    (lambda: Compression(min_bytes=-1), "compression.min_bytes"),
]

OWNERS = pytest.mark.parametrize("owner", [0, 1], ids=["on-the-sender", "on-another-node"])
"""Which node of two owns the key the first one sends to: the refusals must not depend on it."""


def describe_limits() -> None:
    @pytest.mark.parametrize(("build", "parameter"), INVALID, ids=[parameter for _, parameter in INVALID])
    def it_rejects_inconsistent_sizes_naming_them(build: Callable[[], object], parameter: str) -> None:
        with pytest.raises(ValueError, match=parameter.replace(".", r"\.")):
            build()

    def when_every_node_and_client_raise_the_message_limit() -> None:
        async def it_carries_a_payload_the_default_refuses_and_reads_it_back_identically() -> None:
            payload = bytes(range(256)) * (16 * MIB // 256)

            async with Harness.start(2, limits=Limits(message=32 * MIB)) as harness:
                a, b = harness.nodes
                key = await _key_on(b, a)
                client = await harness.client()

                assert await a.system.ref(echo, key).ask(Echo, payload) == payload
                assert await client.ref(echo, key).ask(Echo, payload) == payload

    def when_the_nodes_keep_the_defaults() -> None:
        @OWNERS
        async def it_raises_at_the_sender_for_a_message_or_initial_state_over_4_mib(owner: int) -> None:
            async with Harness.start(2) as harness:
                a = harness.nodes[0]
                key = await _key_on(harness.nodes[owner], a)
                ref = a.system.ref(echo, key)
                client = await harness.client()

                # Raised where it is sent, not the `TimeoutError` of an answer that never comes.
                with pytest.raises(MessageTooLarge, match=r"message to .* limits\.message of 4194304"):
                    await ref.ask(Echo, bytes(OVER))
                with pytest.raises(MessageTooLarge, match=r"message to .* limits\.message of 4194304"):
                    ref.tell(Keep(bytes(OVER)))
                with pytest.raises(MessageTooLarge, match=r"initial state of .* limits\.message of 4194304"):
                    a.system.ref(echo, key, initial=bytes(OVER))
                with pytest.raises(MessageTooLarge, match=r"limits\.message of 4194304"):
                    await client.ref(echo, key).ask(Echo, bytes(OVER))
                # Refused before it reached the wire, so the connection it would have broken still carries the rest.
                assert await ref.ask(Echo, b"after") == b"after"
                assert await client.ref(echo, key).ask(Echo, b"after") == b"after"

        @OWNERS
        async def it_fails_the_ask_whose_answer_is_over_4_mib_and_the_body_goes_on(owner: int) -> None:
            async with Harness.start(2) as harness:
                a = harness.nodes[0]
                key = await _key_on(harness.nodes[owner], a)
                client = await harness.client()

                with pytest.raises(MessageTooLarge, match=r"answer takes \d+ bytes .* limits\.message of 4194304"):
                    await a.system.ref(echo, key).ask(Grow, OVER)
                with pytest.raises(MessageTooLarge, match=r"answer takes \d+ bytes .* limits\.message of 4194304"):
                    await client.ref(echo, key).ask(Grow, OVER)
                assert await a.system.ref(echo, key).ask(Grow, 3) == bytes(3)
                assert await client.ref(echo, key).ask(Grow, 3) == bytes(3)

    def when_compression_sets_min_bytes() -> None:
        async def it_sends_uncompressed_every_frame_shorter_than_it() -> None:
            # Zeros: a frame that is compressed shrinks to almost nothing.
            payload = bytes(MIB)
            sent: dict[int, int] = {}

            # 2 MiB is above every frame, which the default limits keep at 256 KiB.
            for min_bytes in (4096, 2 * MIB):
                async with Harness.start(2, compression=Compression(min_bytes=min_bytes)) as harness:
                    a, b = harness.nodes
                    ref = a.system.ref(echo, await _key_on(b, a))
                    before = harness.forwarded

                    assert await ref.ask(Echo, payload) == payload
                    sent[min_bytes] = harness.forwarded - before

            # The payload crosses twice, there and back.
            assert sent[2 * MIB] > 2 * MIB
            assert sent[4096] < MIB // 4

    def when_a_field_name_leaves_no_room_in_a_message() -> None:
        async def it_raises_message_too_large_from_the_write_naming_the_field_and_the_limit() -> None:
            async with Harness.start(2, limits=NARROW) as harness:
                ref = harness.nodes[0].system.ref(narrow, "k")

                refused = await ref.ask(Widen)
                assert WIDE in refused
                assert "Limits.message" in refused
                # Nothing was written, so the key is still what it was.
                assert await ref.ask(Widen) == refused

    def when_a_client_has_other_limits_than_the_cluster() -> None:
        async def it_is_refused_naming_the_limits() -> None:
            async with Harness.start(1) as harness:
                client = Client(seeds=(harness.nodes[0].address,), limits=Limits(message=32 * MIB))

                with pytest.raises(Refused, match="limits"):
                    async with client:
                        pass


async def _key_on(node: Node, asked_from: Node, /) -> str:
    """The first of `k-0`, `k-1`, … whose owner is `node`, seen from `asked_from`."""
    for index in range(_TRIES):
        key = f"k-{index}"
        if await asked_from.system.ref(echo, key).ask(Locate) == node.system.node:
            return key
    raise AssertionError(f"none of the first {_TRIES} keys is owned by {node.address}")


_TRIES = 100
