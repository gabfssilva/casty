import re
from collections import abc
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal
from enum import Enum, Flag, IntEnum, StrEnum, auto
from pathlib import Path, PurePosixPath, PureWindowsPath
from types import GenericAlias
from typing import TYPE_CHECKING, Annotated, assert_never

import pytest

from casty import ActorSystem, Askable, Context, NodeId, Opaque, Ref, SchemaError, actor
from tests import app
from tests.cluster import Harness, Node
from tests.support import eventually

if TYPE_CHECKING:
    from tests.app import Ledger


@dataclass
class Mutable:
    value: int


@dataclass(frozen=True)
class Line:
    tags: list[str]


@dataclass(frozen=True)
class Cart:
    lines: tuple[Line, ...]


@dataclass(frozen=True)
class Prices:
    by_name: dict[str, int]


class Plain:
    pass


@dataclass(frozen=True)
class Holder:
    value: object


@dataclass(frozen=True)
class Pending:
    pass


@dataclass(frozen=True)
class Journal:
    ledger: "Ledger"


@dataclass(frozen=True)
class Batch:
    amounts: list[int]


class Colour(Enum):
    RED = "red"
    GREEN = "green"


class Primary(Enum):
    """`Colour` as a version of it that no longer has `GREEN` reads it."""

    RED = "red"


class Hue(Enum):
    """`Colour` after `GREEN` was renamed, with the old name kept as an alias."""

    RED = "red"
    EMERALD = "green"
    GREEN = "green"


class Level(IntEnum):
    LOW = 1
    HIGH = 2


class Size(StrEnum):
    SMALL = "s"
    LARGE = "l"


class Permission(Flag):
    READ = auto()
    WRITE = auto()


@dataclass(frozen=True)
class Paint:
    colour: Colour


@dataclass(frozen=True)
class PrimaryPaint:
    colour: Primary


@dataclass(frozen=True)
class Grant:
    permissions: Permission


@dataclass(frozen=True)
class Palette:
    colour: Enum


@dataclass(frozen=True)
class Meeting:
    at: time


@dataclass(frozen=True)
class Appointment:
    starts: datetime


@dataclass(frozen=True)
class Plan:
    colour: Colour
    level: Level | None
    size: Size
    every: timedelta
    on: date
    at: time
    budget: Decimal


@dataclass(frozen=True)
class Labels:
    names: set[str]


def packed(numbers: list[int]) -> bytes:
    return bytes(numbers)


def unpacked(data: bytes) -> list[int]:
    return list(data)


def unchanged[T](value: T) -> T:
    return value


NUMBERS = Opaque(encode=packed, decode=unpacked)

# A `list`, which the schema refuses on its own.
type Numbers = Annotated[list[int], NUMBERS]
type Twice = Annotated[list[int], NUMBERS, NUMBERS]
# An `encode` that hands back what it is given, which is not bytes when the value is not what the annotation says.
type Unchecked = Annotated[bytes, Opaque[bytes](encode=unchanged, decode=unchanged)]


@dataclass(frozen=True)
class Sketch:
    strokes: Numbers
    source: PurePosixPath


@dataclass(frozen=True)
class Draw(Askable[bool]):
    stroke: int


@dataclass(frozen=True)
class Drawing:
    strokes: Numbers
    node: NodeId


@dataclass(frozen=True)
class Look(Askable[Drawing]):
    pass


type SketchMsg = Draw | Look


@actor(initial=Sketch([], PurePosixPath("sketches/untitled.svg")))
async def sketch(ctx: Context[Sketch, SketchMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Draw(stroke, reply_to=reply_to):
                await ctx.state.set(Sketch([*ctx.state.value.strokes, stroke], ctx.state.value.source))
                reply_to.tell(True)
            case Look(reply_to=reply_to):
                reply_to.tell(Drawing(ctx.state.value.strokes, ctx.system.node))
            case _:
                assert_never(msg)


async def mutable_state(ctx: Context[Mutable]) -> None:
    pass


async def list_in_a_dataclass_in_a_tuple(ctx: Context[Cart]) -> None:
    pass


async def dict_field(ctx: Context[Prices]) -> None:
    pass


async def plain_class(ctx: Context[Plain]) -> None:
    pass


async def object_field(ctx: Context[Holder]) -> None:
    pass


async def ambiguous_union(ctx: Context[Pending, tuple[int, ...] | frozenset[int]]) -> None:
    pass


async def homonymous_dataclasses(ctx: Context[Pending, app.Pending | Pending]) -> None:
    pass


async def unresolved_name(ctx: Context[Journal]) -> None:
    pass


async def invalid_message(ctx: Context[Pending, Batch]) -> None:
    pass


async def flag_field(ctx: Context[Grant]) -> None:
    pass


async def enum_without_members(ctx: Context[Palette]) -> None:
    pass


async def planner(ctx: Context[Plan, Plan]) -> None:
    pass


async def set_field(ctx: Context[Labels]) -> None:
    pass


UNSUPPORTED = [
    pytest.param(
        lambda: actor(mutable_state),
        "state: Mutable is not supported; use @dataclass(frozen=True)",
        id="mutable dataclass",
    ),
    pytest.param(
        lambda: actor(list_in_a_dataclass_in_a_tuple),
        "state: Cart.lines.tags: list[str] is not supported; use tuple[str, ...]",
        id="list in a dataclass in a tuple",
    ),
    pytest.param(
        lambda: actor(dict_field),
        "state: Prices.by_name: dict[str, int] is not supported; use Mapping[str, int]",
        id="dict",
    ),
    pytest.param(
        lambda: actor(plain_class),
        "state: Plain is not supported",
        id="plain class",
    ),
    pytest.param(
        lambda: actor(object_field),
        "state: Holder.value: object is not supported",
        id="object",
    ),
    pytest.param(
        lambda: actor(ambiguous_union),
        "message: tuple[int, ...] | frozenset[int] is ambiguous: tuple[int, ...] and frozenset[int] have the same "
        "structure",
        id="ambiguous union",
    ),
    pytest.param(
        lambda: actor(homonymous_dataclasses),
        "message: Pending | Pending is ambiguous: two dataclasses named Pending",
        id="homonymous dataclasses",
    ),
    pytest.param(
        lambda: actor(unresolved_name),
        "state: Journal: name 'Ledger' is not defined",
        id="unresolved name",
    ),
    pytest.param(
        lambda: actor()(invalid_message),
        "message: Batch.amounts: list[int] is not supported; use tuple[int, ...]",
        id="invalid message with a valid state",
    ),
    pytest.param(
        lambda: actor(flag_field),
        "state: Grant.permissions: Permission is not supported; use a frozenset of an Enum",
        id="flag",
    ),
    pytest.param(
        lambda: actor(enum_without_members),
        "state: Palette.colour: Enum is not supported",
        id="enum without members",
    ),
    pytest.param(
        lambda: actor(set_field),
        "state: Labels.names: set[str] is not supported; use frozenset[str]",
        id="set",
    ),
]


def encoded(annotation: object, value: object) -> bytes:
    system = ActorSystem()
    return system._encode(system._schema(annotation), value)  # pyright: ignore[reportPrivateUsage]


def decoded(annotation: object, data: bytes) -> object:
    system = ActorSystem()
    return system._decode(system._schema(annotation), data)  # pyright: ignore[reportPrivateUsage]


def round_trip(annotation: object, value: object) -> object:
    return decoded(annotation, encoded(annotation, value))


def describe_actor() -> None:
    def when_state_or_messages_are_not_serializable() -> None:
        @pytest.mark.parametrize(("define", "message"), UNSUPPORTED)
        def it_fails_naming_the_field(define: Callable[[], object], message: str) -> None:
            with pytest.raises(SchemaError, match=re.escape(message)):
                define()

    def it_takes_enums_dates_durations_and_decimals_as_state_and_messages() -> None:
        actor(planner)


def describe_enums() -> None:
    @pytest.mark.parametrize("member", [Colour.GREEN, Level.HIGH, Size.LARGE])
    def it_decodes_the_member_itself(member: Enum) -> None:
        assert round_trip(type(member), member) is member

    def it_writes_the_name_of_the_member_and_not_its_value() -> None:
        assert encoded(Colour, Colour.GREEN) == b"\xa5GREEN"

    def it_keeps_an_int_enum_apart_from_int_in_a_union() -> None:
        member = round_trip(int | Level, Level.HIGH)
        assert type(member) is Level
        assert member is Level.HIGH
        assert type(round_trip(int | Level, 2)) is int

    @pytest.mark.parametrize(
        ("annotation", "value", "message"),
        [
            pytest.param(Level, 2, "expected Level, got int", id="int for an IntEnum"),
            pytest.param(Size, "s", "expected Size, got str", id="str for a StrEnum"),
            pytest.param(Colour, Primary.RED, "expected Colour, got Primary", id="member of another enum"),
        ],
    )
    def it_refuses_a_value_that_is_not_a_member(annotation: object, value: object, message: str) -> None:
        with pytest.raises(SchemaError, match=re.escape(message)):
            encoded(annotation, value)

    def it_reads_a_renamed_member_by_the_alias_of_its_old_name() -> None:
        assert decoded(Hue, encoded(Colour, Colour.GREEN)) is Hue.EMERALD

    def it_refuses_a_member_the_enum_no_longer_has_naming_the_field() -> None:
        written = encoded(tuple[Paint], (Paint(Colour.GREEN),))
        with pytest.raises(SchemaError, match=re.escape("colour: Primary has no member GREEN")):
            decoded(tuple[PrimaryPaint], written)


def describe_dates_durations_and_decimals() -> None:
    @pytest.mark.parametrize(
        ("annotation", "value"),
        [
            pytest.param(timedelta, timedelta(days=-2, seconds=5, microseconds=7), id="negative timedelta"),
            pytest.param(timedelta, timedelta(weeks=5000, microseconds=1), id="long timedelta"),
            pytest.param(date, date(1969, 7, 20), id="date before the epoch"),
            pytest.param(date, date(9999, 12, 31), id="last date"),
            pytest.param(time, time(23, 59, 59, 999_999, tzinfo=timezone(timedelta(hours=-3))), id="time"),
            pytest.param(time, time(0, 0, tzinfo=UTC), id="midnight in UTC"),
            pytest.param(Decimal, Decimal("-1234.5600"), id="decimal with trailing zeros"),
            pytest.param(Decimal, Decimal("1E+30"), id="decimal with an exponent"),
            pytest.param(Decimal, Decimal("-Infinity"), id="infinite decimal"),
            pytest.param(Decimal, Decimal("NaN"), id="decimal that is not a number"),
        ],
    )
    def it_round_trips_value_and_type(annotation: object, value: object) -> None:
        assert repr(round_trip(annotation, value)) == repr(value)

    @pytest.mark.parametrize(
        ("annotation", "value", "message"),
        [
            pytest.param(timedelta, 5, "expected timedelta, got int", id="int for a timedelta"),
            pytest.param(
                timedelta,
                timedelta.max,
                "timedelta does not fit in 64 bits of microseconds: 999999999 days, 23:59:59.999999",
                id="timedelta past 64 bits",
            ),
            pytest.param(
                date, datetime(2026, 9, 22, tzinfo=UTC), "expected date, got datetime", id="datetime for a date"
            ),
            pytest.param(time, "09:00", "expected time, got str", id="str for a time"),
            pytest.param(Decimal, 1.5, "expected Decimal, got float", id="float for a Decimal"),
            pytest.param(Meeting, Meeting(time(9)), "at: time without time zone: 09:00:00", id="naive time"),
            pytest.param(
                Appointment,
                Appointment(datetime(2026, 9, 22, 9)),
                "starts: datetime without time zone: 2026-09-22 09:00:00",
                id="naive datetime",
            ),
        ],
    )
    def it_refuses_a_value_of_another_type(annotation: object, value: object, message: str) -> None:
        with pytest.raises(SchemaError, match=re.escape(message)):
            encoded(annotation, value)

    @pytest.mark.parametrize(
        ("written", "value", "read", "message"),
        [
            pytest.param(str, "twelve", Decimal, "expected Decimal, got str", id="str that is not a number"),
            pytest.param(float, 1.5, timedelta, "expected timedelta, got float", id="float for a timedelta"),
            pytest.param(str, "2026-09-22", date, "expected date, got str", id="str for a date"),
            pytest.param(int, 9, time, "expected time, got int", id="int for a time"),
        ],
    )
    def it_refuses_a_payload_of_another_type(written: object, value: object, read: object, message: str) -> None:
        with pytest.raises(SchemaError, match=re.escape(message)):
            decoded(read, encoded(written, value))


def describe_paths() -> None:
    @pytest.mark.parametrize(
        ("annotation", "value"),
        [
            pytest.param(PurePosixPath, PurePosixPath("/srv/data/raw.csv"), id="posix"),
            pytest.param(PureWindowsPath, PureWindowsPath("C:/Users/ada/notes.txt"), id="windows"),
            pytest.param(Path, Path("data/raw.csv"), id="of this machine"),
        ],
    )
    def it_round_trips_value_and_type(annotation: object, value: object) -> None:
        assert repr(round_trip(annotation, value)) == repr(value)

    def it_writes_the_string_of_the_path() -> None:
        assert encoded(PureWindowsPath, PureWindowsPath("C:/tmp")) == b"\xa6C:\\tmp"

    def it_reads_the_class_it_was_annotated_with_whatever_class_wrote_it() -> None:
        assert decoded(PurePosixPath, encoded(PureWindowsPath, PureWindowsPath("C:/tmp"))) == PurePosixPath("C:\\tmp")

    @pytest.mark.parametrize(
        ("annotation", "value", "message"),
        [
            pytest.param(Path, "data/raw.csv", "expected Path, got str", id="str for a Path"),
            pytest.param(Path, PurePosixPath("data"), "expected Path, got PurePosixPath", id="pure path for a Path"),
            pytest.param(
                PurePosixPath,
                PureWindowsPath("C:/tmp"),
                "expected PurePosixPath, got PureWindowsPath",
                id="path of the other flavour",
            ),
            pytest.param(
                Path | str, "data", "Path | str is ambiguous: Path and str have the same structure", id="Path | str"
            ),
        ],
    )
    def it_refuses_a_value_of_another_class(annotation: object, value: object, message: str) -> None:
        with pytest.raises(SchemaError, match=re.escape(message)):
            encoded(annotation, value)

    def it_refuses_a_payload_that_is_not_a_string() -> None:
        with pytest.raises(SchemaError, match=re.escape("expected Path, got int")):
            decoded(Path, encoded(int, 3))


def describe_sets() -> None:
    def it_takes_a_frozenset_which_is_the_immutable_set() -> None:
        assert round_trip(frozenset[str], frozenset({"a", "b"})) == frozenset({"a", "b"})


def describe_opaque_values() -> None:
    def it_round_trips_a_value_the_schema_refuses() -> None:
        numbers = round_trip(Numbers, [3, 1, 2])
        assert type(numbers) is list
        assert numbers == [3, 1, 2]

    def it_writes_the_bytes_encode_returns() -> None:
        assert encoded(Numbers, [1, 2]) == b"\xc4\x02\x01\x02"

    def it_reads_an_optional_one_by_what_arrives() -> None:
        assert round_trip(Numbers | None, [7]) == [7]
        assert round_trip(Numbers | None, None) is None

    def it_goes_inside_a_dataclass_inside_a_container() -> None:
        drawn = (Sketch([1, 2], PurePosixPath("a.svg")), Sketch([], PurePosixPath("b.svg")))
        assert round_trip(tuple[Sketch, ...], drawn) == drawn

    def it_refuses_what_encode_returns_when_it_is_not_bytes() -> None:
        with pytest.raises(SchemaError, match=re.escape("the encode of bytes returned str, not bytes")):
            encoded(Unchecked, "text")

    def it_refuses_a_payload_that_is_not_bytes() -> None:
        with pytest.raises(SchemaError, match=re.escape("expected list[int], got str")):
            decoded(Numbers, encoded(str, "one"))

    @pytest.mark.parametrize(
        ("annotation", "message"),
        [
            pytest.param(
                bytes | Numbers,
                "bytes | Numbers is ambiguous: bytes and Numbers have the same structure",
                id="union with bytes",
            ),
            pytest.param(Twice, "list[int] has more than one Opaque", id="two pairs of functions"),
        ],
    )
    def it_refuses_an_annotation_it_cannot_write(annotation: object, message: str) -> None:
        with pytest.raises(SchemaError, match=re.escape(message)):
            encoded(annotation, b"")

    def when_a_state_holds_one() -> None:
        async def it_replicates_the_page_and_another_node_reads_it_back() -> None:
            async with Harness.start(3) as harness:
                a, b, _ = harness.nodes
                key = await _drawn_on(b, a)
                gone = b.system.node
                for stroke in (3, 1, 2):
                    assert await a.system.ref(sketch, key).ask(Draw(stroke))

                harness.isolate(b)
                await harness.crash(b)

                async def another_node_answers_with_every_stroke() -> None:
                    drawing = await a.system.ref(sketch, key).ask(Look())
                    assert drawing.strokes == [3, 1, 2]
                    assert drawing.node != gone

                await eventually(another_node_answers_with_every_stroke, timedelta(seconds=10))


def describe_malformed_input() -> None:
    @pytest.mark.parametrize(
        ("annotation", "message"),
        [
            pytest.param(GenericAlias(frozenset, ()), "frozenset[] is not supported", id="frozenset of nothing"),
            pytest.param(GenericAlias(abc.Mapping, (int,)), "Mapping[int] is not supported", id="Mapping of a key"),
            pytest.param(GenericAlias(Ref, ()), "Ref[] is not supported", id="Ref of nothing"),
        ],
    )
    def it_refuses_a_container_without_the_arguments_it_takes(annotation: object, message: str) -> None:
        with pytest.raises(SchemaError, match=re.escape(message)):
            encoded(annotation, None)

    def it_refuses_a_count_the_payload_cannot_hold_before_reading_it() -> None:
        # Five bytes that claim four billion items.
        with pytest.raises(SchemaError, match=re.escape("the payload ends in the middle of a value")):
            decoded(tuple[int, ...], b"\xdd\xff\xff\xff\xff")


async def _drawn_on(owner: Node, asked_from: Node) -> str:
    """The first of `s-0`, `s-1`, … whose owner is `owner`, seen from `asked_from`."""
    for index in range(100):
        key = f"s-{index}"
        drawing = await asked_from.system.ref(sketch, key).ask(Look())
        if drawing.node == owner.system.node:
            return key
    raise AssertionError(f"none of the first 100 keys is owned by {owner.address}")
