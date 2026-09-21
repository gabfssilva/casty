import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from casty import Context, SchemaError, actor
from tests import app

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
]


def describe_actor() -> None:
    def when_state_or_messages_are_not_serializable() -> None:
        @pytest.mark.parametrize(("define", "message"), UNSUPPORTED)
        def it_fails_naming_the_field(define: Callable[[], object], message: str) -> None:
            with pytest.raises(SchemaError, match=re.escape(message)):
                define()
