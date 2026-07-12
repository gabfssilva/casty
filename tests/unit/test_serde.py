from __future__ import annotations

import datetime
import enum
import uuid

import pytest

import casty
from casty.errors import SerializationError, SerializationSchemaError
from casty.serde import codec, registry


class Color(enum.Enum):
    RED = "red"
    BLUE = "blue"


@casty.message(name="test.Point")
class Point:
    x: int
    y: int


@casty.message(name="test.Shape")
class Shape:
    name: str
    origin: Point
    color: Color = Color.RED
    tags: set[str] | None = None


@casty.message(name="test.Everything")
class Everything:
    ints: list[int]
    mapping: dict[str, Point]
    pair: tuple[int, str]
    variadic: tuple[int, ...]
    frozen: frozenset[int]
    maybe: int | None
    either: Point | str
    stamp: datetime.datetime
    ident: uuid.UUID
    blob: bytes
    ratio: float


def roundtrip(obj: object) -> object:
    return codec.decode(codec.encode(obj))


def test_simple_roundtrip() -> None:
    assert roundtrip(Point(x=1, y=-2)) == Point(x=1, y=-2)


def test_nested_and_collections_roundtrip() -> None:
    original = Everything(
        ints=[1, 2, 3],
        mapping={"a": Point(x=0, y=0)},
        pair=(7, "seven"),
        variadic=(1, 2, 3, 4),
        frozen=frozenset({9, 10}),
        maybe=None,
        either=Point(x=5, y=5),
        stamp=datetime.datetime(2026, 7, 10, 12, 0, tzinfo=datetime.UTC),
        ident=uuid.uuid4(),
        blob=b"\x00\x01",
        ratio=2.5,
    )
    assert roundtrip(original) == original


def test_union_prefers_embedded_wire_name() -> None:
    assert roundtrip(Shape(name="s", origin=Point(x=1, y=1), color=Color.BLUE)) == Shape(
        name="s", origin=Point(x=1, y=1), color=Color.BLUE
    )
    either_str = Everything(
        ints=[],
        mapping={},
        pair=(0, ""),
        variadic=(),
        frozen=frozenset(),
        maybe=1,
        either="just a string",
        stamp=datetime.datetime.now(datetime.UTC),
        ident=uuid.uuid4(),
        blob=b"",
        ratio=0.0,
    )
    decoded = roundtrip(either_str)
    assert isinstance(decoded, Everything)
    assert decoded.either == "just a string"


def test_slots_generated() -> None:
    point = Point(x=1, y=2)
    with pytest.raises(AttributeError):
        point.z = 3  # type: ignore[attr-defined]


def test_unknown_field_ignored_forward_compat() -> None:
    import msgpack

    raw = msgpack.packb(["test.Point", {"x": 1, "y": 2, "added_in_v3": True}])
    assert codec.decode(raw) == Point(x=1, y=2)


def test_missing_field_with_default_uses_default() -> None:
    import msgpack

    raw = msgpack.packb(["test.Shape", {"name": "s", "origin": ["test.Point", {"x": 0, "y": 0}]}])
    decoded = codec.decode(raw)
    assert isinstance(decoded, Shape)
    assert decoded.color is Color.RED and decoded.tags is None


def test_missing_field_without_default_fails() -> None:
    import msgpack

    raw = msgpack.packb(["test.Point", {"x": 1}])
    with pytest.raises(SerializationError, match=r"test\.Point\.y"):
        codec.decode(raw)


def test_unknown_wire_name_fails() -> None:
    import msgpack

    raw = msgpack.packb(["test.DoesNotExist", {}])
    with pytest.raises(SerializationError, match="unknown wire name"):
        codec.decode(raw)


def test_duplicate_wire_name_fails() -> None:
    with pytest.raises(SerializationSchemaError, match="already registered"):

        @casty.message(name="test.Point")
        class AnotherPoint:
            x: int


def test_illegal_field_type_fails_at_import_with_path() -> None:
    class NotSerializable:
        pass

    with pytest.raises(SerializationSchemaError, match=r"test\.Bad\.thing"):

        @casty.message(name="test.Bad")
        class Bad:
            thing: NotSerializable

    assert registry.lookup_by_name("test.Bad") is None  # rollback happened


def test_self_referential_message() -> None:
    @casty.message(name="test.TreeNode")
    class TreeNode:
        value: int
        left: TreeNode | None = None
        right: TreeNode | None = None

    tree = TreeNode(value=1, left=TreeNode(value=2), right=None)
    assert roundtrip(tree) == tree


def test_type_mismatch_fails() -> None:
    import msgpack

    raw = msgpack.packb(["test.Point", {"x": "not an int", "y": 2}])
    with pytest.raises(SerializationError, match=r"test\.Point\.x"):
        codec.decode(raw)


def test_encoding_unregistered_fails() -> None:
    class Plain:
        pass

    with pytest.raises(SerializationError, match="not a registered"):
        codec.encode(Plain())


def test_no_pickle_anywhere() -> None:
    import inspect

    import casty.serde.codec as codec_module

    assert "pickle" not in inspect.getsource(codec_module)
