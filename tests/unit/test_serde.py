from __future__ import annotations

import collections
import dataclasses
import datetime
import enum
import typing
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


@dataclasses.dataclass
class PlainInner:
    label: str
    n: int = 0


@casty.message(name="test.HasPlain")
class HasPlain:
    inner: PlainInner
    inners: list[PlainInner] = dataclasses.field(default_factory=list)
    maybe: PlainInner | None = None


def test_plain_dataclass_field_auto_registers() -> None:
    # a bare @dataclass used as a @message field is registered transitively at
    # import time under module.QualName — no @casty.message needed on it.
    assert registry.wire_name_of(PlainInner) is not None
    original = HasPlain(
        inner=PlainInner(label="a", n=3),
        inners=[PlainInner(label="b"), PlainInner(label="c", n=9)],
        maybe=PlainInner(label="d"),
    )
    assert roundtrip(original) == original


@dataclasses.dataclass
class PlainParam:
    sku: str
    qty: int = 1


def test_plain_dataclass_actor_param_auto_registers() -> None:
    @casty.actor(name="test.ActorPlainParam")
    class ActorPlainParam:
        seen: int = 0

        async def take(self, item: PlainParam) -> int:
            self.seen += item.qty
            return self.seen

    assert registry.wire_name_of(PlainParam) is not None
    encoded = codec.encode_raw(PlainParam(sku="x", qty=2))
    assert codec.decode_raw(encoded, PlainParam) == PlainParam(sku="x", qty=2)


class Vec(typing.NamedTuple):
    x: int
    y: int
    label: str = ""


@casty.message(name="test.HasVec")
class HasVec:
    origin: Vec
    path: list[Vec] = dataclasses.field(default_factory=list)
    either: Vec | Point = Vec(x=0, y=0)


def test_namedtuple_roundtrips_as_record() -> None:
    # a typing.NamedTuple is a record with identity: it travels as
    # [wire_name, {fields}], defaults fill in, and it disambiguates a union.
    assert registry.wire_name_of(Vec) is not None
    original = HasVec(
        origin=Vec(x=1, y=2, label="o"),
        path=[Vec(x=3, y=4), Vec(x=5, y=6, label="p")],
        either=Vec(x=7, y=8),
    )
    decoded = roundtrip(original)
    assert decoded == original
    assert isinstance(decoded, HasVec)
    assert isinstance(decoded.either, Vec)  # union resolved to the embedded wire name


def test_namedtuple_default_is_applied_on_decode() -> None:
    import msgpack

    # 'label' omitted on the wire -> the NamedTuple default fills it
    vec = ["tests.unit.test_serde.Vec", {"x": 1, "y": 2}]
    raw = msgpack.packb(["test.HasVec", {"origin": vec}])
    decoded = codec.decode(raw)
    assert isinstance(decoded, HasVec)
    assert decoded.origin == Vec(x=1, y=2, label="")


UntypedNT = collections.namedtuple("UntypedNT", ["a", "b"])  # deliberately untyped


def test_untyped_namedtuple_is_rejected() -> None:
    with pytest.raises(SerializationSchemaError, match="not serializable"):

        @casty.message(name="test.HasUntyped")
        class HasUntyped:
            thing: UntypedNT


class OpaqueClass:  # not a dataclass, module scope so the annotation resolves
    pass


def test_non_dataclass_field_still_fails() -> None:
    with pytest.raises(SerializationSchemaError, match="not serializable"):

        @casty.message(name="test.StillBad")
        class StillBad:
            thing: OpaqueClass
