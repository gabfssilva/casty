from __future__ import annotations

import typing
from collections.abc import AsyncIterator

import pytest

import casty
from casty.actors import registry
from casty.errors import SerializationSchemaError


@casty.actor(name="test.Counter")
class Counter:
    value: int = 0

    async def add(self, n: int) -> int:
        self.value += n
        return self.value

    async def read(self) -> int:
        return self.value

    def _helper(self) -> None: ...


def test_registration_collects_methods_and_state() -> None:
    info = registry.info_of(Counter)
    assert info is not None
    assert info.wire_name == "test.Counter"
    assert info.state_fields == ("value",)
    assert set(info.methods) == {"add", "read"}
    assert info.methods["add"].params == (("n", int),)
    assert info.methods["add"].returns is int


def test_state_roundtrip() -> None:
    info = registry.info_of(Counter)
    assert info is not None
    counter = Counter()
    counter.value = 7
    assert info.state_of(counter) == {"value": 7}
    fresh = Counter()
    info.restore_into(fresh, {"value": 7, "unknown": 1})
    assert fresh.value == 7


def test_slots_reject_undeclared_attributes() -> None:
    counter = Counter()
    with pytest.raises(AttributeError):
        counter.typo = 1  # type: ignore[attr-defined]


def test_transient_and_hooks() -> None:
    @casty.actor(name="test.WithHooks")
    class WithHooks:
        cursor: int = 0
        conn: object = casty.transient()

        @casty.activate
        async def _open(self) -> None:
            self.conn = object()

        @casty.deactivate
        async def _close(self) -> None:
            self.conn = None

        async def touch(self) -> None: ...

    info = registry.info_of(WithHooks)
    assert info is not None
    assert info.state_fields == ("cursor",)
    assert info.transient_fields == ("conn",)
    assert info.activate_hook == "_open"
    assert info.deactivate_hook == "_close"
    assert set(info.methods) == {"touch"}


def test_field_without_default_fails_at_import() -> None:
    with pytest.raises(SerializationSchemaError, match="need a default"):

        @casty.actor(name="test.NoDefault")
        class NoDefault:
            value: int

            async def get(self) -> int:
                return self.value


def test_sync_public_method_fails_at_import() -> None:
    with pytest.raises(SerializationSchemaError, match="must be async"):

        @casty.actor(name="test.SyncMethod")
        class SyncMethod:
            async def ok(self) -> None: ...

            def not_ok(self) -> None: ...


def test_unserializable_annotation_fails_at_import() -> None:
    class Opaque: ...

    with pytest.raises(SerializationSchemaError):

        @casty.actor(name="test.BadParam")
        class BadParam:
            async def take(self, x: Opaque) -> None: ...


def test_missing_annotation_fails_at_import() -> None:
    with pytest.raises(SerializationSchemaError, match="type annotation"):

        @casty.actor(name="test.NoAnnotation")
        class NoAnnotation:
            async def take(self, x) -> None: ...  # type: ignore[no-untyped-def]


def test_unserializable_state_field_fails_at_import() -> None:
    class Opaque: ...

    with pytest.raises(SerializationSchemaError):

        @casty.actor(name="test.BadState")
        class BadState:
            thing: Opaque | None = None

            async def get(self) -> None: ...


def test_duplicate_wire_name_fails() -> None:
    with pytest.raises(SerializationSchemaError, match="already registered"):

        @casty.actor(name="test.Counter")
        class Twin:
            async def get(self) -> None: ...


def test_streaming_methods_register_element_types() -> None:
    @casty.actor(name="test.Streams")
    class Streams:
        async def tail(self, since: int) -> AsyncIterator[bytes]:
            yield b""

        async def ingest(self, items: AsyncIterator[bytes]) -> int:
            return 0

        async def transform(self, xs: AsyncIterator[int], limit: int) -> AsyncIterator[str]:
            yield ""

    info = registry.info_of(Streams)
    assert info is not None
    tail = info.methods["tail"]
    assert tail.is_streaming
    assert tail.stream_out is bytes
    assert tail.stream_in is None
    assert tail.params == (("since", int),)

    ingest = info.methods["ingest"]
    assert ingest.is_streaming
    assert ingest.stream_in == ("items", bytes)
    assert ingest.stream_out is None
    assert ingest.params == ()

    transform = info.methods["transform"]
    assert transform.stream_in == ("xs", int)
    assert transform.stream_out is str
    assert transform.params == (("limit", int),)


def test_two_iterator_params_fail_at_import() -> None:
    with pytest.raises(SerializationSchemaError, match="at most one AsyncIterator"):

        @casty.actor(name="test.TwoIters")
        class TwoIters:
            async def zip(self, a: AsyncIterator[int], b: AsyncIterator[int]) -> None: ...


def test_iterator_return_without_async_generator_fails() -> None:
    with pytest.raises(SerializationSchemaError, match="not an async generator"):

        @casty.actor(name="test.FakeGen")
        class FakeGen:
            async def stream(self) -> AsyncIterator[int]:
                return  # type: ignore[return-value]  # no yield: a coroutine, not a generator


def test_async_generator_without_iterator_annotation_fails() -> None:
    with pytest.raises(SerializationSchemaError, match="async generator must be annotated"):

        @casty.actor(name="test.MisAnnotated")
        class MisAnnotated:
            async def stream(self) -> int:  # type: ignore[misc]
                yield 1


def test_unserializable_stream_element_fails_at_import() -> None:
    class Opaque: ...

    with pytest.raises(SerializationSchemaError):

        @casty.actor(name="test.BadStreamElem")
        class BadStreamElem:
            async def stream(self) -> AsyncIterator[Opaque]:
                yield Opaque()


def test_bare_async_iterator_fails_at_import() -> None:
    with pytest.raises(SerializationSchemaError):

        @casty.actor(name="test.BareIter")
        class BareIter:
            async def stream(self) -> AsyncIterator:  # type: ignore[type-arg]
                yield 1


@casty.actor(name="test.TypedStream")
class TypedStream:
    async def tail(self, since: int) -> AsyncIterator[bytes]:
        yield b""

    async def ingest(self, items: AsyncIterator[bytes]) -> int:
        return 0

    async def transform(self, xs: AsyncIterator[int]) -> AsyncIterator[str]:
        yield ""


async def _bytes_src() -> AsyncIterator[bytes]:
    yield b""


async def _int_src() -> AsyncIterator[int]:
    yield 0


async def _static_typing_check(system: casty.Node) -> None:
    """Never executed; mypy checks that proxies and maps preserve user types."""
    counter = system.actor(Counter, "k")
    typing.assert_type(counter, Counter)
    typing.assert_type(await counter.add(1), int)
    stock: casty.Map[str, int] = system.map("stock")
    typing.assert_type(await stock.get("sku"), int | None)

    stream = system.actor(TypedStream, "k")
    async for chunk in stream.tail(0):
        typing.assert_type(chunk, bytes)
    typing.assert_type(await stream.ingest(_bytes_src()), int)
    async for label in stream.transform(_int_src()):
        typing.assert_type(label, str)
