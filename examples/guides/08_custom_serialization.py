"""Custom Serialization — implement the Serializer protocol with cloudpickle.

Casty's ``Serializer`` is a Protocol, not an ABC. Any object with
``serialize``, ``deserialize``, and ``set_ref_factory`` methods satisfies it.

This example uses cloudpickle to serialize lambdas and closures that
standard pickle cannot handle.

    pip install cloudpickle
"""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import cloudpickle

from casty import (
    ActorAddress,
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    JsonSerializer,
    PickleSerializer,
    TypeRegistry,
)


# ── The Serializer protocol (for reference) ──────────────────────────
#
#   class Serializer(Protocol):
#       def serialize(self, obj: Any) -> bytes: ...
#       def deserialize(self, data: bytes) -> Any: ...
#       def set_ref_factory(self, factory: Callable[[ActorAddress], Any]) -> None: ...


# ── CloudpickleSerializer ────────────────────────────────────────────


class CloudpickleSerializer:
    """Serializer that uses cloudpickle for lambdas and closures."""

    def __init__(self) -> None:
        self._ref_factory: Callable[[ActorAddress], Any] | None = None

    def serialize(self, obj: Any) -> bytes:
        return cloudpickle.dumps(obj)

    def deserialize(self, data: bytes) -> Any:
        return cloudpickle.loads(data)  # noqa: S301

    def set_ref_factory(
        self, factory: Callable[[ActorAddress], Any]
    ) -> None:
        self._ref_factory = factory


# ── Messages ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Greet:
    name: str
    reply_to: ActorRef[str]


@dataclass(frozen=True)
class ApplyFn:
    fn: Callable[[int], int]


type DemoMsg = Greet | ApplyFn


# ── Main ─────────────────────────────────────────────────────────────


def make_ref_factory(addr: ActorAddress) -> ActorRef[Any]:
    """Stub ref factory for demonstration — real clusters use RemoteTransport."""
    return ActorRef(address=addr, _transport=None)  # type: ignore[arg-type]


async def main() -> None:
    # 1. Roundtrip with JsonSerializer
    print("── JsonSerializer ──")
    registry = TypeRegistry()
    registry.register(Greet)
    json_ser = JsonSerializer(registry, ref_factory=make_ref_factory)

    original = Greet(
        name="Alice",
        reply_to=ActorRef(
            address=ActorAddress.from_uri("casty://demo/greeter"),
            _transport=None,  # type: ignore[arg-type]
        ),
    )
    data = json_ser.serialize(original)
    print(f"  JSON bytes: {data.decode()}")
    restored = json_ser.deserialize(data)
    print(f"  Restored:   {restored}")
    print(f"  Match:      {original.name == restored.name}")

    # 2. Roundtrip with PickleSerializer
    print("\n── PickleSerializer ──")
    pickle_ser = PickleSerializer()
    pickle_ser.set_ref_factory(make_ref_factory)

    data = pickle_ser.serialize(original)
    print(f"  Pickle bytes: {len(data)} bytes")
    restored = pickle_ser.deserialize(data)
    print(f"  Restored:     {restored}")
    print(f"  Match:        {original.name == restored.name}")

    # 3. CloudpickleSerializer — handles lambdas!
    print("\n── CloudpickleSerializer ──")
    cloud_ser = CloudpickleSerializer()

    multiplier = 3
    fn = lambda x: x * multiplier  # noqa: E731

    data = cloud_ser.serialize(fn)
    print(f"  Serialized lambda: {len(data)} bytes")
    restored_fn = cloud_ser.deserialize(data)
    print(f"  restored_fn(10) = {restored_fn(10)}")
    print(f"  Captures closure variable multiplier={multiplier}")

    # 4. Lambdas in messages
    print("\n── Lambda messages with cloudpickle ──")
    msg = ApplyFn(fn=lambda x: x + 42)
    data = cloud_ser.serialize(msg)
    restored_msg = cloud_ser.deserialize(data)
    print(f"  Original:  ApplyFn(fn=<lambda>)")
    print(f"  Roundtrip: fn(0) = {restored_msg.fn(0)}")

    # 5. Standard pickle CANNOT do this
    print("\n── Standard pickle vs cloudpickle ──")
    import pickle

    try:
        pickle.dumps(lambda x: x + 1)
        print("  pickle: OK (unexpected)")
    except Exception as e:
        print(f"  pickle: {type(e).__name__} — {e}")

    cloudpickle.dumps(lambda x: x + 1)
    print("  cloudpickle: OK")

    # 6. Prove it works in a real actor
    print("\n── Actor using cloudpickle-serialized functions ──")

    def accumulator(value: int = 0) -> Behavior[ApplyFn]:
        async def receive(
            ctx: ActorContext[ApplyFn], msg: ApplyFn
        ) -> Behavior[ApplyFn]:
            new_value = msg.fn(value)
            print(f"  {value} → {new_value}")
            return accumulator(new_value)

        return Behaviors.receive(receive)

    async with ActorSystem() as system:
        ref: ActorRef[ApplyFn] = system.spawn(accumulator(), "calc")

        # Functions survive serialization roundtrip, then get applied
        for fn in [lambda x: x + 10, lambda x: x * 3, lambda x: x - 5]:
            data = cloud_ser.serialize(ApplyFn(fn=fn))
            restored_msg = cloud_ser.deserialize(data)
            ref.tell(restored_msg)

        await asyncio.sleep(0.1)


asyncio.run(main())
