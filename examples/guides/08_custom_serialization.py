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
class ApplyFn:
    fn: Callable[[int], int]


@dataclass(frozen=True)
class GetValue:
    reply_to: ActorRef[int]


type ComputeMsg = ApplyFn | GetValue


# ── Behavior ─────────────────────────────────────────────────────────


def accumulator(value: int = 0) -> Behavior[ComputeMsg]:
    async def receive(
        ctx: ActorContext[ComputeMsg], msg: ComputeMsg
    ) -> Behavior[ComputeMsg]:
        match msg:
            case ApplyFn(fn):
                new_value = fn(value)
                print(f"  {value} → {new_value}")
                return accumulator(new_value)
            case GetValue(reply_to):
                reply_to.tell(value)
                return Behaviors.same()

    return Behaviors.receive(receive)


# ── Main ─────────────────────────────────────────────────────────────


async def main() -> None:
    serializer = CloudpickleSerializer()

    # Prove standard pickle can't do this
    print("── Standard pickle vs cloudpickle ──")
    import pickle

    try:
        pickle.dumps(lambda x: x + 1)
        print("  pickle:      OK (unexpected)")
    except Exception as e:
        print(f"  pickle:      {type(e).__name__}")

    cloudpickle.dumps(lambda x: x + 1)
    print("  cloudpickle: OK")

    # Roundtrip a lambda that captures a local variable
    print("\n── Roundtrip a closure ──")
    multiplier = 3
    fn: Callable[[int], int] = lambda x: x * multiplier  # noqa: E731

    data = serializer.serialize(fn)
    restored_fn = serializer.deserialize(data)
    print(f"  restored_fn(10) = {restored_fn(10)}")

    # Roundtrip a message carrying a lambda
    print("\n── Roundtrip a message ──")
    msg = ApplyFn(fn=lambda x: x + 42)
    data = serializer.serialize(msg)
    restored_msg = serializer.deserialize(data)
    print(f"  fn(0) = {restored_msg.fn(0)}")

    # Use it in a real actor
    print("\n── Actor applying serialized functions ──")
    async with ActorSystem() as system:
        ref: ActorRef[ComputeMsg] = system.spawn(accumulator(), "calc")

        fns: list[Callable[[int], int]] = [
            lambda x: x + 10,
            lambda x: x * 3,
            lambda x: x - 5,
        ]
        for fn in fns:
            data = serializer.serialize(ApplyFn(fn=fn))
            ref.tell(serializer.deserialize(data))

        await asyncio.sleep(0.1)

        value: int = await system.ask(
            ref, lambda r: GetValue(reply_to=r), timeout=5.0
        )
        print(f"  Final value: {value}")


asyncio.run(main())
