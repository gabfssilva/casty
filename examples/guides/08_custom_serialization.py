"""Custom Serialization — implement the Serializer protocol with cloudpickle.

Casty's ``Serializer`` is a Protocol, not an ABC. Any object with
``serialize`` and ``deserialize`` methods satisfies it.

This example uses cloudpickle to serialize lambdas and closures that
standard pickle cannot handle.

    pip install cloudpickle
"""

import asyncio
import pickle
from collections.abc import Callable
from dataclasses import dataclass

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
#       def deserialize(self, data: bytes, *, ref_factory=None) -> Any: ...


# ── CloudpickleSerializer ────────────────────────────────────────────


class CloudpickleSerializer:
    """Serializer that uses cloudpickle for lambdas and closures."""

    def serialize[M](self, obj: M) -> bytes:
        return cloudpickle.dumps(obj)

    def deserialize[M, R](
        self,
        data: bytes,
        *,
        ref_factory: Callable[[ActorAddress], ActorRef[R]] | None = None,
    ) -> M:
        return cloudpickle.loads(data)  # noqa: S301


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


# ── Demonstrations ───────────────────────────────────────────────────


def pickle_vs_cloudpickle() -> None:
    print("── Standard pickle vs cloudpickle ──")
    try:
        pickle.dumps(lambda x: x + 1)
        print("  pickle:      OK (unexpected)")
    except Exception as e:
        print(f"  pickle:      {type(e).__name__}")

    cloudpickle.dumps(lambda x: x + 1)
    print("  cloudpickle: OK")


def roundtrip_closure(serializer: CloudpickleSerializer) -> None:
    print("\n── Roundtrip a closure ──")
    multiplier = 3
    fn: Callable[[int], int] = lambda x: x * multiplier  # noqa: E731

    data = serializer.serialize(fn)
    restored_fn = serializer.deserialize(data)
    print(f"  restored_fn(10) = {restored_fn(10)}")


def roundtrip_message(serializer: CloudpickleSerializer) -> None:
    print("\n── Roundtrip a message ──")
    msg = ApplyFn(fn=lambda x: x + 42)
    data = serializer.serialize(msg)
    restored_msg = serializer.deserialize(data)
    print(f"  fn(0) = {restored_msg.fn(0)}")


async def actor_with_serialized_fns(serializer: CloudpickleSerializer) -> None:
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


# ── Main ─────────────────────────────────────────────────────────────


async def main() -> None:
    serializer = CloudpickleSerializer()
    pickle_vs_cloudpickle()
    roundtrip_closure(serializer)
    roundtrip_message(serializer)
    await actor_with_serialized_fns(serializer)


asyncio.run(main())
