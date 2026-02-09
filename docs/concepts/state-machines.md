# State Machines

Because behaviors are values and state transitions are function calls, finite state machines emerge as a natural pattern. Each state is a behavior function, each transition is a return value. No enum, no conditional dispatch on a status field — the behavior **is** the state.

```python
import asyncio
from dataclasses import dataclass
from casty import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors

@dataclass(frozen=True)
class Item:
    name: str
    price: float

@dataclass(frozen=True)
class Receipt:
    items: tuple[Item, ...]
    total: float

@dataclass(frozen=True)
class AddItem:
    item: Item

@dataclass(frozen=True)
class Checkout:
    reply_to: ActorRef[Receipt]

@dataclass(frozen=True)
class GetTotal:
    reply_to: ActorRef[float]

type CartMsg = AddItem | Checkout | GetTotal

def empty_cart() -> Behavior[CartMsg]:
    async def receive(ctx: ActorContext[CartMsg], msg: CartMsg) -> Behavior[CartMsg]:
        match msg:
            case AddItem(item):
                return active_cart({item.name: item})
            case Checkout():
                return Behaviors.same()  # cannot checkout empty cart
            case GetTotal(reply_to):
                reply_to.tell(0.0)
                return Behaviors.same()

    return Behaviors.receive(receive)

def active_cart(items: dict[str, Item]) -> Behavior[CartMsg]:
    async def receive(ctx: ActorContext[CartMsg], msg: CartMsg) -> Behavior[CartMsg]:
        match msg:
            case AddItem(item):
                return active_cart({**items, item.name: item})
            case Checkout(reply_to):
                all_items = tuple(items.values())
                total = sum(i.price for i in all_items)
                reply_to.tell(Receipt(items=all_items, total=total))
                return checked_out()
            case GetTotal(reply_to):
                reply_to.tell(sum(i.price for i in items.values()))
                return Behaviors.same()

    return Behaviors.receive(receive)

def checked_out() -> Behavior[CartMsg]:
    async def receive(ctx: ActorContext[CartMsg], msg: CartMsg) -> Behavior[CartMsg]:
        match msg:
            case GetTotal(reply_to):
                reply_to.tell(0.0)
                return Behaviors.same()
            case _:
                return Behaviors.same()  # terminal state, ignore modifications

    return Behaviors.receive(receive)

async def main() -> None:
    async with ActorSystem() as system:
        cart = system.spawn(empty_cart(), "cart")

        cart.tell(AddItem(Item("keyboard", 75.0)))
        cart.tell(AddItem(Item("mouse", 25.0)))
        await asyncio.sleep(0.1)

        total = await system.ask(cart, lambda r: GetTotal(reply_to=r), timeout=5.0)
        print(f"Total: ${total:.2f}")  # Total: $100.00

        receipt = await system.ask(cart, lambda r: Checkout(reply_to=r), timeout=5.0)
        print(f"Receipt: {len(receipt.items)} items, ${receipt.total:.2f}")
        # Receipt: 2 items, $100.00

        # Further modifications are ignored — checked_out is a terminal state
        cart.tell(AddItem(Item("monitor", 300.0)))

asyncio.run(main())
```

Three functions, three states: `empty_cart`, `active_cart`, `checked_out`. The transitions are explicit in the return values — `empty_cart` transitions to `active_cart` on the first `AddItem`, `active_cart` transitions to `checked_out` on `Checkout`. A `checked_out` actor ignores further modifications. The type checker ensures every state handles the full `CartMsg` union.

---

**Next:** [Actor Runtime](actor-runtime.md)
