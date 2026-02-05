"""Shopping Cart — state machine via behavior transitions.

Each cart state is a separate function returning a different behavior.
Instead of if/else on a status field, the behavior itself encodes the state.

States:
  empty_cart()         -> accepts AddItem, rejects Checkout
  active_cart(items)   -> accepts AddItem, RemoveItem, Checkout, GetTotal
  checked_out(receipt) -> terminal, only responds to GetTotal
"""

import asyncio
from dataclasses import dataclass

from casty import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors


# --- Data ---


@dataclass(frozen=True)
class Item:
    name: str
    price: float
    quantity: int = 1


@dataclass(frozen=True)
class Receipt:
    items: tuple[Item, ...]
    total: float


# --- Messages ---


@dataclass(frozen=True)
class AddItem:
    item: Item


@dataclass(frozen=True)
class RemoveItem:
    name: str


@dataclass(frozen=True)
class Checkout:
    reply_to: ActorRef[Receipt]


@dataclass(frozen=True)
class GetTotal:
    reply_to: ActorRef[float]


type CartMsg = AddItem | RemoveItem | Checkout | GetTotal


# --- Behaviors (one per state) ---


def empty_cart() -> Behavior[CartMsg]:
    """Empty cart — waiting for the first item."""

    async def receive(ctx: ActorContext[CartMsg], msg: CartMsg) -> Behavior[CartMsg]:
        match msg:
            case AddItem(item):
                print(f"  [cart] added {item.name} (${item.price:.2f})")
                return active_cart({item.name: item})
            case RemoveItem():
                print("  [cart] nothing to remove, cart is empty")
                return Behaviors.same()
            case Checkout():
                print("  [cart] cannot checkout an empty cart")
                return Behaviors.same()
            case GetTotal(reply_to):
                reply_to.tell(0.0)
                return Behaviors.same()

    return Behaviors.receive(receive)


def active_cart(items: dict[str, Item]) -> Behavior[CartMsg]:
    """Active cart — has items, accepts modifications and checkout."""

    async def receive(ctx: ActorContext[CartMsg], msg: CartMsg) -> Behavior[CartMsg]:
        match msg:
            case AddItem(item):
                if item.name in items:
                    existing = items[item.name]
                    new_qty = existing.quantity + item.quantity
                    updated = Item(existing.name, existing.price, new_qty)
                    new_items = {**items, item.name: updated}
                    print(f"  [cart] {item.name} x{new_qty} (was x{existing.quantity})")
                else:
                    new_items = {**items, item.name: item}
                    print(f"  [cart] added {item.name} (${item.price:.2f})")
                return active_cart(new_items)

            case RemoveItem(name):
                if name not in items:
                    print(f"  [cart] {name} not in cart")
                    return Behaviors.same()
                new_items = {k: v for k, v in items.items() if k != name}
                print(f"  [cart] removed {name}")
                if not new_items:
                    print("  [cart] cart is now empty")
                    return empty_cart()
                return active_cart(new_items)

            case Checkout(reply_to):
                all_items = tuple(items.values())
                total = sum(i.price * i.quantity for i in all_items)
                receipt = Receipt(items=all_items, total=total)
                print(f"  [cart] checked out! total: ${total:.2f}")
                reply_to.tell(receipt)
                return checked_out(receipt)

            case GetTotal(reply_to):
                total = sum(i.price * i.quantity for i in items.values())
                reply_to.tell(total)
                return Behaviors.same()

    return Behaviors.receive(receive)


def checked_out(receipt: Receipt) -> Behavior[CartMsg]:
    """Checked out — terminal state. Only responds to GetTotal."""

    async def receive(ctx: ActorContext[CartMsg], msg: CartMsg) -> Behavior[CartMsg]:
        match msg:
            case GetTotal(reply_to):
                reply_to.tell(receipt.total)
                return Behaviors.same()
            case _:
                print(f"  [cart] already checked out, ignoring {type(msg).__name__}")
                return Behaviors.same()

    return Behaviors.receive(receive)


# --- Main ---


async def main() -> None:
    async with ActorSystem() as system:
        cart = system.spawn(empty_cart(), "cart")

        # State: empty_cart
        total = await system.ask(cart, lambda r: GetTotal(reply_to=r), timeout=5.0)
        print(f"  total (empty): ${total:.2f}")

        cart.tell(RemoveItem("phone"))  # no-op on empty cart

        # Transition: empty_cart -> active_cart
        cart.tell(AddItem(Item("keyboard", 75.00)))
        cart.tell(AddItem(Item("mouse", 25.00)))
        cart.tell(AddItem(Item("keyboard", 75.00, quantity=2)))  # now 3 keyboards
        await asyncio.sleep(0.1)

        total = await system.ask(cart, lambda r: GetTotal(reply_to=r), timeout=5.0)
        print(f"  total (active): ${total:.2f}")

        # Remove mouse -> still active
        cart.tell(RemoveItem("mouse"))
        await asyncio.sleep(0.1)

        # Transition: active_cart -> checked_out
        receipt = await system.ask(cart, lambda r: Checkout(reply_to=r), timeout=5.0)
        print(f"  receipt: {len(receipt.items)} item(s), ${receipt.total:.2f}")

        # State: checked_out — further modifications are rejected
        cart.tell(AddItem(Item("monitor", 300.00)))
        await asyncio.sleep(0.1)

        total = await system.ask(cart, lambda r: GetTotal(reply_to=r), timeout=5.0)
        print(f"  total (checked out): ${total:.2f}")


asyncio.run(main())
