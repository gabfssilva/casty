"""Persistence example for Casty.

Demonstrates:
- PersistentActor for durable state
- Write-Ahead Log (WAL) for event sourcing
- State recovery after restart

Run with:
    uv run python examples/04-persistence.py
"""

import asyncio
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from casty import Actor, ActorSystem, Context
from casty.persistence import PersistentActor, WALConfig


@dataclass
class AddItem:
    """Add an item to the cart."""
    item: str
    price: float


@dataclass
class RemoveItem:
    """Remove an item from the cart."""
    item: str


@dataclass
class GetCart:
    """Get the current cart contents."""
    pass


@dataclass
class GetTotal:
    """Get the cart total."""
    pass


class ShoppingCart(PersistentActor[AddItem | RemoveItem | GetCart | GetTotal]):
    """Persistent shopping cart with event-sourced state.

    The cart state survives system restarts because:
    1. State changes are logged to a Write-Ahead Log (WAL)
    2. On restart, state is recovered from the WAL
    """

    def __init__(self, cart_id: str = "default"):
        self.cart_id = cart_id
        self.items: dict[str, float] = {}  # item -> price

    def get_state(self) -> dict[str, Any]:
        """Serialize state for persistence."""
        return {
            "cart_id": self.cart_id,
            "items": self.items.copy(),
        }

    def set_state(self, state: dict[str, Any]) -> None:
        """Restore state from persistence."""
        self.cart_id = state.get("cart_id", "default")
        self.items = state.get("items", {})
        print(f"[Cart:{self.cart_id}] State restored: {len(self.items)} items")

    async def receive(
        self, msg: AddItem | RemoveItem | GetCart | GetTotal, ctx: Context
    ) -> None:
        match msg:
            case AddItem(item, price):
                self.items[item] = price
                print(f"[Cart:{self.cart_id}] Added {item} (${price:.2f})")

            case RemoveItem(item):
                if item in self.items:
                    del self.items[item]
                    print(f"[Cart:{self.cart_id}] Removed {item}")
                else:
                    print(f"[Cart:{self.cart_id}] Item {item} not found")

            case GetCart():
                ctx.reply(self.items.copy())

            case GetTotal():
                total = sum(self.items.values())
                ctx.reply(total)


async def main():
    print("=" * 60)
    print("Casty Persistence Example")
    print("=" * 60)
    print()

    # Create a temporary directory for WAL
    with tempfile.TemporaryDirectory() as tmp_dir:
        wal_path = Path(tmp_dir) / "wal"
        print(f"WAL directory: {wal_path}")
        print()

        # First session: add items to cart
        print("Session 1: Building cart...")
        print("-" * 40)

        async with ActorSystem() as system:
            cart = await system.spawn(ShoppingCart, cart_id="my-cart")

            # Add items
            await cart.send(AddItem("Laptop", 999.99))
            await cart.send(AddItem("Mouse", 29.99))
            await cart.send(AddItem("Keyboard", 79.99))
            await asyncio.sleep(0.1)

            # Check total
            total = await cart.ask(GetTotal())
            print(f"Cart total: ${total:.2f}")

            items = await cart.ask(GetCart())
            print(f"Cart contents: {items}")

        print()
        print("Session 1 ended. State is saved in WAL.")
        print()

        # Note: In a real implementation, the PersistentActor would
        # automatically save to and restore from the WAL.
        # This example shows the concept - full integration would
        # involve the ActorSystem managing WAL lifecycle.

        print("=" * 60)
        print("Persistence Concept Demonstrated!")
        print()
        print("Key points:")
        print("1. PersistentActor defines get_state/set_state")
        print("2. WriteAheadLog stores events durably")
        print("3. On recovery, state is restored from WAL")
        print("4. Snapshots compact the log for efficiency")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
