"""CQRS Example - Command/Query Responsibility Segregation.

Demonstrates:
- Write model (command side) processes commands and emits events
- Read model (query side) projects events into optimized views
- Separate actors for write and read responsibilities
- Product catalog example with denormalized read views

Run with:
    uv run python examples/practical/06-cqrs.py
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import uuid4

from casty import Actor, ActorSystem, Context, LocalActorRef, on


# --- Domain Events ---

@dataclass(frozen=True)
class ProductCreated:
    """Product was added to catalog."""
    product_id: str
    name: str
    description: str
    price: float
    category: str
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class ProductPriceChanged:
    """Product price was updated."""
    product_id: str
    old_price: float
    new_price: float
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class ProductCategoryChanged:
    """Product category was changed."""
    product_id: str
    old_category: str
    new_category: str
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class ProductDeleted:
    """Product was removed from catalog."""
    product_id: str
    timestamp: datetime = field(default_factory=datetime.now)


type ProductEvent = ProductCreated | ProductPriceChanged | ProductCategoryChanged | ProductDeleted


# --- Commands (Write Side) ---

@dataclass
class CreateProduct:
    """Command to create a new product."""
    name: str
    description: str
    price: float
    category: str


@dataclass
class ChangePrice:
    """Command to change a product's price."""
    product_id: str
    new_price: float


@dataclass
class ChangeCategory:
    """Command to change a product's category."""
    product_id: str
    new_category: str


@dataclass
class DeleteProduct:
    """Command to delete a product."""
    product_id: str


type ProductCommand = CreateProduct | ChangePrice | ChangeCategory | DeleteProduct


# --- Queries (Read Side) ---

@dataclass
class GetProduct:
    """Query for a single product."""
    product_id: str


@dataclass
class GetAllProducts:
    """Query for all products."""
    pass


@dataclass
class GetProductsByCategory:
    """Query for products in a category."""
    category: str


@dataclass
class GetCategorySummary:
    """Query for category statistics."""
    pass


type ProductQuery = GetProduct | GetAllProducts | GetProductsByCategory | GetCategorySummary


# --- Write Model (Command Handler) ---

@dataclass
class _Product:
    """Internal product state in write model."""
    product_id: str
    name: str
    description: str
    price: float
    category: str


type WriteModelMessage = ProductCommand


class WriteModel(Actor[WriteModelMessage]):
    """Command handler that processes commands and emits events.

    The write model:
    - Validates commands
    - Applies business rules
    - Emits domain events
    - Publishes events to read models
    """

    def __init__(self, read_models: list[LocalActorRef]):
        self.read_models = read_models
        self.products: dict[str, _Product] = {}

    async def _publish_event(self, event: ProductEvent) -> None:
        """Publish event to all read models."""
        for read_model in self.read_models:
            await read_model.send(event)

    @on(CreateProduct)
    async def handle_create(self, msg: CreateProduct, ctx: Context) -> None:
        product_id = str(uuid4())[:8]

        # Validate
        if msg.price < 0:
            print(f"[WriteModel] Invalid price: {msg.price}")
            await ctx.reply(None)
            return

        # Create product
        product = _Product(
            product_id=product_id,
            name=msg.name,
            description=msg.description,
            price=msg.price,
            category=msg.category,
        )
        self.products[product_id] = product

        # Emit event
        event = ProductCreated(
            product_id=product_id,
            name=msg.name,
            description=msg.description,
            price=msg.price,
            category=msg.category,
        )
        await self._publish_event(event)
        print(f"[WriteModel] ProductCreated: {product_id} - {msg.name}")

        await ctx.reply(product_id)

    @on(ChangePrice)
    async def handle_change_price(self, msg: ChangePrice, ctx: Context) -> None:
        product = self.products.get(msg.product_id)
        if not product:
            print(f"[WriteModel] Product not found: {msg.product_id}")
            return

        old_price = product.price
        product.price = msg.new_price

        event = ProductPriceChanged(
            product_id=msg.product_id,
            old_price=old_price,
            new_price=msg.new_price,
        )
        await self._publish_event(event)
        print(f"[WriteModel] PriceChanged: {msg.product_id} ${old_price:.2f} -> ${msg.new_price:.2f}")

    @on(ChangeCategory)
    async def handle_change_category(self, msg: ChangeCategory, ctx: Context) -> None:
        product = self.products.get(msg.product_id)
        if not product:
            print(f"[WriteModel] Product not found: {msg.product_id}")
            return

        old_category = product.category
        product.category = msg.new_category

        event = ProductCategoryChanged(
            product_id=msg.product_id,
            old_category=old_category,
            new_category=msg.new_category,
        )
        await self._publish_event(event)
        print(f"[WriteModel] CategoryChanged: {msg.product_id} '{old_category}' -> '{msg.new_category}'")

    @on(DeleteProduct)
    async def handle_delete(self, msg: DeleteProduct, ctx: Context) -> None:
        if msg.product_id in self.products:
            del self.products[msg.product_id]
            event = ProductDeleted(product_id=msg.product_id)
            await self._publish_event(event)
            print(f"[WriteModel] ProductDeleted: {msg.product_id}")


# --- Read Model (Query Handler + Event Projector) ---

@dataclass
class _ProductView:
    """Denormalized product view."""
    product_id: str
    name: str
    description: str
    price: float
    category: str
    created_at: datetime
    updated_at: datetime


@dataclass
class CategorySummary:
    """Category statistics."""
    category: str
    product_count: int
    total_value: float
    avg_price: float


type ReadModelMessage = ProductEvent | ProductQuery


class ReadModel(Actor[ReadModelMessage]):
    """Query handler that projects events into optimized views.

    The read model:
    - Receives events from write model
    - Maintains denormalized views for efficient queries
    - Handles query requests
    """

    def __init__(self):
        self.products: dict[str, _ProductView] = {}
        self.by_category: dict[str, set[str]] = {}  # category -> product_ids

    # --- Event Handlers (Projectors) ---

    @on(ProductCreated)
    async def project_created(self, event: ProductCreated, ctx: Context) -> None:
        view = _ProductView(
            product_id=event.product_id,
            name=event.name,
            description=event.description,
            price=event.price,
            category=event.category,
            created_at=event.timestamp,
            updated_at=event.timestamp,
        )
        self.products[event.product_id] = view

        if event.category not in self.by_category:
            self.by_category[event.category] = set()
        self.by_category[event.category].add(event.product_id)

        print(f"  [ReadModel] Projected ProductCreated: {event.product_id}")

    @on(ProductPriceChanged)
    async def project_price_changed(self, event: ProductPriceChanged, ctx: Context) -> None:
        view = self.products.get(event.product_id)
        if view:
            view.price = event.new_price
            view.updated_at = event.timestamp
            print(f"  [ReadModel] Projected PriceChanged: {event.product_id}")

    @on(ProductCategoryChanged)
    async def project_category_changed(self, event: ProductCategoryChanged, ctx: Context) -> None:
        view = self.products.get(event.product_id)
        if view:
            # Update category index
            if event.old_category in self.by_category:
                self.by_category[event.old_category].discard(event.product_id)

            if event.new_category not in self.by_category:
                self.by_category[event.new_category] = set()
            self.by_category[event.new_category].add(event.product_id)

            view.category = event.new_category
            view.updated_at = event.timestamp
            print(f"  [ReadModel] Projected CategoryChanged: {event.product_id}")

    @on(ProductDeleted)
    async def project_deleted(self, event: ProductDeleted, ctx: Context) -> None:
        view = self.products.pop(event.product_id, None)
        if view:
            if view.category in self.by_category:
                self.by_category[view.category].discard(event.product_id)
            print(f"  [ReadModel] Projected ProductDeleted: {event.product_id}")

    # --- Query Handlers ---

    @on(GetProduct)
    async def handle_get_product(self, query: GetProduct, ctx: Context) -> None:
        view = self.products.get(query.product_id)
        await ctx.reply(view)

    @on(GetAllProducts)
    async def handle_get_all(self, query: GetAllProducts, ctx: Context) -> None:
        await ctx.reply(list(self.products.values()))

    @on(GetProductsByCategory)
    async def handle_get_by_category(self, query: GetProductsByCategory, ctx: Context) -> None:
        product_ids = self.by_category.get(query.category, set())
        products = [self.products[pid] for pid in product_ids if pid in self.products]
        await ctx.reply(products)

    @on(GetCategorySummary)
    async def handle_category_summary(self, query: GetCategorySummary, ctx: Context) -> None:
        summaries = []
        for category, product_ids in self.by_category.items():
            products = [self.products[pid] for pid in product_ids if pid in self.products]
            if products:
                total = sum(p.price for p in products)
                summaries.append(CategorySummary(
                    category=category,
                    product_count=len(products),
                    total_value=total,
                    avg_price=total / len(products),
                ))
        await ctx.reply(summaries)


# --- Main ---

async def main():
    print("=" * 60)
    print("Casty CQRS Example")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        # Create read model first
        read_model = await system.actor(ReadModel, name="read-model")
        print("Read model created")

        # Create write model with reference to read model
        write_model = await system.actor(WriteModel, name="write-model", read_models=[read_model])
        print("Write model created")
        print()

        # Execute commands
        print("--- Executing commands ---")
        laptop_id = await write_model.ask(CreateProduct(
            name="Gaming Laptop",
            description="High-performance gaming laptop",
            price=1299.99,
            category="Electronics",
        ))
        await asyncio.sleep(0.05)

        mouse_id = await write_model.ask(CreateProduct(
            name="Wireless Mouse",
            description="Ergonomic wireless mouse",
            price=49.99,
            category="Electronics",
        ))
        await asyncio.sleep(0.05)

        chair_id = await write_model.ask(CreateProduct(
            name="Office Chair",
            description="Ergonomic office chair",
            price=299.99,
            category="Furniture",
        ))
        await asyncio.sleep(0.05)

        await write_model.send(ChangePrice(laptop_id, 1199.99))
        await asyncio.sleep(0.05)

        await write_model.send(ChangeCategory(mouse_id, "Accessories"))
        await asyncio.sleep(0.1)
        print()

        # Execute queries against read model
        print("--- Querying read model ---")
        print()

        # Get single product
        print("Query: GetProduct (laptop)")
        product: _ProductView = await read_model.ask(GetProduct(laptop_id))
        if product:
            print(f"  {product.name}: ${product.price:.2f} ({product.category})")
        print()

        # Get all products
        print("Query: GetAllProducts")
        all_products: list[_ProductView] = await read_model.ask(GetAllProducts())
        for p in all_products:
            print(f"  {p.product_id}: {p.name} - ${p.price:.2f} ({p.category})")
        print()

        # Get by category
        print("Query: GetProductsByCategory (Electronics)")
        electronics: list[_ProductView] = await read_model.ask(GetProductsByCategory("Electronics"))
        for p in electronics:
            print(f"  {p.name}: ${p.price:.2f}")
        print()

        # Get category summary
        print("Query: GetCategorySummary")
        summaries: list[CategorySummary] = await read_model.ask(GetCategorySummary())
        for s in summaries:
            print(f"  {s.category}: {s.product_count} products, total=${s.total_value:.2f}, avg=${s.avg_price:.2f}")
        print()

        # Delete a product
        print("--- Deleting a product ---")
        await write_model.send(DeleteProduct(mouse_id))
        await asyncio.sleep(0.1)
        print()

        # Final category summary
        print("--- Final category summary ---")
        summaries = await read_model.ask(GetCategorySummary())
        for s in summaries:
            print(f"  {s.category}: {s.product_count} products, total=${s.total_value:.2f}")

    print()
    print("=" * 60)
    print("CQRS system shut down")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
