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
from uuid import uuid4

from casty import actor, ActorSystem, Mailbox, LocalActorRef


@dataclass(frozen=True)
class ProductCreated:
    product_id: str
    name: str
    description: str
    price: float
    category: str
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class ProductPriceChanged:
    product_id: str
    old_price: float
    new_price: float
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class ProductCategoryChanged:
    product_id: str
    old_category: str
    new_category: str
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class ProductDeleted:
    product_id: str
    timestamp: datetime = field(default_factory=datetime.now)


ProductEvent = ProductCreated | ProductPriceChanged | ProductCategoryChanged | ProductDeleted


@dataclass
class CreateProduct:
    name: str
    description: str
    price: float
    category: str


@dataclass
class ChangePrice:
    product_id: str
    new_price: float


@dataclass
class ChangeCategory:
    product_id: str
    new_category: str


@dataclass
class DeleteProduct:
    product_id: str


ProductCommand = CreateProduct | ChangePrice | ChangeCategory | DeleteProduct


@dataclass
class GetProduct:
    product_id: str


@dataclass
class GetAllProducts:
    pass


@dataclass
class GetProductsByCategory:
    category: str


@dataclass
class GetCategorySummary:
    pass


ProductQuery = GetProduct | GetAllProducts | GetProductsByCategory | GetCategorySummary


@dataclass
class _Product:
    product_id: str
    name: str
    description: str
    price: float
    category: str


@dataclass
class _ProductView:
    product_id: str
    name: str
    description: str
    price: float
    category: str
    created_at: datetime
    updated_at: datetime


@dataclass
class CategorySummary:
    category: str
    product_count: int
    total_value: float
    avg_price: float


WriteModelMsg = ProductCommand
ReadModelMsg = ProductEvent | ProductQuery


@actor
async def write_model(read_models: list[LocalActorRef[ReadModelMsg]], *, mailbox: Mailbox[WriteModelMsg]):
    products: dict[str, _Product] = {}

    async def publish_event(event: ProductEvent):
        for read_model in read_models:
            await read_model.send(event)

    async for msg, ctx in mailbox:
        match msg:
            case CreateProduct(name, description, price, category):
                if price < 0:
                    print(f"[WriteModel] Invalid price: {price}")
                    await ctx.reply(None)
                    continue

                product_id = str(uuid4())[:8]
                product = _Product(
                    product_id=product_id,
                    name=name,
                    description=description,
                    price=price,
                    category=category,
                )
                products[product_id] = product

                event = ProductCreated(
                    product_id=product_id,
                    name=name,
                    description=description,
                    price=price,
                    category=category,
                )
                await publish_event(event)
                print(f"[WriteModel] ProductCreated: {product_id} - {name}")
                await ctx.reply(product_id)

            case ChangePrice(product_id, new_price):
                product = products.get(product_id)
                if not product:
                    print(f"[WriteModel] Product not found: {product_id}")
                    continue

                old_price = product.price
                product.price = new_price

                event = ProductPriceChanged(
                    product_id=product_id,
                    old_price=old_price,
                    new_price=new_price,
                )
                await publish_event(event)
                print(f"[WriteModel] PriceChanged: {product_id} ${old_price:.2f} -> ${new_price:.2f}")

            case ChangeCategory(product_id, new_category):
                product = products.get(product_id)
                if not product:
                    print(f"[WriteModel] Product not found: {product_id}")
                    continue

                old_category = product.category
                product.category = new_category

                event = ProductCategoryChanged(
                    product_id=product_id,
                    old_category=old_category,
                    new_category=new_category,
                )
                await publish_event(event)
                print(f"[WriteModel] CategoryChanged: {product_id} '{old_category}' -> '{new_category}'")

            case DeleteProduct(product_id):
                if product_id in products:
                    del products[product_id]
                    event = ProductDeleted(product_id=product_id)
                    await publish_event(event)
                    print(f"[WriteModel] ProductDeleted: {product_id}")


@actor
async def read_model(*, mailbox: Mailbox[ReadModelMsg]):
    products: dict[str, _ProductView] = {}
    by_category: dict[str, set[str]] = {}

    async for msg, ctx in mailbox:
        match msg:
            case ProductCreated() as event:
                view = _ProductView(
                    product_id=event.product_id,
                    name=event.name,
                    description=event.description,
                    price=event.price,
                    category=event.category,
                    created_at=event.timestamp,
                    updated_at=event.timestamp,
                )
                products[event.product_id] = view

                if event.category not in by_category:
                    by_category[event.category] = set()
                by_category[event.category].add(event.product_id)

                print(f"  [ReadModel] Projected ProductCreated: {event.product_id}")

            case ProductPriceChanged() as event:
                view = products.get(event.product_id)
                if view:
                    view.price = event.new_price
                    view.updated_at = event.timestamp
                    print(f"  [ReadModel] Projected PriceChanged: {event.product_id}")

            case ProductCategoryChanged() as event:
                view = products.get(event.product_id)
                if view:
                    if event.old_category in by_category:
                        by_category[event.old_category].discard(event.product_id)

                    if event.new_category not in by_category:
                        by_category[event.new_category] = set()
                    by_category[event.new_category].add(event.product_id)

                    view.category = event.new_category
                    view.updated_at = event.timestamp
                    print(f"  [ReadModel] Projected CategoryChanged: {event.product_id}")

            case ProductDeleted() as event:
                view = products.pop(event.product_id, None)
                if view:
                    if view.category in by_category:
                        by_category[view.category].discard(event.product_id)
                    print(f"  [ReadModel] Projected ProductDeleted: {event.product_id}")

            case GetProduct(product_id):
                view = products.get(product_id)
                await ctx.reply(view)

            case GetAllProducts():
                await ctx.reply(list(products.values()))

            case GetProductsByCategory(category):
                product_ids = by_category.get(category, set())
                result = [products[pid] for pid in product_ids if pid in products]
                await ctx.reply(result)

            case GetCategorySummary():
                summaries = []
                for category, product_ids in by_category.items():
                    category_products = [products[pid] for pid in product_ids if pid in products]
                    if category_products:
                        total = sum(p.price for p in category_products)
                        summaries.append(CategorySummary(
                            category=category,
                            product_count=len(category_products),
                            total_value=total,
                            avg_price=total / len(category_products),
                        ))
                await ctx.reply(summaries)


async def main():
    print("=" * 60)
    print("Casty CQRS Example")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        rm = await system.actor(read_model(), name="read-model")
        print("Read model created")

        wm = await system.actor(write_model(read_models=[rm]), name="write-model")
        print("Write model created")
        print()

        print("--- Executing commands ---")
        laptop_id = await wm.ask(CreateProduct(
            name="Gaming Laptop",
            description="High-performance gaming laptop",
            price=1299.99,
            category="Electronics",
        ))
        await asyncio.sleep(0.05)

        mouse_id = await wm.ask(CreateProduct(
            name="Wireless Mouse",
            description="Ergonomic wireless mouse",
            price=49.99,
            category="Electronics",
        ))
        await asyncio.sleep(0.05)

        chair_id = await wm.ask(CreateProduct(
            name="Office Chair",
            description="Ergonomic office chair",
            price=299.99,
            category="Furniture",
        ))
        await asyncio.sleep(0.05)

        await wm.send(ChangePrice(laptop_id, 1199.99))
        await asyncio.sleep(0.05)

        await wm.send(ChangeCategory(mouse_id, "Accessories"))
        await asyncio.sleep(0.1)
        print()

        print("--- Querying read model ---")
        print()

        print("Query: GetProduct (laptop)")
        product: _ProductView = await rm.ask(GetProduct(laptop_id))
        if product:
            print(f"  {product.name}: ${product.price:.2f} ({product.category})")
        print()

        print("Query: GetAllProducts")
        all_products: list[_ProductView] = await rm.ask(GetAllProducts())
        for p in all_products:
            print(f"  {p.product_id}: {p.name} - ${p.price:.2f} ({p.category})")
        print()

        print("Query: GetProductsByCategory (Electronics)")
        electronics: list[_ProductView] = await rm.ask(GetProductsByCategory("Electronics"))
        for p in electronics:
            print(f"  {p.name}: ${p.price:.2f}")
        print()

        print("Query: GetCategorySummary")
        summaries: list[CategorySummary] = await rm.ask(GetCategorySummary())
        for s in summaries:
            print(f"  {s.category}: {s.product_count} products, total=${s.total_value:.2f}, avg=${s.avg_price:.2f}")
        print()

        print("--- Deleting a product ---")
        await wm.send(DeleteProduct(mouse_id))
        await asyncio.sleep(0.1)
        print()

        print("--- Final category summary ---")
        summaries = await rm.ask(GetCategorySummary())
        for s in summaries:
            print(f"  {s.category}: {s.product_count} products, total=${s.total_value:.2f}")

    print()
    print("=" * 60)
    print("CQRS system shut down")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
