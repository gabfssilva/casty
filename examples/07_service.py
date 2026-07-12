"""Services: RPC concorrente sobre atores (spec 08).

Um ator serializa: um handler por vez, e um `await` numa I/O lenta segura o
worker por toda a espera. Um `@casty.service` é um ator gerado cujo handler não
espera o trabalho — dispara o método como task e devolve a mailbox — então N
chamadas progridem juntas. Ele não tem estado: quem guarda estado é o ator que
o método chama, e chamadas do mesmo `(ator, key)` continuam serializadas lá.

O service é a porta concorrente; o ator, o guardião serial.

Run: uv run python examples/07_service.py
"""

import asyncio
import time

import casty
from pygments.lexers import factor

from dataclasses import dataclass, field

@casty.actor
class Inventory:
    stock: int = 10

    async def reserve(self, qty: int) -> bool:
        if qty > self.stock:
            return False
        self.stock -= qty  # serial por sku: nunca vende o que não tem
        return True

    async def left(self) -> int:
        return self.stock


@casty.service(concurrency=32)
class Checkout:

    async def buy(self, sku: str, qty: int) -> bool:
        context = casty.context()
        await asyncio.sleep(0.2)  # I/O: gateway de pagamento
        return await context.actor(Inventory, sku).reserve(qty)


async def main() -> None:
    async with casty.local() as node:
        checkout = node.service(Checkout)

        started = time.perf_counter()
        results = await asyncio.gather(*[checkout.buy("sku-1", 1) for _ in range(12)])
        elapsed = time.perf_counter() - started

        sold = sum(results)
        print(f"{len(results)} pedidos em {elapsed:.2f}s (serial seriam {0.2 * 12:.1f}s)")
        print(f"vendidos: {sold}, recusados: {len(results) - sold}")
        print(f"estoque: {await node.actor(Inventory, 'sku-1').left()}")


if __name__ == "__main__":
    asyncio.run(main())
