"""Debug cluster issues."""
import asyncio
import sys
import traceback

from casty import actor, Mailbox, message
from casty.protocols import System
from casty.cluster import DevelopmentCluster


@message
class Inc:
    pass


@message
class Get:
    pass


CounterMsg = Inc | Get


@actor(clustered=True)
async def counter(state: int = 0, *, mailbox: Mailbox[CounterMsg], system: System):
    async for msg, ctx in mailbox:
        match msg:
            case Inc():
                state += 1
                print(f"[{system.node_id}] inc: {state}")
            case Get():
                print(f"[{system.node_id}] get: {state}")
                await ctx.reply(state)


def exception_handler(loop, context):
    exception = context.get('exception')
    if exception:
        print(f"EXCEPTION: {exception}", file=sys.stderr)
        traceback.print_exception(type(exception), exception, exception.__traceback__)
    else:
        print(f"ERROR: {context['message']}", file=sys.stderr)


async def main():
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(exception_handler)

    from casty.cluster.messages import GetAliveMembers

    async with asyncio.timeout(15):
        async with DevelopmentCluster(nodes=2) as cluster:
            print(f"Cluster ready with {len(cluster.nodes)} nodes")

            # Check membership first
            m0 = await cluster[0]._system.actor(name="membership")
            m1 = await cluster[1]._system.actor(name="membership")

            members0 = await m0.ask(GetAliveMembers())
            members1 = await m1.ask(GetAliveMembers())

            print(f"\n=== INITIAL MEMBERSHIP ===")
            print(f"Node-0 sees: {list(members0.keys())}")
            print(f"Node-1 sees: {list(members1.keys())}")
            print(f"===========================\n")

            # Wait for SWIM to propagate
            await asyncio.sleep(3)

            members0 = await m0.ask(GetAliveMembers())
            members1 = await m1.ask(GetAliveMembers())

            print(f"\n=== AFTER 3 SECONDS ===")
            print(f"Node-0 sees: {list(members0.keys())}")
            print(f"Node-1 sees: {list(members1.keys())}")
            print(f"=========================\n")

            ref = await cluster.actor(counter(), name="counter")
            print(f"Actor ref: {ref}")

            for i in range(5):
                print(f"Sending Inc #{i+1}...")
                await ref.send(Inc())

            await asyncio.sleep(0.5)

            print("Asking Get...")
            result = await ref.ask(Get())
            print(f"Counter value: {result}")


if __name__ == "__main__":
    asyncio.run(main())
