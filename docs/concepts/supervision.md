# Supervision

In traditional programming, errors propagate upward through the call stack via exceptions. In the actor model, errors propagate **to the supervisor**. This is the "let it crash" philosophy pioneered by Erlang/OTP: instead of writing defensive code within the actor to handle every possible failure, let the actor fail fast and let its supervisor decide the recovery strategy.

A supervisor is any actor that has spawned children. The supervision strategy defines what happens when a child fails:

| Directive | Effect |
|-----------|--------|
| `Directive.restart` | Restart the actor with its initial behavior, resetting state |
| `Directive.stop` | Stop the actor permanently |
| `Directive.escalate` | Propagate the failure to the next supervisor up the hierarchy |

`OneForOneStrategy` supervises each child independently. It tracks restart counts within a configurable time window — if a child exceeds the limit, it is stopped instead of restarted:

```python
def unreliable_worker() -> Behavior[str]:
    async def receive(ctx: ActorContext[str], msg: str) -> Behavior[str]:
        if msg == "crash":
            raise RuntimeError("something went wrong")
        print(f"Processed: {msg}")
        return Behaviors.same()

    return Behaviors.receive(receive)

async def main() -> None:
    strategy = OneForOneStrategy(
        max_restarts=3,
        within=60.0,
        decider=lambda exc: Directive.restart,
    )

    async with ActorSystem() as system:
        ref = system.spawn(
            Behaviors.supervise(unreliable_worker(), strategy),
            "worker",
        )

        ref.tell("crash")          # fails, supervisor restarts
        await asyncio.sleep(0.2)

        ref.tell("hello")          # succeeds — actor recovered
        await asyncio.sleep(0.1)

asyncio.run(main())
```

The `decider` function receives the exception and returns a directive. This allows fine-grained control: restart on transient errors, stop on fatal ones, escalate on unknown failures.

An important interaction to note: when a supervised actor without event sourcing is restarted, its state is reset to the initial behavior. This means the bank account from previous sections would lose its balance on restart. Event sourcing (covered in [Persistence](../persistence/event-sourcing.md)) solves this by replaying persisted events to reconstruct state after a restart.

---

**Next:** [State Machines](state-machines.md)
