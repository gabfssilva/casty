# Functional State

The actor model requires each actor to designate the behavior for its *next* message. This is the mechanism for state transitions. In Casty, state is captured in closures: the message handler closes over the current state, and returning a new behavior with different closed-over values constitutes a state transition.

Consider a bank account actor that tracks a balance:

```python
@dataclass(frozen=True)
class Deposit:
    amount: int

@dataclass(frozen=True)
class GetBalance:
    reply_to: ActorRef[int]

type AccountMsg = Deposit | GetBalance

def bank_account(balance: int = 0) -> Behavior[AccountMsg]:
    async def receive(ctx: ActorContext[AccountMsg], msg: AccountMsg) -> Behavior[AccountMsg]:
        match msg:
            case Deposit(amount):
                return bank_account(balance + amount)
            case GetBalance(reply_to):
                reply_to.tell(balance)
                return Behaviors.same()

    return Behaviors.receive(receive)

async def main() -> None:
    async with ActorSystem() as system:
        account = system.spawn(bank_account(), "account")
        account.tell(Deposit(100))
        account.tell(Deposit(50))
        await asyncio.sleep(0.1)

asyncio.run(main())
```

The line `return bank_account(balance + amount)` is the state transition. It creates a new `ReceiveBehavior` whose handler closes over `balance + amount`. There is no mutable field, no `self.balance = ...`, no `nonlocal`. The function call **is** the state transition.

This approach — called **behavior recursion** — makes state transitions explicit, traceable, and impossible to corrupt through accidental sharing. Each invocation of `bank_account(n)` produces a completely independent behavior value.

---

**Next:** [Request-Reply](request-reply.md)
