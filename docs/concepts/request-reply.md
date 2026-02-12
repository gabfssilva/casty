# Request-Reply

Actors communicate via fire-and-forget `tell()` by default. When a response is needed, the actor model uses the **reply-to pattern**: the sender includes its own `ActorRef` in the message so the receiver can send a response back.

```python
@dataclass(frozen=True)
class Deposit:
    amount: int

@dataclass(frozen=True)
class Withdraw:
    amount: int
    reply_to: ActorRef[str]

@dataclass(frozen=True)
class GetBalance:
    reply_to: ActorRef[int]

type AccountMsg = Deposit | Withdraw | GetBalance

def bank_account(balance: int = 0) -> Behavior[AccountMsg]:
    async def receive(ctx: ActorContext[AccountMsg], msg: AccountMsg) -> Behavior[AccountMsg]:
        match msg:
            case Deposit(amount):
                return bank_account(balance + amount)
            case Withdraw(amount, reply_to) if balance >= amount:
                reply_to.tell("ok")
                return bank_account(balance - amount)
            case Withdraw(_, reply_to):
                reply_to.tell(f"insufficient funds (balance={balance})")
                return Behaviors.same()
            case GetBalance(reply_to):
                reply_to.tell(balance)
                return Behaviors.same()

    return Behaviors.receive(receive)

async def main() -> None:
    async with ActorSystem() as system:
        account = system.spawn(bank_account(), "account")
        account.tell(Deposit(100))

        balance = await system.ask(
            account,
            lambda reply_to: GetBalance(reply_to=reply_to),
            timeout=5.0,
        )
        print(f"Balance: {balance}")  # Balance: 100

        result = await system.ask(
            account,
            lambda reply_to: Withdraw(200, reply_to=reply_to),
            timeout=5.0,
        )
        print(f"Withdraw: {result}")  # Withdraw: insufficient funds (balance=100)

asyncio.run(main())
```

`system.ask()` is a convenience that creates a temporary actor behind the scenes, passes its `ActorRef` as the `reply_to` field, and awaits the response with a timeout. The underlying mechanism is still `tell` — `ask` simply wraps the reply-to pattern into a coroutine.

!!! warning
    `system.ask()` is meant for use **outside** actors — from `main()`, HTTP handlers, or other external code. If called inside an actor's receive handler, it blocks that actor's mailbox until the response arrives (actors process one message at a time), which can lead to deadlocks.

    Inside actors, pass `ctx.self` as `reply_to` and handle the response as a regular message:

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

@dataclass(frozen=True)
class CheckBalance:
    account: ActorRef[AccountMsg]

type MonitorMsg = CheckBalance | int

def monitor() -> Behavior[MonitorMsg]:
    async def receive(ctx: ActorContext[MonitorMsg], msg: MonitorMsg) -> Behavior[MonitorMsg]:
        match msg:
            case CheckBalance(account):
                # Non-blocking: sends the request and keeps processing
                account.tell(GetBalance(reply_to=ctx.self))
                return Behaviors.same()
            case int() as balance:
                print(f"Balance: {balance}")
                return Behaviors.same()

    return Behaviors.receive(receive)

async def main() -> None:
    async with ActorSystem() as system:
        acc = system.spawn(bank_account(), "account")
        acc.tell(Deposit(100))

        mon = system.spawn(monitor(), "monitor")
        mon.tell(CheckBalance(account=acc))  # prints "Balance: 100"
```

---

**Next:** [Pipe to Self](pipe-to-self.md)
