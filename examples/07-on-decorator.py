#!/usr/bin/env python
"""Example of using @on decorator for type-safe message dispatch.

The @on decorator provides a cleaner way to handle different message types
compared to pattern matching in receive(). It also enables composition of
message handlers through mixins.
"""

import asyncio
from dataclasses import dataclass
from casty import Actor, ActorSystem, Context, on


@dataclass
class Deposit:
    amount: float


@dataclass
class Withdraw:
    amount: float


@dataclass
class GetBalance:
    pass


# Example 1: Simple handlers with @on
class BankAccount(Actor[Deposit | Withdraw | GetBalance]):
    def __init__(self, initial_balance: float = 0.0):
        self.balance = initial_balance

    @on(Deposit)
    async def handle_deposit(self, msg: Deposit, ctx: Context):
        self.balance += msg.amount
        print(f"Deposited {msg.amount}, new balance: {self.balance}")

    @on(Withdraw)
    async def handle_withdraw(self, msg: Withdraw, ctx: Context):
        if self.balance >= msg.amount:
            self.balance -= msg.amount
            print(f"Withdrew {msg.amount}, new balance: {self.balance}")
        else:
            print(f"Insufficient funds. Balance: {self.balance}")

    @on(GetBalance)
    async def handle_query(self, msg: GetBalance, ctx: Context):
        await ctx.reply(self.balance)


# Example 2: Composition via mixins
class DepositMixin:
    @on(Deposit)
    async def handle_deposit(self, msg: Deposit, ctx: Context):
        self.balance += msg.amount


class WithdrawMixin:
    @on(Withdraw)
    async def handle_withdraw(self, msg: Withdraw, ctx: Context):
        self.balance -= msg.amount


class QueryMixin:
    @on(GetBalance)
    async def handle_query(self, msg: GetBalance, ctx: Context):
        await ctx.reply(self.balance)


class ComposedBankAccount(
    Actor[Deposit | Withdraw | GetBalance],
    DepositMixin,
    WithdrawMixin,
    QueryMixin,
):
    def __init__(self, initial_balance: float = 0.0):
        self.balance = initial_balance


async def main():
    async with ActorSystem() as system:
        # Example 1: Simple handlers
        print("=== Simple Handler Example ===")
        account1 = await system.spawn(BankAccount, initial_balance=100.0)

        await account1.send(Deposit(50.0))
        await account1.send(Withdraw(30.0))
        balance = await account1.ask(GetBalance())
        print(f"Final balance: {balance}\n")

        # Example 2: Composed handlers via mixins
        print("=== Mixin Composition Example ===")
        account2 = await system.spawn(ComposedBankAccount, initial_balance=200.0)

        await account2.send(Deposit(75.0))
        await account2.send(Withdraw(50.0))
        balance = await account2.ask(GetBalance())
        print(f"Final balance: {balance}\n")

        # Comparison with traditional match statement
        print("=== Traditional receive() Example ===")

        class TraditionalAccount(Actor[Deposit | Withdraw | GetBalance]):
            def __init__(self, initial_balance: float = 0.0):
                self.balance = initial_balance

            async def receive(self, msg: Deposit | Withdraw | GetBalance, ctx: Context):
                match msg:
                    case Deposit(amount):
                        self.balance += amount
                        print(f"Deposited {amount}, new balance: {self.balance}")
                    case Withdraw(amount):
                        self.balance -= amount
                        print(f"Withdrew {amount}, new balance: {self.balance}")
                    case GetBalance():
                        await ctx.reply(self.balance)

        account3 = await system.spawn(TraditionalAccount, initial_balance=150.0)
        await account3.send(Deposit(60.0))
        balance = await account3.ask(GetBalance())
        print(f"Final balance: {balance}")


if __name__ == "__main__":
    asyncio.run(main())
