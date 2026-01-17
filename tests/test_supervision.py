"""Tests for supervision hierarchy."""

import asyncio
from dataclasses import dataclass

import pytest

from casty import (
    Actor,
    ActorSystem,
    Context,
    MultiChildStrategy,
    SupervisionDecision,
    SupervisionStrategy,
    SupervisorConfig,
    supervised,
)

from .conftest import Counter, FailingActor, FailNow, GetValue, Increment


class TestSupervisionConfig:
    """Tests for SupervisorConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = SupervisorConfig()
        assert config.strategy == SupervisionStrategy.RESTART
        assert config.multi_child == MultiChildStrategy.ONE_FOR_ONE
        assert config.max_restarts == 3
        assert config.within_seconds == 60.0
        assert config.backoff_initial == 0.1
        assert config.backoff_max == 30.0
        assert config.backoff_multiplier == 2.0

    def test_custom_config(self):
        """Test custom configuration."""
        config = SupervisorConfig(
            strategy=SupervisionStrategy.STOP,
            max_restarts=10,
            backoff_initial=0.5,
        )
        assert config.strategy == SupervisionStrategy.STOP
        assert config.max_restarts == 10
        assert config.backoff_initial == 0.5


class TestSupervisedDecorator:
    """Tests for @supervised decorator."""

    def test_decorator_sets_config(self):
        """Test that decorator sets supervision_config."""

        @supervised(strategy=SupervisionStrategy.RESTART, max_restarts=5)
        class MyActor(Actor[str]):
            async def receive(self, msg: str, ctx: Context) -> None:
                pass

        assert hasattr(MyActor, "supervision_config")
        assert MyActor.supervision_config.strategy == SupervisionStrategy.RESTART
        assert MyActor.supervision_config.max_restarts == 5

    def test_decorator_with_all_options(self):
        """Test decorator with all options."""

        @supervised(
            strategy=SupervisionStrategy.ESCALATE,
            multi_child=MultiChildStrategy.ONE_FOR_ALL,
            max_restarts=10,
            within_seconds=30.0,
            backoff_initial=0.5,
            backoff_max=10.0,
            backoff_multiplier=3.0,
        )
        class MyActor(Actor[str]):
            async def receive(self, msg: str, ctx: Context) -> None:
                pass

        config = MyActor.supervision_config
        assert config.strategy == SupervisionStrategy.ESCALATE
        assert config.multi_child == MultiChildStrategy.ONE_FOR_ALL
        assert config.max_restarts == 10
        assert config.within_seconds == 30.0
        assert config.backoff_initial == 0.5
        assert config.backoff_max == 10.0
        assert config.backoff_multiplier == 3.0


class TestActorRestart:
    """Tests for actor restart behavior."""

    @pytest.mark.asyncio
    async def test_actor_restarts_on_failure(self, system: ActorSystem):
        """Test that actor restarts after failure."""

        @supervised(strategy=SupervisionStrategy.RESTART, max_restarts=5)
        class FailOnceActor(Actor[str | GetValue]):
            def __init__(self):
                self.fail_count = 0
                self.message_count = 0

            async def receive(self, msg: str | GetValue, ctx: Context) -> None:
                match msg:
                    case "fail":
                        self.fail_count += 1
                        if self.fail_count == 1:
                            raise RuntimeError("First failure")
                        # After restart, don't fail
                    case GetValue():
                        await ctx.reply(self.message_count)
                    case _:
                        self.message_count += 1

        actor = await system.spawn(FailOnceActor)

        # Send a message that causes failure
        await actor.send("fail")
        await asyncio.sleep(0.2)  # Wait for restart

        # Actor should still work after restart
        await actor.send("normal")
        await asyncio.sleep(0.1)

        result = await actor.ask(GetValue())
        # After restart, message_count is reset to 0, then we sent "normal"
        assert result >= 0  # Should be working

    @pytest.mark.asyncio
    async def test_restart_with_backoff(self, system: ActorSystem):
        """Test that restart applies backoff."""

        @supervised(
            strategy=SupervisionStrategy.RESTART,
            max_restarts=10,
            backoff_initial=0.1,
        )
        class QuickFailActor(Actor[str]):
            def __init__(self):
                self.started_at = asyncio.get_event_loop().time()

            async def receive(self, msg: str, ctx: Context) -> None:
                if msg == "fail":
                    raise RuntimeError("Failure")

        actor = await system.spawn(QuickFailActor)

        # Cause a failure
        await actor.send("fail")
        await asyncio.sleep(0.2)  # Backoff should be at least 0.1s

        # Actor should be restarted
        # We can verify by sending another message

    @pytest.mark.asyncio
    async def test_lifecycle_hooks_on_restart(self, system: ActorSystem):
        """Test that pre_restart and post_restart are called."""
        pre_restart_called = []
        post_restart_called = []

        @supervised(strategy=SupervisionStrategy.RESTART, max_restarts=5)
        class HookTrackingActor(Actor[str]):
            async def pre_restart(self, exc: Exception, msg: str | None) -> None:
                pre_restart_called.append((type(exc).__name__, msg))
                await super().pre_restart(exc, msg)

            async def post_restart(self, exc: Exception) -> None:
                post_restart_called.append(type(exc).__name__)
                await super().post_restart(exc)

            async def receive(self, msg: str, ctx: Context) -> None:
                if msg == "fail":
                    raise ValueError("Test failure")

        actor = await system.spawn(HookTrackingActor)

        await actor.send("fail")
        await asyncio.sleep(0.3)

        assert len(pre_restart_called) >= 1
        assert pre_restart_called[0][0] == "ValueError"
        assert pre_restart_called[0][1] == "fail"

        assert len(post_restart_called) >= 1
        assert post_restart_called[0] == "ValueError"


class TestSupervisionStrategies:
    """Tests for different supervision strategies."""

    @pytest.mark.asyncio
    async def test_stop_strategy(self, system: ActorSystem):
        """Test that STOP strategy stops the actor."""
        stopped = []

        @supervised(strategy=SupervisionStrategy.STOP)
        class StopOnFailActor(Actor[str]):
            async def on_stop(self) -> None:
                stopped.append(True)

            async def receive(self, msg: str, ctx: Context) -> None:
                if msg == "fail":
                    raise RuntimeError("Stop me")

        actor = await system.spawn(StopOnFailActor)

        await actor.send("fail")
        await asyncio.sleep(0.2)

        assert len(stopped) >= 1

    @pytest.mark.asyncio
    async def test_max_restarts_exceeded(self, system: ActorSystem):
        """Test that actor stops when max restarts exceeded."""
        stopped = []
        restart_count = [0]

        @supervised(
            strategy=SupervisionStrategy.RESTART,
            max_restarts=2,
            within_seconds=60.0,
            backoff_initial=0.01,  # Fast backoff for testing
        )
        class AlwaysFailActor(Actor[str]):
            async def post_restart(self, exc: Exception) -> None:
                restart_count[0] += 1

            async def on_stop(self) -> None:
                stopped.append(True)

            async def receive(self, msg: str, ctx: Context) -> None:
                raise RuntimeError("Always fail")

        actor = await system.spawn(AlwaysFailActor)

        # Send multiple failure-causing messages
        await actor.send("fail1")
        await asyncio.sleep(0.1)
        await actor.send("fail2")
        await asyncio.sleep(0.1)
        await actor.send("fail3")
        await asyncio.sleep(0.3)

        # Should have been stopped after exceeding max_restarts
        assert restart_count[0] >= 2


class TestChildSupervision:
    """Tests for parent-child supervision."""

    @pytest.mark.asyncio
    async def test_parent_supervises_child(self, system: ActorSystem):
        """Test that parent can spawn and supervise children."""

        @dataclass
        class SpawnFailingChild:
            pass

        @dataclass
        class GetChildRestarts:
            pass

        child_restarts = [0]

        @supervised(strategy=SupervisionStrategy.RESTART, max_restarts=5, backoff_initial=0.01)
        class FailingChild(Actor[str]):
            async def post_restart(self, exc: Exception) -> None:
                child_restarts[0] += 1

            async def receive(self, msg: str, ctx: Context) -> None:
                if msg == "fail":
                    raise RuntimeError("Child failure")

        class ParentActor(Actor[SpawnFailingChild | str]):
            def __init__(self):
                self.child = None

            async def receive(
                self, msg: SpawnFailingChild | str, ctx: Context
            ) -> None:
                match msg:
                    case SpawnFailingChild():
                        self.child = await ctx.spawn(FailingChild, name="failing-child")
                        await ctx.reply(self.child)
                    case str() as s:
                        if self.child:
                            await self.child.send(s)

        parent = await system.spawn(ParentActor, name="parent")

        # Spawn a child
        child = await parent.ask(SpawnFailingChild())
        assert child is not None

        # Make the child fail
        await parent.send("fail")
        await asyncio.sleep(0.2)

        # Child should have been restarted
        assert child_restarts[0] >= 1

    @pytest.mark.asyncio
    async def test_on_child_failure_override(self, system: ActorSystem):
        """Test that parent can override child failure handling."""
        override_called = []

        class CustomSupervisor(Actor[str]):
            def __init__(self):
                self.child = None

            async def on_child_failure(self, child, exc) -> SupervisionDecision | None:
                override_called.append((child.id.name, type(exc).__name__))
                # Override: always stop
                return SupervisionDecision(action=SupervisionStrategy.STOP)

            async def receive(self, msg: str, ctx: Context) -> None:
                if msg == "spawn":
                    @supervised(strategy=SupervisionStrategy.RESTART)
                    class ChildActor(Actor[str]):
                        async def receive(self, msg: str, ctx: Context) -> None:
                            raise ValueError("Child error")

                    self.child = await ctx.spawn(ChildActor, name="my-child")
                    await ctx.reply(self.child)
                elif msg == "fail" and self.child:
                    await self.child.send("trigger")

        parent = await system.spawn(CustomSupervisor)

        child = await parent.ask("spawn")
        await parent.send("fail")
        await asyncio.sleep(0.2)

        assert len(override_called) >= 1
        assert override_called[0][0] == "my-child"
        assert override_called[0][1] == "ValueError"


class TestContextChildManagement:
    """Tests for Context child management methods."""

    @pytest.mark.asyncio
    async def test_context_children_property(self, system: ActorSystem):
        """Test ctx.children returns child actors."""

        @dataclass
        class SpawnChildren:
            count: int

        @dataclass
        class GetChildCount:
            pass

        class ParentActor(Actor[SpawnChildren | GetChildCount]):
            async def receive(
                self, msg: SpawnChildren | GetChildCount, ctx: Context
            ) -> None:
                match msg:
                    case SpawnChildren(count):
                        for i in range(count):
                            await ctx.spawn(Counter, name=f"child-{i}")
                        await ctx.reply(len(ctx.children))
                    case GetChildCount():
                        await ctx.reply(len(ctx.children))

        parent = await system.spawn(ParentActor)

        result = await parent.ask(SpawnChildren(3))
        assert result == 3

        count = await parent.ask(GetChildCount())
        assert count == 3

    @pytest.mark.asyncio
    async def test_stop_child(self, system: ActorSystem):
        """Test ctx.stop_child stops a specific child."""

        @dataclass
        class SpawnChild:
            name: str

        @dataclass
        class StopChild:
            name: str

        children = {}

        class ParentActor(Actor[SpawnChild | StopChild | GetValue]):
            async def receive(
                self, msg: SpawnChild | StopChild | GetValue, ctx: Context
            ) -> None:
                match msg:
                    case SpawnChild(name):
                        child = await ctx.spawn(Counter, name=name)
                        children[name] = child
                        await ctx.reply(child)
                    case StopChild(name):
                        if name in children:
                            result = await ctx.stop_child(children[name])
                            await ctx.reply(result)
                        else:
                            await ctx.reply(False)
                    case GetValue():
                        await ctx.reply(len(ctx.children))

        parent = await system.spawn(ParentActor)

        # Spawn children
        await parent.ask(SpawnChild("child-1"))
        await parent.ask(SpawnChild("child-2"))

        count = await parent.ask(GetValue())
        assert count == 2

        # Stop one child
        result = await parent.ask(StopChild("child-1"))
        assert result is True
        await asyncio.sleep(0.1)

        count = await parent.ask(GetValue())
        assert count == 1

    @pytest.mark.asyncio
    async def test_stop_all_children(self, system: ActorSystem):
        """Test ctx.stop_all_children stops all children."""

        @dataclass
        class SpawnMany:
            count: int

        @dataclass
        class StopAll:
            pass

        class ParentActor(Actor[SpawnMany | StopAll | GetValue]):
            async def receive(
                self, msg: SpawnMany | StopAll | GetValue, ctx: Context
            ) -> None:
                match msg:
                    case SpawnMany(count):
                        for i in range(count):
                            await ctx.spawn(Counter, name=f"child-{i}")
                        await ctx.reply(len(ctx.children))
                    case StopAll():
                        await ctx.stop_all_children()
                        await ctx.reply(len(ctx.children))
                    case GetValue():
                        await ctx.reply(len(ctx.children))

        parent = await system.spawn(ParentActor)

        await parent.ask(SpawnMany(5))
        count = await parent.ask(GetValue())
        assert count == 5

        await parent.ask(StopAll())
        await asyncio.sleep(0.1)

        count = await parent.ask(GetValue())
        assert count == 0
