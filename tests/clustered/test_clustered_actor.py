import pytest
from dataclasses import dataclass

from casty import Actor


class TestClusteredActor:
    def test_inherits_from_actor(self):
        from casty.cluster.clustered_actor import ClusteredActor

        assert issubclass(ClusteredActor, Actor)

    def test_can_define_subclass(self):
        from casty.cluster.clustered_actor import ClusteredActor

        @dataclass
        class Increment:
            amount: int

        class Counter(ClusteredActor[Increment]):
            def __init__(self):
                self.count = 0

            async def receive(self, msg, ctx):
                match msg:
                    case Increment(amount=amount):
                        self.count += amount

        counter = Counter()
        assert counter.count == 0

    def test_get_state_works(self):
        from casty.cluster.clustered_actor import ClusteredActor

        @dataclass
        class Msg:
            pass

        class MyActor(ClusteredActor[Msg]):
            def __init__(self):
                self.value = 42
                self.name = "test"

            async def receive(self, msg, ctx):
                pass

        actor = MyActor()
        state = actor.get_state()

        assert state == {"value": 42, "name": "test"}

    def test_set_state_works(self):
        from casty.cluster.clustered_actor import ClusteredActor

        @dataclass
        class Msg:
            pass

        class MyActor(ClusteredActor[Msg]):
            def __init__(self):
                self.value = 0
                self.name = ""

            async def receive(self, msg, ctx):
                pass

        actor = MyActor()
        actor.set_state({"value": 100, "name": "updated"})

        assert actor.value == 100
        assert actor.name == "updated"

    def test_can_override_get_state(self):
        from casty.cluster.clustered_actor import ClusteredActor

        @dataclass
        class Msg:
            pass

        class MyActor(ClusteredActor[Msg]):
            def __init__(self):
                self.value = 42
                self._internal = "secret"

            async def receive(self, msg, ctx):
                pass

            def get_state(self) -> dict:
                # Custom implementation that includes internal state
                return {"value": self.value, "has_internal": True}

        actor = MyActor()
        state = actor.get_state()

        assert state == {"value": 42, "has_internal": True}
        assert "_internal" not in state  # Demonstrates custom behavior

    def test_can_override_set_state(self):
        from casty.cluster.clustered_actor import ClusteredActor

        @dataclass
        class Msg:
            pass

        class MyActor(ClusteredActor[Msg]):
            def __init__(self):
                self.value = 0
                self.multiplier = 2

            async def receive(self, msg, ctx):
                pass

            def set_state(self, state: dict) -> None:
                # Custom implementation that applies transformation
                if "value" in state:
                    self.value = state["value"] * self.multiplier

        actor = MyActor()
        actor.set_state({"value": 10})

        assert actor.value == 20  # Demonstrates custom behavior (10 * 2)
