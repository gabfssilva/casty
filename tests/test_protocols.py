"""Tests for Casty protocols."""

import pytest
from typing import runtime_checkable, Protocol


class TestRefProtocol:
    """Tests for Ref protocol."""

    def test_ref_protocol_exists(self):
        from casty.protocols import ActorRef
        assert hasattr(ActorRef, '__protocol_attrs__')

    def test_ref_protocol_is_runtime_checkable(self):
        from casty.protocols import ActorRef
        # runtime_checkable protocols can be used with isinstance
        assert getattr(ActorRef, '_is_runtime_protocol', False)

    def test_ref_protocol_has_send(self):
        from casty.protocols import ActorRef
        assert 'send' in dir(ActorRef)

    def test_ref_protocol_has_ask(self):
        from casty.protocols import ActorRef
        assert 'ask' in dir(ActorRef)

    def test_ref_protocol_has_rshift(self):
        from casty.protocols import ActorRef
        assert '__rshift__' in dir(ActorRef)

    def test_ref_protocol_has_lshift(self):
        from casty.protocols import ActorRef
        assert '__lshift__' in dir(ActorRef)

    def test_local_ref_implements_ref_protocol(self):
        from casty.protocols import ActorRef
        from casty.actor import LocalActorRef
        # LocalRef should have all methods required by Ref
        assert hasattr(LocalActorRef, 'send')
        assert hasattr(LocalActorRef, 'ask')
        assert hasattr(LocalActorRef, '__rshift__')
        assert hasattr(LocalActorRef, '__lshift__')

    def test_clustered_ref_implements_ref_protocol(self):
        from casty.protocols import ActorRef
        from casty.cluster.clustered_ref import ClusteredActorRef
        # ClusteredRef should have all methods required by Ref
        assert hasattr(ClusteredActorRef, 'send')
        assert hasattr(ClusteredActorRef, 'ask')
        assert hasattr(ClusteredActorRef, '__rshift__')
        assert hasattr(ClusteredActorRef, '__lshift__')


class TestSystemProtocol:
    """Tests for System protocol."""

    def test_system_protocol_exists(self):
        from casty.protocols import System
        assert hasattr(System, '__protocol_attrs__')

    def test_system_protocol_is_runtime_checkable(self):
        from casty.protocols import System
        assert getattr(System, '_is_runtime_protocol', False)

    def test_system_protocol_has_spawn(self):
        from casty.protocols import System
        assert 'spawn' in dir(System)

    def test_system_protocol_has_stop(self):
        from casty.protocols import System
        assert 'stop' in dir(System)

    def test_system_protocol_has_schedule(self):
        from casty.protocols import System
        assert 'schedule' in dir(System)

    def test_system_protocol_has_cancel_schedule(self):
        from casty.protocols import System
        assert 'cancel_schedule' in dir(System)

    def test_system_protocol_has_tick(self):
        from casty.protocols import System
        assert 'tick' in dir(System)

    def test_system_protocol_has_cancel_tick(self):
        from casty.protocols import System
        assert 'cancel_tick' in dir(System)

    def test_system_protocol_has_shutdown(self):
        from casty.protocols import System
        assert 'shutdown' in dir(System)

    def test_system_protocol_has_start(self):
        from casty.protocols import System
        assert 'start' in dir(System)

    def test_system_protocol_has_context_manager(self):
        from casty.protocols import System
        assert '__aenter__' in dir(System)
        assert '__aexit__' in dir(System)

    def test_local_system_implements_system_protocol(self):
        from casty.protocols import System
        from casty.system import LocalSystem
        # LocalSystem should have all methods required by System
        assert hasattr(LocalSystem, 'spawn')
        assert hasattr(LocalSystem, 'stop')
        assert hasattr(LocalSystem, 'schedule')
        assert hasattr(LocalSystem, 'cancel_schedule')
        assert hasattr(LocalSystem, 'tick')
        assert hasattr(LocalSystem, 'cancel_tick')
        assert hasattr(LocalSystem, 'shutdown')
        assert hasattr(LocalSystem, 'start')
        assert hasattr(LocalSystem, '__aenter__')
        assert hasattr(LocalSystem, '__aexit__')


class TestProtocolIntegration:
    """Integration tests for protocols with actual instances."""

    @pytest.mark.asyncio
    async def test_local_ref_isinstance_check(self):
        from casty.protocols import ActorRef
        from casty.actor_system import ActorSystem
        from casty import Actor, Context, on
        from dataclasses import dataclass

        @dataclass
        class Ping:
            pass

        class PingActor(Actor[Ping]):
            @on(Ping)
            async def handle(self, msg: Ping, ctx: Context):
                await ctx.reply("pong")

        async with ActorSystem.local() as system:
            ref = await system.spawn(PingActor)
            # runtime_checkable protocols can be used with isinstance
            assert isinstance(ref, ActorRef)

    @pytest.mark.asyncio
    async def test_actor_system_isinstance_check(self):
        from casty.protocols import System
        from casty.actor_system import ActorSystem

        async with ActorSystem.local() as system:
            # runtime_checkable protocols can be used with isinstance
            assert isinstance(system, System)


class TestLocalSystemRename:
    """Tests for LocalSystem (renamed from ActorSystem)."""

    def test_local_system_exists(self):
        from casty.system import LocalSystem
        assert LocalSystem is not None

    def test_local_system_is_old_actor_system(self):
        from casty.system import LocalSystem
        assert hasattr(LocalSystem, 'spawn')
        assert hasattr(LocalSystem, 'shutdown')


class TestClusteredSystemRename:
    """Tests for ClusteredSystem (renamed from ClusteredActorSystem)."""

    def test_clustered_system_exists(self):
        from casty.cluster.clustered_system import ClusteredSystem
        assert ClusteredSystem is not None

    def test_clustered_system_inherits_local_system(self):
        from casty.cluster.clustered_system import ClusteredSystem
        from casty.system import LocalSystem
        assert issubclass(ClusteredSystem, LocalSystem)


class TestActorSystemDecorator:
    """Tests for ActorSystem decorator."""

    @pytest.mark.asyncio
    async def test_actor_system_local_factory(self):
        from casty.actor_system import ActorSystem as ActorSystemDecorator

        async with ActorSystemDecorator.local() as system:
            assert system is not None
            assert hasattr(system, 'spawn')
            assert hasattr(system, '_inner')

    @pytest.mark.asyncio
    async def test_actor_system_decorator_delegates_to_inner(self):
        from casty.actor_system import ActorSystem as ActorSystemDecorator
        from casty.system import LocalSystem

        inner = LocalSystem()
        decorator = ActorSystemDecorator(inner)
        assert decorator._inner is inner

    @pytest.mark.asyncio
    async def test_actor_system_decorator_spawn_works(self):
        from casty.actor_system import ActorSystem as ActorSystemDecorator
        from casty import Actor, Context, on
        from casty.protocols import ActorRef
        from dataclasses import dataclass

        @dataclass
        class Ping:
            pass

        class PingActor(Actor[Ping]):
            @on(Ping)
            async def handle(self, msg: Ping, ctx: Context):
                await ctx.reply("pong")

        async with ActorSystemDecorator.local() as system:
            ref = await system.spawn(PingActor)
            assert ref is not None
            assert isinstance(ref, ActorRef)
            result = await ref.ask(Ping())
            assert result == "pong"

    @pytest.mark.asyncio
    async def test_actor_system_decorator_stop_works(self):
        from casty.actor_system import ActorSystem as ActorSystemDecorator
        from casty import Actor, Context, on
        from dataclasses import dataclass

        @dataclass
        class Msg:
            pass

        class SimpleActor(Actor[Msg]):
            @on(Msg)
            async def handle(self, msg: Msg, ctx: Context):
                pass

        async with ActorSystemDecorator.local() as system:
            ref = await system.spawn(SimpleActor)
            result = await system.stop(ref)
            assert result is True

    def test_actor_system_decorator_has_all_methods(self):
        from casty.actor_system import ActorSystem as ActorSystemDecorator
        assert hasattr(ActorSystemDecorator, 'spawn')
        assert hasattr(ActorSystemDecorator, 'stop')
        assert hasattr(ActorSystemDecorator, 'schedule')
        assert hasattr(ActorSystemDecorator, 'cancel_schedule')
        assert hasattr(ActorSystemDecorator, 'tick')
        assert hasattr(ActorSystemDecorator, 'cancel_tick')
        assert hasattr(ActorSystemDecorator, 'shutdown')
        assert hasattr(ActorSystemDecorator, 'start')
        assert hasattr(ActorSystemDecorator, '__aenter__')
        assert hasattr(ActorSystemDecorator, '__aexit__')

    def test_actor_system_decorator_has_factory_methods(self):
        from casty.actor_system import ActorSystem as ActorSystemDecorator
        assert hasattr(ActorSystemDecorator, 'local')
        assert hasattr(ActorSystemDecorator, 'clustered')
        assert callable(ActorSystemDecorator.local)
        assert callable(ActorSystemDecorator.clustered)

    def test_actor_system_clustered_factory_creates_instance(self):
        from casty.actor_system import ActorSystem as ActorSystemDecorator
        from casty.cluster.clustered_system import ClusteredSystem

        system = ActorSystemDecorator.clustered(port=0)
        assert system is not None
        assert isinstance(system._inner, ClusteredSystem)


class TestActorSystemIntegration:
    """Integration tests for ActorSystem.local() and ActorSystem.clustered()."""

    @pytest.mark.asyncio
    async def test_actor_system_local_spawn_and_ask(self):
        from casty.actor_system import ActorSystem
        from casty import Actor, Context, on
        from dataclasses import dataclass

        @dataclass
        class Ping:
            pass

        @dataclass
        class Pong:
            pass

        class PingActor(Actor[Ping]):
            @on(Ping)
            async def handle(self, msg: Ping, ctx: Context):
                await ctx.reply(Pong())

        async with ActorSystem.local() as system:
            ref = await system.spawn(PingActor)
            result = await ref.ask(Ping())
            assert isinstance(result, Pong)

    @pytest.mark.asyncio
    async def test_actor_system_clustered_spawn(self):
        from casty.actor_system import ActorSystem
        from casty import Actor, Context, on
        from dataclasses import dataclass

        @dataclass
        class Ping:
            pass

        @dataclass
        class Pong:
            pass

        class SimpleActor(Actor[Ping]):
            @on(Ping)
            async def handle(self, msg: Ping, ctx: Context):
                await ctx.reply(Pong())

        async with ActorSystem.clustered(port=0) as system:
            ref = await system.spawn(SimpleActor)
            result = await ref.ask(Ping())
            assert isinstance(result, Pong)


class TestPublicAPIExports:
    """Tests for public API exports from casty package."""

    def test_public_api_exports(self):
        import casty

        # Protocols
        assert hasattr(casty, 'System')
        assert hasattr(casty, 'ActorRef')

        # Decorator
        assert hasattr(casty, 'ActorSystem')

        # Implementations
        assert hasattr(casty, 'LocalSystem')
        assert hasattr(casty, 'LocalActorRef')
        assert hasattr(casty, 'ClusteredSystem')
        assert hasattr(casty, 'ClusteredActorRef')

        # Verify ActorSystem is the decorator, not the alias
        assert hasattr(casty.ActorSystem, 'local')
        assert hasattr(casty.ActorSystem, 'clustered')
