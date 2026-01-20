"""Session Manager Example - Entity Lifecycle Management.

Demonstrates:
- Session manager creating session actors on-demand
- Touch to renew session timeout
- Passivation after inactivity (actor stops)
- Reactivation on-demand (actor recreated)
- Session state persistence across passivation

Run with:
    uv run python examples/practical/04-session-manager.py
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import uuid4

from casty import actor, ActorSystem, Mailbox, LocalActorRef


@dataclass
class CreateSession:
    user_id: str


@dataclass
class GetSession:
    session_id: str


@dataclass
class TouchSession:
    session_id: str


@dataclass
class EndSession:
    session_id: str


@dataclass
class SetData:
    key: str
    value: Any


@dataclass
class GetData:
    key: str


@dataclass
class GetAllData:
    pass


@dataclass
class _SessionTimeout:
    session_id: str
    touch_count: int


@dataclass
class ListSessions:
    pass


@dataclass
class SessionInfo:
    session_id: str
    user_id: str
    created_at: datetime
    is_active: bool


@dataclass
class _Passivate:
    session_id: str
    data: dict[str, Any]


@dataclass
class _SessionState:
    user_id: str
    created_at: datetime
    data: dict[str, Any]
    actor_ref: LocalActorRef | None = None


SessionMsg = SetData | GetData | GetAllData | _SessionTimeout
SessionManagerMsg = CreateSession | GetSession | TouchSession | EndSession | ListSessions | _Passivate


@actor
async def session(
    session_id: str,
    user_id: str,
    manager_ref: LocalActorRef[SessionManagerMsg],
    timeout: float = 5.0,
    initial_data: dict[str, Any] | None = None,
    *,
    mailbox: Mailbox[SessionMsg],
):
    data: dict[str, Any] = initial_data or {}
    touch_count = 0

    await mailbox._self_ref.send(_SessionTimeout(session_id, touch_count))
    print(f"  [Session:{session_id}] Started (user: {user_id}, timeout: {timeout}s)")

    async def schedule_timeout():
        nonlocal touch_count
        await mailbox._self_ref.send(_SessionTimeout(session_id, touch_count))

    async def touch():
        nonlocal touch_count
        touch_count += 1
        await schedule_timeout()

    async for msg, ctx in mailbox:
        match msg:
            case SetData(key, value):
                data[key] = value
                await touch()
                print(f"  [Session:{session_id}] SET {key} = {value}")

            case GetData(key):
                value = data.get(key)
                await touch()
                print(f"  [Session:{session_id}] GET {key} -> {value}")
                await ctx.reply(value)

            case GetAllData():
                await touch()
                await ctx.reply(data.copy())

            case _SessionTimeout(sid, tc):
                if tc == touch_count:
                    print(f"  [Session:{session_id}] Timeout - passivating")
                    await manager_ref.send(_Passivate(session_id, data.copy()))


@actor
async def session_manager(session_timeout: float = 5.0, *, mailbox: Mailbox[SessionManagerMsg]):
    sessions: dict[str, _SessionState] = {}

    async for msg, ctx in mailbox:
        match msg:
            case CreateSession(user_id):
                session_id = str(uuid4())[:8]

                session_ref = await ctx.actor(
                    session(
                        session_id=session_id,
                        user_id=user_id,
                        manager_ref=ctx._self_ref,
                        timeout=session_timeout,
                    ),
                    name=f"session-{session_id}",
                )

                sessions[session_id] = _SessionState(
                    user_id=user_id,
                    created_at=datetime.now(),
                    data={},
                    actor_ref=session_ref,
                )

                print(f"[SessionManager] Created session {session_id} for user {user_id}")
                await ctx.reply(session_ref)

            case GetSession(session_id):
                state = sessions.get(session_id)

                if state is None:
                    print(f"[SessionManager] Session {session_id} not found")
                    await ctx.reply(None)
                    continue

                if state.actor_ref is None:
                    print(f"[SessionManager] Reactivating session {session_id}")
                    session_ref = await ctx.actor(
                        session(
                            session_id=session_id,
                            user_id=state.user_id,
                            manager_ref=ctx._self_ref,
                            timeout=session_timeout,
                            initial_data=state.data,
                        ),
                        name=f"session-{session_id}",
                    )
                    state.actor_ref = session_ref

                await ctx.reply(state.actor_ref)

            case TouchSession(session_id):
                state = sessions.get(session_id)
                if state and state.actor_ref:
                    await state.actor_ref.send(GetAllData())

            case EndSession(session_id):
                state = sessions.pop(session_id, None)
                if state:
                    print(f"[SessionManager] Ended session {session_id}")

            case _Passivate(session_id, data):
                state = sessions.get(session_id)
                if state:
                    state.data = data
                    state.actor_ref = None
                    print(f"[SessionManager] Passivated session {session_id} (data preserved)")

            case ListSessions():
                session_list = [
                    SessionInfo(
                        session_id=sid,
                        user_id=state.user_id,
                        created_at=state.created_at,
                        is_active=state.actor_ref is not None,
                    )
                    for sid, state in sessions.items()
                ]
                await ctx.reply(session_list)


async def main():
    print("=" * 60)
    print("Casty Session Manager Example")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        manager = await system.actor(session_manager(session_timeout=2.0), name="session-manager")
        print("Session manager created (timeout: 2s)")
        print()

        print("--- Creating sessions ---")
        session1: LocalActorRef = await manager.ask(CreateSession("alice"))
        session2: LocalActorRef = await manager.ask(CreateSession("bob"))
        await asyncio.sleep(0.1)
        print()

        print("--- Using sessions ---")
        await session1.send(SetData("cart", ["item1", "item2"]))
        await session1.send(SetData("prefs", {"theme": "dark"}))
        await session2.send(SetData("cart", ["item3"]))
        await asyncio.sleep(0.1)
        print()

        print("--- Listing sessions ---")
        sessions_list: list[SessionInfo] = await manager.ask(ListSessions())
        for s in sessions_list:
            status = "active" if s.is_active else "passivated"
            print(f"  {s.session_id}: user={s.user_id}, status={status}")
        print()

        print("--- Waiting 2.5s for passivation ---")
        await asyncio.sleep(2.5)
        print()

        print("--- Listing sessions (after passivation) ---")
        sessions_list = await manager.ask(ListSessions())
        for s in sessions_list:
            status = "active" if s.is_active else "passivated"
            print(f"  {s.session_id}: user={s.user_id}, status={status}")
        print()

        print("--- Reactivating session1 ---")
        session1_reactivated: LocalActorRef = await manager.ask(GetSession(sessions_list[0].session_id))
        data = await session1_reactivated.ask(GetAllData())
        print(f"  Session data preserved: {data}")
        print()

        print("--- Listing sessions (after reactivation) ---")
        sessions_list = await manager.ask(ListSessions())
        for s in sessions_list:
            status = "active" if s.is_active else "passivated"
            print(f"  {s.session_id}: user={s.user_id}, status={status}")
        print()

        print("--- Ending session2 ---")
        await manager.send(EndSession(sessions_list[1].session_id))
        await asyncio.sleep(0.1)
        print()

        print("--- Final session list ---")
        sessions_list = await manager.ask(ListSessions())
        for s in sessions_list:
            status = "active" if s.is_active else "passivated"
            print(f"  {s.session_id}: user={s.user_id}, status={status}")

    print()
    print("=" * 60)
    print("Session manager shut down")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
