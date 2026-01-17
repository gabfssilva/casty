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
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import uuid4

from casty import Actor, ActorSystem, Context, LocalRef, on


# --- Messages ---

@dataclass
class CreateSession:
    """Create a new session for a user."""
    user_id: str


@dataclass
class GetSession:
    """Get or reactivate a session by ID."""
    session_id: str


@dataclass
class TouchSession:
    """Renew session timeout."""
    session_id: str


@dataclass
class EndSession:
    """Explicitly end a session."""
    session_id: str


@dataclass
class SetData:
    """Set data in a session."""
    key: str
    value: Any


@dataclass
class GetData:
    """Get data from a session."""
    key: str


@dataclass
class GetAllData:
    """Get all data from a session."""
    pass


@dataclass
class _SessionTimeout:
    """Internal message for session timeout."""
    session_id: str
    touch_count: int  # To verify the timeout wasn't renewed


@dataclass
class ListSessions:
    """List all sessions (active and passivated)."""
    pass


@dataclass
class SessionInfo:
    """Information about a session."""
    session_id: str
    user_id: str
    created_at: datetime
    is_active: bool


# --- Session Actor ---

type SessionMessage = SetData | GetData | GetAllData | _SessionTimeout


class Session(Actor[SessionMessage]):
    """Individual session actor.

    Holds session data and manages its own timeout.
    When inactive, the session passivates (stops) but data is preserved
    in the session manager for later reactivation.
    """

    def __init__(
        self,
        session_id: str,
        user_id: str,
        manager_ref: LocalRef,
        timeout: float = 5.0,
        initial_data: dict[str, Any] | None = None,
    ):
        self.session_id = session_id
        self.user_id = user_id
        self.manager_ref = manager_ref
        self.timeout = timeout
        self.data: dict[str, Any] = initial_data or {}
        self.touch_count = 0
        self.created_at = datetime.now()
        self._timeout_task_id: str | None = None

    async def on_start(self) -> None:
        """Start the inactivity timeout."""
        await self._schedule_timeout()
        print(f"  [Session:{self.session_id}] Started (user: {self.user_id}, timeout: {self.timeout}s)")

    async def _schedule_timeout(self) -> None:
        """Schedule the next timeout check."""
        if self._timeout_task_id:
            await self._ctx.cancel_schedule(self._timeout_task_id)
        self._timeout_task_id = await self._ctx.schedule(
            self.timeout,
            _SessionTimeout(self.session_id, self.touch_count),
        )

    async def _touch(self) -> None:
        """Renew the session timeout."""
        self.touch_count += 1
        await self._schedule_timeout()

    @on(SetData)
    async def handle_set(self, msg: SetData, ctx: Context) -> None:
        self.data[msg.key] = msg.value
        await self._touch()
        print(f"  [Session:{self.session_id}] SET {msg.key} = {msg.value}")

    @on(GetData)
    async def handle_get(self, msg: GetData, ctx: Context) -> None:
        value = self.data.get(msg.key)
        await self._touch()
        print(f"  [Session:{self.session_id}] GET {msg.key} -> {value}")
        await ctx.reply(value)

    @on(GetAllData)
    async def handle_get_all(self, msg: GetAllData, ctx: Context) -> None:
        await self._touch()
        await ctx.reply(self.data.copy())

    @on(_SessionTimeout)
    async def handle_timeout(self, msg: _SessionTimeout, ctx: Context) -> None:
        # Only passivate if the touch_count matches (no activity since scheduled)
        if msg.touch_count == self.touch_count:
            print(f"  [Session:{self.session_id}] Timeout - passivating")
            # Notify manager to passivate this session
            await self.manager_ref.send(_Passivate(self.session_id, self.data.copy()))


@dataclass
class _Passivate:
    """Internal message from session to manager to passivate."""
    session_id: str
    data: dict[str, Any]


# --- Session Manager Actor ---

@dataclass
class _SessionState:
    """Preserved session state."""
    user_id: str
    created_at: datetime
    data: dict[str, Any]
    actor_ref: LocalRef | None = None


type SessionManagerMessage = CreateSession | GetSession | TouchSession | EndSession | ListSessions | _Passivate


class SessionManager(Actor[SessionManagerMessage]):
    """Manages session lifecycle.

    Creates sessions on-demand, tracks active and passivated sessions,
    and reactivates sessions when accessed.
    """

    def __init__(self, session_timeout: float = 5.0):
        self.session_timeout = session_timeout
        self.sessions: dict[str, _SessionState] = {}

    @on(CreateSession)
    async def handle_create(self, msg: CreateSession, ctx: Context) -> None:
        session_id = str(uuid4())[:8]

        # Create session actor
        session_ref = await ctx.spawn(
            Session,
            session_id=session_id,
            user_id=msg.user_id,
            manager_ref=ctx.self_ref,
            timeout=self.session_timeout,
        )

        self.sessions[session_id] = _SessionState(
            user_id=msg.user_id,
            created_at=datetime.now(),
            data={},
            actor_ref=session_ref,
        )

        print(f"[SessionManager] Created session {session_id} for user {msg.user_id}")
        await ctx.reply(session_ref)

    @on(GetSession)
    async def handle_get(self, msg: GetSession, ctx: Context) -> None:
        state = self.sessions.get(msg.session_id)

        if state is None:
            print(f"[SessionManager] Session {msg.session_id} not found")
            await ctx.reply(None)
            return

        # Reactivate if passivated
        if state.actor_ref is None:
            print(f"[SessionManager] Reactivating session {msg.session_id}")
            session_ref = await ctx.spawn(
                Session,
                session_id=msg.session_id,
                user_id=state.user_id,
                manager_ref=ctx.self_ref,
                timeout=self.session_timeout,
                initial_data=state.data,
            )
            state.actor_ref = session_ref

        await ctx.reply(state.actor_ref)

    @on(TouchSession)
    async def handle_touch(self, msg: TouchSession, ctx: Context) -> None:
        state = self.sessions.get(msg.session_id)
        if state and state.actor_ref:
            # Send a no-op message to trigger touch
            await state.actor_ref.send(GetAllData())

    @on(EndSession)
    async def handle_end(self, msg: EndSession, ctx: Context) -> None:
        state = self.sessions.pop(msg.session_id, None)
        if state:
            if state.actor_ref:
                await ctx.stop_child(state.actor_ref)
            print(f"[SessionManager] Ended session {msg.session_id}")

    @on(_Passivate)
    async def handle_passivate(self, msg: _Passivate, ctx: Context) -> None:
        state = self.sessions.get(msg.session_id)
        if state:
            # Save data and mark as passivated
            state.data = msg.data
            if state.actor_ref:
                await ctx.stop_child(state.actor_ref)
            state.actor_ref = None
            print(f"[SessionManager] Passivated session {msg.session_id} (data preserved)")

    @on(ListSessions)
    async def handle_list(self, msg: ListSessions, ctx: Context) -> None:
        sessions = [
            SessionInfo(
                session_id=sid,
                user_id=state.user_id,
                created_at=state.created_at,
                is_active=state.actor_ref is not None,
            )
            for sid, state in self.sessions.items()
        ]
        await ctx.reply(sessions)


# --- Main ---

async def main():
    print("=" * 60)
    print("Casty Session Manager Example")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        # Create session manager with 2-second timeout for demo
        manager = await system.spawn(SessionManager, session_timeout=2.0)
        print("Session manager created (timeout: 2s)")
        print()

        # Create sessions
        print("--- Creating sessions ---")
        session1: LocalRef = await manager.ask(CreateSession("alice"))
        session2: LocalRef = await manager.ask(CreateSession("bob"))
        await asyncio.sleep(0.1)
        print()

        # Use sessions
        print("--- Using sessions ---")
        await session1.send(SetData("cart", ["item1", "item2"]))
        await session1.send(SetData("prefs", {"theme": "dark"}))
        await session2.send(SetData("cart", ["item3"]))
        await asyncio.sleep(0.1)
        print()

        # List sessions (both active)
        print("--- Listing sessions ---")
        sessions: list[SessionInfo] = await manager.ask(ListSessions())
        for s in sessions:
            status = "active" if s.is_active else "passivated"
            print(f"  {s.session_id}: user={s.user_id}, status={status}")
        print()

        # Wait for sessions to passivate
        print("--- Waiting 2.5s for passivation ---")
        await asyncio.sleep(2.5)
        print()

        # List sessions (both should be passivated)
        print("--- Listing sessions (after passivation) ---")
        sessions = await manager.ask(ListSessions())
        for s in sessions:
            status = "active" if s.is_active else "passivated"
            print(f"  {s.session_id}: user={s.user_id}, status={status}")
        print()

        # Reactivate session1 and verify data was preserved
        print("--- Reactivating session1 ---")
        session1_reactivated: LocalRef = await manager.ask(GetSession(sessions[0].session_id))
        data = await session1_reactivated.ask(GetAllData())
        print(f"  Session data preserved: {data}")
        print()

        # List sessions again
        print("--- Listing sessions (after reactivation) ---")
        sessions = await manager.ask(ListSessions())
        for s in sessions:
            status = "active" if s.is_active else "passivated"
            print(f"  {s.session_id}: user={s.user_id}, status={status}")
        print()

        # End a session
        print("--- Ending session2 ---")
        await manager.send(EndSession(sessions[1].session_id))
        await asyncio.sleep(0.1)
        print()

        # Final list
        print("--- Final session list ---")
        sessions = await manager.ask(ListSessions())
        for s in sessions:
            status = "active" if s.is_active else "passivated"
            print(f"  {s.session_id}: user={s.user_id}, status={status}")

    print()
    print("=" * 60)
    print("Session manager shut down")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
