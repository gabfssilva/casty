"""Chat Room Example - Pub/Sub Pattern.

Demonstrates:
- Managing a set of participants (subscribers)
- Broadcasting messages to all participants
- Join/Leave lifecycle
- Listing online users

Run with:
    uv run python examples/practical/01-chat-room.py
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime

from casty import Actor, ActorSystem, Context, LocalActorRef, on


# --- Messages ---

@dataclass
class Join:
    """A user joins the chat room."""
    username: str
    user_ref: LocalActorRef


@dataclass
class Leave:
    """A user leaves the chat room."""
    username: str


@dataclass
class SendMessage:
    """Send a message to the room."""
    username: str
    text: str


@dataclass
class ListUsers:
    """Request list of online users."""
    pass


@dataclass
class ChatMessage:
    """Message delivered to participants."""
    sender: str
    text: str
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class SystemNotification:
    """System notification (join/leave)."""
    text: str
    timestamp: datetime = field(default_factory=datetime.now)


# --- Chat Room Actor ---

type ChatRoomMessage = Join | Leave | SendMessage | ListUsers


class ChatRoom(Actor[ChatRoomMessage]):
    """Chat room that manages participants and broadcasts messages.

    Participants are stored as a mapping from username to their actor ref.
    When a message is sent, it's broadcast to all participants.
    """

    def __init__(self, room_name: str = "general"):
        self.room_name = room_name
        self.participants: dict[str, LocalActorRef] = {}

    @on(Join)
    async def handle_join(self, msg: Join, ctx: Context) -> None:
        if msg.username in self.participants:
            print(f"[{self.room_name}] User '{msg.username}' is already in the room")
            return

        self.participants[msg.username] = msg.user_ref
        print(f"[{self.room_name}] User '{msg.username}' joined ({len(self.participants)} users)")

        notification = SystemNotification(f"{msg.username} has joined the room")
        await self._broadcast(notification, exclude=msg.username)

    @on(Leave)
    async def handle_leave(self, msg: Leave, ctx: Context) -> None:
        if msg.username not in self.participants:
            return

        del self.participants[msg.username]
        print(f"[{self.room_name}] User '{msg.username}' left ({len(self.participants)} users)")

        notification = SystemNotification(f"{msg.username} has left the room")
        await self._broadcast(notification)

    @on(SendMessage)
    async def handle_send(self, msg: SendMessage, ctx: Context) -> None:
        if msg.username not in self.participants:
            print(f"[{self.room_name}] User '{msg.username}' is not in the room")
            return

        chat_msg = ChatMessage(sender=msg.username, text=msg.text)
        await self._broadcast(chat_msg, exclude=msg.username)

    @on(ListUsers)
    async def handle_list(self, msg: ListUsers, ctx: Context) -> None:
        users = list(self.participants.keys())
        await ctx.reply(users)

    async def _broadcast(self, message: ChatMessage | SystemNotification, exclude: str | None = None) -> None:
        """Broadcast a message to all participants except the excluded one."""
        for username, ref in self.participants.items():
            if username != exclude:
                await ref.send(message)


# --- User Actor ---

type UserMessage = ChatMessage | SystemNotification


class User(Actor[UserMessage]):
    """User actor that receives chat messages and notifications."""

    def __init__(self, username: str):
        self.username = username
        self.messages_received: list[str] = []

    @on(ChatMessage)
    async def handle_chat(self, msg: ChatMessage, ctx: Context) -> None:
        formatted = f"[{msg.timestamp.strftime('%H:%M:%S')}] {msg.sender}: {msg.text}"
        self.messages_received.append(formatted)
        print(f"  {self.username} received: {formatted}")

    @on(SystemNotification)
    async def handle_notification(self, msg: SystemNotification, ctx: Context) -> None:
        formatted = f"[{msg.timestamp.strftime('%H:%M:%S')}] * {msg.text}"
        self.messages_received.append(formatted)
        print(f"  {self.username} received: {formatted}")


# --- Main ---

async def main():
    print("=" * 60)
    print("Casty Chat Room Example")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        # Create chat room
        room = await system.actor(ChatRoom, name="chat-room-python-devs", room_name="python-devs")
        print("Chat room 'python-devs' created")
        print()

        # Create users
        alice_ref = await system.actor(User, name="user-alice", username="alice")
        bob_ref = await system.actor(User, name="user-bob", username="bob")
        charlie_ref = await system.actor(User, name="user-charlie", username="charlie")

        # Users join the room
        print("--- Users joining ---")
        await room.send(Join("alice", alice_ref))
        await room.send(Join("bob", bob_ref))
        await room.send(Join("charlie", charlie_ref))
        await asyncio.sleep(0.1)
        print()

        # List users
        print("--- Listing users ---")
        users = await room.ask(ListUsers())
        print(f"Online users: {users}")
        print()

        # Send messages
        print("--- Sending messages ---")
        await room.send(SendMessage("alice", "Hello everyone!"))
        await asyncio.sleep(0.05)

        await room.send(SendMessage("bob", "Hi Alice! How are you?"))
        await asyncio.sleep(0.05)

        await room.send(SendMessage("charlie", "Hey folks!"))
        await asyncio.sleep(0.1)
        print()

        # User leaves
        print("--- User leaving ---")
        await room.send(Leave("bob"))
        await asyncio.sleep(0.1)
        print()

        # More messages
        print("--- More messages ---")
        await room.send(SendMessage("alice", "Bye Bob!"))
        await asyncio.sleep(0.1)
        print()

        # Final user list
        users = await room.ask(ListUsers())
        print(f"Final online users: {users}")

    print()
    print("=" * 60)
    print("Chat room closed")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
