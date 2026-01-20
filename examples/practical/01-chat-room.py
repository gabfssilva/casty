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

from casty import actor, ActorSystem, Mailbox, LocalActorRef


@dataclass
class Join:
    username: str
    user_ref: LocalActorRef


@dataclass
class Leave:
    username: str


@dataclass
class SendMessage:
    username: str
    text: str


@dataclass
class ListUsers:
    pass


@dataclass
class ChatMessage:
    sender: str
    text: str
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class SystemNotification:
    text: str
    timestamp: datetime = field(default_factory=datetime.now)


ChatRoomMsg = Join | Leave | SendMessage | ListUsers
UserMsg = ChatMessage | SystemNotification


@actor
async def chat_room(room_name: str = "general", *, mailbox: Mailbox[ChatRoomMsg]):
    participants: dict[str, LocalActorRef] = {}

    async def broadcast(message: ChatMessage | SystemNotification, exclude: str | None = None):
        for username, ref in participants.items():
            if username != exclude:
                await ref.send(message)

    async for msg, ctx in mailbox:
        match msg:
            case Join(username, user_ref):
                if username in participants:
                    print(f"[{room_name}] User '{username}' is already in the room")
                    continue

                participants[username] = user_ref
                print(f"[{room_name}] User '{username}' joined ({len(participants)} users)")
                await broadcast(SystemNotification(f"{username} has joined the room"), exclude=username)

            case Leave(username):
                if username not in participants:
                    continue

                del participants[username]
                print(f"[{room_name}] User '{username}' left ({len(participants)} users)")
                await broadcast(SystemNotification(f"{username} has left the room"))

            case SendMessage(username, text):
                if username not in participants:
                    print(f"[{room_name}] User '{username}' is not in the room")
                    continue

                await broadcast(ChatMessage(sender=username, text=text), exclude=username)

            case ListUsers():
                await ctx.reply(list(participants.keys()))


@actor
async def user(username: str, *, mailbox: Mailbox[UserMsg]):
    messages_received: list[str] = []

    async for msg, ctx in mailbox:
        match msg:
            case ChatMessage(sender, text, timestamp):
                formatted = f"[{timestamp.strftime('%H:%M:%S')}] {sender}: {text}"
                messages_received.append(formatted)
                print(f"  {username} received: {formatted}")

            case SystemNotification(text, timestamp):
                formatted = f"[{timestamp.strftime('%H:%M:%S')}] * {text}"
                messages_received.append(formatted)
                print(f"  {username} received: {formatted}")


async def main():
    print("=" * 60)
    print("Casty Chat Room Example")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        room = await system.actor(chat_room(room_name="python-devs"), name="chat-room-python-devs")
        print("Chat room 'python-devs' created")
        print()

        alice_ref = await system.actor(user(username="alice"), name="user-alice")
        bob_ref = await system.actor(user(username="bob"), name="user-bob")
        charlie_ref = await system.actor(user(username="charlie"), name="user-charlie")

        print("--- Users joining ---")
        await room.send(Join("alice", alice_ref))
        await room.send(Join("bob", bob_ref))
        await room.send(Join("charlie", charlie_ref))
        await asyncio.sleep(0.1)
        print()

        print("--- Listing users ---")
        users = await room.ask(ListUsers())
        print(f"Online users: {users}")
        print()

        print("--- Sending messages ---")
        await room.send(SendMessage("alice", "Hello everyone!"))
        await asyncio.sleep(0.05)

        await room.send(SendMessage("bob", "Hi Alice! How are you?"))
        await asyncio.sleep(0.05)

        await room.send(SendMessage("charlie", "Hey folks!"))
        await asyncio.sleep(0.1)
        print()

        print("--- User leaving ---")
        await room.send(Leave("bob"))
        await asyncio.sleep(0.1)
        print()

        print("--- More messages ---")
        await room.send(SendMessage("alice", "Bye Bob!"))
        await asyncio.sleep(0.1)
        print()

        users = await room.ask(ListUsers())
        print(f"Final online users: {users}")

    print()
    print("=" * 60)
    print("Chat room closed")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
