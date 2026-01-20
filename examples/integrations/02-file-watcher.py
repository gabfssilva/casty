#!/usr/bin/env python
"""File Watcher Example - Actor-based File Monitoring.

Demonstrates:
- Integration with watchdog library for file system events
- Actor receiving file events (created, modified, deleted)
- Debouncing rapid events to avoid processing same file multiple times
- Asynchronous file processing
- Filtering events by file extension

This example monitors a directory for file changes and processes
files asynchronously through an actor. It includes debouncing to
handle rapid file system events (common with editors that save multiple times).

Required dependencies:
    uv add watchdog

Run with:
    uv run python examples/integrations/02-file-watcher.py

Then create/modify/delete .txt files in the ./watched directory to see events.
"""

import asyncio
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

EventType = Literal["created", "modified", "deleted", "moved"]

from watchdog.events import FileSystemEventHandler, FileSystemEvent
from watchdog.observers import Observer

from casty import actor, ActorSystem, Mailbox, LocalActorRef


@dataclass
class FileEvent:
    event_type: EventType
    path: str
    timestamp: float


@dataclass
class ProcessFile:
    path: str
    event_type: EventType


@dataclass
class GetStats:
    pass


@dataclass
class _DebounceTimeout:
    path: str
    event_type: EventType


class ActorEventHandler(FileSystemEventHandler):
    def __init__(
        self,
        actor_ref: LocalActorRef,
        loop: asyncio.AbstractEventLoop,
        extensions: set[str] | None = None
    ):
        self.actor_ref = actor_ref
        self.loop = loop
        self.extensions = extensions

    def _should_process(self, path: str) -> bool:
        if self.extensions is None:
            return True
        return Path(path).suffix.lower() in self.extensions

    def _send_event(self, event_type: EventType, path: str):
        if not self._should_process(path):
            return

        event = FileEvent(
            event_type=event_type,
            path=path,
            timestamp=time.time()
        )

        asyncio.run_coroutine_threadsafe(
            self.actor_ref.send(event),
            self.loop
        )

    def on_created(self, event: FileSystemEvent):
        if not event.is_directory:
            self._send_event("created", event.src_path)

    def on_modified(self, event: FileSystemEvent):
        if not event.is_directory:
            self._send_event("modified", event.src_path)

    def on_deleted(self, event: FileSystemEvent):
        if not event.is_directory:
            self._send_event("deleted", event.src_path)

    def on_moved(self, event: FileSystemEvent):
        if not event.is_directory:
            self._send_event("moved", event.dest_path)


DebouncerMsg = FileEvent | _DebounceTimeout


@actor
async def file_debouncer(
    processor: LocalActorRef,
    debounce_seconds: float = 0.5,
    *,
    mailbox: Mailbox[DebouncerMsg],
):
    pending: dict[str, str] = {}
    event_types: dict[str, EventType] = {}

    async for msg, ctx in mailbox:
        match msg:
            case FileEvent(event_type, path, _):
                if path in pending:
                    await ctx.cancel_schedule(pending[path])

                event_types[path] = event_type

                task_id = await ctx.schedule(
                    _DebounceTimeout(path, event_type),
                    delay=debounce_seconds,
                )
                pending[path] = task_id

                print(f"[Debouncer] Received {event_type}: {Path(path).name} (debouncing...)")

            case _DebounceTimeout(path, event_type):
                pending.pop(path, None)
                actual_event_type = event_types.pop(path, event_type)

                await processor.send(ProcessFile(path, actual_event_type))


ProcessorMsg = ProcessFile | GetStats


@actor
async def file_processor(*, mailbox: Mailbox[ProcessorMsg]):
    files_created = 0
    files_modified = 0
    files_deleted = 0
    files_moved = 0
    total_processed = 0

    async def process_new_file(path: Path):
        print(f"[Processor] Processing new file: {path.name}")
        if path.exists():
            await asyncio.sleep(0.1)
            content = path.read_text()
            word_count = len(content.split())
            print(f"[Processor]   -> New file has {word_count} words")
        else:
            print(f"[Processor]   -> File no longer exists")

    async def process_modified_file(path: Path):
        print(f"[Processor] Processing modified file: {path.name}")
        if path.exists():
            await asyncio.sleep(0.1)
            content = path.read_text()
            line_count = len(content.splitlines())
            print(f"[Processor]   -> Modified file has {line_count} lines")
        else:
            print(f"[Processor]   -> File no longer exists")

    async def handle_deleted_file(path: Path):
        print(f"[Processor] File deleted: {path.name}")

    async def process_moved_file(path: Path):
        print(f"[Processor] File moved to: {path.name}")

    async for msg, ctx in mailbox:
        match msg:
            case ProcessFile(path_str, event_type):
                path = Path(path_str)

                match event_type:
                    case "created":
                        files_created += 1
                        await process_new_file(path)
                    case "modified":
                        files_modified += 1
                        await process_modified_file(path)
                    case "deleted":
                        files_deleted += 1
                        await handle_deleted_file(path)
                    case "moved":
                        files_moved += 1
                        await process_moved_file(path)

                total_processed += 1

            case GetStats():
                await ctx.reply({
                    "files_created": files_created,
                    "files_modified": files_modified,
                    "files_deleted": files_deleted,
                    "files_moved": files_moved,
                    "total_processed": total_processed,
                })


async def main():
    print("=" * 60)
    print("Casty File Watcher Example")
    print("=" * 60)
    print()

    watch_dir = Path(tempfile.mkdtemp(prefix="casty_watch_"))
    print(f"[Setup] Watching directory: {watch_dir}")
    print()

    async with ActorSystem() as system:
        processor = await system.actor(file_processor(), name="file-processor")
        debouncer = await system.actor(
            file_debouncer(processor=processor, debounce_seconds=0.3),
            name="file-debouncer",
        )

        print("[System] Actors created: file_processor, file_debouncer")

        loop = asyncio.get_running_loop()
        event_handler = ActorEventHandler(
            actor_ref=debouncer,
            loop=loop,
            extensions={".txt", ".log", ".json"}
        )

        observer = Observer()
        observer.schedule(event_handler, str(watch_dir), recursive=False)
        observer.start()

        print("[Watchdog] Observer started")
        print()
        print(f"Monitoring for .txt, .log, .json files in: {watch_dir}")
        print()

        print("=" * 60)
        print("Demonstrating file operations...")
        print("=" * 60)
        print()

        test_file = watch_dir / "hello.txt"
        test_file.write_text("Hello, World!")
        await asyncio.sleep(0.5)

        for i in range(3):
            test_file.write_text(f"Hello, World! Edit #{i+1}\nLine 2\nLine 3")
            await asyncio.sleep(0.1)

        await asyncio.sleep(0.5)

        test_file2 = watch_dir / "data.json"
        test_file2.write_text('{"key": "value"}')
        await asyncio.sleep(0.5)

        test_file.unlink()
        await asyncio.sleep(0.5)

        ignored_file = watch_dir / "ignored.py"
        ignored_file.write_text("# This should be ignored")
        await asyncio.sleep(0.3)

        print()
        print("=" * 60)
        print("Final Statistics")
        print("=" * 60)

        stats = await processor.ask(GetStats())
        print(f"  Files created:  {stats['files_created']}")
        print(f"  Files modified: {stats['files_modified']}")
        print(f"  Files deleted:  {stats['files_deleted']}")
        print(f"  Files moved:    {stats['files_moved']}")
        print(f"  Total processed: {stats['total_processed']}")

        print()
        print("[Cleanup] Stopping observer...")
        observer.stop()
        observer.join()

        import shutil
        shutil.rmtree(watch_dir)
        print(f"[Cleanup] Removed temp directory: {watch_dir}")

        print()
        print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
