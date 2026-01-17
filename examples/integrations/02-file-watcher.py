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
import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

EventType = Literal["created", "modified", "deleted", "moved"]

from watchdog.events import FileSystemEventHandler, FileSystemEvent
from watchdog.observers import Observer

from casty import Actor, ActorSystem, Context, LocalRef, on


# --- Messages ---

@dataclass
class FileEvent:
    """File system event notification."""
    event_type: EventType
    path: str
    timestamp: float


@dataclass
class ProcessFile:
    """Request to process a file (after debounce)."""
    path: str
    event_type: EventType


@dataclass
class GetStats:
    """Query processing statistics."""
    pass


@dataclass
class _DebounceTimeout:
    """Internal: debounce timer expired for a file."""
    path: str
    event_type: EventType


# --- File Event Handler (bridges watchdog to actor) ---

class ActorEventHandler(FileSystemEventHandler):
    """Watchdog event handler that forwards events to an actor.

    Filters events by extension and sends them to the actor
    for debouncing and processing.
    """

    def __init__(
        self,
        actor: LocalRef,
        loop: asyncio.AbstractEventLoop,
        extensions: set[str] | None = None
    ):
        self.actor = actor
        self.loop = loop
        self.extensions = extensions  # e.g., {".txt", ".json"}

    def _should_process(self, path: str) -> bool:
        """Check if file matches extension filter."""
        if self.extensions is None:
            return True
        return Path(path).suffix.lower() in self.extensions

    def _send_event(self, event_type: EventType, path: str):
        """Send event to actor from watchdog thread."""
        if not self._should_process(path):
            return

        event = FileEvent(
            event_type=event_type,
            path=path,
            timestamp=time.time()
        )

        # Schedule the send on the asyncio event loop (thread-safe)
        asyncio.run_coroutine_threadsafe(
            self.actor.send(event),
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


# --- Debouncer Actor ---

class FileDebouncer(Actor[FileEvent | _DebounceTimeout]):
    """Actor that debounces rapid file events.

    When multiple events arrive for the same file within the debounce
    window, only the last event is forwarded to the processor.
    """

    def __init__(self, processor: LocalRef, debounce_seconds: float = 0.5):
        self.processor = processor
        self.debounce_seconds = debounce_seconds
        self.pending: dict[str, str] = {}  # path -> scheduled task id
        self.event_types: dict[str, EventType] = {}  # path -> last event type

    @on(FileEvent)
    async def handle_file_event(self, msg: FileEvent, ctx: Context):
        path = msg.path

        # Cancel existing debounce timer for this path
        if path in self.pending:
            await ctx.cancel_schedule(self.pending[path])

        # Store the event type
        self.event_types[path] = msg.event_type

        # Schedule debounced processing
        task_id = await ctx.schedule(
            self.debounce_seconds,
            _DebounceTimeout(path, msg.event_type)
        )
        self.pending[path] = task_id

        print(f"[Debouncer] Received {msg.event_type}: {Path(path).name} (debouncing...)")

    @on(_DebounceTimeout)
    async def handle_debounce_timeout(self, msg: _DebounceTimeout, ctx: Context):
        path = msg.path

        # Clean up tracking
        self.pending.pop(path, None)
        event_type = self.event_types.pop(path, msg.event_type)

        # Forward to processor
        await self.processor.send(ProcessFile(path, event_type))


# --- File Processor Actor ---

class FileProcessor(Actor[ProcessFile | GetStats]):
    """Actor that processes file events.

    Handles created/modified/deleted events and maintains statistics.
    File processing is simulated with async delays.
    """

    def __init__(self):
        self.files_created = 0
        self.files_modified = 0
        self.files_deleted = 0
        self.files_moved = 0
        self.total_processed = 0

    @on(ProcessFile)
    async def handle_process_file(self, msg: ProcessFile, ctx: Context):
        path = Path(msg.path)

        match msg.event_type:
            case "created":
                self.files_created += 1
                await self._process_new_file(path)

            case "modified":
                self.files_modified += 1
                await self._process_modified_file(path)

            case "deleted":
                self.files_deleted += 1
                await self._handle_deleted_file(path)

            case "moved":
                self.files_moved += 1
                await self._process_moved_file(path)

        self.total_processed += 1

    async def _process_new_file(self, path: Path):
        """Process a newly created file."""
        print(f"[Processor] Processing new file: {path.name}")

        if path.exists():
            # Simulate async file processing
            await asyncio.sleep(0.1)
            content = path.read_text()
            word_count = len(content.split())
            print(f"[Processor]   -> New file has {word_count} words")
        else:
            print(f"[Processor]   -> File no longer exists")

    async def _process_modified_file(self, path: Path):
        """Process a modified file."""
        print(f"[Processor] Processing modified file: {path.name}")

        if path.exists():
            await asyncio.sleep(0.1)
            content = path.read_text()
            line_count = len(content.splitlines())
            print(f"[Processor]   -> Modified file has {line_count} lines")
        else:
            print(f"[Processor]   -> File no longer exists")

    async def _handle_deleted_file(self, path: Path):
        """Handle a deleted file."""
        print(f"[Processor] File deleted: {path.name}")
        # Could trigger cleanup, update indexes, etc.

    async def _process_moved_file(self, path: Path):
        """Process a moved/renamed file."""
        print(f"[Processor] File moved to: {path.name}")

    @on(GetStats)
    async def handle_get_stats(self, msg: GetStats, ctx: Context):
        await ctx.reply({
            "files_created": self.files_created,
            "files_modified": self.files_modified,
            "files_deleted": self.files_deleted,
            "files_moved": self.files_moved,
            "total_processed": self.total_processed,
        })


async def main():
    print("=" * 60)
    print("Casty File Watcher Example")
    print("=" * 60)
    print()

    # Create a temporary directory to watch
    watch_dir = Path(tempfile.mkdtemp(prefix="casty_watch_"))
    print(f"[Setup] Watching directory: {watch_dir}")
    print()

    async with ActorSystem() as system:
        # Create the actor pipeline: events -> debouncer -> processor
        processor = await system.spawn(FileProcessor)
        debouncer = await system.spawn(FileDebouncer, processor=processor, debounce_seconds=0.3)

        print("[System] Actors spawned: FileProcessor, FileDebouncer")

        # Setup watchdog observer
        loop = asyncio.get_running_loop()
        event_handler = ActorEventHandler(
            actor=debouncer,
            loop=loop,
            extensions={".txt", ".log", ".json"}  # Only watch these extensions
        )

        observer = Observer()
        observer.schedule(event_handler, str(watch_dir), recursive=False)
        observer.start()

        print("[Watchdog] Observer started")
        print()
        print(f"Monitoring for .txt, .log, .json files in: {watch_dir}")
        print()

        # Demonstrate with some file operations
        print("=" * 60)
        print("Demonstrating file operations...")
        print("=" * 60)
        print()

        # Create a file
        test_file = watch_dir / "hello.txt"
        test_file.write_text("Hello, World!")
        await asyncio.sleep(0.5)  # Wait for debounce

        # Modify the file (rapidly, to show debouncing)
        for i in range(3):
            test_file.write_text(f"Hello, World! Edit #{i+1}\nLine 2\nLine 3")
            await asyncio.sleep(0.1)  # Rapid modifications

        await asyncio.sleep(0.5)  # Wait for debounce

        # Create another file
        test_file2 = watch_dir / "data.json"
        test_file2.write_text('{"key": "value"}')
        await asyncio.sleep(0.5)

        # Delete a file
        test_file.unlink()
        await asyncio.sleep(0.5)

        # Create a file with non-matching extension (should be ignored)
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

        # Cleanup
        print()
        print("[Cleanup] Stopping observer...")
        observer.stop()
        observer.join()

        # Remove temp directory
        import shutil
        shutil.rmtree(watch_dir)
        print(f"[Cleanup] Removed temp directory: {watch_dir}")

        print()
        print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
