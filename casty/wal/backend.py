from __future__ import annotations

import asyncio
import struct
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, AsyncIterator

from .entry import WALEntry


class StoreBackend(ABC):
    @abstractmethod
    async def initialize(self) -> None:
        ...

    @abstractmethod
    async def append(self, entry: WALEntry) -> None:
        ...

    @abstractmethod
    def read_all(self) -> AsyncIterator[WALEntry]:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...


class InMemoryStoreBackend(StoreBackend):
    def __init__(self) -> None:
        self._entries: list[WALEntry] = []

    async def initialize(self) -> None:
        pass

    async def append(self, entry: WALEntry) -> None:
        self._entries.append(entry)

    async def read_all(self) -> AsyncIterator[WALEntry]:
        for entry in self._entries:
            yield entry

    async def close(self) -> None:
        pass


class FileStoreBackend(StoreBackend):
    def __init__(
        self,
        log_dir: Path,
        max_segment_size: int = 64 * 1024 * 1024,
    ) -> None:
        self._log_dir = log_dir
        self._max_segment_size = max_segment_size
        self._current_segment: Path | None = None
        self._file: Any | None = None
        self._segment_size = 0
        self._segment_index = 0

    async def initialize(self) -> None:
        self._log_dir.mkdir(parents=True, exist_ok=True)

        segments = sorted(self._log_dir.glob("*.wal"))
        if segments:
            last = segments[-1]
            self._segment_index = int(last.stem)

        await self._open_segment()

    async def append(self, entry: WALEntry) -> None:
        if self._segment_size >= self._max_segment_size:
            await self._rotate_segment()

        data = entry.as_bytes
        length_prefix = struct.pack(">I", len(data))
        full_data = length_prefix + data

        await asyncio.to_thread(self._file.write, full_data)
        await asyncio.to_thread(self._file.flush)
        self._segment_size += len(full_data)

    async def read_all(self) -> AsyncIterator[WALEntry]:
        segments = sorted(self._log_dir.glob("*.wal"))

        for segment in segments:
            def read_sync() -> list[WALEntry]:
                entries = []
                with open(segment, "rb") as f:
                    while True:
                        length_bytes = f.read(4)
                        if len(length_bytes) < 4:
                            break

                        entry_len = struct.unpack(">I", length_bytes)[0]
                        entry_data = f.read(entry_len)
                        if len(entry_data) < entry_len:
                            break

                        entries.append(WALEntry.from_bytes(entry_data))
                return entries

            entries = await asyncio.to_thread(read_sync)
            for entry in entries:
                yield entry

    async def _open_segment(self) -> None:
        segment_name = f"{self._segment_index:016d}.wal"
        self._current_segment = self._log_dir / segment_name
        self._file = await asyncio.to_thread(open, self._current_segment, "ab")
        self._segment_size = (
            self._current_segment.stat().st_size
            if self._current_segment.exists()
            else 0
        )

    async def _rotate_segment(self) -> None:
        if self._file:
            await asyncio.to_thread(self._file.close)
        self._segment_index += 1
        await self._open_segment()

    async def close(self) -> None:
        if self._file:
            await asyncio.to_thread(self._file.flush)
            await asyncio.to_thread(self._file.close)
            self._file = None
