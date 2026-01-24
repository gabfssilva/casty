from __future__ import annotations

import sys
from datetime import datetime
from enum import IntEnum
from typing import Any


class Level(IntEnum):
    DEBUG = 0
    INFO = 1
    WARN = 2
    ERROR = 3
    OFF = 4


class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"

    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"

    BG_RED = "\033[41m"
    BG_YELLOW = "\033[43m"
    BG_BLUE = "\033[44m"
    BG_MAGENTA = "\033[45m"


_level: Level = Level.DEBUG
_use_colors: bool = sys.stdout.isatty()


def set_level(level: Level) -> None:
    global _level
    _level = level


def set_colors(enabled: bool) -> None:
    global _use_colors
    _use_colors = enabled


def _color(text: str, *codes: str) -> str:
    if not _use_colors:
        return text
    return f"{''.join(codes)}{text}{Colors.RESET}"


def _format_time() -> str:
    now = datetime.now()
    time_str = now.strftime("%H:%M:%S.%f")[:-3]
    return _color(time_str, Colors.DIM)


def _format_level(level: Level) -> str:
    match level:
        case Level.DEBUG:
            return _color("DBG", Colors.MAGENTA)
        case Level.INFO:
            return _color("INF", Colors.CYAN)
        case Level.WARN:
            return _color("WRN", Colors.YELLOW, Colors.BOLD)
        case Level.ERROR:
            return _color("ERR", Colors.RED, Colors.BOLD)
        case _:
            return "???"


def _format_context(ctx: str | None) -> str:
    if not ctx:
        return ""
    return _color(f"[{ctx}]", Colors.BLUE) + " "


def _format_message(msg: str, level: Level) -> str:
    match level:
        case Level.ERROR:
            return _color(msg, Colors.RED)
        case Level.WARN:
            return _color(msg, Colors.YELLOW)
        case _:
            return msg


def _format_fields(fields: dict[str, Any]) -> str:
    if not fields:
        return ""
    parts = []
    for key, value in fields.items():
        k = _color(key, Colors.DIM)
        v = _color(repr(value), Colors.GREEN) if isinstance(value, str) else str(value)
        parts.append(f"{k}={v}")
    return " " + " ".join(parts)


def _log(level: Level, msg: str, ctx: str | None = None, **fields: Any) -> None:
    if level < _level:
        return

    time = _format_time()
    lvl = _format_level(level)
    context = _format_context(ctx)
    message = _format_message(msg, level)
    extra = _format_fields(fields)

    line = f"{time} {lvl} {context}{message}{extra}"
    print(line, file=sys.stderr, flush=True)


def debug(msg: str, ctx: str | None = None, **fields: Any) -> None:
    _log(Level.DEBUG, msg, ctx, **fields)


def info(msg: str, ctx: str | None = None, **fields: Any) -> None:
    _log(Level.INFO, msg, ctx, **fields)


def warn(msg: str, ctx: str | None = None, **fields: Any) -> None:
    _log(Level.WARN, msg, ctx, **fields)


def error(msg: str, ctx: str | None = None, **fields: Any) -> None:
    _log(Level.ERROR, msg, ctx, **fields)
