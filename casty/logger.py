from __future__ import annotations

import logging
import sys
from datetime import datetime
from enum import IntEnum
from typing import Any, Protocol


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


class FormatterFn(Protocol):
    def __call__(
        self,
        *,
        time: datetime,
        level: Level,
        location: str,
        message: str,
        fields: dict[str, Any],
    ) -> str: ...


_use_colors: bool = sys.stderr.isatty()


def _color(text: str, *codes: str) -> str:
    if not _use_colors:
        return text
    return f"{''.join(codes)}{text}{Colors.RESET}"


def _format_level(level: Level) -> str:
    match level:
        case Level.DEBUG:
            return _color("[DEBUG]", Colors.MAGENTA)
        case Level.INFO:
            return _color("[INFO]", Colors.CYAN)
        case Level.WARN:
            return _color("[WARN]", Colors.YELLOW, Colors.BOLD)
        case Level.ERROR:
            return _color("[ERROR]", Colors.RED, Colors.BOLD)
        case _:
            return "???"


def _format_fields(fields: dict[str, Any]) -> str:
    if not fields:
        return ""
    parts = []
    for key, value in fields.items():
        k = _color(key, Colors.DIM)
        v = _color(repr(value), Colors.GREEN) if isinstance(value, str) else str(value)
        parts.append(f"{k}={v}")
    return " " + " ".join(parts)


class formatters:
    @staticmethod
    def verbose(
        *,
        time: datetime,
        level: Level,
        location: str,
        message: str,
        fields: dict[str, Any],
    ) -> str:
        time_str = _color(time.strftime("%H:%M:%S.%f")[:-3], Colors.DIM)
        lvl = _format_level(level)
        loc = _color(location, Colors.BLUE)
        msg = message
        if level == Level.ERROR:
            msg = _color(message, Colors.RED)
        elif level == Level.WARN:
            msg = _color(message, Colors.YELLOW)
        extra = _format_fields(fields)
        return f"{time_str} {lvl} {loc} {msg}{extra}"

    @staticmethod
    def compact(
        *,
        time: datetime,
        level: Level,
        location: str,
        message: str,
        fields: dict[str, Any],
    ) -> str:
        del location
        time_str = _color(time.strftime("%H:%M:%S"), Colors.DIM)
        lvl = _format_level(level)
        extra = _format_fields(fields)
        return f"{time_str} {lvl} {message}{extra}"

    @staticmethod
    def minimal(
        *,
        time: datetime,
        level: Level,
        location: str,
        message: str,
        fields: dict[str, Any],
    ) -> str:
        del time, location, fields
        lvl = _format_level(level)
        return f"{lvl} {message}"


_formatter: FormatterFn = formatters.verbose


_LEVEL_TO_LOGGING = {
    Level.DEBUG: logging.DEBUG,
    Level.INFO: logging.INFO,
    Level.WARN: logging.WARNING,
    Level.ERROR: logging.ERROR,
    Level.OFF: logging.CRITICAL + 1,
}

_LOGGING_TO_LEVEL = {
    logging.DEBUG: Level.DEBUG,
    logging.INFO: Level.INFO,
    logging.WARNING: Level.WARN,
    logging.ERROR: Level.ERROR,
    logging.CRITICAL: Level.ERROR,
}


class _CastyFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        location = f"{record.name}:{record.funcName}:{record.lineno}"
        level = _LOGGING_TO_LEVEL.get(record.levelno, Level.INFO)
        return _formatter(
            time=datetime.fromtimestamp(record.created),
            level=level,
            location=location,
            message=record.getMessage(),
            fields=getattr(record, "fields", {}),
        )


_logger = logging.getLogger("casty")
_logger.setLevel(logging.DEBUG)
_logger.propagate = False

_handler = logging.StreamHandler(sys.stderr)
_handler.setFormatter(_CastyFormatter())
_logger.addHandler(_handler)


def set_level(level: Level) -> None:
    _logger.setLevel(_LEVEL_TO_LOGGING[level])


def set_formatter(fn: FormatterFn) -> None:
    global _formatter
    _formatter = fn


def set_colors(enabled: bool) -> None:
    global _use_colors
    _use_colors = enabled


def debug(msg: str, **fields: Any) -> None:
    _logger.debug(msg, extra={"fields": fields}, stacklevel=2)


def info(msg: str, **fields: Any) -> None:
    _logger.info(msg, extra={"fields": fields}, stacklevel=2)


def warn(msg: str, **fields: Any) -> None:
    _logger.warning(msg, extra={"fields": fields}, stacklevel=2)


def error(msg: str, **fields: Any) -> None:
    _logger.error(msg, extra={"fields": fields}, stacklevel=2)
