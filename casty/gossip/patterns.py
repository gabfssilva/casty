"""
Pattern matching utilities for gossip subscriptions.

Supports glob-style patterns:
- "*" matches any single path segment
- "**" matches zero or more path segments
- Exact strings match exactly

Examples:
    "cluster/*" matches "cluster/leader" but not "cluster/nodes/node-1"
    "cluster/**" matches "cluster/leader" and "cluster/nodes/node-1"
    "cluster/nodes/*" matches "cluster/nodes/node-1"
"""

import re


def compile_pattern(pattern: str) -> re.Pattern:
    """Compile a glob pattern to a regex.

    Args:
        pattern: Glob-style pattern with * and ** support.

    Returns:
        Compiled regex pattern.

    Example:
        p = compile_pattern("cluster/**")
        p.match("cluster/nodes/node-1")  # Match
        p.match("app/config")  # No match
    """
    # Split by path separator
    parts = pattern.split("/")
    regex_parts = []

    for part in parts:
        if part == "**":
            # Match zero or more path segments
            regex_parts.append(r"(?:[^/]+(?:/[^/]+)*)?")
        elif part == "*":
            # Single * matches exactly one segment (no slashes)
            regex_parts.append(r"[^/]+")
        elif "*" in part or "?" in part:
            # Glob pattern within a segment - convert * to [^/]* and ? to [^/]
            segment_pattern = ""
            for char in part:
                if char == "*":
                    segment_pattern += r"[^/]*"
                elif char == "?":
                    segment_pattern += r"[^/]"
                else:
                    segment_pattern += re.escape(char)
            regex_parts.append(segment_pattern)
        else:
            # Exact match
            regex_parts.append(re.escape(part))

    # Join with path separator
    regex = "^" + "/".join(regex_parts) + "$"

    # Fix double star pattern edge cases
    regex = regex.replace(r"(?:[^/]+(?:/[^/]+)*)?/", r"(?:[^/]+/)*")
    regex = regex.replace(r"/(?:[^/]+(?:/[^/]+)*)?", r"(?:/[^/]+)*")

    return re.compile(regex)


def matches(pattern: str, key: str) -> bool:
    """Check if a key matches a pattern.

    Args:
        pattern: Glob-style pattern.
        key: Key to match against.

    Returns:
        True if key matches pattern.

    Example:
        matches("cluster/*", "cluster/leader")  # True
        matches("cluster/*", "cluster/nodes/x")  # False
        matches("cluster/**", "cluster/nodes/x")  # True
    """
    compiled = compile_pattern(pattern)
    return compiled.match(key) is not None


class PatternMatcher:
    """Efficient pattern matcher for multiple patterns.

    Caches compiled patterns for reuse.

    Example:
        matcher = PatternMatcher()
        matcher.add_pattern("cluster/*", handler1)
        matcher.add_pattern("app/**", handler2)

        # Get all handlers matching a key
        handlers = matcher.get_matching("cluster/leader")
    """

    def __init__(self):
        self._patterns: dict[str, re.Pattern] = {}
        self._handlers: dict[str, set] = {}

    def add_pattern(self, pattern: str, handler) -> None:
        """Add a pattern with a handler.

        Args:
            pattern: Glob-style pattern.
            handler: Handler to associate with pattern.
        """
        if pattern not in self._patterns:
            self._patterns[pattern] = compile_pattern(pattern)
            self._handlers[pattern] = set()
        self._handlers[pattern].add(handler)

    def remove_pattern(self, pattern: str, handler) -> bool:
        """Remove a handler from a pattern.

        Args:
            pattern: Pattern to remove handler from.
            handler: Handler to remove.

        Returns:
            True if handler was removed, False if not found.
        """
        if pattern not in self._handlers:
            return False

        self._handlers[pattern].discard(handler)

        # Clean up empty patterns
        if not self._handlers[pattern]:
            del self._handlers[pattern]
            del self._patterns[pattern]

        return True

    def get_matching(self, key: str) -> set:
        """Get all handlers matching a key.

        Args:
            key: Key to match.

        Returns:
            Set of handlers whose patterns match the key.
        """
        result = set()
        for pattern, regex in self._patterns.items():
            if regex.match(key):
                result.update(self._handlers[pattern])
        return result

    def has_pattern(self, pattern: str) -> bool:
        """Check if a pattern is registered.

        Args:
            pattern: Pattern to check.

        Returns:
            True if pattern is registered.
        """
        return pattern in self._patterns

    def clear(self) -> None:
        """Remove all patterns and handlers."""
        self._patterns.clear()
        self._handlers.clear()
