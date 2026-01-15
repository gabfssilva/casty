"""Tests for gossip pattern matching."""

import pytest

from casty.gossip.patterns import matches, compile_pattern, PatternMatcher


class TestPatternMatching:
    """Tests for pattern matching."""

    def test_exact_match(self):
        """Exact string matches."""
        assert matches("cluster/leader", "cluster/leader")
        assert not matches("cluster/leader", "cluster/follower")
        assert not matches("cluster/leader", "cluster")

    def test_single_wildcard(self):
        """Single wildcard matches one segment."""
        assert matches("cluster/*", "cluster/leader")
        assert matches("cluster/*", "cluster/follower")
        assert not matches("cluster/*", "cluster/nodes/node-1")
        assert not matches("cluster/*", "cluster")

    def test_double_wildcard(self):
        """Double wildcard matches zero or more segments."""
        assert matches("cluster/**", "cluster/leader")
        assert matches("cluster/**", "cluster/nodes/node-1")
        assert matches("cluster/**", "cluster/nodes/node-1/status")
        # Note: ** should also match the base path depending on implementation
        # This test documents current behavior

    def test_mixed_wildcards(self):
        """Mixed patterns."""
        assert matches("*/leader", "cluster/leader")
        assert matches("*/leader", "app/leader")
        assert not matches("*/leader", "cluster/follower")

    def test_wildcard_in_middle(self):
        """Wildcard in middle of pattern."""
        assert matches("cluster/*/status", "cluster/node-1/status")
        assert not matches("cluster/*/status", "cluster/node-1/health")
        assert not matches("cluster/*/status", "cluster/nodes/node-1/status")

    def test_empty_pattern(self):
        """Empty pattern edge case."""
        # Empty pattern should match empty key
        assert matches("", "")
        assert not matches("", "anything")

    def test_pattern_with_question_mark(self):
        """Question mark matches single character."""
        assert matches("node-?", "node-1")
        assert matches("node-?", "node-a")
        assert not matches("node-?", "node-12")


class TestPatternMatcher:
    """Tests for PatternMatcher class."""

    def test_add_and_match(self):
        """Add patterns and match keys."""
        matcher = PatternMatcher()
        handler1 = object()
        handler2 = object()

        matcher.add_pattern("cluster/*", handler1)
        matcher.add_pattern("app/**", handler2)

        # cluster/* should match
        handlers = matcher.get_matching("cluster/leader")
        assert handler1 in handlers
        assert handler2 not in handlers

        # app/** should match
        handlers = matcher.get_matching("app/config/timeout")
        assert handler2 in handlers
        assert handler1 not in handlers

        # Neither matches
        handlers = matcher.get_matching("other/key")
        assert len(handlers) == 0

    def test_multiple_handlers_same_pattern(self):
        """Multiple handlers for same pattern."""
        matcher = PatternMatcher()
        handler1 = object()
        handler2 = object()

        matcher.add_pattern("cluster/*", handler1)
        matcher.add_pattern("cluster/*", handler2)

        handlers = matcher.get_matching("cluster/leader")
        assert handler1 in handlers
        assert handler2 in handlers

    def test_remove_pattern(self):
        """Remove handler from pattern."""
        matcher = PatternMatcher()
        handler = object()

        matcher.add_pattern("cluster/*", handler)
        assert handler in matcher.get_matching("cluster/leader")

        result = matcher.remove_pattern("cluster/*", handler)
        assert result is True
        assert handler not in matcher.get_matching("cluster/leader")

    def test_remove_nonexistent(self):
        """Remove from nonexistent pattern."""
        matcher = PatternMatcher()
        handler = object()

        result = matcher.remove_pattern("nonexistent", handler)
        assert result is False

    def test_has_pattern(self):
        """Check if pattern is registered."""
        matcher = PatternMatcher()
        handler = object()

        assert not matcher.has_pattern("cluster/*")

        matcher.add_pattern("cluster/*", handler)
        assert matcher.has_pattern("cluster/*")

    def test_clear(self):
        """Clear all patterns."""
        matcher = PatternMatcher()
        matcher.add_pattern("cluster/*", object())
        matcher.add_pattern("app/*", object())

        matcher.clear()

        assert not matcher.has_pattern("cluster/*")
        assert not matcher.has_pattern("app/*")

    def test_overlapping_patterns(self):
        """Key matching multiple patterns."""
        matcher = PatternMatcher()
        handler1 = object()
        handler2 = object()
        handler3 = object()

        matcher.add_pattern("*", handler1)
        matcher.add_pattern("cluster/*", handler2)
        matcher.add_pattern("cluster/**", handler3)

        handlers = matcher.get_matching("cluster/leader")
        # Depending on patterns:
        # "*" matches single segment, so doesn't match "cluster/leader"
        # "cluster/*" matches
        # "cluster/**" matches
        assert handler2 in handlers
        assert handler3 in handlers
