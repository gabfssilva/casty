"""Merge helper functions for three-way merge.

These helpers implement common merge strategies for different
data types, using the base (common ancestor) state to compute
and combine deltas from divergent modifications.
"""

from __future__ import annotations

from typing import TypeVar

T = TypeVar("T")


def merge_sum(
    base: int | float,
    mine: int | float,
    theirs: int | float,
) -> int | float:
    """Merge numeric values by summing deltas from base.

    This is ideal for counters, balances, or any additive value
    where concurrent modifications should be combined.

    Example:
        base = 100, mine = 120 (+20), theirs = 130 (+30)
        result = 100 + 20 + 30 = 150

    Args:
        base: The common ancestor value
        mine: My current value
        theirs: Their current value

    Returns:
        Base plus combined deltas from both sides
    """
    my_delta = mine - base
    their_delta = theirs - base
    return base + my_delta + their_delta


def merge_max(
    _base: int | float,
    mine: int | float,
    theirs: int | float,
) -> int | float:
    """Merge by taking the maximum value.

    Useful for high-water marks, version numbers, or any
    monotonically increasing value.

    Args:
        base: The common ancestor value (unused, for API consistency)
        mine: My current value
        theirs: Their current value

    Returns:
        The maximum of mine and theirs
    """
    return max(mine, theirs)


def merge_min(
    _base: int | float,
    mine: int | float,
    theirs: int | float,
) -> int | float:
    """Merge by taking the minimum value.

    Useful for low-water marks or deadline tracking.

    Args:
        base: The common ancestor value (unused, for API consistency)
        mine: My current value
        theirs: Their current value

    Returns:
        The minimum of mine and theirs
    """
    return min(mine, theirs)


def merge_lww(
    mine: T,
    theirs: T,
    my_ts: float,
    their_ts: float,
) -> T:
    """Last-Writer-Wins merge based on timestamp.

    Simple strategy where the most recent write wins.
    Requires tracking timestamps for each modification.

    Args:
        mine: My current value
        theirs: Their current value
        my_ts: Timestamp of my last modification
        their_ts: Timestamp of their last modification

    Returns:
        The value with the higher timestamp
    """
    return theirs if their_ts > my_ts else mine


def merge_union(mine: set[T], theirs: set[T]) -> set[T]:
    """Merge sets by union (add-wins).

    All elements from both sets are preserved. This is a
    grow-only set merge - elements are never removed.

    Args:
        mine: My current set
        theirs: Their current set

    Returns:
        Union of both sets
    """
    return mine | theirs


def merge_intersection(mine: set[T], theirs: set[T]) -> set[T]:
    """Merge sets by intersection (remove-wins).

    Only elements present in both sets are kept.

    Args:
        mine: My current set
        theirs: Their current set

    Returns:
        Intersection of both sets
    """
    return mine & theirs


def merge_set_add_remove(
    base: set[T],
    mine: set[T],
    theirs: set[T],
) -> set[T]:
    """Merge sets with add/remove tracking from base.

    Computes adds and removes from each side and applies them.
    Adds win over removes (if both add and remove, element is kept).

    Args:
        base: The common ancestor set
        mine: My current set
        theirs: Their current set

    Returns:
        Merged set with combined adds/removes
    """
    my_adds = mine - base
    my_removes = base - mine
    their_adds = theirs - base
    their_removes = base - theirs

    # Start with base, apply all adds, remove only what both removed
    result = base | my_adds | their_adds
    common_removes = my_removes & their_removes
    return result - common_removes


def merge_list_append(
    base: list[T],
    mine: list[T],
    theirs: list[T],
) -> list[T]:
    """Merge lists by appending new items from both sides.

    Detects new items added to each list since the base and
    appends them in order (mine first, then theirs).

    Note: This uses equality for deduplication, so items must
    be properly comparable.

    Args:
        base: The common ancestor list
        mine: My current list
        theirs: Their current list

    Returns:
        Base list plus new items from both sides
    """
    base_set = set(base)
    my_new = [x for x in mine if x not in base_set]
    their_new = [x for x in theirs if x not in base_set]

    # Deduplicate between my_new and their_new
    my_new_set = set(my_new)
    their_unique = [x for x in their_new if x not in my_new_set]

    return list(base) + my_new + their_unique


def merge_dict_shallow(
    base: dict[str, T],
    mine: dict[str, T],
    theirs: dict[str, T],
) -> dict[str, T]:
    """Merge dictionaries with shallow key-level merge.

    For each key:
    - If only one side modified, use that value
    - If both modified same key, theirs wins (LWW)
    - Deletions: if both deleted, key is removed
      if only one deleted and other modified, modification wins

    Args:
        base: The common ancestor dict
        mine: My current dict
        theirs: Their current dict

    Returns:
        Merged dictionary
    """
    result = dict(base)

    # Track modifications
    all_keys = set(base.keys()) | set(mine.keys()) | set(theirs.keys())

    for key in all_keys:
        in_base = key in base
        in_mine = key in mine
        in_theirs = key in theirs

        if in_mine and in_theirs:
            # Both have the key
            if mine[key] != base.get(key) and theirs[key] != base.get(key):
                # Both modified - theirs wins (LWW)
                result[key] = theirs[key]
            elif mine[key] != base.get(key):
                result[key] = mine[key]
            elif theirs[key] != base.get(key):
                result[key] = theirs[key]
            else:
                # Neither modified from base
                result[key] = base.get(key, mine[key])
        elif in_mine and not in_theirs:
            if in_base:
                # They deleted it
                if mine[key] != base[key]:
                    # I modified, modification wins over delete
                    result[key] = mine[key]
                else:
                    # I didn't modify, respect their delete
                    result.pop(key, None)
            else:
                # I added it
                result[key] = mine[key]
        elif in_theirs and not in_mine:
            if in_base:
                # I deleted it
                if theirs[key] != base[key]:
                    # They modified, modification wins over delete
                    result[key] = theirs[key]
                else:
                    # They didn't modify, respect my delete
                    result.pop(key, None)
            else:
                # They added it
                result[key] = theirs[key]
        else:
            # Neither has the key
            result.pop(key, None)

    return result
