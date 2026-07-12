"""Actor state as pages (spec 09). A page is the unit of commit and replication;
a handler that touches one field replicates one page, not the whole state.

The basis a diff runs against is the *committed* bytes of each page. A field with
no committed page restores to its default, so a field still at its default costs
nothing on the wire — and every regime here is allowed to over-report a change,
never to miss one.
"""

from __future__ import annotations

import base64
import dataclasses
import typing
from collections.abc import (
    ItemsView,
    Iterable,
    Iterator,
    KeysView,
    Mapping,
    MutableMapping,
    ValuesView,
)
from dataclasses import dataclass

from casty.actors.registry import ActorInfo, Integral, Paged, Tracked
from casty.actors.replication import Page, StoredPage
from casty.errors import SerializationError
from casty.serde import codec

type EntryKey = str | int | bytes

_MISSING = object()


class TrackedDict(MutableMapping[EntryKey, object]):
    """A dict that remembers which keys a handler touched, so a commit encodes one
    page per changed entry instead of the whole dict (spec 09 §4).

    A `MutableMapping` and not a `dict` subclass on purpose: `dict`'s C methods
    (`update`, `pop`, `clear`, `|=`) write straight into the hash table without
    going through `__setitem__`, and a mutation missed here would be silent
    divergence. Every mutator of this one lands on `__setitem__`/`__delitem__`.
    Entries put in by `__init__` and `reset` are the committed ones: they start
    clean.
    """

    __slots__ = ("_entries", "deleted", "dirty")

    def __init__(self, entries: Mapping[EntryKey, object] | None = None) -> None:
        self._entries: dict[EntryKey, object] = dict(entries or {})
        self.dirty: set[EntryKey] = set()
        self.deleted: set[EntryKey] = set()

    def committed(self) -> None:
        self.dirty.clear()
        self.deleted.clear()

    def reset(self, entries: Mapping[EntryKey, object], added: Iterable[EntryKey]) -> None:
        """Put the dict back the way the last commit left it: restore the entries it
        held, drop the ones the failed handler added."""
        self._entries.update(entries)
        for key in added:
            del self._entries[key]
        self.committed()

    def __getitem__(self, key: EntryKey) -> object:
        return self._entries[key]

    def __setitem__(self, key: EntryKey, value: object) -> None:
        self._entries[key] = value
        self.dirty.add(key)
        self.deleted.discard(key)

    def __delitem__(self, key: EntryKey) -> None:
        del self._entries[key]
        self.dirty.discard(key)
        self.deleted.add(key)

    def __iter__(self) -> Iterator[EntryKey]:
        return iter(self._entries)

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, key: object) -> bool:
        return key in self._entries

    def __repr__(self) -> str:
        return repr(self._entries)

    def __ior__(self, other: Mapping[EntryKey, object], /) -> typing.Self:
        self.update(other)
        return self

    # the mixins reach these one key at a time; the dict's own views do not
    def keys(self) -> KeysView[EntryKey]:
        return self._entries.keys()

    def values(self) -> ValuesView[object]:
        return self._entries.values()

    def items(self) -> ItemsView[EntryKey, object]:
        return self._entries.items()

    def clear(self) -> None:
        self.deleted.update(self._entries)
        self.dirty.clear()
        self._entries.clear()


@dataclass(slots=True)
class State:
    """An activation's private view of what is committed: the page bytes a diff runs
    against, and the pager shadows it compares values to. The bytes are shared with
    the replica store, so this costs a dict of pointers, not a copy."""

    pages: dict[str, bytes] = dataclasses.field(default_factory=dict)
    shadows: dict[str, object] = dataclasses.field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Delta:
    """What one handler changed: the pages to replicate and the ones to remove."""

    pages: list[Page]
    dropped: list[str]
    tracked: dict[str, TrackedDict]  # the wrappers whose marks this delta consumed
    snapshots: dict[str, object]  # the pager shadows to adopt if it lands

    @property
    def empty(self) -> bool:
        return not self.pages and not self.dropped


def activate(
    info: ActorInfo, stored: Mapping[str, StoredPage], instance: object
) -> State:
    """Rebuild the instance from the pages the last commit left behind, and return
    the basis the next diff runs against. An integral field with no page is at its
    default, so a field that never moved costs nothing on the wire."""
    pages = {page_key: page.data for page_key, page in stored.items()}
    for regime in info.regimes.values():
        if isinstance(regime, Integral) and regime.name not in pages:
            pages[regime.name] = regime.default
    state = State(pages=pages)
    reset(info, state, instance)
    return state


def reset(info: ActorInfo, state: State, instance: object) -> None:
    """Put every field back to its committed bytes, and re-shadow the paged ones."""
    grouped: dict[str, dict[str, bytes]] = {
        regime.name: {}
        for regime in info.regimes.values()
        if not isinstance(regime, Integral)
    }
    for page_key, data in state.pages.items():
        name, sep, sub_key = page_key.partition("/")
        if sep and name in grouped:  # a page of a field this build does not know: ignored
            grouped[name][sub_key] = data
    values: dict[str, object] = {}
    for regime in info.regimes.values():
        match regime:
            case Integral(name=name, hint=hint):
                values[name] = _decode(hint, state.pages[name], name)
            case Tracked(name=name):
                values[name] = TrackedDict(
                    {
                        _entry_key(regime, sub_key): _decode(regime.value_hint, data, name)
                        for sub_key, data in grouped[name].items()
                    }
                )
            case Paged(name=name, pager=pager):
                values[name] = pager.restore(grouped[name])
            case _:
                typing.assert_never(regime)
    info.restore_into(instance, values)
    state.shadows = {
        regime.name: regime.pager.snapshot(values[regime.name])
        for regime in info.regimes.values()
        if isinstance(regime, Paged)
    }


def commit(state: State, delta: Delta) -> None:
    """The delta landed: it is the committed basis now."""
    for page in delta.pages:
        state.pages[page.key] = page.data
    for page_key in delta.dropped:
        state.pages.pop(page_key, None)
    state.shadows.update(delta.snapshots)
    for entries in delta.tracked.values():
        entries.committed()


def diff(
    info: ActorInfo,
    instance: object,
    values: Mapping[str, object],
    last_values: Mapping[str, object] | None,
    state: State,
) -> Delta:
    """The pages this handler changed. An immutable field whose value still compares
    equal cannot have been mutated in place (no aliasing is possible), so it skips
    the encode; a tracked dict encodes only the entries it marked; a paged field asks
    its pager; everything else is decided on the encoded bytes."""
    basis = state.pages

    def rearm(regime: Tracked, value: object) -> TrackedDict:
        """The handler replaced the whole dict, so the wrapper that was tracking it is
        gone: wrap the new one and treat every entry as touched."""
        if not isinstance(value, dict):
            raise SerializationError(
                f"{info.wire_name}.{regime.name}: expected a dict, "
                f"got {type(value).__qualname__}"
            )
        entries = TrackedDict(value)
        entries.dirty.update(entries)
        for page_key in basis:
            name, sep, entry = page_key.partition("/")
            if sep and name == regime.name:
                key = _entry_key(regime, entry)
                if key not in entries:
                    entries.deleted.add(key)
        setattr(instance, regime.name, entries)
        return entries

    pages: list[Page] = []
    dropped: list[str] = []
    tracked: dict[str, TrackedDict] = {}
    snapshots: dict[str, object] = {}
    for regime in info.regimes.values():
        match regime:
            case Integral(name=name, immutable=immutable):
                value = values[name]
                if immutable and last_values is not None:
                    previous = last_values.get(name, _MISSING)
                    if previous is value or previous == value:
                        continue
                data = codec.encode_raw(value)
                if basis.get(name) != data:
                    pages.append(Page(key=name, data=data))
            case Tracked(name=name):
                entries = values[name]
                if not isinstance(entries, TrackedDict):
                    entries = rearm(regime, entries)
                tracked[name] = entries
                for key in entries.dirty:
                    page_key = _page_key(regime, key)
                    data = codec.encode_raw(entries[key])
                    if basis.get(page_key) != data:
                        pages.append(Page(key=page_key, data=data))
                dropped.extend(
                    page_key
                    for key in entries.deleted
                    if (page_key := _page_key(regime, key)) in basis
                )
            case Paged(name=name, pager=pager):
                value = values[name]
                changed = pager.pages(value, state.shadows[name])
                pages.extend(
                    Page(key=f"{name}/{sub_key}", data=data)
                    for sub_key, data in changed.changed.items()
                )
                dropped.extend(f"{name}/{sub_key}" for sub_key in changed.dropped)
                snapshots[name] = pager.snapshot(value)
            case _:
                typing.assert_never(regime)
    return Delta(pages=pages, dropped=dropped, tracked=tracked, snapshots=snapshots)


def rollback(info: ActorInfo, instance: object, state: State, delta: Delta) -> None:
    """Undo an uncommitted mutation: only the pages the failed commit would have
    carried need putting back. A paged field is put back by its shadow, which *is*
    the value as the handler found it."""
    fields: dict[str, object] = {}
    resets: dict[str, tuple[dict[EntryKey, object], list[EntryKey]]] = {}
    for page_key in [*(page.key for page in delta.pages), *delta.dropped]:
        name, _, entry = page_key.partition("/")
        regime = info.regimes[name]
        match regime:
            case Integral(hint=hint):
                fields[name] = _decode(hint, state.pages[page_key], page_key)
            case Tracked(value_hint=value_hint):
                held, added = resets.setdefault(name, ({}, []))
                key = _entry_key(regime, entry)
                data = state.pages.get(page_key)
                if data is None:
                    added.append(key)  # the handler added it; it was never committed
                else:
                    held[key] = _decode(value_hint, data, page_key)
            case Paged(pager=pager):
                if name not in fields:
                    restored = pager.rollback(state.shadows[name])
                    fields[name] = restored
                    # re-shadowed: the old shadow is the live value now, and a shadow
                    # that *is* the value it is compared against would see no change
                    state.shadows[name] = pager.snapshot(restored)
            case _:
                typing.assert_never(regime)
    for name, (held, added) in resets.items():
        delta.tracked[name].reset(held, added)
    info.restore_into(instance, fields)


def _page_key(regime: Tracked, key: object) -> str:
    if isinstance(key, bytes):
        return f"{regime.name}/{base64.urlsafe_b64encode(key).decode('ascii')}"
    return f"{regime.name}/{key}"


def _entry_key(regime: Tracked, entry: str) -> EntryKey:
    if regime.key is bytes:
        return base64.urlsafe_b64decode(entry)
    if regime.key is int:
        return int(entry)
    return entry


def _decode(hint: object, data: bytes, path: str) -> object:
    return codec.decode_raw(data, hint, path=path)
