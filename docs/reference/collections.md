# casty.collections

::: casty.collections
    options:
      members: false
      show_root_heading: false

`Collections` builds every collection below; their constructors are internal.

## Factory

::: casty.collections.Collections

## Data

::: casty.collections.Counter
    options:
      merge_init_into_class: false
      filters: ["!^_[^_]", "!^__init__$"]

::: casty.collections.Register
    options:
      merge_init_into_class: false
      filters: ["!^_[^_]", "!^__init__$"]

::: casty.collections.Dict
    options:
      merge_init_into_class: false
      filters: ["!^_[^_]", "!^__init__$"]

::: casty.collections.Set
    options:
      merge_init_into_class: false
      filters: ["!^_[^_]", "!^__init__$"]

::: casty.collections.MultiMap
    options:
      merge_init_into_class: false
      filters: ["!^_[^_]", "!^__init__$"]

::: casty.collections.Queue
    options:
      merge_init_into_class: false
      filters: ["!^_[^_]", "!^__init__$"]

## Coordination

::: casty.collections.Semaphore
    options:
      merge_init_into_class: false
      filters: ["!^_[^_]", "!^__init__$"]

::: casty.collections.Lock
    options:
      merge_init_into_class: false
      filters: ["!^_[^_]", "!^__init__$"]

::: casty.collections.Lease
    options:
      merge_init_into_class: false
      filters: ["!^_[^_]", "!^__init__$"]

::: casty.collections.Barrier
    options:
      merge_init_into_class: false
      filters: ["!^_[^_]", "!^__init__$"]

## The semaphore as an actor

::: casty.collections.semaphore
    options:
      members: ["Acquire", "Release", "Renew", "Get", "actor"]

::: casty.collections.SemaphoreState

::: casty.collections.Acquired

::: casty.collections.Denied

::: casty.collections.Status

## Absent values and errors

::: casty.collections.Missing

::: casty.collections.MISSING

::: casty.collections.ConfigurationError
