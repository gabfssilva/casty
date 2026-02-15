# Behaviors as Values

In most actor frameworks, an actor is defined by subclassing a base class and overriding a `receive` method. Casty takes a different approach: **behaviors are values**, not classes.

A behavior is a frozen dataclass that describes how an actor processes messages. You compose behaviors using the `Behaviors` factory:

| Factory | Purpose |
|---------|---------|
| `Behaviors.receive(handler)` | Create a behavior from an async message handler `(ctx, msg) -> Behavior` |
| `Behaviors.setup(factory)` | Run initialization logic with access to `ActorContext`, then return the real behavior |
| `Behaviors.ignore()` | Accept any message and keep the current behavior — useful for terminal states |
| `Behaviors.same()` | Keep the current behavior unchanged (returned from a message handler) |
| `Behaviors.stopped()` | Stop the actor gracefully |
| `Behaviors.unhandled()` | Signal that the message was not handled |
| `Behaviors.restart()` | Explicitly restart the actor |
| `Behaviors.supervise(behavior, strategy)` | Wrap a behavior with a supervision strategy |
| `Behaviors.with_lifecycle(behavior, ...)` | Attach lifecycle hooks (pre_start, post_stop, etc.) |
| `Behaviors.event_sourced(...)` | Persist actor state as a sequence of events |
| `Behaviors.persisted(events)` | Return from a command handler to persist events and update state |
| `Behaviors.sharded(entity_factory, ...)` | Distribute entities across cluster nodes via sharding |
| `Behaviors.singleton(factory)` | Run exactly one actor instance across the cluster |
| `Behaviors.broadcasted(behavior)` | Enable broadcasting messages to all instances across the cluster |
| `Behaviors.discoverable(behavior, key=...)` | Auto-register the actor with the cluster receptionist on spawn |
| `Behaviors.spy(behavior, observer)` | Observe all messages an actor processes |

Because behaviors are values, they compose naturally. A behavior can be wrapped with supervision, decorated with lifecycle hooks, and backed by event sourcing — all through function composition, not class inheritance.

---

**Next:** [Functional State](functional-state.md)
