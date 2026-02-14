# Custom Serialization

In this guide you'll implement a **custom serializer using cloudpickle** to handle lambdas and closures that standard pickle cannot. Along the way you'll learn the `Serializer` protocol, compare all three serializer implementations, and prove the roundtrip works inside a real actor.

## Why Custom Serialization?

Casty ships with two serializers: `JsonSerializer` (human-readable, cross-language) and `PickleSerializer` (fast, Python-only). But standard pickle can't serialize lambdas or closures defined inside functions. If your actors need to receive functions as messages — think distributed compute, map-reduce, or rule engines — you need something like cloudpickle.

## The Serializer Protocol

Casty uses a `Protocol`, not an ABC. Any object with these three methods satisfies it:

```python
--8<-- "examples/guides/08_custom_serialization.py:32:37"
```

`serialize` converts to bytes. `deserialize` converts back. `set_ref_factory` is called by the transport layer so your serializer can reconstruct `ActorRef` objects during deserialization.

## CloudpickleSerializer

The implementation is minimal — cloudpickle does the heavy lifting:

```python
--8<-- "examples/guides/08_custom_serialization.py:43:58"
```

No inheritance, no registration. If it quacks like a `Serializer`, it *is* a `Serializer`.

## Comparing All Three

The example walks through each serializer with the same message:

### JsonSerializer

Requires a `TypeRegistry` to map type names to classes. Produces human-readable JSON:

```python
--8<-- "examples/guides/08_custom_serialization.py:88:104"
```

### PickleSerializer

No registry needed — pickle handles Python types natively:

```python
--8<-- "examples/guides/08_custom_serialization.py:107:115"
```

### CloudpickleSerializer — Lambdas!

Standard pickle chokes on lambdas defined inside functions. Cloudpickle handles them, including captured closure variables:

```python
--8<-- "examples/guides/08_custom_serialization.py:118:128"
```

`multiplier = 3` is captured in the closure. Cloudpickle serializes *both* the lambda and its captured state.

## Lambdas in Messages

Frozen dataclasses can carry callables. Cloudpickle roundtrips the whole message:

```python
--8<-- "examples/guides/08_custom_serialization.py:64:75"
```

## Proof: Standard Pickle Fails

```python
--8<-- "examples/guides/08_custom_serialization.py:138:149"
```

Output:

```
pickle: AttributeError — Can't get local object 'main.<locals>.<lambda>'
cloudpickle: OK
```

## Real Actor with Serialized Functions

Functions survive the serialize-deserialize roundtrip and get applied by a real actor:

```python
--8<-- "examples/guides/08_custom_serialization.py:154:173"
```

Output:

```
0 → 10
10 → 30
30 → 25
```

## Run the Full Example

```bash
pip install cloudpickle
git clone https://github.com/gabfssilva/casty.git
cd casty
uv run python examples/guides/08_custom_serialization.py
```

---

**What you learned:**

- **`Serializer`** is a Protocol — implement three methods and you're done. No inheritance required.
- **`JsonSerializer`** needs a `TypeRegistry` and produces human-readable output.
- **`PickleSerializer`** is fast and requires no setup, but can't handle lambdas inside functions.
- **Cloudpickle** handles lambdas, closures, and captured variables that standard pickle cannot.
- **Custom serializers** satisfy the protocol by structural subtyping — no base class needed.
