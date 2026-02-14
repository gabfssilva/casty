# Custom Serialization

In this guide you'll implement a **custom serializer using cloudpickle** that can handle lambdas and closures. Along the way you'll learn the `Serializer` protocol, build a working implementation, and prove it roundtrips functions through a real actor.

## The Serializer Protocol

Casty uses a `Protocol`, not an ABC. Any object with these two methods satisfies it:

```python
--8<-- "examples/guides/08_custom_serialization.py:30:34"
```

`serialize` converts to bytes. `deserialize` converts back. The optional `ref_factory` keyword argument lets the transport layer pass a factory for reconstructing `ActorRef` objects during deserialization.

## CloudpickleSerializer

The implementation is minimal — cloudpickle does the heavy lifting:

```python
--8<-- "examples/guides/08_custom_serialization.py:40:52"
```

No inheritance, no registration. If it quacks like a `Serializer`, it *is* a `Serializer`.

## Why Cloudpickle?

Standard pickle can't serialize lambdas defined inside functions. Cloudpickle can:

```python
--8<-- "examples/guides/08_custom_serialization.py:93:102"
```

Output:

```
pickle:      AttributeError
cloudpickle: OK
```

## Roundtripping Closures

Cloudpickle captures local variables along with the lambda. `multiplier = 3` survives the serialize-deserialize roundtrip:

```python
--8<-- "examples/guides/08_custom_serialization.py:105:112"
```

## Messages with Lambdas

A frozen dataclass carrying a callable roundtrips cleanly:

```python
--8<-- "examples/guides/08_custom_serialization.py:57:67"
```

```python
--8<-- "examples/guides/08_custom_serialization.py:115:120"
```

## Actor Applying Serialized Functions

The full loop: serialize a message carrying a lambda, deserialize it, and `tell()` it to an actor that applies the function to its state:

```python
--8<-- "examples/guides/08_custom_serialization.py:74:87"
```

```python
--8<-- "examples/guides/08_custom_serialization.py:123:142"
```

Output:

```
0 → 10
10 → 30
30 → 25
Final value: 25
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

- **`Serializer`** is a Protocol — implement two methods and you're done. No inheritance required.
- **Cloudpickle** handles lambdas, closures, and captured variables that standard pickle cannot.
- **Messages carrying callables** roundtrip cleanly through cloudpickle serialization.
- **Structural subtyping** means your serializer satisfies the protocol without any base class.
