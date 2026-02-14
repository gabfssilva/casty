# Custom Serialization

In this guide you'll implement a **custom serializer using cloudpickle** that can handle lambdas and closures. Along the way you'll learn the `Serializer` protocol, build a working implementation, and prove it roundtrips functions through a real actor.

## The Serializer Protocol

Casty uses a `Protocol`, not an ABC. Any object with these three methods satisfies it:

```python
--8<-- "examples/guides/08_custom_serialization.py:30:35"
```

`serialize` converts to bytes. `deserialize` converts back. `set_ref_factory` is called by the transport layer so your serializer can reconstruct `ActorRef` objects during deserialization.

## CloudpickleSerializer

The implementation is minimal — cloudpickle does the heavy lifting:

```python
--8<-- "examples/guides/08_custom_serialization.py:40:55"
```

No inheritance, no registration. If it quacks like a `Serializer`, it *is* a `Serializer`.

## Why Cloudpickle?

Standard pickle can't serialize lambdas defined inside functions. Cloudpickle can:

```python
--8<-- "examples/guides/08_custom_serialization.py:99:110"
```

Output:

```
pickle:      AttributeError
cloudpickle: OK
```

## Roundtripping Closures

Cloudpickle captures local variables along with the lambda. `multiplier = 3` survives the serialize-deserialize roundtrip:

```python
--8<-- "examples/guides/08_custom_serialization.py:112:119"
```

## Messages with Lambdas

A frozen dataclass carrying a callable roundtrips cleanly:

```python
--8<-- "examples/guides/08_custom_serialization.py:61:71"
```

```python
--8<-- "examples/guides/08_custom_serialization.py:122:126"
```

## Actor Applying Serialized Functions

The full loop: serialize a message carrying a lambda, deserialize it, and `tell()` it to an actor that applies the function to its state:

```python
--8<-- "examples/guides/08_custom_serialization.py:77:90"
```

```python
--8<-- "examples/guides/08_custom_serialization.py:129:147"
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

- **`Serializer`** is a Protocol — implement three methods and you're done. No inheritance required.
- **Cloudpickle** handles lambdas, closures, and captured variables that standard pickle cannot.
- **Messages carrying callables** roundtrip cleanly through cloudpickle serialization.
- **Structural subtyping** means your serializer satisfies the protocol without any base class.
