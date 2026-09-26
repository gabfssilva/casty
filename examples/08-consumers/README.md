# Consumers

Runs one consumer per partition in a cluster of three nodes, using an async generator to simulate the records of an external source. Each consumer saves its offset after processing a record; the program takes down the busiest node and checks that the consumers resume on the survivors with no action from the caller, possibly repeating records processed before the offset was saved.

Needs local TCP ports `7431` to `7433` to be free; needs neither Kafka nor any other broker.

```sh
cd examples/08-consumers
uv run main.py
```
