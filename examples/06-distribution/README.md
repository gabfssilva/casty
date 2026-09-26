# Distribution

Spreads twelve carts over three nodes with the consistent hash ring and sends messages through different nodes, without saying where each key is. The program adds a fourth node and then gracefully stops one of the earlier ones, printing the distribution of the carts and checking that their items were kept at each step.

Needs local TCP ports `7411` to `7414` to be free.

```sh
cd examples/06-distribution
uv run main.py
```
