# Failures

Shows two errors the caller can observe: `ActorFailed` after a division by zero, and `MailboxFull` when a bounded mailbox is full. The program also checks that the restarted actor recovers the last saved state and that, in a type without a default initial state, only the `initial` of the first `ref` of a key counts.

```sh
cd examples/04-failures
uv run main.py
```
