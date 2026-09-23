# Comunicação entre atores

Demonstra um ator que coordena transferências consultando dois atores de conta por meio de `ctx.system.ref` e `ask`. O programa deposita um saldo inicial, tenta duas transferências e imprime os saldos finais; saque e depósito são operações separadas, sem uma transação atômica entre as contas.

```sh
cd examples/02-actors-talking
uv run main.py
```
