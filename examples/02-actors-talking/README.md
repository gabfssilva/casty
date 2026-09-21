# Comunicação entre atores

Demonstra um ator que coordena transferências consultando dois atores de conta por meio de `ctx.system.ref` e `ask`. O programa deposita um saldo inicial, tenta duas transferências e imprime os saldos finais; saque e depósito são operações separadas, sem uma transação atômica entre as contas.

Com `uv`, Python 3.13 ou superior e uma toolchain Rust, execute a partir da raiz do repositório. O `uv` compila e instala a versão local do casty.

```sh
cd examples/02-actors-talking
uv run main.py
```
