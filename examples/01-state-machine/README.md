# Máquina de estados

Modela um pedido nos estados pendente, pago e enviado, cada um com seu próprio ator e tipo de estado. `ctx.become` troca o comportamento que recebe as próximas mensagens sem mudar a referência do pedido; o programa imprime as transições aceitas e as operações recusadas em cada estado.

Com `uv`, Python 3.13 ou superior e uma toolchain Rust, execute a partir da raiz do repositório. O `uv` compila e instala a versão local do casty.

```sh
cd examples/01-state-machine
uv run main.py
```
