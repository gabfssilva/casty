# Máquina de estados

Modela um pedido nos estados pendente, pago e enviado, cada um com seu próprio ator e tipo de estado. `ctx.become` troca o comportamento que recebe as próximas mensagens sem mudar a referência do pedido; o programa imprime as transições aceitas e as operações recusadas em cada estado.

```sh
cd examples/01-state-machine
uv run main.py
```
