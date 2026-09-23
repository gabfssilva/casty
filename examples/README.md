# Exemplos

Cada diretório é um programa que demonstra uma parte do casty. Todos usam o projeto `uv` deste diretório, que instala a versão local do casty a partir da raiz do repositório.

Requer `uv`, Python 3.12 ou superior e uma toolchain Rust: na primeira execução, o `uv` compila o casty. Os comandos partem da raiz do repositório, e cada exemplo roda com `uv run` dentro do seu diretório:

```sh
cd examples/00-hello-world
uv run main.py
```

Os exemplos com vários nós iniciam todos no mesmo processo, cada um numa porta TCP local, e os encerram ao terminar. `09-docker-cluster` é a exceção: roda em contêineres e só precisa de Docker.

- [`00-hello-world`](00-hello-world): um ator com estado inicial, perguntas com `ask` e um estado por chave.
- [`01-state-machine`](01-state-machine): um pedido como máquina de estados, em que `ctx.become` troca o comportamento sem mudar a referência.
- [`02-actors-talking`](02-actors-talking): um ator que coordena transferências perguntando a outros atores por `ctx.system.ref`.
- [`03-streams`](03-streams): `ctx.merge` juntando a mailbox e um gerador assíncrono no mesmo loop do ator.
- [`04-failures`](04-failures): `ActorFailed`, `MailboxFull` e o estado que o ator mantém depois de uma falha.
- [`05-replication`](05-replication): um estado com três réplicas que sobrevive à queda do nó que executava a chave.
- [`06-distribution`](06-distribution): chaves distribuídas pelo anel de hash enquanto nós entram e saem do cluster.
- [`07-client`](07-client): um `Client` que usa o cluster sem fazer parte dele, com cada nó em seu próprio processo.
- [`08-consumers`](08-consumers): um consumidor por partição, que o cluster retoma sozinho quando seu nó cai.
- [`09-docker-cluster`](09-docker-cluster): um cluster de trinta e três nós em Docker Compose, redimensionado durante a execução.
- [`10-agent-per-node`](10-agent-per-node): um agente por nó, declarado com `pinned=True` e alcançado pelo endereço do nó.
- [`11-durable-state`](11-durable-state): um estado com `durable="write"` num store SQLite, que sobrevive à parada de todos os nós.
