# Um agente por nó

Mantém um agente em cada nó de um cluster de três nós. O tipo é declarado com `pinned=True`, então cada referência nomeia o nó com `at=` em vez de deixar o anel de hash escolher: cada nó chega ao próprio agente pelo seu `NodeId`, o primeiro nó chega a todos pelos `Member` da sua tabela, e também pelo endereço `host:port` que o nó anuncia. O programa adiciona um quarto nó e mostra que nenhum agente se move; encerra o segundo nó de forma ordenada e mostra que o agente dele fica indisponível, sem que outro nó o assuma; e inicia um novo processo no mesmo endereço, onde a mesma referência volta a responder e o agente recomeça do estado `initial`.

Com `uv`, Python 3.13 ou superior, uma toolchain Rust e as portas TCP locais `7441` a `7444` livres, execute a partir da raiz do repositório. O `uv` compila e instala a versão local do casty; o programa inicia e encerra os nós automaticamente.

```sh
cd examples/10-agent-per-node
uv run main.py
```
