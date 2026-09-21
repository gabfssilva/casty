# Distribuição

Distribui doze carrinhos entre três nós usando o anel de hash consistente e envia mensagens por nós diferentes, sem informar onde cada chave está. O programa adiciona um quarto nó e depois encerra um dos anteriores de forma ordenada, imprimindo a distribuição dos carrinhos e verificando que seus itens foram preservados em cada etapa.

Com `uv`, Python 3.13 ou superior, uma toolchain Rust e as portas TCP locais `7411` a `7414` livres, execute a partir da raiz do repositório. O `uv` compila e instala a versão local do casty; o programa inicia e encerra os nós automaticamente.

```sh
cd examples/06-distribution
uv run main.py
```
