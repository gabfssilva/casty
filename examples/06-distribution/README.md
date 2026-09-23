# Distribuição

Distribui doze carrinhos entre três nós usando o anel de hash consistente e envia mensagens por nós diferentes, sem informar onde cada chave está. O programa adiciona um quarto nó e depois encerra um dos anteriores de forma ordenada, imprimindo a distribuição dos carrinhos e verificando que seus itens foram preservados em cada etapa.

Precisa das portas TCP locais `7411` a `7414` livres.

```sh
cd examples/06-distribution
uv run main.py
```
