# Cliente externo

Demonstra um `Client` que acessa atores do cluster sem hospedar atores nem armazenar seu estado. Dois processos executam os nós, enquanto um terceiro envia votos para quatro opções e consulta a contagem e o nó responsável por cada uma; os tipos compartilhados ficam em `app.py`, e votos repetidos do mesmo eleitor na mesma opção contam apenas uma vez.

Precisa das portas TCP locais `7421` e `7422` livres. Execute cada bloco em um terminal separado.

Terminal 1:

```sh
cd examples/07-client
uv run node.py 7421
```

Terminal 2:

```sh
cd examples/07-client
uv run node.py 7422
```

Depois que os nós imprimirem `is up`, execute no terminal 3:

```sh
cd examples/07-client
uv run client.py
```

Encerre os nós com `Ctrl+C` nos respectivos terminais.
