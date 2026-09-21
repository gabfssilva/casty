# Replicação

Inicia três nós no mesmo processo e grava um diário com três réplicas e confirmação por maioria. Depois de salvar três entradas, o programa encerra abruptamente o nó responsável pelo ator, lê as entradas em um sobrevivente e acrescenta outra, demonstrando a recuperação do estado confirmado após a perda de um nó.

Com `uv`, Python 3.13 ou superior, uma toolchain Rust e as portas TCP locais `7401` a `7403` livres, execute a partir da raiz do repositório. O `uv` compila e instala a versão local do casty; o programa inicia e encerra os nós automaticamente.

```sh
cd examples/05-replication
uv run main.py
```
