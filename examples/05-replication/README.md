# Replicação

Inicia três nós no mesmo processo e grava um diário com três réplicas e confirmação por maioria. Depois de salvar três entradas, o programa encerra abruptamente o nó responsável pelo ator, lê as entradas em um sobrevivente e acrescenta outra, demonstrando a recuperação do estado confirmado após a perda de um nó.

Precisa das portas TCP locais `7401` a `7403` livres.

```sh
cd examples/05-replication
uv run main.py
```
