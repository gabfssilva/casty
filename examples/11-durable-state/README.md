# Estado durável

Mostra um tipo declarado com `durable="write"`, cujo estado fica também no store do sistema: cada `state.set` retorna depois que o store guardou a escrita. Três nós no mesmo processo compartilham um store num arquivo SQLite (`casty.sqlite.SQLiteStore`), recebem depósitos e param todos juntos, o que perde todas as réplicas. Três nós novos iniciam sobre o mesmo arquivo e cada conta volta com o último saldo confirmado, enquanto um tipo mantido só em memória recomeça do zero. O código de `SQLiteStore`, em `src/casty/sqlite.py`, é também o formato de um store sobre qualquer outro banco: cada método é um único comando SQL.

Precisa das portas TCP locais `7451` a `7453` livres. O arquivo do store fica num diretório temporário.

```sh
cd examples/11-durable-state
uv run main.py
```
