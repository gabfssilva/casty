# Plano: Persistência e Replicação de Estado de Atores

## Resumo Executivo

Este documento descreve a arquitetura e implementação de um sistema de persistência e replicação de estado para atores no Casty, inspirado no Infinispan. O objetivo é fornecer durabilidade e alta disponibilidade para atores sharded, de forma transparente para o desenvolvedor.

**Princípios Fundamentais:**
1. **Transparência** - O desenvolvedor escreve atores normalmente; persistência é configuração
2. **Flexibilidade** - Múltiplos níveis de consistência para diferentes casos de uso
3. **Extensibilidade** - Interface plugável para diferentes backends de storage
4. **Performance** - Otimizações como batching, compressão e delta updates

---

## 1. Estado Atual e Problemas

### 1.1 O Que Temos Hoje

```
┌─────────────────────────────────────────────────────────────────┐
│                    Arquitetura Atual                            │
└─────────────────────────────────────────────────────────────────┘

    accounts["user-1"].send(Deposit(100))
                │
                ▼
    ┌───────────────────┐
    │     Raft Log      │  ← Mensagens replicadas (ordenação) ✓
    │  ShardedMessage   │
    └───────────────────┘
                │
                ▼
    ┌───────────────────┐
    │   Primary Node    │  ← Estado apenas aqui (memória) ✗
    │   balance = 100   │
    └───────────────────┘
                │
                ▼
         Resposta ao cliente
```

**Problema:** Se o nó primário cai, o estado `balance = 100` é perdido.

### 1.2 Limitações Atuais

| Aspecto | Estado Atual | Impacto |
|---------|--------------|---------|
| Mensagens | Replicadas via Raft | Ordenação consistente ✓ |
| Estado do ator | Apenas memória local | Perda em crash ✗ |
| Failover | Shard realocado | Estado perdido ✗ |
| Recuperação | Não implementada | Ator reinicia do zero ✗ |

### 1.3 Cenários de Falha

**Cenário 1: Crash do nó primário**
```
Tempo T1: accounts["user-1"] tem balance=150 no Node-A
Tempo T2: Node-A crash
Tempo T3: Shard realocado para Node-B
Tempo T4: accounts["user-1"] recriado com balance=0 ← DADOS PERDIDOS
```

**Cenário 2: Network partition**
```
Tempo T1: Node-A isolado da rede
Tempo T2: Cluster elege novo líder sem Node-A
Tempo T3: Shards de Node-A realocados
Tempo T4: Node-A volta - estados divergentes possíveis
```

---

## 2. Arquitetura Proposta

### 2.1 Visão Geral

```
┌─────────────────────────────────────────────────────────────────┐
│                    Arquitetura Proposta                         │
└─────────────────────────────────────────────────────────────────┘

    accounts["user-1"].send(Deposit(100))
                │
                ▼
    ┌───────────────────┐
    │     Raft Log      │  ← Mensagens replicadas
    │  ShardedMessage   │
    └───────────────────┘
                │
                ▼
    ┌───────────────────┐
    │   Primary Node    │  ← Processa mensagem
    │   balance = 100   │
    └───────────────────┘
                │
                ▼
    ┌───────────────────┐
    │   State Sync      │  ← NOVO: Replicação de estado
    │  EntityStateCmd   │
    └───────────────────┘
                │
       ┌────────┼────────┐
       ▼        ▼        ▼
    ┌──────┐ ┌──────┐ ┌──────┐
    │Backup│ │Backup│ │Store │  ← Cópias em memória + persistência
    │Node 1│ │Node 2│ │SQLite│
    └──────┘ └──────┘ └──────┘
```

### 2.2 Componentes Principais

```
┌─────────────────────────────────────────────────────────────────┐
│                      Componentes                                │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    DistributedActorSystem                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                   ShardingManager                        │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │   │
│  │  │ EntityActor │  │ StateSync   │  │ FailoverManager │  │   │
│  │  │  Manager    │  │   Manager   │  │                 │  │   │
│  │  └─────────────┘  └─────────────┘  └─────────────────┘  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              │                                  │
│                              ▼                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    ActorStore                            │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐  │   │
│  │  │ SQLite   │  │ Postgres │  │  Redis   │  │   S3    │  │   │
│  │  │  Store   │  │  Store   │  │  Store   │  │  Store  │  │   │
│  │  └──────────┘  └──────────┘  └──────────┘  └─────────┘  │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### 2.3 Fluxo de Dados Detalhado

```
┌─────────────────────────────────────────────────────────────────┐
│                 Fluxo: Processamento de Mensagem                │
└─────────────────────────────────────────────────────────────────┘

 Cliente                Primary              Backups            Storage
    │                      │                    │                  │
    │  1. send(Deposit)    │                    │                  │
    │─────────────────────>│                    │                  │
    │                      │                    │                  │
    │                      │ 2. Raft append     │                  │
    │                      │   (ShardedMsg)     │                  │
    │                      │- - - - - - - - - ->│                  │
    │                      │                    │                  │
    │                      │ 3. Process msg     │                  │
    │                      │   balance += 100   │                  │
    │                      │                    │                  │
    │                      │ 4. Extract state   │                  │
    │                      │   get_state()      │                  │
    │                      │                    │                  │
    │                      │ 5. Raft append     │                  │
    │                      │   (StateUpdate)    │                  │
    │                      │- - - - - - - - - ->│                  │
    │                      │                    │                  │
    │                      │ 6. Persist (async/sync based on mode) │
    │                      │───────────────────────────────────────>│
    │                      │                    │                  │
    │  7. Ack/Response     │                    │                  │
    │<─────────────────────│                    │                  │
    │                      │                    │                  │


┌─────────────────────────────────────────────────────────────────┐
│                    Fluxo: Failover                              │
└─────────────────────────────────────────────────────────────────┘

 Primary (crash)        New Primary           Backups            Storage
    │                      │                    │                  │
    │ ══╪══ CRASH ══╪══    │                    │                  │
    X                      │                    │                  │
                           │                    │                  │
                           │ 1. Shard assigned  │                  │
                           │<───────────────────│                  │
                           │                    │                  │
                           │ 2. Check backup    │                  │
                           │   memory state     │                  │
                           │<───────────────────│                  │
                           │                    │                  │
                           │ 3. If not found,   │                  │
                           │   load from store  │                  │
                           │<──────────────────────────────────────│
                           │                    │                  │
                           │ 4. Restore state   │                  │
                           │   set_state()      │                  │
                           │                    │                  │
                           │ 5. Resume          │                  │
                           │   processing       │                  │
                           │                    │                  │
```

---

## 3. Níveis de Consistência (Write Modes)

### 3.1 Matriz de Modos

```
┌─────────────────────────────────────────────────────────────────┐
│                    Write Modes                                  │
└─────────────────────────────────────────────────────────────────┘

┌────────────────────┬─────────┬─────────┬─────────┬─────────────┐
│       Modo         │ Memória │ Storage │ Latência│ Durabilidade│
├────────────────────┼─────────┼─────────┼─────────┼─────────────┤
│ MEMORY_ONLY        │ Sync    │ Nenhum  │ ~1ms    │ Baixa       │
│ ASYNC              │ Sync    │ Async   │ ~1ms    │ Média       │
│ SYNC_LOCAL         │ Sync    │ Local   │ ~5ms    │ Média-Alta  │
│ SYNC_LOCAL_ASYNC   │ Sync    │ L+Async │ ~5ms    │ Alta        │
│ SYNC_QUORUM        │ Sync    │ Quorum  │ ~10ms   │ Muito Alta  │
│ SYNC_ALL           │ Sync    │ Todos   │ ~20ms   │ Máxima      │
└────────────────────┴─────────┴─────────┴─────────┴─────────────┘
```

### 3.2 Detalhamento de Cada Modo

#### MEMORY_ONLY
```
Uso: Desenvolvimento, dados efêmeros, caches

    Primary ──────> Backup1 ──────> Backup2
       │              │                │
       │    Raft      │     Raft       │
       │   (state)    │    (state)     │
       ▼              ▼                ▼
    [Memory]       [Memory]        [Memory]

    Storage: Nenhum

Garantias:
- Estado sobrevive crash de minoria dos nós
- Perda total se maioria dos nós caem simultaneamente
```

#### ASYNC
```
Uso: Alta performance, tolerância a perda eventual

    Primary ──────> Backup1 ──────> Backup2
       │              │                │
       │    Raft      │     Raft       │
       │   (state)    │    (state)     │
       ▼              ▼                ▼
    [Memory]       [Memory]        [Memory]
       │
       │ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ▶ [Storage]
              (background batch)

Garantias:
- Mesmo que MEMORY_ONLY + eventual persistência
- Janela de perda: async_flush_interval (default 100ms)
```

#### SYNC_LOCAL
```
Uso: Dados importantes, nó único confiável

    Primary ──────> Backup1 ──────> Backup2
       │              │                │
       │    Raft      │     Raft       │
       │   (state)    │    (state)     │
       ▼              ▼                ▼
    [Memory]       [Memory]        [Memory]
       │
       │ ◀─────────────────────────────▶ [Storage]
       │         (sync write)               (local)
       │
    Ack só após storage confirmar

Garantias:
- Estado persistido localmente antes de ack
- Recuperação garantida se disco do primário sobrevive
```

#### SYNC_LOCAL_ASYNC_BACKUP (Recomendado)
```
Uso: Produção geral, bom equilíbrio

    Primary ──────> Backup1 ──────> Backup2
       │              │                │
       │    Raft      │     Raft       │
       │   (state)    │    (state)     │
       ▼              ▼                ▼
    [Memory]       [Memory]        [Memory]
       │              │                │
       ▼              │ ─ ─ ─ ▶        │ ─ ─ ─ ▶
    [Storage]      [Storage]       [Storage]
      sync           async           async
       │
    Ack após storage local

Garantias:
- Persistência local garantida
- Backups eventualmente persistem
- Recuperação de qualquer nó que sobreviver
```

#### SYNC_QUORUM
```
Uso: Dados críticos, alta disponibilidade

    Primary ──────> Backup1 ──────> Backup2
       │              │                │
       │    Raft      │     Raft       │
       │   (state)    │    (state)     │
       ▼              ▼                ▼
    [Memory]       [Memory]        [Memory]
       │              │                │
       ▼              ▼                │ ─ ─ ▶
    [Storage]      [Storage]       [Storage]
      sync           sync            async
       │              │
       └──────────────┘
              │
         Ack após 2/3 confirmarem

Garantias:
- Maioria dos nós persistiu antes de ack
- Sobrevive crash de minoria + perda de disco
```

#### SYNC_ALL
```
Uso: Dados financeiros, compliance, auditoria

    Primary ──────> Backup1 ──────> Backup2
       │              │                │
       │    Raft      │     Raft       │
       │   (state)    │    (state)     │
       ▼              ▼                ▼
    [Memory]       [Memory]        [Memory]
       │              │                │
       ▼              ▼                ▼
    [Storage]      [Storage]       [Storage]
      sync           sync            sync
       │              │                │
       └──────────────┴────────────────┘
                      │
              Ack após TODOS confirmarem

Garantias:
- Todos os nós persistiram antes de ack
- Máxima durabilidade
- Maior latência
```

### 3.3 Casos de Uso por Modo

```
┌─────────────────────────────────────────────────────────────────┐
│                 Recomendações por Caso de Uso                   │
└─────────────────────────────────────────────────────────────────┘

┌────────────────────────┬─────────────────────────────────────────┐
│ Caso de Uso            │ Modo Recomendado                        │
├────────────────────────┼─────────────────────────────────────────┤
│ Cache de sessão        │ MEMORY_ONLY                             │
│ Contadores analytics   │ ASYNC                                   │
│ Carrinho de compras    │ SYNC_LOCAL                              │
│ Inventário             │ SYNC_LOCAL_ASYNC_BACKUP                 │
│ Saldo de conta         │ SYNC_QUORUM                             │
│ Transações financeiras │ SYNC_ALL                                │
│ Registros de auditoria │ SYNC_ALL                                │
└────────────────────────┴─────────────────────────────────────────┘
```

---

## 4. Interface ActorStore

### 4.1 Contrato Base

```python
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator
from dataclasses import dataclass

@dataclass
class StateEntry:
    """Entrada de estado persistido."""
    entity_type: str
    entity_id: str
    state: bytes
    version: int
    timestamp: float
    metadata: dict[str, Any] | None = None


class ActorStore(ABC):
    """Interface abstrata para persistência de estado de atores.

    Implementações devem ser thread-safe e suportar operações
    concorrentes de múltiplos nós.

    Ciclo de vida:
        store = SQLiteActorStore(path)
        await store.start()      # Inicializa conexões
        ...                      # Usa a store
        await store.stop()       # Cleanup gracioso

    Transações:
        Implementações devem garantir atomicidade por operação.
        Para operações em batch, usar save_batch().
    """

    # ─────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Inicializa a store (conexões, pools, etc).

        Chamado uma vez antes de qualquer operação.
        Deve ser idempotente.
        """
        pass

    async def stop(self) -> None:
        """Encerra a store graciosamente.

        Deve:
        - Aguardar operações em andamento
        - Flush de buffers pendentes
        - Fechar conexões
        """
        pass

    async def health_check(self) -> bool:
        """Verifica se a store está saudável.

        Usado para readiness probes e monitoramento.
        """
        return True

    # ─────────────────────────────────────────────────────────────
    # CRUD Operations
    # ─────────────────────────────────────────────────────────────

    @abstractmethod
    async def save(
        self,
        entity_type: str,
        entity_id: str,
        state: bytes,
        version: int,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        """Persiste o estado de um ator.

        Args:
            entity_type: Tipo do ator (ex: "account")
            entity_id: ID da entidade (ex: "user-123")
            state: Estado serializado (msgpack bytes)
            version: Versão monotônica para CAS
            metadata: Dados extras (timestamps, tags, etc)

        Returns:
            True se salvou, False se versão conflitante

        Semantics:
            - Se entity não existe: cria
            - Se version > stored_version: atualiza
            - Se version <= stored_version: ignora (retorna False)

        Thread Safety:
            Múltiplas chamadas concorrentes para mesmo entity
            devem ser serializadas pela implementação.
        """
        ...

    @abstractmethod
    async def load(
        self,
        entity_type: str,
        entity_id: str,
    ) -> StateEntry | None:
        """Carrega o estado de um ator.

        Args:
            entity_type: Tipo do ator
            entity_id: ID da entidade

        Returns:
            StateEntry se encontrado, None caso contrário
        """
        ...

    @abstractmethod
    async def delete(
        self,
        entity_type: str,
        entity_id: str,
    ) -> bool:
        """Remove o estado de um ator.

        Returns:
            True se removeu, False se não existia
        """
        ...

    @abstractmethod
    async def exists(
        self,
        entity_type: str,
        entity_id: str,
    ) -> bool:
        """Verifica se um ator existe na store."""
        ...

    # ─────────────────────────────────────────────────────────────
    # Batch Operations
    # ─────────────────────────────────────────────────────────────

    async def save_batch(
        self,
        entries: list[tuple[str, str, bytes, int]],
    ) -> int:
        """Salva múltiplos estados em batch.

        Args:
            entries: Lista de (entity_type, entity_id, state, version)

        Returns:
            Número de entradas salvas com sucesso

        Default implementation saves one by one.
        Override for optimized batch operations.
        """
        saved = 0
        for entity_type, entity_id, state, version in entries:
            if await self.save(entity_type, entity_id, state, version):
                saved += 1
        return saved

    async def load_batch(
        self,
        keys: list[tuple[str, str]],
    ) -> dict[tuple[str, str], StateEntry]:
        """Carrega múltiplos estados em batch.

        Args:
            keys: Lista de (entity_type, entity_id)

        Returns:
            Dict mapeando chaves encontradas para StateEntry
        """
        results = {}
        for entity_type, entity_id in keys:
            entry = await self.load(entity_type, entity_id)
            if entry:
                results[(entity_type, entity_id)] = entry
        return results

    async def delete_batch(
        self,
        keys: list[tuple[str, str]],
    ) -> int:
        """Remove múltiplos estados em batch.

        Returns:
            Número de entradas removidas
        """
        deleted = 0
        for entity_type, entity_id in keys:
            if await self.delete(entity_type, entity_id):
                deleted += 1
        return deleted

    # ─────────────────────────────────────────────────────────────
    # Query Operations
    # ─────────────────────────────────────────────────────────────

    @abstractmethod
    async def list_entities(
        self,
        entity_type: str,
        limit: int = 1000,
        offset: int = 0,
    ) -> list[str]:
        """Lista entity_ids de um tipo.

        Args:
            entity_type: Tipo para filtrar
            limit: Máximo de resultados
            offset: Pular N primeiros

        Returns:
            Lista de entity_ids
        """
        ...

    @abstractmethod
    async def count_entities(
        self,
        entity_type: str,
    ) -> int:
        """Conta entidades de um tipo."""
        ...

    async def iter_entities(
        self,
        entity_type: str,
        batch_size: int = 100,
    ) -> AsyncIterator[StateEntry]:
        """Itera sobre todas entidades de um tipo.

        Útil para migrações e backups.
        Usa paginação internamente para não sobrecarregar memória.
        """
        offset = 0
        while True:
            ids = await self.list_entities(entity_type, batch_size, offset)
            if not ids:
                break
            for entity_id in ids:
                entry = await self.load(entity_type, entity_id)
                if entry:
                    yield entry
            offset += len(ids)

    # ─────────────────────────────────────────────────────────────
    # Maintenance Operations
    # ─────────────────────────────────────────────────────────────

    async def compact(self) -> None:
        """Compacta a store (remove tombstones, rebuilds indexes).

        Implementação opcional, pode ser no-op.
        """
        pass

    async def backup(self, path: str) -> None:
        """Cria backup da store.

        Implementação opcional.
        """
        raise NotImplementedError("Backup not supported by this store")

    async def get_stats(self) -> dict[str, Any]:
        """Retorna estatísticas da store.

        Útil para monitoramento e debugging.
        """
        return {}
```

### 4.2 Implementação SQLite

```python
import aiosqlite
import time
from pathlib import Path
from contextlib import asynccontextmanager

class SQLiteActorStore(ActorStore):
    """Store padrão usando SQLite.

    Características:
    - Zero configuração
    - ACID compliant
    - WAL mode para concorrência
    - Suporta múltiplos processos (via file locking)

    Limitações:
    - Single node (não distribuído)
    - Performance limitada em alto throughput
    - Para produção pesada, considere PostgresActorStore

    Schema:
        actor_states (
            entity_type TEXT,
            entity_id TEXT,
            state BLOB,
            version INTEGER,
            created_at REAL,
            updated_at REAL,
            metadata TEXT,  -- JSON
            PRIMARY KEY (entity_type, entity_id)
        )

    Exemplo:
        store = SQLiteActorStore("./data/actors.db")
        await store.start()

        await store.save("account", "user-1", state_bytes, version=1)
        entry = await store.load("account", "user-1")

        await store.stop()
    """

    def __init__(
        self,
        path: str | Path = "actors.db",
        *,
        busy_timeout: float = 5.0,
        cache_size: int = -64000,  # 64MB
    ):
        """
        Args:
            path: Caminho do arquivo SQLite
            busy_timeout: Timeout para locks (segundos)
            cache_size: Tamanho do cache (-N = N KB, +N = N pages)
        """
        self._path = Path(path)
        self._busy_timeout = busy_timeout
        self._cache_size = cache_size
        self._conn: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()

    async def start(self) -> None:
        # Criar diretório se necessário
        self._path.parent.mkdir(parents=True, exist_ok=True)

        # Conectar
        self._conn = await aiosqlite.connect(
            self._path,
            timeout=self._busy_timeout,
        )

        # Configurar para performance
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.execute(f"PRAGMA cache_size={self._cache_size}")
        await self._conn.execute("PRAGMA temp_store=MEMORY")
        await self._conn.execute("PRAGMA mmap_size=268435456")  # 256MB

        # Criar schema
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS actor_states (
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                state BLOB NOT NULL,
                version INTEGER NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                metadata TEXT,
                PRIMARY KEY (entity_type, entity_id)
            )
        """)

        await self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_entity_type
            ON actor_states(entity_type)
        """)

        await self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_updated_at
            ON actor_states(updated_at)
        """)

        await self._conn.commit()

    async def stop(self) -> None:
        if self._conn:
            # Checkpoint WAL
            await self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            await self._conn.close()
            self._conn = None

    async def health_check(self) -> bool:
        if not self._conn:
            return False
        try:
            async with self._conn.execute("SELECT 1") as cursor:
                await cursor.fetchone()
            return True
        except Exception:
            return False

    async def save(
        self,
        entity_type: str,
        entity_id: str,
        state: bytes,
        version: int,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        now = time.time()
        metadata_json = json.dumps(metadata) if metadata else None

        async with self._write_lock:
            # Upsert com verificação de versão
            cursor = await self._conn.execute(
                """
                INSERT INTO actor_states
                    (entity_type, entity_id, state, version, created_at, updated_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (entity_type, entity_id) DO UPDATE SET
                    state = excluded.state,
                    version = excluded.version,
                    updated_at = excluded.updated_at,
                    metadata = excluded.metadata
                WHERE actor_states.version < excluded.version
                """,
                (entity_type, entity_id, state, version, now, now, metadata_json),
            )
            await self._conn.commit()
            return cursor.rowcount > 0

    async def load(
        self,
        entity_type: str,
        entity_id: str,
    ) -> StateEntry | None:
        async with self._conn.execute(
            """
            SELECT state, version, created_at, updated_at, metadata
            FROM actor_states
            WHERE entity_type = ? AND entity_id = ?
            """,
            (entity_type, entity_id),
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None

            return StateEntry(
                entity_type=entity_type,
                entity_id=entity_id,
                state=row[0],
                version=row[1],
                timestamp=row[3],
                metadata=json.loads(row[4]) if row[4] else None,
            )

    async def delete(
        self,
        entity_type: str,
        entity_id: str,
    ) -> bool:
        async with self._write_lock:
            cursor = await self._conn.execute(
                "DELETE FROM actor_states WHERE entity_type = ? AND entity_id = ?",
                (entity_type, entity_id),
            )
            await self._conn.commit()
            return cursor.rowcount > 0

    async def exists(
        self,
        entity_type: str,
        entity_id: str,
    ) -> bool:
        async with self._conn.execute(
            "SELECT 1 FROM actor_states WHERE entity_type = ? AND entity_id = ? LIMIT 1",
            (entity_type, entity_id),
        ) as cursor:
            return await cursor.fetchone() is not None

    async def save_batch(
        self,
        entries: list[tuple[str, str, bytes, int]],
    ) -> int:
        if not entries:
            return 0

        now = time.time()
        data = [
            (et, eid, state, ver, now, now, None)
            for et, eid, state, ver in entries
        ]

        async with self._write_lock:
            cursor = await self._conn.executemany(
                """
                INSERT INTO actor_states
                    (entity_type, entity_id, state, version, created_at, updated_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (entity_type, entity_id) DO UPDATE SET
                    state = excluded.state,
                    version = excluded.version,
                    updated_at = excluded.updated_at
                WHERE actor_states.version < excluded.version
                """,
                data,
            )
            await self._conn.commit()
            return cursor.rowcount

    async def list_entities(
        self,
        entity_type: str,
        limit: int = 1000,
        offset: int = 0,
    ) -> list[str]:
        async with self._conn.execute(
            """
            SELECT entity_id FROM actor_states
            WHERE entity_type = ?
            ORDER BY entity_id
            LIMIT ? OFFSET ?
            """,
            (entity_type, limit, offset),
        ) as cursor:
            rows = await cursor.fetchall()
            return [row[0] for row in rows]

    async def count_entities(self, entity_type: str) -> int:
        async with self._conn.execute(
            "SELECT COUNT(*) FROM actor_states WHERE entity_type = ?",
            (entity_type,),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def compact(self) -> None:
        async with self._write_lock:
            await self._conn.execute("VACUUM")
            await self._conn.execute("ANALYZE")

    async def backup(self, path: str) -> None:
        async with self._write_lock:
            await self._conn.execute("VACUUM INTO ?", (path,))

    async def get_stats(self) -> dict[str, Any]:
        stats = {}

        async with self._conn.execute(
            "SELECT entity_type, COUNT(*), SUM(LENGTH(state)) FROM actor_states GROUP BY entity_type"
        ) as cursor:
            rows = await cursor.fetchall()
            stats["entities_by_type"] = {
                row[0]: {"count": row[1], "total_bytes": row[2]}
                for row in rows
            }

        async with self._conn.execute("PRAGMA page_count") as cursor:
            row = await cursor.fetchone()
            stats["page_count"] = row[0]

        async with self._conn.execute("PRAGMA page_size") as cursor:
            row = await cursor.fetchone()
            stats["page_size"] = row[0]

        stats["file_size"] = self._path.stat().st_size if self._path.exists() else 0

        return stats
```

### 4.3 Outras Implementações Planejadas

```python
# PostgreSQL - Para produção de alta escala
class PostgresActorStore(ActorStore):
    """
    Vantagens:
    - Alta concorrência (MVCC)
    - Replicação nativa
    - Suporte a JSON/JSONB para metadata
    - Connection pooling

    Uso:
        store = PostgresActorStore(
            dsn="postgresql://user:pass@host/db",
            pool_size=10,
        )
    """
    pass


# Redis - Para baixa latência
class RedisActorStore(ActorStore):
    """
    Vantagens:
    - Sub-millisecond latency
    - Cluster mode nativo
    - TTL para expiração automática

    Desvantagens:
    - Dados em memória (custo)
    - Persistência eventual (RDB/AOF)

    Uso:
        store = RedisActorStore(
            url="redis://localhost:6379",
            key_prefix="casty:",
        )
    """
    pass


# S3 - Para snapshots e backup
class S3ActorStore(ActorStore):
    """
    Vantagens:
    - Custo baixo para grande volume
    - Durabilidade 99.999999999%
    - Versionamento nativo

    Desvantagens:
    - Alta latência (~100ms)
    - Não adequado para uso primário

    Uso:
        store = S3ActorStore(
            bucket="my-bucket",
            prefix="actors/",
        )
    """
    pass


# Composite - Combina múltiplas stores
class CompositeActorStore(ActorStore):
    """
    Usa múltiplas stores em camadas:
    - Leitura: tenta em ordem até encontrar
    - Escrita: escreve em todas

    Exemplo: Redis (cache) + PostgreSQL (persistência)

    Uso:
        store = CompositeActorStore(
            primary=RedisActorStore(...),
            secondary=PostgresActorStore(...),
        )
    """
    pass
```

---

## 5. Protocolo de Estado para Atores

### 5.1 Interface Stateful

```python
from typing import Protocol, Any, runtime_checkable

@runtime_checkable
class StatefulActor(Protocol):
    """Protocolo para atores com estado persistível.

    Atores que implementam este protocolo têm controle total
    sobre como seu estado é serializado e restaurado.

    Para a maioria dos casos, usar PersistentActorMixin é suficiente.
    """

    def __casty_get_state__(self) -> dict[str, Any]:
        """Retorna o estado serializável do ator.

        O dict retornado será serializado com msgpack.
        Todos os valores devem ser serializáveis:
        - Primitivos (int, float, str, bool, None, bytes)
        - Listas e dicts (com valores serializáveis)
        - Dataclasses

        Exemplo:
            def __casty_get_state__(self) -> dict[str, Any]:
                return {
                    "balance": self.balance,
                    "transactions": self.transactions[-100:],  # Últimas 100
                }
        """
        ...

    def __casty_set_state__(self, state: dict[str, Any]) -> None:
        """Restaura o estado do ator.

        Chamado durante failover ou restart.
        O dict recebido é o mesmo retornado por __casty_get_state__.

        Exemplo:
            def __casty_set_state__(self, state: dict[str, Any]) -> None:
                self.balance = state["balance"]
                self.transactions = state.get("transactions", [])
        """
        ...


class PersistentActorMixin:
    """Mixin que fornece implementação padrão de persistência.

    Comportamento padrão:
    - get_state: retorna __dict__ excluindo campos com prefixo _
    - set_state: atualiza __dict__ com os valores recebidos

    Customização via class attributes:
    - __casty_include_fields__: Set de campos para incluir (whitelist)
    - __casty_exclude_fields__: Set de campos para excluir (blacklist)

    Exemplo:
        class Account(PersistentActorMixin, Actor[Deposit | GetBalance]):
            __casty_exclude_fields__ = {"_cache", "_temp_data"}

            def __init__(self, entity_id: str):
                self.entity_id = entity_id
                self.balance = 0.0
                self._cache = {}  # Excluído por prefixo _
                self._temp_data = None  # Excluído explicitamente
    """

    # Override para whitelist (se definido, só estes são incluídos)
    __casty_include_fields__: set[str] | None = None

    # Override para blacklist (campos adicionais a excluir)
    __casty_exclude_fields__: set[str] = set()

    def __casty_get_state__(self) -> dict[str, Any]:
        if self.__casty_include_fields__ is not None:
            # Whitelist mode
            return {
                k: v for k, v in self.__dict__.items()
                if k in self.__casty_include_fields__
            }
        else:
            # Blacklist mode (default)
            return {
                k: v for k, v in self.__dict__.items()
                if not k.startswith("_")
                and k not in self.__casty_exclude_fields__
            }

    def __casty_set_state__(self, state: dict[str, Any]) -> None:
        for k, v in state.items():
            setattr(self, k, v)


# Decorator alternativo para simplicidade
def persistent(*fields: str):
    """Decorator que marca campos específicos para persistência.

    Uso:
        @persistent("balance", "transaction_count")
        class Account(Actor[Deposit | GetBalance]):
            def __init__(self, entity_id: str):
                self.entity_id = entity_id  # Não persistido
                self.balance = 0.0          # Persistido
                self.transaction_count = 0  # Persistido
    """
    def decorator(cls):
        cls.__casty_include_fields__ = set(fields)

        if not hasattr(cls, "__casty_get_state__"):
            def get_state(self) -> dict[str, Any]:
                return {k: getattr(self, k) for k in fields if hasattr(self, k)}
            cls.__casty_get_state__ = get_state

        if not hasattr(cls, "__casty_set_state__"):
            def set_state(self, state: dict[str, Any]) -> None:
                for k, v in state.items():
                    if k in fields:
                        setattr(self, k, v)
            cls.__casty_set_state__ = set_state

        return cls
    return decorator
```

### 5.2 Serialização de Estado

```python
def serialize_actor_state(actor: Any) -> bytes:
    """Serializa o estado de um ator para bytes.

    Ordem de precedência:
    1. Se tem __casty_get_state__, usa
    2. Se é dataclass, usa asdict()
    3. Senão, usa __dict__ (excluindo _prefixed)
    """
    if hasattr(actor, "__casty_get_state__"):
        state_dict = actor.__casty_get_state__()
    elif is_dataclass(actor) and not isinstance(actor, type):
        state_dict = asdict(actor)
    else:
        state_dict = {
            k: v for k, v in actor.__dict__.items()
            if not k.startswith("_")
        }

    return serialize(state_dict)


def restore_actor_state(actor: Any, state_bytes: bytes, type_registry: dict) -> None:
    """Restaura o estado de um ator a partir de bytes."""
    state_dict = deserialize(state_bytes, type_registry)

    if hasattr(actor, "__casty_set_state__"):
        actor.__casty_set_state__(state_dict)
    else:
        for k, v in state_dict.items():
            setattr(actor, k, v)
```

---

## 6. Tipos Raft para Replicação

### 6.1 Comandos de Estado

```python
from dataclasses import dataclass
from typing import Literal, Any

@dataclass(frozen=True, slots=True)
class EntityStateUpdate:
    """Comando Raft para replicar estado de entidade.

    Fluxo:
    1. Primary processa mensagem, atualiza estado
    2. Primary cria EntityStateUpdate com novo estado
    3. EntityStateUpdate vai para Raft log
    4. Todos os nós aplicam (backups guardam em _backup_states)

    Versionamento:
    - version é monotônico por (entity_type, entity_id)
    - Nós só aplicam se version > versão local
    - Previne aplicação out-of-order após partições
    """
    entity_type: str
    entity_id: str
    state: bytes  # Estado serializado via msgpack
    version: int  # Versão monotônica para ordenação
    primary_node: str  # Nó que originou a atualização
    timestamp: float  # Timestamp do primary


@dataclass(frozen=True, slots=True)
class EntityStateDelete:
    """Comando Raft para remover estado de entidade.

    Usado quando entidade é explicitamente removida ou
    durante garbage collection de entidades inativas.
    """
    entity_type: str
    entity_id: str
    version: int
    reason: Literal["explicit", "gc", "migration"]


@dataclass(frozen=True, slots=True)
class EntityStateBatch:
    """Batch de atualizações de estado para eficiência.

    Agrupa múltiplas atualizações em um único comando Raft.
    Usado quando async_batch_size é atingido ou flush_interval expira.
    """
    updates: tuple[EntityStateUpdate, ...]
    batch_id: str  # UUID para deduplicação


@dataclass(frozen=True, slots=True)
class EntityStateRequest:
    """Requisição de estado para sincronização.

    Usado quando:
    - Novo nó assume shard (failover)
    - Nó reconecta após partição
    - Verificação de consistência
    """
    entity_type: str
    entity_id: str
    requesting_node: str
    last_known_version: int  # Versão mais recente que o nó conhece


@dataclass(frozen=True, slots=True)
class EntityStateResponse:
    """Resposta com estado atual de entidade."""
    entity_type: str
    entity_id: str
    state: bytes | None  # None se entidade não existe
    version: int
    source_node: str
```

### 6.2 Estados em Memória

```python
@dataclass
class BackupState:
    """Estado mantido em nós de backup."""
    state: bytes
    version: int
    primary_node: str
    last_updated: float
    access_count: int = 0  # Para LRU eviction


class BackupStateManager:
    """Gerencia estados de backup em memória.

    Responsabilidades:
    - Armazenar estados recebidos via Raft
    - Fornecer estado para failover
    - Eviction de estados antigos (LRU)
    - Sincronização com storage
    """

    def __init__(
        self,
        max_entries: int = 100_000,
        max_memory_mb: int = 512,
    ):
        self._states: dict[str, BackupState] = {}  # key = "type:id"
        self._max_entries = max_entries
        self._max_memory = max_memory_mb * 1024 * 1024
        self._current_memory = 0
        self._lock = asyncio.Lock()

    async def store(self, update: EntityStateUpdate) -> None:
        """Armazena ou atualiza estado de backup."""
        key = f"{update.entity_type}:{update.entity_id}"

        async with self._lock:
            existing = self._states.get(key)

            # Só atualiza se versão maior
            if existing and existing.version >= update.version:
                return

            # Calcula delta de memória
            new_size = len(update.state)
            old_size = len(existing.state) if existing else 0
            delta = new_size - old_size

            # Eviction se necessário
            while (
                self._current_memory + delta > self._max_memory
                or len(self._states) >= self._max_entries
            ):
                await self._evict_one()

            # Armazena
            self._states[key] = BackupState(
                state=update.state,
                version=update.version,
                primary_node=update.primary_node,
                last_updated=update.timestamp,
            )
            self._current_memory += delta

    async def get(
        self,
        entity_type: str,
        entity_id: str,
    ) -> tuple[bytes, int] | None:
        """Obtém estado de backup."""
        key = f"{entity_type}:{entity_id}"

        async with self._lock:
            state = self._states.get(key)
            if state:
                state.access_count += 1
                return (state.state, state.version)
            return None

    async def delete(self, entity_type: str, entity_id: str) -> None:
        """Remove estado de backup."""
        key = f"{entity_type}:{entity_id}"

        async with self._lock:
            state = self._states.pop(key, None)
            if state:
                self._current_memory -= len(state.state)

    async def _evict_one(self) -> None:
        """Remove entrada menos usada (LRU)."""
        if not self._states:
            return

        # Encontra entrada com menor access_count e mais antiga
        min_key = min(
            self._states.keys(),
            key=lambda k: (
                self._states[k].access_count,
                self._states[k].last_updated,
            ),
        )

        state = self._states.pop(min_key)
        self._current_memory -= len(state.state)

    def get_stats(self) -> dict[str, Any]:
        """Retorna estatísticas do backup manager."""
        return {
            "entry_count": len(self._states),
            "memory_used": self._current_memory,
            "memory_limit": self._max_memory,
            "utilization": self._current_memory / self._max_memory,
        }
```

---

## 7. Integração com DistributedActorSystem

### 7.1 Configuração

```python
@dataclass
class ReplicationConfig:
    """Configuração de replicação e persistência."""

    # ─────────────────────────────────────────────────────────────
    # Replicação em Memória
    # ─────────────────────────────────────────────────────────────

    # Número de cópias em memória (além do primário)
    # 0 = sem backup, 1 = um backup, 2 = dois backups (recomendado)
    memory_replicas: int = 2

    # Tamanho máximo do cache de backup por nó
    backup_max_entries: int = 100_000
    backup_max_memory_mb: int = 512

    # ─────────────────────────────────────────────────────────────
    # Persistência
    # ─────────────────────────────────────────────────────────────

    # Modo de escrita (ver WriteMode enum)
    write_mode: WriteMode = WriteMode.SYNC_LOCAL_ASYNC_BACKUP

    # Para ASYNC: tamanho do batch antes de flush
    async_batch_size: int = 100

    # Para ASYNC: intervalo máximo entre flushes
    async_flush_interval: float = 0.1  # 100ms

    # Timeout para operações síncronas
    sync_timeout: float = 5.0

    # ─────────────────────────────────────────────────────────────
    # Failover
    # ─────────────────────────────────────────────────────────────

    # Tentar restaurar de backups antes de storage
    prefer_memory_restore: bool = True

    # Timeout para buscar estado em failover
    failover_timeout: float = 10.0

    # ─────────────────────────────────────────────────────────────
    # Otimizações
    # ─────────────────────────────────────────────────────────────

    # Comprimir estados maiores que N bytes
    compress_threshold: int = 1024

    # Algoritmo de compressão
    compression: Literal["none", "zstd", "lz4"] = "zstd"

    # Agrupar múltiplas atualizações do mesmo ator
    coalesce_updates: bool = True
    coalesce_window_ms: int = 10


# Configurações predefinidas para casos comuns
class ReplicationPresets:
    """Presets de configuração para casos comuns."""

    @staticmethod
    def development() -> ReplicationConfig:
        """Para desenvolvimento local - mínima sobrecarga."""
        return ReplicationConfig(
            memory_replicas=0,
            write_mode=WriteMode.MEMORY_ONLY,
        )

    @staticmethod
    def high_performance() -> ReplicationConfig:
        """Para alta performance - aceita perda eventual."""
        return ReplicationConfig(
            memory_replicas=1,
            write_mode=WriteMode.ASYNC,
            async_batch_size=500,
            async_flush_interval=0.5,
        )

    @staticmethod
    def balanced() -> ReplicationConfig:
        """Equilíbrio entre performance e durabilidade (recomendado)."""
        return ReplicationConfig(
            memory_replicas=2,
            write_mode=WriteMode.SYNC_LOCAL_ASYNC_BACKUP,
        )

    @staticmethod
    def high_durability() -> ReplicationConfig:
        """Para dados críticos - máxima durabilidade."""
        return ReplicationConfig(
            memory_replicas=2,
            write_mode=WriteMode.SYNC_QUORUM,
        )

    @staticmethod
    def maximum_safety() -> ReplicationConfig:
        """Para compliance/financeiro - zero perda."""
        return ReplicationConfig(
            memory_replicas=2,
            write_mode=WriteMode.SYNC_ALL,
            compression="none",  # Facilita auditoria
        )
```

### 7.2 Inicialização do Sistema

```python
class DistributedActorSystem:
    def __init__(
        self,
        host: str,
        port: int,
        *,
        seeds: list[str] | None = None,
        expected_cluster_size: int = 3,
        # NOVO: Configurações de persistência
        actor_store: ActorStore | None = None,
        replication: ReplicationConfig | None = None,
        # ... outros parâmetros existentes
    ):
        # ... código existente ...

        # Inicializar subsistema de persistência
        self._actor_store = actor_store
        self._replication_config = replication or ReplicationConfig()

        # Manager de backups em memória
        self._backup_manager = BackupStateManager(
            max_entries=self._replication_config.backup_max_entries,
            max_memory_mb=self._replication_config.backup_max_memory_mb,
        )

        # Versionamento de estados
        self._state_versions: dict[str, int] = {}

        # Queue para escritas async
        self._persistence_queue: asyncio.Queue[EntityStateUpdate] = asyncio.Queue()
        self._persistence_task: asyncio.Task | None = None

    async def __aenter__(self):
        # ... código existente ...

        # Iniciar actor store se configurado
        if self._actor_store:
            await self._actor_store.start()

        # Iniciar flusher para modo async
        if self._replication_config.write_mode == WriteMode.ASYNC:
            self._persistence_task = asyncio.create_task(
                self._persistence_flusher()
            )

        return self

    async def __aexit__(self, *args):
        # Parar flusher
        if self._persistence_task:
            self._persistence_task.cancel()
            try:
                await self._persistence_task
            except asyncio.CancelledError:
                pass

        # Flush final
        await self._flush_persistence_queue()

        # Fechar store
        if self._actor_store:
            await self._actor_store.stop()

        # ... código existente ...
```

### 7.3 Fluxo de Processamento Atualizado

```python
async def _apply_sharded_message(self, msg: ShardedMessage) -> None:
    """Processa mensagem sharded com replicação de estado."""

    config = self._sharded_entities.get(msg.entity_type)
    if not config:
        log.warning(f"Unknown sharded entity type: {msg.entity_type}")
        return

    # Verificar se somos responsáveis por este shard
    shard_id = self._get_shard_id(msg.entity_id, config.num_shards)
    responsible_node = self._get_node_for_shard(msg.entity_type, shard_id)

    if responsible_node != self.node_id:
        return

    # Obter ou criar o ator (com restauração de estado)
    entity_key = f"{msg.entity_type}:{msg.entity_id}"
    actor_ref = await self._get_or_create_entity_with_state(
        msg.entity_type,
        msg.entity_id,
    )

    # Deserializar payload
    payload = msg.payload
    if isinstance(payload, bytes):
        payload = deserialize(payload, self._cluster.type_registry)

    # Processar mensagem
    if msg.request_id and msg.reply_to:
        try:
            result = await actor_ref.ask(payload)
            # Replicar estado ANTES de enviar resposta (se sync)
            await self._replicate_entity_state(
                msg.entity_type,
                msg.entity_id,
                actor_ref,
            )
            await self._send_shard_response(
                msg.reply_to,
                msg.request_id,
                result,
                success=True,
            )
        except Exception as e:
            log.error(f"Error processing sharded ask: {e}")
            await self._send_shard_response(
                msg.reply_to,
                msg.request_id,
                None,
                success=False,
            )
    else:
        await actor_ref.send(payload)
        # Replicar estado após processamento
        await self._replicate_entity_state(
            msg.entity_type,
            msg.entity_id,
            actor_ref,
        )


async def _get_or_create_entity_with_state(
    self,
    entity_type: str,
    entity_id: str,
) -> ActorRef:
    """Obtém ou cria entidade, restaurando estado se disponível."""

    entity_key = f"{entity_type}:{entity_id}"

    # Já existe localmente?
    if entity_key in self._entity_actors:
        return self._entity_actors[entity_key]

    # Obter classe do ator
    actor_cls = self._cluster.type_registry.get(f"__sharded_cls__{entity_type}")
    if actor_cls is None:
        try:
            config = self._sharded_entities[entity_type]
            actor_cls = _import_type(config.actor_cls_name)
            self._cluster.type_registry[f"__sharded_cls__{entity_type}"] = actor_cls
        except Exception as e:
            log.error(f"Actor class not found for {entity_type}: {e}")
            raise

    # Tentar restaurar estado
    state_dict = await self._try_restore_entity_state(entity_type, entity_id)

    # Criar o ator
    config = self._sharded_entities[entity_type]
    actor_ref = await ActorSystem.spawn(
        self,
        actor_cls,
        name=entity_key,
        entity_id=entity_id,
        **config.kwargs,
    )

    # Restaurar estado se encontrado
    if state_dict:
        actor = actor_ref._actor
        restore_actor_state(actor, state_dict)
        log.info(f"Restored state for {entity_key}")

    self._entity_actors[entity_key] = actor_ref
    return actor_ref


async def _try_restore_entity_state(
    self,
    entity_type: str,
    entity_id: str,
) -> dict[str, Any] | None:
    """Tenta restaurar estado de backups ou storage."""

    config = self._replication_config

    # 1. Tentar backup em memória (mais rápido)
    if config.prefer_memory_restore:
        result = await self._backup_manager.get(entity_type, entity_id)
        if result:
            state_bytes, version = result
            log.debug(f"Restored {entity_type}:{entity_id} from memory backup (v{version})")
            return deserialize(state_bytes, self._cluster.type_registry)

    # 2. Tentar storage persistente
    if self._actor_store:
        try:
            entry = await asyncio.wait_for(
                self._actor_store.load(entity_type, entity_id),
                timeout=config.failover_timeout,
            )
            if entry:
                log.debug(f"Restored {entity_type}:{entity_id} from storage (v{entry.version})")
                return deserialize(entry.state, self._cluster.type_registry)
        except asyncio.TimeoutError:
            log.warning(f"Timeout loading {entity_type}:{entity_id} from storage")
        except Exception as e:
            log.error(f"Error loading {entity_type}:{entity_id} from storage: {e}")

    # 3. Nenhum estado encontrado
    log.debug(f"No state found for {entity_type}:{entity_id}, starting fresh")
    return None


async def _replicate_entity_state(
    self,
    entity_type: str,
    entity_id: str,
    actor_ref: ActorRef,
) -> None:
    """Replica o estado do ator para backups e storage."""

    config = self._replication_config

    # Se MEMORY_ONLY e sem replicas, nada a fazer
    if config.write_mode == WriteMode.MEMORY_ONLY and config.memory_replicas == 0:
        return

    # Extrair e serializar estado
    actor = actor_ref._actor
    state_bytes = serialize_actor_state(actor)

    # Comprimir se necessário
    if config.compress_threshold and len(state_bytes) > config.compress_threshold:
        state_bytes = self._compress_state(state_bytes, config.compression)

    # Incrementar versão
    key = f"{entity_type}:{entity_id}"
    version = self._state_versions.get(key, 0) + 1
    self._state_versions[key] = version

    # Criar comando de atualização
    update = EntityStateUpdate(
        entity_type=entity_type,
        entity_id=entity_id,
        state=state_bytes,
        version=version,
        primary_node=self.node_id,
        timestamp=time.time(),
    )

    # Aplicar baseado no modo de escrita
    await self._apply_write_mode(update, config)


async def _apply_write_mode(
    self,
    update: EntityStateUpdate,
    config: ReplicationConfig,
) -> None:
    """Aplica a atualização de acordo com o write mode configurado."""

    match config.write_mode:
        case WriteMode.MEMORY_ONLY:
            # Apenas replicar em memória via Raft
            if config.memory_replicas > 0:
                await self.append_command(update)

        case WriteMode.ASYNC:
            # Replicar em memória + queue para persistência async
            if config.memory_replicas > 0:
                await self.append_command(update)
            await self._persistence_queue.put(update)

        case WriteMode.SYNC_LOCAL:
            # Replicar em memória + persistir local sync
            tasks = []
            if config.memory_replicas > 0:
                tasks.append(self.append_command(update))
            if self._actor_store:
                tasks.append(self._actor_store.save(
                    update.entity_type,
                    update.entity_id,
                    update.state,
                    update.version,
                ))
            await asyncio.gather(*tasks)

        case WriteMode.SYNC_LOCAL_ASYNC_BACKUP:
            # Persistir local sync + replicar async
            tasks = []
            if self._actor_store:
                await self._actor_store.save(
                    update.entity_type,
                    update.entity_id,
                    update.state,
                    update.version,
                )
            if config.memory_replicas > 0:
                # Fire and forget para memória
                asyncio.create_task(self.append_command(update))

        case WriteMode.SYNC_QUORUM:
            # Persistir e replicar, esperar quorum
            tasks = []
            if self._actor_store:
                tasks.append(self._actor_store.save(
                    update.entity_type,
                    update.entity_id,
                    update.state,
                    update.version,
                ))
            if config.memory_replicas > 0:
                # Raft já garante quorum
                tasks.append(self.append_command(update))

            await asyncio.wait_for(
                asyncio.gather(*tasks),
                timeout=config.sync_timeout,
            )

        case WriteMode.SYNC_ALL:
            # Persistir em todos os nós
            # Isso requer enviar para storage de todos os peers
            tasks = []

            # Local
            if self._actor_store:
                tasks.append(self._actor_store.save(
                    update.entity_type,
                    update.entity_id,
                    update.state,
                    update.version,
                ))

            # Replicar via Raft (que propaga para todos)
            if config.memory_replicas > 0:
                tasks.append(self.append_command(update))

            # TODO: Propagar para storage de outros nós

            await asyncio.wait_for(
                asyncio.gather(*tasks),
                timeout=config.sync_timeout,
            )


async def _persistence_flusher(self) -> None:
    """Background task que flush a queue de persistência."""

    config = self._replication_config
    batch: list[EntityStateUpdate] = []
    last_flush = time.time()

    while True:
        try:
            # Esperar próximo item ou timeout
            try:
                update = await asyncio.wait_for(
                    self._persistence_queue.get(),
                    timeout=config.async_flush_interval,
                )
                batch.append(update)
            except asyncio.TimeoutError:
                pass

            # Verificar se deve fazer flush
            should_flush = (
                len(batch) >= config.async_batch_size
                or (batch and time.time() - last_flush >= config.async_flush_interval)
            )

            if should_flush and batch:
                await self._flush_batch(batch)
                batch = []
                last_flush = time.time()

        except asyncio.CancelledError:
            # Flush final antes de sair
            if batch:
                await self._flush_batch(batch)
            raise
        except Exception as e:
            log.error(f"Error in persistence flusher: {e}")
            await asyncio.sleep(1)


async def _flush_batch(self, batch: list[EntityStateUpdate]) -> None:
    """Persiste um batch de atualizações."""

    if not self._actor_store or not batch:
        return

    entries = [
        (u.entity_type, u.entity_id, u.state, u.version)
        for u in batch
    ]

    try:
        saved = await self._actor_store.save_batch(entries)
        log.debug(f"Flushed {saved}/{len(batch)} state updates to storage")
    except Exception as e:
        log.error(f"Error flushing batch to storage: {e}")
        # Re-queue failed items
        for update in batch:
            await self._persistence_queue.put(update)
```

### 7.4 Aplicação de Comandos Raft

```python
async def _apply_sharding_command(self, cmd: Any) -> None:
    """Apply a sharding command to local state."""

    # ... código existente para RegisterShardedEntityCommand, ShardAllocationCommand ...

    # NOVO: Aplicar atualização de estado
    if isinstance(cmd, EntityStateUpdate):
        # Só armazenar se não somos o primário (backups)
        if cmd.primary_node != self.node_id:
            await self._backup_manager.store(cmd)
            log.debug(
                f"Stored backup state for {cmd.entity_type}:{cmd.entity_id} "
                f"v{cmd.version} from {cmd.primary_node[:8]}"
            )

    elif isinstance(cmd, EntityStateDelete):
        await self._backup_manager.delete(cmd.entity_type, cmd.entity_id)
        if self._actor_store:
            await self._actor_store.delete(cmd.entity_type, cmd.entity_id)
        log.debug(f"Deleted state for {cmd.entity_type}:{cmd.entity_id}")

    elif isinstance(cmd, EntityStateBatch):
        for update in cmd.updates:
            if update.primary_node != self.node_id:
                await self._backup_manager.store(update)
```

---

## 8. Uso pelo Desenvolvedor

### 8.1 Caso Simples (Zero Config)

```python
from casty import Actor, Context, DistributedActorSystem

@dataclass
class Deposit:
    amount: float

@dataclass
class GetBalance:
    pass

# Ator completamente normal - nada especial!
class Account(Actor[Deposit | GetBalance]):
    def __init__(self, entity_id: str):
        self.entity_id = entity_id
        self.balance = 0.0

    async def receive(self, msg, ctx):
        match msg:
            case Deposit(amount):
                self.balance += amount
            case GetBalance():
                ctx.reply(self.balance)


async def main():
    # Sistema com defaults - já tem persistência!
    async with DistributedActorSystem(
        "0.0.0.0", 8001,
        expected_cluster_size=3,
    ) as system:
        accounts = await system.spawn(Account, name="account", shards=100)

        await accounts["user-1"].send(Deposit(100))
        balance = await accounts["user-1"].ask(GetBalance())
        print(f"Balance: {balance}")  # 100.0
```

### 8.2 Com Configuração Explícita

```python
from casty import Actor, Context, DistributedActorSystem
from casty.storage import (
    SQLiteActorStore,
    ReplicationConfig,
    ReplicationPresets,
    WriteMode,
)

async def main():
    # Opção 1: Usar preset
    system = DistributedActorSystem(
        "0.0.0.0", 8001,
        expected_cluster_size=3,
        replication=ReplicationPresets.high_durability(),
    )

    # Opção 2: Configuração customizada
    system = DistributedActorSystem(
        "0.0.0.0", 8001,
        expected_cluster_size=3,
        actor_store=SQLiteActorStore("./data/actors.db"),
        replication=ReplicationConfig(
            memory_replicas=2,
            write_mode=WriteMode.SYNC_QUORUM,
            async_batch_size=200,
            compress_threshold=2048,
        ),
    )

    async with system:
        accounts = await system.spawn(Account, name="account", shards=100)
        # ...
```

### 8.3 Com Estado Customizado

```python
from casty import Actor, Context
from casty.persistence import PersistentActorMixin, persistent

# Opção 1: Usando mixin com configuração
class Account(PersistentActorMixin, Actor[Deposit | GetBalance]):
    # Excluir campos específicos
    __casty_exclude_fields__ = {"_session_cache"}

    def __init__(self, entity_id: str):
        self.entity_id = entity_id
        self.balance = 0.0
        self.transaction_history = []
        self._session_cache = {}  # Não persistido

    async def receive(self, msg, ctx):
        # ...


# Opção 2: Usando decorator
@persistent("balance", "transaction_count")  # Só estes campos
class Counter(Actor[Increment | GetCount]):
    def __init__(self, entity_id: str):
        self.entity_id = entity_id  # Não persistido
        self.balance = 0.0          # Persistido
        self.transaction_count = 0  # Persistido
        self.temp_data = None       # Não persistido


# Opção 3: Controle total
class ComplexActor(Actor[SomeMsg]):
    def __init__(self, entity_id: str):
        self.entity_id = entity_id
        self.important_state = {}
        self._derived_state = {}  # Recalculado

    def __casty_get_state__(self) -> dict[str, Any]:
        # Serializar apenas o essencial
        return {
            "important": self.important_state,
            "version": 2,  # Para migrations
        }

    def __casty_set_state__(self, state: dict[str, Any]) -> None:
        self.important_state = state["important"]
        # Recalcular estado derivado
        self._derived_state = self._compute_derived()
```

### 8.4 Monitoramento

```python
async def main():
    async with DistributedActorSystem(...) as system:
        # Estatísticas de backup
        backup_stats = system._backup_manager.get_stats()
        print(f"Backup entries: {backup_stats['entry_count']}")
        print(f"Memory used: {backup_stats['memory_used'] / 1024 / 1024:.1f} MB")

        # Estatísticas de storage
        if system._actor_store:
            store_stats = await system._actor_store.get_stats()
            print(f"Stored entities: {store_stats}")

        # Health check
        if system._actor_store:
            healthy = await system._actor_store.health_check()
            print(f"Storage healthy: {healthy}")
```

---

## 9. Plano de Implementação

### Fase 1: Replicação em Memória (1-2 semanas)

**Objetivo:** Estado replicado para N nós em memória

**Tarefas:**
1. [ ] Criar tipos Raft: `EntityStateUpdate`, `EntityStateDelete`
2. [ ] Implementar `BackupStateManager`
3. [ ] Adicionar `_replicate_entity_state()` após processamento
4. [ ] Implementar `_try_restore_entity_state()` para failover
5. [ ] Adicionar `ReplicationConfig` básico (só `memory_replicas`)
6. [ ] Testes: failover com restore de estado

**Entregável:** Atores sobrevivem crash de nó único

### Fase 2: Interface ActorStore (1 semana)

**Objetivo:** Abstração para persistência plugável

**Tarefas:**
1. [ ] Definir interface `ActorStore`
2. [ ] Implementar `SQLiteActorStore`
3. [ ] Integrar com `DistributedActorSystem`
4. [ ] Testes: save/load/delete básico

**Entregável:** Persistência SQLite funcional

### Fase 3: Write Modes (1-2 semanas)

**Objetivo:** Múltiplos níveis de consistência

**Tarefas:**
1. [ ] Implementar `WriteMode` enum
2. [ ] Background flusher para `ASYNC`
3. [ ] Lógica de quorum para `SYNC_QUORUM`
4. [ ] Propagação para `SYNC_ALL`
5. [ ] Testes: cada modo individualmente
6. [ ] Benchmarks: latência vs durabilidade

**Entregável:** Todos os write modes funcionais

### Fase 4: Protocolo de Estado (1 semana)

**Objetivo:** Controle fino sobre serialização

**Tarefas:**
1. [ ] Definir protocolo `StatefulActor`
2. [ ] Implementar `PersistentActorMixin`
3. [ ] Criar decorator `@persistent`
4. [ ] Documentação e exemplos

**Entregável:** API ergonômica para desenvolvedores

### Fase 5: Otimizações (1-2 semanas)

**Objetivo:** Performance em produção

**Tarefas:**
1. [ ] Compressão de estado (zstd/lz4)
2. [ ] Batching de atualizações
3. [ ] Delta updates (só mudanças)
4. [ ] Coalescing de updates frequentes
5. [ ] Benchmarks comparativos

**Entregável:** Performance aceitável para produção

### Fase 6: Stores Adicionais (opcional)

**Objetivo:** Mais opções de storage

**Tarefas:**
1. [ ] `PostgresActorStore`
2. [ ] `RedisActorStore`
3. [ ] `S3ActorStore` (para snapshots)
4. [ ] `CompositeActorStore`

**Entregável:** Ecossistema de stores

---

## 10. Considerações de Design

### 10.1 Trade-offs

| Aspecto | Decisão | Razão |
|---------|---------|-------|
| Serialização | msgpack | Já usado no projeto, bom equilíbrio tamanho/velocidade |
| Compressão | zstd default | Melhor ratio que lz4, velocidade aceitável |
| Versionamento | Monotônico simples | Evita complexidade de vector clocks |
| LRU eviction | Por access_count | Simples e efetivo para cache de backups |
| Batch size | 100 default | Bom equilíbrio latência/throughput |

### 10.2 Não Escopo (Futuro)

- **Event Sourcing completo**: Replay de todos eventos desde T0
- **CRDT para conflitos**: Resolução automática em partições
- **Geo-replicação**: Clusters multi-região
- **Encryption at rest**: Criptografia de estados persistidos
- **Compaction de log**: Garbage collection de estados antigos

### 10.3 Riscos e Mitigações

| Risco | Impacto | Mitigação |
|-------|---------|-----------|
| Overhead de serialização | Performance | Compressão, batching |
| Crescimento de memória | OOM | LRU eviction, limits |
| Partições de rede | Inconsistência | Versionamento, Raft |
| Storage lento | Latência | Modos async, timeouts |

---

## 11. Métricas de Sucesso

### 11.1 Funcionalidade

- [ ] Estado sobrevive crash de 1 nó em cluster de 3
- [ ] Failover < 5s para restaurar estado
- [ ] Zero perda de dados em modo SYNC_ALL
- [ ] API transparente (código existente funciona)

### 11.2 Performance

- [ ] Overhead < 10% em modo MEMORY_ONLY
- [ ] Overhead < 20% em modo SYNC_LOCAL
- [ ] Throughput > 10k ops/s em modo ASYNC
- [ ] P99 latência < 50ms em modo SYNC_QUORUM

### 11.3 Operacional

- [ ] Métricas expostas para monitoramento
- [ ] Health checks funcionais
- [ ] Documentação completa
- [ ] Exemplos para cada modo

---

## Apêndice A: Referências

- [Infinispan Documentation](https://infinispan.org/docs/stable/)
- [Akka Persistence](https://doc.akka.io/docs/akka/current/typed/persistence.html)
- [Microsoft Orleans Grain Persistence](https://learn.microsoft.com/en-us/dotnet/orleans/grains/grain-persistence/)
- [Raft Paper](https://raft.github.io/raft.pdf)

## Apêndice B: Glossário

- **Primary**: Nó responsável por processar mensagens de um shard
- **Backup**: Nó que mantém cópia do estado para failover
- **Shard**: Partição lógica de entidades
- **Entity**: Instância individual de um ator sharded
- **State**: Dados em memória de um ator
- **Version**: Número monotônico para ordenação de estados
- **Write Mode**: Nível de garantia de durabilidade
- **Quorum**: Maioria dos nós (N/2 + 1)
