# Spec 06 — Collections

Escopo: coleções distribuídas como açúcar sobre atores de shard. Nenhum mecanismo novo — placement, replicação, quórum e fencing são exatamente os da spec 05. Cada coleção é um facade tipado no cliente sobre uma classe de shard-actor comum.

## 1. Modelo comum

Uma coleção distribuída tem duas metades:

- **Shard-actor** — uma classe `@casty.actor` comum, com estado replicado por snapshot, HLC, fencing — tudo herdado. A classe é materializada por **tripla de replicação** `(replicas, write, read)`, com wire name autodescritivo:

  ```
  casty.<Kind>Shard[r3,wmajority,rdone]
  ```

  Qualquer nó que receba uma chamada para um wire name nesse formato materializa a classe localmente (`_sharded.ensure`), então a fábrica (`node.set`, `node.counter`, …) não precisa ter sido chamada em todos os nós. O contador de shards **não** entra no nome: afeta só o roteamento no chamador (código homogêneo cobre usar o mesmo valor em todo lugar).

- **Facade cliente** — roteia ops de item único por `blake2b(item_encodado) % shards` para o shard dono, e faz fan-out paralelo aos shards para agregações. Herda de `_sharded.ShardRouter`.

Keys, values e elementos são encodados isoladamente (`encode_raw`); decode é estrutural (`decode_any`). Tipos suportados: primitivos, containers, `@casty.message`.

O esqueleto compartilhado vive em `casty.collections._sharded`: `wire_name`, `register(prefix, factory)`, `materialize`, `ensure(wire_name)` e a base `ShardRouter` (`_shard_of`, `_call`, `_fanout`). Cada coleção registra seu prefixo em import time; `casty.collections.ensure` despacha por prefixo para qualquer coleção registrada.

Lite members (`casty.connect`) têm o mesmo facade — roteiam como qualquer chamada de ator.

## 2. Coleções sharded por item

Shard = `hash(item) % shards`. Herdam o modelo comum inteiro; escalam com o número de shards e de nós.

### 2.1 `Map[K, V]`

```python
node.map(name, *, replicas=3, write=MAJORITY, read=ONE, shards=32) -> Map[K, V]
```

Estado do shard: `dict[bytes, bytes]`. Shard de uma key = `hash(key)`.

```python
async def put(self, key: K, value: V) -> None
async def get(self, key: K) -> V | None
async def remove(self, key: K) -> bool          # True se existia
async def contains(self, key: K) -> bool
async def size(self) -> int                     # soma dos shards
async def clear(self) -> None
async def items(self) -> list[tuple[K, V]]      # materializa; para maps pequenos
```

### 2.2 `Set[T]`

```python
node.set(name, *, replicas=3, write=MAJORITY, read=ONE, shards=32) -> Set[T]
```

Estado do shard: `set[bytes]`. Shard de um elemento = `hash(elemento)`.

```python
async def add(self, item: T) -> bool            # True se novo
async def remove(self, item: T) -> bool         # True se existia
async def contains(self, item: T) -> bool
async def size(self) -> int
async def clear(self) -> None
async def items(self) -> list[T]
async def union(self, other: Set[T]) -> set[T]        # merge no cliente
async def intersection(self, other: Set[T]) -> set[T]
async def difference(self, other: Set[T]) -> set[T]
```

### 2.3 `MultiMap[K, V]`

key → conjunto de values. Todos os values de uma key co-localizados (shard por key), então `get(key)` é uma chamada.

```python
node.multimap(name, *, replicas=3, write=MAJORITY, read=ONE, shards=32) -> MultiMap[K, V]
```

Estado do shard: `dict[bytes, set[bytes]]`. Shard de uma key = `hash(key)`.

```python
async def put(self, key: K, value: V) -> bool           # True se novo par
async def remove(self, key: K, value: V) -> bool        # True se o par existia
async def get(self, key: K) -> list[V]
async def remove_key(self, key: K) -> int               # nº de values removidos
async def contains(self, key: K, value: V) -> bool
async def size(self) -> int                             # total de pares
async def clear(self) -> None
```

## 3. Coleções striped

Um valor lógico único dividido em stripes para evitar hotspot de escrita. Sem key: `add` cai num stripe rotativo no cliente, `get` soma o fan-out.

### 3.1 `Counter`

```python
node.counter(name, *, replicas=3, write=MAJORITY, read=ONE, stripes=32) -> Counter
```

Estado do shard: `value: int`.

```python
async def add(self, delta: int = 1) -> None     # cai num stripe
async def get(self) -> int                       # soma dos stripes
async def reset(self) -> None
```

## 4. Coleções single-owner

Um ator dono, sem sharding (`shards=1`). Ordem ou identidade única exigem serialização num só dono — não escalam dentro de uma instância; escalar = muitas instâncias nomeadas. A tripla `(replicas, write, read)` ainda vale (durabilidade e quórum herdados).

### 4.1 `Register[T]` (atomic ref)

Valor único nomeado com compare-and-set. CAS é correto de graça pelo single-writer (o dono serializa as escritas).

```python
node.register(name, *, replicas=3, write=MAJORITY, read=ONE) -> Register[T]
```

Estado do shard: `value: bytes | None`.

```python
async def get(self) -> T | None
async def set(self, value: T) -> None
async def compare_and_set(self, expected: T | None, new: T) -> bool
async def get_and_set(self, value: T) -> T | None
```

### 4.2 `Queue[T]`

FIFO correto por ter um dono único. **Não escala numa fila só** — todo o tráfego da fila vai para um nó. Escalar throughput = particionar em múltiplas filas nomeadas no nível da aplicação.

```python
node.queue(name, *, replicas=3, write=MAJORITY, read=ONE) -> Queue[T]
```

Estado do shard: `items: list[bytes]`.

```python
async def offer(self, item: T) -> None
async def poll(self) -> T | None                 # remove e retorna a cabeça
async def peek(self) -> T | None
async def size(self) -> int
async def drain(self, max_items: int) -> list[T] # remove e retorna até max_items
async def clear(self) -> None
```

### 4.3 `Semaphore`

Semáforo contável distribuído: `capacity` permits sob um dono único que serializa os grants. Cada permit é um **lease** — `acquire` devolve um **fencing token** monotônico com TTL; o holder renova antes de expirar ou o permit é reclamado (expiração lazy, no acesso). Bloqueio é **client-side**: o mailbox do ator é serial, então esperar por um permit dentro do handler travaria o `release` que o liberaria. `acquire(timeout=...)` faz poll com backoff.

```python
node.semaphore(name, *, capacity, replicas=3, write=MAJORITY, read=ONE) -> Semaphore
```

Estado do shard: `next_token: int`, `leases: dict[int, tuple[permits, expiry]]`. `available = capacity - Σ permits dos leases vivos`. `capacity` é por-instância (o facade passa em cada chamada); `ttl` é por-acquire.

```python
async def try_acquire(self, n: int = 1, *, ttl: float = 30.0) -> Lease | None
async def acquire(self, n: int = 1, *, ttl: float = 30.0, timeout: float | None = None) -> Lease
async def available(self) -> int
```

`Lease` (frozen, async context manager) carrega `token: int` e:

```python
async def renew(self, ttl: float = 30.0) -> bool   # False se já expirou (permit perdido)
async def release(self) -> bool                     # False se já expirado/liberado
```

TTL usa wall-clock (`time.time()`) — deadlines absolutos precisam sobreviver a failover pra outro nó; regra: `ttl >> skew` entre relógios. O fencing token é entregue ao recurso protegido pela app, que rejeita holder stale (lease expirado). Failover de nó é coberto pela replicação (o lease commitado em quórum é restaurado no novo dono); o que TTL/fencing cobre é a morte do **cliente** que segura o lease.

### 4.4 `Lock`

Exclusão mútua = `Semaphore(capacity=1)`. Mesmo shard, nomes de lock. Async context manager: `async with node.lock(name):` adquire (bloqueando até `timeout`) e solta no exit.

```python
node.lock(name, *, ttl=30.0, timeout=None, replicas=3, write=MAJORITY, read=ONE) -> Lock

async def try_lock(self, *, ttl: float | None = None) -> Lease | None
async def acquire(self, *, ttl: float | None = None, timeout: float | None = None) -> Lease
async def locked(self) -> bool
```

## 5. Fora do modelo

- **`List[T]` indexada** — `insert(i, x)` desloca todos os índices; o mapa índice→shard quebra. Só faria sentido single-owner, com baixo valor. Fora da v1.
- **CRDTs** — resolvem multi-writer concorrente, exatamente o que a arquitetura evita de propósito (single-writer por key, sem vector clocks). Seriam um modo de replicação novo, não uma coleção. Fora da v1.

## 6. Critérios de verificação (por coleção)

1. API completa em cluster de 3 nós, chamada de nós distintos e de lite member; values tipados incluindo `@casty.message`.
2. Mesmos testes de falha da etapa 5: morte do dono não perde estado (`replicas=3, w=MAJORITY`); mutação na minoria particionada → `QuorumUnavailableError`.
3. Específicos: distribuição por shards/nós (Map/Set/MultiMap); soma correta sob concorrência (Counter); CAS sob contenção rejeita corretamente (Register); ordem FIFO preservada por dono único (Queue); bound de capacity respeitado sob contenção, TTL reclama permit, fencing token estritamente crescente (Semaphore); exclusão mútua e release no exit do context manager (Lock).
4. mypy strict: `s: casty.Set[int] = node.set("x")` tipa `add/contains` estaticamente.
