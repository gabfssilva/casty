# Spec 03 — Placement (token ring com vnodes)

Escopo: módulo puro de placement — dado o conjunto de membros vivos, decide dono e réplicas de qualquer key, deterministicamente, sem coordenação e em 0 hops. Não cobre roteamento (etapa 4) nem replicação (etapa 5).

## 1. Tokens

Espaço de tokens: u64 (`0 .. 2^64-1`), circular.

```
node_token(node_id, i) = u64_be(blake2b(node_id.bytes ++ u32_be(i), digest_size=8))   i ∈ [0, V)
key_token(key)         = u64_be(blake2b(key.encode("utf-8"), digest_size=8))
```

- `V` = vnodes por nó físico (default 128, configurável por cluster — **todos os nós precisam usar o mesmo V**).
- blake2b da stdlib, digest de 8 bytes, big-endian. A função é parte do protocolo: mudá-la é breaking change de cluster (nunca `hash()` builtin, que é salteado por processo).
- Colisão de token entre nós distintos: desempate determinístico por `node_id` (ordenação lexicográfica dos bytes).

## 2. Ring

`Ring.build(members, vnodes)` constrói a lista ordenada de `(token, node_id)` — V tokens por membro. Dado `key`:

- **Dono**: primeiro token ≥ `key_token(key)` (com wrap para o início).
- **Réplicas** (`replicas(key, n)`): caminha a ring a partir do dono coletando nós físicos **distintos** (pula vnodes de nós já coletados) até `n` ou esgotar os nós. Dono é sempre o primeiro. `n` maior que o número de nós → retorna todos.

## 3. Ranges

Unidade de handoff/replicação é o **token range** `(prev_token, token]` — o intervalo que cada vnode possui. `ranges_of(node_id)` devolve os ranges cujo dono é o nó. Ring de um nó só: o único nó possui o espaço inteiro.

`Ring` é imutável; mudança de view = construir outra ring. Propriedade garantida por construção (e verificada em teste): adicionar um nó só move keys para o nó novo; remover um nó só move as keys que eram dele.

## 4. Interface

```python
class Ring:
    @classmethod
    def build(cls, nodes: Iterable[uuid.UUID], *, vnodes: int = 128) -> Ring: ...
    def owner(self, key: str) -> uuid.UUID: ...
    def replicas(self, key: str, n: int) -> tuple[uuid.UUID, ...] :...  # dono primeiro
    def replicas_for_token(self, token: int, n: int) -> tuple[uuid.UUID, ...]: ...
    def ranges_of(self, node_id: uuid.UUID) -> list[TokenRange]: ...
    @property
    def nodes(self) -> frozenset[uuid.UUID]: ...
```

O módulo opera sobre `uuid.UUID`; a tradução Member ↔ node_id é da camada de cima.

## 5. Critérios de verificação da etapa

1. **Estabilidade do hash**: valores de token fixados em teste (guarda contra mudança acidental de protocolo).
2. **Uniformidade**: 8 nós × V=128 — fração do keyspace por nó (soma exata dos ranges) dentro de ±30% do ideal.
3. **Movimento mínimo**: join de 1 nó em cluster de 5 → toda key que mudou de dono foi para o nó novo; fração movida ≈ 1/6. Leave → só keys do nó removido mudam.
4. **Réplicas distintas**: `replicas(key, 3)` sempre devolve 3 nós físicos distintos; `n` > #nós → todos os nós, sem repetição.
5. mypy strict limpo.
