# Spec 02 — Membership

Escopo: overlay HyParView, broadcast Plumtree, ring state (member table com incarnations), suspicion/refutação, anti-entropy, leave gracioso. Interface para as camadas de cima: `alive_members() -> frozenset[Member]` + eventos de view. Não cobre placement (spec 03) nem lite members (etapa 4).

Todo o tráfego de membership vai no **canal de controle (stream 1)** das conexões do transporte (spec 01), `msg_type` 0x20–0x3F, sempre tell-style (`correlation_id = 0`) — respostas do protocolo são mensagens próprias, casadas pela máquina de estados, não pelo transporte.

## 1. Member table (ring state)

Cada nó mantém a member list completa:

```
Record { member: Member{node_id, addr}, incarnation: int, status: ALIVE | SUSPECT | DEAD | LEFT }
```

Merge de um record recebido (gossip ou anti-entropy) contra o local:

1. Nó desconhecido → insere (inclusive tombstones DEAD/LEFT — impedem ressurreição por gossip atrasado).
2. `incarnation` maior vence.
3. Empate de incarnation → precedência de status: `DEAD > LEFT > SUSPECT > ALIVE`.

Transições de status geram eventos de view (`joined`, `suspect`, `alive`, `dead`, `left`). `alive_members()` = self + records com status ALIVE ou SUSPECT (suspeito ainda não é morto).

Tombstones DEAD/LEFT são varridos após `tombstone_ttl` (default 60s).

**Refutação**: nó que vê SUSPECT ou DEAD sobre *si mesmo* com incarnation ≥ a própria incrementa a incarnation e faz broadcast de `NodeAlive`. É o subprotocolo de suspicion do SWIM sobre o overlay.

**Suspicion**: só vizinhos da *active view* suspeitam — queda de conexão de link ativo sem DISCONNECT prévio → marca SUSPECT + broadcast `NodeSuspect`. Todo nó com um SUSPECT não refutado em `suspicion_timeout` marca DEAD + broadcast `NodeDead` (idempotente; todos rodam o timer). Queda de conexão de data path (fora da active view) não gera suspeita.

## 2. HyParView

- **Active view**: conexões TCP simétricas, tamanho `active_view_size` (default 5, ~log(n)+1 para centenas de nós). Links ativos são as arestas do overlay: carregam gossip e detecção de falha.
- **Passive view**: `passive_view_size` (default 30), só endereços, sem conexão. Reserva para promoção.

Mensagens (payload sempre `@casty.message` via serde):

| msg_type | Nome | Semântica |
|---|---|---|
| 0x20 | JOIN `{member}` | joiner → seed. Seed adiciona à active view e propaga FORWARD_JOIN aos demais ativos |
| 0x21 | FORWARD_JOIN `{member, ttl}` | random walk; ttl=0 ou active view ≤ 1 → adiciona o joiner à active view (dial + NEIGHBOR); ttl == `passive_rwl` → adiciona à passive; decrementa e repassa a vizinho aleatório |
| 0x22 | NEIGHBOR `{member, priority}` | pedido de link ativo; `priority=high` (active view vazia) é sempre aceito |
| 0x23 | NEIGHBOR_REPLY `{accepted}` | recusa → solicitante tenta outro candidato da passive |
| 0x24 | DISCONNECT `{}` | remoção graciosa do link ativo (eviction por capacidade); receptor move o emissor para a passive, sem suspeita |
| 0x25 | SHUFFLE `{origin, sample, ttl}` | random walk; ttl>0 e active>1 → repassa; senão integra e responde |
| 0x26 | SHUFFLE_REPLY `{sample}` | amostra da passive do respondedor |

- **Join**: joiner dial a um seed e envia JOIN. Falha → próximo seed; todos falharam → erro ao usuário. O seed adiciona o joiner (evictando link aleatório com DISCONNECT se cheio), envia FORWARD_JOIN com `ttl=active_rwl` (default 6) a todos os ativos, e faz broadcast de `NodeJoined`; o joiner também broadcasta o próprio `NodeJoined` (merge é idempotente).
- **Link-up** (qualquer origem: JOIN, NEIGHBOR aceito, FORWARD_JOIN ttl=0): ambos os lados marcam o link, adicionam o peer ao Plumtree (eager) e trocam SYNC (bootstrap de estado do joiner é o mesmo mecanismo do anti-entropy).
- **Falha de vizinho**: remove da active, suspeita (acima), promove candidato aleatório da passive: dial + NEIGHBOR com `priority = high` se a active view ficou vazia. Recusa/falha → tenta outro.
- **Rejoin**: active view zerada (minoria isolada pós-partição, falha massiva) → a cada sweep o nó tenta re-entrar via JOIN por qualquer endereço conhecido (passive view, table, seeds originais). O SYNC do link-up expõe o registro DEAD sobre si; a refutação (incarnation+1) o ressuscita na view dos demais.
- **Shuffle**: a cada `shuffle_interval` (default 10s), amostra `{self} ∪ active[:k_a] ∪ passive[:k_p]` enviada a vizinho ativo aleatório com `ttl=passive_rwl`. Integração: adiciona à passive (nunca self nem membros da active), evictando preferencialmente os que enviamos na amostra.

## 3. Plumtree (broadcast)

Sobre a active view. Estado por nó: `eager ∪ lazy = active view`; inicial: todos eager.

| msg_type | Nome | Semântica |
|---|---|---|
| 0x27 | GOSSIP `{id, round, event}` | eager push do evento completo |
| 0x28 | IHAVE `{ids}` | lazy push: anúncio de ids |
| 0x29 | GRAFT `{id}` | pede o payload + promove o link a eager |
| 0x2A | PRUNE `{}` | demove o link a lazy |

- `id` = `node_id (16 bytes) + seq u64` do originador. Cache de vistos com TTL `seen_ttl` (default 60s).
- GOSSIP novo: entrega local (merge na table), repassa eager (exceto emissor), IHAVE aos lazy, emissor vira eager. Duplicado: PRUNE ao emissor (vira lazy).
- IHAVE de id não visto: agenda timer `graft_timeout` (default 0.5s); expirado sem o payload → GRAFT a quem anunciou (vira eager).
- GRAFT: emissor vira eager; se temos o payload, reenvia GOSSIP.
- Link ativo novo → eager; removido → some dos dois conjuntos.

Eventos de cluster (payload de GOSSIP): `NodeJoined{member, incarnation}`, `NodeLeft{node_id, incarnation}`, `NodeSuspect{node_id, incarnation}`, `NodeAlive{member, incarnation}`, `NodeDead{node_id, incarnation}`.

## 4. Anti-entropy

| msg_type | Nome | Semântica |
|---|---|---|
| 0x2B | SYNC `{records}` | table completa; receptor faz merge e responde |
| 0x2C | SYNC_REPLY `{records}` | merge sem resposta |

A cada `anti_entropy_interval` (default 30s) com vizinho ativo aleatório; também disparado em todo link-up (bootstrap do joiner). Repara perda de broadcast sob churn. Table completa é aceitável na escala alvo (centenas de nós × ~50 bytes/record); delta-sync fica fora da v1.

## 5. Leave gracioso

`leave()`: broadcast `NodeLeft{self, incarnation}` → aguarda flush → DISCONNECT aos vizinhos ativos → fecha. Receptores marcam LEFT sem janela de suspicion. Usado pelo `node.stop()` na etapa 4.

## 6. Interface

```python
class Membership:
    def alive_members(self) -> frozenset[Member]: ...   # inclui self
    def subscribe(self, cb: Callable[[ViewEvent], None]) -> Callable[[], None]: ...
    async def join(self, seeds: Sequence[str]) -> None: ...
    async def leave(self) -> None: ...

@dataclass(frozen=True)
class ViewEvent:
    kind: Literal["joined", "suspect", "alive", "dead", "left"]
    member: Member
```

O dono do transporte (Node, etapa 4; conftest nos testes da etapa 2) constrói Server + Pool e liga os handlers: `on_control → membership.handle_control`, `on_connection → membership.handle_connection`, `on_close → membership.handle_close`. Membership não abre listener próprio.

## 7. Configuração

```python
@casty.message
class MembershipConfig:
    active_view_size: int = 5
    passive_view_size: int = 30
    active_rwl: int = 6          # ARWL: ttl inicial do FORWARD_JOIN
    passive_rwl: int = 3         # PRWL: ttl do SHUFFLE / inserção na passive
    shuffle_interval: float = 10.0
    shuffle_active_sample: int = 3
    shuffle_passive_sample: int = 4
    suspicion_timeout: float = 5.0
    anti_entropy_interval: float = 30.0
    tombstone_ttl: float = 60.0
    graft_timeout: float = 0.5
    seen_ttl: float = 60.0
    sweep_interval: float = 0.25   # tick dos timers (suspicion, graft, GC)
    join_timeout: float = 10.0
```

## 8. Layout

```
src/casty/membership/
├── messages.py    # wire messages + msg_types 0x20–0x3F
├── table.py       # member table: merge, incarnations, transições — puro
├── hyparview.py   # bookkeeping das views (random injetável) — puro
├── plumtree.py    # eager/lazy, seen cache, comandos — puro
└── service.py     # asyncio: liga tudo sobre Pool/Server, timers
```

Os módulos puros devolvem *comandos* (enviar X a Y) em vez de fazer I/O — unit-testáveis sem rede, mesmo racional do mux sans-IO da spec 01.

## 9. Critérios de verificação da etapa

1. Cluster de N nós (TCP real em localhost): todos convergem para a mesma view de N membros.
2. Matar nós abruptamente → sobreviventes convergem (suspect → dead) dentro de `suspicion_timeout` + margem; evento `dead` emitido uma vez por observador.
3. Refutação: suspect falso sobre nó vivo → nó refuta com incarnation+1, todos convergem para ALIVE; nó não é expulso.
4. Leave gracioso → todos marcam LEFT sem evento suspect/dead.
5. Falha simultânea de 30% dos nós → overlay não particiona; sobreviventes convergem para a view correta.
6. Unit: merge da table (incarnation, precedência), views (capacidade, eviction, shuffle), plumtree (dup → prune, IHAVE+timeout → graft).
7. mypy strict limpo.
