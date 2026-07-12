# casty — design

Biblioteca Python de modelo de atores com clusterização, servindo de primitivo para distributed collections.

## Princípios

- Sem nó líder. Nós homogêneos; papéis de "primário" existem apenas por partição da keyspace, eleitos por posição na ring — nunca por protocolo de eleição.
- Python 3.12+, totalmente tipado. A API expõe tipos estáticos verificáveis (mypy/pyright).
- TCP com protocolo binário próprio, TLS configurável. Cluster e mensagens de usuário na mesma camada remota.
- Atores virtuais como primitivo único; collections são açúcar por cima.
- Tudo parametrizável: cada timeout, tamanho de view, intervalo de protocolo é um knob com default documentado.

## Decisões

| Tema | Decisão |
|---|---|
| Modelo de ator | Virtual actors (estilo Orleans): identidade = (tipo, key), ativação sob demanda no nó dono, desativação por idle (timeout configurável por classe) |
| API | Proxy de métodos tipado — classe comum com métodos async, `node.actor(Cls, key)` retorna proxy tipado como `Cls` |
| Escala alvo | Centenas de nós |
| Membership | **HyParView + Plumtree + disseminação de ring** (estilo Partisan). HyParView mantém o overlay conectado; Plumtree faz broadcast eficiente dos eventos de cluster; cada nó reconstrói a member list completa localmente. Detalhes abaixo |
| Placement | **Token ring com vnodes** (estilo Cassandra/Riak). Determinístico dado a view, sem tabela coordenada, resolução em 0 hops. Detalhes abaixo |
| Consistência | Quórum R/W configurável por classe de ator / collection |
| Replicação | Primary-based: o dono do range é o único coordenador de escrita (single writer por key) |
| Granularidade | Snapshot completo após cada handler que mutou estado (map msgpack dos fields anotados). Delta/log de métodos descartados: menos bytes não paga a superfície extra de bug |
| Versionamento | HLC (hybrid logical clock) por key, incrementado pelo dono. Vector clocks desnecessários: não há multi-coordenador no caminho comum |
| Ativação única | Eventual (estilo Orleans): handshake de ativação com quórum das réplicas; duplicata em view divergente é detectada e resolvida via HLC |
| Split-brain | Fencing por quórum de réplicas: dono que não alcança W-1 backups rejeita escritas |
| Transporte | TCP + protocolo próprio **multiplexado** (desenho do yamux/HashiCorp, wire nosso): streams independentes na mesma conexão com flow control por créditos — request/reply, fire-and-forget e bulk multi-GB sem head-of-line blocking. asyncio, TLS via `ssl.SSLContext` opcional, conexões sob demanda com pool (data path fora do overlay) |
| Serialização | `@casty.message` marca dataclass como serializável e a registra (wire name estável, overridável). `@casty.actor` usa a mesma maquinaria: o snapshot de estado do ator É uma message |
| Estado do ator | Fields anotados na classe definem o estado — fonte da verdade para `__state__()` / `__restore__()`, gerados pelo decorator. Snapshot = map msgpack `{field: valor}` |
| Slots | `@casty.actor` e `@casty.message` geram `slots=True`: atributo não declarado levanta `AttributeError` — sem drift silencioso |
| Estado transiente | `casty.transient()` marca fields fora do `__state__`; reconstruídos a cada ativação |
| Lifecycle hooks | `@casty.activate` / `@casty.deactivate` em métodos (nome livre): validados em import time, fora da interface do proxy. `activate` roda a cada ativação (inclusive reativação pós-migração, estado já restaurado) |
| Concorrência | Clássica: um handler por vez por ator, mailbox FIFO (unbounded por default; tamanho máximo e política configuráveis). Reentrância proibida — ciclo de ask (A→B→A) é detectado via call-chain id e levanta erro, em vez de deadlock silencioso |
| Supervisão | Supervisor global (com override por classe) como política de falha, adaptado a atores virtuais — ver seção abaixo |
| Lite members | `casty.connect()`: participa do membership e conhece a ring (rota em 1 hop), mas fora do placement — não hospeda atores |
| Modo local | `casty.local()`: sistema de atores 100% em processo — sem bind, sem `Server`/`Pool`/membership/replicação. Segundo filho de `ActorSystem` (base que dá `actor()`/`map()` sobre `call_actor`), ao lado do `Node` distribuído; despacha direto no `ActorHost`. `replicas>1` roda single-copy (sem quórum, sem durabilidade) — **não** é cluster de 1 nó, que fencearia escrita replicada com `QuorumUnavailableError` |
| Shutdown gracioso | `node.close()` drena: para de aceitar ativações → espera handlers em voo (com timeout) → handoff dos ranges aos novos donos (etapa 5) → broadcast de leave limpo (sem janela de suspicion) → fecha conexões. Deploy/restart não pode virar tempestade de reativações. `close()` é uniforme em todo `ActorSystem` (Node/Client/local) e todos são async context managers (`async with`) |
| Código homogêneo | Premissa explícita: todos os membros rodam o mesmo código de aplicação — a classe do ator precisa existir no nó dono. Wire name desconhecido no destino → `UnknownActorTypeError` no chamador. Rolling deploy de tipo novo: o tipo existe em todos os nós antes do primeiro uso |
| Observabilidade | v1: logging estruturado (stdlib `logging`, eventos com campos: view change, ativação, migração, suspeita) + hook de métricas plugável (protocolo tipado com contadores/histogramas; no-op por default). Sem dashboard, sem dep |
| Compressão | Plugável e negociada por conexão via capabilities no HELLO (zlib stdlib sempre; lz4/zstd como extras opcionais `casty[lz4]`). Threshold de tamanho — frames pequenos do caminho quente nunca comprimem; quem paga são snapshots/handoff/anti-entropy |
| Dependências | Runtime: só `msgpack`. Compressão além de zlib é extra opcional |
| Tooling | uv, mypy strict, ruff, pytest + pytest-asyncio. Src layout |

### Aberto

- **Fan-out pub/sub** (`Stream`/`Channel`): N subscribers independentes no mesmo tópico, com buffer/durabilidade. Fora do roadmap até o caso aparecer — o streaming RPC (spec 07) cobre um subscriber por chamada, que é o suficiente sem fan-out.

(Streams na API do usuário — método de ator recebendo/retornando `AsyncIterator` sobre os bulk streams do transporte, bidirecional — fechados na spec `docs/specs/07-streaming.md`.)

(Handshake de ativação: fechado na spec `docs/specs/05-replication.md`.)

## Membership: HyParView + Plumtree + ring

- **HyParView** mantém o overlay: active view pequena (conexões TCP simétricas, tamanho configurável, default ~log(n)+1), passive view maior (~6× active) como reserva. Join via seed com forward-join em random walk; shuffle periódico troca amostras das views; falha de vizinho promove candidato da passive view. Resultado: overlay conectado mesmo sob falha massiva.
- **Plumtree** (epidemic broadcast trees) sobre a active view: eager push na árvore, lazy push (IHAVE) nos demais links, graft/prune para reparo. Usado para broadcast dos eventos de cluster.
- **Ring state**: cada nó mantém a member list completa, construída dos eventos broadcast (`node-joined`, `node-left`, `node-dead`, com incarnation) + anti-entropy periódico (sync de estado completo com peer aleatório da active view) para reparar perdas de broadcast em churn.
- **Morte**: vizinhos da active view detectam queda (TCP break/keepalive) e broadcast `suspect(node, incarnation)`; o nó vivo refuta com `alive(incarnation+1)`; sem refutação em T → `dead`, removido da ring. (É o subprotocolo de suspicion do SWIM reimplementado sobre o overlay — custo conhecido e aceito da escolha.)
- **Data path fora do overlay**: chamadas de ator vão por conexão TCP direta ao nó dono, sob demanda. O overlay é só control plane.

## Placement e replicação

- Token ring: cada nó físico ocupa V posições (vnodes, default 128, configurável), `token(node, i) = hash(node_id, i)`. Key → `hash(key)` → primeiro token ≥ hash (com wrap) → nó dono. Réplicas = próximos R-1 nós físicos **distintos** caminhando a ring (pula vnodes do mesmo nó).
- Hash da ring: função estável entre processos, plataformas e versões (blake2b truncado, da stdlib) — **nunca** `hash()` builtin (salteado por processo). A função é parte do protocolo: mudá-la é breaking change de cluster.
- Unidade de handoff/replicação = token range.
- Escrita: proxy roteia ao dono → ator aplica → dono incrementa HLC da key → replica aos backups → responde quando W réplicas ackaram.
- Leitura: `read=ONE` direto no dono; `read=QUORUM` consulta R réplicas, vence a maior HLC, read repair nas defasadas.
- Join/leave move só os ranges afetados. Atores de range migrado desativam no nó antigo e reativam sob demanda no novo; estado vem das réplicas.
- Views divergentes (janela de churn) podem produzir dois donos para o mesmo range → duas ativações. Resolução: handshake de ativação com quórum + merge por HLC. Ativação única é *eventual*, não absoluta — absoluta exigiria consenso.

## Supervisão e erros

Dois domínios de falha, tratados separadamente:

**Falha de infraestrutura** (rede, timeout, sem quórum, range migrando) → exceção tipada no *chamador*, levantada pelo proxy: `CastyTimeoutError`, `ActorUnavailableError`, `QuorumUnavailableError`, `RangeMovingError`, `UnknownActorTypeError` (classe não registrada no nó dono — ver premissa de código homogêneo). Retry automático apenas no que é comprovadamente seguro (roteamento para dono novo após view change); o resto sobe.

**Falha do ator** (exceção no handler) → decidida pelo **supervisor**: global no `casty.start(supervisor=...)`, override por classe no `@casty.actor(supervisor=...)`. Não é árvore de supervisão Erlang — atores virtuais não têm pai que os criou; é uma política de destino da ativação:

- `KEEP` (default): estado mantido, ator segue vivo; chamador recebe `ActorFailedError` com a exceção original.
- `RESET`: ativação descartada, próxima chamada reativa do último snapshot replicado.
- `STOP`: desativa; chamadas subsequentes reativam do zero/snapshot.

Supervisor é um callable `(actor_cls, key, exc, ctx) -> Directive`, então políticas custom (por tipo de exceção, com contador de falhas, escalando de KEEP para RESET) são funções puras do usuário. Dead-letter hook para mensagens não entregues.

## Protocolo de rede

Ver spec detalhada em `docs/specs/01-transport.md`. Resumo:

```
u8 version | u8 type | u16 flags | u32 stream_id | u32 length | payload
```

Frames DATA/WINDOW_UPDATE/PING/GO_AWAY, estilo yamux; streams são byte pipes usados como canais de vida longa (stream 1 = controle/membership, stream 3 = mensagens de atores) + streams efêmeros para bulk. Mensagens têm delimitação própria (length-prefixed) dentro dos canais e correlação por `correlation_id` no envelope — respostas são assíncronas e fora de ordem, como manda o modelo de atores. Flow control por créditos por stream; frames DATA pequenos (256 KiB) garantem intercalação — um handoff de GBs não bloqueia chamadas de ator na mesma conexão. Handshake (HELLO no canal de controle) valida versão e cluster name e negocia compressão antes de qualquer outro stream. TLS/mTLS via `ssl.SSLContext`.

`@casty.message` registra a dataclass num registry com wire name estável (default: nome qualificado; overridável via `@casty.message(name="...")` — permite renomear a classe depois). Validação de serializabilidade recursiva em import time. Evolução: campo novo com default é tolerado por receptores antigos; campo desconhecido é ignorado (forward compat).

## API

```python
import casty

node = await casty.start(
    listen="0.0.0.0:7001",
    seeds=["10.0.0.1:7001", "10.0.0.2:7001"],
    tls=casty.TLS(cert="node.pem", key="node.key", ca="ca.pem"),  # ou None
    config=casty.Config(...),          # todos os knobs de protocolo
    supervisor=my_policy,              # opcional
)

client = await casty.connect(seeds=["10.0.0.1:7001"])  # lite member
```

```python
@casty.message
class Order:
    sku: str
    qty: int

@casty.actor
class Counter:
    value: int = 0          # field anotado = estado replicável; sem __init__

    async def add(self, n: int) -> int:
        self.value += n
        return self.value

counter = node.actor(Counter, "page:home")  # tipo estático: Counter; não cria nada
total = await counter.add(3)                # RPC ao dono do range de "page:home"
```

Estado transiente e lifecycle:

```python
@casty.actor
class Feed:
    cursor: int = 0                       # replicado via __state__
    conn: Connection = casty.transient()  # fora do snapshot

    @casty.activate
    async def _connect(self) -> None:     # a cada ativação, estado já restaurado
        self.conn = await connect(...)

    @casty.deactivate
    async def _close(self) -> None:
        await self.conn.close()
```

Contexto via contextvar, sem poluir assinaturas:

```python
@casty.actor
class Session:
    async def touch(self) -> None:
        ctx = casty.context()
        ctx.key                              # "user:42"
        ctx.deactivate()                     # após este handler
        peer = ctx.actor(Counter, "global")  # ator chamando ator
```

Replicação declarada por classe, abstraída do chamador:

```python
@casty.actor(replicas=3, write=casty.MAJORITY, read=casty.ONE, idle_timeout=300)
class Inventory: ...
```

Collections como açúcar sobre atores de range:

```python
stock: casty.Map[str, int] = node.map("stock", replicas=3, write=casty.MAJORITY)
await stock.put("sku-1", 40)
n = await stock.get("sku-1")
```

Services: RPC concorrente sobre um ator gerado. O handler do coordenador não
espera o trabalho — dispara-o como task e devolve a mailbox — então N chamadas
progridem juntas. Sem estado, sem key, sem lugar na ring: o estado mora nos
atores que o método chama.

```python
@casty.service(concurrency=64)
class Checkout:
    async def buy(self, sku: str, qty: int) -> bool:
        stock = casty.context().actor(Inventory, sku)  # ator: serial, replicado
        return await stock.reserve(qty)

checkout = node.service(Checkout)                       # sem key
ok = await asyncio.gather(*[checkout.buy(s, 1) for s in skus])
```

## Roadmap

Cada etapa com critério de verificação próprio; a seguinte só começa com a anterior verde.

1. **Transporte + serde** — frames, TLS, request/reply por correlation id, pool, `@casty.message` + registry. Spec: `docs/specs/01-transport.md`.
   *Verificação: round-trip com e sem TLS/mTLS, frames malformados rejeitados, reconexão com backoff, registry valida tipos ilegais em import time.*
2. **Membership** — HyParView + Plumtree + ring state, atrás da interface `alive_members() -> frozenset[Member]` + eventos de view.
   *Verificação: cluster de N processos; matar nós e medir convergência da view; nó lento não é expulso (refutação por incarnation); overlay sobrevive a falha de 30% dos nós simultânea.*
3. **Placement** — token ring com vnodes. Módulo puro.
   *Verificação: unit tests de uniformidade (desvio entre nós com V=128), movimento mínimo de ranges em join/leave, réplicas em nós físicos distintos.*
4. **Atores virtuais sem replicação** — ativação sob demanda, proxy tipado, mailbox, roteamento, re-roteamento em rebalance, idle deactivation, supervisor, detecção de reentrância, shutdown gracioso (drenar handlers em voo + leave broadcast; handoff entra na etapa 5).
   *Verificação: chamadas cruzadas em cluster real; matar o dono e observar reativação; ciclo A→B→A levanta erro; `close()` não gera suspicion nem perde mensagens aceitas; mypy limpo na API pública.*
5. **Replicação + quórum + fencing** — HLC, ack por W, read repair, handshake de ativação, fencing de minoria, handoff de ranges no `close()`.
   *Verificação: testes de partição de rede (toxiproxy); escrita rejeitada na minoria; convergência pós-heal sem perda com W=MAJORITY; rolling restart de cluster de 5 nós sem perda de estado.*
6. **Collections** — Map primeiro, sobre atores de range.
   *Verificação: API completa do Map com os mesmos testes de falha da etapa 5.*
7. **Streaming RPC** — métodos de ator recebendo/retornando `AsyncIterator` sobre os bulk streams, bidirecional. Spec: `docs/specs/07-streaming.md`.
   *Verificação: server/client/duplex fim-a-fim em cluster real; backpressure via créditos; matar o dono no meio → erro tipado no `__anext__`; mypy strict limpo.*
8. **Services** — `@casty.service`: ator gerado cujo handler destaca o reply (`ctx.detach()`) e roda o método como task fora da mailbox. Concorrência ilimitada (ou `concurrency`), stateless, roteamento local-first. Spec: `docs/specs/08-services.md`.
   *Verificação: N chamadas concorrentes progridem juntas; ativação não desativa com reply pendente; `node.service` despacha local e `client.service` balanceia entre membros; matar o host em voo → erro tipado no chamador.*

Até a etapa 4 existe um Orleans mínimo utilizável sem replicação. A etapa 5 concentra o risco do projeto.

## Fora da v1

Hinted handoff; merkle/anti-entropy de dados (só o read repair); persistência de estado em disco (ator morre = estado vem dos backups); árvores de supervisão Erlang (só a política global/por classe); ring de roteamento parcial (Chord-like); transações multi-key.
