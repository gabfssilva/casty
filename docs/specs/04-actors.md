# Spec 04 — Atores virtuais (sem replicação)

Escopo: `@casty.actor`, ativação sob demanda, proxy tipado, mailbox, roteamento pela ring, re-roteamento em view change, idle deactivation, supervisor, detecção de reentrância, shutdown gracioso, lite members (`casty.connect`). Não cobre replicação/quórum/handoff (spec 05) — nesta etapa, morte do dono = estado perdido, reativação do zero.

## 1. Decorators

### `@casty.actor` / `@casty.actor(name=..., idle_timeout=..., supervisor=...)`

1. Aplica `dataclass(slots=True, eq=False)`; todo field precisa de default (ativação constrói `Cls()` sem args) — sem default → erro em import time.
2. Fields anotados = estado replicável; `casty.transient()` marca fields fora do snapshot (default `None` ou `factory=`), reconstruídos por hook de ativação.
3. Registra o schema de estado no registry serde com wire name `<wire>#state` (valida serializabilidade dos fields em import time; campos com pager ficam de fora — spec 09 §5). Decide o **regime de paginação** de cada field (`ActorInfo.regimes`, spec 09 §3) e gera os acessores `ActorInfo.state_of(instance)` / `restore_into(instance, values)` — usados pela etapa 5.
4. Métodos públicos `async` (nome sem `_`) formam a interface do proxy. Em import time valida: método público é async; parâmetros e retorno anotados com tipos serializáveis (a chamada cruza a rede; código homogêneo garante a classe nos dois lados, e as anotações guiam o decode).
5. `@casty.activate` / `@casty.deactivate` marcam hooks (nome livre, só `self`); fora da interface do proxy. `activate` roda a cada ativação; `deactivate` em desativação graciosa (idle, STOP, `ctx.deactivate()`, shutdown) — não em RESET.
6. Opções: `name` (wire name), `idle_timeout` (default `Config.default_idle_timeout`), `supervisor` (override do global). `replicas`/`write`/`read` entram na etapa 5.

## 2. Identidade, roteamento, wire

- Identidade = `(wire_name, key)`; token da ring = `key_token(f"{wire_name}/{key}")`.
- `node.actor(Cls, "key")` retorna proxy tipado como `Cls` (cast estático; runtime é `ActorProxy`). Não cria nada — ativação acontece no dono, na primeira chamada.
- Chamada: proxy resolve o dono na ring local. Dono == self → dispatch local (sem rede). Senão → `ask` `ACTOR_CALL` (0x40) na conexão do pool ao dono.
- **Wire**: `ActorCall {actor: str, key: str, method: str, args: list[bytes], chain: list[str]}`; cada arg é encodado isoladamente (`encode_raw`) e decodado no destino contra a anotação do parâmetro (type-driven). Resposta = retorno encodado; decodado contra a anotação de retorno.
- Receptor valida contra a *própria* ring: não sou o dono → `RemoteError(code=WRONG_OWNER)`. Chamador re-resolve e retenta **uma vez** — retry seguro por construção: a chamada não foi executada. Nenhum outro erro é retentado.
- Códigos de erro (RemoteError.code → exceção tipada no chamador): `10 UnknownActorTypeError`, `11 WRONG_OWNER` (interno), `12 ActorFailedError`, `13 ReentrancyError`, `14 ActorUnavailableError` (nó drenando/parado).

## 3. Ativação e mailbox

- `(wire, key)` → uma ativação no dono. Primeira mensagem cria: `Cls()`, hook activate, worker da mailbox.
- Mailbox FIFO unbounded; **um handler por vez**. O worker aguarda item com timeout = `idle_timeout` da classe: expirado com mailbox vazia → desativação graciosa (hook deactivate, remove). Corrida enfileirar×desativar resolvida por flag `closing` checada sincronamente no loop (sem await entre checagem e decisão); mensagem que chegue durante finalização espera e recria a ativação.
- `ctx.deactivate()`: desativa após o handler corrente; itens restantes na mailbox são re-despachados para a ativação nova.
- **Reentrância**: `chain` carrega as identidades da cadeia de asks. Dispatch para identidade já presente na chain → `ReentrancyError` (A→B→A falha rápido em vez de deadlock). Proxy dentro de ator anexa a chain do contexto + a própria identidade.

## 4. Contexto

`casty.context()` (contextvar, válido dentro de handler): `.key`, `.actor_class`, `.node`, `.deactivate()`, `.actor(Cls, key)` (proxy que propaga a chain). Fora de handler → `LookupError`.

## 5. Supervisor

`Supervisor = Callable[[type, str, Exception, FailureContext], Directive]`; `FailureContext.failures` = falhas consecutivas da ativação. Directives:

- `KEEP` (default): estado mantido; chamador recebe `ActorFailedError` (mensagem original embutida).
- `RESET`: ativação descartada sem hook deactivate; próxima chamada cria do zero.
- `STOP`: desativação graciosa (hook roda); próxima chamada reativa.

Global em `casty.start(supervisor=...)`, override por classe. Exceção *do supervisor* → KEEP + log.

## 6. Node e lifecycle

```python
node = await casty.start(listen="0.0.0.0:7001", seeds=[...], tls=..., config=casty.Config(...), supervisor=...)
client = await casty.connect(seeds=[...])   # lite member
```

- `Node` compõe: Server + Pool (spec 01), Membership (spec 02), Ring (spec 03, reconstruída a cada evento de view com membros `role="member"`), ActorHost.
- **View change**: ring nova. Atores de range migrado: o nó antigo responde WRONG_OWNER às chamadas novas (roteadas pelo chamador ao dono novo); a ativação antiga morre por idle. Sem replicação, o estado não migra (etapa 5).
- **`node.stop()`**: (1) para de aceitar ativações e chamadas (`ActorUnavailableError`); (2) drena handlers em voo com `Config.drain_timeout`; (3) hooks deactivate; (4) `membership.leave()` (LEFT limpo, sem suspicion); (5) fecha pool e server.
- **Lite member** (`casty.connect`): fora do overlay e do placement. Mantém a member table via SYNC (pull a cada `Config.client_sync_interval` contra um membro conhecido) e roteia chamadas em 1 hop como um nó normal. `role="client"` no HELLO; membros nunca o incluem na ring.

## 7. Configuração

```python
@dataclass(frozen=True)
class Config:
    transport: TransportConfig = TransportConfig()
    membership: MembershipConfig = MembershipConfig()
    call_timeout: float = 10.0            # ask de ator, overridável por chamada futura
    default_idle_timeout: float = 300.0
    drain_timeout: float = 10.0
    client_sync_interval: float = 5.0
```

## 8. Layout

```
src/casty/actors/
├── registry.py    # @actor, transient, activate/deactivate, ActorInfo, validação
├── messages.py    # ActorCall + msg types 0x40–0x5F + códigos de erro
├── context.py     # ActorContext + contextvar
├── host.py        # ativações, mailbox/worker, supervisor, reentrância
└── proxy.py       # ActorProxy + roteamento/retry
src/casty/node.py  # casty.start / casty.connect / Node / Client / Config
```

## 9. Critérios de verificação da etapa

1. Cluster real (3 nós): chamadas para a mesma key de nós diferentes acertam a mesma ativação (contador soma); keys distribuem entre nós.
2. Matar o dono → chamada seguinte reativa em outro nó (estado zerado — sem replicação ainda).
3. Ciclo A→B→A levanta `ReentrancyError` no chamador; A→B (sem ciclo) funciona.
4. Idle deactivation: hook roda, próxima chamada reativa; `ctx.deactivate()` idem.
5. Supervisor: KEEP mantém estado pós-falha; RESET zera; STOP desativa com hook; `ActorFailedError` chega ao chamador com a mensagem original.
6. `stop()`: chamada em voo completa; sem evento suspect/dead nos sobreviventes (leave limpo); chamadas novas → `ActorUnavailableError`.
7. Lite member: `connect()` roteia para o cluster; não aparece na ring.
8. Validação em import time: field sem default, método não-async, anotação não-serializável → `SerializationSchemaError`/`TypeError` no import.
9. mypy strict limpo; proxy preserva tipos (checado com `assert_type` em teste).
