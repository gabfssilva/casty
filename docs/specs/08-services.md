# Spec 08 — Services (RPC concorrente sobre atores)

Escopo: `@casty.service`, `node.service(Cls)`, o coordenador de ator gerado dinamicamente pelo decorator, reply adiado (`ctx.detach`), tracking de in-flight que segura a ativação, roteamento local-first. Reusa transporte/serde/proxy/host das specs 01/04 sem primitivo de rede novo. Não cobre streaming em service (método async-gen num service → erro em import time nesta versão) nem estado — service é stateless por construção e delega qualquer estado a atores.

## 1. Modelo

Um ator serializa: um handler por vez por `(wire, key)`, e um handler que dá `await` numa I/O lenta segura o worker por toda a espera. Isso é a garantia de estado do ator — e o teto de concorrência dele.

Um **service** remove esse teto sem tirar o ator do meio. Ele é um ator gerado cujo handler **não espera** o trabalho: dispara o método do usuário como uma task destacada, registra o reply do chamador num mapa transiente, e retorna em O(1). O trabalho roda fora da mailbox, concorrente; quando termina, resolve o reply. O ator continua serial e dono único do registry de callbacks — o paralelismo é o trabalho *escapando* da mailbox, não handlers rodando em paralelo.

Consequências:

- **Concorrência ilimitada** (ou limitada por `concurrency`): N chamadas ao mesmo service progridem juntas. Nenhuma espera a anterior sair da mailbox.
- **Sem estado no service**: todos os fields do coordenador são transientes (futures/tasks em voo). Morte do nó = chamadas em voo falham no chamador com erro tipado. Nada replica.
- **Estado mora nos atores**: o método do service chama `ctx.actor(Cls, key)` pra qualquer coisa com estado. A concorrência do service não vaza pro ator — chamadas do mesmo `(wire, key)` continuam serializadas no dono. O service é a porta concorrente; o ator, o guardião serial.
- **Sem supervisor**: cada chamada é independente e não há estado a preservar. Exceção no método → `ActorFailedError` no chamador, sem KEEP/RESET/STOP.

## 2. Superfície do usuário

```python
import casty

@casty.service
class Fetcher:
    async def fetch(self, url: str) -> bytes:
        return await http_get(url)

    async def head(self, url: str) -> int:
        return (await http_head(url)).status
```

```python
fetcher = node.service(Fetcher)             # sem key — proxy tipado como Fetcher
results = await asyncio.gather(*[fetcher.fetch(u) for u in urls])  # concorrentes
```

`await fetcher.fetch(u)` bloqueia até o trabalho terminar (o reply adiado resolve transparente), mas as tasks correm em paralelo. Combinado com atores:

```python
@casty.service
class Checkout:
    async def buy(self, sku: str, qty: int) -> bool:
        stock = casty.context().actor(Inventory, sku)   # ator: serial, replicado
        return await stock.reserve(qty)                 # reservas do mesmo sku serializam
```

Opções: `@casty.service(name=..., concurrency=None)`. `name` é o wire name (default `module.QualName`); `concurrency` limita as tasks concorrentes por ativação (default ilimitado).

## 3. O coordenador gerado

`@casty.service` **não** escreve um ator análogo — sintetiza a classe via `type(...)` e a passa pela mesma maquinaria do `@casty.actor` (spec 04). O resultado é um ator legítimo: slots, mailbox, registro no wire, proxy tipado.

```python
def service(cls=None, /, *, name=None, concurrency=None):
    if cls is None:
        return lambda inner: _build(inner, name, concurrency)
    return _build(cls, name, concurrency)


def _build(user_cls, name, concurrency):
    if getattr(user_cls, "__annotations__", {}):
        raise SerializationSchemaError(f"{user_cls.__qualname__}: service é stateless; use um ator")
    impls = {}
    for n, f in vars(user_cls).items():
        if n.startswith("_") or not inspect.isfunction(f):
            continue
        if inspect.isasyncgenfunction(f):
            raise SerializationSchemaError(f"{n}: streaming em service não é suportado (v1)")
        if not inspect.iscoroutinefunction(f):
            raise SerializationSchemaError(f"{n}: métodos públicos de service devem ser async")
        impls[n] = f
    ns = {
        "__module__": user_cls.__module__,
        "__qualname__": user_cls.__qualname__,
        "__annotations__": {
            "_impl": object, "_pending": dict, "_seq": object, "_tasks": set, "_sem": object,
        },
        "_impl": registry.transient(),                                  # instância do usuário
        "_pending": registry.transient(factory=dict),                  # cid -> Reply
        "_seq": registry.transient(factory=lambda: itertools.count()),
        "_tasks": registry.transient(factory=set),
        "_sem": registry.transient(
            factory=(lambda: asyncio.Semaphore(concurrency)) if concurrency else lambda: None
        ),
        "_boot": _boot(user_cls),        # @activate: self._impl = user_cls()
        "_shutdown": _shutdown,          # @deactivate: cancela tasks em voo
    }
    for mname, fn in impls.items():
        ns[mname] = _dispatcher(mname, fn)
    coordinator = type(user_cls.__name__, (), ns)
    return registry.actor(name=name, kind="service")(coordinator)
```

O dispatcher gerado por método:

```python
def _dispatcher(mname, fn):
    async def dispatcher(self, *args, **kwargs):
        ctx = current_context()
        if self._sem is not None:
            await self._sem.acquire()        # acima do limite, segura a mailbox: backpressure real
        reply = ctx.detach()                 # host NÃO resolve o caller na volta
        cid = next(self._seq)
        self._pending[cid] = reply           # registra o callback (roda serializado)
        task = asyncio.create_task(
            _drive(self._impl, self._pending, self._sem, cid, fn, args, kwargs)
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        # handler retorna já; mailbox livre pro próximo request

    dispatcher.__name__ = dispatcher.__qualname__ = mname
    dispatcher.__signature__ = inspect.signature(fn)          # o registry colhe params/kinds daqui
    dispatcher.__annotations__ = typing.get_type_hints(fn)    # tipos já resolvidos no módulo do usuário
    return dispatcher


async def _drive(impl, pending, sem, cid, fn, args, kwargs):
    try:
        value = await fn(impl, *args, **kwargs)              # roda fora da mailbox, concorrente
    except asyncio.CancelledError:
        reply = pending.pop(cid, None)
        if reply is not None:                                # chamador recebe erro tipado, não hang
            reply.fail(ActorUnavailableError(f"{fn.__qualname__}: cancelado no shutdown"))
        raise
    except Exception as exc:
        pending.pop(cid).fail(exc)
    else:
        pending.pop(cid).set(value)
    finally:
        if sem is not None:
            sem.release()
```

Dois pontos de metaprogramação que fazem o registry aceitar o coordenador como se o usuário o tivesse escrito:

- `dispatcher.__signature__ = inspect.signature(fn)` — `_collect_methods` (registry.py) lê params/kinds do dispatcher; clonando a assinatura do método real, `*args/**kwargs` não são rejeitados e o proxy fica idêntico à API declarada.
- `dispatcher.__annotations__ = get_type_hints(fn)` — resolvido no módulo do usuário devolve **tipos reais**, não strings; quando o registry chamar `get_type_hints(dispatcher)`, não precisa do `__globals__` do módulo do service pra resolver nomes do usuário.

**Contexto na task**: `asyncio.create_task` copia o contexto no momento da criação, então `_drive` herda o `ActorContext` do dispatcher — `.key`, `.chain` (com a identidade do coordenador), `.actor(...)`. O método do usuário chama outro ator normalmente e a chain propaga; A→service→A é detectado como reentrância. O `reset_context` do host no fim do handler não afeta a cópia da task.

## 4. Reply adiado (mudança no host)

Único primitivo novo no host. Hoje `_handle` resolve `item.future` logo após o handler (host.py:350). O service precisa segurar esse future e resolvê-lo fora de banda.

`context.py` — `ctx.detach()` marca a activation como em-voo e devolve o `Reply` que embrulha o future do chamador:

```python
class ActorContext:
    def __init__(self, host, actor_class, key, chain, reply=None):
        ...
        self._reply = reply
        self.detached = False

    def detach(self):
        if self._reply is None:                 # hooks e handlers de stream não têm reply
            raise RuntimeError("detach() só é válido dentro de um handler unário")
        self.detached = True
        self._reply._activation.inflight += 1   # segura a ativação enquanto há voo
        return self._reply
```

`host.py` — `Reply`, o campo `inflight` na `_Activation`, e `_handle`/idle respeitando o detach:

```python
@dataclass(slots=True)
class Reply:
    _future: asyncio.Future[object]
    _activation: _Activation
    _fired: bool = False

    def set(self, value):
        if self._release() and not self._future.done():
            self._future.set_result(value)

    def fail(self, exc):
        if self._release() and not self._future.done():
            self._future.set_exception(exc)

    def _release(self):
        # fire-once e incondicional ao estado do future: um chamador local
        # cancelado cancela o próprio future (done() = True) — se o decremento
        # dependesse dele, inflight vazaria e a ativação nunca desativaria.
        if self._fired:
            return False
        self._fired = True
        self._activation.inflight -= 1
        return True
```

```python
# _Activation ganha:  inflight: int = 0

# _handle, ramo de sucesso (host.py:348) constrói o ctx com reply e respeita o detach:
ctx = ActorContext(self, info.cls, activation.key, chain=[...], reply=Reply(item.future, activation))
...
else:
    activation.failures = 0
    commit_error = await self._commit_if_mutated(activation)   # service: replicas=1, no-op
    if ctx.detached:
        pass                                   # o Reply resolve fora de banda
    elif not item.future.done():
        item.future.set_result(result) if commit_error is None \
            else item.future.set_exception(commit_error)

# ramo de exceção: se o handler falhou DEPOIS do detach, o Reply nunca vai
# disparar — libera o voo aqui (fire-once: o future, já resolvido com a
# exceção pelo código existente, não é tocado; só o decremento acontece):
if ctx.detached:
    ctx._reply.fail(exc)

# a desativação por idle (host.py:255) passa a exigir voo zerado:
if activation.queue.empty() and activation.inflight == 0:
    activation.closing = True
    break
```

Timeout: só o caminho **remoto** tem prazo (`conn.ask(timeout=call_timeout)`, node.py:118). O dispatch local (`Node._dispatch_local`, `Local.call_actor`) aguarda o future sem timeout — hoje é inócuo porque o handler sempre retorna, mas com reply adiado um método do usuário que trava deixa o chamador local esperando indefinidamente e segura o `inflight` (a ativação não desativa por idle). Limitação v1 aceita; armar `call_timeout` no host pro future destacado fica pra depois. Como `inflight` conta a partir do `detach` e zera exatamente uma vez no `set`/`fail` (fire-once), a ativação nunca desativa com reply pendente.

## 5. Roteamento e wire

Service não tem key nem dono na ring. É stateless, então **qualquer ativação é intercambiável**. Uma ativação basta por nó — o handler quase não faz trabalho (só dispara task), então não há dono quente; o trabalho real roda como task no nó que hospeda o coordenador.

Decisão v1 — **local-first no nó, load-balanced a partir do cliente**:

- `node.service(Cls)` → proxy que roteia à **ativação local** do coordenador, chave fixa por wire (ex. `(service_wire, "@")`). Um coordenador por nó; dispatch sem rede, zero hop. O trabalho das tasks roda no nó do chamador — carga espalha naturalmente entre os nós que chamam.
- `client.service(Cls)` (lite member, sem host) → escolhe um membro (round-robin sobre a view sincronizada — `_addrs`, populada via SYNC; Client não tem membership própria) e manda `ACTOR_CALL` com o service; o membro despacha no host local dele. Um hop.

`ActorInfo` ganha `kind: Literal["actor", "service"]` (default `"actor"`). O roteamento ramifica por ele:

- `call_actor` num `_Router`: `info.kind == "service"` → pula a resolução na ring. Node → `dispatch(info, "@", ...)` no host local. Client → membro load-balanced, `ACTOR_CALL`.
- Receptor de `ACTOR_CALL` (node.py:519): service **não** valida dono contra a ring — despacha local incondicional (não há WRONG_OWNER pra service; qualquer nó atende).

Sem msg_type novo, sem stream kind novo: service reusa `ACTOR_CALL` (0x40). A diferença é só o roteamento no chamador e a ausência do check de dono no receptor.

## 6. Validação (import time)

`@casty.service` reusa `_collect_methods`/`_register` da spec 04, com estes ajustes:

- Métodos públicos async → viram dispatchers; params e retorno anotados e serializáveis (a chamada cruza a rede; anotações guiam encode/decode) — idêntico a ator. Método público **síncrono** → erro explícito em `_build` (um filtro silencioso o faria sumir do coordenador em vez de falhar).
- **Sem fields de estado**: fields anotados na classe do usuário → erro (`SerializationSchemaError`: "service é stateless; use um ator"). Todo estado do coordenador é transiente e gerado.
- **Sem streaming v1**: método async-gen ou param/retorno `AsyncIterator` → `SerializationSchemaError`. (Streaming em service é extensão futura — o dispatcher atual só cobre unário.)
- **Sem hooks do usuário**: `@casty.activate`/`@casty.deactivate` na classe do usuário → erro (os hooks são gerados: `_boot`/`_shutdown`).
- `replicas`/`write`/`read` não são aceitos (service é `replicas=1` fixo).

## 7. Ciclo de vida

- **Ativação**: primeira chamada cria `(service_wire, "@")` no host local; `_boot` instancia `user_cls()` em `_impl`.
- **Idle**: mailbox vazia por `idle_timeout` **e** `inflight == 0` → desativa; `_shutdown` cancela tasks residuais (nenhuma, se `inflight == 0`). Próxima chamada reativa.
- **`node.stop()`** (spec 04 §6): o drain espera os handlers da mailbox — que são O(1) — mas as tasks de `_drive` estão fora da mailbox. `_shutdown` (deactivate hook) as cancela; cada cancel resolve o `Reply` como falha via o ramo `CancelledError` de `_drive` (chamador recebe `ActorUnavailableError`). Trabalho em voo não sobrevive a `stop` — consistente com "service é stateless".
- **Morte do nó**: ativação some com o processo; chamadas em voo falham no chamador (ask timeout / conexão caída). Nada a recuperar.

## 8. Guardrails v1

- Service é stateless: field de estado na classe → erro em import. Estado mora em atores.
- Sem streaming (só unário); sem supervisor (cada chamada independente); sem replicação.
- Roteamento local-first fixo; `key=`/singleton cluster-wide ficam pra depois (§10).
- `concurrency` limita concorrência por ativação; ilimitado por default. O semáforo é adquirido **no dispatcher**, antes do `create_task`: acima do limite a mailbox segura os requests — backpressure real, `_pending`/`_tasks` não passam do teto (a fila da mailbox cresce no lugar, mas o timeout do chamador remoto se aplica). Sem o knob, um produtor rápido demais infla `_pending` sem teto.
- Falso positivo de reentrância: toda chamada carrega `service/@` na chain, então qualquer caminho downstream que volte ao **mesmo** service (X→service→ator→service) estoura `ReentrancyError`, mesmo sem deadlock possível — o handler do service é O(1) e não segura a mailbox. Aceito na v1; isentar `kind == "service"` do check de ciclo fica pra depois.
- Ordem: conclusões chegam em ordem de término, não de chamada. Esperado pra RPC concorrente.

## 9. Layout

```
src/casty/services/
├── __init__.py
└── builder.py        # @service, _build, _dispatcher, _drive, _boot, _shutdown
src/casty/actors/
├── registry.py       # + ActorInfo.kind; @service reusa _register/_collect_methods
├── context.py        # + ctx.detach(); ActorContext ganha reply
└── host.py           # + Reply, _Activation.inflight, _handle respeita detach, idle exige inflight==0
src/casty/system.py   # + ActorSystem.service(Cls) -> proxy (Node local, Client load-balanced)
src/casty/node.py     # call_actor/receptor ramificam por info.kind == "service"
examples/07_service.py
```

## 10. Etapas de implementação

Cada uma com verificação própria; a seguinte só começa com a anterior verde.

1. **Reply adiado no host** — `ctx.detach()`, `Reply`, `_Activation.inflight`, `_handle` respeita detach, idle exige `inflight == 0`. Independe do resto.
   *Verificação (via `casty.local`, com um ator de teste que faz `ctx.detach()` na mão): o chamador aguarda até o `Reply.set`; a ativação não desativa com reply pendente; `fail` propaga a exceção; mypy strict limpo.*
2. **Builder** — `@casty.service`, coordenador gerado, dispatcher com assinatura/hints clonados, `_drive`, `_boot`/`_shutdown`, `ActorInfo.kind`, validação (stateless, sem streaming/hooks).
   *Verificação (import + `casty.local`): service válido registra e o proxy tipa como a classe; N chamadas concorrentes progridem juntas (uma barreira compartilhada só libera quando todas entraram); field de estado / método streaming / hook → `SerializationSchemaError`; contexto propaga pra `ctx.actor` dentro do método (A→service→A → `ReentrancyError`).*
3. **Roteamento no node** — `ActorSystem.service`, `call_actor`/receptor ramificando por `kind`, local-first no Node, load-balance no Client.
   *Verificação (cluster real de 3 nós + 1 client): `node.service` despacha local (zero hop, contável); `client.service` distribui entre membros vivos; matar o nó que hospeda o coordenador no meio de chamadas em voo → erro tipado no chamador; carga de fetch concorrente satura I/O sem serializar.*
4. **Superfície + exemplo** — export de `casty.service`/`node.service`, `examples/07_service.py` (service de fan-out I/O chamando um ator com estado), doc no DESIGN.
   *Verificação: exemplo roda em cluster real; `concurrency` limita a concorrência observável; mypy strict limpo na API pública.*

Fica aberto pra uma spec futura: `key=` opcional (rotear/cachear por partição, ex. uma conexão cara por shard), service singleton cluster-wide (uma ativação, todo o trabalho num nó — coordenação/fan-out puro), e streaming em service.
