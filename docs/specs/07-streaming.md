# Spec 07 — Streaming RPC (async iterators sobre a rede)

Escopo: métodos de ator que recebem e/ou retornam `AsyncIterator[T]`, transportados sobre os bulk streams do mux (spec 01) com backpressure fim-a-fim. Bidirecional: server-streaming, client-streaming e duplex. Nenhum mecanismo novo de transporte — só uma nova forma de invocação sobre a stream bulk que existe desde a etapa 1. Não cobre pub/sub com fan-out nem durabilidade (fora do roadmap); um subscriber por chamada, falha terminal, sem resume/cursor.

## 1. Modelo

Um método streaming é **um handler comum**. Ganha um canal de saída para emitir e/ou um canal de entrada para drenar, ocupa o worker da mailbox pela vida inteira do stream e **commita o snapshot uma vez ao retornar** — idêntico a um handler não-streaming. Consequências, todas herdadas do modelo da spec 04/05 sem exceção:

- **Concorrência inalterada**: um handler por vez por `(wire, key)`. Nenhum acesso concorrente a `self`; nenhum hazard de estado novo.
- **Replicação inalterada**: o handler muta `self` livremente durante o stream e `_commit_if_mutated` roda uma vez no fim, como em qualquer chamada. Ingest (client-streaming) folda a entrada no estado e commita atômico. Server-streaming lê estado e não muta → commit é no-op.
- **Idle timeout**: o worker está ocupado durante o stream, não ocioso → não há desativação no meio.

Custo assumido: um stream longo segura a chave (nenhuma outra chamada àquela chave progride até ele terminar). Consistente com a filosofia do casty — single-owner não escala dentro de uma chave; particiona para escalar. Sem resume: interrupção é terminal.

Três formas, o mesmo mecanismo:

```python
@casty.actor
class Log:
    entries: list[bytes] = dataclasses.field(default_factory=list)

    # server-streaming: async generator, sem mutação
    async def tail(self, since: int) -> AsyncIterator[bytes]:
        for e in self.entries[since:]:
            yield e

    # client-streaming: corrotina que drena o param e commita no fim
    async def ingest(self, items: AsyncIterator[bytes]) -> int:
        n = 0
        async for item in items:
            self.entries.append(item)
            n += 1
        return n

    # duplex: async generator que consome o param
    async def transform(self, xs: AsyncIterator[bytes]) -> AsyncIterator[int]:
        async for x in xs:
            yield len(x)
```

Do lado do chamador:

```python
log = node.actor(Log, "app:1")
async for line in log.tail(0):          # server → chamador
    ...
count = await log.ingest(source())      # chamador → ator, source() é AsyncIterator[bytes]
async for n in log.transform(source()): # duplex
    ...
```

## 2. Declaração e validação (import time)

`@casty.actor` reconhece `AsyncIterator[T]` (equivalente: `AsyncGenerator[T, None]`) em posição de parâmetro e/ou retorno:

- **Retorno `AsyncIterator[T]`** → método server-streaming; o corpo é um async generator (`inspect.isasyncgenfunction`). Valida `T` serializável, não o iterator.
- **Parâmetro `AsyncIterator[A]`** → o param de entrada; valida `A`. **No máximo um** param iterador por método (v1). Os demais params são unários (enviados uma vez na abertura).
- Combinar entrada iterador + retorno iterador = duplex.
- Um método client-streaming puro (param iterador, retorno escalar) é uma corrotina `async def` normal.

`MethodInfo` ganha `stream_out: object | None` (o `T`, ou `None`) e `stream_in: tuple[str, object] | None` (nome do param + `A`). Erros em import time (`SerializationSchemaError`): dois params iterador; `T`/`A` não serializável; retorno `AsyncIterator` num método que não é async generator; param iterador num método non-async.

Código homogêneo (premissa do DESIGN) garante a classe idêntica nos dois lados; as anotações guiam o encode/decode elemento a elemento.

## 3. Wire

Sem novo `msg_type` no canal de mensagens. Um **novo kind de bulk stream** `casty.stream`, roteado pela ring exatamente como `ACTOR_CALL`:

1. Chamador resolve o dono na ring local. Dono == self → dispatch local, sem rede (§6). Senão → `open_bulk(kind="casty.stream", meta=StreamOpen)` na conexão do pool ao dono.
2. `StreamOpen {actor: str, key: str, method: str, args: list[bytes], chain: list[str]}` — os args **unários** (`encode_raw` cada), decodados no destino contra as anotações; o param iterador **não** entra aqui, chega como frames.
3. O bulk stream do mux é bidirecional e tem flow-control por créditos por stream — o backpressure fim-a-fim (consumidor lento freia o produtor) é herdado, sem buffer extra.

Frames dentro da stream (envelope length-prefixed, um por elemento):

- `ITEM(payload)` — um elemento `encode_raw`. Chamador→dono carrega os elementos da entrada (client-streaming); dono→chamador carrega a saída (server-streaming). Em duplex, as duas metades correm independentes.
- `ERROR(code, message)` — falha terminal; mapeia para exceção tipada no chamador (mesmos códigos da spec 04 §2: `WRONG_OWNER`, `ActorFailedError`, `ReentrancyError`, `ActorUnavailableError`, `RangeMovingError`, `UnknownActorTypeError`, `QuorumUnavailableError`).

Fim de cada direção = **FIN do mux** (cada lado meio-fecha a sua metade quando o respectivo iterador esgota). Erro = frame `ERROR` **antes** de fechar — RST não carrega payload, então o código do erro precisa de um frame in-band. Abandono do chamador = RST (§5).

## 4. Dono: dispatch

`Pool`/`Server` do `Node` passam a expor `on_bulk`. O handler despacha por `receiver.kind`:

- `casty.replication` — já tratado à parte na connection (lane de envelopes; spec 05), não chega aqui.
- `casty.stream` → `_on_actor_stream`.

`_on_actor_stream`:

1. Parseia `StreamOpen`; resolve `info` (registry ou `ensure_collection`); valida dono contra a **própria** ring (`ring.owner == self`, senão `ERROR(WRONG_OWNER)`); método existe e é streaming; aridade dos args unários bate.
2. Decoda os args unários contra as anotações.
3. Se client/duplex: monta o **in-iter** — um async iterator que faz `decode_raw(item, A)` por frame `ITEM` do `BulkReceiver`, levanta no `ERROR`, `StopAsyncIteration` no FIN.
4. Constrói um `BulkSender` sobre o `stream_id` aceito (o dono responde na mesma stream).
5. `host.dispatch_stream(info, key, method, unary_args, in_iter, out_sender, chain)` — enfileira na mailbox; verificação de reentrância pela `chain` reusa a da spec 04.
6. Erros do handler → `ERROR(code)` mapeado, depois fecha.

`ActorHost`: `_CallItem` ganha variante streaming (in-iter + out-sink). `_handle` ramifica:

- server-streaming: `async for elem in method(*unary_args): await out.send(encode_raw(elem))`; `out` fecha (FIN) no fim.
- client-streaming: `result = await method(*unary_args, in_iter)`; resultado vai como frame terminal (um `ITEM` + FIN, ou canal de resposta unário — ver §6).
- duplex: método é async gen que recebe `in_iter`; saída emitida como no server-streaming.

Depois do corpo, `_commit_if_mutated` roda igual a um handler normal. Supervisor, `ctx`, `deactivate` idem. O worker fica ocupado durante todo o stream — cada `out.send` pode bloquear na janela de flow-control, e isso é o backpressure.

## 5. Proxy: chamador

`ActorProxy.__getattr__` ramifica por `MethodInfo`:

- **server/duplex** → devolve callable que retorna um **async iterator** (não awaitable). Esse iterador é um async generator: abre a stream, (se duplex) dispara em paralelo uma task que sobe o param iterador como frames `ITEM`, e itera os frames `ITEM` de volta fazendo `decode_raw(payload, T)`; `ERROR` levanta a exceção tipada; FIN encerra. `aclose()` (disparado por `break`/`return`/GC no `async for`) faz `sender.abort()` → RST → o handler do dono é cancelado e libera o worker.
- **client-streaming puro** → devolve corrotina: abre a stream, sobe o param iterador, aguarda o frame terminal, `decode_raw` contra o retorno escalar.

Estaticamente o proxy é `cast` para `Cls`, então `tail(...)` já tipa como `AsyncIterator[T]` e `ingest(...)` como `Awaitable[int]` sem trabalho extra.

## 6. Dispatch local

Dono == self: sem wire. O in-iter e o out-sink viram filas `asyncio` in-process (ou o próprio async generator conectado direto), ligando o `async for` do chamador ao handler streaming do host. Mesma semântica, mesmos erros tipados; espelha o `_Local` da spec 04.

## 7. Falhas e migração

- **Dono desativa / range migra** no meio → handler cancelado → `ERROR(RANGE_MOVING | ActorUnavailableError)` → `__anext__` do chamador levanta o erro. Sem resume: o chamador reabre do zero se quiser.
- **Conexão cai** → `BulkReceiver` já superfície `ConnectionLostError`/`StreamResetError`; o `async for` do chamador levanta.
- **Chamador abandona** (`break`) → RST; o handler do dono é cancelado e o worker liberado.
- **WRONG_OWNER** na abertura → o chamador re-resolve e reabre **uma vez** (seguro: nenhum elemento foi processado ainda). Depois do primeiro `ITEM` consumido, WRONG_OWNER não reabre — o stream é stateful.

## 8. Guardrails v1

- No máximo um param `AsyncIterator` por método.
- Sem resume/cursor/exactly-once — erro é terminal.
- Backpressure = janelas de crédito do bulk stream; sem buffer adicional.
- Streaming roteia por key como chamada unária; sem stream cross-key.
- Fan-out pub/sub (N subscribers) e durabilidade ficam fora — um subscriber por chamada.

## 9. Etapas de implementação

Cada uma com verificação própria; a seguinte só começa com a anterior verde.

1. **Registry + tipos** — detecção de `AsyncIterator` em params/retorno, `MethodInfo` estendido, guardrails em import time.
   *Verificação: método streaming válido registra; dois params iterador / iterator não-serializável / retorno iterator sem async generator → `SerializationSchemaError`. mypy strict limpo.*
2. **Host** — `dispatch_stream`, ramificação em `_handle`, commit único no fim.
   *Verificação: em `casty.local()`, server/client/duplex fim-a-fim; ingest commita e sobrevive à reativação; server-streaming não altera snapshot; `break` cancela o handler e libera o worker.*
3. **Wire + node** — kind `casty.stream`, frames ITEM/ERROR, `on_bulk`, `_on_actor_stream`, `BulkSender` sobre stream aceito, dispatch local.
   *Verificação: cluster real nas três formas; backpressure observável (consumidor lento freia o produtor via créditos); matar o dono no meio → erro tipado no `__anext__`; WRONG_OWNER reabre uma vez.*
4. **Proxy + superfície pública** — `__getattr__` streaming, export, exemplo em `examples/`.
   *Verificação: `async for x in node.actor(Cls, key).tail()` tipa e roda; mypy strict limpo na API pública.*
