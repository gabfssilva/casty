# Spec 05 — Replicação, quórum e fencing

Escopo: HLC por key, replicação primary-based do snapshot de estado, ack por W, fencing de minoria, handshake de ativação com quórum, read repair, handoff no `stop()`. Mensagens 0x60–0x7F.

Só se aplica a atores com `replicas > 1`. `replicas=1` (default) mantém o comportamento da etapa 4: estado morre com o dono.

## 1. HLC

```
Hlc { millis: int, counter: int, node: uuid }    # ordem: (millis, counter, node.bytes)
```

Relógio por nó (`HlcClock`): `next()` = `(max(wall_ms, last.millis), counter+1 se empatou senão 0)`; `observe(hlc)` avança o relógio local (usado no handshake, para que o novo dono gere HLCs acima de tudo que adotou). Um HLC por key, incrementado só pelo dono — single writer, sem vector clocks.

## 2. Quórum

`replicas=R`, `write`/`read` ∈ {`ONE`, `MAJORITY`, `ALL`} ou int. **Resolvidos contra o R configurado, nunca contra o tamanho da view** — capar no tamanho da view reabriria o buraco do split-brain (minoria com view de 1 nó acharia W=1). Cluster com menos de W membros alcançáveis → indisponível para escrita, por design. Réplicas de uma key = `ring.replicas_for_token(token, R)` (dono primeiro).

## 3. Escrita (commit por mutação)

Decisão fechada no DESIGN: snapshot completo após cada handler que mutou estado.

1. Handler roda no dono. Ao final, o host encoda o estado (`state_cls`, map msgpack dos fields) e compara com o último snapshot commitado — bytes iguais → sem replicação, responde direto.
2. Mudou → `new_hlc = clock.next()`; `REPLICATE {actor, key, prev_hlc, hlc, state, repair=False}` para as R-1 backups; guarda no store local.
3. Responde ao chamador com **W acks** (o dono conta como 1). Menos que W em `replication_timeout` → **rollback** do estado em memória para o snapshot anterior + `QuorumUnavailableError` ao chamador. É o fencing: dono em minoria não commita nada.
4. **Regra de aceitação na réplica** (modo normal): NACK `STALE_WRITE` se `stored_hlc > prev_hlc` — a réplica conhece história mais nova que a base do dono (dono stale pós-partição). Aceita caso contrário (réplica atrasada é sobrescrita — o quórum garante que a cadeia commitada nunca regride).
5. Dono que recebe `STALE_WRITE`: sua ativação é stale (perdeu escritas de outro dono). Rollback + descarte da ativação (sem hook) + `RangeMovingError` ao chamador. O proxy retenta uma vez (seguro: a escrita não foi aplicada em lugar nenhum); o retry reativa com handshake e estado atual.

## 4. Handshake de ativação

Toda ativação de ator com `replicas > 1`:

1. Dono consulta as R réplicas (`FETCH_STATE {actor, key}`); resposta vazia (`hlc=None`) é autoritativa e conta para o quórum.
2. Exige **W respostas** (contando o store local). Menos → ativação falha com `QuorumUnavailableError` (chamador decide retry).
3. Adota o conjunto de páginas de maior HLC (spec 09 §7: busca só as páginas que faltam); `paging.activate` reconstrói o estado; `clock.observe(hlc)`; hook activate roda depois, com estado já restaurado.
4. **Read repair**: réplicas que responderam com HLC menor recebem `REPLICATE {repair=True}` (aceitação monotônica: aplica se `stored_hlc < hlc`), fire-and-forget.

Ativação dupla (views divergentes) resolve eventualmente: só um dos donos consegue quórum de escrita; o outro leva `STALE_WRITE` na primeira mutação e se descarta. Sem consenso — ativação única é eventual, como no DESIGN.

## 5. Handoff no stop()

Após drenar handlers e antes do leave: para cada entrada `(actor, key) → (hlc, state)` do store local, o nó calcula as réplicas na ring **sem ele mesmo** e envia `REPLICATE {repair=True}` aos alvos que ainda não são réplicas... simplificado: envia a todos os alvos; a aceitação monotônica descarta o que já está atualizado. Best-effort com `handoff_timeout`; como toda mutação já foi commitada em W réplicas, o handoff só encurta a janela de reativação, não é condição de durabilidade.

## 6. Wire

| msg_type | Nome | Semântica |
|---|---|---|
| 0x60 | REPLICATE `{actor, key, prev_hlc, hlc, state, repair}` | ask; réplica guarda e ack; NACK `RemoteError(20)` se stale (modo normal) |
| 0x61 | FETCH_STATE `{actor, key}` | ask; responde `StateReply {hlc, state}` (vazio se não tem) |

Códigos novos: `15 RANGE_MOVING`, `16 QUORUM_UNAVAILABLE`, `20 STALE_WRITE` (interno, nunca chega ao usuário).

## 7. Configuração

```python
replication_timeout: float = 5.0   # espera pelos W acks
handoff_timeout: float = 10.0
```

`@casty.actor(replicas=3, write=casty.MAJORITY, read=casty.ONE, ...)`. `read` é aceito e validado, mas na v1 leitura é o estado em memória do dono, protegido pelo fencing de escrita e pelo handshake (que usa W, garantindo overlap com qualquer escrita commitada); read por método fica para quando houver anotação de método read-only.

## 8. Critérios de verificação da etapa

1. Escrita em cluster de 3 com `replicas=3, write=MAJORITY`: estado sobrevive à morte abrupta do dono (novo dono reativa via handshake com o estado commitado).
2. **Partição**: dono isolado da maioria → escrita levanta `QuorumUnavailableError` e o estado local não avança (rollback). Lado majoritário elege novo dono e segue escrevendo.
3. **Pós-heal**: nó da minoria volta à view; escrita nova baseada em estado stale leva `STALE_WRITE` internamente e o chamador (com retry do proxy) vê a escrita aplicada sobre o estado correto — sem lost update.
4. **Rolling restart** de cluster de 5 nós (`replicas=3, write=MAJORITY`): parar e substituir um nó por vez; nenhum valor perdido ao final.
5. Handshake sem quórum (só 1 nó vivo, R=3, W=2) → `QuorumUnavailableError`.
6. mypy strict limpo.

Partições são simuladas in-process (bloqueio de dial + fechamento de conexões entre grupos); toxiproxy fica para um harness futuro fora do CI.
