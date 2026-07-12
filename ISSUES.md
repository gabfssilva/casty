# Resiliência — issues mapeadas

Levantamento de pontos fracos de tolerância a falhas, com proposta de resolução para cada um.

## Ordenação e dependências

- **3 (dead letters) primeiro** — é destino dos descartes das issues 1, 6, 13, 17 e 19.
- **Transporte em sequência:** 3 → 1 (fila com bound) → 2 (backoff).
- **7 (fencing por epoch) antes de 12** — o fencing torna o cache stale detectável; 12 vira consequência.
- **24 (líder mais antigo) antes de 5** — líder estável reduz o custo do quórum e simplifica o teste.
- **22 e 23 (discovery/liveness) são independentes do resto** — podem ir em paralelo.
- **26 (rebalance) depende de 7** — mover shard sem fencing reintroduz zumbis.

## Decisões fechadas (2026-07-04)

1. Lock (14): lease com `lease_id` + timeout; TTL 30s, renovação interna a cada 10s; fencing token exposto como atributo; sem quebra de API.
2. Fila (16): `dequeue()` inalterado; at-least-once via métodos novos `reserve()`/`ack()`.
3. Quórum (5): desligado por default, ligável via config.
4. Discovery (23): só o callable; sem persistência de peers em disco.
5. Eleição (24): sem suporte a cluster de versões mistas; regra troca sem mecanismo de migração.
6. Leave (25): automático no `shutdown()`; SIGTERM handler opt-in.
7. Rebalance (26): sem flag e sem perda de estado em nenhum cenário — movimento planejado usa handoff ao vivo (estado transferido antes de virar a alocação). Persistência não é o mecanismo de continuidade cross-node: entidade event-sourced entrega eventos/snapshot no handoff; entidade de closure entrega estado respondendo a `HandoffRequested`; todas as estruturas built-in implementam o contrato. Entidade de usuário que não trata handoff não é movida voluntariamente — a capacidade é derivada, nunca configurada.

## Transporte TCP

### 1. Fila do drainer ilimitada

`asyncio.Queue()` sem bound em `src/casty/remote/tcp_transport.py:536`. Um peer fora do ar acumula frames indefinidamente — vazamento de memória se ele nunca voltar.

**Resolução:** trocar por `asyncio.Queue(maxsize=N)` com N configurável no `TransportConfig` (ex.: 10_000 frames). Na saturação, aplicar a mesma semântica de overflow do mailbox (`drop_new` / `drop_oldest`), encaminhando o frame descartado para dead letters (issue 3) em vez de perder silenciosamente. O `put` do drainer passa a ser `put_nowait` com tratamento de `QueueFull`, mantendo o caminho de envio não-bloqueante.

### 2. Blacklist fixa de 5s sem backoff exponencial

Falha de conexão adiciona o peer à blacklist por `blacklist_duration` fixo (`src/casty/remote/tcp_transport.py:334`, `:622`). Outages transientes esperam 5s inteiros; peers permanentemente mortos são retentados na mesma cadência para sempre.

**Resolução:** substituir a duração fixa por backoff exponencial com jitter por peer: manter um contador de falhas consecutivas no dict de blacklist (`{addr: (until, failures)}`) e calcular `min(base * 2**failures, cap) + jitter` (ex.: base 0.5s, cap 30s). Zerar o contador no sucesso de conexão ou no `clear_blacklist` já existente. A mudança fica contida na função de decisão de blacklist — a estrutura de retry em volta não muda.

### 3. Sem dead-letter queue — perda silenciosa de mensagens

Frames enfileirados para um peer que caiu são descartados com warning (`src/casty/remote/tcp_transport.py:569`), e mensagens dropadas por overflow de mailbox também somem sem rastro. Entrega é at-most-once sem observabilidade da perda.

**Resolução:** criar um ator de sistema `/dead-letters` que recebe `DeadLetter(message, recipient, reason)` e publica no `EventStream` já existente. Todos os pontos de descarte (drainer, overflow de mailbox, peer down, reply de ask após timeout) passam a rotear para ele em vez de logar e sumir. Consumidores assinam via event stream para métricas ou reprocessamento. Não muda a garantia de entrega — torna a perda observável.

## Supervisão

### 4. Restart imediato, sem delay exponencial nem jitter

`OneForOneStrategy` limita restarts por janela deslizante (`src/casty/core/supervision.py:54`), mas cada restart é imediato. Falha causada por dependência externa (banco fora) gera rajadas de restart até estourar o limite, e múltiplos filhos falhando juntos reiniciam em sincronia (thundering herd).

**Resolução:** adicionar `BackoffStrategy` (ou parâmetros `backoff_base`/`backoff_max`/`jitter` na estratégia existente): antes de reiniciar, agendar o restart via scheduler com delay `min(base * 2**n_restarts, max) * uniform(0.8, 1.2)`. O contador de restarts por `child_id` já existe — só passa a alimentar o cálculo do delay além do limite da janela. Durante o backoff, o mailbox do filho segue acumulando (ou dropando, conforme a política do mailbox).

## Cluster

### 5. Eleição de líder sem quórum — split-brain

O líder é o menor `NodeAddress` entre os membros vivos, decisão puramente topológica. Duas partições isoladas elegem cada uma o seu líder; o coordinator realoca shards unilateralmente e o singleton pode rodar duplicado (documentado em `src/casty/cluster/singleton.py`).

**Resolução:** mitigação incremental antes de qualquer consenso completo: exigir que o líder enxergue maioria dos membros conhecidos (`len(reachable) > len(all_members) // 2`) para exercer o papel — partição minoritária fica sem líder (coordinator em modo `pending`, singleton parado) em vez de divergir. Custo: cluster de 2 nós perde disponibilidade quando 1 cai; documentar e permitir opt-out via config para quem prefere disponibilidade. Consenso real (Raft para o estado do coordinator) fica como evolução separada, se necessário.

### 6. Buffer ilimitado de `GetShardLocation` no follower/pending

O coordinator em modo `pending` e o follower bufferizam requisições em tuplas sem limite (`src/casty/cluster/coordinator.py:200`, `:279`, `:455`) enquanto o líder está inalcançável. Líder ausente por muito tempo = crescimento sem bound + requisições respondidas tarde demais para o caller (ask já expirou).

**Resolução:** limitar o buffer (ex.: 1_000 entradas) e anexar um deadline a cada entrada bufferizada. No flush, descartar entradas cujo deadline passou (rotear para dead letters); na saturação, responder imediatamente com falha em vez de enfileirar. O caller já trata timeout de ask, então falhar rápido é estritamente melhor que responder tarde.

### 7. Rejoin sem handoff — réplicas zumbis

Quando um nó é marcado down e o shard é realocado, réplicas antigas no nó que volta podem continuar aceitando escrita (`src/casty/cluster/topology_actor.py:392-402`). Não há cerca (fencing) entre a época antiga e a nova.

**Resolução:** usar o epoch já existente nas alocações como fencing token: primary e réplicas guardam o epoch da sua alocação e rejeitam comandos/replicação com epoch menor que o corrente conhecido via gossip. No rejoin, o nó compara as alocações locais com o snapshot recebido e para entidades cujo shard foi realocado (epoch maior em outro nó). O mecanismo de versionamento já existe — falta propagá-lo até a entidade e verificar no caminho de escrita.

### 8. Heartbeat sem timeout próprio

`HeartbeatRequest` confia no timeout do TCP para detectar peer travado (processo vivo, event loop bloqueado) — detecção pode levar minutos dependendo do SO (`src/casty/cluster/topology_actor.py:547-562`).

**Resolução:** nenhuma mudança no protocolo é necessária — o phi accrual já detecta ausência de resposta: se o peer travado não responde, o intervalo desde o último heartbeat cresce e o phi estoura o threshold no `CheckAvailability`. Validar com teste (peer que aceita conexão mas não responde heartbeat) que a detecção ocorre em tempo previsível; se o teste mostrar que o envio bloqueia o loop de heartbeat, envolver o send em `wait_for` curto. Provável que seja só teste + ajuste pontual.

## Persistência e replicação

### 9. Recovery carrega o event log inteiro em memória

`Journal.load()` retorna todos os eventos de uma vez (`src/casty/core/journal.py:373-395`) — O(n) de memória e latência na recuperação de entidades com histórico longo.

**Resolução:** mudar o protocolo de `load()` para `AsyncIterator[PersistedEvent]` (ou adicionar `load_stream()` mantendo `load()` por cima) e fazer o replay do event sourcing consumir o iterador incrementalmente. No SQLite, vira um cursor com `fetchmany` em lotes; no in-memory, um generator sobre a lista. Combinado com snapshot policy, o replay pós-snapshot tende a ser curto — mas o streaming remove o pior caso.

### 10. Ack de replicação com timeout fixo global

O primary espera `min_acks` dentro de `ack_timeout` fixo (default 5s). Uma réplica lenta consome o timeout inteiro em toda escrita; não há tuning por réplica nem detecção de réplica consistentemente lenta.

**Resolução:** primeiro passo barato: como `min_acks` já permite prosseguir sem a réplica lenta, garantir que o primary responda ao caller assim que atinge `min_acks` (não esperar o timeout) — verificar se já é o caso e cobrir com teste. Segundo passo: registrar latência de ack por réplica (EWMA) e emitir warning/métrica quando uma réplica fica consistentemente acima do percentil — insumo para o coordinator realocar. Timeout adaptativo por réplica só se a métrica mostrar necessidade real.

### 11. Escritas do journal SQLite serializadas por lock, sem batching

`SqliteJournal` usa lock de thread por escrita (`src/casty/core/journal.py:331`, `:365-371`) — contenção sob escrita concorrente de muitas entidades.

**Resolução:** agrupar escritas: um writer único (task dedicada) consome uma fila de `(persistence_id, events)` e faz commit em lote (`executemany` + um commit por batch, coalescendo o que estiver na fila). WAL já permite leituras concorrentes durante o batch. Fica transparente para o caller — `persist()` continua resolvendo quando o batch que contém seus eventos commita. Medir antes: só vale se benchmark mostrar contenção real.

## Mensageria

### 12. Cache de alocação de shard sem invalidação precisa

Followers servem alocações cacheadas e só atualizam no bump de epoch do `TopologySnapshot` (`src/casty/cluster/coordinator.py:424-431`) — janela de cache stale durante transições.

**Resolução:** tratar como consequência da issue 7: com fencing por epoch na entidade, um envio para o nó errado é rejeitado com o epoch corrente na resposta, e o proxy/cliente invalida a entrada de cache e reconsulta o coordinator. Ou seja, em vez de tentar tornar o cache sempre fresco (impossível sem coordenação), tornar o miss detectável e barato de corrigir com retry único.

### 13. Reply de ask atrasado é dropado sem rastro

O ask cria um ator temporário e resolve um future com timeout (`src/casty/core/system.py:167-191`); reply que chega depois do timeout é descartado silenciosamente, e não há limite de asks em voo.

**Resolução:** no ator temporário de ask, se o future já está `done()` quando o reply chega, rotear a mensagem para dead letters (issue 3) com `reason="late-reply"` — dá visibilidade a timeouts mal calibrados. Limite de in-flight não vale a complexidade agora: cada ask é um ator temporário barato e o timeout já limita a janela; registrar como não-issue a menos que apareça em profiling.

## Estruturas distribuídas

### 14. Lock sem lease nem fencing — deadlock permanente ou exclusão mútua violada

`lock_held` só transiciona quando o holder envia `LockRelease` (`src/casty/distributed/lock.py:95-129`) — se o nó do holder morre, o lock fica preso para sempre e os waiters bloqueiam indefinidamente. O problema tem também a direção oposta: `lock_entity` é comportamento puramente em memória (`lock.py:72-73`), então crash/rebalance do nó que hospeda a *entidade* reinicia o lock como `lock_free` — outro cliente adquire enquanto o holder original acredita que ainda o tem.

**Resolução:** lease com TTL e fencing token: `LockAcquired` carrega `(lease_id, deadline)`; o holder renova via `LockRefresh` periódico (o client `Lock` faz isso internamente com o scheduler) e a entidade agenda um `LeaseExpired` para si que libera o lock e atende o próximo waiter se o refresh não chegar. O fencing token (contador monotônico persistido via event sourcing, como a fila persistente já faz) permite que recursos protegidos rejeitem operações de um holder expirado. Persistir o estado também fecha o caso do rebalance. Documentar que lock distribuído sem fencing no recurso protegido é inerentemente advisory.

### 15. Barrier trava para sempre se um participante morre

`barrier_waiting` acumula waiters e só libera quando `len(waiters) >= expected` (`src/casty/distributed/barrier.py:50-74`). Participante que morre antes de chegar deixa os demais bloqueados indefinidamente; não há timeout nem integração com detecção de falha.

**Resolução:** timeout por rodada: a primeira chegada fixa `expected` e agenda um `BarrierTimeout(round_id)` na própria entidade (TTL configurável no client). Se o timeout dispara antes de completar, responder `BarrierFailed` a todos os waiters e resetar a rodada — falhar todos explicitamente é o único comportamento correto, já que liberar parcial violaria a semântica de barreira. O client `arrive()` converte `BarrierFailed` em exceção.

### 16. Dequeue sem ack — item perdido se o consumidor morre

`Dequeue` faz `popleft()` e responde imediatamente (`src/casty/distributed/queue.py:77-78`). Se o reply se perde (ask expirou, nó do consumidor caiu, frame dropado pelo transporte), o item saiu da fila e não existe mais em lugar nenhum — perda de dados mesmo na variante persistente, que journala o dequeue antes de o consumidor receber.

**Resolução:** dequeue em duas fases via métodos novos, `dequeue()` inalterado: `reserve()` move o item para um buffer in-flight `(item, delivery_id, deadline)` em vez de removê-lo; o consumidor confirma com `ack(delivery_id)`, e um tick da entidade re-enfileira itens cujo deadline venceu (at-least-once — documentar que o consumidor precisa ser idempotente). O `dequeue()` atual permanece como variante rápida at-most-once para quem não precisa da garantia.

### 17. Buffer do ClusterClient ilimitado quando o cluster está inacessível

O proxy do client bufferiza envelopes por shard sem limite enquanto não conhece a alocação (`src/casty/client/client.py:136-141`). Com todos os seeds fora do ar, a aplicação continua enviando e o buffer cresce sem bound até esgotar a memória do processo cliente.

**Resolução:** mesmo tratamento da issue 6: bound configurável no buffer (total e/ou por shard) com deadline por entrada; na saturação ou expiração, falhar o envio imediatamente (exceção no ask / dead letter no tell) em vez de enfileirar. Falha rápida no cliente é melhor sinal para a aplicação do que espera indefinida com memória crescendo.

## Ciclo de vida e mensageria (continuação)

### 18. EventStream mantém subscribers mortos para sempre

O `event_stream_actor` guarda refs em tuplas e só remove via `Unsubscribe` explícito (`src/casty/core/event_stream.py:44-60`). Subscriber que para sem desassinar continua recebendo `tell()` a cada publish — cada evento vira um DeadLetter, e se dead letters forem publicados no próprio stream, há risco de laço de retroalimentação.

**Resolução:** o event stream passa a observar (`watch`) cada handler no `Subscribe` e remove a ref ao receber `Terminated` — o mecanismo de DeathWatch já existe no core, é só usá-lo aqui. Garantir também que a publicação de `DeadLetter` cujo destino é um subscriber do próprio stream não republique (guard simples por tipo no caminho de publish).

### 19. Falha de serialização descarta a mensagem silenciosamente

O drainer captura `Exception` na serialização, loga e segue (`src/casty/remote/tcp_transport.py:549-555`). Mensagem com payload não-serializável some sem o remetente saber; se um ator produz sistematicamente payloads inválidos, nada circuit-breaka.

**Resolução:** rotear a falha para dead letters (issue 3) com `reason="serialization-error"` e a exceção anexada — mesmo destino dos outros descartes, dá observabilidade sem mudar o contrato at-most-once. Validação prévia no `tell()` seria o ideal ergonômico, mas serializar duas vezes é caro; um modo debug opcional (`config.validate_serialization`) que serializa no enqueue e falha no caller cobre desenvolvimento sem custo em produção.

### 20. Mensagens de infraestrutura perdidas durante shutdown

`system.shutdown()` para atores de usuário e depois scheduler, task runner e event stream (`src/casty/core/system.py:211-221`). Um `pipe_to_self` que completa durante a janela de shutdown entrega via task runner já parado e a mensagem some sem rastro.

**Resolução:** de menor severidade — em shutdown, perda de mensagens in-flight é semântica aceitável para um sistema at-most-once. O que vale corrigir é a observabilidade e a ordem: drenar o task runner (aguardar tasks em voo com timeout curto) antes de pará-lo, e rotear entregas pós-shutdown para dead letters em vez de dropar. Documentar a garantia ("mensagens in-flight podem ser perdidas no shutdown") no docstring de `shutdown()`.

## Cluster (continuação)

### 21. Estado de gossip cresce para sempre — membros `removed` nunca são podados

Membros transicionam até `removed` mas nunca saem do `frozenset` de `ClusterState`, e o vector clock acumula entradas de nós mortos (`src/casty/cluster/state.py`). Cluster com churn (autoscaling, substituição de nós) carrega todos os membros históricos em cada mensagem de gossip — tamanho de mensagem, memória e convergência degradam com a idade do cluster.

**Resolução:** poda com tombstone temporal: membro em `removed` ganha timestamp; o líder remove do estado (membro e entrada do vector clock) os que passaram de um TTL (ex.: 24h — longo o suficiente para que gossip antigo em trânsito perca para o merge por status, curto o suficiente para conter o crescimento). A poda precisa ser propagada como mudança de estado normal via gossip para todos convergirem. É a issue mais delicada de implementar corretamente em CRDT — um nó isolado por mais que o TTL pode "ressuscitar" um membro podado; mitigar exigindo que rejoin após poda passe pelo fluxo de join normal. Sob churn (issue 22+), o crescimento é linear no número de nós que *já passaram* pelo cluster, não no tamanho atual — em spot instances isso degrada em dias, não meses; incluir também o set `handled_unreachable` do topology actor na poda.

## Cluster dinâmico (spot instances / churn constante)

Cenário: nós entram e saem a cada poucos minutos (spot/autoscaling). Pouco dado, muito churn de membership.

### 22. ClusterClient para de acompanhar a topologia após o primeiro snapshot

O `topology_subscriber` só reagenda o timer de liveness e rotaciona contact points enquanto `last_snapshot is None` (`src/casty/client/client.py:371-389`); depois do primeiro snapshot, `SubscriptionTimeout` é ignorado e nenhum timer novo é agendado. Se o contact point morre depois disso — inevitável em spot —, o client nunca percebe: fica servindo topologia congelada até todos os nós que conhece sumirem. Agravante: `_build_contacts` inclui membros aprendidos do snapshot, mas só é chamado no caminho guardado por `last_snapshot is None`, onde o snapshot é sempre `None` — na prática os membros aprendidos nunca são usados.

**Resolução:** liveness contínuo: reagendar o timer a cada snapshot recebido (o `/_topology` já publica em toda mudança; em cluster estável, adicionar um snapshot periódico ou heartbeat da assinatura). No timeout, re-assinar rotacionando pelos contacts construídos do último snapshot — aí o `_build_contacts` passa a cumprir o papel para o qual foi escrito, e o client sobrevive à substituição completa dos seeds originais desde que tenha recebido ao menos um snapshot com os nós novos.

### 23. Nó com seeds estáticos não consegue entrar num cluster saudável

`seed_refs` é construído uma vez a partir da config e o `JoinRetry` (1s) tenta os mesmos seeds para sempre (`src/casty/cluster/cluster.py:183-195`, `:322-337`). Nó novo (ou reiniciado) cujos seeds já foram todos substituídos fica preso em retry eterno contra endereços mortos, mesmo com o cluster saudável ao lado. Em spot, a lista de seeds de qualquer config estática envelhece em horas.

**Resolução:** desacoplar "seeds" de "endereços fixos": aceitar um callable `discovery: () -> list[(host, port)]` como fonte de contact points, reconsultado a cada tentativa de join — cobre DNS, API da cloud, arquivo, ou lista estática como caso trivial (mesmo padrão do `address_map` que já virou callable no transporte). Complemento barato: persistir os últimos N peers conhecidos em disco e usá-los como fallback no restart. O ClusterClient (issue 22) deve aceitar o mesmo callable.

### 24. Eleição por menor endereço + churn = líder trocando o tempo todo

O líder é `min(address)` entre os membros `up`, recomputado a cada snapshot, sem qualquer histerese (`src/casty/cluster/state.py:514-525`). Em spot, com endereços aleatórios, todo join tem chance de trazer um endereço menor que o do líder atual — e cada troca custa: coordinator demove/promove com bump de epoch e invalidação de caches em todos os nós, e o singleton é *parado no líder antigo e respawnado do zero no novo* (`src/casty/cluster/singleton.py:4-6`, `:117-118`) — singleton não-persistente perde o estado a cada troca, potencialmente a cada poucos minutos.

**Resolução:** trocar o critério de eleição de menor endereço para membro mais antigo (ordem de join, com endereço só como desempate) — nó novo nunca desloca o líder atual, então troca de líder passa a acontecer apenas quando o líder de fato morre, que é o mínimo possível. Precisa de um marcador de antiguidade monotônico no `Member` (epoch de promoção a `up`, atribuído pelo líder no momento do up) propagado via gossip como o resto do estado. É o mesmo racional do "oldest member" do Akka.

### 25. Sem leave voluntário — toda saída é tratada como crash

O status `leaving` existe no enum, mas nenhuma API o usa: não há mensagem de saída voluntária no codebase, então todo shutdown vira o caminho de falha — phi accrual detecta (segundos de janela), líder faz auto-down (`src/casty/cluster/topology_actor.py:587`), e só então shards failover e singleton respawna. Spot instances dão ~2 minutos de aviso que hoje são desperdiçados; sob churn, o cluster vive permanentemente em recuperação reativa de saídas que eram anunciáveis.

**Resolução:** protocolo de leave gracioso: `system.leave()` (chamado pelo `shutdown()` clusterizado e opcionalmente por signal handler de SIGTERM) marca o próprio membro como `leaving` via gossip; o líder, ao ver `leaving`, realoca os shards e coordena handover do singleton *antes* de marcar `down`, e o nó só encerra o transporte depois do ack (com timeout para não travar o shutdown). Elimina a janela de indisponibilidade em toda saída planejada — que em spot é a maioria.

### 26. Join não rebalanceia — shards concentram nos sobreviventes antigos

`handle_node_down()` realoca apenas em falha; `RegisterRegion` de nó novo não move nada (`src/casty/cluster/coordinator.py:334-352`, `:375-385`). Não thrashing é a escolha certa como default, mas sob churn contínuo o efeito acumulado é skew: nós novos ficam ociosos e os shards se concentram nos poucos nós antigos — justamente os próximos a serem terminados, tornando cada falha deles mais cara.

**Resolução:** rebalance incremental automático com handoff ao vivo: ao registrar região nova, o líder move no máximo N shards por intervalo (cap + cooldown) do nó mais carregado para o menos carregado, parando quando a razão max/min fica abaixo de um limiar. Mover é um protocolo de três passos — o primary antigo entrega o estado, o nó novo confirma a restauração, e só então a alocação vira (com o fencing da issue 7 bloqueando escrita no nó antigo). A fonte do estado depende da entidade, nunca de flag: event-sourced (journal em memória incluído) entrega eventos/snapshot; entidade de closure responde a um `HandoffRequested(reply_to)` do framework com seu estado serializável, que a factory no destino recebe em `(entity_id, estado)`. Todas as estruturas built-in (queue, dict, counter, set, lock, barrier) implementam o contrato. Entidade de usuário que não trata `HandoffRequested` não é realocada voluntariamente — capacidade derivada da própria entidade. O cap por intervalo é o que impede o thrashing que justificou não rebalancear; mensagens que chegam durante o handoff são bufferizadas pela região e reencaminhadas após a virada (mesma janela curta do failover atual).

### 27. Detector de falha sem histórico para nós recém-entrados

Nó novo não tem amostras de heartbeat; o phi usa `first_heartbeat_estimate_ms` (1000ms) com desvio frouxo até acumular histórico (`src/casty/cluster/failure_detector.py:102-107`). Sob churn, uma fração constante do cluster está sempre em warm-up — detecção de morte de nó recém-entrado (comum em spot: instância reclamada logo após provisionar) é mais lenta que o normal.

**Resolução:** de menor severidade — semear o histórico do nó novo com a média/desvio corrente dos nós estabelecidos (o detector do observador já tem essas amostras dos outros peers) em vez da estimativa fixa. Ganho marginal; só vale junto de outra mexida no failure detector. Registrar principalmente para documentar o comportamento esperado.
