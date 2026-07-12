# Spec 09 — Estado paginado e replicação parcial

Escopo: replicar apenas o que mudou no estado de um ator, em vez do snapshot integral da spec 05. O estado de uma instância vira um conjunto de **páginas**; o commit carrega só as páginas que o handler mexeu. Três regimes de granularidade, decididos por campo: integral, dict-tracked, e pager.

Motivação: o design da spec 05 encoda e replica o estado inteiro a cada handler mutante — com estado de GB isso estoura `max_message_bytes`, mata a conexão, e o custo de encode é O(estado) por commit.

Status: **implementada**. Medido no cluster in-process de `tests/unit/`:

| estado | commit | wire (2 réplicas) | integral seria |
|---|---|---|---|
| 100k entradas (`dict[str, int]`, 360 KB) | muta 1 entrada | **514 B** | ~740 KB |
| 2,5M x 5 float64 (96 MB) | edita 1 célula | **258 KB** | 192 MB |
| 65 KB (3 campos) | muta 1 campo `int` | **504 B** | ~131 KB |

## 1. Modelo de páginas

Uma **página** é a unidade de commit e replicação: `(actor, instance_key) → {page_key → (hlc, bytes)}`.

- O `ReplicaStore` guarda, por instância, `Stored(hlc, pages)`: o HLC do último commit e as páginas que ele deixou. Não há manifesto separado — o índice *é* o dict de páginas.
- `page_key` é hierárquica: `"campo"` (integral), `"campo/k"` (entrada de dict), `"campo/col/bloco"` (pager). Nomes de campo são identificadores Python, então o primeiro `/` sempre separa o campo do resto, e o resto é opaco para o framework.
- **HLC por commit**, não por página: single writer por instância continua valendo, então as páginas compartilham a linha do tempo da instância. Cada página guarda o HLC do commit que a escreveu — não para ordenar, mas para o diff de catch-up (§7).
- **Página ausente = valor default do campo.** Um campo que nunca saiu do default não custa nada no wire, e a ativação o reconstrói a partir de `Integral.default` (encodado uma vez no registro).

## 2. Delta só aplica sobre a base em que foi calculado

É o ponto que replicação por delta introduz e que a spec 05 não tinha. Um `Replicate` declara `prev_hlc` (a base do dono) e `hlc` (o commit). Contra o HLC corrente `C` da réplica:

| situação | réplica | dono |
|---|---|---|
| `C` mais novo que `prev_hlc` | NACK `CODE_STALE_WRITE` | fencing: descarta a ativação (spec 05 §3) |
| `C == prev_hlc` | aplica o delta | ack |
| `C` mais velho que `prev_hlc` | NACK `CODE_NEED_FULL` | reenvia o conjunto **completo** de páginas para essa réplica |

Sem o caso 3, uma réplica que perdeu um commit (W=2 de 3: a terceira réplica pode perder todos) aplicaria o delta sobre uma base velha e divergiria em silêncio — com o HLC mais alto, seria adotada na próxima ativação. O NEED_FULL fecha o buraco e é auto-curável: custa um RTT extra na primeira escrita depois do gap, e a réplica volta a receber deltas.

Não há watermark por réplica no dono: a checagem na réplica é a verdade, e o reenvio completo é a resposta.

**Tombstone não é necessário** (era um ABERTO). Deleção viaja em `Replicate.dropped`; réplica atrasada não recebe delta (leva NEED_FULL → substituição completa); réplica atrasada detectada na ativação recebe read repair, cujo `dropped` sai do diff de índices. Não existe caminho em que uma página deletada ressuscite, porque o índice do dono é sempre autoritativo.

## 3. Regimes por campo

Decididos estaticamente no `@casty.actor` (`ActorInfo.regimes`), e **inspecionáveis com `casty.explain(Actor)`** — dois regimes escolhidos por type hint não podem ser invisíveis:

```
app.Doc  replicas=3 write=2
  title   str                   integral  immutable: compared, not re-encoded
  body    list[str]             integral
  index   dict[str, int]        tracked   one page per entry
  nested  dict[str, list[int]]  integral
  df      DataFrame             paged     PandasPager
```

(A coluna de tipo é a anotação como o módulo a declara — sob `from __future__ import annotations` ela sai como o texto escrito, `pd.DataFrame`.)

| regime | quando | detecção de mutação | página |
|---|---|---|---|
| **integral** | default | re-encode do campo + compara bytes contra a base | 1 por campo |
| **dict-tracked** | `dict[K, V]`, `V` imutável, default vazio | wrapper marca as keys tocadas | 1 por entrada |
| **pager** | campo declarado com `casty.paged(pager)` | delegada ao pager (§5) | definida pelo pager |

Regras:

- `dict[K, V]` com **V mutável** cai no integral: mutação in-place (`d[k].append(x)`) não passa pelo wrapper e seria falso-negativo. `casty.MultiMap` (`dict[bytes, set[bytes]]`) é integral por isso; `casty.Map` (`dict[bytes, bytes]`) é tracked.
- Imutabilidade é **por campo** (`Integral.immutable`), não tudo-ou-nada: campo imutável cujo valor ainda compara igual não pode ter sido mutado in-place, então nem é re-encodado. Contam como imutáveis: escalares, enums, `datetime`, `uuid`, tuplas/frozensets deles, e **`@casty.message` que seja `frozen` até o fim** (é o que torna `dict[str, PedidoFrozen]` tracked).
- `K` é `str`, `int` ou `bytes` (o que o serde já aceita como chave de dict); `bytes` vira base64 urlsafe na page key.
- **Default vazio é obrigatório** num campo tracked, e é checado no registro. Não há como distinguir "entrada ausente porque foi deletada" de "entrada ausente porque está no default" sem tombstone — então o default não-vazio é recusado em import time em vez de corromper em runtime.
- Deep proxy foi avaliado e descartado: extensões C (`PyList_SET_ITEM`, buffers numpy/pandas) mutam por baixo de interceptação Python → falso-negativo = corrupção silenciosa. Todo regime aqui erra só para o lado de replicar a mais.

### Limitação: página é atômica no wire

Uma página não pode ser dividida entre mensagens, então precisa caber sozinha em `max_message_bytes`. Campo integral que encoda acima disso falha o commit com `SerializationError` nomeando o campo — erro local e acionável, em vez da morte da conexão que o design da spec 05 provocava.

## 4. Dict-tracked

`paging.TrackedDict` é um **`MutableMapping`, não subclasse de `dict`**: os métodos C do `dict` (`update`, `pop`, `clear`, `|=`) escrevem direto na hash table sem passar por `__setitem__`, e uma mutação perdida ali seria divergência silenciosa. Nos mixins do `MutableMapping` todo mutador cai em `__setitem__`/`__delitem__`, então a cobertura é estrutural, não uma lista de overrides que alguém pode esquecer de atualizar. (`codec._encode_value` casa `Mapping()`, não `dict()`, para que o wrapper serialize.)

- Mantém `dirty` e `deleted`, limpos no commit. Cada key suja ainda é comparada contra a base antes de virar página — key reescrita com o mesmo valor não vai ao wire.
- Handler que **reatribui o dict inteiro** (`self.d = {...}`) perde o wrapper. O diff detecta (o valor não é um `TrackedDict`), reembrulha, e trata como rewrite completo: toda entrada suja, e as keys da base que sumiram viram `dropped`.
- Rollback restaura só as entradas que o commit falho teria carregado — não reconstrói o dict.

## 5. Pagers

```python
class Pager[T](Protocol):
    def snapshot(self, value: object) -> object: ...            # handle barato do valor atual
    def pages(self, value: object, previous: object) -> PageSet: ...  # o que mudou desde o snapshot
    def restore(self, pages: Mapping[str, bytes]) -> T: ...     # mapping vazio = default do campo
    def rollback(self, previous: object) -> T: ...              # o snapshot *é* o valor pré-handler
```

`T` aparece só em posição de retorno, então é covariante: `casty.paged[T](pager: Pager[T]) -> T` tipa o field pelo pager, e o framework guarda o mesmo objeto como `Pager[object]`. Sem `Any`, sem cast.

A verdade é o `pages()` contra o snapshot anterior — nunca interceptação de escrita. Falso-positivo permitido; falso-negativo proibido. O shadow vive em `paging.State.shadows`, privado da ativação.

### 5.1 `casty.pagers.PandasPager` (extra `pandas`)

- `snapshot` = `df.copy(deep=False)` + o endereço de buffer de cada coluna: **17 µs** para 2M x 5, zero cópia.
- `pages` compara endereços (**33 µs**): o CoW do pandas copia o bloco na primeira escrita por *qualquer* caminho, porque a checagem vive no block manager. Coluna cujo buffer não se moveu não foi escrita.
- Blocos são **consolidados**, então escrever uma coluna move o buffer das vizinhas. O refinamento por row-block (`.equals` contra o shadow) é o que impede as vizinhas de chegarem ao wire — e sai de graça, porque o shadow mantém os buffers antigos vivos.
- Páginas: `schema` (colunas, linhas, RangeIndex, nomes dos eixos), `index/{bloco}` (só se o índice não for RangeIndex), `col/{b64(nome)}/{bloco}` — Arrow IPC, blocos de 16.384 linhas.
- `rollback` devolve o shadow: custo zero, e resolve o único caso onde restaurar da base seria caro.
- **pandas < 3.0 é recusado no construtor**, que roda no corpo da classe do ator: erro na declaração, não em runtime. Abaixo de 3.0 o CoW pode ser desligado e `.to_numpy()` devolve view gravável — as duas coisas tornam um falso-negativo possível.
- Escape numpy fechado: em frame homogêneo `.values`/`.to_numpy()` são **read-only**; em frame misto o pandas devolve uma **cópia**, e a escrita nunca chega ao frame. Testado (`test_numpy_cannot_write_behind_the_pagers_back`), junto com os 11 caminhos de escrita que o pandas oferece.
- Limitações, todas ruidosas em vez de silenciosas: nomes de coluna `str` e únicos, sem MultiIndex; mudança no número de linhas re-pagina o frame (os blocos deixam de alinhar).
- `_mgr`/`mgr_locs` são API interna do pandas; os Protocols em `pagers.py` dizem exatamente o que é usado, então um upgrade que mude o formato quebra ali e alto.
- Custo de memória: o dono guarda o frame vivo *e* as páginas Arrow commitadas (é réplica de si mesmo). ~2x. Regenerar páginas sob demanda no fetch é uma otimização possível, fora de escopo.

## 6. Commit multi-mensagem e atomicidade

Um commit que não cabe em uma mensagem vira N `Replicate` com o mesmo `hlc`; só o último leva `final=True`. A réplica acumula as parciais em staging e **publica no `final`** — nunca expõe meio commit, e o fencing/NACK é avaliado em cada parcial (falha rápido, sem bufferizar megabytes à toa).

- Batch alvo: `Config.replication_batch_bytes` (256 KiB). Os overheads de framing (`_PAGE_OVERHEAD`, `_REF_OVERHEAD` = 96 B) são **medidos**, não chutados: o HLC embutido é uma mensagem própria e custa a maior parte deles.
- Rollback do dono em falha de quórum: restaura só as páginas sujas a partir da base (`paging.State.pages`, os bytes commitados). O caso caro (restaurar um df de GB) some no regime pager, onde o rollback é devolver o shadow.
- Falha no *encode* (não no quórum) desfaz o estado inteiro a partir da base: o diff pode ter parado no meio, então nenhum campo é confiável. Sem isso a exceção escapava do worker e o chamador ficava pendurado para sempre.
- Staging abandonado (dono morreu no meio de uma transferência) fica preso até o próximo commit daquela instância. Limite: um commit em voo por instância.

## 7. Ativação, read repair e handoff

- **Ativação** (spec 05 §4): `FETCH_STATE` devolve o índice `[(page_key, hlc, size)]` de cada réplica, com as páginas inline quando o estado inteiro cabe no batch (1 RTT para ator pequeno). Adota o HLC mais alto com W respostas, e busca **só as páginas que faltam**: página local com o mesmo `(page_key, hlc)` da fonte tem os mesmos bytes (o HLC identifica o commit que a escreveu, e é único por nó). Reativação no mesmo nó não busca nada.
- **Índice grande** (era um ABERTO): 1M de entradas não descrevem seu índice numa mensagem. `FetchState.after` é um cursor **posicional** e `StateReply.more` diz que há mais — o dict de páginas só muda num commit, e um commit move o HLC da entry, que o scanner confere. Índice rasgado não passa: vira `RangeMovingError` e o caller reativa.
- `FETCH_PAGES` traz o resto em batches, 4 em voo, com a mesma guarda de HLC.
- **Read repair**: cada réplica atrasada recebe exatamente as páginas em que o índice dela diverge do adotado — não o estado inteiro. Varre o índice da réplica em background quando ele não cabe numa resposta.
- **Handoff** (spec 05 §5): empurra o conjunto completo de páginas de cada instância para o replica set do anel sem este nó, em batches (20k páginas → dezenas de mensagens, nunca um ask por página).

## 8. Wire

| msg_type | Nome | Semântica |
|---|---|---|
| 0x60 | `Replicate {actor, key, prev_hlc, hlc, pages, dropped, full, final, repair}` | ask; ack, ou NACK `STALE_WRITE`/`NEED_FULL` |
| 0x61 | `FetchState {actor, key, after}` → `StateReply {hlc, index, pages, more}` | índice (em chunks se preciso); páginas inline se o estado for pequeno |
| 0x62 | `FetchPages {actor, key, keys}` → `PagesReply {hlc, pages}` | batch de páginas de um commit conhecido |

Sem versionamento: o protocolo paginado substitui o da spec 05 (não há cluster em produção para manter compatível).

## 9. Semântica alterada

- **Mutação no `activate` hook**: antes, o basis era o encode pós-hook, então mudanças de estado feitas no hook nunca eram replicadas. Agora o basis são as páginas commitadas, e as mudanças do hook pegam carona no próximo commit — inclusive num handler read-only, que passa a commitar. Depende de `_Activation.last_values` ser tirado **antes** do hook: o atalho de campo imutável (§3) compara contra ele, não contra a base, e um snapshot pós-hook faria o campo tocado pelo hook parecer limpo.
- `__state__`/`__restore__` deixaram de ser métodos gerados na classe; viraram `ActorInfo.state_of` / `ActorInfo.restore_into` (mata dois `typing.cast`).
- Campo `dict` de valores imutáveis com default não-vazio agora é **erro em import time** (§3).

## 10. Fora do escopo

- Replicação op-based (replicar a operação, não o estado): descartada — exigiria handlers determinísticos, contamina o modelo de programação.
- CDC/content-addressed chunks (dedup estilo Xet) para catch-up: camada futura possível sobre §7.
- Estrutura de dados própria (`casty.Table`): rejeitada em favor de pagers para libs existentes.
- Pager polars: DataFrames imutáveis na prática, diff por identidade de chunks Arrow. Pager separado, mais simples. Não feito.
- Chunking byte-level de uma página única acima de `max_message_bytes` (campo escalar de 10 MB): possível depois, sem mudança de wire.
- Mensagens grandes entre atores (o outro lado da conversa): spec própria.

## 11. Critérios de verificação

`tests/unit/test_paged_state.py` (16) e `tests/unit/test_paged_frames.py` (15), sobre um cluster in-process onde cada byte replicado é contado.

1. ✅ Handler que muta 1 de N campos → só a página desse campo no wire.
2. ✅ Handler read-only → zero bytes.
3. ✅ Réplica que perdeu commits → NACK `NEED_FULL`, recebe o estado completo, converge.
4. ✅ Reativação no nó que já tem as páginas → zero páginas buscadas; nó que não tem nenhuma → busca o estado inteiro.
5. ✅ Falha de quórum → rollback só das páginas sujas; página não tocada sobrevive.
6. ✅ Página acima de `max_message_bytes` → `SerializationError` no caller, conexão viva.
7. ✅ dict-tracked: mutar 1 entrada de 100k → 1 página; deletar entrada → réplica atrasada não a ressuscita.
8. ✅ Todo mutador de dict (`[]=`, `del`, `pop`, `update`, `|=`, `clear`) chega ao wire; reatribuir o dict inteiro rearma o tracker.
9. ✅ Índice grande → varrido em chunks; nenhuma resposta passa de `max_message_bytes`.
10. ✅ Handoff de dict de 20k entradas → mensagens agrupadas, nenhum ask por página.
11. ✅ Pager pandas: editar 1 célula de um df de 96 MB → 258 KB no wire; `assert_frame_equal` após ativação em outro nó; dtypes e índice rotulado sobrevivem.
12. ✅ Nenhum dos 11 caminhos de escrita do pandas escapa do diff; `.values` não escreve por baixo.
13. ✅ pandas < 3.0 → erro na declaração do ator.
14. ✅ `casty.explain(Actor)` nomeia o regime de cada campo (`dict[str, list[int]]` → integral).
15. ✅ Mutação no `activate` hook → replicada no commit seguinte (§9), e o contador sobrevive à reativação em outro nó.
16. ✅ Pager que recusa o valor no meio do diff → estado inteiro desfeito a partir da base, inclusive o campo integral que o handler já tinha escrito; chamador recebe `SerializationError`.
