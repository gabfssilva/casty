# Spec 01 — Transporte e serialização

Escopo: framing multiplexado, handshake, interações (request/reply, fire-and-forget, bulk streams), pool de conexões, TLS, compressão, `@casty.message` e registry. Não cobre membership (spec 02) nem protocolo de atores (spec 04).

O desenho do mux segue a spec do **yamux** (HashiCorp) — header fixo, streams com SYN/ACK/FIN/RST, flow control por créditos — com desvios próprios (handshake HELLO, flag de compressão, delimitação de mensagem). Não é wire-compatible com yamux; é o desenho dele validado por uma década de Consul/Nomad, com o wire nosso.

## 1. Frame

Toda a conexão é uma sequência de frames de header fixo (12 bytes):

```
u8   version    versão do protocolo (atual: 1)
u8   type       tipo do frame (mux, não de mensagem)
u16  flags      bitfield: SYN 0x1 | ACK 0x2 | FIN 0x4 | RST 0x8 | COMPRESSED 0x10
u32  stream_id  0 = frames de conexão (PING, GO_AWAY)
u32  length     bytes de payload após o header
```

Tipos de frame:

| type | Nome | Semântica |
|---|---|---|
| 0x0 | DATA | payload de stream; SYN no primeiro frame abre o stream, FIN fecha o lado do emissor, RST aborta |
| 0x1 | WINDOW_UPDATE | payload vazio; `length` = créditos (bytes) concedidos ao emissor no stream |
| 0x2 | PING | keepalive de conexão; flag ACK distingue ping/pong; `length`=8, payload = opaque |
| 0x3 | GO_AWAY | fechamento da conexão; payload msgpack `{code: int, reason: str}` |

- Violação de protocolo (version desconhecida pós-handshake, type inválido, DATA excedendo janela, flags reservadas ≠ 0) → GO_AWAY e fecha. A conexão é a unidade de sanidade; um stream corrompido não é "consertado", a conexão morre. Erro *de aplicação* dentro de um stream → RST só daquele stream.
- `length` de DATA ≤ `max_frame_bytes` (default 256 KiB). O valor é deliberadamente pequeno: é a granularidade de intercalação — um bulk de GBs são milhares de frames DATA entre os quais frames de outros streams entram. Head-of-line máximo = 1 frame.

## 2. Streams

- `stream_id` ímpar = aberto pelo iniciador da conexão; par = pelo aceitador. Monotônico crescente por lado. 0 é reservado para frames de conexão.
- Abertura: primeiro DATA com SYN. Aceitação implícita (primeiro frame de volta carrega ACK) — sem round-trip extra para abrir.
- Fechamento: FIN fecha o lado de quem envia (half-close; o outro lado ainda pode responder); RST aborta os dois lados imediatamente.
- `max_concurrent_streams` (default 1024, por conexão): SYN acima do limite → RST com code=refused.

### Flow control

- Janela por stream e por direção, inicial `initial_window_bytes` (default 256 KiB).
- Emissor decrementa a janela a cada DATA enviado; janela ≤ 0 → bloqueia (backpressure). Receptor devolve créditos com WINDOW_UPDATE conforme consome — é assim que um consumidor lento freia um produtor de GBs sem afetar os outros streams.
- Receptor pode ampliar a janela logo após o SYN (WINDOW_UPDATE imediato) para streams que sabe serem bulk.
- DATA excedendo a janela disponível = violação de protocolo (conexão fecha; é bug, não condição de corrida).

## 3. Handshake

O iniciador abre o stream 1 (controle) imediatamente após TCP/TLS e envia HELLO como primeira mensagem. Nenhum outro stream é aceito antes do handshake completar (violação → GO_AWAY).

```
HELLO {
  versions:     [int]
  cluster_name: str
  node_id:      bytes(16)  # uuid, estável pela vida do processo
  listen_addr:  str | nil  # nil para lite members sem listener
  role:         "member" | "client"
  capabilities: map        # extensível; hoje: {compression: [str]} em ordem de preferência
}
```

Validação no receptor, na ordem: `cluster_name` (≠ → REJECT code=1), interseção de `versions` (∅ → code=2, escolhida = maior comum), `node_id` ≠ próprio (→ code=3). Sucesso → `HELLO_ACK {version, node_id, listen_addr, role, compression: str | nil}`; falha → `HELLO_REJECT {code, reason}` + GO_AWAY. Sem HELLO em `handshake_timeout` (default 5s) → fecha. REJECT é terminal, sem retry (erro de configuração sobe ao usuário).

**Compressão**: o receptor escolhe o primeiro codec da lista do iniciador que também suporta; sem interseção → `nil`, conexão sem compressão — nunca motivo de REJECT. Flag COMPRESSED indica, por frame DATA, payload comprimido com o codec negociado. Emissor só comprime payload ≥ `compression.min_bytes` e só se encolher.

## 4. Canais, mensagens e interações

Streams são **byte pipes** (semântica yamux pura), usados como canais. Um frame não delimita nada: mensagens têm delimitação própria dentro do stream — podem atravessar frames DATA, e um frame pode carregar várias mensagens pequenas.

```
u32  length          bytes após este campo
u8   msg_type
u64  correlation_id  0 = sem resposta esperada
...  corpo msgpack
```

Canais padrão de uma conexão:

- **stream 1 — controle**: handshake (HELLO/HELLO_ACK/HELLO_REJECT) e, depois, membership (spec 02). Vida = vida da conexão.
- **stream 3 — mensagens**: todo o tráfego de atores e RPC interno. Vida = vida da conexão.
- **lane de replicação**: segundo canal de envelopes, reservado ao tráfego de replicação (spec 05) para que fan-out de estado e chamadas de ator não disputem a mesma fila/janela. Cada lado abre a sua sob demanda via `BULK_OPEN {kind: "casty.replication"}`; após o header o stream carrega envelopes normais, e respostas voltam pelo stream em que o request chegou. Vida = vida da conexão.
- **streams efêmeros — bulk**: um por transferência grande (handoff, snapshot, anti-entropy). Abre com mensagem `BULK_OPEN {kind, meta}` (qualquer `kind` ≠ `casty.replication`), seguem bytes crus (sem envelope por chunk), FIN encerra, RST aborta. Flow control por stream isola o bulk do tráfego pequeno.

Faixas de `msg_type`: 0x00–0x1F conexão (HELLO 0x01, HELLO_ACK 0x02, HELLO_REJECT 0x03, RESPONSE 0x10, ERROR 0x11, BULK_OPEN 0x12), 0x20–0x3F membership (spec 02), 0x40–0x5F atores (spec 04), 0x60–0x7F replicação (spec 05).

Interações (convenções sobre o canal de mensagens):

- **ask**: envia mensagem com `correlation_id` gerado (contador u64 por conexão, por lado; cada lado casa respostas contra os ids que ele gerou). A resposta é **assíncrona e fora de ordem** — chega como RESPONSE (ou ERROR) com o mesmo id, quando chegar. Timeout por id (`request_timeout`, default 10s, overridável por chamada) → descarta o pendente + `CastyTimeoutError`; resposta tardia é descartada silenciosamente.
- **tell**: mensagem com `correlation_id = 0`. Sem estado pendente.
- **bulk**: stream próprio, como acima.

Mensagens no canal de mensagens ≤ `max_message_bytes` (default 4 MiB). O limite existe porque o canal é compartilhado e ordenado: uma mensagem grande segura as de trás (até `max_message_bytes / max_frame_bytes` frames de head-of-line intra-canal). Maior que isso → é bulk, stream próprio.

ERROR: `{code: int, message: str, data: map}`; o transporte converte no tipo de exceção registrado para o code.

- Conexão caiu → asks pendentes falham com `ConnectionLostError`; streams bulk com `StreamResetError`. RST no canal de mensagens = violação de protocolo (canais de vida longa não são resetáveis; fecha a conexão). Retry é decisão da camada de cima, nunca do transporte.

## 5. Pool de conexões

- Uma conexão por par de nós, keyed por `node_id` destino. Corrida de abertura simultânea: ambos aceitam; fecha-se a conexão iniciada pelo `node_id` maior (determinístico, sem negociação).
- Abertura sob demanda; falha → backoff exponencial com jitter (`reconnect_base` 100ms, `reconnect_max` 15s, fator 2, jitter ±25%).
- `max_connections` (default 1024): fecha a LRU sem streams ativos.
- Keepalive: PING a cada `keepalive_interval` (default 15s) em conexão ociosa; sem ACK em `keepalive_timeout` (default 5s) → fecha e notifica interessados (membership usa como sinal de suspeita de vizinho).

## 6. TLS

- `casty.TLS(cert, key, ca=None, require_client_cert=True)` constrói `ssl.SSLContext`; ou o usuário passa um pronto.
- `ca` fornecida → verificação de peer nos dois papéis; `require_client_cert` → mTLS.
- TLS é do cluster inteiro: nó com TLS não aceita plaintext e vice-versa (falha antes do HELLO).

## 7. Serialização e `@casty.message`

### Registry

`@casty.message` sobre classe com fields anotados:

1. Aplica `dataclass(slots=True, eq=True)` se ainda não for dataclass.
2. Registra com **wire name** = `module.QualName` por default, overridável: `@casty.message(name="casty.order.v1")` — renomear a classe mantendo `name` não quebra o wire.
3. Valida recursivamente, em import time, que todo field é serializável; ilegal → `SerializationSchemaError` no import, com o caminho do field.
4. Colisão de wire name → erro no import.

`@casty.actor` reusa a maquinaria: o schema de estado (fields não-transient) é registrado como message com wire name `<actor_wire_name>#state`.

### Tipos suportados

Primitivos msgpack (`int`, `float`, `bool`, `str`, `bytes`, `None`); `list[T]`, `dict[K, V]` (K ∈ str/int/bytes), `set[T]`, `frozenset[T]`, `tuple[...]`; dataclasses registradas aninhadas; `enum.Enum` de valores primitivos; uniões de tipos suportados e `T | None`; `datetime`/`uuid` via ext types. **Nada de pickle, em nenhum caminho.** Fora da lista → erro em import time (schema) ou `SerializationError` no envio.

### Encoding e evolução

Instância → `[wire_name: str, fields: map[str, value]]`. Decode: registry resolve wire_name; campo desconhecido **ignorado** (forward compat); campo ausente assume default — sem default → `SerializationError`. União ambígua resolve pelo wire name embutido (dataclasses) ou tipo msgpack (primitivos). Regras para o usuário: campo novo sempre com default; nunca reusar wire name para schema incompatível; renomear classe exige fixar `name`.

## 8. Compressão (codec plugável)

```python
class Codec(Protocol):
    name: str
    def compress(self, data: bytes) -> bytes: ...
    def decompress(self, data: bytes, max_size: int) -> bytes: ...
```

- `decompress` falha com `ProtocolError` se o resultado excede `max_size` (= `max_message_bytes` para frames de mensagem; `max_frame_bytes` para bulk) — proteção contra bomba de descompressão.
- Built-ins: `zlib` (stdlib, sempre); `lz4`/`zstd` auto-registrados se instalados — extras `casty[lz4]`, `casty[zstd]`. Runtime obrigatório continua só `msgpack`.
- Custom: registrado por nome; os dois lados precisam conhecê-lo para negociar.

```python
@casty.message
class CompressionConfig:
    codecs: list[str] | None = None   # None = auto: ["zstd", "lz4", "zlib"] filtrado pelos instalados; [] = desabilitado
    min_bytes: int = 4096
```

Racional do threshold: mensagens de ator são pequenas e latência-sensíveis; quem comprime são os frames de bulk, que passam do threshold naturalmente.

## 9. Configuração

```python
@casty.message
class TransportConfig:
    max_frame_bytes: int = 256 * 1024        # cap de DATA; granularidade de intercalação
    max_message_bytes: int = 4 * 1024 * 1024 # mensagens não-bulk
    initial_window_bytes: int = 256 * 1024   # flow control por stream
    max_concurrent_streams: int = 1024       # por conexão
    handshake_timeout: float = 5.0
    request_timeout: float = 10.0
    connect_timeout: float = 5.0
    keepalive_interval: float = 15.0
    keepalive_timeout: float = 5.0
    reconnect_base: float = 0.1
    reconnect_max: float = 15.0
    max_connections: int = 1024
    compression: CompressionConfig = CompressionConfig()
```

## 10. Erros

```
CastyError
├── TransportError
│   ├── ConnectionLostError
│   ├── StreamResetError      (RST recebido / stream abortado)
│   ├── HandshakeError        (code/reason do REJECT)
│   └── ProtocolError         (frame inválido, janela violada, bomba de descompressão)
├── CastyTimeoutError
└── SerializationError
    └── SerializationSchemaError   (import time)
```

## 11. Layout do projeto (uv)

```
casty/
├── pyproject.toml            # uv; mypy strict; ruff; pytest + pytest-asyncio
├── src/casty/
│   ├── __init__.py           # API pública: start, connect, actor, message, ...
│   ├── errors.py
│   ├── config.py
│   ├── serde/
│   │   ├── registry.py       # @casty.message, wire names, validação de schema
│   │   └── codec.py          # encode/decode msgpack + ext types
│   └── transport/
│       ├── frame.py          # (de)framing puro, sem I/O — unit-testável isolado
│       ├── mux.py            # streams, janelas, SYN/FIN/RST — sans-IO, unit-testável
│       ├── connection.py     # handshake, interações, keepalive (asyncio)
│       ├── pool.py
│       └── server.py
└── tests/
    ├── unit/                 # frame, mux, codec, registry — sem rede
    └── integration/          # conexões reais em localhost, TLS, falhas
```

`mux.py` sans-IO (bytes/eventos in, bytes/eventos out) é deliberado: todo o flow control e máquina de estados de stream testável sem abrir socket, estilo h2.

## 12. Critérios de verificação da etapa

1. Round-trip ask/tell entre dois processos, plaintext, TLS e mTLS; respostas fora de ordem casadas corretamente (asks concorrentes com respostas invertidas); mensagem atravessando múltiplos frames e múltiplas mensagens num frame decodificadas corretamente.
2. **Intercalação**: bulk de ≥1 GB em andamento; um ask concorrente na mesma conexão responde em < N ms (medido; N ~ poucos ms em localhost). Head-of-line entre streams limitado a 1 frame.
3. **Flow control**: consumidor lento de bulk trava o produtor (janela zerada, medido por memória estável no receptor); WINDOW_UPDATE retoma; outros streams seguem fluindo.
4. HELLO com cluster errado / versão incompatível rejeitados com code correto; plaintext contra nó TLS falha antes do HELLO; stream ≠ 1 antes do handshake → GO_AWAY.
5. Frame com flags reservadas, DATA acima da janela, type inválido → GO_AWAY; RST aborta um stream sem afetar os demais.
6. Queda de conexão: pendentes falham com `ConnectionLostError`/`StreamResetError`; reconexão respeita backoff (medido); corrida de conexão dupla converge para uma.
7. Registry: tipo ilegal falha no import com caminho do field; campo desconhecido ignorado; campo ausente sem default falha; wire name duplicado falha.
8. Compressão: negociação converge (lz4×lz4 → lz4; lz4×sem → zlib; sem interseção → nil e funciona); payload < min_bytes sem comprimir; bomba de descompressão → ProtocolError.
9. mypy strict limpo; API pública sem `Any` visível.
