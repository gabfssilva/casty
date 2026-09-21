# Falhas

Mostra dois erros observáveis pelo chamador: `ActorFailed` após uma divisão por zero e `MailboxFull` quando uma mailbox limitada fica cheia. O programa também verifica que o ator reiniciado recupera o último estado salvo e que, num tipo sem estado inicial padrão, só o `initial` do primeiro `ref` de uma chave vale.

Com `uv`, Python 3.13 ou superior e uma toolchain Rust, execute a partir da raiz do repositório. O `uv` compila e instala a versão local do casty.

```sh
cd examples/04-failures
uv run main.py
```
