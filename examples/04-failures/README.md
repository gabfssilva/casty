# Falhas

Mostra dois erros observáveis pelo chamador: `ActorFailed` após uma divisão por zero e `MailboxFull` quando uma mailbox limitada fica cheia. O programa também verifica que o ator reiniciado recupera o último estado salvo e que, num tipo sem estado inicial padrão, só o `initial` do primeiro `ref` de uma chave vale.

```sh
cd examples/04-failures
uv run main.py
```
