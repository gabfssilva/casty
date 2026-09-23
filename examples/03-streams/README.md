# Streams

Demonstra `ctx.merge` combinando mensagens da mailbox com eventos de um gerador assíncrono no mesmo loop do ator. Um medidor recebe leituras, fecha uma janela a cada meio segundo e salva sua média; ao final, o programa imprime as médias das janelas que receberam dados.

```sh
cd examples/03-streams
uv run main.py
```
