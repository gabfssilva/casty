# Hello world

Demonstra a criação de um ator com estado inicial, o envio de perguntas com `ask` e a persistência do estado com `ctx.state.set`. Cada chave mantém seu próprio contador: duas mensagens para `ana` incrementam o mesmo contador, enquanto `bia` começa em um.

Com `uv`, Python 3.13 ou superior e uma toolchain Rust, execute a partir da raiz do repositório. O `uv` compila e instala a versão local do casty.

```sh
cd examples/00-hello-world
uv run main.py
```
