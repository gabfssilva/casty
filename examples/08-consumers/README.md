# Consumidores

Executa um consumidor por partição em um cluster de três nós, usando um gerador assíncrono para simular os registros de uma fonte externa. Cada consumidor salva seu offset após processar um registro; o programa derruba o nó mais ocupado e verifica que os consumidores retomam nos sobreviventes sem intervenção do chamador, podendo repetir registros processados antes de salvar o offset.

Precisa das portas TCP locais `7431` a `7433` livres; não precisa de Kafka nem de outro broker.

```sh
cd examples/08-consumers
uv run main.py
```
