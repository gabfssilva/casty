# Cluster com Docker

Executa um cluster em contêineres com três nós de seed e trinta nós adicionais, além de um cliente que incrementa contadores de quinhentas páginas. A cada rodada, o cliente imprime quantas páginas responderam, como estão distribuídas e a soma dos contadores; é possível alterar a quantidade de nós durante a execução para observar a redistribuição dos atores.

Requer Docker em execução e Docker Compose. A imagem compila a wheel do casty em um estágio de build e instala só ela, então não é preciso ter Rust na máquina. A partir da raiz do repositório, inicie o cluster e execute o cliente:

```sh
cd examples/09-docker-cluster
docker compose up -d --build
docker compose run --rm client
```

O cliente executa cinco rodadas por padrão. Para observar a redistribuição, execute mais rodadas:

```sh
docker compose run --rm -e ROUNDS=30 client
```

Enquanto o cliente roda, em outro terminal no mesmo diretório:

```sh
docker compose up -d --scale node=45
```

Ao terminar, encerre o cluster no diretório do exemplo:

```sh
docker compose down
```
