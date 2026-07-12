FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends iproute2 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .[uvloop] pytest pytest-asyncio docker
COPY tests ./tests

CMD ["python", "-m", "tests.chaos.node_main"]
