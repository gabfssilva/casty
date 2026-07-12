FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends procps \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .
COPY examples/sentinel/node.py ./node.py

CMD ["python", "node.py"]
