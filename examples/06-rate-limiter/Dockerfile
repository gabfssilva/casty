FROM python:3.12-slim

WORKDIR /app

# Install uv
RUN pip install uv

# Copy project files (context is root of casty project)
COPY pyproject.toml .
COPY casty/ casty/
COPY examples/06-rate-limiter/ .

# Install dependencies
RUN uv sync

EXPOSE 9000-9999

CMD ["uv", "run", "python", "cluster_node.py"]
