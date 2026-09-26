# The everyday commands of casty. `make check` runs what CI checks, on the Python of `.venv`.

PY_PATHS := src tests reliability benchmarks examples docs
FT_ENV := .venv-3.14t
LINUX_TARGETS := x86_64 aarch64
STACK ?= local
STORES_COMPOSE := tests/stores/compose.yaml
# The databases of $(STORES_COMPOSE), as `casty.stores.SQL` names them.
STORES := postgres://casty:casty@127.0.0.1:55432/casty mysql://casty:casty@127.0.0.1:53306/casty \
	mysql://casty:casty@127.0.0.1:53307/casty postgres://root@127.0.0.1:56257/casty?sslmode=disable
.DEFAULT_GOAL := help
.PHONY: help sync build test test-ft test-rust test-stores lint fmt typecheck docs check chaos performance bench \
	wheels-linux

help: ## List the targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-z-]+:.*## / {printf "  %-13s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

sync: ## Install the dev tools and build the extension
	uv sync

build: ## Rebuild the extension: uv does not rebuild it after a Rust change
	uv sync --reinstall-package casty

test: build ## Python suite
	uv run pytest -q

test-ft: ## Python suite on free-threaded 3.14t, in its own environment
	UV_PROJECT_ENVIRONMENT=$(FT_ENV) uv sync --python 3.14t --reinstall-package casty
	UV_PROJECT_ENVIRONMENT=$(FT_ENV) uv run --python 3.14t pytest -q

test-rust: ## Rust suite
	cargo test --all-targets

# The databases are removed however the tests end: an interruption exits the shell, and the exit removes them.
test-stores: build ## The store tests on SQLite and on PostgreSQL, MySQL, MariaDB and CockroachDB in containers
	trap 'docker compose --file $(STORES_COMPOSE) down --volumes' EXIT; trap 'exit 130' INT TERM; \
	docker compose --file $(STORES_COMPOSE) up --detach --wait && \
	CASTY_STORES="$(STORES)" cargo test -p casty-store && \
	CASTY_STORES="$(STORES)" uv run pytest -q tests/test_storage.py

lint: ## ruff, rustfmt and clippy, with warnings as errors as in CI
	uv run ruff check $(PY_PATHS)
	uv run ruff format --check $(PY_PATHS)
	cargo fmt --all --check
	cargo clippy --all-targets -- -D warnings

fmt: ## Format Python and Rust
	uv run ruff format $(PY_PATHS)
	cargo fmt --all

typecheck: ## pyright, strict
	uv run pyright

docs: ## Build the reference and check every public name has its page
	uv run mkdocs build --strict
	uv run python docs/check_reference.py

check: lint typecheck test-rust test docs ## Everything CI checks, but 3.14t (make test-ft)

# Both bring up the Kubernetes of the Pulumi stack STACK (local: a kind cluster) and destroy it when the run ends.
chaos: ## Chaos run on Kubernetes, shaped by CHAOS_NODES, CHAOS_MINUTES and CHAOS_SEED
	KUBE_STACK=$(STACK) uv run python -m reliability chaos

performance: ## Throughput and latency on Kubernetes, shaped by PERFORMANCE_SCENARIOS and the rest
	KUBE_STACK=$(STACK) uv run python -m reliability performance

bench: build ## Micro benchmarks, into benchmarks/results/micro.json
	uv run python -m benchmarks.micro --output benchmarks/results/micro.json

wheels-linux: ## manylinux wheels for x86_64 and aarch64 into dist/, which skyward ships to its nodes
	for target in $(LINUX_TARGETS); do \
		uvx maturin build --release --zig --target $$target-unknown-linux-gnu --compatibility manylinux2014 --out dist || exit 1; \
	done
