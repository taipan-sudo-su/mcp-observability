# Contributing

## Prerequisites

- Python 3.12+
- Docker + Docker Compose (for the local dev stack)
- A kubeconfig pointing at a cluster (optional — K8s tools will fail gracefully without one)

## Local setup

```bash
# Install dependencies
pip install -r requirements.txt

# Copy example env and fill in values
cp example.env .env

# Run the server
python server.py
```

## Running the local dev stack (no cluster required)

```bash
docker compose up
```

This starts the MCP server plus Prometheus, Loki, Tempo, AlertManager, and Grafana on localhost. Kubernetes tools won't work without a live cluster but all other tools will be functional.

## Running tests

```bash
python -m pytest test_server.py -v
```

Tests are pure unit tests — they stub out the kubernetes client and mock httpx so no live backends are required.

## Adding a new tool

1. Add an `async def` decorated with `@mcp.tool()` in [server.py](server.py) in the appropriate backend section.
2. Write a docstring — the first line becomes the tool description Claude reads, so make it precise.
3. Add a test in [test_server.py](test_server.py) that mocks the HTTP response.
4. If the tool writes data (not just reads), add it to the `readonly` role pattern in `_ROLE_PATTERNS` only if it's truly read-only; otherwise leave it out so `readonly` users cannot call it.

## Adding a new backend

1. Add a `FOO_URL` env var at the top of [server.py](server.py) with an in-cluster default.
2. Add it to `_check_ready()` so `/ready` probes it.
3. Add it to `docker-compose.yml` and `dev/` with a minimal config.
4. Add it to `helm/values.yaml` under `backends`.

## Code style

Run `ruff check server.py` before opening a PR. The CI will enforce it.

## Pull requests

- One logical change per PR.
- Include a test for any new tool or middleware behaviour.
- Update `example.env` if you add a new env var.
