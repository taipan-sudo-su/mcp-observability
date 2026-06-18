
# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the server

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally (requires kubeconfig or in-cluster; will fall back to ~/.kube/config)
python server.py
```

The server listens on `http://0.0.0.0:8000`.

## Building and running with Docker

```bash
docker build -t mcp-observability .
docker run -p 8000:8000 \
  -e MCP_API_KEYS=alice:key-abc,bob:key-xyz \
  -e PROMETHEUS_URL=http://... \
  mcp-observability
```

## Architecture

The entire server lives in `server.py`. There are no other source files.

**Stack:** FastMCP (MCP protocol) + Uvicorn (ASGI server). The MCP app is wrapped in a hand-rolled ASGI middleware class (`_Middleware`) that handles auth and logging before delegating to the MCP streamable-HTTP app.

**Request flow:**
1. `_Middleware.__call__` receives every HTTP request.
2. `/health` is answered inline without touching MCP.
3. Bearer token is looked up in `API_KEY_MAP` (`{key: username}`) — 401 if missing.
4. Request body is buffered (`_buffer_body`) so the MCP JSON-RPC payload can be inspected to extract the tool name (`_extract_tool`), then replayed to the inner app unchanged.
5. Response status is captured via a wrapped `send` callable.
6. After the request completes, `_log_access` emits a structured JSON line to stdout: `{"ts", "user", "tool", "path", "status"}`.

**MCP tools** are plain `async def` functions decorated with `@mcp.tool()`. They call Prometheus, Loki, Tempo, AlertManager, and Kubernetes via `httpx` (HTTP backends) and the `kubernetes` Python client (K8s). All five backends have their URLs configurable via env vars with in-cluster defaults.

## Auth / API keys

| Env var | Format | Notes |
|---|---|---|
| `MCP_API_KEYS` | `user1:key1,user2:key2` | Preferred — one entry per user |
| `MCP_API_KEY` | `somekey` | Legacy single-key; username becomes `"default"` |

Both can coexist. If neither is set, all requests pass unauthenticated.

## Observability of the server itself

Access logs are JSON on stdout — Loki ingests them automatically in-cluster. To query per-user tool usage:

```logql
{service_name="blockmaze-mcp"} | json | user="alice"
```
