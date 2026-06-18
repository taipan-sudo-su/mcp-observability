# MCP Observability Server

An [MCP (Model Context Protocol)](https://modelcontextprotocol.io) server that gives AI assistants read access to your full observability stack — Prometheus, Loki, Tempo, AlertManager, and Kubernetes — via structured, authenticated tool calls.

Designed to run in-cluster on AWS EKS and integrate with Loki for access logging.

---

## Features

- **Prometheus** — instant and range PromQL queries, metric discovery, scrape targets, alerting rules
- **Loki** — LogQL log queries, label and value discovery
- **Tempo** — distributed trace search, full trace retrieval, tag discovery
- **AlertManager** — active alerts, silences
- **Kubernetes** — pods, deployments, events, nodes, per-pod describe + logs
- **Multi-user auth** — per-user API keys via environment variables
- **RBAC** — per-user roles (`admin` / `readonly`) controlling which tools a user may call
- **Rate limiting** — per-user sliding-window request cap configurable via env var
- **Structured access logging** — JSON on stdout, ingested automatically by Loki in-cluster
- **Health & readiness endpoints** — `/health` and `/ready` with per-backend probes
- **`.env` file support** — environment variables can be loaded from a local `.env` file

---

## Prerequisites

### Runtime

| Requirement | Minimum version | Notes |
|---|---|---|
| Python | 3.10 | Uses `X \| Y` union syntax throughout |
| pip | any recent | For installing dependencies |
| Docker | any recent | Only needed for containerised deployment |

### Kubernetes access

The server loads Kubernetes credentials at startup — **one of these must be present**:

- **In-cluster** (production): the pod's ServiceAccount is used automatically.
- **Local dev**: a valid kubeconfig at `~/.kube/config` (e.g. from `aws eks update-kubeconfig --name <cluster>`).

If neither is available the server will crash on startup with a `ConfigException`.

### Observability stack

This server is a proxy — it has no storage of its own. **All five backends must be deployed and reachable** before this MCP server is useful:

| Service | What it powers | Typical install |
|---|---|---|
| [Prometheus](https://prometheus.io) | All `prometheus_*` tools + rightsizing | `kube-prometheus-stack` Helm chart |
| [Loki](https://grafana.com/oss/loki/) | All `loki_*` tools | `grafana/loki` Helm chart |
| [Tempo](https://grafana.com/oss/tempo/) | All `tempo_*` tools | `grafana/tempo` Helm chart |
| [AlertManager](https://prometheus.io/docs/alerting/latest/alertmanager/) | All `alertmanager_*` tools | Bundled with `kube-prometheus-stack` |
| [Grafana](https://grafana.com/oss/grafana/) | `grafana_*` annotation tools | Bundled with `kube-prometheus-stack` |

### Telemetry data

The backends are only useful if your applications are sending data to them. Make sure the following are in place:

| Signal | Backend | How to send |
|---|---|---|
| **Metrics** | Prometheus | Instrument apps with a Prometheus client library (e.g. `prometheus_client` for Python, `prom-client` for Node) or expose a `/metrics` endpoint that the Prometheus scraper can reach |
| **Logs** | Loki | Ship container stdout/stderr via Promtail, the Grafana Agent, or the Loki Docker driver; or push logs directly from apps via the Loki HTTP API |
| **Traces** | Tempo | Instrument apps with OpenTelemetry and configure the OTLP exporter to point at the Tempo endpoint (gRPC `4317` or HTTP `4318`) |

Without telemetry flowing in, all tool calls will return empty results.

For **local development**, port-forward each service from an existing cluster:

```bash
kubectl port-forward -n monitoring svc/kube-prometheus-stack-prometheus 9090:9090
kubectl port-forward -n monitoring svc/loki 3100:3100
kubectl port-forward -n monitoring svc/tempo 3200:3200
kubectl port-forward -n monitoring svc/kube-prometheus-stack-alertmanager 9093:9093
kubectl port-forward -n monitoring svc/grafana 3000:3000
```

Then set the corresponding `*_URL` env vars (see [Configuration](#configuration)) to point at `localhost`.

---

## Quick Start

### Run locally

```bash
pip install -r requirements.txt
python server.py
```

Requires a valid kubeconfig at `~/.kube/config` (or in-cluster credentials). The server listens on `http://0.0.0.0:8000`.

### Run with Docker

```bash
docker build -t mcp-observability .

docker run -p 8000:8000 \
  -e MCP_API_KEYS=alice:key-abc,bob:key-xyz \
  -e PROMETHEUS_URL=http://your-prometheus:9090 \
  -e LOKI_URL=http://your-loki:3100 \
  -e TEMPO_URL=http://your-tempo:3200 \
  -e ALERTMANAGER_URL=http://your-alertmanager:9093 \
  mcp-observability
```

---

## Configuration

All configuration is via environment variables.

### API Keys

| Variable | Format | Description |
|---|---|---|
| `MCP_API_KEYS` | `user1:key1,user2:key2` | Preferred — one entry per user |
| `MCP_API_KEY` | `somekey` | Legacy single key; username becomes `"default"` |

Both can coexist. If neither is set, all requests are unauthenticated.

### RBAC

| Variable | Format | Description |
|---|---|---|
| `MCP_ROLES` | `user1:admin,user2:readonly` | Per-user role assignment |

Built-in roles:

| Role | Access |
|---|---|
| `admin` | All tools (default when `MCP_ROLES` is unset or the user has no entry) |
| `readonly` | All read/query tools — excludes `grafana_create_annotation` |

### Rate Limiting

| Variable | Default | Description |
|---|---|---|
| `MCP_RATE_LIMIT` | `0` (unlimited) | Max requests per minute per user (sliding window); `0` disables limiting |

Requests exceeding the limit receive `429 Too Many Requests` with a `Retry-After: 60` header.

### `.env` file

Environment variables can be placed in a `.env` file at the project root. The server loads it automatically on startup via `python-dotenv`. See [`example.env`](example.env) for a template.

### Backend URLs

| Variable | Default (in-cluster) | Description |
|---|---|---|
| `PROMETHEUS_URL` | `http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090` | Prometheus |
| `LOKI_URL` | `http://loki.monitoring.svc.cluster.local:3100` | Loki |
| `TEMPO_URL` | `http://tempo.monitoring.svc.cluster.local:3200` | Tempo |
| `ALERTMANAGER_URL` | `http://kube-prometheus-stack-alertmanager.monitoring.svc.cluster.local:9093` | AlertManager |
| `GRAFANA_URL` | `http://grafana.monitoring.svc.cluster.local:3000` | Grafana |
| `GRAFANA_TOKEN` | _(empty)_ | Grafana service account token for annotation API calls |

---

## MCP Tools

### Prometheus
| Tool | Description |
|---|---|
| `prometheus_query` | Instant PromQL query |
| `prometheus_query_range` | Range PromQL query |
| `prometheus_list_metrics` | List all metric names |
| `prometheus_get_targets` | Scrape target health |
| `prometheus_get_rules` | Alerting and recording rules |
| `prometheus_compare_metric` | Compare current metric value against the same window in the past (e.g. same time yesterday) |

### Loki
| Tool | Description |
|---|---|
| `loki_query_logs` | Query logs with LogQL |
| `loki_list_labels` | List all label names |
| `loki_list_label_values` | List values for a label |
| `loki_get_log_patterns` | Cluster log lines into recurring patterns by frequency (Loki ≥ 3.0) |

### Tempo
| Tool | Description |
|---|---|
| `tempo_search_traces` | Search traces by service, tags, duration |
| `tempo_get_trace` | Fetch a full trace by ID |
| `tempo_list_tags` | List searchable tag names |
| `tempo_list_tag_values` | List values for a tag |

### Grafana
| Tool | Description |
|---|---|
| `grafana_list_annotations` | List annotations — correlate deployments/incidents with metric spikes |
| `grafana_create_annotation` | Create an annotation to mark an event on dashboards |

### AlertManager
| Tool | Description |
|---|---|
| `alertmanager_get_alerts` | Get active/silenced/inhibited alerts |
| `alertmanager_get_silences` | List all silences |

### Kubernetes
| Tool | Description |
|---|---|
| `k8s_get_pods` | List pods with status |
| `k8s_get_deployments` | List deployments with replica counts |
| `k8s_get_events` | Cluster events (warnings by default) |
| `k8s_get_nodes` | Node status, instance type, zone, capacity |
| `k8s_describe_pod` | Pod conditions, container states, last 100 log lines |
| `k8s_get_pod_logs` | Fetch logs from all pods matching a label selector |
| `k8s_resource_rightsizing` | Severity-ranked CPU/memory rightsizing recommendations backed by Prometheus usage data |

### Incident Response
| Tool | Description |
|---|---|
| `incident_summary` | Unified incident report for a service — correlates active alerts, error logs, slow/error traces, Kubernetes warning events, and recent deployments in a single parallel call; returns a severity-ranked (`critical` / `warning` / `ok`) snapshot. This is the recommended first tool to call when investigating an incident. |

---

## Endpoints

| Path | Description |
|---|---|
| `POST /mcp` | MCP streamable-HTTP endpoint (requires Bearer token) |
| `GET /health` | Liveness probe — always returns `200 {"status":"ok"}` |
| `GET /ready` | Readiness probe — probes all backends, returns `503` if any are down |

---

## Access Logs

Every authenticated request emits a structured JSON line to stdout:

```json
{"ts": 1716900000.123, "user": "alice", "tool": "prometheus_query", "path": "/mcp", "status": 200}
```

When deployed in-cluster, Loki ingests these automatically. To query per-user tool usage:

```logql
{service_name="blockmaze-mcp"} | json | user="alice"
```

---

## Architecture

```
HTTP Request
     │
     ▼
_Middleware (ASGI)
     ├── /health  → inline response
     ├── /ready   → probe all backends concurrently
     └── /mcp
          ├── Auth:         Bearer token → username  (401 if invalid)
          ├── Rate limit:   sliding-window per user   (429 if exceeded)
          ├── Buffer body → extract tool name
          ├── RBAC:         role check for tool name  (403 if denied)
          ├── Forward to FastMCP app
          └── Log + record metrics: {ts, user, tool, path, status, duration}
                         │
              ┌──────────┼──────────────┐
              ▼          ▼              ▼
         Prometheus    Loki/Tempo   Kubernetes
         AlertManager  Grafana      (python client)
         (httpx shared connection pool)
```

The entire server lives in [`server.py`](server.py). There are no other source files.

**Stack:** [FastMCP](https://github.com/jlowin/fastmcp) · [Uvicorn](https://www.uvicorn.org/) · [httpx](https://www.python-httpx.org/) · [kubernetes-client](https://github.com/kubernetes-client/python) · [python-dotenv](https://github.com/theskumar/python-dotenv)

---

## Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
python -m pytest test_server.py -v

# Syntax check
python -m py_compile server.py
```

---

## Deployment

The server is deployed as a pod inside AWS EKS. All Kubernetes changes are managed via GitOps — commit to git and ArgoCD will reconcile. Do not apply manifests directly to the cluster.

---

## License

MIT
