import asyncio
import os
import time

import httpx
from mcp.server.fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Config from environment
# ---------------------------------------------------------------------------
PROMETHEUS_URL = os.getenv(
    "PROMETHEUS_URL",
    "http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090",
)
LOKI_URL = os.getenv(
    "LOKI_URL",
    "http://loki.monitoring.svc.cluster.local:3100",
)
TEMPO_URL = os.getenv(
    "TEMPO_URL",
    "http://tempo.monitoring.svc.cluster.local:3200",
)
ALERTMANAGER_URL = os.getenv(
    "ALERTMANAGER_URL",
    "http://kube-prometheus-stack-alertmanager.monitoring.svc.cluster.local:9093",
)
MCP_API_KEY = os.getenv("MCP_API_KEY", "")

# ---------------------------------------------------------------------------
# Kubernetes client (in-cluster, falls back to kubeconfig for local dev)
# ---------------------------------------------------------------------------
from kubernetes import client as _k8s_client, config as _k8s_config

try:
    _k8s_config.load_incluster_config()
except _k8s_config.ConfigException:
    _k8s_config.load_kube_config()

_k8s_v1 = _k8s_client.CoreV1Api()
_k8s_apps = _k8s_client.AppsV1Api()


def _parse_since_to_ns(since: str) -> int:
    """Convert duration string like '1h', '30m', '2d' to nanoseconds offset from now."""
    multipliers = {"s": int(1e9), "m": int(60e9), "h": int(3600e9), "d": int(86400e9)}
    unit = since[-1].lower()
    val = float(since[:-1])
    return int(time.time() * 1e9) - int(val * multipliers.get(unit, int(3600e9)))


def _parse_since_to_s(since: str) -> int:
    """Convert duration string to Unix timestamp seconds."""
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    unit = since[-1].lower()
    val = float(since[:-1])
    return int(time.time()) - int(val * multipliers.get(unit, 3600))


# ---------------------------------------------------------------------------
# MCP server
# ---------------------------------------------------------------------------
mcp = FastMCP("blockmaze-observability", stateless_http=True, host="0.0.0.0")


# ── Prometheus ──────────────────────────────────────────────────────────────

@mcp.tool()
async def prometheus_query(promql: str, timestamp: str = "") -> dict:
    """Execute an instant PromQL query.

    Args:
        promql: PromQL expression, e.g. 'up', 'rate(http_requests_total[5m])'
        timestamp: optional RFC3339 or Unix timestamp; defaults to now
    """
    params: dict = {"query": promql}
    if timestamp:
        params["time"] = timestamp
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{PROMETHEUS_URL}/api/v1/query", params=params, timeout=30)
        return r.json()


@mcp.tool()
async def prometheus_query_range(
    promql: str, start: str, end: str, step: str = "60s"
) -> dict:
    """Execute a range PromQL query.

    Args:
        promql: PromQL expression
        start: start time — RFC3339 or Unix timestamp, e.g. '2024-01-01T00:00:00Z' or relative like 'now-1h' won't work; use Unix epoch
        end: end time — same format as start
        step: resolution step, e.g. '60s', '5m', '1h'
    """
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{PROMETHEUS_URL}/api/v1/query_range",
            params={"query": promql, "start": start, "end": end, "step": step},
            timeout=30,
        )
        return r.json()


@mcp.tool()
async def prometheus_list_metrics(match: str = "") -> list:
    """List all available Prometheus metric names.

    Args:
        match: optional PromQL metric selector to filter, e.g. '{job="kubelet"}'
    """
    params: dict = {}
    if match:
        params["match[]"] = match
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{PROMETHEUS_URL}/api/v1/label/__name__/values", params=params, timeout=30
        )
        return r.json().get("data", [])


@mcp.tool()
async def prometheus_get_targets() -> dict:
    """Get all Prometheus scrape targets and their health/last-scrape status."""
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{PROMETHEUS_URL}/api/v1/targets", timeout=30)
        return r.json()


@mcp.tool()
async def prometheus_get_rules() -> dict:
    """Get all Prometheus alerting and recording rules and their evaluation state."""
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{PROMETHEUS_URL}/api/v1/rules", timeout=30)
        return r.json()


# ── Loki ────────────────────────────────────────────────────────────────────

@mcp.tool()
async def loki_query_logs(
    logql: str,
    limit: int = 100,
    since: str = "1h",
    direction: str = "backward",
) -> dict:
    """Query Loki logs using LogQL.

    Args:
        logql: LogQL stream selector + optional pipeline, e.g. '{namespace="services",app="evm-api"} |= "error"'
        limit: max number of log lines to return (default 100)
        since: how far back to look, e.g. '30m', '2h', '1d' (default '1h')
        direction: 'backward' returns newest-first (default), 'forward' returns oldest-first
    """
    start_ns = _parse_since_to_ns(since)
    end_ns = int(time.time() * 1e9)
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{LOKI_URL}/loki/api/v1/query_range",
            params={
                "query": logql,
                "limit": limit,
                "start": start_ns,
                "end": end_ns,
                "direction": direction,
            },
            timeout=30,
        )
        return r.json()


@mcp.tool()
async def loki_list_labels() -> list:
    """List all available Loki label names (e.g. 'namespace', 'app', 'container')."""
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{LOKI_URL}/loki/api/v1/labels", timeout=15)
        return r.json().get("data", [])


@mcp.tool()
async def loki_list_label_values(label: str) -> list:
    """List all values for a specific Loki label.

    Args:
        label: label name — use loki_list_labels first to discover names.
            Common values: 'k8s_namespace_name', 'k8s_pod_name',
            'k8s_container_name', 'k8s_deployment_name', 'service_name'
    """
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{LOKI_URL}/loki/api/v1/label/{label}/values", timeout=15
        )
        return r.json().get("data", [])


# ── Tempo ────────────────────────────────────────────────────────────────────

@mcp.tool()
async def tempo_search_traces(
    service: str = "",
    operation: str = "",
    tags: str = "",
    min_duration: str = "",
    max_duration: str = "",
    limit: int = 20,
    since: str = "1h",
) -> dict:
    """Search distributed traces in Tempo.

    Args:
        service: service name filter, e.g. 'evm-api'
        operation: span operation/name filter, e.g. 'POST /api/v1/tx'
        tags: space-separated key=value tag filters, e.g. 'http.status_code=500 error=true'
        min_duration: min trace duration, e.g. '100ms', '1s'
        max_duration: max trace duration, e.g. '5s'
        limit: max results (default 20)
        since: how far back to search, e.g. '1h', '6h', '1d' (default '1h')
    """
    start = _parse_since_to_s(since)
    end = int(time.time())
    params: dict = {"start": start, "end": end, "limit": limit}
    if service:
        params["service.name"] = service
    if operation:
        params["name"] = operation
    if min_duration:
        params["minDuration"] = min_duration
    if max_duration:
        params["maxDuration"] = max_duration
    if tags:
        for tag in tags.split():
            if "=" in tag:
                k, v = tag.split("=", 1)
                params[k] = v
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{TEMPO_URL}/api/search", params=params, timeout=30)
        return r.json()


@mcp.tool()
async def tempo_get_trace(trace_id: str) -> dict:
    """Fetch a complete trace tree by trace ID from Tempo.

    Args:
        trace_id: hex trace ID, e.g. '1234abcd5678ef90'
    """
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{TEMPO_URL}/api/traces/{trace_id}", timeout=30)
        return r.json()


@mcp.tool()
async def tempo_list_tags() -> dict:
    """List all searchable tag names available in Tempo (span attributes)."""
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{TEMPO_URL}/api/search/tags", timeout=15)
        return r.json()


@mcp.tool()
async def tempo_list_tag_values(tag: str) -> dict:
    """List all values for a searchable Tempo tag.

    Args:
        tag: tag name, e.g. 'service.name', 'http.status_code', 'span.kind'
    """
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{TEMPO_URL}/api/search/tag/{tag}/values", timeout=15)
        return r.json()


# ── AlertManager ─────────────────────────────────────────────────────────────

@mcp.tool()
async def alertmanager_get_alerts(
    active: bool = True,
    silenced: bool = False,
    inhibited: bool = False,
    filter: str = "",
) -> list:
    """Get current alerts from AlertManager.

    Args:
        active: include active alerts (default True)
        silenced: include silenced alerts (default False)
        inhibited: include inhibited alerts (default False)
        filter: label matcher string, e.g. 'severity=critical' or 'alertname=HighMemory'
    """
    params: dict = {
        "active": str(active).lower(),
        "silenced": str(silenced).lower(),
        "inhibited": str(inhibited).lower(),
    }
    if filter:
        params["filter"] = filter
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{ALERTMANAGER_URL}/api/v2/alerts", params=params, timeout=15)
        return r.json()


@mcp.tool()
async def alertmanager_get_silences() -> list:
    """Get all silences currently configured in AlertManager."""
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{ALERTMANAGER_URL}/api/v2/silences", timeout=15)
        return r.json()


# ── Kubernetes ───────────────────────────────────────────────────────────────

@mcp.tool()
async def k8s_get_pods(namespace: str = "", label_selector: str = "") -> list:
    """List Kubernetes pods with their status.

    Args:
        namespace: namespace to query; empty string means all namespaces
        label_selector: optional label selector, e.g. 'app=evm-api'
    """
    kwargs: dict = {}
    if label_selector:
        kwargs["label_selector"] = label_selector

    if namespace:
        pods = await asyncio.to_thread(
            _k8s_v1.list_namespaced_pod, namespace, **kwargs
        )
    else:
        pods = await asyncio.to_thread(
            _k8s_v1.list_pod_for_all_namespaces, **kwargs
        )

    result = []
    for pod in pods.items:
        container_statuses = pod.status.container_statuses or []
        result.append(
            {
                "name": pod.metadata.name,
                "namespace": pod.metadata.namespace,
                "phase": pod.status.phase,
                "ready": all(cs.ready for cs in container_statuses),
                "restarts": sum(cs.restart_count for cs in container_statuses),
                "node": pod.spec.node_name,
                "created": str(pod.metadata.creation_timestamp),
            }
        )
    return result


@mcp.tool()
async def k8s_get_deployments(namespace: str = "") -> list:
    """List Kubernetes Deployments with replica counts.

    Args:
        namespace: namespace to query; empty string means all namespaces
    """
    if namespace:
        deploys = await asyncio.to_thread(
            _k8s_apps.list_namespaced_deployment, namespace
        )
    else:
        deploys = await asyncio.to_thread(
            _k8s_apps.list_deployment_for_all_namespaces
        )

    result = []
    for d in deploys.items:
        image = ""
        if d.spec.template.spec.containers:
            image = d.spec.template.spec.containers[0].image
        result.append(
            {
                "name": d.metadata.name,
                "namespace": d.metadata.namespace,
                "desired": d.spec.replicas,
                "ready": d.status.ready_replicas or 0,
                "available": d.status.available_replicas or 0,
                "image": image,
            }
        )
    return result


@mcp.tool()
async def k8s_get_events(
    namespace: str = "", warning_only: bool = True, field_selector: str = ""
) -> list:
    """Get Kubernetes events, useful for spotting crashloops, OOMs, scheduling failures.

    Args:
        namespace: namespace to query; empty string means all namespaces
        warning_only: if True (default) only return Warning-type events
        field_selector: optional extra field selector
    """
    selectors = []
    if warning_only:
        selectors.append("type=Warning")
    if field_selector:
        selectors.append(field_selector)
    kwargs: dict = {}
    if selectors:
        kwargs["field_selector"] = ",".join(selectors)

    if namespace:
        events = await asyncio.to_thread(
            _k8s_v1.list_namespaced_event, namespace, **kwargs
        )
    else:
        events = await asyncio.to_thread(
            _k8s_v1.list_event_for_all_namespaces, **kwargs
        )

    def _ts(e):
        return e.last_timestamp or e.event_time or e.metadata.creation_timestamp

    result = []
    for e in sorted(events.items, key=_ts, reverse=True)[:50]:
        result.append(
            {
                "namespace": e.metadata.namespace,
                "reason": e.reason,
                "message": e.message,
                "type": e.type,
                "count": e.count,
                "object": f"{e.involved_object.kind}/{e.involved_object.name}",
                "last_seen": str(_ts(e)),
            }
        )
    return result


@mcp.tool()
async def k8s_describe_pod(name: str, namespace: str) -> dict:
    """Get detailed info about a specific pod: conditions, container states, and last 100 log lines.

    Args:
        name: pod name
        namespace: pod namespace
    """
    pod = await asyncio.to_thread(_k8s_v1.read_namespaced_pod, name, namespace)

    containers = []
    for cs in pod.status.container_statuses or []:
        state_str = ""
        if cs.state.running:
            state_str = "running"
        elif cs.state.waiting:
            state_str = f"waiting:{cs.state.waiting.reason}"
        elif cs.state.terminated:
            state_str = f"terminated:{cs.state.terminated.reason}(exit {cs.state.terminated.exit_code})"
        containers.append(
            {
                "name": cs.name,
                "ready": cs.ready,
                "restarts": cs.restart_count,
                "state": state_str,
                "image": cs.image,
            }
        )

    logs = ""
    try:
        logs = await asyncio.to_thread(
            _k8s_v1.read_namespaced_pod_log, name, namespace, tail_lines=100
        )
    except Exception:
        pass

    return {
        "name": pod.metadata.name,
        "namespace": pod.metadata.namespace,
        "node": pod.spec.node_name,
        "phase": pod.status.phase,
        "conditions": [
            {"type": c.type, "status": c.status, "reason": c.reason}
            for c in (pod.status.conditions or [])
        ],
        "containers": containers,
        "recent_logs": logs[-3000:] if logs else "",
    }


@mcp.tool()
async def k8s_get_nodes() -> list:
    """Get Kubernetes node status, instance type, zone, and capacity."""
    nodes = await asyncio.to_thread(_k8s_v1.list_node)
    result = []
    for n in nodes.items:
        labels = n.metadata.labels or {}
        conditions = {c.type: c.status for c in (n.status.conditions or [])}
        result.append(
            {
                "name": n.metadata.name,
                "ready": conditions.get("Ready") == "True",
                "instance_type": labels.get("node.kubernetes.io/instance-type", ""),
                "zone": labels.get("topology.kubernetes.io/zone", ""),
                "kubelet_version": n.status.node_info.kubelet_version,
                "cpu_capacity": n.status.capacity.get("cpu"),
                "memory_capacity": n.status.capacity.get("memory"),
            }
        )
    return result


# ---------------------------------------------------------------------------
# ASGI middleware — handles /health inline, enforces API key, passes
# lifespan events straight through so MCP's task group initialises correctly
# ---------------------------------------------------------------------------
_HEALTH_BODY = b'{"status":"ok","service":"blockmaze-mcp-observability"}'
_HEALTH_HEADERS = [
    (b"content-type", b"application/json"),
    (b"content-length", str(len(_HEALTH_BODY)).encode()),
]


class _Middleware:
    def __init__(self, inner):
        self.inner = inner

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            # lifespan and websocket pass through untouched
            await self.inner(scope, receive, send)
            return

        path = scope.get("path", "")

        if path == "/health":
            await send({"type": "http.response.start", "status": 200, "headers": _HEALTH_HEADERS})
            await send({"type": "http.response.body", "body": _HEALTH_BODY})
            return

        if MCP_API_KEY:
            headers = dict(scope.get("headers", []))
            auth = headers.get(b"authorization", b"").decode("utf-8", errors="ignore")
            if not auth.startswith("Bearer ") or auth[7:] != MCP_API_KEY:
                body = b"Unauthorized"
                await send({"type": "http.response.start", "status": 401,
                            "headers": [(b"content-length", str(len(body)).encode())]})
                await send({"type": "http.response.body", "body": body})
                return

        await self.inner(scope, receive, send)


# ---------------------------------------------------------------------------
# ASGI app
# ---------------------------------------------------------------------------
app = _Middleware(mcp.streamable_http_app())

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=8000,
        log_level="info",
    )
