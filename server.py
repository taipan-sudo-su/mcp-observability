import asyncio
import collections
import hmac
import json
import logging
import os
import re
import sys
import time
from urllib.parse import quote

import httpx
from dotenv import load_dotenv
from kubernetes import client as _k8s_client, config as _k8s_config
from mcp.server.fastmcp import FastMCP

load_dotenv()

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
GRAFANA_URL = os.getenv(
    "GRAFANA_URL",
    "http://grafana.monitoring.svc.cluster.local:3000",
)
GRAFANA_TOKEN = os.getenv("GRAFANA_TOKEN", "")

MCP_SERVER_NAME = os.getenv("MCP_SERVER_NAME", "blockmaze-observability")
MCP_HOST = os.getenv("MCP_HOST", "0.0.0.0")
MCP_PORT = int(os.getenv("MCP_PORT", "8000"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "info")
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30"))

# ---------------------------------------------------------------------------
# API key → username map
#
# Preferred: MCP_API_KEYS=alice:key-abc,bob:key-xyz  (comma-separated user:key pairs)
# Legacy:    MCP_API_KEY=somekey                      (single key, username "default")
# ---------------------------------------------------------------------------
def _load_api_keys() -> dict:
    """Return {api_key: username} mapping from env vars."""
    keys: dict = {}
    legacy = os.getenv("MCP_API_KEY", "")
    if legacy:
        keys[legacy] = "default"
    multi = os.getenv("MCP_API_KEYS", "")
    for pair in multi.split(","):
        pair = pair.strip()
        if ":" in pair:
            user, key = pair.split(":", 1)
            key = key.strip()
            if not key:
                continue
            keys[key] = user.strip()
    return keys

API_KEY_MAP = _load_api_keys()  # {key: username}

# ---------------------------------------------------------------------------
# RBAC — role → allowed tool patterns
#
# MCP_ROLES=alice:admin,bob:readonly
# Built-in roles:
#   admin    — all tools (default when MCP_ROLES is not set or user has no entry)
#   readonly — all read/query tools; excludes grafana_create_annotation
# ---------------------------------------------------------------------------
_ROLE_PATTERNS: dict[str, re.Pattern] = {
    "admin": re.compile(r".*"),
    "readonly": re.compile(
        r"^(prometheus_query|prometheus_query_range|prometheus_list_metrics|"
        r"prometheus_get_targets|prometheus_get_rules|prometheus_compare_metric|"
        r"loki_query_logs|loki_list_labels|loki_list_label_values|loki_get_log_patterns|"
        r"tempo_search_traces|tempo_get_trace|tempo_list_tags|tempo_list_tag_values|"
        r"alertmanager_get_alerts|alertmanager_get_silences|"

        r"k8s_get_pods|k8s_get_deployments|k8s_get_events|k8s_describe_pod|"
        r"k8s_get_pod_logs|k8s_get_nodes|k8s_resource_rightsizing|incident_summary)$"
    ),
}


def _load_roles() -> dict[str, str]:
    """Return {username: role} from MCP_ROLES=alice:admin,bob:readonly."""
    roles: dict[str, str] = {}
    for pair in os.getenv("MCP_ROLES", "").split(","):
        pair = pair.strip()
        if ":" in pair:
            user, role = pair.split(":", 1)
            roles[user.strip()] = role.strip()
    return roles


USER_ROLES: dict[str, str] = _load_roles()


def _is_tool_allowed(username: str, tool: str) -> bool:
    role = USER_ROLES.get(username, "admin")
    pattern = _ROLE_PATTERNS.get(role, _ROLE_PATTERNS["admin"])
    return bool(pattern.match(tool))


# ---------------------------------------------------------------------------
# Per-user rate limiting — sliding window
#
# MCP_RATE_LIMIT=60  — max requests per minute per user (0 = unlimited)
# ---------------------------------------------------------------------------
RATE_LIMIT_RPM = int(os.getenv("MCP_RATE_LIMIT", "0"))
_rate_buckets: dict[str, list[float]] = collections.defaultdict(list)
_rate_lock = asyncio.Lock()


async def _check_rate_limit(username: str) -> bool:
    """Return True if within limit, False if the user has exceeded it."""
    if not RATE_LIMIT_RPM:
        return True
    now = time.time()
    async with _rate_lock:
        cutoff = now - 60.0
        _rate_buckets[username] = [t for t in _rate_buckets[username] if t > cutoff]
        if len(_rate_buckets[username]) >= RATE_LIMIT_RPM:
            return False
        _rate_buckets[username].append(now)
        return True



# ---------------------------------------------------------------------------
# Structured access logging  (JSON → stdout → Loki)
# ---------------------------------------------------------------------------
logging.basicConfig(stream=sys.stdout, level=LOG_LEVEL.upper(), format="%(message)s")
_log = logging.getLogger("mcp.access")


def _log_access(user: str, tool: str | None, path: str, status: int) -> None:
    _log.info(json.dumps({
        "ts": time.time(),
        "user": user,
        "tool": tool,
        "path": path,
        "status": status,
    }))

# ---------------------------------------------------------------------------
# Kubernetes client (in-cluster, falls back to kubeconfig for local dev)
# ---------------------------------------------------------------------------
try:
    _k8s_config.load_incluster_config()
except _k8s_config.ConfigException:
    try:
        _k8s_config.load_kube_config()
    except _k8s_config.ConfigException:
        print("WARNING: No Kubernetes config found — K8s tools will be unavailable", flush=True)

_k8s_v1 = _k8s_client.CoreV1Api()
_k8s_apps = _k8s_client.AppsV1Api()

# Shared HTTP client — connection pool reused across all tool calls
_http = httpx.AsyncClient(timeout=HTTP_TIMEOUT)


async def _get(url: str, **kwargs) -> httpx.Response:
    """Wrapper around _http.get that raises a clean RuntimeError on network failure.

    HTTP 4xx/5xx bodies are returned as-is because backends (e.g. Prometheus)
    embed useful error details in the JSON even on error status codes.
    """
    try:
        return await _http.get(url, **kwargs)
    except httpx.TimeoutException:
        backend = url.split("/")[2]
        raise RuntimeError(f"Timeout reaching {backend}")
    except httpx.ConnectError:
        backend = url.split("/")[2]
        raise RuntimeError(f"Cannot connect to {backend} — is the backend service running in-cluster?")
    except httpx.HTTPError as e:
        raise RuntimeError(f"HTTP error: {e}")


_DURATION_RE = re.compile(r"^\d+(\.\d+)?[smhd]$")
_NS_RE = re.compile(r"^[a-z0-9][a-z0-9\-]*$")


def _parse_since_to_ns(since: str) -> int:
    """Convert duration string like '1h', '30m', '2d' to nanoseconds offset from now."""
    multipliers = {"s": int(1e9), "m": int(60e9), "h": int(3600e9), "d": int(86400e9)}
    if not _DURATION_RE.match(since):
        raise ValueError(f"Invalid since format: {since!r} — expected e.g. '30m', '2h', '1d'")
    unit = since[-1].lower()
    val = float(since[:-1])
    return int(time.time() * 1e9) - int(val * multipliers[unit])


def _parse_since_to_s(since: str) -> int:
    """Convert duration string to Unix timestamp seconds."""
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    if not _DURATION_RE.match(since):
        raise ValueError(f"Invalid since format: {since!r} — expected e.g. '30m', '2h', '1d'")
    unit = since[-1].lower()
    val = float(since[:-1])
    return int(time.time()) - int(val * multipliers[unit])


# ---------------------------------------------------------------------------
# MCP server
# ---------------------------------------------------------------------------
mcp = FastMCP(MCP_SERVER_NAME, stateless_http=True, host=MCP_HOST)


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
    r = await _get(f"{PROMETHEUS_URL}/api/v1/query", params=params)
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
    r = await _get(
        f"{PROMETHEUS_URL}/api/v1/query_range",
        params={"query": promql, "start": start, "end": end, "step": step},
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
    r = await _get(f"{PROMETHEUS_URL}/api/v1/label/__name__/values", params=params)
    return r.json().get("data", [])


@mcp.tool()
async def prometheus_get_targets() -> dict:
    """Get all Prometheus scrape targets and their health/last-scrape status."""
    r = await _get(f"{PROMETHEUS_URL}/api/v1/targets")
    return r.json()


@mcp.tool()
async def prometheus_get_rules() -> dict:
    """Get all Prometheus alerting and recording rules and their evaluation state."""
    r = await _get(f"{PROMETHEUS_URL}/api/v1/rules")
    return r.json()


@mcp.tool()
async def prometheus_compare_metric(
    promql: str,
    window: str = "5m",
    offset: str = "1d",
) -> dict:
    """Compare a PromQL metric's current value against the same window in the past.

    Useful for answering "is error rate higher than yesterday at the same time?"
    without writing two manual range queries.

    Args:
        promql: PromQL expression to evaluate, e.g. 'rate(http_requests_total[5m])'
        window: aggregation window already embedded in promql or used for display, e.g. '5m'
        offset: how far back to compare, e.g. '1d' for same time yesterday, '1w' for last week
    """
    now = int(time.time())
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
    unit = offset[-1].lower()
    offset_secs = int(float(offset[:-1]) * multipliers.get(unit, 86400))
    past_ts = now - offset_secs

    current_r, past_r = await asyncio.gather(
        _get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": promql, "time": now}),
        _get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": promql, "time": past_ts}),
    )
    current_data = current_r.json()
    past_data = past_r.json()

    def _scalar(data: dict) -> float | None:
        results = data.get("data", {}).get("result", [])
        if not results:
            return None
        try:
            return float(results[0]["value"][1])
        except (KeyError, IndexError, ValueError):
            return None

    current_val = _scalar(current_data)
    past_val = _scalar(past_data)

    change_pct: float | None = None
    if current_val is not None and past_val is not None and past_val != 0:
        change_pct = round((current_val - past_val) / abs(past_val) * 100, 2)

    return {
        "promql": promql,
        "window": window,
        "offset": offset,
        "current": {"timestamp": now, "value": current_val},
        "past": {"timestamp": past_ts, "value": past_val},
        "change_pct": change_pct,
        "summary": (
            f"{change_pct:+.1f}% vs {offset} ago"
            if change_pct is not None
            else "comparison unavailable (no data for one or both windows)"
        ),
    }


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
    r = await _get(
        f"{LOKI_URL}/loki/api/v1/query_range",
        params={
            "query": logql,
            "limit": limit,
            "start": start_ns,
            "end": end_ns,
            "direction": direction,
        },
    )
    return r.json()


@mcp.tool()
async def loki_list_labels() -> list:
    """List all available Loki label names (e.g. 'namespace', 'app', 'container')."""
    r = await _get(f"{LOKI_URL}/loki/api/v1/labels")
    return r.json().get("data", [])


@mcp.tool()
async def loki_list_label_values(label: str) -> list:
    """List all values for a specific Loki label.

    Args:
        label: label name — use loki_list_labels first to discover names.
            Common values: 'k8s_namespace_name', 'k8s_pod_name',
            'k8s_container_name', 'k8s_deployment_name', 'service_name'
    """
    r = await _get(f"{LOKI_URL}/loki/api/v1/label/{quote(label, safe='')}/values")
    return r.json().get("data", [])


@mcp.tool()
async def loki_get_log_patterns(
    logql: str,
    since: str = "1h",
    limit: int = 20,
) -> dict:
    """Cluster log lines into recurring patterns using Loki's pattern detection (Loki ≥ 3.0).

    Returns patterns ranked by frequency — ideal for "what are the most common
    errors in the last hour?" without reading thousands of individual log lines.

    Args:
        logql: LogQL stream selector, e.g. '{namespace="services",app="evm-api"}'
        since: lookback window, e.g. '30m', '2h', '1d' (default '1h')
        limit: max number of patterns to return (default 20)
    """
    start_ns = _parse_since_to_ns(since)
    end_ns = int(time.time() * 1e9)
    r = await _get(
        f"{LOKI_URL}/loki/api/v1/patterns",
        params={"query": logql, "start": start_ns, "end": end_ns},
    )
    if r.status_code == 404:
        return {"error": "Pattern detection requires Loki ≥ 3.0 — this instance returned 404"}
    data = r.json()
    patterns = data.get("data", data) if isinstance(data, dict) else data
    if isinstance(patterns, list):
        patterns = sorted(patterns, key=lambda p: p.get("volume", 0), reverse=True)[:limit]
    return {"since": since, "patterns": patterns}


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
    r = await _get(f"{TEMPO_URL}/api/search", params=params)
    return r.json()


@mcp.tool()
async def tempo_get_trace(trace_id: str) -> dict:
    """Fetch a complete trace tree by trace ID from Tempo.

    Args:
        trace_id: hex trace ID, e.g. '1234abcd5678ef90'
    """
    r = await _get(f"{TEMPO_URL}/api/traces/{quote(trace_id, safe='')}")
    return r.json()


@mcp.tool()
async def tempo_list_tags() -> dict:
    """List all searchable tag names available in Tempo (span attributes)."""
    r = await _get(f"{TEMPO_URL}/api/search/tags")
    return r.json()


@mcp.tool()
async def tempo_list_tag_values(tag: str) -> dict:
    """List all values for a searchable Tempo tag.

    Args:
        tag: tag name, e.g. 'service.name', 'http.status_code', 'span.kind'
    """
    r = await _get(f"{TEMPO_URL}/api/search/tag/{quote(tag, safe='')}/values")
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
    r = await _get(f"{ALERTMANAGER_URL}/api/v2/alerts", params=params)
    return r.json()


@mcp.tool()
async def alertmanager_get_silences() -> list:
    """Get all silences currently configured in AlertManager."""
    r = await _get(f"{ALERTMANAGER_URL}/api/v2/silences")
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
async def k8s_get_pod_logs(
    namespace: str,
    label_selector: str,
    container: str = "",
    tail_lines: int = 200,
    previous: bool = False,
) -> list:
    """Fetch recent logs from all pods matching a label selector.

    Saves the step of looking up pod names first — useful for deployments with
    multiple replicas or when you only know the app label.

    Args:
        namespace: namespace to search in
        label_selector: label selector, e.g. 'app=evm-api' or 'app=evm-api,env=prod'
        container: specific container name to fetch logs from; defaults to the first container
        tail_lines: number of log lines per pod (default 200)
        previous: if True, fetch logs from the previous (crashed) container instance
    """
    pods = await asyncio.to_thread(
        _k8s_v1.list_namespaced_pod, namespace, label_selector=label_selector
    )

    if not pods.items:
        return [{"error": f"No pods found in {namespace!r} with selector {label_selector!r}"}]

    results = []
    for pod in pods.items:
        kwargs: dict = {"tail_lines": tail_lines, "previous": previous}
        if container:
            kwargs["container"] = container
        try:
            logs = await asyncio.to_thread(
                _k8s_v1.read_namespaced_pod_log,
                pod.metadata.name,
                namespace,
                **kwargs,
            )
        except Exception as e:
            logs = f"<error fetching logs: {e}>"

        results.append({
            "pod": pod.metadata.name,
            "phase": pod.status.phase,
            "logs": logs[-5000:] if isinstance(logs, str) else logs,
        })

    return results


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


# ── Resource Rightsizing ─────────────────────────────────────────────────────

def _cpu_to_m(s: str | None) -> float | None:
    """Parse a Kubernetes CPU quantity string to millicores."""
    if not s:
        return None
    try:
        return float(s[:-1]) if s.endswith("m") else float(s) * 1000
    except ValueError:
        return None


def _mem_to_mib(s: str | None) -> float | None:
    """Parse a Kubernetes memory quantity string to MiB."""
    if not s:
        return None
    for suffix, factor in [
        ("Ki", 2**10), ("Mi", 2**20), ("Gi", 2**30), ("Ti", 2**40),
        ("K", 10**3), ("M", 10**6), ("G", 10**9), ("T", 10**12),
    ]:
        if s.endswith(suffix):
            try:
                return float(s[:-len(suffix)]) * factor / (2**20)
            except ValueError:
                return None
    try:
        return float(s) / (2**20)
    except ValueError:
        return None


def _prom_to_map(results: list) -> dict[tuple[str, str], float]:
    """Convert Prometheus instant-query results to {(namespace, container): float}."""
    out: dict[tuple[str, str], float] = {}
    for r in results:
        ns = r["metric"].get("namespace", "")
        ctr = r["metric"].get("container", "")
        if ns and ctr:
            try:
                v = float(r["value"][1])
                if v == v:  # skip NaN
                    out[(ns, ctr)] = v
            except (KeyError, ValueError, IndexError):
                pass
    return out


@mcp.tool()
async def k8s_resource_rightsizing(
    namespace: str = "",
    lookback: str = "7d",
    cpu_headroom_factor: float = 2.0,
    memory_headroom_factor: float = 1.3,
) -> dict:
    """Analyze resource requests and limits across the EKS cluster and produce
    right-sizing recommendations backed by real Prometheus usage trends.

    Runs 7 parallel Prometheus queries (avg / P95 / peak CPU and memory, plus restart
    counts) over the requested lookback window, then merges that data with each
    container's configured requests and limits from the Kubernetes API.

    Returns a severity-ranked report (critical → warning → ok) with current config,
    observed usage stats, recommended values, and the delta between them.

    Severity rules:
      critical — request is below actual average usage (eviction / OOM risk), or
                 peak memory exceeds 90 % of the configured limit, or > 10 restarts
      warning  — request is more than 3× the P95 usage (over-provisioned)
      ok       — everything looks reasonable

    Args:
        namespace: filter to a single namespace; empty string means all namespaces
        lookback: Prometheus look-back window, e.g. '7d', '3d', '1d' (default '7d')
        cpu_headroom_factor: multiply peak CPU by this for the recommended limit (default 2.0)
        memory_headroom_factor: multiply peak memory by this for the recommended limit (default 1.3)
    """
    if namespace and not _NS_RE.match(namespace):
        raise ValueError(f"Invalid namespace: {namespace!r}")
    if not re.match(r"^\d+(\.\d+)?[smhdw]$", lookback):
        raise ValueError(f"Invalid lookback: {lookback!r} — expected e.g. '7d', '3d', '1h'")

    ns_label = f', namespace="{namespace}"' if namespace else ""
    cpu_sel = f'container!="", container!="POD"{ns_label}'
    mem_sel = cpu_sel

    queries = {
        "cpu_avg":  f'avg by(namespace,container)(avg_over_time(rate(container_cpu_usage_seconds_total{{{cpu_sel}}}[5m])[{lookback}:5m]))*1000',
        "cpu_p95":  f'max by(namespace,container)(quantile_over_time(0.95,rate(container_cpu_usage_seconds_total{{{cpu_sel}}}[5m])[{lookback}:5m]))*1000',
        "cpu_max":  f'max by(namespace,container)(max_over_time(rate(container_cpu_usage_seconds_total{{{cpu_sel}}}[5m])[{lookback}:5m]))*1000',
        "mem_avg":  f'avg by(namespace,container)(avg_over_time(container_memory_working_set_bytes{{{mem_sel}}}[{lookback}]))/1048576',
        "mem_p95":  f'max by(namespace,container)(quantile_over_time(0.95,container_memory_working_set_bytes{{{mem_sel}}}[{lookback}]))/1048576',
        "mem_max":  f'max by(namespace,container)(max_over_time(container_memory_working_set_bytes{{{mem_sel}}}[{lookback}]))/1048576',
        "restarts": f'sum by(namespace,container)(increase(kube_pod_container_status_restarts_total{{container!=""{ns_label}}}[{lookback}]))',
    }

    async def _pq(promql: str) -> list:
        r = await _get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": promql})
        return r.json().get("data", {}).get("result", [])

    async def _fetch_pods():
        if namespace:
            return await asyncio.to_thread(_k8s_v1.list_namespaced_pod, namespace)
        return await asyncio.to_thread(_k8s_v1.list_pod_for_all_namespaces)

    *raw_list, pods_resp = await asyncio.gather(*[_pq(q) for q in queries.values()], _fetch_pods())
    prom = {k: _prom_to_map(res) for k, res in zip(queries, raw_list)}

    k8s_cfg: dict[tuple[str, str], dict] = {}
    k8s_restarts: dict[tuple[str, str], int] = {}

    for pod in pods_resp.items:
        pod_ns = pod.metadata.namespace
        for cs in pod.status.container_statuses or []:
            key = (pod_ns, cs.name)
            k8s_restarts[key] = k8s_restarts.get(key, 0) + cs.restart_count
        for c in pod.spec.containers:
            key = (pod_ns, c.name)
            if key in k8s_cfg:
                continue
            res = c.resources
            reqs = (res.requests or {}) if res else {}
            lims = (res.limits or {}) if res else {}
            k8s_cfg[key] = {
                "cpu_req_m":   _cpu_to_m(reqs.get("cpu")),
                "cpu_lim_m":   _cpu_to_m(lims.get("cpu")),
                "mem_req_mi":  _mem_to_mib(reqs.get("memory")),
                "mem_lim_mi":  _mem_to_mib(lims.get("memory")),
            }

    # ── Merge & classify ──────────────────────────────────────────────────────
    all_keys = (
        set(prom["cpu_avg"]) | set(prom["mem_avg"]) | set(k8s_cfg)
    )

    def _sev(cpu_avg, cpu_p95, cpu_max, mem_avg, mem_p95, mem_max,
             cpu_req, cpu_lim, mem_req, mem_lim, restarts) -> str:
        if (
            (mem_avg and mem_req and mem_req < mem_avg * 0.9)
            or (cpu_avg and cpu_req and cpu_req < cpu_avg * 0.9)
            or (mem_max and mem_lim and mem_max > mem_lim * 0.9)
            or (restarts and restarts > 10)
        ):
            return "critical"
        if (
            (cpu_p95 and cpu_req and cpu_req > cpu_p95 * 3)
            or (mem_p95 and mem_req and mem_req > mem_p95 * 3)
        ):
            return "warning"
        return "ok"

    def _r1(v):
        return round(v, 1) if v is not None else None

    containers = []
    for key in sorted(all_keys):
        ns_name, cname = key
        if not cname or cname == "POD":
            continue

        p = {k: prom[k].get(key) for k in prom}
        k = k8s_cfg.get(key, {})

        cpu_avg, cpu_p95, cpu_max = p["cpu_avg"], p["cpu_p95"], p["cpu_max"]
        mem_avg, mem_p95, mem_max = p["mem_avg"], p["mem_p95"], p["mem_max"]
        cpu_req, cpu_lim = k.get("cpu_req_m"), k.get("cpu_lim_m")
        mem_req, mem_lim = k.get("mem_req_mi"), k.get("mem_lim_mi")
        restarts = p.get("restarts") or 0

        rec_cpu_req = round(cpu_p95) if cpu_p95 is not None else None
        rec_cpu_lim = round(cpu_max * cpu_headroom_factor) if cpu_max is not None else None
        rec_mem_req = round(mem_p95) if mem_p95 is not None else None
        rec_mem_lim = round(mem_max * memory_headroom_factor) if mem_max is not None else None

        def _delta(cur, rec):
            return round(cur - rec, 1) if cur is not None and rec is not None else None

        containers.append({
            "namespace": ns_name,
            "container": cname,
            "severity": _sev(
                cpu_avg, cpu_p95, cpu_max,
                mem_avg, mem_p95, mem_max,
                cpu_req, cpu_lim, mem_req, mem_lim,
                restarts,
            ),
            "usage": {
                "cpu_avg_m":  _r1(cpu_avg),
                "cpu_p95_m":  _r1(cpu_p95),
                "cpu_max_m":  _r1(cpu_max),
                "mem_avg_mi": _r1(mem_avg),
                "mem_p95_mi": _r1(mem_p95),
                "mem_max_mi": _r1(mem_max),
            },
            "current": {
                "cpu_req_m":  _r1(cpu_req),
                "cpu_lim_m":  _r1(cpu_lim),
                "mem_req_mi": _r1(mem_req),
                "mem_lim_mi": _r1(mem_lim),
            },
            "recommended": {
                "cpu_req_m":  rec_cpu_req,
                "cpu_lim_m":  rec_cpu_lim,
                "mem_req_mi": rec_mem_req,
                "mem_lim_mi": rec_mem_lim,
            },
            "delta": {
                "cpu_req_m":  _delta(cpu_req, rec_cpu_req),
                "cpu_lim_m":  _delta(cpu_lim, rec_cpu_lim),
                "mem_req_mi": _delta(mem_req, rec_mem_req),
                "mem_lim_mi": _delta(mem_lim, rec_mem_lim),
            },
            "restarts_in_window": round(restarts),
        })

    sev_rank = {"critical": 0, "warning": 1, "ok": 2}
    containers.sort(key=lambda c: (sev_rank[c["severity"]], c["namespace"], c["container"]))

    counts = {"critical": 0, "warning": 0, "ok": 0}
    for c in containers:
        counts[c["severity"]] += 1

    reclaimable_cpu = sum(
        c["delta"]["cpu_req_m"] for c in containers
        if (c["delta"]["cpu_req_m"] or 0) > 0
    )
    reclaimable_mem = sum(
        c["delta"]["mem_req_mi"] for c in containers
        if (c["delta"]["mem_req_mi"] or 0) > 0
    )

    return {
        "lookback": lookback,
        "namespace_filter": namespace or "all",
        "summary": {
            "total_containers": len(containers),
            "critical": counts["critical"],
            "warning": counts["warning"],
            "ok": counts["ok"],
            "reclaimable_cpu_m": round(reclaimable_cpu),
            "reclaimable_mem_mi": round(reclaimable_mem),
        },
        "containers": containers,
    }


# ── Incident Summary ─────────────────────────────────────────────────────────

@mcp.tool()
async def incident_summary(
    service: str,
    namespace: str = "",
    since: str = "1h",
) -> dict:
    """Produce a unified incident report by correlating alerts, error logs, slow traces,
    and Kubernetes events for a service — all in a single call.

    Runs all backend queries in parallel and returns a structured, severity-ranked
    snapshot. This is the recommended first tool to call when investigating an incident.

    Args:
        service: service name, e.g. 'evm-api'
        namespace: Kubernetes namespace to scope K8s queries; empty searches all
        since: lookback window, e.g. '30m', '1h', '6h' (default '1h')
    """
    gather_results = await asyncio.gather(
        alertmanager_get_alerts(filter=f'service="{service}"'),
        loki_query_logs(
            logql=f'{{service_name="{service}"}} |= "error"',
            limit=20,
            since=since,
        ),
        tempo_search_traces(service=service, tags="error=true", since=since, limit=10),
        k8s_get_events(namespace=namespace, warning_only=True),
        prometheus_query(
            f'sum(rate(http_requests_total{{service="{service}",status=~"5.."}}[5m]))'
        ),
        return_exceptions=True,
    )
    alerts_r, logs_r, traces_r, events_r, err_rate_r = gather_results

    def _safe(r, default):
        return default if isinstance(r, Exception) else r

    active_alerts = _safe(alerts_r, [])
    log_data      = _safe(logs_r, {})
    trace_data    = _safe(traces_r, {})
    k8s_events    = _safe(events_r, [])
    err_rate_data = _safe(err_rate_r, {})

    # Extract log lines
    log_lines = [
        line
        for stream in (log_data.get("data", {}).get("result", []) or [])
        for _, line in stream.get("values", [])
    ]

    # Extract traces list
    trace_list = trace_data.get("traces", []) if isinstance(trace_data, dict) else []

    # Filter K8s events to those mentioning this service
    svc_events = [
        e for e in k8s_events
        if service in (e.get("object", "") + " " + e.get("message", ""))
    ]

    # Extract current error rate (rps)
    err_rate_val: float | None = None
    for res in (err_rate_data.get("data", {}).get("result", []) or []):
        try:
            err_rate_val = float(res["value"][1])
            break
        except (KeyError, ValueError, IndexError):
            pass

    # Severity — worst signal wins
    critical_alerts = [a for a in active_alerts if a.get("labels", {}).get("severity") == "critical"]
    if critical_alerts or (err_rate_val and err_rate_val > 0.1):
        severity = "critical"
    elif active_alerts or trace_list or (err_rate_val and err_rate_val > 0):
        severity = "warning"
    else:
        severity = "ok"

    return {
        "service": service,
        "namespace": namespace or "all",
        "since": since,
        "severity": severity,
        "summary": {
            "active_alerts":       len(active_alerts),
            "critical_alerts":     len(critical_alerts),
            "error_log_lines":     len(log_lines),
            "error_traces":        len(trace_list),
            "k8s_warning_events":  len(svc_events),
            "error_rate_rps":      round(err_rate_val, 4) if err_rate_val is not None else None,
        },
        "alerts":              active_alerts[:5],
        "recent_errors":       log_lines[:10],
        "traces":              trace_list[:5],
        "k8s_events":          svc_events[:10],
    }


# ---------------------------------------------------------------------------
# ASGI middleware — handles /health inline, enforces API key, passes
# lifespan events straight through so MCP's task group initialises correctly
# ---------------------------------------------------------------------------
_HEALTH_BODY = json.dumps({"status": "ok", "service": MCP_SERVER_NAME}).encode()
_HEALTH_HEADERS = [
    (b"content-type", b"application/json"),
    (b"content-length", str(len(_HEALTH_BODY)).encode()),
]


async def _check_ready() -> tuple[bool, dict]:
    """Probe each backend concurrently. Returns (all_ok, {backend: status})."""
    probes = {
        "prometheus":   f"{PROMETHEUS_URL}/-/ready",
        "loki":         f"{LOKI_URL}/ready",
        "tempo":        f"{TEMPO_URL}/ready",
        "alertmanager": f"{ALERTMANAGER_URL}/api/v2/status",
    }

    async def _probe(name: str, url: str) -> tuple[str, str]:
        try:
            r = await _http.get(url, timeout=5.0)
            return name, "ok" if r.status_code < 500 else f"http_{r.status_code}"
        except Exception:
            return name, "unreachable"

    pairs = await asyncio.gather(*[_probe(n, u) for n, u in probes.items()])
    results = dict(pairs)
    all_ok = all(v == "ok" for v in results.values())
    return all_ok, results


async def _buffer_body(receive):
    """Buffer the full request body and return (raw_bytes, replay_receive)."""
    messages = []
    body = b""
    more = True
    while more:
        msg = await receive()
        messages.append(msg)
        if msg["type"] == "http.request":
            body += msg.get("body", b"")
            more = msg.get("more_body", False)
        else:
            break

    pos = [0]

    async def _replay():
        if pos[0] < len(messages):
            m = messages[pos[0]]
            pos[0] += 1
            return m
        return await receive()

    return body, _replay


def _extract_tool(raw: bytes) -> str | None:
    """Parse MCP JSON-RPC body and return the tool name if it's a tools/call."""
    try:
        payload = json.loads(raw)
        if payload.get("method") == "tools/call":
            return payload.get("params", {}).get("name")
    except Exception:
        pass
    return None


class _Middleware:
    def __init__(self, inner):
        self.inner = inner

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            try:
                await self.inner(scope, receive, send)
            finally:
                if scope["type"] == "lifespan":
                    await _http.aclose()
            return

        path = scope.get("path", "")

        if path == "/health":
            await send({"type": "http.response.start", "status": 200, "headers": _HEALTH_HEADERS})
            await send({"type": "http.response.body", "body": _HEALTH_BODY})
            return

        if path == "/ready":
            all_ok, statuses = await _check_ready()
            body = json.dumps({"ready": all_ok, "backends": statuses}).encode()
            status = 200 if all_ok else 503
            headers = [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
            ]
            await send({"type": "http.response.start", "status": status, "headers": headers})
            await send({"type": "http.response.body", "body": body})
            return

        # ── Auth ──────────────────────────────────────────────────────────
        username: str | None = None
        if API_KEY_MAP:
            headers = dict(scope.get("headers", []))
            auth = headers.get(b"authorization", b"").decode("utf-8", errors="ignore")
            key = auth[7:] if auth.startswith("Bearer ") else ""
            username = next(
                (u for k, u in API_KEY_MAP.items() if hmac.compare_digest(k, key)),
                None,
            )
            if username is None:
                body = b"Unauthorized"
                await send({"type": "http.response.start", "status": 401,
                            "headers": [(b"content-length", str(len(body)).encode())]})
                await send({"type": "http.response.body", "body": body})
                return

        # ── Rate limiting ─────────────────────────────────────────────────
        if username and not await _check_rate_limit(username):
            body = b"Too Many Requests"
            await send({"type": "http.response.start", "status": 429,
                        "headers": [(b"content-length", str(len(body)).encode()),
                                    (b"retry-after", b"60")]})
            await send({"type": "http.response.body", "body": body})
            return

        # ── Buffer body to extract tool name for logging / RBAC ───────────
        tool_name: str | None = None
        if username:
            raw_body, receive = await _buffer_body(receive)
            tool_name = _extract_tool(raw_body)

        # ── RBAC ──────────────────────────────────────────────────────────
        if username and tool_name and not _is_tool_allowed(username, tool_name):
            body = b"Forbidden"
            await send({"type": "http.response.start", "status": 403,
                        "headers": [(b"content-length", str(len(body)).encode())]})
            await send({"type": "http.response.body", "body": body})
            _log_access(username, tool_name, path, 403)
            return

        # ── Capture response status ────────────────────────────────────────
        status_holder = [200]

        async def _send(message):
            if message["type"] == "http.response.start":
                status_holder[0] = message["status"]
            await send(message)

        await self.inner(scope, receive, _send)

        if username:
            _log_access(username, tool_name, path, status_holder[0])


# ---------------------------------------------------------------------------
# ASGI app
# ---------------------------------------------------------------------------
app = _Middleware(mcp.streamable_http_app())

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "server:app",
        host=MCP_HOST,
        port=MCP_PORT,
        log_level=LOG_LEVEL,
    )
