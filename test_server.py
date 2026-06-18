"""Unit tests for server.py — covers the new multi-user auth and logging logic."""
import json
import os
import sys
import time
import types
import unittest
from unittest.mock import AsyncMock, MagicMock, patch


# ---------------------------------------------------------------------------
# Stub the kubernetes module so server.py can be imported without a cluster
# ---------------------------------------------------------------------------
def _make_k8s_stub():
    k8s = types.ModuleType("kubernetes")
    k8s.client = types.ModuleType("kubernetes.client")
    k8s.config = types.ModuleType("kubernetes.config")

    class ConfigException(Exception):
        pass

    k8s.config.ConfigException = ConfigException
    k8s.config.load_incluster_config = MagicMock(side_effect=ConfigException)
    k8s.config.load_kube_config = MagicMock()
    k8s.client.CoreV1Api = MagicMock(return_value=MagicMock())
    k8s.client.AppsV1Api = MagicMock(return_value=MagicMock())
    return k8s


_k8s_stub = _make_k8s_stub()
sys.modules.setdefault("kubernetes", _k8s_stub)
sys.modules.setdefault("kubernetes.client", _k8s_stub.client)
sys.modules.setdefault("kubernetes.config", _k8s_stub.config)


# ---------------------------------------------------------------------------
# Now import the functions we want to test directly
# ---------------------------------------------------------------------------
os.environ.setdefault("MCP_API_KEYS", "alice:key-abc,bob:key-xyz")

import server as srv  # noqa: E402


# ---------------------------------------------------------------------------
# _load_api_keys
# ---------------------------------------------------------------------------
class TestLoadApiKeys(unittest.TestCase):
    def _load(self, multi="", legacy=""):
        with patch.dict(os.environ, {"MCP_API_KEYS": multi, "MCP_API_KEY": legacy}, clear=False):
            # Re-run the function (not the module-level cached value)
            return srv._load_api_keys()

    def test_multi_key_parsing(self):
        keys = self._load(multi="alice:key-abc,bob:key-xyz")
        self.assertEqual(keys["key-abc"], "alice")
        self.assertEqual(keys["key-xyz"], "bob")

    def test_legacy_key(self):
        keys = self._load(legacy="oldkey")
        self.assertEqual(keys["oldkey"], "default")

    def test_both_coexist(self):
        keys = self._load(multi="carol:key-c", legacy="legacykey")
        self.assertIn("key-c", keys)
        self.assertIn("legacykey", keys)
        self.assertEqual(keys["key-c"], "carol")
        self.assertEqual(keys["legacykey"], "default")

    def test_whitespace_trimmed(self):
        keys = self._load(multi=" dave : key-d , eve : key-e ")
        self.assertIn("key-d", keys)
        self.assertEqual(keys["key-d"], "dave")

    def test_empty_returns_empty(self):
        keys = self._load(multi="", legacy="")
        self.assertEqual(keys, {})

    def test_malformed_pair_ignored(self):
        keys = self._load(multi="nodash,alice:key-abc")
        self.assertNotIn("nodash", keys)
        self.assertIn("key-abc", keys)


# ---------------------------------------------------------------------------
# _extract_tool
# ---------------------------------------------------------------------------
class TestExtractTool(unittest.TestCase):
    def test_tools_call(self):
        payload = json.dumps({"method": "tools/call", "params": {"name": "prometheus_query"}}).encode()
        self.assertEqual(srv._extract_tool(payload), "prometheus_query")

    def test_non_tools_call(self):
        payload = json.dumps({"method": "tools/list"}).encode()
        self.assertIsNone(srv._extract_tool(payload))

    def test_invalid_json(self):
        self.assertIsNone(srv._extract_tool(b"not json"))

    def test_empty_body(self):
        self.assertIsNone(srv._extract_tool(b""))


# ---------------------------------------------------------------------------
# _parse_since helpers
# ---------------------------------------------------------------------------
class TestParseSince(unittest.TestCase):
    def test_ns_hour(self):
        before = int(time.time() * 1e9)
        result = srv._parse_since_to_ns("1h")
        expected_delta = int(3600e9)
        self.assertAlmostEqual(before - result, expected_delta, delta=int(1e9))

    def test_s_minutes(self):
        before = int(time.time())
        result = srv._parse_since_to_s("30m")
        expected = before - 1800
        self.assertAlmostEqual(result, expected, delta=2)


# ---------------------------------------------------------------------------
# _Middleware — auth and routing
# ---------------------------------------------------------------------------
class TestMiddleware(unittest.IsolatedAsyncioTestCase):
    def _make_scope(self, path, auth_header=None):
        headers = []
        if auth_header:
            headers.append((b"authorization", auth_header.encode()))
        return {"type": "http", "path": path, "headers": headers}

    async def _run_middleware(self, scope, body=b"", api_key_map=None):
        inner_called = []

        async def inner(scope, receive, send):
            inner_called.append(True)
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        responses = []

        async def send(msg):
            responses.append(msg)

        async def receive():
            return {"type": "http.request", "body": body, "more_body": False}

        middleware = srv._Middleware(inner)
        if api_key_map is not None:
            with patch.object(srv, "API_KEY_MAP", api_key_map):
                await middleware(scope, receive, send)
        else:
            await middleware(scope, receive, send)

        status = next((m["status"] for m in responses if m["type"] == "http.response.start"), None)
        return status, inner_called

    async def test_health_path_no_auth_needed(self):
        scope = self._make_scope("/health")
        status, inner = await self._run_middleware(scope, api_key_map={"key": "alice"})
        self.assertEqual(status, 200)
        self.assertEqual(inner, [])  # inner app not called

    async def test_valid_bearer_passes(self):
        scope = self._make_scope("/mcp", auth_header="Bearer key-abc")
        status, inner = await self._run_middleware(scope, api_key_map={"key-abc": "alice"})
        self.assertEqual(status, 200)
        self.assertTrue(inner)

    async def test_invalid_bearer_returns_401(self):
        scope = self._make_scope("/mcp", auth_header="Bearer wrong-key")
        status, inner = await self._run_middleware(scope, api_key_map={"key-abc": "alice"})
        self.assertEqual(status, 401)
        self.assertEqual(inner, [])

    async def test_missing_auth_returns_401(self):
        scope = self._make_scope("/mcp")
        status, inner = await self._run_middleware(scope, api_key_map={"key-abc": "alice"})
        self.assertEqual(status, 401)

    async def test_no_api_key_map_allows_all(self):
        scope = self._make_scope("/mcp")
        status, inner = await self._run_middleware(scope, api_key_map={})
        self.assertEqual(status, 200)

    async def test_lifespan_passes_through(self):
        inner_called = []

        async def inner(scope, receive, send):
            inner_called.append(scope["type"])

        middleware = srv._Middleware(inner)
        await middleware({"type": "lifespan"}, AsyncMock(), AsyncMock())
        self.assertEqual(inner_called, ["lifespan"])


# ---------------------------------------------------------------------------
# _log_access — emits valid JSON
# ---------------------------------------------------------------------------
class TestLogAccess(unittest.TestCase):
    def test_emits_json_with_required_fields(self):
        captured = []
        with patch.object(srv._log, "info", side_effect=captured.append):
            srv._log_access("alice", "prometheus_query", "/mcp", 200)

        self.assertEqual(len(captured), 1)
        obj = json.loads(captured[0])
        self.assertEqual(obj["user"], "alice")
        self.assertEqual(obj["tool"], "prometheus_query")
        self.assertEqual(obj["path"], "/mcp")
        self.assertEqual(obj["status"], 200)
        self.assertIn("ts", obj)

    def test_tool_can_be_none(self):
        captured = []
        with patch.object(srv._log, "info", side_effect=captured.append):
            srv._log_access("bob", None, "/health", 200)
        obj = json.loads(captured[0])
        self.assertIsNone(obj["tool"])


# ---------------------------------------------------------------------------
# _check_ready — backend probing
# ---------------------------------------------------------------------------
class TestCheckReady(unittest.IsolatedAsyncioTestCase):
    async def test_all_ok(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200

        async def fake_get(url, timeout):
            return mock_resp

        with patch.object(srv._http, "get", side_effect=fake_get):
            ok, statuses = await srv._check_ready()

        self.assertTrue(ok)
        self.assertTrue(all(v == "ok" for v in statuses.values()))

    async def test_one_backend_down(self):
        import httpx as _httpx

        call_count = [0]

        async def fake_get(url, timeout):
            call_count[0] += 1
            if "prometheus" in url:
                raise _httpx.ConnectError("refused")
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            return mock_resp

        with patch.object(srv._http, "get", side_effect=fake_get):
            ok, statuses = await srv._check_ready()

        self.assertFalse(ok)
        self.assertIn("unreachable", statuses["prometheus"])
        self.assertEqual(statuses["loki"], "ok")


# ---------------------------------------------------------------------------
# k8s_get_pod_logs
# ---------------------------------------------------------------------------
class TestK8sGetPodLogs(unittest.IsolatedAsyncioTestCase):
    def _make_pod(self, name, phase="Running"):
        pod = MagicMock()
        pod.metadata.name = name
        pod.metadata.namespace = "services"
        pod.status.phase = phase
        return pod

    async def test_returns_logs_for_each_pod(self):
        pod_list = MagicMock()
        pod_list.items = [self._make_pod("evm-api-abc"), self._make_pod("evm-api-def")]

        srv._k8s_v1.list_namespaced_pod = MagicMock(return_value=pod_list)
        srv._k8s_v1.read_namespaced_pod_log = MagicMock(side_effect=lambda name, ns, **kw: f"log line from {name}")

        result = await srv.k8s_get_pod_logs("services", "app=evm-api")

        self.assertEqual(len(result), 2)
        pods_in_result = {r["pod"] for r in result}
        self.assertIn("evm-api-abc", pods_in_result)
        self.assertTrue(all("log line" in r["logs"] for r in result))

    async def test_no_pods_returns_error(self):
        pod_list = MagicMock()
        pod_list.items = []
        srv._k8s_v1.list_namespaced_pod = MagicMock(return_value=pod_list)
        result = await srv.k8s_get_pod_logs("services", "app=missing")
        self.assertEqual(len(result), 1)
        self.assertIn("error", result[0])

    async def test_log_fetch_error_included_in_result(self):
        pod_list = MagicMock()
        pod_list.items = [self._make_pod("crashpod")]
        srv._k8s_v1.list_namespaced_pod = MagicMock(return_value=pod_list)
        srv._k8s_v1.read_namespaced_pod_log = MagicMock(side_effect=Exception("container not ready"))
        result = await srv.k8s_get_pod_logs("services", "app=crash")
        self.assertIn("error fetching logs", result[0]["logs"])



# ---------------------------------------------------------------------------
# prometheus_compare_metric
# ---------------------------------------------------------------------------
class TestPrometheusCompareMetric(unittest.IsolatedAsyncioTestCase):
    def _make_prom_response(self, value: float | None):
        mock = MagicMock()
        if value is not None:
            mock.json.return_value = {
                "data": {"result": [{"value": [int(time.time()), str(value)]}]}
            }
        else:
            mock.json.return_value = {"data": {"result": []}}
        return mock

    async def test_returns_change_pct(self):
        call_count = [0]

        async def fake_get(url, params):
            call_count[0] += 1
            # first call = current (value 110), second = past (value 100)
            return self._make_prom_response(110.0 if call_count[0] == 1 else 100.0)

        with patch.object(srv._http, "get", side_effect=fake_get):
            result = await srv.prometheus_compare_metric("up", window="5m", offset="1d")

        self.assertAlmostEqual(result["change_pct"], 10.0)
        self.assertIn("+10.0%", result["summary"])

    async def test_no_data_returns_none_change(self):
        async def fake_get(url, params):
            return self._make_prom_response(None)

        with patch.object(srv._http, "get", side_effect=fake_get):
            result = await srv.prometheus_compare_metric("up")

        self.assertIsNone(result["change_pct"])
        self.assertIn("unavailable", result["summary"])

    async def test_decrease_shows_negative_pct(self):
        call_count = [0]

        async def fake_get(url, params):
            call_count[0] += 1
            return self._make_prom_response(80.0 if call_count[0] == 1 else 100.0)

        with patch.object(srv._http, "get", side_effect=fake_get):
            result = await srv.prometheus_compare_metric("up")

        self.assertAlmostEqual(result["change_pct"], -20.0)
        self.assertIn("-20.0%", result["summary"])

    async def test_result_contains_timestamps(self):
        async def fake_get(url, params):
            return self._make_prom_response(1.0)

        with patch.object(srv._http, "get", side_effect=fake_get):
            result = await srv.prometheus_compare_metric("up", offset="1d")

        self.assertIn("timestamp", result["current"])
        self.assertIn("timestamp", result["past"])
        delta = result["current"]["timestamp"] - result["past"]["timestamp"]
        self.assertAlmostEqual(delta, 86400, delta=5)


# ---------------------------------------------------------------------------
# loki_get_log_patterns
# ---------------------------------------------------------------------------
class TestLokiGetLogPatterns(unittest.IsolatedAsyncioTestCase):
    async def test_returns_patterns_sorted_by_volume(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": [
            {"pattern": "error connecting to <_>", "volume": 5},
            {"pattern": "timeout after <_>ms", "volume": 20},
            {"pattern": "retrying request", "volume": 10},
        ]}

        async def fake_get(url, params):
            return mock_resp

        with patch.object(srv._http, "get", side_effect=fake_get):
            result = await srv.loki_get_log_patterns('{app="evm-api"}')

        patterns = result["patterns"]
        self.assertEqual(patterns[0]["volume"], 20)
        self.assertEqual(patterns[1]["volume"], 10)

    async def test_404_returns_friendly_error(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_resp.json.return_value = {}

        async def fake_get(url, params):
            return mock_resp

        with patch.object(srv._http, "get", side_effect=fake_get):
            result = await srv.loki_get_log_patterns('{app="evm-api"}')

        self.assertIn("error", result)
        self.assertIn("Loki ≥ 3.0", result["error"])

    async def test_limit_applied(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": [
            {"pattern": f"pattern {i}", "volume": i} for i in range(30)
        ]}

        async def fake_get(url, params):
            return mock_resp

        with patch.object(srv._http, "get", side_effect=fake_get):
            result = await srv.loki_get_log_patterns('{app="x"}', limit=5)

        self.assertLessEqual(len(result["patterns"]), 5)


# ---------------------------------------------------------------------------
# grafana_list_annotations / grafana_create_annotation
# ---------------------------------------------------------------------------
class TestGrafanaAnnotations(unittest.IsolatedAsyncioTestCase):
    async def test_list_annotations_passes_from_param(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [{"id": 1, "text": "deploy"}]
        captured = {}

        async def fake_get(url, params, headers, timeout):
            captured["params"] = params
            return mock_resp

        with patch.object(srv._http, "get", side_effect=fake_get):
            result = await srv.grafana_list_annotations(since="1h")

        self.assertIn("from", captured["params"])
        expected_from = int(time.time() - 3600) * 1000
        self.assertAlmostEqual(captured["params"]["from"], expected_from, delta=5000)
        self.assertEqual(result[0]["text"], "deploy")

    async def test_list_annotations_with_tags(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = []
        captured = {}

        async def fake_get(url, params, headers, timeout):
            captured["params"] = params
            return mock_resp

        with patch.object(srv._http, "get", side_effect=fake_get):
            await srv.grafana_list_annotations(tags="deploy,prod")

        self.assertEqual(captured["params"]["tags"], ["deploy", "prod"])

    async def test_create_annotation_sends_correct_payload(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"id": 42, "message": "Annotation added"}
        captured = {}

        async def fake_post(url, json, headers, timeout):
            captured["payload"] = json
            return mock_resp

        with patch.object(srv._http, "post", side_effect=fake_post):
            result = await srv.grafana_create_annotation(
                text="Deployed evm-api v1.4.2",
                tags="deploy,production",
            )

        self.assertEqual(result["id"], 42)
        self.assertEqual(captured["payload"]["text"], "Deployed evm-api v1.4.2")
        self.assertIn("deploy", captured["payload"]["tags"])
        self.assertIn("production", captured["payload"]["tags"])
        self.assertIn("time", captured["payload"])

    async def test_create_annotation_with_dashboard_uid(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"id": 7}
        captured = {}

        async def fake_post(url, json, headers, timeout):
            captured["payload"] = json
            return mock_resp

        with patch.object(srv._http, "post", side_effect=fake_post):
            await srv.grafana_create_annotation(
                text="incident start",
                dashboard_uid="abc123",
                panel_id=5,
            )

        self.assertEqual(captured["payload"]["dashboardUID"], "abc123")
        self.assertEqual(captured["payload"]["panelId"], 5)

    async def test_grafana_token_added_to_auth_header(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = []
        captured = {}

        async def fake_get(url, params, headers, timeout):
            captured["headers"] = headers
            return mock_resp

        with patch.object(srv, "GRAFANA_TOKEN", "my-secret-token"):
            with patch.object(srv._http, "get", side_effect=fake_get):
                await srv.grafana_list_annotations()

        self.assertEqual(captured["headers"]["Authorization"], "Bearer my-secret-token")


class TestResourceRightsizing(unittest.IsolatedAsyncioTestCase):
    """Tests for k8s_resource_rightsizing and its helper functions."""

    # ── Helper: _cpu_to_m ────────────────────────────────────────────────────

    def test_cpu_to_m_millicores(self):
        self.assertEqual(srv._cpu_to_m("500m"), 500.0)

    def test_cpu_to_m_cores(self):
        self.assertEqual(srv._cpu_to_m("2"), 2000.0)

    def test_cpu_to_m_none(self):
        self.assertIsNone(srv._cpu_to_m(None))

    def test_cpu_to_m_empty(self):
        self.assertIsNone(srv._cpu_to_m(""))

    # ── Helper: _mem_to_mib ──────────────────────────────────────────────────

    def test_mem_to_mib_mebibytes(self):
        self.assertAlmostEqual(srv._mem_to_mib("256Mi"), 256.0)

    def test_mem_to_mib_gibibytes(self):
        self.assertAlmostEqual(srv._mem_to_mib("1Gi"), 1024.0)

    def test_mem_to_mib_kibibytes(self):
        self.assertAlmostEqual(srv._mem_to_mib("512Ki"), 0.5)

    def test_mem_to_mib_none(self):
        self.assertIsNone(srv._mem_to_mib(None))

    # ── Helper: _prom_to_map ─────────────────────────────────────────────────

    def test_prom_to_map_basic(self):
        results = [
            {"metric": {"namespace": "services", "container": "evm-api"}, "value": [1, "12.5"]},
            {"metric": {"namespace": "monitoring", "container": "prometheus"}, "value": [1, "101.2"]},
        ]
        m = srv._prom_to_map(results)
        self.assertAlmostEqual(m[("services", "evm-api")], 12.5)
        self.assertAlmostEqual(m[("monitoring", "prometheus")], 101.2)

    def test_prom_to_map_skips_nan(self):
        results = [
            {"metric": {"namespace": "services", "container": "bad"}, "value": [1, "NaN"]},
        ]
        m = srv._prom_to_map(results)
        self.assertNotIn(("services", "bad"), m)

    def test_prom_to_map_skips_missing_labels(self):
        results = [
            {"metric": {"container": "orphan"}, "value": [1, "5.0"]},  # no namespace
            {"metric": {"namespace": "ns"}, "value": [1, "5.0"]},       # no container
        ]
        m = srv._prom_to_map(results)
        self.assertEqual(len(m), 0)

    # ── Tool: k8s_resource_rightsizing ───────────────────────────────────────

    def _make_prom_response(self, ns, container, value):
        return {
            "data": {"result": [
                {"metric": {"namespace": ns, "container": container}, "value": [1, str(value)]}
            ]}
        }

    def _make_pod(self, ns, container, cpu_req, mem_req, cpu_lim, mem_lim, restarts=0):
        pod = MagicMock()
        pod.metadata.namespace = ns
        pod.metadata.name = f"{container}-pod"
        cs = MagicMock()
        cs.name = container
        cs.restart_count = restarts
        pod.status.container_statuses = [cs]
        c = MagicMock()
        c.name = container
        c.resources.requests = {"cpu": cpu_req, "memory": mem_req}
        c.resources.limits = {"cpu": cpu_lim, "memory": mem_lim}
        pod.spec.containers = [c]
        return pod

    async def test_rightsizing_critical_mem_request_below_usage(self):
        """Container with mem request far below average usage → critical."""
        # evm-tx-decoder: 100Mi request, but avg usage 132Mi
        prom_data = {
            "cpu_avg": 16.0, "cpu_p95": 16.0, "cpu_max": 17.5,
            "mem_avg": 132.0, "mem_p95": 156.0, "mem_max": 158.0,
            "restarts": 0,
        }

        async def fake_get(url, **kwargs):
            r = MagicMock()
            q = kwargs["params"]["query"]
            # map query string to key
            if "avg_over_time(rate" in q:
                val = prom_data["cpu_avg"]
            elif "quantile_over_time(0.95,rate" in q:
                val = prom_data["cpu_p95"]
            elif "max_over_time(rate" in q:
                val = prom_data["cpu_max"]
            elif "avg_over_time(container_memory" in q:
                val = prom_data["mem_avg"]
            elif "quantile_over_time(0.95,container_memory" in q:
                val = prom_data["mem_p95"]
            elif "max_over_time(container_memory" in q:
                val = prom_data["mem_max"]
            else:
                val = prom_data["restarts"]
            r.json.return_value = {
                "data": {"result": [
                    {"metric": {"namespace": "services", "container": "evm-tx-decoder"},
                     "value": [1, str(val)]}
                ]}
            }
            return r

        pod = self._make_pod("services", "evm-tx-decoder", "30m", "100Mi", "60m", "256Mi", restarts=16)
        mock_resp = MagicMock()
        mock_resp.items = [pod]

        with patch.object(srv._http, "get", side_effect=fake_get):
            with patch.object(srv._k8s_v1, "list_namespaced_pod", return_value=mock_resp):
                result = await srv.k8s_resource_rightsizing(namespace="services", lookback="7d")

        self.assertEqual(result["summary"]["critical"], 1)
        ctr = result["containers"][0]
        self.assertEqual(ctr["severity"], "critical")
        self.assertEqual(ctr["container"], "evm-tx-decoder")
        # recommended mem request should be ≥ actual p95
        self.assertGreaterEqual(ctr["recommended"]["mem_req_mi"], 150)
        # delta should be negative (current request < recommended)
        self.assertLess(ctr["delta"]["mem_req_mi"], 0)

    async def test_rightsizing_warning_overprovisioned(self):
        """Container with request >> 3× P95 usage → warning."""
        async def fake_get(url, **kwargs):
            r = MagicMock()
            q = kwargs["params"]["query"]
            if "quantile_over_time(0.95,rate" in q:
                val = 5.0   # P95 CPU = 5m
            elif "avg_over_time(rate" in q:
                val = 3.0
            elif "max_over_time(rate" in q:
                val = 6.0
            elif "quantile_over_time(0.95,container_memory" in q:
                val = 10.0   # P95 mem = 10Mi
            elif "avg_over_time(container_memory" in q:
                val = 8.0
            elif "max_over_time(container_memory" in q:
                val = 12.0
            else:
                val = 0.0    # no restarts
            r.json.return_value = {
                "data": {"result": [
                    {"metric": {"namespace": "services", "container": "blockmaze-docs"},
                     "value": [1, str(val)]}
                ]}
            }
            return r

        # request = 100m, P95 = 5m → 20× overprovisioned → warning
        pod = self._make_pod("services", "blockmaze-docs", "100m", "100Mi", "200m", "256Mi", restarts=0)
        mock_resp = MagicMock()
        mock_resp.items = [pod]

        with patch.object(srv._http, "get", side_effect=fake_get):
            with patch.object(srv._k8s_v1, "list_namespaced_pod", return_value=mock_resp):
                result = await srv.k8s_resource_rightsizing(namespace="services", lookback="7d")

        self.assertEqual(result["summary"]["warning"], 1)
        ctr = result["containers"][0]
        self.assertEqual(ctr["severity"], "warning")

    async def test_rightsizing_summary_reclaimable(self):
        """Reclaimable CPU/mem is the sum of positive deltas (over-provisioned containers)."""
        async def fake_get(url, **kwargs):
            r = MagicMock()
            q = kwargs["params"]["query"]
            # Return low usage for everything → over-provisioned
            if "quantile_over_time(0.95,rate" in q:
                val = 5.0
            elif "avg_over_time(rate" in q:
                val = 3.0
            elif "max_over_time(rate" in q:
                val = 6.0
            elif "quantile_over_time(0.95,container_memory" in q:
                val = 10.0
            elif "avg_over_time(container_memory" in q:
                val = 8.0
            elif "max_over_time(container_memory" in q:
                val = 12.0
            else:
                val = 0.0
            r.json.return_value = {
                "data": {"result": [
                    {"metric": {"namespace": "services", "container": "sso-frontend"},
                     "value": [1, str(val)]}
                ]}
            }
            return r

        # request = 200m, P95 = 5m → reclaimable = 200 - 5 = 195m
        pod = self._make_pod("services", "sso-frontend", "200m", "200Mi", "400m", "512Mi")
        mock_resp = MagicMock()
        mock_resp.items = [pod]

        with patch.object(srv._http, "get", side_effect=fake_get):
            with patch.object(srv._k8s_v1, "list_namespaced_pod", return_value=mock_resp):
                result = await srv.k8s_resource_rightsizing(namespace="services", lookback="7d")

        self.assertGreater(result["summary"]["reclaimable_cpu_m"], 100)
        self.assertGreater(result["summary"]["reclaimable_mem_mi"], 100)

    async def test_rightsizing_namespace_filter_applied(self):
        """namespace arg is embedded in Prometheus queries."""
        captured_queries = []

        async def fake_get(url, **kwargs):
            captured_queries.append(kwargs["params"]["query"])
            r = MagicMock()
            r.json.return_value = {"data": {"result": []}}
            return r

        mock_resp = MagicMock()
        mock_resp.items = []

        with patch.object(srv._http, "get", side_effect=fake_get):
            with patch.object(srv._k8s_v1, "list_namespaced_pod", return_value=mock_resp):
                await srv.k8s_resource_rightsizing(namespace="monitoring", lookback="3d")

        for q in captured_queries:
            self.assertIn("monitoring", q)
        for q in captured_queries:
            self.assertIn("3d", q)


if __name__ == "__main__":
    unittest.main()
