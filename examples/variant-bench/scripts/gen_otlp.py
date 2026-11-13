#!/usr/bin/env python3
"""Generate synthetic OTLP/JSON trace payloads for the variant benchmark.

Usage: uv run gen_otlp.py <out-dir> <total-spans> [spans-per-file]

Spans carry 14 attributes (8 http/db/net keys, 6 app keys) so the attribute map is
realistically wide. Values are drawn from fixed-size pools (deterministic seed) so
every layout is fed identical input.
"""
import json
import random
import sys
import time
from pathlib import Path

out = Path(sys.argv[1])
total = int(sys.argv[2])
per_file = int(sys.argv[3]) if len(sys.argv) > 3 else 10_000
out.mkdir(parents=True, exist_ok=True)
rng = random.Random(42)

ROUTES = [f"/api/{r}" for r in (
    "cart", "checkout", "orders", "orders/{id}", "users/{id}", "search", "items/{id}",
    "payments", "inventory", "health", "login", "logout", "recommendations",
    "shipping", "reviews", "profile", "notifications", "reports", "export", "admin")]
METHODS = ["GET"] * 6 + ["POST"] * 3 + ["PUT", "DELETE"]
STATUS = [200] * 90 + [201] * 3 + [301, 304, 400, 401, 404, 404, 429, 500, 500, 502, 503]
SERVICES = [("checkout", "shop"), ("cart", "shop"), ("search", "shop"),
            ("billing", "finance"), ("inventory", "ops")]
TENANTS = [f"tenant-{i:03d}" for i in range(200)]
HOSTS = [f"host-{i:03d}" for i in range(50)]
DBS = ["postgres", "redis", "mysql", "none", "none", "none"]


def attr(k, v):
    if isinstance(v, bool):
        return {"key": k, "value": {"boolValue": v}}
    if isinstance(v, int):
        return {"key": k, "value": {"intValue": str(v)}}
    if isinstance(v, float):
        return {"key": k, "value": {"doubleValue": v}}
    return {"key": k, "value": {"stringValue": v}}


# The server batches spans by (service, first-span-time bucket). All spans share one narrow
# window (50 ms, aligned to a bucket boundary) so a whole run lands in one batch per service
# and the output files are large, as they would be in production.
base_ns = time.time_ns() // 60_000_000 * 60_000_000
n = 0
file_no = 0
while n < total:
    count = min(per_file, total - n)
    by_service = {}
    for i in range(count):
        svc = rng.choice(SERVICES)
        route, method, status = rng.choice(ROUTES), rng.choice(METHODS), rng.choice(STATUS)
        dur = int(rng.lognormvariate(16, 1))
        start = base_ns + rng.randrange(50_000_000)
        attrs = [
            attr("http.request.method", method),
            attr("http.route", route),
            attr("http.response.status_code", status),
            attr("url.scheme", "https"),
            attr("server.address", f"{svc[0]}.internal"),
            attr("net.peer.ip", f"10.{rng.randrange(256)}.{rng.randrange(256)}.{rng.randrange(256)}"),
            attr("db.system", rng.choice(DBS)),
            attr("user_agent.original", "Mozilla/5.0 (compatible; bench/1.0)"),
            attr("app.tenant", rng.choice(TENANTS)),
            attr("app.latency_ms", dur // 1_000_000),
            attr("app.cache_hit", rng.random() < 0.7),
            attr("app.retry_count", rng.choice([0] * 9 + [1, 2])),
            attr("app.request_size", rng.randrange(100, 50_000)),
            attr("app.region", rng.choice(["us-east-1", "us-west-2", "eu-west-1"])),
        ]
        span = {
            "traceId": f"{rng.getrandbits(128):032x}",
            "spanId": f"{rng.getrandbits(64):016x}",
            "name": f"{method} {route}",
            "kind": 2,
            "startTimeUnixNano": str(start),
            "endTimeUnixNano": str(start + dur),
            "attributes": attrs,
            "status": {"code": 2 if status >= 500 else 1},
        }
        by_service.setdefault(svc, []).append(span)
    resource_spans = []
    for (name, ns), spans in by_service.items():
        resource_spans.append({
            "resource": {"attributes": [
                attr("service.name", name),
                attr("service.namespace", ns),
                attr("deployment.environment", "production"),
                attr("host.name", rng.choice(HOSTS)),
            ]},
            "scopeSpans": [{"scope": {"name": "bench"}, "spans": spans}],
        })
    (out / f"traces-{file_no:05d}.json").write_text(json.dumps({"resourceSpans": resource_spans}))
    n += count
    file_no += 1
print(f"wrote {file_no} files, {n} spans to {out}")
