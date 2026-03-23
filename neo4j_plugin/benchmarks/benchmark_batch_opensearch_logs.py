"""
Performance Benchmark: get_logs_for Loop vs get_logs_for_nodes Batch

Vergleicht zwei Varianten, Logs fuer mehrere Knoten abzufragen:
  A) Loop:  Cypher MATCH produziert N Pod-Rows, get_logs_for pro Row
  B) Batch: Pods gesammelt, ein einziger get_logs_for_nodes Aufruf
            (baut intern eine einzige OpenSearch-Query mit OR-Filter)

Zusaetzlich wird gemessen:
  C) Graph-Match:  Nur die Cypher-Traversal (kein Procedure-Aufruf)
  D) OpenSearch:   Direkter POST /_search mit bool.should OR (kein Neo4j)
  → Overhead = Batch - Graph-Match - OpenSearch HTTP

Ablauf:
  - 35 Iterationen (1 cold + 4 warmup + 30 measured)

Nutzung:
  pip install neo4j requests
  python benchmark_batch_opensearch_logs.py [--service-name frontend]
                                             [--index "otel-logs-*"]
                                             [--range -10m]
                                             [--runs 35]
"""

import argparse
import base64
import csv
import json
import os
import statistics
import time
from datetime import datetime, timezone, timedelta

import requests as req_lib
from neo4j import GraphDatabase


# Default pod query field (same as FieldNameResolver.OPENSEARCH_POD_QUERY_FIELD)
DEFAULT_POD_QUERY_FIELD = "resource.k8s.pod.name.keyword"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def timer(fn):
    start = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - start
    return result, elapsed


def clear_neo4j_query_cache(driver, database):
    with driver.session(database=database) as s:
        try:
            s.run("CALL db.clearQueryCaches()").consume()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

DOWNSTREAM_QUERY = (
    "MATCH (src:Service {name: $service})"
    "-[:HAS_OPERATION]->(:Operation)"
    "-[:DEPENDS_ON*]->(:Operation)"
    "<-[:HAS_OPERATION]-(called:Service) "
    "WITH DISTINCT called "
    "MATCH (p:Pod)-[]-(called) "
    "RETURN called.name AS service, p.name AS pod"
)


def discover_downstream(driver, database, service_name):
    with driver.session(database=database) as s:
        result = s.run(DOWNSTREAM_QUERY, service=service_name)
        return [(r["service"], r["pod"]) for r in result]


def discover_opensearch_context(driver, database, index, opensearch_url):
    """Reads stored podField from LogIndex node if available."""
    pod_query_field = DEFAULT_POD_QUERY_FIELD
    with driver.session(database=database) as s:
        result = s.run(
            "MATCH (idx:LogIndex {name: $index}) "
            "RETURN idx.podField AS podField LIMIT 1",
            index=index,
        )
        rec = result.single()
        if rec and rec.get("podField"):
            pod_query_field = rec["podField"] + ".keyword"

    return opensearch_url.rstrip("/"), pod_query_field


# ---------------------------------------------------------------------------
# Benchmark queries
# ---------------------------------------------------------------------------

# limit: 10000 to override the procedure's default of 100,
# the actual result size is controlled by --range
CYPHER_LOOP = (
    "MATCH (src:Service {name: $service})"
    "-[:HAS_OPERATION]->(:Operation)"
    "-[:DEPENDS_ON*]->(:Operation)"
    "<-[:HAS_OPERATION]-(called:Service) "
    "WITH DISTINCT called AS s "
    "MATCH (p:Pod)-[]-(s) "
    "CALL graphobs.data.get_logs_for(p, $index, "
    "  {time: toString(datetime()), range: $range, limit: 10000}) "
    "YIELD timestamp, level, message, source "
    "RETURN s.name AS service, timestamp, level, message, source"
)

CYPHER_BATCH = (
    "MATCH (src:Service {name: $service})"
    "-[:HAS_OPERATION]->(:Operation)"
    "-[:DEPENDS_ON*]->(:Operation)"
    "<-[:HAS_OPERATION]-(called:Service) "
    "WITH DISTINCT called AS s "
    "MATCH (p:Pod)-[]-(s) "
    "WITH collect(p) AS pods "
    "CALL graphobs.data.get_logs_for_nodes(pods, $index, "
    "  {time: toString(datetime()), range: $range, limit: 10000}) "
    "YIELD timestamp, level, message, source "
    "RETURN timestamp, level, message, source"
)

CYPHER_GRAPH_MATCH = (
    "MATCH (src:Service {name: $service})"
    "-[:HAS_OPERATION]->(:Operation)"
    "-[:DEPENDS_ON*]->(:Operation)"
    "<-[:HAS_OPERATION]-(called:Service) "
    "WITH DISTINCT called AS s "
    "MATCH (p:Pod)-[]-(s) "
    "WITH collect(p) AS pods "
    "RETURN size(pods) AS podCount"
)


def run_loop(driver, database, service_name, index, range_str):
    def _run():
        with driver.session(database=database) as s:
            result = s.run(CYPHER_LOOP,
                           service=service_name, index=index,
                           range=range_str)
            records = list(result)
            result.consume()
            return {"logs": len(records)}
    return timer(_run)


def run_batch(driver, database, service_name, index, range_str):
    def _run():
        with driver.session(database=database) as s:
            result = s.run(CYPHER_BATCH,
                           service=service_name, index=index,
                           range=range_str)
            records = list(result)
            result.consume()
            return {"logs": len(records)}
    return timer(_run)


def run_graph_match(driver, database, service_name):
    def _run():
        with driver.session(database=database) as s:
            result = s.run(CYPHER_GRAPH_MATCH, service=service_name)
            records = list(result)
            result.consume()
            return {"pods": records[0]["podCount"] if records else 0}
    return timer(_run)


def run_opensearch_direct(os_url, os_user, os_password, pod_names,
                          pod_query_field, index, range_str, session=None):
    """
    Direct OpenSearch query — same as get_logs_for_nodes:
    single POST with bool.should OR filter for all pods.
    """
    range_clean = range_str.lstrip("-")
    unit = range_clean[-1]
    num = int(range_clean[:-1])
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    range_seconds = num * multipliers.get(unit, 3600)

    now = datetime.now(timezone.utc)
    start_time = now - timedelta(seconds=range_seconds)

    # Build bool.should with one term per pod (same as procedure)
    should_clauses = [
        {"term": {pod_query_field: pod}} for pod in pod_names
    ]

    query_body = {
        "size": 10000,
        "sort": [{"@timestamp": {"order": "desc"}}],
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "@timestamp": {
                                "gte": start_time.isoformat(),
                                "lte": now.isoformat(),
                            }
                        }
                    },
                    {
                        "bool": {
                            "should": should_clauses,
                            "minimum_should_match": 1,
                        }
                    },
                ]
            }
        },
    }

    url = f"{os_url}/{index}/_search"

    headers = {"Content-Type": "application/json"}
    if os_user:
        cred = base64.b64encode(
            f"{os_user}:{os_password or ''}".encode()
        ).decode()
        headers["Authorization"] = f"Basic {cred}"

    http = session or req_lib

    def _run():
        resp = http.post(
            url, json=query_body, headers=headers,
            timeout=30, verify=False,
        )
        resp.raise_for_status()
        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        return {"logs": len(hits)}

    return timer(_run)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark: get_logs_for loop vs get_logs_for_nodes batch"
    )
    parser.add_argument("--neo4j-uri", default="bolt://localhost:7687")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", default="12345678")
    parser.add_argument("--neo4j-database", default="neo4j")
    parser.add_argument("--opensearch-url", default="http://localhost:9202")
    parser.add_argument("--opensearch-user", default="")
    parser.add_argument("--opensearch-password", default="")
    parser.add_argument("--service-name", default="frontend")
    parser.add_argument("--index", default="otel-logs-*")
    parser.add_argument("--range", default="-1m", dest="range_str")
    parser.add_argument("--runs", type=int, default=35)
    parser.add_argument("--output-dir", default=".")
    args = parser.parse_args()

    assert args.runs >= 6

    warmup_count = 5
    measure_start = warmup_count

    # Suppress InsecureRequestWarning for self-signed certs
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    driver = GraphDatabase.driver(
        args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password)
    )
    driver.verify_connectivity()

    downstream = discover_downstream(driver, args.neo4j_database, args.service_name)
    services = sorted(set(s for s, _ in downstream))
    pod_names = sorted(set(p for _, p in downstream))

    os_url, pod_query_field = discover_opensearch_context(
        driver, args.neo4j_database, args.index, args.opensearch_url,
    )
    os_user = args.opensearch_user or None
    os_password = args.opensearch_password or None

    print(f"Service:      {args.service_name}")
    print(f"Downstream:   {services}")
    print(f"Pods:         {len(pod_names)}")
    print(f"Index:        {args.index}")
    print(f"Range:        {args.range_str}")
    print(f"OpenSearch:   {os_url}")
    print(f"Pod field:    {pod_query_field}")
    print(f"Runs:         {args.runs} (1 cold + {warmup_count - 1} warmup "
          f"+ {args.runs - warmup_count} measured)\n")

    http_session = req_lib.Session()

    labels = ["loop", "batch", "graph_match", "os_http"]
    timings = {label: [] for label in labels}

    for i in range(args.runs):
        phase = ("COLD" if i == 0
                 else ("WARM" if i < warmup_count
                       else f"RUN {i - warmup_count + 1:02d}"))

        clear_neo4j_query_cache(driver, args.neo4j_database)

        stats_loop, t_loop = run_loop(
            driver, args.neo4j_database,
            args.service_name, args.index, args.range_str,
        )
        timings["loop"].append(t_loop)

        # graph_match and batch both run warm (after loop), same cache state
        clear_neo4j_query_cache(driver, args.neo4j_database)
        _, t_graph = run_graph_match(
            driver, args.neo4j_database, args.service_name,
        )
        timings["graph_match"].append(t_graph)

        clear_neo4j_query_cache(driver, args.neo4j_database)

        stats_batch, t_batch = run_batch(
            driver, args.neo4j_database,
            args.service_name, args.index, args.range_str,
        )
        timings["batch"].append(t_batch)

        _, t_os = run_opensearch_direct(
            os_url, os_user, os_password, pod_names,
            pod_query_field, args.index, args.range_str,
            session=http_session,
        )
        timings["os_http"].append(t_os)

        extra = ""
        if i == measure_start:
            extra = (f"  loop={stats_loop.get('logs', '?')} logs"
                     f"  batch={stats_batch.get('logs', '?')} logs")

        print(f"  [{phase:>6s}]  loop={t_loop*1000:8.2f}ms  "
              f"batch={t_batch*1000:8.2f}ms  "
              f"graph={t_graph*1000:8.2f}ms  "
              f"os={t_os*1000:8.2f}ms{extra}")

    http_session.close()

    # --- Write CSV ---
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(
        args.output_dir, f"benchmark_batch_opensearch_{timestamp_str}.csv"
    )

    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["run", "phase", "loop_ms", "batch_ms", "graph_match_ms",
                         "os_http_ms", "overhead_batch_ms"])
        for i in range(args.runs):
            if i == 0:
                phase = "cold"
            elif i < warmup_count:
                phase = "warmup"
            else:
                phase = "measure"
            loop_ms = timings['loop'][i] * 1000
            batch_ms = timings['batch'][i] * 1000
            graph_ms = timings['graph_match'][i] * 1000
            os_ms = timings['os_http'][i] * 1000
            writer.writerow([
                i + 1, phase,
                f"{loop_ms:.4f}", f"{batch_ms:.4f}", f"{graph_ms:.4f}",
                f"{os_ms:.4f}", f"{batch_ms - graph_ms - os_ms:.4f}",
            ])

    print(f"\nRohdaten: {csv_path}")

    # --- Statistics ---
    print("\n" + "=" * 70)
    print(f"  {'Variante':>12s}  {'Cold (ms)':>12s}  {'Mean (ms)':>12s}  {'Std (ms)':>12s}")
    print("-" * 70)

    for label in labels:
        cold = timings[label][0] * 1000
        measured = [t * 1000 for t in timings[label][measure_start:]]
        mean = statistics.mean(measured)
        std = statistics.stdev(measured) if len(measured) > 1 else 0.0
        print(f"  {label:>12s}  {cold:12.2f}  {mean:12.2f}  {std:12.2f}")

    print("=" * 70)

    mean_loop = statistics.mean([t * 1000 for t in timings["loop"][measure_start:]])
    mean_batch = statistics.mean([t * 1000 for t in timings["batch"][measure_start:]])
    mean_graph = statistics.mean([t * 1000 for t in timings["graph_match"][measure_start:]])
    mean_os = statistics.mean([t * 1000 for t in timings["os_http"][measure_start:]])
    speedup = mean_loop / mean_batch if mean_batch > 0 else float("inf")

    n_pods = len(pod_names)
    overhead_batch = mean_batch - mean_graph - mean_os
    saving = mean_loop - mean_batch

    print(f"\n  Komponentenzerlegung Batch (Mittelwerte):")
    print(f"    Graph-Match:      {mean_graph:8.2f} ms  ({mean_graph/mean_batch*100:5.1f}%)")
    print(f"    OpenSearch HTTP:  {mean_os:8.2f} ms  ({mean_os/mean_batch*100:5.1f}%)")
    print(f"    Overhead:         {overhead_batch:8.2f} ms  ({overhead_batch/mean_batch*100:5.1f}%)")
    print(f"    ---")
    print(f"    Batch (gesamt):   {mean_batch:8.2f} ms  (100.0%)")
    print(f"\n  Speedup (batch vs loop): {speedup:.2f}x")
    print(f"  Loop:           {mean_loop:8.2f} ms  ({mean_loop/n_pods:.2f} ms/pod)")
    print(f"  Batch:          {mean_batch:8.2f} ms  ({mean_batch/n_pods:.2f} ms/pod)")
    print(f"  Einsparung:     {saving:8.2f} ms  ({saving/mean_loop*100:.1f}%)")
    print(f"  Pods:           {n_pods}")

    driver.close()


if __name__ == "__main__":
    main()
