"""
Performance Benchmark: OpenSearch Log Query (single Pod)

Misst drei Komponenten separat:
  1. Full Query    – MATCH Pod via Service + graphobs.data.get_logs_for
  2. Graph Match   – nur MATCH: Pod-Lookup + OpenSearch config aus Graph
  3. OpenSearch    – direkter POST /_search an OpenSearch (kein Neo4j)

Full Query:
  MATCH (p:Pod)-[]-(s:Service {name: "ad"})
  WITH p LIMIT 1
  CALL graphobs.data.get_logs_for(p, $index,
       {time: toString(datetime()), range: "-10m", limit: 100})
  YIELD timestamp, level, message, source
  RETURN timestamp, level, message, source

Die Procedure nutzt FieldNameResolver, das optional gespeicherte Feld-Mappings
vom :LogIndex-Knoten laedt. Fallback: resource.k8s.pod.name.keyword (hardcodiert).

Ablauf:
  - 35 Iterationen pro Query
  - Run 1 = Cold-Start
  - Runs 2-5 = Warmup
  - Runs 6-35 = 30 gemessene Runs

Nutzung:
  pip install neo4j requests
  python benchmark_opensearch_logs.py [--service-name frontend]
                                      [--index "otel-logs-*"]
                                      [--range -10m]
                                      [--limit 100]
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

import requests
from neo4j import GraphDatabase


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


# Default pod query field (same as FieldNameResolver.OPENSEARCH_POD_QUERY_FIELD)
DEFAULT_POD_QUERY_FIELD = "resource.k8s.pod.name.keyword"


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def discover_opensearch_context(driver, database, service_name, index,
                                opensearch_url, opensearch_user, opensearch_password):
    """
    Reads first Pod name for the service and stored field mappings from the graph.
    """
    # 1) First pod for service
    with driver.session(database=database) as s:
        result = s.run(
            "MATCH (p:Pod)-[]-(n:Service {name: $svc}) "
            "RETURN p.name AS podName LIMIT 1",
            svc=service_name,
        )
        rec = result.single()
        if not rec:
            raise RuntimeError(f"Kein Pod fuer Service '{service_name}' gefunden.")
        pod_name = rec["podName"]

    # 2) Stored pod field from LogIndex (if available)
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

    return opensearch_url.rstrip("/"), opensearch_user, opensearch_password, pod_name, pod_query_field


# ---------------------------------------------------------------------------
# Benchmark queries
# ---------------------------------------------------------------------------

def run_full_query(driver, database, service_name, index, range_str, limit):
    """Full Cypher query calling the get_logs_for procedure."""
    cypher = (
        "MATCH (p:Pod)-[]-(s:Service {name: $service}) "
        "WITH p LIMIT 1 "
        "CALL graphobs.data.get_logs_for(p, $index, "
        "  {time: toString(datetime()), range: $range, limit: $limit}) "
        "YIELD timestamp, level, message, source "
        "RETURN timestamp, level, message, source"
    )

    def _run():
        with driver.session(database=database) as s:
            result = s.run(
                cypher,
                service=service_name, index=index,
                range=range_str, limit=limit,
            )
            records = list(result)
            result.consume()
            return {"logs": len(records)}

    return timer(_run)


def run_graph_match_only(driver, database, service_name):
    """
    Measures only the Cypher traversal: find Pod via Service.
    The OpenSearch/LogIndex lookup happens inside the procedure via Java API,
    not in Cypher, so it must NOT be included here.
    """
    cypher = (
        "MATCH (p:Pod)-[]-(n:Service {name: $service}) "
        "WITH p LIMIT 1 "
        "RETURN p.name AS pod"
    )

    def _run():
        with driver.session(database=database) as s:
            result = s.run(cypher, service=service_name)
            records = list(result)
            result.consume()
            return records

    return timer(_run)


def run_opensearch_only(os_url, os_user, os_password, pod_name,
                        pod_query_field, index, range_str, limit,
                        session=None):
    """
    Direct POST to OpenSearch /_search, bypassing Neo4j.
    Uses the same pod field that the procedure resolves
    (stored from LogIndex or hardcoded default).
    """
    range_clean = range_str.lstrip("-")
    unit = range_clean[-1]
    num = int(range_clean[:-1])
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    range_seconds = num * multipliers.get(unit, 3600)

    now = datetime.now(timezone.utc)
    start_time = now - timedelta(seconds=range_seconds)

    query_body = {
        "size": limit,
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
                        "term": {
                            pod_query_field: pod_name
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

    http = session or requests

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
        description="Benchmark: OpenSearch log query (single Pod)"
    )
    parser.add_argument("--neo4j-uri", default="bolt://localhost:7687")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", default="12345678")
    parser.add_argument("--neo4j-database", default="neo4j")
    parser.add_argument("--opensearch-url", default="http://localhost:9202")
    parser.add_argument("--opensearch-user", default="")
    parser.add_argument("--opensearch-password", default="")
    parser.add_argument("--service-name", default="ad")
    parser.add_argument("--index", default="otel-logs-*")
    parser.add_argument("--range", default="-10m", dest="range_str")
    parser.add_argument("--limit", type=int, default=100)
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

    os_url, os_user, os_password, pod_name, pod_query_field = (
        discover_opensearch_context(
            driver, args.neo4j_database, args.service_name, args.index,
            args.opensearch_url, args.opensearch_user or None,
            args.opensearch_password or None,
        )
    )

    print(f"OpenSearch URL:  {os_url}")
    print(f"OpenSearch Auth: {'yes' if os_user else 'no'}")
    print(f"Service:         {args.service_name}")
    print(f"Pod:             {pod_name}")
    print(f"Pod query field: {pod_query_field}")
    print(f"Index:           {args.index}")
    print(f"Range:           {args.range_str}")
    print(f"Limit:           {args.limit}")
    print(f"Runs:            {args.runs} (1 cold + {warmup_count - 1} warmup "
          f"+ {args.runs - warmup_count} measured)\n")

    http_session = requests.Session()

    labels = ["full_query", "graph_match", "opensearch"]
    timings = {label: [] for label in labels}
    data_logs = []   # log entries returned per run

    for i in range(args.runs):
        phase = ("COLD" if i == 0
                 else ("WARM" if i < warmup_count
                       else f"RUN {i - warmup_count + 1:02d}"))

        # graph_match and full_query run back-to-back without cache clear —
        # both see the same warm state for the same 1 Pod + 1 Service nodes
        clear_neo4j_query_cache(driver, args.neo4j_database)

        _, t_graph = run_graph_match_only(
            driver, args.neo4j_database, args.service_name,
        )
        timings["graph_match"].append(t_graph)

        stats_full, t_full = run_full_query(
            driver, args.neo4j_database,
            args.service_name, args.index, args.range_str, args.limit,
        )
        timings["full_query"].append(t_full)

        stats_os, t_os = run_opensearch_only(
            os_url, os_user, os_password, pod_name,
            pod_query_field, args.index,
            args.range_str, args.limit,
            session=http_session,
        )
        timings["opensearch"].append(t_os)
        data_logs.append(stats_os.get('logs', 0))

        n_logs = stats_os.get('logs', '?')
        print(f"  [{phase:>6s}]  full={t_full*1000:8.2f}ms  "
              f"graph={t_graph*1000:8.2f}ms  "
              f"os={t_os*1000:8.2f}ms  logs={n_logs}")

    http_session.close()

    # --- Write CSV ---
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(
        args.output_dir, f"benchmark_opensearch_{timestamp_str}.csv"
    )

    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "run", "phase",
            "full_query_ms", "graph_match_ms", "opensearch_ms", "overhead_ms",
            "data_logs",
        ])
        for i in range(args.runs):
            if i == 0:
                phase = "cold"
            elif i < warmup_count:
                phase = "warmup"
            else:
                phase = "measure"
            full = timings['full_query'][i] * 1000
            graph = timings['graph_match'][i] * 1000
            os_t = timings['opensearch'][i] * 1000
            logs_v = data_logs[i] if i < len(data_logs) else ""
            writer.writerow([
                i + 1, phase,
                f"{full:.4f}", f"{graph:.4f}", f"{os_t:.4f}",
                f"{full - graph - os_t:.4f}",
                logs_v,
            ])

    print(f"\nRohdaten: {csv_path}")

    # --- Statistics ---
    print("\n" + "=" * 70)
    print(f"{'':>20s}  {'Cold (ms)':>12s}  {'Mean (ms)':>12s}  {'Std (ms)':>12s}")
    print("-" * 70)

    for label in labels:
        cold = timings[label][0] * 1000
        measured = [t * 1000 for t in timings[label][measure_start:]]
        mean = statistics.mean(measured)
        std = statistics.stdev(measured) if len(measured) > 1 else 0.0
        print(f"  {label:>18s}  {cold:12.2f}  {mean:12.2f}  {std:12.2f}")

    print("=" * 70)

    measured_full = [t * 1000 for t in timings["full_query"][measure_start:]]
    measured_graph = [t * 1000 for t in timings["graph_match"][measure_start:]]
    measured_os = [t * 1000 for t in timings["opensearch"][measure_start:]]

    mean_full = statistics.mean(measured_full)
    mean_graph = statistics.mean(measured_graph)
    mean_os = statistics.mean(measured_os)
    overhead = mean_full - mean_graph - mean_os

    print(f"\n  Komponentenzerlegung (Mittelwerte):")
    print(f"    Graph-Match:     {mean_graph:8.2f} ms  ({mean_graph/mean_full*100:5.1f}%)")
    print(f"    OpenSearch HTTP: {mean_os:8.2f} ms  ({mean_os/mean_full*100:5.1f}%)")
    print(f"    Overhead:        {overhead:8.2f} ms  ({overhead/mean_full*100:5.1f}%)")
    print(f"    ---")
    print(f"    Full Query:      {mean_full:8.2f} ms  (100.0%)")

    measured_logs = data_logs[measure_start:]
    if measured_logs:
        print(f"\n  Datenvolumen (gemessene Runs):")
        print(f"    Log-Eintraege: avg={statistics.mean(measured_logs):.0f}"
              f"  min={min(measured_logs)}"
              f"  max={max(measured_logs)}")

    driver.close()


if __name__ == "__main__":
    main()
