"""
Performance Benchmark: get_time_series (single Pod)

Misst drei Komponenten separat:
  1. Full Query   – MATCH Pod via Service + graphobs.data.get_time_series
  2. Graph Match  – nur MATCH-Traversal im Graphen (kein HTTP)
  3. Prometheus   – identischer PromQL-Query direkt an Prometheus (kein Neo4j)

Der Full Query entspricht:
  MATCH (p:Pod)-[]-(n:Service {name: "frontend"})
  WITH p LIMIT 1
  CALL graphobs.data.get_time_series(p, "container_memory_rss", ...)

Die Procedure nutzt den gespeicherten podLabel vom :Prometheus-Knoten
(z.B. metric{pod="<name>"}). Fallback: 8-fach-OR-Kette ueber alle Kandidaten.

Ablauf:
  - 35 Iterationen pro Query
  - Run 1 wird als Cold-Start separat reportet
  - Runs 2-5 sind Warmup (verworfen)
  - Runs 6-35 (= 30 Runs) fuer Mean und Std
  - Rohdaten werden als CSV gespeichert

Nutzung:
  pip install neo4j requests
  python benchmark_get_time_series.py [--service-name frontend]
                                      [--metric container_memory_rss]
                                      [--range -1h]
                                      [--runs 35]
"""

import argparse
import csv
import os
import statistics
import time
from datetime import datetime

import requests
from neo4j import GraphDatabase


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def timer(fn):
    """Runs fn() and returns (result, elapsed_seconds)."""
    start = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - start
    return result, elapsed


def clear_neo4j_query_cache(driver, database):
    """Best-effort: clears the query plan cache between runs."""
    with driver.session(database=database) as s:
        try:
            s.run("CALL db.clearQueryCaches()").consume()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

# Same label candidates as PrometheusTimeSeriesSource.POSSIBLE_IDENTIFIER_LABELS
POSSIBLE_IDENTIFIER_LABELS = [
    "name", "pod", "pod_name", "instance",
    "host", "hostname", "container", "job",
]


def build_promql(metric, pod_name, pod_label):
    """
    Builds the exact PromQL that the procedure uses for a Pod node.
    If podLabel is known: metric{podLabel="name"} (single-label, optimized).
    Else: multi-label OR chain (fallback).
    """
    if pod_label:
        return f'{metric}{{{pod_label}="{pod_name}"}}'
    parts = [f'{metric}{{{lbl}="{pod_name}"}}' for lbl in POSSIBLE_IDENTIFIER_LABELS]
    return " or ".join(parts)


def discover_prometheus_context(driver, database, service_name, metric, prometheus_url):
    """
    Reads Pod name and stored podLabel from the graph.
    Prometheus URL comes from CLI arg (localhost), not the graph.
    """
    with driver.session(database=database) as s:
        result = s.run(
            "MATCH (p:Pod)-[]-(n:Service {name: $svc}) "
            "MATCH (p)-[:HAS_TIME_SERIES]->(prom:Prometheus) "
            "WHERE $metric IN prom.names "
            "RETURN p.name AS podName, prom.podLabel AS podLabel "
            "LIMIT 1",
            svc=service_name,
            metric=metric,
        )
        rec = result.single()
        if not rec:
            raise RuntimeError(
                f"Kein Pod mit HAS_TIME_SERIES->Prometheus fuer "
                f"Service '{service_name}' und Metrik '{metric}' gefunden."
            )
        pod_name = rec["podName"]
        pod_label = rec.get("podLabel")

    prom_url = prometheus_url.rstrip("/")
    promql = build_promql(metric, pod_name, pod_label)
    return prom_url, promql, pod_name, pod_label


# ---------------------------------------------------------------------------
# Benchmark queries
# ---------------------------------------------------------------------------

def run_full_query(driver, database, service_name, metric, range_str):
    """Full Cypher query matching the user's actual query pattern."""
    cypher = (
        "MATCH (p:Pod)-[]-(n:Service {name: $service}) "
        "WITH p LIMIT 1 "
        "CALL graphobs.data.get_time_series(p, $metric, "
        "  {time: toString(datetime()), range: $range}) "
        "YIELD timestamps, values "
        "RETURN size(timestamps) AS points"
    )

    def _run():
        with driver.session(database=database) as s:
            result = s.run(cypher, service=service_name, metric=metric, range=range_str)
            records = list(result)
            result.consume()
            points = records[0]["points"] if records else 0
            return {"series": len(records), "points": points}

    return timer(_run)


def run_graph_match_only(driver, database, service_name):
    """
    Measures only the Cypher portion that runs before the procedure call:
    Find Pod via Service, WITH p LIMIT 1.
    The Prometheus node traversal happens inside the procedure via Java API,
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


def run_prometheus_only(prometheus_url, promql, range_str, session=None):
    """
    Direct HTTP call to Prometheus using the exact same PromQL
    the procedure builds for a Pod node.
    Uses a requests.Session for connection reuse.
    """
    range_clean = range_str.lstrip("-")
    unit = range_clean[-1]
    num = int(range_clean[:-1])
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    range_seconds = num * multipliers.get(unit, 3600)

    now = int(time.time())
    start_ts = now - range_seconds
    step = "60s"

    url = (
        f"{prometheus_url}/api/v1/query_range"
        f"?query={requests.utils.quote(promql)}"
        f"&start={start_ts}&end={now}&step={step}"
    )

    http = session or requests

    def _run():
        resp = http.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("data", {}).get("result", [])
        # Procedure uses only first matching series (results[0]),
        # so count only that to make comparison fair
        n_points = len(results[0].get("values", [])) if results else 0
        return {"series": len(results), "points": n_points}

    return timer(_run)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Benchmark: get_time_series (single Pod)")
    parser.add_argument("--neo4j-uri", default="bolt://localhost:7687")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", default="12345678")
    parser.add_argument("--neo4j-database", default="neo4j")
    parser.add_argument("--service-name", default="frontend")
    parser.add_argument("--prometheus-url", default="http://localhost:9090")
    parser.add_argument("--metric", default="container_memory_rss")
    parser.add_argument("--range", default="-1h", dest="range_str")
    parser.add_argument("--runs", type=int, default=35)
    parser.add_argument("--output-dir", default=".")
    args = parser.parse_args()

    assert args.runs >= 6, "Mindestens 6 Runs (1 cold + 4 warmup + 1 measure)"

    warmup_count = 5
    measure_start = warmup_count

    driver = GraphDatabase.driver(
        args.neo4j_uri,
        auth=(args.neo4j_user, args.neo4j_password),
    )
    driver.verify_connectivity()

    prom_url, promql, pod_name, pod_label = discover_prometheus_context(
        driver, args.neo4j_database, args.service_name, args.metric,
        args.prometheus_url
    )

    print(f"Prometheus URL:  {prom_url}")
    print(f"Service:         {args.service_name}")
    print(f"Pod:             {pod_name}")
    print(f"Stored podLabel: {pod_label or '(none — using multi-label fallback)'}")
    print(f"Metric:          {args.metric}")
    print(f"Range:           {args.range_str}")
    print(f"PromQL:          {promql[:120]}{'...' if len(promql) > 120 else ''}")
    print(f"Runs: {args.runs} (1 cold + {warmup_count - 1} warmup "
          f"+ {args.runs - warmup_count} measured)\n")

    http_session = requests.Session()

    labels = ["full_query", "graph_match", "prometheus"]
    timings = {label: [] for label in labels}
    data_points = []   # time-series points returned per run

    for i in range(args.runs):
        phase = ("COLD" if i == 0
                 else ("WARM" if i < warmup_count
                       else f"RUN {i - warmup_count + 1:02d}"))

        # graph_match and full_query both run at the same cache state:
        # graph_match first (warms 1 Pod + 1 Service), then full_query immediately after
        clear_neo4j_query_cache(driver, args.neo4j_database)

        _, t_graph = run_graph_match_only(
            driver, args.neo4j_database,
            args.service_name,
        )
        timings["graph_match"].append(t_graph)

        # No clearQueryCaches here — full_query sees the same warm state graph_match left
        stats_full, t_full = run_full_query(
            driver, args.neo4j_database,
            args.service_name, args.metric, args.range_str,
        )
        timings["full_query"].append(t_full)

        stats_prom, t_prom = run_prometheus_only(
            prom_url, promql, args.range_str,
            session=http_session,
        )
        timings["prometheus"].append(t_prom)
        data_points.append(stats_prom.get('points', 0))

        pts = stats_prom.get('points', '?')
        print(f"  [{phase:>6s}]  full={t_full*1000:8.2f}ms  "
              f"graph={t_graph*1000:8.2f}ms  "
              f"prom={t_prom*1000:8.2f}ms  pts={pts}")

    http_session.close()

    # --- Write CSV ---
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(args.output_dir, f"benchmark_timeseries_{timestamp_str}.csv")

    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "run", "phase",
            "full_query_ms", "graph_match_ms", "prometheus_ms", "overhead_ms",
            "data_points",
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
            prom = timings['prometheus'][i] * 1000
            pts = data_points[i] if i < len(data_points) else ""
            writer.writerow([
                i + 1, phase,
                f"{full:.4f}", f"{graph:.4f}", f"{prom:.4f}",
                f"{full - graph - prom:.4f}",
                pts,
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
    measured_prom = [t * 1000 for t in timings["prometheus"][measure_start:]]

    mean_full = statistics.mean(measured_full)
    mean_graph = statistics.mean(measured_graph)
    mean_prom = statistics.mean(measured_prom)
    overhead = mean_full - mean_graph - mean_prom

    print(f"\n  Komponentenzerlegung (Mittelwerte):")
    print(f"    Graph-Match:     {mean_graph:8.2f} ms  ({mean_graph/mean_full*100:5.1f}%)")
    print(f"    Prometheus HTTP: {mean_prom:8.2f} ms  ({mean_prom/mean_full*100:5.1f}%)")
    print(f"    Overhead:        {overhead:8.2f} ms  ({overhead/mean_full*100:5.1f}%)")
    print(f"    ---")
    print(f"    Full Query:      {mean_full:8.2f} ms  (100.0%)")
    print()

    measured_pts = data_points[measure_start:]
    if measured_pts:
        print(f"  Datenvolumen (gemessene Runs):")
        print(f"    Zeitschritte:  avg={statistics.mean(measured_pts):.0f}"
              f"  min={min(measured_pts)}"
              f"  max={max(measured_pts)}")


    driver.close()


if __name__ == "__main__":
    main()
