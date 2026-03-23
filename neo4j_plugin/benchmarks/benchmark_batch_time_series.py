"""
Performance Benchmark: get_time_series Loop vs Batch

Vergleicht zwei Varianten, Zeitreihen fuer mehrere Knoten abzufragen:
  A) Loop:  Cypher MATCH produziert N Rows, Procedure wird pro Row aufgerufen
  B) Batch: Knoten werden gesammelt, eine einzige Batch-Procedure wird aufgerufen

Zusaetzlich wird gemessen:
  C) Graph-Match: Nur die Cypher-Traversal (kein Procedure-Aufruf)
  D) Prometheus:  Direkter HTTP-Call mit Regex-Query (kein Neo4j)
  → Overhead = Batch - Graph-Match - Prometheus HTTP

Ablauf:
  - 35 Iterationen (1 cold + 4 warmup + 30 measured)

Nutzung:
  pip install neo4j requests
  python benchmark_batch_time_series.py [--service-name frontend]
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

import requests as req_lib
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
    """Returns list of (service_name, pod_name) tuples for downstream services."""
    with driver.session(database=database) as s:
        result = s.run(DOWNSTREAM_QUERY, service=service_name)
        rows = [(r["service"], r["pod"]) for r in result]
    return rows


def discover_prometheus_context(driver, database, pod_names, metric, prometheus_url):
    """Reads Prometheus URL and podLabel from graph for the batch regex query."""
    with driver.session(database=database) as s:
        result = s.run(
            "MATCH (p:Pod)-[:HAS_TIME_SERIES]->(prom:Prometheus) "
            "WHERE p.name IN $pods AND $metric IN prom.names "
            "RETURN prom.url AS url, prom.podLabel AS podLabel "
            "LIMIT 1",
            pods=pod_names, metric=metric,
        )
        rec = result.single()

    prom_url = prometheus_url.rstrip("/")
    pod_label = rec.get("podLabel") if rec else None
    return prom_url, pod_label


# ---------------------------------------------------------------------------
# Benchmark queries
# ---------------------------------------------------------------------------

CYPHER_LOOP = (
    "MATCH (src:Service {name: $service})"
    "-[:HAS_OPERATION]->(:Operation)"
    "-[:DEPENDS_ON*]->(:Operation)"
    "<-[:HAS_OPERATION]-(called:Service) "
    "WITH DISTINCT called AS s "
    "MATCH (p:Pod)-[]-(s) "
    "CALL graphobs.data.get_time_series(p, $metric, "
    "  {time: toString(datetime()), range: $range}) "
    "YIELD timestamps, values "
    "RETURN s.name AS service, size(timestamps) AS points"
)

CYPHER_BATCH = (
    "MATCH (src:Service {name: $service})"
    "-[:HAS_OPERATION]->(:Operation)"
    "-[:DEPENDS_ON*]->(:Operation)"
    "<-[:HAS_OPERATION]-(called:Service) "
    "WITH DISTINCT called AS s "
    "MATCH (p:Pod)-[]-(s) "
    "WITH collect(p) AS pods "
    "CALL graphobs.data.get_time_series_batch(pods, $metric, "
    "  {time: toString(datetime()), range: $range}) "
    "YIELD timestamps, values "
    "RETURN size(timestamps) AS points"
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


def run_loop(driver, database, service_name, metric, range_str):
    def _run():
        with driver.session(database=database) as s:
            result = s.run(CYPHER_LOOP,
                           service=service_name, metric=metric, range=range_str)
            records = list(result)
            result.consume()
            return {"series": len(records)}
    return timer(_run)


def run_batch(driver, database, service_name, metric, range_str):
    def _run():
        with driver.session(database=database) as s:
            result = s.run(CYPHER_BATCH,
                           service=service_name, metric=metric, range=range_str)
            records = list(result)
            result.consume()
            return {"series": len(records)}
    return timer(_run)


def run_graph_match(driver, database, service_name):
    def _run():
        with driver.session(database=database) as s:
            result = s.run(CYPHER_GRAPH_MATCH, service=service_name)
            records = list(result)
            result.consume()
            return {"pods": records[0]["podCount"] if records else 0}
    return timer(_run)


def run_prometheus_direct(prom_url, metric, pod_names, pod_label,
                          range_str, session=None):
    """
    Direct Prometheus query_range with regex filter — same as fetchBatch():
    metric{podLabel=~"pod1|pod2|..."}
    """
    regex = "|".join(n.replace(".", "\\.") for n in pod_names)
    label_key = pod_label or "pod"
    promql = f'{metric}{{{label_key}=~"{regex}"}}'

    range_clean = range_str.lstrip("-")
    unit = range_clean[-1]
    num = int(range_clean[:-1])
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    range_seconds = num * multipliers.get(unit, 3600)

    now = int(time.time())
    start_ts = now - range_seconds

    url = (
        f"{prom_url}/api/v1/query_range"
        f"?query={req_lib.utils.quote(promql)}"
        f"&start={start_ts}&end={now}&step=60s"
    )

    http = session or req_lib

    def _run():
        resp = http.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("data", {}).get("result", [])
        return {"series": len(results)}

    return timer(_run)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark: get_time_series loop vs batch"
    )
    parser.add_argument("--neo4j-uri", default="bolt://localhost:7687")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", default="12345678")
    parser.add_argument("--neo4j-database", default="neo4j")
    parser.add_argument("--prometheus-url", default="http://localhost:9090")
    parser.add_argument("--service-name", default="frontend")
    parser.add_argument("--metric", default="container_memory_rss")
    parser.add_argument("--range", default="-1h", dest="range_str")
    parser.add_argument("--runs", type=int, default=35)
    parser.add_argument("--output-dir", default=".")
    args = parser.parse_args()

    assert args.runs >= 6

    warmup_count = 5
    measure_start = warmup_count

    driver = GraphDatabase.driver(
        args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password)
    )
    driver.verify_connectivity()

    downstream = discover_downstream(driver, args.neo4j_database, args.service_name)
    services = sorted(set(s for s, _ in downstream))
    pod_names = sorted(set(p for _, p in downstream))

    prom_url, pod_label = discover_prometheus_context(
        driver, args.neo4j_database, pod_names, args.metric, args.prometheus_url,
    )

    print(f"Service:      {args.service_name}")
    print(f"Downstream:   {services}")
    print(f"Pods:         {len(pod_names)}")
    print(f"Metric:       {args.metric}")
    print(f"Range:        {args.range_str}")
    print(f"Prometheus:   {prom_url}")
    print(f"Pod label:    {pod_label or '(none — fallback to pod)'}")
    print(f"Runs:         {args.runs} (1 cold + {warmup_count - 1} warmup "
          f"+ {args.runs - warmup_count} measured)\n")

    http_session = req_lib.Session()

    labels = ["loop", "batch", "graph_match", "prom_http"]
    timings = {label: [] for label in labels}

    for i in range(args.runs):
        phase = ("COLD" if i == 0
                 else ("WARM" if i < warmup_count
                       else f"RUN {i - warmup_count + 1:02d}"))

        clear_neo4j_query_cache(driver, args.neo4j_database)
        stats_loop, t_loop = run_loop(
            driver, args.neo4j_database,
            args.service_name, args.metric, args.range_str,
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
            args.service_name, args.metric, args.range_str,
        )
        timings["batch"].append(t_batch)

        _, t_prom = run_prometheus_direct(
            prom_url, args.metric, pod_names, pod_label,
            args.range_str, session=http_session,
        )
        timings["prom_http"].append(t_prom)

        extra = ""
        if i == measure_start:
            extra = (f"  loop={stats_loop.get('series', '?')} series"
                     f"  batch={stats_batch.get('series', '?')} series")

        print(f"  [{phase:>6s}]  loop={t_loop*1000:8.2f}ms  "
              f"batch={t_batch*1000:8.2f}ms  "
              f"graph={t_graph*1000:8.2f}ms  "
              f"prom={t_prom*1000:8.2f}ms{extra}")

    http_session.close()

    # --- Write CSV ---
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(
        args.output_dir, f"benchmark_batch_timeseries_{timestamp_str}.csv"
    )

    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["run", "phase", "loop_ms", "batch_ms", "graph_match_ms",
                         "prom_http_ms", "overhead_batch_ms"])
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
            prom_ms = timings['prom_http'][i] * 1000
            writer.writerow([
                i + 1, phase,
                f"{loop_ms:.4f}", f"{batch_ms:.4f}", f"{graph_ms:.4f}",
                f"{prom_ms:.4f}", f"{batch_ms - graph_ms - prom_ms:.4f}",
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
    mean_prom = statistics.mean([t * 1000 for t in timings["prom_http"][measure_start:]])
    speedup = mean_loop / mean_batch if mean_batch > 0 else float("inf")

    n_pods = len(pod_names)
    overhead_batch = mean_batch - mean_graph - mean_prom
    saving = mean_loop - mean_batch

    print(f"\n  Komponentenzerlegung Batch (Mittelwerte):")
    print(f"    Graph-Match:      {mean_graph:8.2f} ms  ({mean_graph/mean_batch*100:5.1f}%)")
    print(f"    Prometheus HTTP:  {mean_prom:8.2f} ms  ({mean_prom/mean_batch*100:5.1f}%)")
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
