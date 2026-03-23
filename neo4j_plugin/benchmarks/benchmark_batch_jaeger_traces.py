"""
Performance Benchmark: get_trace_for_node Loop vs get_traces_for_nodes Batch

Vergleicht zwei Varianten, Traces fuer mehrere Pods abzufragen:
  A) Loop:  Cypher MATCH produziert N Pod-Rows, get_trace_for_node pro Row
  B) Batch: Pods gesammelt, ein einziger get_traces_for_nodes Aufruf

Zusaetzlich wird gemessen:
  C) Graph-Match: Nur die Cypher-Traversal (kein Procedure-Aufruf)
  D) Jaeger HTTP:  Direkte HTTP-Calls an Jaeger (1 pro Service, kein Neo4j)
  → Overhead = Batch - Graph-Match - Jaeger HTTP

Ablauf:
  - 35 Iterationen (1 cold + 4 warmup + 30 measured)
  - 1s delay zwischen Iterationen (Jaeger OOM-Schutz)

Nutzung:
  pip install neo4j requests
  python benchmark_batch_jaeger_traces.py [--service-name frontend]
                                           [--range -10m]
                                           [--limit 20]
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


def load_jaeger_context_from_graph(driver, database, fallback_url):
    """Reads baseUrl and podTagKey from the Jaeger node in the graph."""
    try:
        with driver.session(database=database) as s:
            rec = s.run(
                "MATCH (j:Jaeger) "
                "RETURN j.baseUrl AS baseUrl, j.podTagKey AS podTagKey "
                "LIMIT 1"
            ).single()
            if rec and rec.get("baseUrl"):
                return rec["baseUrl"].rstrip("/"), rec.get("podTagKey")
    except Exception:
        pass
    return fallback_url.rstrip("/"), None


def wait_for_jaeger(jaeger_url, max_wait=120, interval=5):
    """Polls Jaeger health endpoint until it responds."""
    health_url = jaeger_url.rstrip("/").replace("/api", "")
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            resp = requests.get(health_url, timeout=5)
            if resp.status_code < 500:
                return True
        except (requests.ConnectionError, requests.Timeout):
            pass
        print(f"    Warte auf Jaeger...")
        time.sleep(interval)
    return False


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
    "CALL graphobs.data.get_trace_for_node(p, "
    "  {time: toString(datetime()), range: $range, limit: $limit}) "
    "YIELD traceId, service, spanCount, startTime, durationMs "
    "RETURN DISTINCT traceId, service, spanCount, startTime, durationMs"
)

CYPHER_BATCH = (
    "MATCH (src:Service {name: $service})"
    "-[:HAS_OPERATION]->(:Operation)"
    "-[:DEPENDS_ON*]->(:Operation)"
    "<-[:HAS_OPERATION]-(called:Service) "
    "WITH DISTINCT called AS s "
    "MATCH (p:Pod)-[]-(s) "
    "WITH collect(p) AS pods "
    "CALL graphobs.data.get_traces_for_nodes(pods, "
    "  {time: toString(datetime()), range: $range, limit: $limit}) "
    "YIELD traceId, service, spanCount, startTime, durationMs "
    "RETURN traceId, service, spanCount, startTime, durationMs"
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


def run_graph_match(driver, database, service_name):
    def _run():
        with driver.session(database=database) as s:
            result = s.run(CYPHER_GRAPH_MATCH, service=service_name)
            records = list(result)
            result.consume()
            return {"pods": records[0]["podCount"] if records else 0}
    return timer(_run)


def run_loop(driver, database, service_name, range_str, limit):
    def _run():
        with driver.session(database=database) as s:
            result = s.run(CYPHER_LOOP,
                           service=service_name, range=range_str, limit=limit)
            records = list(result)
            result.consume()
            return {"traces": len(records)}
    return timer(_run)


def run_batch(driver, database, service_name, range_str, limit):
    def _run():
        with driver.session(database=database) as s:
            result = s.run(CYPHER_BATCH,
                           service=service_name, range=range_str, limit=limit)
            records = list(result)
            result.consume()
            return {"traces": len(records)}
    return timer(_run)


def run_jaeger_direct(jaeger_api_url, pod_to_service, pod_tag_key,
                      downstream_services, range_str, limit, session=None):
    """
    Direct Jaeger HTTP calls — mirrors get_traces_for_nodes (optimized path):

    If pod_tag_key is known:
      1 request per pod using the pod's own owning service → N_pods calls
      Same count as the loop variant.

    If pod_tag_key is None (client-side fallback):
      1 request per service → N_services calls (no pod tag filter possible)
    """
    range_clean = range_str.lstrip("-")
    unit = range_clean[-1]
    num = int(range_clean[:-1])
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    range_seconds = num * multipliers.get(unit, 3600)

    now_micros = int(time.time() * 1_000_000)
    start_micros = now_micros - (range_seconds * 1_000_000)

    http = session or requests

    def _run():
        total_traces = 0
        if pod_tag_key:
            # Optimized path: 1 call per pod with its own owning service
            for pod_name, own_service in pod_to_service.items():
                tag_query = f"{pod_tag_key}:{pod_name}"
                url = (
                    f"{jaeger_api_url}/traces"
                    f"?service={requests.utils.quote(own_service)}"
                    f"&start={start_micros}"
                    f"&end={now_micros}"
                    f"&limit={limit}"
                    f"&tag={requests.utils.quote(tag_query)}"
                )
                resp = http.get(url, timeout=30, headers={"Accept": "application/json"})
                resp.raise_for_status()
                total_traces += len(resp.json().get("data", []))
        else:
            # Client-side fallback: 1 call per service, no pod filter
            for svc in downstream_services:
                url = (
                    f"{jaeger_api_url}/traces"
                    f"?service={requests.utils.quote(svc)}"
                    f"&start={start_micros}"
                    f"&end={now_micros}"
                    f"&limit={limit}"
                )
                resp = http.get(url, timeout=30, headers={"Accept": "application/json"})
                resp.raise_for_status()
                total_traces += len(resp.json().get("data", []))
        return {"traces": total_traces}

    return timer(_run)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark: get_trace_for_node loop vs get_traces_for_nodes batch"
    )
    parser.add_argument("--neo4j-uri", default="bolt://localhost:7687")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", default="12345678")
    parser.add_argument("--neo4j-database", default="neo4j")
    parser.add_argument("--jaeger-url", default="http://localhost:16686")
    parser.add_argument("--service-name", default="frontend")
    parser.add_argument("--range", default="-10m", dest="range_str")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--runs", type=int, default=35)
    parser.add_argument("--delay", type=float, default=1.0,
                        help="Sekunden Pause zwischen Iterationen (default: 1.0)")
    parser.add_argument("--output-dir", default=".")
    args = parser.parse_args()

    assert args.runs >= 6

    warmup_count = 5
    measure_start = warmup_count

    driver = GraphDatabase.driver(
        args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password)
    )
    driver.verify_connectivity()

    jaeger_api_url, pod_tag_key = load_jaeger_context_from_graph(
        driver, args.neo4j_database, args.jaeger_url
    )

    downstream = discover_downstream(
        driver, args.neo4j_database, args.service_name
    )
    downstream_services = sorted(set(s for s, _ in downstream))
    # Build pod → own-service mapping (same ordering as procedure)
    pod_to_service = {}
    for svc, pod in downstream:
        if pod not in pod_to_service:
            pod_to_service[pod] = svc
    pods = sorted(pod_to_service.keys())

    n_services = len(downstream_services)
    n_pods = len(pods)
    if pod_tag_key:
        expected_jaeger_calls = n_pods
        jaeger_http_desc = f"{n_pods} calls (1 per pod, own service — optimized path)"
    else:
        expected_jaeger_calls = n_services
        jaeger_http_desc = f"{n_services} calls (client-side fallback, no stored podTagKey)"

    print(f"Service:      {args.service_name}")
    print(f"Downstream:   {downstream_services}")
    print(f"Pods:         {n_pods} pods across {n_services} services")
    print(f"Jaeger URL:   {jaeger_api_url}")
    print(f"Pod tag key:  {pod_tag_key or '(none — no server-side pod filter)'}")
    print(f"jaeger_http:  {jaeger_http_desc}")
    print(f"Loop calls:   {n_pods} (1 per pod)")
    print(f"Range:        {args.range_str}")
    print(f"Limit:        {args.limit} (pro Pod)")
    print(f"Delay:        {args.delay}s zwischen Iterationen")
    print(f"Runs:         {args.runs} (1 cold + {warmup_count - 1} warmup "
          f"+ {args.runs - warmup_count} measured)\n")

    http_session = requests.Session()

    labels = ["loop", "batch", "graph_match", "jaeger_http"]
    timings = {label: [] for label in labels}
    max_retries = 3

    for i in range(args.runs):
        phase = ("COLD" if i == 0
                 else ("WARM" if i < warmup_count
                       else f"RUN {i - warmup_count + 1:02d}"))

        clear_neo4j_query_cache(driver, args.neo4j_database)

        # Loop variant
        stats_loop = None
        t_loop = None
        for attempt in range(max_retries):
            try:
                stats_loop, t_loop = run_loop(
                    driver, args.neo4j_database,
                    args.service_name, args.range_str, args.limit,
                )
                break
            except Exception as e:
                print(f"    RETRY loop (attempt {attempt+1}): {e}")
                if not wait_for_jaeger(jaeger_api_url):
                    print("    FEHLER: Jaeger nicht erreichbar. Abbruch.")
                    driver.close()
                    return
        if t_loop is None:
            print(f"    SKIP [{phase}]: loop fehlgeschlagen")
            continue

        timings["loop"].append(t_loop)

        # graph_match and batch both run warm (after loop), same cache state
        clear_neo4j_query_cache(driver, args.neo4j_database)
        _, t_graph = run_graph_match(
            driver, args.neo4j_database, args.service_name,
        )
        timings["graph_match"].append(t_graph)

        clear_neo4j_query_cache(driver, args.neo4j_database)

        # Batch variant
        stats_batch = None
        t_batch = None
        for attempt in range(max_retries):
            try:
                stats_batch, t_batch = run_batch(
                    driver, args.neo4j_database,
                    args.service_name, args.range_str, args.limit,
                )
                break
            except Exception as e:
                print(f"    RETRY batch (attempt {attempt+1}): {e}")
                if not wait_for_jaeger(jaeger_api_url):
                    print("    FEHLER: Jaeger nicht erreichbar. Abbruch.")
                    driver.close()
                    return
        if t_batch is None:
            print(f"    SKIP [{phase}]: batch fehlgeschlagen")
            timings["loop"].pop()
            timings["graph_match"].pop()
            continue

        timings["batch"].append(t_batch)

        # Direct Jaeger HTTP
        t_jaeger = None
        for attempt in range(max_retries):
            try:
                _, t_jaeger = run_jaeger_direct(
                    jaeger_api_url, pod_to_service, pod_tag_key,
                    downstream_services, args.range_str, args.limit,
                    session=http_session,
                )
                break
            except Exception as e:
                print(f"    RETRY jaeger_http (attempt {attempt+1}): {e}")
                http_session.close()
                http_session = requests.Session()
                if not wait_for_jaeger(jaeger_api_url):
                    print("    FEHLER: Jaeger nicht erreichbar. Abbruch.")
                    driver.close()
                    return
        if t_jaeger is None:
            print(f"    SKIP [{phase}]: jaeger_http fehlgeschlagen")
            timings["graph_match"].pop()
            timings["loop"].pop()
            timings["batch"].pop()
            continue

        timings["jaeger_http"].append(t_jaeger)

        extra = ""
        if i == measure_start:
            extra = (f"  loop={stats_loop.get('traces', '?')} traces"
                     f"  batch={stats_batch.get('traces', '?')} traces")

        print(f"  [{phase:>6s}]  loop={t_loop*1000:8.2f}ms  "
              f"batch={t_batch*1000:8.2f}ms  "
              f"graph={t_graph*1000:8.2f}ms  "
              f"jaeger={t_jaeger*1000:8.2f}ms{extra}")

        if i < args.runs - 1:
            time.sleep(args.delay)

    http_session.close()

    actual_runs = len(timings["loop"])
    if actual_runs < measure_start + 1:
        print(f"\nFEHLER: Nur {actual_runs} erfolgreiche Runs.")
        driver.close()
        return

    # --- Write CSV ---
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(
        args.output_dir, f"benchmark_batch_jaeger_{timestamp_str}.csv"
    )

    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["run", "phase", "loop_ms", "batch_ms", "graph_match_ms",
                         "jaeger_http_ms", "neo4j_overhead_ms"])
        for i in range(actual_runs):
            if i == 0:
                phase = "cold"
            elif i < warmup_count:
                phase = "warmup"
            else:
                phase = "measure"
            loop_ms = timings['loop'][i] * 1000
            batch_ms = timings['batch'][i] * 1000
            graph_ms = timings['graph_match'][i] * 1000
            jaeger_ms = timings['jaeger_http'][i] * 1000
            writer.writerow([
                i + 1, phase,
                f"{loop_ms:.4f}", f"{batch_ms:.4f}", f"{graph_ms:.4f}",
                f"{jaeger_ms:.4f}", f"{batch_ms - jaeger_ms:.4f}",
            ])

    print(f"\nRohdaten: {csv_path}")

    # --- Statistics ---
    print("\n" + "=" * 70)
    print(f"  {'Variante':>12s}  {'Cold (ms)':>12s}  {'Mean (ms)':>12s}  {'Std (ms)':>12s}")
    print("-" * 70)

    for label in labels:
        cold = timings[label][0] * 1000
        measured = [t * 1000 for t in timings[label][measure_start:]]
        if not measured:
            continue
        mean = statistics.mean(measured)
        std = statistics.stdev(measured) if len(measured) > 1 else 0.0
        print(f"  {label:>12s}  {cold:12.2f}  {mean:12.2f}  {std:12.2f}")

    print("=" * 70)

    measured_loop = [t * 1000 for t in timings["loop"][measure_start:]]
    measured_batch = [t * 1000 for t in timings["batch"][measure_start:]]
    measured_graph = [t * 1000 for t in timings["graph_match"][measure_start:]]
    measured_jaeger = [t * 1000 for t in timings["jaeger_http"][measure_start:]]

    if measured_loop and measured_batch and measured_graph and measured_jaeger:
        mean_loop = statistics.mean(measured_loop)
        mean_batch = statistics.mean(measured_batch)
        mean_graph = statistics.mean(measured_graph)
        mean_jaeger = statistics.mean(measured_jaeger)
        speedup = mean_loop / mean_batch if mean_batch > 0 else float("inf")

        overhead_batch = mean_batch - mean_graph - mean_jaeger
        saving = mean_loop - mean_batch

        print(f"\n  Komponentenzerlegung Batch (Mittelwerte):")
        print(f"    Graph-Match:    {mean_graph:8.2f} ms  ({mean_graph/mean_batch*100:5.1f}%)")
        print(f"    Jaeger HTTP:    {mean_jaeger:8.2f} ms  ({mean_jaeger/mean_batch*100:5.1f}%) [{jaeger_http_desc}]")
        print(f"    Overhead:       {overhead_batch:8.2f} ms  ({overhead_batch/mean_batch*100:5.1f}%)")
        print(f"    ---")
        print(f"    Batch (gesamt): {mean_batch:8.2f} ms  (100.0%)")
        print(f"\n  Speedup (batch vs loop): {speedup:.2f}x")
        print(f"  Loop:           {mean_loop:8.2f} ms  ({mean_loop/n_pods:.2f} ms/pod, {n_pods} calls)")
        print(f"  Batch:          {mean_batch:8.2f} ms  ({mean_batch/n_pods:.2f} ms/pod, {expected_jaeger_calls} jaeger calls)")
        print(f"  Einsparung:     {saving:8.2f} ms  ({saving/mean_loop*100:.1f}%)")
        print(f"  Pods:           {n_pods}")
        print(f"  Services:       {n_services}")

    driver.close()


if __name__ == "__main__":
    main()
