"""
Performance Benchmark: Jaeger Trace Query (single Pod)

Misst drei Komponenten separat:
  1. Full Query   – MATCH Pod via Service + graphobs.data.get_trace_for_node
  2. Graph Match  – nur MATCH: Pod-Lookup + Jaeger config aus Graph
  3. Jaeger HTTP  – direkter /api/traces Call an Jaeger mit &tag= Filter (kein Neo4j)

Full Query:
  MATCH (p:Pod)-[]-(s:Service {name: "frontend"})
  WITH p LIMIT 1
  CALL graphobs.data.get_trace_for_node(p,
       {time: toString(datetime()), range: "-10m", limit: 20})
  YIELD traceId, service, spanCount, startTime, durationMs
  RETURN traceId, service, spanCount, startTime, durationMs

Die Procedure nutzt den gespeicherten podTagKey vom :Jaeger-Knoten
(z.B. &tag=k8s.pod.name:podName). Fallback: client-seitige Filterung.

Ablauf:
  - 35 Iterationen pro Query
  - Run 1 = Cold-Start
  - Runs 2-5 = Warmup
  - Runs 6-35 = 30 gemessene Runs

Nutzung:
  pip install neo4j requests
  python benchmark_jaeger_traces.py [--service-name frontend]
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


def wait_for_jaeger(jaeger_base_url, max_wait=120, interval=5):
    """Polls Jaeger health endpoint until it responds (handles OOM restarts)."""
    health_url = jaeger_base_url.replace("/api", "")
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            resp = requests.get(health_url, timeout=5)
            if resp.status_code < 500:
                return True
        except (requests.ConnectionError, requests.Timeout):
            pass
        print(f"    Warte auf Jaeger ({health_url})...")
        time.sleep(interval)
    return False


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def discover_context(driver, database, service_name, jaeger_url):
    """
    Reads Pod name, owning service, podTagKey and the stored API base URL from the graph.
    The stored baseUrl (from import_graph) is the correctly detected API URL.
    The jaeger_url CLI arg is used as fallback if no Jaeger node exists.
    """
    with driver.session(database=database) as s:
        # Find a pod of the service
        result = s.run(
            "MATCH (p:Pod)-[]-(s:Service {name: $svc}) "
            "RETURN p.name AS podName, s.name AS serviceName "
            "LIMIT 1",
            svc=service_name,
        )
        rec = result.single()
        if not rec:
            raise RuntimeError(f"Kein Pod fuer Service '{service_name}' gefunden.")
        pod_name = rec["podName"]
        svc_name = rec["serviceName"]

        # Get baseUrl + podTagKey from Jaeger node
        result = s.run(
            "MATCH (j:Jaeger) "
            "RETURN j.baseUrl AS baseUrl, j.podTagKey AS podTagKey "
            "LIMIT 1"
        )
        rec = result.single()
        if rec and rec.get("baseUrl"):
            base_url = rec["baseUrl"].rstrip("/")
        else:
            base_url = jaeger_url.rstrip("/")
        pod_tag_key = rec.get("podTagKey") if rec else None

    return base_url, svc_name, pod_name, pod_tag_key


# ---------------------------------------------------------------------------
# Benchmark queries
# ---------------------------------------------------------------------------

def run_full_query(driver, database, service_name, range_str, limit):
    """Full Cypher query calling get_trace_for_node for a Pod."""
    cypher = (
        "MATCH (p:Pod)-[]-(s:Service {name: $service}) "
        "WITH p LIMIT 1 "
        "CALL graphobs.data.get_trace_for_node(p, "
        "  {time: toString(datetime()), range: $range, limit: $limit}) "
        "YIELD traceId, service, spanCount, startTime, durationMs "
        "RETURN traceId, service, spanCount, startTime, durationMs"
    )

    def _run():
        with driver.session(database=database) as s:
            result = s.run(
                cypher, service=service_name, range=range_str, limit=limit
            )
            records = list(result)
            result.consume()
            return {"traces": len(records)}

    return timer(_run)


def run_graph_match_only(driver, database, service_name):
    """
    Measures only the Cypher portion that runs before the procedure call:
    Find Pod via Service, WITH p LIMIT 1.
    The Jaeger node lookup happens inside the procedure via Java API,
    not in Cypher, so it must NOT be included here.
    """
    cypher = (
        "MATCH (p:Pod)-[]-(s:Service {name: $service}) "
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


def run_jaeger_only(jaeger_base_url, svc_name, pod_name, pod_tag_key,
                    range_str, limit, session=None):
    """
    Direct HTTP call to Jaeger /api/traces with &tag= filter,
    replicating the procedure's server-side pod filtering.
    If podTagKey is unknown, queries without tag filter (same as fallback).
    """
    range_clean = range_str.lstrip("-")
    unit = range_clean[-1]
    num = int(range_clean[:-1])
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    range_seconds = num * multipliers.get(unit, 3600)

    now_micros = int(time.time() * 1_000_000)
    start_micros = now_micros - (range_seconds * 1_000_000)

    url = (
        f"{jaeger_base_url}/traces"
        f"?service={requests.utils.quote(svc_name)}"
        f"&start={start_micros}"
        f"&end={now_micros}"
        f"&limit={limit}"
    )
    # Server-side pod tag filter (matches procedure behavior)
    if pod_tag_key and pod_name:
        tag_query = f"{pod_tag_key}:{pod_name}"
        url += f"&tag={requests.utils.quote(tag_query)}"

    http = session or requests

    def _run():
        resp = http.get(url, timeout=30, headers={"Accept": "application/json"})
        resp.raise_for_status()
        data = resp.json()
        traces = data.get("data", [])
        total_spans = sum(len(t.get("spans", [])) for t in traces)
        return {"traces": len(traces), "spans": total_spans}

    return timer(_run)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark: Jaeger trace query (single Pod)"
    )
    parser.add_argument("--neo4j-uri", default="bolt://localhost:7687")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", default="12345678")
    parser.add_argument("--neo4j-database", default="neo4j")
    parser.add_argument("--jaeger-url", default="http://localhost:16686/jaeger/ui/api")
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

    jaeger_url, svc_name, pod_name, pod_tag_key = discover_context(
        driver, args.neo4j_database, args.service_name, args.jaeger_url
    )

    # Check Jaeger is reachable
    print(f"Jaeger URL:      {jaeger_url}")
    if not wait_for_jaeger(jaeger_url, max_wait=30, interval=3):
        print("FEHLER: Jaeger nicht erreichbar. Abbruch.")
        driver.close()
        return

    print(f"Service:         {args.service_name}")
    print(f"Pod:             {pod_name}")
    print(f"Owning service:  {svc_name}")
    print(f"Stored podTagKey:{pod_tag_key or ' (none — no server-side pod filter)'}")
    print(f"Range:           {args.range_str}")
    print(f"Limit:           {args.limit}")
    print(f"Delay:           {args.delay}s zwischen Iterationen")
    print(f"Runs:            {args.runs} (1 cold + {warmup_count - 1} warmup "
          f"+ {args.runs - warmup_count} measured)\n")

    http_session = requests.Session()

    labels = ["full_query", "graph_match", "jaeger_http"]
    timings = {label: [] for label in labels}
    vol_traces = []  # traces returned per run (from direct Jaeger call)
    vol_spans  = []  # total spans across those traces
    max_retries = 3

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

        # Full query (via Neo4j procedure -> Jaeger)
        stats_full = None
        t_full = None
        for attempt in range(max_retries):
            try:
                stats_full, t_full = run_full_query(
                    driver, args.neo4j_database,
                    args.service_name, args.range_str, args.limit,
                )
                break
            except Exception as e:
                print(f"    RETRY full_query (attempt {attempt+1}): {e}")
                if not wait_for_jaeger(jaeger_url):
                    print("    FEHLER: Jaeger nicht mehr erreichbar. Abbruch.")
                    driver.close()
                    return
        if t_full is None:
            print(f"    SKIP [{phase}]: full_query fehlgeschlagen")
            timings["graph_match"].pop()
            continue

        timings["full_query"].append(t_full)

        # Direct Jaeger HTTP call
        stats_jaeger = None
        t_jaeger = None
        for attempt in range(max_retries):
            try:
                stats_jaeger, t_jaeger = run_jaeger_only(
                    jaeger_url, svc_name, pod_name, pod_tag_key,
                    args.range_str, args.limit,
                    session=http_session,
                )
                break
            except Exception as e:
                print(f"    RETRY jaeger_http (attempt {attempt+1}): {e}")
                http_session.close()
                http_session = requests.Session()
                if not wait_for_jaeger(jaeger_url):
                    print("    FEHLER: Jaeger nicht mehr erreichbar. Abbruch.")
                    driver.close()
                    return
        if t_jaeger is None:
            print(f"    SKIP [{phase}]: jaeger_http fehlgeschlagen")
            timings["graph_match"].pop()
            timings["full_query"].pop()
            continue

        timings["jaeger_http"].append(t_jaeger)
        vol_traces.append(stats_jaeger.get('traces', 0))
        vol_spans.append(stats_jaeger.get('spans', 0))

        n_traces = stats_jaeger.get('traces', '?')
        n_spans  = stats_jaeger.get('spans', '?')
        print(f"  [{phase:>6s}]  full={t_full*1000:8.2f}ms  "
              f"graph={t_graph*1000:8.2f}ms  "
              f"jaeger={t_jaeger*1000:8.2f}ms  "
              f"traces={n_traces}  spans={n_spans}")

        if i < args.runs - 1:
            time.sleep(args.delay)

    http_session.close()

    actual_runs = len(timings["full_query"])
    if actual_runs < measure_start + 1:
        print(f"\nFEHLER: Nur {actual_runs} erfolgreiche Runs.")
        driver.close()
        return

    # --- Write CSV ---
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(args.output_dir, f"benchmark_jaeger_{timestamp_str}.csv")

    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "run", "phase",
            "full_query_ms", "graph_match_ms", "jaeger_http_ms", "overhead_ms",
            "data_traces", "data_spans",
        ])
        for i in range(actual_runs):
            if i == 0:
                phase = "cold"
            elif i < warmup_count:
                phase = "warmup"
            else:
                phase = "measure"
            full = timings['full_query'][i] * 1000
            graph = timings['graph_match'][i] * 1000
            jaeger = timings['jaeger_http'][i] * 1000
            traces_v = vol_traces[i] if i < len(vol_traces) else ""
            spans_v  = vol_spans[i]  if i < len(vol_spans)  else ""
            writer.writerow([
                i + 1, phase,
                f"{full:.4f}", f"{graph:.4f}", f"{jaeger:.4f}",
                f"{full - graph - jaeger:.4f}",
                traces_v, spans_v,
            ])

    print(f"\nRohdaten: {csv_path}")

    # --- Statistics ---
    print("\n" + "=" * 70)
    print(f"{'':>20s}  {'Cold (ms)':>12s}  {'Mean (ms)':>12s}  {'Std (ms)':>12s}")
    print("-" * 70)

    for label in labels:
        cold = timings[label][0] * 1000
        measured = [t * 1000 for t in timings[label][measure_start:]]
        if not measured:
            continue
        mean = statistics.mean(measured)
        std = statistics.stdev(measured) if len(measured) > 1 else 0.0
        print(f"  {label:>18s}  {cold:12.2f}  {mean:12.2f}  {std:12.2f}")

    print("=" * 70)

    measured_full = [t * 1000 for t in timings["full_query"][measure_start:]]
    measured_graph = [t * 1000 for t in timings["graph_match"][measure_start:]]
    measured_jaeger = [t * 1000 for t in timings["jaeger_http"][measure_start:]]

    if measured_full and measured_graph and measured_jaeger:
        mean_full = statistics.mean(measured_full)
        mean_graph = statistics.mean(measured_graph)
        mean_jaeger = statistics.mean(measured_jaeger)
        overhead = mean_full - mean_graph - mean_jaeger

        print(f"\n  Komponentenzerlegung (Mittelwerte):")
        print(f"    Graph-Match:     {mean_graph:8.2f} ms  ({mean_graph/mean_full*100:5.1f}%)")
        print(f"    Jaeger HTTP:     {mean_jaeger:8.2f} ms  ({mean_jaeger/mean_full*100:5.1f}%)")
        print(f"    Overhead:        {overhead:8.2f} ms  ({overhead/mean_full*100:5.1f}%)")
        print(f"    ---")
        print(f"    Full Query:      {mean_full:8.2f} ms  (100.0%)")

        m_traces = vol_traces[measure_start:]
        m_spans  = vol_spans[measure_start:]
        if m_traces:
            print(f"\n  Datenvolumen (gemessene Runs):")
            print(f"    Traces:  avg={statistics.mean(m_traces):.0f}"
                  f"  min={min(m_traces)}"
                  f"  max={max(m_traces)}")
            print(f"    Spans:   avg={statistics.mean(m_spans):.0f}"
                  f"  min={min(m_spans)}"
                  f"  max={max(m_spans)}")

    driver.close()


if __name__ == "__main__":
    main()
