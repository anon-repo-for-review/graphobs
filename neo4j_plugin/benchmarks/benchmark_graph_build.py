"""
Performance Benchmark: Graph-Build (import_graph + register_prometheus)

Misst die beiden Procedures, die den Graphen aufbauen:
  1. graphobs.jaeger.import_graph       – Importiert Services, Operations, Dependencies aus Jaeger
  2. graphobs.datasources.register_prometheus – Registriert Pods + Metriken aus Prometheus, linkt zu Services

Ablauf pro Iteration:
  1. Graph loeschen (MATCH (n) DETACH DELETE n)
  2. import_graph ausfuehren und messen
  3. register_prometheus ausfuehren und messen

Da der Graph bei jeder Iteration komplett neu gebaut wird, gibt es kein
Warmup im ueblichen Sinne. Trotzdem: Run 1 = Cold (Java-Klassen noch nicht
geladen), Runs 2+ = Warm.

Nutzung:
  pip install neo4j
  python benchmark_graph_build.py [--jaeger-url http://localhost:16686]
                                   [--prometheus-url http://localhost:9090]
                                   [--runs 10]
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


def clear_graph(driver, database):
    """Deletes all nodes and relationships."""
    with driver.session(database=database) as s:
        s.run("MATCH (n) DETACH DELETE n").consume()


def clear_neo4j_query_cache(driver, database):
    with driver.session(database=database) as s:
        try:
            s.run("CALL db.clearQueryCaches()").consume()
        except Exception:
            pass


def wait_for_jaeger(jaeger_url, max_wait=120, interval=5):
    """Polls Jaeger until it responds (recovers from OOMKill)."""
    base = jaeger_url.rstrip("/").replace("/api", "")
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            resp = requests.get(base, timeout=5)
            if resp.status_code < 500:
                return True
        except (requests.ConnectionError, requests.Timeout):
            pass
        print(f"    Warte auf Jaeger ({base})...")
        time.sleep(interval)
    return False


def count_nodes(driver, database):
    """Returns dict with node counts per label."""
    with driver.session(database=database) as s:
        result = s.run(
            "MATCH (n) "
            "UNWIND labels(n) AS label "
            "RETURN label, count(*) AS cnt "
            "ORDER BY cnt DESC"
        )
        return {r["label"]: r["cnt"] for r in result}


# ---------------------------------------------------------------------------
# Benchmark queries
# ---------------------------------------------------------------------------

def run_import_graph(driver, database, jaeger_url):
    """Runs graphobs.jaeger.import_graph and returns (stats, elapsed)."""
    cypher = (
        "CALL graphobs.jaeger.import_graph($url) "
        "YIELD services, operations, dependencies, durationMs, message "
        "RETURN services, operations, dependencies, durationMs, message"
    )

    def _run():
        with driver.session(database=database) as s:
            result = s.run(cypher, url=jaeger_url)
            rec = result.single()
            result.consume()
            if rec:
                return {
                    "services": rec["services"],
                    "operations": rec["operations"],
                    "dependencies": rec["dependencies"],
                    "message": rec["message"],
                }
            return {"status": "no result"}

    return timer(_run)


def run_register_prometheus(driver, database, prometheus_url):
    """Runs graphobs.datasources.register_prometheus and returns (stats, elapsed)."""
    cypher = (
        "CALL graphobs.datasources.register_prometheus($url) "
        "YIELD message, url, podCount "
        "RETURN message, podCount"
    )

    def _run():
        with driver.session(database=database) as s:
            result = s.run(cypher, url=prometheus_url)
            rec = result.single()
            result.consume()
            if rec:
                return {
                    "message": rec["message"],
                    "pods": rec["podCount"],
                }
            return {"message": "no result"}

    return timer(_run)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark: Graph-Build (import_graph + register_prometheus)"
    )
    parser.add_argument("--neo4j-uri", default="bolt://localhost:7687")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", default="12345678")
    parser.add_argument("--neo4j-database", default="neo4j")
    parser.add_argument("--jaeger-url", default="http://localhost:16686")
    parser.add_argument("--prometheus-url", default="http://localhost:9090")
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--delay", type=float, default=3.0,
                        help="Sekunden Pause zwischen Iterationen (default: 3.0)")
    parser.add_argument("--output-dir", default=".")
    args = parser.parse_args()

    assert args.runs >= 2, "Mindestens 2 Runs (1 cold + 1 measured)"

    warmup_count = 1  # nur 1 Cold-Start-Run
    measure_start = warmup_count

    driver = GraphDatabase.driver(
        args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password)
    )
    driver.verify_connectivity()

    print(f"Jaeger URL:     {args.jaeger_url}")
    print(f"Prometheus URL: {args.prometheus_url}")
    print(f"Runs:           {args.runs} (1 cold + {args.runs - 1} measured)\n")

    labels = ["import_graph", "register_prometheus", "total"]
    timings = {label: [] for label in labels}

    max_retries = 3

    for i in range(args.runs):
        phase = "COLD" if i == 0 else f"RUN {i:02d}"

        # 1) Clear graph
        clear_graph(driver, args.neo4j_database)
        clear_neo4j_query_cache(driver, args.neo4j_database)

        # 2) Import graph from Jaeger (with retry)
        stats_import = None
        t_import = None
        for attempt in range(max_retries):
            try:
                stats_import, t_import = run_import_graph(
                    driver, args.neo4j_database, args.jaeger_url,
                )
                break
            except Exception as e:
                print(f"    RETRY import (attempt {attempt+1}): {e}")
                if not wait_for_jaeger(args.jaeger_url):
                    print("    FEHLER: Jaeger nicht erreichbar. Abbruch.")
                    driver.close()
                    return
        if t_import is None:
            print(f"    SKIP [{phase}]: import fehlgeschlagen")
            continue

        timings["import_graph"].append(t_import)

        # 3) Register Prometheus
        stats_prom, t_prom = run_register_prometheus(
            driver, args.neo4j_database, args.prometheus_url,
        )
        timings["register_prometheus"].append(t_prom)

        t_total = t_import + t_prom
        timings["total"].append(t_total)

        extra = ""
        if i == measure_start:
            nodes = count_nodes(driver, args.neo4j_database)
            extra = (f"  svc={stats_import.get('services', '?')}"
                     f"  ops={stats_import.get('operations', '?')}"
                     f"  deps={stats_import.get('dependencies', '?')}"
                     f"  pods={stats_prom.get('pods', '?')}"
                     f"  nodes={nodes}")

        import_status = stats_import.get("message", "?")
        prom_status = stats_prom.get("message", "?")

        print(f"  [{phase:>6s}]  import={t_import*1000:8.2f}ms  "
              f"prom={t_prom*1000:8.2f}ms  "
              f"total={t_total*1000:8.2f}ms{extra}")

        if "error" in str(import_status).lower():
            print(f"    WARN import: {import_status}")
        if "error" in str(prom_status).lower():
            print(f"    WARN prometheus: {prom_status}")

        # Delay between iterations to let Jaeger recover
        if i < args.runs - 1:
            time.sleep(args.delay)

    actual_runs = len(timings["import_graph"])
    if actual_runs < measure_start + 1:
        print(f"\nFEHLER: Nur {actual_runs} erfolgreiche Runs.")
        driver.close()
        return

    # --- Write CSV ---
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(
        args.output_dir, f"benchmark_graph_build_{timestamp_str}.csv"
    )

    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["run", "phase", "import_graph_ms", "register_prometheus_ms", "total_ms"])
        for i in range(actual_runs):
            phase = "cold" if i == 0 else "measure"
            writer.writerow([
                i + 1, phase,
                f"{timings['import_graph'][i] * 1000:.4f}",
                f"{timings['register_prometheus'][i] * 1000:.4f}",
                f"{timings['total'][i] * 1000:.4f}",
            ])

    print(f"\nRohdaten: {csv_path}")

    # --- Statistics ---
    print("\n" + "=" * 70)
    print(f"  {'':>22s}  {'Cold (ms)':>12s}  {'Mean (ms)':>12s}  {'Std (ms)':>12s}")
    print("-" * 70)

    for label in labels:
        cold = timings[label][0] * 1000
        measured = [t * 1000 for t in timings[label][measure_start:]]
        if not measured:
            continue
        mean = statistics.mean(measured)
        std = statistics.stdev(measured) if len(measured) > 1 else 0.0
        print(f"  {label:>22s}  {cold:12.2f}  {mean:12.2f}  {std:12.2f}")

    print("=" * 70)

    measured_import = [t * 1000 for t in timings["import_graph"][measure_start:]]
    measured_prom = [t * 1000 for t in timings["register_prometheus"][measure_start:]]
    measured_total = [t * 1000 for t in timings["total"][measure_start:]]

    if measured_total:
        mean_import = statistics.mean(measured_import)
        mean_prom = statistics.mean(measured_prom)
        mean_total = statistics.mean(measured_total)

        print(f"\n  Komponentenzerlegung (Mittelwerte):")
        print(f"    import_graph:        {mean_import:8.2f} ms  ({mean_import/mean_total*100:5.1f}%)")
        print(f"    register_prometheus: {mean_prom:8.2f} ms  ({mean_prom/mean_total*100:5.1f}%)")
        print(f"    ---")
        print(f"    Total:               {mean_total:8.2f} ms  (100.0%)")

    driver.close()


if __name__ == "__main__":
    main()
