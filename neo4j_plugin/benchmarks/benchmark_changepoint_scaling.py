"""
Performance Benchmark: PELT Changepoint Detection – Skalierung ueber Zeitreihenlaenge

Misst wie die Changepoint-Detection mit wachsender Datenmenge skaliert.
Gleicher Query, nur der range-Parameter variiert: 10m, 60m, 240m.

Query:
  MATCH (p:Pod)-[]-(s:Service {name: "frontend"})
  CALL graphobs.data.get_time_series(p, "container_memory_rss",
       {time: toString(datetime()), range: "-Xm", resolution: "10s"})
  YIELD timestamps, values
  CALL graphobs.analysis.pelt(timestamps, values, 100.0)
  YIELD values as cps
  RETURN timestamps, values, cps

Ablauf pro Range:
  - 35 Iterationen (konfigurierbar)
  - Run 1 = Cold-Start
  - Runs 2-5 = Warmup (verworfen)
  - Runs 6-35 = 30 gemessene Runs -> Mean + Std

Nutzung:
  pip install neo4j
  python benchmark_changepoint_scaling.py [--neo4j-uri bolt://localhost:7687]
                                          [--neo4j-user neo4j]
                                          [--neo4j-password <pw>]
                                          [--runs 35]
                                          [--output-dir .]
"""

import argparse
import csv
import os
import statistics
import time
from datetime import datetime

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
    with driver.session(database=database) as s:
        try:
            s.run("CALL db.clearQueryCaches()").consume()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Benchmark query
# ---------------------------------------------------------------------------

CYPHER = (
    "MATCH (p:Pod)-[]-(s:Service {name: $service}) "
    "CALL graphobs.data.get_time_series(p, $metric, "
    "  {time: toString(datetime()), range: $range, resolution: '10s'}) "
    "YIELD timestamps, values "
    "CALL graphobs.analysis.pelt(timestamps, values, $penalty) "
    "YIELD values AS cps "
    "RETURN timestamps, values, cps"
)


def run_changepoint(driver, database, service, metric, range_str, penalty):
    def _run():
        with driver.session(database=database) as s:
            result = s.run(
                CYPHER,
                service=service,
                metric=metric,
                range=range_str,
                penalty=penalty,
            )
            records = list(result)
            result.consume()
            # Extract stats from first record
            if records:
                rec = records[0]
                ts = rec.get("timestamps", [])
                cps = rec.get("cps", [])
                n_points = len(ts) if ts else 0
                n_changepoints = len(cps) if cps else 0
                return {"points": n_points, "changepoints": n_changepoints}
            return {"points": 0, "changepoints": 0}

    return timer(_run)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark: PELT changepoint detection scaling"
    )
    parser.add_argument("--neo4j-uri", default="bolt://localhost:7687")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", default="12345678")
    parser.add_argument("--neo4j-database", default="neo4j")
    parser.add_argument("--service", default="frontend")
    parser.add_argument("--metric", default="container_memory_rss")
    parser.add_argument("--penalty", type=float, default=100.0)
    parser.add_argument("--ranges", default="10,60,300",
                        help="Komma-getrennte Minuten-Werte (default: 10,60,300)")
    parser.add_argument("--runs", type=int, default=35)
    parser.add_argument("--output-dir", default=".")
    args = parser.parse_args()

    ranges_min = [int(x.strip()) for x in args.ranges.split(",")]
    assert args.runs >= 6, "Mindestens 6 Runs"

    warmup_count = 5
    measure_start = warmup_count

    driver = GraphDatabase.driver(
        args.neo4j_uri,
        auth=(args.neo4j_user, args.neo4j_password),
    )
    driver.verify_connectivity()

    print(f"Service:    {args.service}")
    print(f"Metric:     {args.metric}")
    print(f"Penalty:    {args.penalty}")
    print(f"Ranges:     {ranges_min} min")
    print(f"Resolution: 10s")
    print(f"Runs:       {args.runs} (1 cold + {warmup_count - 1} warmup "
          f"+ {args.runs - warmup_count} measured per range)\n")

    # --- Collect all timings ---
    all_timings = {}
    all_stats = {}  # range_min -> {points, changepoints} from first measured run

    for range_min in ranges_min:
        range_str = f"-{range_min}m"
        timings = []

        print(f"{'='*60}")
        print(f"  Range: {range_min} min ({range_str})")
        print(f"{'='*60}")

        for i in range(args.runs):
            phase = ("COLD" if i == 0
                     else ("WARM" if i < warmup_count
                           else f"RUN {i - warmup_count + 1:02d}"))

            clear_neo4j_query_cache(driver, args.neo4j_database)

            stats, elapsed = run_changepoint(
                driver, args.neo4j_database,
                args.service, args.metric,
                range_str, args.penalty,
            )

            timings.append(elapsed)

            extra = ""
            if i == measure_start:
                all_stats[range_min] = stats
                extra = (f"  pts={stats['points']}, "
                         f"cps={stats['changepoints']}")

            print(f"  [{phase:>6s}]  {elapsed*1000:8.2f} ms{extra}")

        all_timings[range_min] = timings
        print()

    # --- Write CSV ---
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(
        args.output_dir, f"benchmark_changepoint_{timestamp_str}.csv"
    )

    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["range_min", "run", "phase", "elapsed_ms"])
        for range_min in ranges_min:
            for i in range(args.runs):
                if i == 0:
                    phase = "cold"
                elif i < warmup_count:
                    phase = "warmup"
                else:
                    phase = "measure"
                writer.writerow([
                    range_min,
                    i + 1,
                    phase,
                    f"{all_timings[range_min][i] * 1000:.4f}",
                ])

    print(f"Rohdaten: {csv_path}\n")

    # --- Summary ---
    print("=" * 74)
    print(f"  {'Range':>10s}  {'Cold (ms)':>12s}  {'Mean (ms)':>12s}  "
          f"{'Std (ms)':>12s}  {'Points':>8s}  {'CPs':>5s}")
    print("-" * 74)

    for range_min in ranges_min:
        cold = all_timings[range_min][0] * 1000
        measured = [t * 1000 for t in all_timings[range_min][measure_start:]]
        mean = statistics.mean(measured)
        std = statistics.stdev(measured) if len(measured) > 1 else 0.0
        st = all_stats.get(range_min, {})

        print(f"  {range_min:>7d} m  {cold:12.2f}  {mean:12.2f}  "
              f"{std:12.2f}  {st.get('points', '?'):>8}  "
              f"{st.get('changepoints', '?'):>5}")

    print("=" * 74)

    # --- Scaling analysis ---
    if len(ranges_min) >= 2:
        print(f"\n  Skalierungsanalyse:")
        base_range = ranges_min[0]
        base_mean = statistics.mean(
            [t * 1000 for t in all_timings[base_range][measure_start:]]
        )

        for range_min in ranges_min[1:]:
            mean = statistics.mean(
                [t * 1000 for t in all_timings[range_min][measure_start:]]
            )
            data_factor = range_min / base_range
            time_factor = mean / base_mean if base_mean > 0 else float("inf")

            # PELT is O(n) best case, O(n^2) worst case
            ratio = time_factor / data_factor
            if ratio < 0.7:
                scaling = "sub-linear"
            elif ratio < 1.3:
                scaling = "linear ~O(n)"
            elif ratio < data_factor * 0.8:
                scaling = "super-linear"
            else:
                scaling = "quadratic ~O(n^2)"

            print(f"    {base_range}m -> {range_min}m: "
                  f"Daten x{data_factor:.0f}, "
                  f"Zeit x{time_factor:.1f}  "
                  f"({scaling})")

    print()
    driver.close()


if __name__ == "__main__":
    main()
