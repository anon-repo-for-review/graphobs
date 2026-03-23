"""
Performance Benchmark: node_group_correlation – Skalierung ueber Zeitreihenlaenge

Misst wie die Correlation-Procedure mit wachsender Datenmenge skaliert.
Gleicher Query, nur der range-Parameter variiert: 10m, 60m, 240m.

Query:
  MATCH (s1:Service {name: "frontend"})
  MATCH (s2:Service {name: "ad"})
  WITH s1, collect(s2) AS service2
  CALL graphobs.analysis.node_group_correlation(s1, ..., service2, ...,
       {range: "-Xm", time: toString(datetime()), percentile: 0.95,
        resolution: "10s", join: "resample"})
  YIELD node, correlation
  RETURN node, correlation

Ablauf pro Range:
  - 35 Iterationen (konfigurierbar)
  - Run 1 = Cold-Start
  - Runs 2-5 = Warmup (verworfen)
  - Runs 6-35 = 30 gemessene Runs -> Mean + Std

Nutzung:
  pip install neo4j
  python benchmark_correlation_scaling.py [--neo4j-uri bolt://localhost:7687]
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
    "MATCH (s1:Service {name: $service1}) "
    "MATCH (s2:Service {name: $service2}) "
    "WITH s1, collect(s2) AS service2 "
    "CALL graphobs.analysis.node_group_correlation(s1, "
    "  $metric1, service2, $metric2, "
    "  {range: $range, time: toString(datetime()), "
    "   percentile: 0.95, resolution: '10s', join: 'resample'}) "
    "YIELD node, correlation "
    "RETURN node, correlation"
)


def run_correlation(driver, database, service1, service2, metric1, metric2, range_str):
    def _run():
        with driver.session(database=database) as s:
            result = s.run(
                CYPHER,
                service1=service1,
                service2=service2,
                metric1=metric1,
                metric2=metric2,
                range=range_str,
            )
            records = list(result)
            result.consume()
            return records

    return timer(_run)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark: node_group_correlation scaling"
    )
    parser.add_argument("--neo4j-uri", default="bolt://localhost:7687")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", default="12345678")
    parser.add_argument("--neo4j-database", default="neo4j")
    parser.add_argument("--service1", default="frontend")
    parser.add_argument("--service2", default="ad")
    parser.add_argument("--metric1",
                        default="traces_span_metrics_duration_milliseconds_bucket")
    parser.add_argument("--metric2",
                        default="traces_span_metrics_duration_milliseconds_bucket")
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

    print(f"Service 1:  {args.service1}")
    print(f"Service 2:  {args.service2}")
    print(f"Metric:     {args.metric1}")
    print(f"Ranges:     {ranges_min} min")
    print(f"Runs:       {args.runs} (1 cold + {warmup_count - 1} warmup "
          f"+ {args.runs - warmup_count} measured per range)\n")

    # --- Collect all timings ---
    all_timings = {}  # range_min -> list of elapsed seconds

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

            records, elapsed = run_correlation(
                driver, args.neo4j_database,
                args.service1, args.service2,
                args.metric1, args.metric2,
                range_str,
            )

            timings.append(elapsed)

            # Show correlation value on first measured run
            extra = ""
            if i == measure_start and records:
                for rec in records:
                    corr = rec.get("correlation", "?")
                    node = rec.get("node", "?")
                    extra = f"  corr={corr}"

            print(f"  [{phase:>6s}]  {elapsed*1000:8.2f} ms{extra}")

        all_timings[range_min] = timings
        print()

    # --- Write CSV ---
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(
        args.output_dir, f"benchmark_correlation_{timestamp_str}.csv"
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
    print("=" * 70)
    print(f"  {'Range':>10s}  {'Cold (ms)':>12s}  {'Mean (ms)':>12s}  "
          f"{'Std (ms)':>12s}  {'Datapoints':>12s}")
    print("-" * 70)

    prev_mean = None
    for range_min in ranges_min:
        cold = all_timings[range_min][0] * 1000
        measured = [t * 1000 for t in all_timings[range_min][measure_start:]]
        mean = statistics.mean(measured)
        std = statistics.stdev(measured) if len(measured) > 1 else 0.0

        # Estimated datapoints: range_minutes * 60s / 10s resolution
        est_points = range_min * 60 // 10

        print(f"  {range_min:>7d} m  {cold:12.2f}  {mean:12.2f}  "
              f"{std:12.2f}  {est_points:>12d}")

        prev_mean = mean

    print("=" * 70)

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
            print(f"    {base_range}m -> {range_min}m: "
                  f"Daten x{data_factor:.0f}, "
                  f"Zeit x{time_factor:.1f}  "
                  f"({'linear' if 0.7 < time_factor/data_factor < 1.3 else 'sub-linear' if time_factor/data_factor < 0.7 else 'super-linear'})")

    print()
    driver.close()


if __name__ == "__main__":
    main()
