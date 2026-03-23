#!/usr/bin/env python3
"""
Runner script for executing benchmarks inside Kubernetes.
Runs selected benchmarks sequentially, writes CSVs to /results,
and prints all output to stdout (visible via kubectl logs).

Usage inside the cluster:
    python run_all.py --suite single     # single-pod benchmarks (default)
    python run_all.py --suite batch      # batch benchmarks
    python run_all.py --suite all        # everything
    python run_all.py --suite pick --benchmarks get_time_series jaeger_traces
"""

import argparse
import os
import subprocess
import sys
import time


# Cluster-internal defaults
DEFAULTS = {
    "neo4j_uri": "bolt://timegraph:7687",
    "neo4j_password": "12345678",
    "prometheus_url": "http://prometheus:9090",
    "jaeger_url": "http://jaeger-query:16686",
    "opensearch_url": "http://opensearch:9200",
    "output_dir": "/results",
}

# Two scenarios run for every single-pod benchmark:
#   narrow: current default time window (-10m)
#   wide:   1h window to see how data volume affects overhead
SINGLE_SCENARIOS = [
    ("narrow [-10m]", "-10m"),
    ("wide   [-5h]",  "-5h"),
]

# Extra CLI args appended only in the wide scenario, per benchmark
WIDE_EXTRAS = {
    # Jaeger default limit=20 would cap data at 20 traces even over 1h
    "jaeger_traces":   ["--limit", "200"],
    # OpenSearch default limit=100 would cap logs even over 1h
    "opensearch_logs": ["--limit", "1000"],
}

# Available benchmarks with their script and extra args
BENCHMARKS = {
    # --- Single-pod benchmarks ---
    "get_time_series": {
        "script": "benchmark_get_time_series.py",
        "group": "single",
        "args": [
            "--prometheus-url", "{prometheus_url}",
        ],
    },
    "jaeger_traces": {
        "script": "benchmark_jaeger_traces.py",
        "group": "single",
        "args": [
            "--jaeger-url", "{jaeger_url}",
        ],
    },
    "opensearch_logs": {
        "script": "benchmark_opensearch_logs.py",
        "group": "single",
        "args": [
            "--opensearch-url", "{opensearch_url}",
        ],
    },
    # --- Batch benchmarks ---
    "batch_time_series": {
        "script": "benchmark_batch_time_series.py",
        "group": "batch",
        "args": [
            "--prometheus-url", "{prometheus_url}",
        ],
    },
    "batch_jaeger_traces": {
        "script": "benchmark_batch_jaeger_traces.py",
        "group": "batch",
        "args": [
            "--jaeger-url", "{jaeger_url}",
        ],
    },
    "batch_opensearch_logs": {
        "script": "benchmark_batch_opensearch_logs.py",
        "group": "batch",
        "args": [
            "--opensearch-url", "{opensearch_url}",
        ],
    },
    # --- Analysis benchmarks ---
    "correlation_scaling": {
        "script": "benchmark_correlation_scaling.py",
        "group": "analysis",
        "args": [],
    },
    "changepoint_scaling": {
        "script": "benchmark_changepoint_scaling.py",
        "group": "analysis",
        "args": [],
    }
}

""",
       # --- Graph build ---
       "graph_build": {
           "script": "benchmark_graph_build.py",
           "group": "build",
           "args": [
               "--jaeger-url", "{jaeger_url}",
               "--prometheus-url", "{prometheus_url}",
           ],
       },"""

SUITES = {
    "single": ["get_time_series", "jaeger_traces", "opensearch_logs"],
    "batch": ["batch_time_series", "batch_jaeger_traces", "batch_opensearch_logs"],
    "analysis": ["correlation_scaling", "changepoint_scaling"],
    "all": list(BENCHMARKS.keys()),
}


def wait_for_neo4j(uri, password, max_wait=180):
    """Wait until Neo4j is reachable."""
    print(f"Waiting for Neo4j at {uri} ...")
    from neo4j import GraphDatabase
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            driver = GraphDatabase.driver(uri, auth=("neo4j", password))
            driver.verify_connectivity()
            driver.close()
            print("Neo4j is ready.")
            return True
        except Exception:
            time.sleep(5)
    print("ERROR: Neo4j not reachable within timeout.")
    return False


def run_benchmark(name, config, defaults, extra_args=None):
    """Run a single benchmark script."""
    script = os.path.join("/benchmarks", config["script"])
    base_args = [
        sys.executable, script,
        "--neo4j-uri", defaults["neo4j_uri"],
        "--neo4j-password", defaults["neo4j_password"],
        "--output-dir", defaults["output_dir"],
    ]

    extra = [a.format(**defaults) for a in config["args"]]
    if extra_args:
        extra += extra_args

    cmd = base_args + extra
    print(f"\n{'='*70}")
    print(f"BENCHMARK: {name}")
    print(f"CMD: {' '.join(cmd)}")
    print(f"{'='*70}\n", flush=True)

    result = subprocess.run(cmd, capture_output=False)

    if result.returncode != 0:
        print(f"\nWARNING: {name} exited with code {result.returncode}")
    else:
        print(f"\n{name} completed successfully.")

    return result.returncode


def main():
    parser = argparse.ArgumentParser(description="Run benchmarks inside Kubernetes")
    parser.add_argument("--suite", default="all",
                        choices=list(SUITES.keys()) + ["pick"],
                        help="Which suite to run (default: single)")
    parser.add_argument("--benchmarks", nargs="+", default=[],
                        choices=list(BENCHMARKS.keys()),
                        help="Specific benchmarks when --suite=pick")
    parser.add_argument("--neo4j-uri", default=DEFAULTS["neo4j_uri"])
    parser.add_argument("--neo4j-password", default=DEFAULTS["neo4j_password"])
    parser.add_argument("--prometheus-url", default=DEFAULTS["prometheus_url"])
    parser.add_argument("--jaeger-url", default=DEFAULTS["jaeger_url"])
    parser.add_argument("--opensearch-url", default=DEFAULTS["opensearch_url"])
    parser.add_argument("--output-dir", default=DEFAULTS["output_dir"])
    args = parser.parse_args()

    defaults = {
        "neo4j_uri": args.neo4j_uri,
        "neo4j_password": args.neo4j_password,
        "prometheus_url": args.prometheus_url,
        "jaeger_url": args.jaeger_url,
        "opensearch_url": args.opensearch_url,
        "output_dir": args.output_dir,
    }

    if args.suite == "pick":
        if not args.benchmarks:
            print("ERROR: --suite=pick requires --benchmarks")
            sys.exit(1)
        to_run = args.benchmarks
    else:
        to_run = SUITES[args.suite]

    print(f"Suite: {args.suite}")
    print(f"Benchmarks: {to_run}")
    print(f"Neo4j: {defaults['neo4j_uri']}")
    print(f"Output: {defaults['output_dir']}")

    if not wait_for_neo4j(defaults["neo4j_uri"], defaults["neo4j_password"]):
        sys.exit(1)

    results = {}
    for name in to_run:
        config = BENCHMARKS[name]
        if config["group"] == "single":
            for label, range_str in SINGLE_SCENARIOS:
                extra = [f"--range={range_str}"]
                if "wide" in label:
                    extra += WIDE_EXTRAS.get(name, [])
                rc = run_benchmark(name, config, defaults, extra_args=extra)
                results[f"{name} [{label}]"] = rc
        else:
            rc = run_benchmark(name, config, defaults)
            results[name] = rc

    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    for name, rc in results.items():
        status = "OK" if rc == 0 else f"FAILED (exit {rc})"
        print(f"  {name:<44s} {status}")

    # List result CSVs
    output_dir = defaults["output_dir"]
    if os.path.isdir(output_dir):
        csvs = sorted(f for f in os.listdir(output_dir) if f.endswith(".csv"))
        if csvs:
            print(f"\nResult CSVs in {output_dir}:")
            for c in csvs:
                path = os.path.join(output_dir, c)
                size = os.path.getsize(path)
                print(f"  {c} ({size} bytes)")

                # Print CSV contents to stdout for kubectl logs
                print(f"\n--- {c} ---")
                with open(path) as f:
                    print(f.read())
                print(f"--- end {c} ---\n")


if __name__ == "__main__":
    main()
