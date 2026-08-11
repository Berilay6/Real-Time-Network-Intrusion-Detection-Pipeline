"""
Demonstrates how DataFusion distributes query execution across CPU cores.

DataFusion parallelizes query execution via `target_partitions`: a query's
physical plan is split into N independent execution partitions (N defaults
to the number of CPU cores), each processed concurrently by DataFusion's
Rust-native thread pool. This script shows both sides of that claim:

  1. The physical plan (via .explain()), showing RepartitionExec stages and
     the partition count DataFusion chose to use.
  2. Real, per-core CPU utilization sampled while a moderately expensive
     aggregation query runs, showing multiple cores actually active at once
     (not just configured to be available).

"""

import argparse
import os
import threading
import time

import psutil
from datafusion import SessionContext, SessionConfig

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MPL = True
except ImportError:
    HAS_MPL = False


def sample_cpu_during(fn, interval=0.1):
    """Runs fn() in a background thread while sampling per-core CPU% on the
    main thread. Returns (result, list_of_percpu_samples)."""
    samples = []
    result = {}

    def worker():
        result["value"] = fn()

    t = threading.Thread(target=worker)
    psutil.cpu_percent(percpu=True)  # prime the first (meaningless) reading
    t.start()
    while t.is_alive():
        samples.append(psutil.cpu_percent(percpu=True, interval=interval))
    t.join()
    return result.get("value"), samples


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="Path to a cleaned CICIDS2017 CSV")
    args = parser.parse_args()

    n_cores = os.cpu_count()
    print(f"Detected {n_cores} logical CPU cores.")

    cfg = SessionConfig().with_target_partitions(n_cores)
    ctx = SessionContext(cfg)
    ctx.register_csv("traffic", args.file)

    query = """
        SELECT "Destination Port" as dst_port,
               COUNT(*) as total_flows,
               AVG("Flow Duration") as avg_duration,
               SUM("Total Length of Fwd Packets") as total_fwd_bytes
        FROM traffic
        GROUP BY "Destination Port"
        ORDER BY total_flows DESC
    """

    df = ctx.sql(query)

    print("\n=== Physical Execution Plan ===")
    df.explain()

    print(f"\n=== Running query with target_partitions={n_cores}, sampling per-core CPU usage ===")

    def run_query():
        return df.to_pandas()

    result, samples = sample_cpu_during(run_query)
    print(f"\nQuery returned {len(result)} rows.")
    print(f"Collected {len(samples)} CPU samples during execution.")

    if samples:
        per_core_max = [max(s[i] for s in samples) for i in range(n_cores)]
        print("\nPer-core peak utilization during query execution:")
        for i, pct in enumerate(per_core_max):
            bar = "#" * int(pct / 4)
            print(f"  core {i:2d}: {pct:5.1f}%  {bar}")

        active_cores = sum(1 for p in per_core_max if p > 25)
        print(f"\n{active_cores} / {n_cores} cores exceeded 25% utilization during this query.")

        if HAS_MPL:
            plt.figure(figsize=(8, 4))
            plt.bar(range(n_cores), per_core_max, color="#4C72B0")
            plt.xlabel("CPU core")
            plt.ylabel("Peak utilization (%)")
            plt.title(f"DataFusion query execution across {n_cores} CPU cores\n(target_partitions={n_cores})")
            plt.ylim(0, 100)
            plt.xticks(range(n_cores))
            plt.tight_layout()
            out_path = "docs/datafusion_core_utilization.png"
            plt.savefig(out_path, dpi=150)
            print(f"\nSaved chart to {out_path}")


if __name__ == "__main__":
    main()