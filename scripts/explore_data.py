"""
Performs exploratory data analysis on raw CICIDS2017 data (*.csv).
"""

import pandas as pd
import numpy as np
import glob
import os
import time

RAW_DIR = "data" 
OUT_PATH = "docs/raw_data_exploration.txt"
CHUNK_SIZE = 50_000
ENCODING = "cp1252"

def explore_file(path: str):
    """Reads the file in chunks and collects all analysis metrics."""
    total_rows = 0
    n_cols = None
    label_counts = pd.Series(dtype=int)
    port_counts = pd.Series(dtype=int)
    
    missing_cols = {}
    total_missing_rows = 0
    total_injection_time = 0

    for chunk in pd.read_csv(path, encoding=ENCODING, low_memory=False, chunksize=CHUNK_SIZE):
        chunk.columns = chunk.columns.str.strip()
        if n_cols is None:
            n_cols = len(chunk.columns)

        chunk_len = len(chunk)
        total_rows += chunk_len

        # 1. Missing and Corrupted Data Analysis
        chunk.replace([np.inf, -np.inf], np.nan, inplace=True)
        missing_rows_in_chunk = chunk.isna().any(axis=1).sum()
        total_missing_rows += missing_rows_in_chunk

        missing_data = chunk.isna().sum()
        for col, count in missing_data[missing_data > 0].items():
            missing_cols[col] = missing_cols.get(col, 0) + count

        # 2. Label Distribution
        if "Label" in chunk.columns:
            label_counts = label_counts.add(chunk["Label"].value_counts(), fill_value=0)

        # 3. Port Distribution
        if "Destination Port" in chunk.columns:
            port_counts = port_counts.add(chunk["Destination Port"].value_counts(), fill_value=0)

        # 4. Timestamp Injection Performance Test
        start_time = time.time()
        start_ms = int(time.time() * 1000)
        chunk["Injected_Timestamp"] = [start_ms + i for i in range(chunk_len)]
        total_injection_time += (time.time() - start_time)

        print(f"    ... {total_rows:,} rows processed", flush=True)
        del chunk

    return total_rows, n_cols, label_counts, port_counts, missing_cols, total_missing_rows, total_injection_time

def main():
    os.makedirs("docs", exist_ok=True)
    files = sorted(glob.glob(f"{RAW_DIR}/*.csv"))

    if not files:
        print(f"ERROR: No CSV files found in {RAW_DIR}/ directory. Please check the path.")
        return

    lines = [f"Number of files found: {len(files)}\n"]
    
    # --- GRAND TOTAL VARIABLES ---
    grand_total_rows = 0
    grand_total_missing = 0
    grand_total_inj_time = 0
    global_labels = pd.Series(dtype=int)
    global_ports = pd.Series(dtype=int)
    global_missing_cols = {}

    for i, f in enumerate(files, 1):
        print(f"\n[{i}/{len(files)}] Analyzing: {f}", flush=True)
        try:
            n_rows, n_cols, labels, ports, missing_cols, missing_rows, inj_time = explore_file(f)
        except Exception as e:
            print(f"  ERROR: {e}", flush=True)
            lines.append(f"{f}: ERROR - {e}\n")
            continue

        # Adding individual file data to the grand totals
        grand_total_rows += n_rows
        grand_total_missing += missing_rows
        grand_total_inj_time += inj_time
        
        if not labels.empty:
            global_labels = global_labels.add(labels, fill_value=0)
        if not ports.empty:
            global_ports = global_ports.add(ports, fill_value=0)
        for col, count in missing_cols.items():
            global_missing_cols[col] = global_missing_cols.get(col, 0) + count

        # Formatting file-based results
        lines.append(f"=== {os.path.basename(f)} ===")
        lines.append(f"  Rows: {n_rows:,}  |  Columns: {n_cols}")
        lines.append(f"  Missing/Corrupted Rows: {missing_rows:,} ({ (missing_rows/n_rows)*100 if n_rows > 0 else 0:.4f}%)")
        lines.append(f"  Timestamp Injection Duration: {inj_time:.4f} sec")
        lines.append("-" * 40)

    # --- PRINTING GRAND TOTAL RESULTS ---
    lines.append("\n" + "=" * 50)
    lines.append("=== GRAND TOTAL RESULTS (ALL FILES) ===")
    lines.append("=" * 50)
    lines.append(f"Total Files Analyzed: {len(files)}")
    lines.append(f"Total Rows: {grand_total_rows:,}")
    
    corrupted_rate = (grand_total_missing / grand_total_rows) * 100 if grand_total_rows > 0 else 0
    lines.append(f"Total Missing/Corrupted Rows: {grand_total_missing:,} ({corrupted_rate:.4f}%)")
    
    avg_speed = grand_total_rows / grand_total_inj_time if grand_total_inj_time > 0 else 0
    lines.append(f"Total Injection Time: {grand_total_inj_time:.4f} sec")
    lines.append(f"Average Throughput: {avg_speed:.0f} rows/sec")

    if global_missing_cols:
        lines.append("\n--- GLOBAL COLUMNS WITH MISSING DATA (TOP 5) ---")
        for col, count in sorted(global_missing_cols.items(), key=lambda x: x[1], reverse=True)[:5]:
            lines.append(f"  - {col}: {int(count):,} counts")

    if not global_labels.empty:
        lines.append("\n--- GLOBAL LABEL DISTRIBUTION ---")
        for label, count in global_labels.sort_values(ascending=False).items():
            percentage = (count / grand_total_rows) * 100 if grand_total_rows > 0 else 0
            lines.append(f"  - {label}: {int(count):,} ({percentage:.4f}%)")

    if not global_ports.empty:
        lines.append("\n--- GLOBAL MOST USED PORTS (TOP 5) ---")
        for port, count in global_ports.sort_values(ascending=False).head(5).items():
            percentage = (count / grand_total_rows) * 100 if grand_total_rows > 0 else 0
            lines.append(f"  - Port {int(port)}: {int(count):,} records ({percentage:.2f}%)")

    output = "\n".join(lines)
    print("\n" + output)

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(output)
    print(f"\nSummary successfully saved to: {OUT_PATH}")

if __name__ == "__main__":
    main()