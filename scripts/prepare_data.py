"""
CICIDS2017 data preparation script.

What it does:
  1. Reads the 8 raw files in data/*.csv (memory-friendly, using chunking)
  2. Strips leading/trailing whitespace from column names
  3. Deduplicates duplicate column names (e.g., "Fwd Header Length") to make them unique
  4. Fixes encoding corruptions in the Label column (Web Attack types)
  5. Detects and removes corrupted rows containing missing (NaN) and infinite (Inf) values
  6. Saves the result under data/clean/ with the same filename

"""

import pandas as pd
import numpy as np
import glob
import re
import os

RAW_DIR = "data"
CLEAN_DIR = "data/clean"
CHUNK_SIZE = 50_000
ENCODING = "cp1252"  # CICIDS2017 CSVs open properly with this encoding

def clean_label(label: str) -> str:
    """Normalizes the broken en-dash characters in Web Attack labels."""
    if not isinstance(label, str):
        return label
    label = re.sub(r"Web Attack.*Brute Force", "Web Attack - Brute Force", label)
    label = re.sub(r"Web Attack.*XSS", "Web Attack - XSS", label)
    label = re.sub(r"Web Attack.*Sql Injection", "Web Attack - Sql Injection", label)
    return label

def dedup_columns(columns: pd.Index) -> pd.Index:
    """Appends suffixes like _1, _2... to duplicate column names (required for DataFusion)."""
    cols = pd.Series(columns)
    for dup in cols[cols.duplicated()].unique():
        dup_idx = cols[cols == dup].index
        for i, idx in enumerate(dup_idx):
            if i != 0:
                cols[idx] = f"{dup}_{i}"
    return pd.Index(cols)

def process_file(src_path: str, dst_path: str) -> int:
    total_cleaned = 0
    first_chunk = True
    
    for chunk in pd.read_csv(src_path, encoding=ENCODING, low_memory=False, chunksize=CHUNK_SIZE):
        # 1. Column Cleaning
        chunk.columns = chunk.columns.str.strip()
        chunk.columns = dedup_columns(chunk.columns)
        
        # 2. Label Cleaning
        if "Label" in chunk.columns:
            chunk["Label"] = chunk["Label"].apply(clean_label)

        # 3. DATA QUALITY CONTROL 
        # Convert infinite (Inf) values to NaN (Missing Data) format
        chunk.replace([np.inf, -np.inf], np.nan, inplace=True)
        # Drop all rows containing NaN values from the dataset
        chunk.dropna(inplace=True)

        # 4. Saving Cleaned Data
        chunk.to_csv(dst_path, mode="w" if first_chunk else "a",
                     header=first_chunk, index=False)
        first_chunk = False
        
        total_cleaned += len(chunk)
        print(f"    ... {total_cleaned:,} cleaned rows processed", flush=True)
        
    return total_cleaned

def main():
    os.makedirs(CLEAN_DIR, exist_ok=True)
    files = sorted(glob.glob(f"{RAW_DIR}/*.csv"))

    if not files:
        print(f"ERROR: No CSV files found under {RAW_DIR}/. Please download CICIDS2017 first.")
        return

    print(f"Number of files found: {len(files)}\n", flush=True)
    grand_total = 0

    for i, src in enumerate(files, 1):
        dst = os.path.join(CLEAN_DIR, os.path.basename(src))
        if os.path.exists(dst):
            print(f"[{i}/{len(files)}] Skipping (already exists): {dst}", flush=True)
            continue

        print(f"[{i}/{len(files)}] Processing: {src}", flush=True)
        try:
            n = process_file(src, dst)
            grand_total += n
            print(f"  -> {dst} completed ({n:,} rows)\n", flush=True)
        except Exception as e:
            print(f"  ERROR: {e}", flush=True)
            if os.path.exists(dst):
                os.remove(dst)  

    print(f"=== ALL OPERATIONS COMPLETED ===")
    print(f"Total clean rows ready to be sent to DataFusion and Kafka: {grand_total:,}\n", flush=True)

if __name__ == "__main__":
    main()