"""
CICIDS2017 veri hazirlik scripti.

Ne yapar:
  1. data/*.csv icindeki 8 ham dosyayi okur (chunk'li, bellek dostu)
  2. Kolon adlarindaki bastaki/sondaki bosluklari temizler
  3. Tekrarli kolon adlarini (orn. "Fwd Header Length") benzersizlestirir
  4. Label kolonundaki encoding bozukluklarini (Web Attack turleri) duzeltir
  5. Sonucu data/clean/ altina, ayni dosya adiyla kaydeder

Kullanim:
  python3 scripts/prepare_data.py

Not: data/ klasoru .gitignore'da, bu yuzden ham CSV'ler repoya girmez.
Her ekip uyesi CICIDS2017'yi kendi indirip data/ altina koymali:
  https://www.kaggle.com/datasets/chethuhn/network-intrusion-dataset
"""

import pandas as pd
import glob
import re
import os

RAW_DIR = "data"
CLEAN_DIR = "data/clean"
CHUNK_SIZE = 50_000
ENCODING = "cp1252"  # CICIDS2017 CSV'leri bu encoding ile duzgun aciliyor


def clean_label(label: str) -> str:
    """Web Attack etiketlerindeki bozuk en-dash karakterini normalize eder."""
    if not isinstance(label, str):
        return label
    label = re.sub(r"Web Attack.*Brute Force", "Web Attack - Brute Force", label)
    label = re.sub(r"Web Attack.*XSS", "Web Attack - XSS", label)
    label = re.sub(r"Web Attack.*Sql Injection", "Web Attack - Sql Injection", label)
    return label


def dedup_columns(columns: pd.Index) -> pd.Index:
    """Tekrarli kolon adlarina _1, _2... son eki ekler (DataFusion icin gerekli)."""
    cols = pd.Series(columns)
    for dup in cols[cols.duplicated()].unique():
        dup_idx = cols[cols == dup].index
        for i, idx in enumerate(dup_idx):
            if i != 0:
                cols[idx] = f"{dup}_{i}"
    return pd.Index(cols)


def process_file(src_path: str, dst_path: str) -> int:
    total = 0
    first_chunk = True
    for chunk in pd.read_csv(src_path, encoding=ENCODING, low_memory=False, chunksize=CHUNK_SIZE):
        chunk.columns = chunk.columns.str.strip()
        chunk.columns = dedup_columns(chunk.columns)
        if "Label" in chunk.columns:
            chunk["Label"] = chunk["Label"].apply(clean_label)

        chunk.to_csv(dst_path, mode="w" if first_chunk else "a",
                      header=first_chunk, index=False)
        first_chunk = False
        total += len(chunk)
        print(f"    ... {total:,} satir islendi", flush=True)
    return total


def main():
    os.makedirs(CLEAN_DIR, exist_ok=True)
    files = sorted(glob.glob(f"{RAW_DIR}/*.csv"))

    if not files:
        print(f"HATA: {RAW_DIR}/ altinda CSV bulunamadi. Once CICIDS2017'yi indirin.")
        return

    print(f"Bulunan dosya sayisi: {len(files)}", flush=True)
    grand_total = 0

    for i, src in enumerate(files, 1):
        dst = os.path.join(CLEAN_DIR, os.path.basename(src))
        if os.path.exists(dst):
            print(f"[{i}/{len(files)}] Atlaniyor (zaten var): {dst}", flush=True)
            continue

        print(f"[{i}/{len(files)}] Isleniyor: {src}", flush=True)
        try:
            n = process_file(src, dst)
            grand_total += n
            print(f"  -> {dst} tamamlandi ({n:,} satir)", flush=True)
        except Exception as e:
            print(f"  HATA: {e}", flush=True)
            if os.path.exists(dst):
                os.remove(dst)  # yarim kalan dosyayi birakma

    print(f"\nTamamlandi. Bu calistirmada islenen satir: {grand_total:,}", flush=True)


if __name__ == "__main__":
    main()
