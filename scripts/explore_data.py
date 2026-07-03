"""
Ham CICIDS2017 verisi (data/*.csv) uzerinde kesif / on inceleme yapar.
prepare_data.py CALISTIRILMADAN ONCE kullanilir - amac ham veriyi
tanimak (satir/kolon sayisi, label dagilimi, encoding/format sorunlari)
ve buradan cikan bulgularla cleaning mantigini tasarlamak.

Kullanim:
  python3 scripts/explore_data.py

Cikti:
  - Ekrana basar (ilerleme dahil, chunk'li okuma ile bellek dostu)
  - docs/raw_data_exploration.txt dosyasina kaydeder
"""

import pandas as pd
import glob
import os

RAW_DIR = "data"
OUT_PATH = "docs/raw_data_exploration.txt"
CHUNK_SIZE = 50_000
ENCODING = "cp1252"


def explore_file(path: str):
    """Dosyayi chunk'li okuyup satir sayisi ve label dagilimini toplar."""
    total_rows = 0
    n_cols = None
    label_counts = None

    for chunk in pd.read_csv(path, encoding=ENCODING, low_memory=False, chunksize=CHUNK_SIZE):
        chunk.columns = chunk.columns.str.strip()
        if n_cols is None:
            n_cols = len(chunk.columns)

        total_rows += len(chunk)

        if "Label" in chunk.columns:
            counts = chunk["Label"].value_counts()
            label_counts = counts if label_counts is None else label_counts.add(counts, fill_value=0)

        print(f"    ... {total_rows:,} satir okundu", flush=True)
        del chunk

    return total_rows, n_cols, label_counts


def main():
    os.makedirs("docs", exist_ok=True)
    files = sorted(glob.glob(f"{RAW_DIR}/*.csv"))

    if not files:
        print(f"HATA: {RAW_DIR}/ altinda CSV bulunamadi. Once CICIDS2017'yi indirin.")
        return

    lines = [f"Bulunan dosya sayisi: {len(files)}\n"]
    grand_total = 0

    for i, f in enumerate(files, 1):
        print(f"[{i}/{len(files)}] Isleniyor: {f}", flush=True)
        try:
            n_rows, n_cols, label_counts = explore_file(f)
        except Exception as e:
            print(f"  HATA: {e}", flush=True)
            lines.append(f"{f}: HATA - {e}\n")
            continue

        grand_total += n_rows
        lines.append(f"{f}")
        lines.append(f"  Satir: {n_rows:,}  Kolon: {n_cols}")
        if label_counts is not None:
            lines.append(label_counts.astype(int).to_string())
        lines.append("")

    lines.append(f"=== TOPLAM SATIR (tum dosyalar): {grand_total:,} ===")

    output = "\n".join(lines)
    print("\n" + output)

    with open(OUT_PATH, "w") as f:
        f.write(output)
    print(f"\nOzet kaydedildi: {OUT_PATH}")


if __name__ == "__main__":
    main()
