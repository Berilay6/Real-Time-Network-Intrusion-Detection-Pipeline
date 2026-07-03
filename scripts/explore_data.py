"""
Ham CICIDS2017 verisi (*.csv) üzerinde keşif / ön inceleme yapar.
Bellek dostu (chunking) okuma kullanarak satır/kolon sayısı, label dağılımı,
port dağılımı, eksik (NaN/Inf) veri oranları ve timestamp injection performansını ölçer.
"""

import pandas as pd
import numpy as np
import glob
import os
import time

# Dosyalarının bulunduğu konuma göre burayı ayarlayabilirsin
RAW_DIR = "data" 
OUT_PATH = "docs/raw_data_exploration.txt"
CHUNK_SIZE = 50_000
ENCODING = "cp1252"

def explore_file(path: str):
    """Dosyayı chunk'lı okuyup tüm analiz metriklerini topluyorum."""
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

        # 1. Eksik ve Bozuk Veri Analizi
        chunk.replace([np.inf, -np.inf], np.nan, inplace=True)
        missing_rows_in_chunk = chunk.isna().any(axis=1).sum()
        total_missing_rows += missing_rows_in_chunk

        eksikler = chunk.isna().sum()
        for sutun, sayi in eksikler[eksikler > 0].items():
            missing_cols[sutun] = missing_cols.get(sutun, 0) + sayi

        # 2. Label Dağılımı
        if "Label" in chunk.columns:
            label_counts = label_counts.add(chunk["Label"].value_counts(), fill_value=0)

        # 3. Port Dağılımı
        if "Destination Port" in chunk.columns:
            port_counts = port_counts.add(chunk["Destination Port"].value_counts(), fill_value=0)

        # 4. Timestamp Injection Performans Testi
        start_time = time.time()
        start_ms = int(time.time() * 1000)
        chunk["Injected_Timestamp"] = [start_ms + i for i in range(chunk_len)]
        total_injection_time += (time.time() - start_time)

        print(f"    ... {total_rows:,} satır işlendi", flush=True)
        del chunk

    return total_rows, n_cols, label_counts, port_counts, missing_cols, total_missing_rows, total_injection_time

def main():
    os.makedirs("docs", exist_ok=True)
    files = sorted(glob.glob(f"{RAW_DIR}/*.csv"))

    if not files:
        print(f"HATA: {RAW_DIR}/ dizininde CSV bulunamadı. Dizin yolunu kontrol et.")
        return

    lines = [f"Bulunan dosya sayısı: {len(files)}\n"]
    
    # --- GENEL TOPLAM DEĞİŞKENLERİ ---
    grand_total_rows = 0
    grand_total_missing = 0
    grand_total_inj_time = 0
    global_labels = pd.Series(dtype=int)
    global_ports = pd.Series(dtype=int)
    global_missing_cols = {}

    for i, f in enumerate(files, 1):
        print(f"\n[{i}/{len(files)}] İnceleniyor: {f}", flush=True)
        try:
            n_rows, n_cols, labels, ports, missing_cols, missing_rows, inj_time = explore_file(f)
        except Exception as e:
            print(f"  HATA: {e}", flush=True)
            lines.append(f"{f}: HATA - {e}\n")
            continue

        # Bireysel dosya verilerini genel toplama ekliyorum
        grand_total_rows += n_rows
        grand_total_missing += missing_rows
        grand_total_inj_time += inj_time
        
        if not labels.empty:
            global_labels = global_labels.add(labels, fill_value=0)
        if not ports.empty:
            global_ports = global_ports.add(ports, fill_value=0)
        for col, count in missing_cols.items():
            global_missing_cols[col] = global_missing_cols.get(col, 0) + count

        # Dosya bazlı sonuçları formatlıyorum
        lines.append(f"=== {os.path.basename(f)} ===")
        lines.append(f"  Satır: {n_rows:,}  |  Kolon: {n_cols}")
        lines.append(f"  Eksik/Bozuk Satır: {missing_rows:,} (%{(missing_rows/n_rows)*100 if n_rows > 0 else 0:.4f})")
        lines.append(f"  Zaman Atama Süresi: {inj_time:.4f} sn")
        lines.append("-" * 40)

    # --- GENEL TOPLAM SONUÇLARIN YAZDIRILMASI ---
    lines.append("\n" + "=" * 50)
    lines.append("=== GENEL TOPLAM SONUÇLAR (TÜM DOSYALAR) ===")
    lines.append("=" * 50)
    lines.append(f"Toplam İncelenen Dosya: {len(files)}")
    lines.append(f"Toplam Satır: {grand_total_rows:,}")
    
    bozuk_oran = (grand_total_missing / grand_total_rows) * 100 if grand_total_rows > 0 else 0
    lines.append(f"Toplam Eksik/Bozuk Satır: {grand_total_missing:,} (%{bozuk_oran:.4f})")
    
    ortalama_hiz = grand_total_rows / grand_total_inj_time if grand_total_inj_time > 0 else 0
    lines.append(f"Toplam Zaman Atama (Injection) Süresi: {grand_total_inj_time:.4f} sn")
    lines.append(f"Ortalama Üretim Hızı (Throughput): {ortalama_hiz:.0f} satır/sn")

    if global_missing_cols:
        lines.append("\n--- GENEL EKSİK İÇEREN SÜTUNLAR ---")
        for col, count in sorted(global_missing_cols.items(), key=lambda x: x[1], reverse=True)[:5]:
            lines.append(f"  - {col}: {int(count):,} adet")

    if not global_labels.empty:
        lines.append("\n--- GENEL LABEL DAĞILIMI ---")
        for label, count in global_labels.sort_values(ascending=False).items():
            yuzde = (count / grand_total_rows) * 100 if grand_total_rows > 0 else 0
            lines.append(f"  - {label}: {int(count):,} (%{yuzde:.4f})")

    if not global_ports.empty:
        lines.append("\n--- GENEL EN ÇOK KULLANILAN İLK 5 PORT ---")
        for port, count in global_ports.sort_values(ascending=False).head(5).items():
            yuzde = (count / grand_total_rows) * 100 if grand_total_rows > 0 else 0
            lines.append(f"  - Port {int(port)}: {int(count):,} kayıt (%{yuzde:.2f})")

    output = "\n".join(lines)
    print("\n" + output)

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(output)
    print(f"\nÖzet başarıyla kaydedildi: {OUT_PATH}")

if __name__ == "__main__":
    main()