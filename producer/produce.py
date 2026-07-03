"""
CICIDS2017 verisini okuyup Kafka'ya gercek zamanli stream olarak gonderir.
Proposal'daki "Injected Timestamp" mantigini uygular: veri setinde gercek
zaman bilgisi olmadigi icin, gonderim aninda her satira guncel timestamp
eklenir.

Kullanim:
  python3 producer/produce.py --file ../data/clean/Friday-WorkingHours-Morning.pcap_ISCX.csv --rate 100 --limit 5000

  --rate  : saniyede kac satir gonderilecek (varsayilan: 50)
  --limit : toplam kac satir gonderilecek, test icin (varsayilan: tum dosya)
"""

import argparse
import csv
import json
import time
import sys
from datetime import datetime, timezone
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "network-traffic"


def delivery_report(err, msg):
    if err is not None:
        print(f"Gonderim hatasi: {err}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="Temizlenmis CSV dosya yolu")
    parser.add_argument("--rate", type=int, default=50, help="Saniyede kac satir")
    parser.add_argument("--limit", type=int, default=None, help="Maks satir sayisi")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    delay = 1.0 / args.rate

    sent = 0
    with open(args.file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if args.limit and sent >= args.limit:
                break

            # Injected Timestamp: gercek zamanli stream simulasyonu
            row["Injected_Timestamp"] = datetime.now(timezone.utc).isoformat()

            dst_port = row.get("Destination Port", "0")
            key = str(dst_port).encode("utf-8")
            value = json.dumps(row).encode("utf-8")

            producer.produce(TOPIC, key=key, value=value, callback=delivery_report)
            producer.poll(0)

            sent += 1
            if sent % 500 == 0:
                print(f"  ... {sent:,} mesaj gonderildi", flush=True)

            time.sleep(delay)

    producer.flush()
    print(f"\nTamamlandi. Toplam gonderilen: {sent:,} mesaj")


if __name__ == "__main__":
    main()
