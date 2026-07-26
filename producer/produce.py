"""
Reads the CICIDS2017 dataset and streams it to Kafka in real time.
Implements the "Injected Timestamp" logic from the proposal: since the
dataset has no real-time timestamp information, the current time is
attached to each row at the moment it is sent.

Usage:
  python3 producer/produce.py --file ../data/clean/Friday-WorkingHours-Morning.pcap_ISCX.csv --rate 100 --limit 5000

  --rate  : rows sent per second (default: 50)
  --limit : total number of rows to send, for testing (default: entire file)
"""

import argparse
import csv
import json
import time
import sys
from datetime import datetime, timezone
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"
TOPIC = "network-traffic"


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="Path to the cleaned CSV file")
    parser.add_argument("--rate", type=int, default=50, help="Rows per second")
    parser.add_argument("--limit", type=int, default=None, help="Max number of rows")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    delay = 1.0 / args.rate

    sent = 0
    with open(args.file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if args.limit and sent >= args.limit:
                break

            # Injected Timestamp: simulates a real-time stream
            row["Injected_Timestamp"] = datetime.now(timezone.utc).isoformat()

            dst_port = row.get("Destination Port", "0")
            key = str(dst_port).encode("utf-8")
            value = json.dumps(row).encode("utf-8")

            producer.produce(TOPIC, key=key, value=value, callback=delivery_report)
            producer.poll(0)

            sent += 1
            if sent % 500 == 0:
                print(f"  ... {sent:,} messages sent", flush=True)

            time.sleep(delay)

    producer.flush()
    print(f"\nDone. Total sent: {sent:,} messages")


if __name__ == "__main__":
    main()