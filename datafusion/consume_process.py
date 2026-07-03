"""
Kafka'dan mesaj okur, N mesajlik micro-batch olusturur, DataFusion ile
isler (agregasyon + basit anomali kurali), sonuclari Cassandra'ya yazar.

DataFusion'in native Kafka streaming destegi olmadigi icin bu script
micro-batching yaklasimi kullanir: consumer mesajlari toplar, batch
doldukca DataFusion'a bir DataFrame olarak verir.

Kullanim:
  python3 datafusion/consume_process.py --batch-size 50 --max-batches 5
"""

import argparse
import json
import pandas as pd
from datetime import datetime, timezone
from confluent_kafka import Consumer
from datafusion import SessionContext
from cassandra.cluster import Cluster
import uuid

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "network-traffic"
CASSANDRA_HOST = "localhost"
KEYSPACE = "intrusion_detection"


def get_cassandra_session():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)
    return cluster, session


def process_batch(rows, ctx, cassandra_session):
    df = pd.DataFrame(rows)

    # Sayisal kolonlari cast et (Kafka'dan hepsi string geliyor)
    df["Destination Port"] = pd.to_numeric(df["Destination Port"], errors="coerce")
    df["Flow Duration"] = pd.to_numeric(df["Flow Duration"], errors="coerce")
    df["Total Length of Fwd Packets"] = pd.to_numeric(df["Total Length of Fwd Packets"], errors="coerce")

    batch_ctx = SessionContext()
    batch_ctx.from_pandas(df, "batch")

    # Basit kural tabanli anomali filtreleme: BENIGN olmayanlari isaretle
    anomalies = batch_ctx.sql("""
        SELECT "Destination Port", "Flow Duration",
               "Total Length of Fwd Packets", "Label"
        FROM batch
        WHERE "Label" != 'BENIGN'
    """).to_pandas()

    metrics = batch_ctx.sql("""
        SELECT "Destination Port" as dst_port,
               COUNT(*) as total_flows,
               SUM(CASE WHEN "Label" = 'BENIGN' THEN 1 ELSE 0 END) as benign_count,
               SUM(CASE WHEN "Label" != 'BENIGN' THEN 1 ELSE 0 END) as attack_count,
               AVG("Flow Duration") as avg_flow_duration
        FROM batch
        GROUP BY "Destination Port"
    """).to_pandas()

    now = datetime.now(timezone.utc)

    for _, row in anomalies.iterrows():
        cassandra_session.execute("""
            INSERT INTO flagged_anomalies
            (detection_id, dst_port, event_time, flow_duration, label, total_fwd_pkt_len)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            uuid.uuid4(),
            int(row["Destination Port"]) if pd.notna(row["Destination Port"]) else 0,
            now,
            int(row["Flow Duration"]) if pd.notna(row["Flow Duration"]) else 0,
            row["Label"],
            float(row["Total Length of Fwd Packets"]) if pd.notna(row["Total Length of Fwd Packets"]) else 0.0,
        ))

    for _, row in metrics.iterrows():
        cassandra_session.execute("""
            INSERT INTO windowed_traffic_metrics
            (dst_port, window_start, window_end, total_flows, benign_count, attack_count, avg_flow_duration)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            int(row["dst_port"]) if pd.notna(row["dst_port"]) else 0,
            now, now,
            int(row["total_flows"]),
            int(row["benign_count"]),
            int(row["attack_count"]),
            float(row["avg_flow_duration"]) if pd.notna(row["avg_flow_duration"]) else 0.0,
        ))

    print(f"  Batch islendi: {len(rows)} mesaj, {len(anomalies)} anomali, {len(metrics)} port grubu Cassandra'ya yazildi", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--max-batches", type=int, default=None, help="Test icin: kac batch sonra dur")
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "datafusion-processor",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    ctx = SessionContext()
    cluster, cassandra_session = get_cassandra_session()

    buffer = []
    batch_count = 0
    print("Consumer basladi, mesaj bekleniyor...", flush=True)

    try:
        while True:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                if buffer:
                    process_batch(buffer, ctx, cassandra_session)
                    buffer = []
                    batch_count += 1
                    if args.max_batches and batch_count >= args.max_batches:
                        break
                continue
            if msg.error():
                print(f"Kafka hatasi: {msg.error()}")
                continue

            row = json.loads(msg.value().decode("utf-8"))
            buffer.append(row)

            if len(buffer) >= args.batch_size:
                process_batch(buffer, ctx, cassandra_session)
                buffer = []
                batch_count += 1
                if args.max_batches and batch_count >= args.max_batches:
                    break
    finally:
        consumer.close()
        cluster.shutdown()
        print(f"\nTamamlandi. Toplam batch: {batch_count}")


if __name__ == "__main__":
    main()
