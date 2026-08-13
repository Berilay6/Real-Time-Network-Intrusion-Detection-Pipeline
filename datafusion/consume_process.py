"""
Reads messages from Kafka, builds an N-message micro-batch, processes it with
DataFusion (aggregation + a simple anomaly rule), and writes the results to
Cassandra.

Since DataFusion has no native Kafka streaming source, this script uses a
micro-batching approach: the consumer accumulates messages, and once a batch
is full, hands it to DataFusion as a DataFrame.
"""

import argparse
import json
import pandas as pd
from datetime import datetime, timezone
from confluent_kafka import Consumer
from datafusion import SessionContext
from cassandra.cluster import Cluster
import uuid

KAFKA_BOOTSTRAP = "localhost:9092,localhost:9093,localhost:9094"
TOPIC = "network-traffic"

CASSANDRA_CONTACT_POINTS = ["127.0.0.1", "127.0.0.2"]
KEYSPACE = "intrusion_detection"

WINDOW_SIZE = "second"  


def get_cassandra_session():
    cluster = Cluster(CASSANDRA_CONTACT_POINTS)
    session = cluster.connect(KEYSPACE)
    return cluster, session


def process_batch(rows, ctx, cassandra_session):
    df = pd.DataFrame(rows)

    # Cast numeric columns
    df["Destination Port"] = pd.to_numeric(df["Destination Port"], errors="coerce")
    df["Flow Duration"] = pd.to_numeric(df["Flow Duration"], errors="coerce")
    df["Total Length of Fwd Packets"] = pd.to_numeric(df["Total Length of Fwd Packets"], errors="coerce")

    df["Injected_Timestamp"] = pd.to_datetime(
        df["Injected_Timestamp"], utc=True, format="ISO8601", errors="coerce"
    )

    batch_ctx = SessionContext()
    batch_ctx.from_pandas(df, "batch")

    # Simple rule-based anomaly filtering: flag anything that isn't BENIGN
    anomalies = batch_ctx.sql("""
        SELECT "Destination Port", "Flow Duration",
               "Total Length of Fwd Packets", "Label", "Injected_Timestamp"
        FROM batch
        WHERE "Label" != 'BENIGN'
    """).to_pandas()

    # Tumbling window: round Injected_Timestamp down to WINDOW_SIZE and aggregate per window + port. 
    metrics = batch_ctx.sql(f"""
        SELECT date_trunc('{WINDOW_SIZE}', "Injected_Timestamp") as window_start,
               "Destination Port" as dst_port,
               COUNT(*) as total_flows,
               SUM(CASE WHEN "Label" = 'BENIGN' THEN 1 ELSE 0 END) as benign_count,
               SUM(CASE WHEN "Label" != 'BENIGN' THEN 1 ELSE 0 END) as attack_count,
               AVG("Flow Duration") as avg_flow_duration
        FROM batch
        GROUP BY date_trunc('{WINDOW_SIZE}', "Injected_Timestamp"), "Destination Port"
    """).to_pandas()

    window_delta = pd.Timedelta(**{WINDOW_SIZE + "s": 1})

    batch_id = uuid.uuid4()

    for _, row in anomalies.iterrows():
        event_time = row["Injected_Timestamp"]
        cassandra_session.execute("""
            INSERT INTO flagged_anomalies
            (detection_id, dst_port, event_time, flow_duration, label, total_fwd_pkt_len)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            uuid.uuid4(),
            int(row["Destination Port"]) if pd.notna(row["Destination Port"]) else 0,
            event_time.to_pydatetime() if pd.notna(event_time) else datetime.now(timezone.utc),
            int(row["Flow Duration"]) if pd.notna(row["Flow Duration"]) else 0,
            row["Label"],
            float(row["Total Length of Fwd Packets"]) if pd.notna(row["Total Length of Fwd Packets"]) else 0.0,
        ))

    for _, row in metrics.iterrows():
        window_start = row["window_start"]
        window_end = window_start + window_delta if pd.notna(window_start) else None
        cassandra_session.execute("""
            INSERT INTO windowed_traffic_metrics
            (dst_port, window_start, batch_id, window_end, total_flows, benign_count, attack_count, avg_flow_duration)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            int(row["dst_port"]) if pd.notna(row["dst_port"]) else 0,
            window_start.to_pydatetime() if pd.notna(window_start) else datetime.now(timezone.utc),
            batch_id,
            window_end.to_pydatetime() if window_end is not None else datetime.now(timezone.utc),
            int(row["total_flows"]),
            int(row["benign_count"]),
            int(row["attack_count"]),
            float(row["avg_flow_duration"]) if pd.notna(row["avg_flow_duration"]) else 0.0,
        ))

    print(f"  Batch processed: {len(rows)} msgs, {len(anomalies)} anomalies, "
          f"{len(metrics)} window/port groups written to Cassandra", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--max-batches", type=int, default=None, help="For testing: stop after N batches")
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
    print("Consumer started, waiting for messages...", flush=True)

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
                print(f"Kafka error: {msg.error()}")
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
        print(f"\nDone. Total batches: {batch_count}")


if __name__ == "__main__":
    main()