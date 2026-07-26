# Real-Time Network Intrusion Detection Pipeline

A locally simulated, real-time network intrusion detection pipeline built on
Apache Kafka, Apache DataFusion, and Apache Cassandra, orchestrated with
Docker Compose. CICIDS2017 network flow records are replayed as a live
stream, processed with event-time tumbling windows, and persisted for
analysis.

The project went through two architectures:

- **Single-node** — the original setup (one Kafka broker, one Cassandra
  node), used for initial development and the Week 7 status report demos.
  Simple to run, but has no fault tolerance.
- **Multi-node** — the current setup (`docker-compose.yml` in this repo):
  3 Kafka brokers (replication factor 3) and 2 Cassandra nodes (replication
  factor 2), which allows killing a broker or node mid-ingestion without
  losing data or stopping the pipeline. This is what satisfies the Systems
  Track's fault-tolerance requirement, and what all current and future work
  in this repo uses.

Both are documented below so it's clear what changed and why. Only the
multi-node setup is runnable as-is from this repo's files.

## Prerequisites

Install the Python dependencies:

```bash
pip install -r requirements.txt --break-system-packages --user
```

Download the CICIDS2017 dataset (requires a Kaggle API token configured at
`~/.kaggle/kaggle.json`):

```bash
cd data
kaggle datasets download -d chethuhn/network-intrusion-dataset
unzip network-intrusion-dataset.zip
rm network-intrusion-dataset.zip
cd ..
```

Clean and prepare the raw data. This reads the raw CSVs under `data/*.csv`,
fixes encoding and duplicate-column issues, drops rows with missing/infinite
values, and writes the result to `data/clean/`:

```bash
python3 -u scripts/prepare_data.py
```

---


## Single-Node Setup (original, for reference)

This was the original architecture, before the multi-node upgrade above. It
is documented here for reference, since the status report and early demos
were run against it; it is **not** the setup currently in this repo's
`docker-compose.yml`, and has no fault tolerance (one broker, one node —
if either goes down, the pipeline stops with no replica to recover from).
The compose file used one broker and one node instead of three/two.

With the file saved as `docker-compose.yml`, the setup commands were:

```bash
docker compose up -d
docker compose ps
```

```bash
docker exec -it kafka kafka-topics --create \
  --topic network-traffic --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

The schema was identical except for a single line —
`replication_factor: 1` instead of `2` (since there was only one node to
hold data):

```bash
docker cp cassandra-init/schema.cql cassandra:/schema.cql
docker exec -it cassandra cqlsh -f /schema.cql
```

```bash
python3 datafusion/consume_process.py --batch-size 50 --max-batches 5
```

```bash
python3 producer/produce.py \
  --file data/clean/Friday-WorkingHours-Morning.pcap_ISCX.csv \
  --rate 100 --limit 5000
```

---

## Multi-Node Setup (current, fault-tolerant)

### 1. Clean up any previous containers

This matters if a container from an earlier experiment is still around
under the same name:

```bash
docker compose down -v
docker rm -f kafka cassandra 2>/dev/null
docker ps -a
```

Confirm no leftover containers from this project remain before continuing.

### 2. Start the services

```bash
docker compose up -d
sleep 45
docker compose ps
```

All six services should show `Up`: `zookeeper`, `kafka1`, `kafka2`,
`kafka3`, `cassandra1`, `cassandra2`.

### 3. Verify the Cassandra cluster formed correctly

```bash
docker exec -it cassandra1 nodetool status
```

You should see **two** rows, both marked `UN` (Up/Normal). If you only see
one row, or the command fails, wait another 15–20 seconds and retry —
Cassandra can take time to gossip and join the ring.

### 4. Verify the Kafka brokers are healthy

```bash
docker exec -it kafka1 kafka-broker-api-versions --bootstrap-server localhost:9092
```

### 5. Create the Kafka topic with replication factor 3

```bash
docker exec -it kafka1 kafka-topics --create \
  --topic network-traffic --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 3
docker exec -it kafka1 kafka-topics --describe \
  --topic network-traffic --bootstrap-server localhost:9092
```

Each partition should list 3 replicas, with the ISR (in-sync replica) list
matching the replica list (e.g. `Replicas: 3,1,2  Isr: 3,1,2`).

### 6. Load the Cassandra schema (replication factor 2)

```bash
docker cp cassandra-init/schema.cql cassandra1:/schema.cql
docker exec -it cassandra1 cqlsh -f /schema.cql
docker exec -it cassandra1 cqlsh -e "DESCRIBE TABLES;" -k intrusion_detection
```

### 7. Run a healthy-path test before simulating any failure

Start the consumer in one terminal:

```bash
python3 datafusion/consume_process.py --batch-size 100 --max-batches 30
```

In a second terminal, start the producer:

```bash
python3 producer/produce.py \
  --file data/clean/Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv \
  --rate 100 --limit 3000
```

### 8. Verify the results

```bash
docker exec -it cassandra1 cqlsh -e "SELECT dst_port, window_start, window_end, total_flows, benign_count, attack_count FROM intrusion_detection.windowed_traffic_metrics WHERE attack_count > 0 LIMIT 10 ALLOW FILTERING;"
docker exec -it cassandra1 cqlsh -e "SELECT dst_port, event_time, label FROM intrusion_detection.flagged_anomalies LIMIT 10;"
```

Only once this healthy-path test passes should a broker/node be stopped
(e.g. `docker stop kafka2` or `docker stop cassandra2`) to measure message
loss, consumer lag recovery, and read availability during the outage —
covered separately as the fault-injection procedure.

---

## Folder Structure

- `data/` — raw CICIDS2017 CSVs (gitignored, download it yourself)
- `data/clean/` — cleaned CSVs (gitignored, produced by `prepare_data.py`)
- `scripts/` — data preparation and helper scripts
- `datafusion/` — DataFusion query/processing code
- `producer/` — Kafka producer (CSV → stream)
- `cassandra-init/` — Cassandra schema (CQL) files
- `docs/` — report, paper, architecture diagram
- `docker-compose.yml` — current multi-node (fault-tolerant) topology