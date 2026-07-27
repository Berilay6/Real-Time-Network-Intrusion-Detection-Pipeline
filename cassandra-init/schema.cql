-- replication_factor raised from 1 to 2 to match the 2-node Cassandra
-- cluster: with factor 1, killing a node during the fault-tolerance test
-- would genuinely destroy data instead of testing recovery from a replica.

CREATE KEYSPACE IF NOT EXISTS intrusion_detection
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};

USE intrusion_detection;

CREATE TABLE IF NOT EXISTS flagged_anomalies (
    detection_id    UUID,
    dst_port        INT,
    event_time      TIMESTAMP,
    src_flow_id     TEXT,
    flow_duration   BIGINT,
    label           TEXT,
    total_fwd_pkt_len DOUBLE,
    PRIMARY KEY (dst_port, event_time, detection_id)
) WITH CLUSTERING ORDER BY (event_time DESC);

-- batch_id added to the primary key: without it, two different micro-batches
-- that happen to land in the same (dst_port, window_start) would silently
-- overwrite each other (Cassandra INSERT is an upsert), discarding the
-- earlier batch's counts. Each micro-batch now gets its own row; totals
-- across batches for a given window are computed at query time via
-- SUM(...) GROUP BY dst_port, window_start.
CREATE TABLE IF NOT EXISTS windowed_traffic_metrics (
    dst_port        INT,
    window_start    TIMESTAMP,
    batch_id        UUID,
    window_end      TIMESTAMP,
    total_flows     BIGINT,
    benign_count    BIGINT,
    attack_count    BIGINT,
    avg_flow_duration DOUBLE,
    PRIMARY KEY (dst_port, window_start, batch_id)
) WITH CLUSTERING ORDER BY (window_start DESC, batch_id ASC);
