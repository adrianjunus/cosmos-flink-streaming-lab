-- =====================================================================
-- Cosmos DB → Kafka → Flink SQL: 5-minute tumbling window per customer
-- =====================================================================
-- Run from inside the Flink SQL Client:
--   docker compose exec jobmanager ./bin/sql-client.sh
-- Then either paste these statements one at a time, or:
--   docker compose exec jobmanager ./bin/sql-client.sh -f /opt/flink/sql/orders_window.sql
-- =====================================================================

-- ─── Source: raw change feed events from Kafka ──────────────────────────
-- Each row in this Kafka topic is a Cosmos document plus metadata fields
-- that the connector adds (_rid, _etag, _ts, _lsn). We map the fields we
-- care about and let Flink ignore the rest.
CREATE TABLE orders_raw (
  id          STRING,
  customerId  STRING,
  amount      DOUBLE,
  items       INT,
  `ts`        TIMESTAMP_LTZ(3),
  WATERMARK FOR `ts` AS `ts` - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'cosmos.orders',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-orders-consumer',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- ─── Sink: windowed aggregations back to Kafka ──────────────────────────
CREATE TABLE orders_windowed (
  customerId    STRING,
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  order_count   BIGINT,
  total_amount  DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders.windowed_5m',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json'
);

-- ─── The streaming job: tumbling 5-minute windows, group by customer ────
INSERT INTO orders_windowed
SELECT
  customerId,
  window_start,
  window_end,
  COUNT(*)    AS order_count,
  SUM(amount) AS total_amount
FROM TABLE(
  TUMBLE(TABLE orders_raw, DESCRIPTOR(`ts`), INTERVAL '5' MINUTES))
GROUP BY customerId, window_start, window_end;

-- =====================================================================
-- Once submitted, this becomes a long-running Flink job. Check
-- http://localhost:8081 for the DAG and metrics.
--
-- Stop it from the Flink UI (Cancel button) or from CLI:
--   docker compose exec jobmanager ./bin/flink list
--   docker compose exec jobmanager ./bin/flink cancel <jobId>
-- =====================================================================
