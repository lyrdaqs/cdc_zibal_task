#!/bin/bash
# ClickHouse bootstrap (runs once on an empty data directory).
#   raw    : CDC landing zone, append-only, written ONLY by the Kafka materialized views.
#   stg    : cleaned current state + history (02_stg.sql, plain MVs).
#   mart   : report-ready model read by Metabase (03_mart.sql, MVs + refreshable MVs).
#   meta   : data-quality checks and results (04_quality.sql).
#   secure : the PII pseudonymisation key; readable by nobody except the admin/definer.
set -eo pipefail

CH=(clickhouse client --host 127.0.0.1 --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery)

"${CH[@]}" <<EOSQL
CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS stg;
CREATE DATABASE IF NOT EXISTS mart;
CREATE DATABASE IF NOT EXISTS secure;

CREATE TABLE secure.pii_key (salt String) ENGINE = TinyLog;
INSERT INTO secure.pii_key VALUES ('${PII_SALT}');

-- ---------------------------------------------------------------- transactions
CREATE TABLE raw.cdc_transactions
(
    transaction_id  Int64,
    merchant_id     Nullable(Int64),
    channel         LowCardinality(Nullable(String)),
    card_bin        Nullable(String),
    amount          Nullable(Int64),
    fee_amount      Nullable(Int64),
    status          LowCardinality(Nullable(String)),
    created_at      Nullable(DateTime64(6, 'UTC')),
    updated_at      Nullable(DateTime64(6, 'UTC')),
    installments    Nullable(Int32),
    _op             LowCardinality(String),
    _lsn            UInt64,
    _source_ts_ms   Int64,
    _is_deleted     UInt8,
    _payload        String CODEC(ZSTD(3)),
    _kafka_partition UInt64,
    _kafka_offset   UInt64,
    _ingested_at    DateTime64(3, 'UTC'),
    INDEX idx_ingested _ingested_at TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(_ingested_at)
ORDER BY (transaction_id, _lsn);

CREATE TABLE raw.kafka_transactions (payload String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'payyar.core.transactions',
         kafka_group_name = 'clickhouse_raw_transactions',
         kafka_format = 'JSONAsString',
         kafka_num_consumers = 1,
         kafka_max_block_size = 100000,
         kafka_flush_interval_ms = 2000,
         kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW raw.mv_cdc_transactions TO raw.cdc_transactions AS
SELECT
    JSONExtract(payload, 'transaction_id', 'Int64')                         AS transaction_id,
    JSONExtract(payload, 'merchant_id', 'Nullable(Int64)')                  AS merchant_id,
    JSONExtract(payload, 'channel', 'Nullable(String)')                     AS channel,
    JSONExtract(payload, 'card_bin', 'Nullable(String)')                    AS card_bin,
    JSONExtract(payload, 'amount', 'Nullable(Int64)')                       AS amount,
    JSONExtract(payload, 'fee_amount', 'Nullable(Int64)')                   AS fee_amount,
    JSONExtract(payload, 'status', 'Nullable(String)')                      AS status,
    parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'created_at'), 6, 'UTC') AS created_at,
    parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'updated_at'), 6, 'UTC') AS updated_at,
    -- Column added later by the schema change: absent key => NULL, never an error.
    JSONExtract(payload, 'installments', 'Nullable(Int32)')                 AS installments,
    JSONExtractString(payload, '__op')                                      AS _op,
    JSONExtract(payload, '__lsn', 'UInt64')                                 AS _lsn,
    JSONExtract(payload, '__source_ts_ms', 'Int64')                         AS _source_ts_ms,
    toUInt8(JSONExtractString(payload, '__op') = 'd'
            OR JSONExtractRaw(payload, '__deleted') IN ('true', '"true"'))  AS _is_deleted,
    payload                                                                 AS _payload,
    _partition                                                              AS _kafka_partition,
    _offset                                                                 AS _kafka_offset,
    now64(3, 'UTC')                                                         AS _ingested_at
FROM raw.kafka_transactions
WHERE length(_error) = 0 AND payload != '';

-- ---------------------------------------------------------------- merchants
CREATE TABLE raw.cdc_merchants
(
    merchant_id             Int64,
    name                    Nullable(String),
    mcc                     LowCardinality(Nullable(String)),
    province                LowCardinality(Nullable(String)),
    risk_level              LowCardinality(Nullable(String)),
    owner_national_id_hash  Nullable(String),
    owner_mobile_hash       Nullable(String),
    owner_mobile_masked     Nullable(String),
    updated_at              Nullable(DateTime64(6, 'UTC')),
    _op                     LowCardinality(String),
    _lsn                    UInt64,
    _source_ts_ms           Int64,
    _is_deleted             UInt8,
    _payload                String CODEC(ZSTD(3)),
    _kafka_partition        UInt64,
    _kafka_offset           UInt64,
    _ingested_at            DateTime64(3, 'UTC'),
    INDEX idx_ingested _ingested_at TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(_ingested_at)
ORDER BY (merchant_id, _lsn);

CREATE TABLE raw.kafka_merchants (payload String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'payyar.core.merchants',
         kafka_group_name = 'clickhouse_raw_merchants',
         kafka_format = 'JSONAsString',
         kafka_num_consumers = 1,
         kafka_flush_interval_ms = 2000,
         kafka_handle_error_mode = 'stream';

-- PII never lands in ClickHouse in clear text: national id and mobile are replaced by a
-- keyed SHA-256 (salt lives in secure.pii_key) and the clear values are stripped from _payload.
CREATE MATERIALIZED VIEW raw.mv_cdc_merchants TO raw.cdc_merchants AS
WITH
    (SELECT any(salt) FROM secure.pii_key)                         AS salt,
    JSONExtract(payload, 'owner_national_id', 'Nullable(String)')  AS nid,
    JSONExtract(payload, 'owner_mobile', 'Nullable(String)')       AS mob
SELECT
    JSONExtract(payload, 'merchant_id', 'Int64')                   AS merchant_id,
    JSONExtract(payload, 'name', 'Nullable(String)')               AS name,
    JSONExtract(payload, 'mcc', 'Nullable(String)')                AS mcc,
    JSONExtract(payload, 'province', 'Nullable(String)')           AS province,
    JSONExtract(payload, 'risk_level', 'Nullable(String)')         AS risk_level,
    if(nid IS NULL, NULL, lower(hex(SHA256(concat(salt, 'nid:', nid)))))  AS owner_national_id_hash,
    if(mob IS NULL, NULL, lower(hex(SHA256(concat(salt, 'mob:', mob)))))  AS owner_mobile_hash,
    if(mob IS NULL, NULL, concat(substring(mob, 1, 4), '****', substring(mob, 9, 3))) AS owner_mobile_masked,
    parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'updated_at'), 6, 'UTC') AS updated_at,
    JSONExtractString(payload, '__op')                             AS _op,
    JSONExtract(payload, '__lsn', 'UInt64')                        AS _lsn,
    JSONExtract(payload, '__source_ts_ms', 'Int64')                AS _source_ts_ms,
    toUInt8(JSONExtractString(payload, '__op') = 'd'
            OR JSONExtractRaw(payload, '__deleted') IN ('true', '"true"')) AS _is_deleted,
    replaceRegexpAll(
        replaceRegexpAll(payload, '"owner_national_id":[[:space:]]*"[^"]*"', '"owner_national_id":"<redacted>"'),
        '"owner_mobile":[[:space:]]*"[^"]*"', '"owner_mobile":"<redacted>"')  AS _payload,
    _partition                                                     AS _kafka_partition,
    _offset                                                        AS _kafka_offset,
    now64(3, 'UTC')                                                AS _ingested_at
FROM raw.kafka_merchants
WHERE length(_error) = 0 AND payload != '';

-- ---------------------------------------------------------------- dead letters
CREATE TABLE raw.cdc_dead_letter
(
    source        LowCardinality(String),
    raw_message   String,
    error         String,
    _kafka_partition UInt64,
    _kafka_offset UInt64,
    _ingested_at  DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = MergeTree ORDER BY _ingested_at;

CREATE MATERIALIZED VIEW raw.mv_dead_letter_transactions TO raw.cdc_dead_letter AS
SELECT 'transactions' AS source, _raw_message AS raw_message, _error AS error,
       _partition AS _kafka_partition, _offset AS _kafka_offset
FROM raw.kafka_transactions WHERE length(_error) > 0;

CREATE MATERIALIZED VIEW raw.mv_dead_letter_merchants TO raw.cdc_dead_letter AS
SELECT 'merchants' AS source, _raw_message AS raw_message, _error AS error,
       _partition AS _kafka_partition, _offset AS _kafka_offset
FROM raw.kafka_merchants WHERE length(_error) > 0;

-- ---------------------------------------------------------------- service accounts
-- All transformations run inside ClickHouse as the admin (definer of the MVs / RMVs).
-- etl: read-only operator account for the ops tooling (benchmark, e2e test).
CREATE USER IF NOT EXISTS ${CH_ETL_USER} IDENTIFIED WITH sha256_password BY '${CH_ETL_PASSWORD}';
GRANT SELECT ON raw.* TO ${CH_ETL_USER};
GRANT SELECT ON stg.* TO ${CH_ETL_USER};
GRANT SELECT ON mart.* TO ${CH_ETL_USER};
GRANT SELECT ON meta.* TO ${CH_ETL_USER};

-- metabase: read-only, report layer only (+ meta.dq_results, granted in 02_apply_sql.sh).
CREATE USER IF NOT EXISTS ${CH_METABASE_USER} IDENTIFIED WITH sha256_password BY '${CH_METABASE_PASSWORD}';
GRANT SELECT ON mart.* TO ${CH_METABASE_USER};
EOSQL

echo "ClickHouse bootstrap complete."
