-- =====================================================================================
-- stg: cleaned current state + merchant history.
-- Plain (insert-triggered) materialized views on raw: every CDC block that lands in raw
-- is projected here in the same insert, so stg is real-time and incremental by design.
-- Rules: type casting only - no joins, no aggregation (so these views cannot fail and
-- block Kafka ingestion). Current state is resolved by ReplacingMergeTree:
--   version = _lsn (PostgreSQL WAL position)  -> newer change always wins, duplicates and
--                                                out-of-order deliveries are harmless
--   _is_deleted = 1                           -> hard deletes are hidden by FINAL
-- =====================================================================================

CREATE TABLE stg.transactions
(
    transaction_id  Int64,
    merchant_id     Int64,
    channel         LowCardinality(String),
    card_bin        String,
    amount          Int64,
    fee_amount      Int64,
    status          LowCardinality(String),
    created_at      DateTime64(6, 'UTC'),
    updated_at      DateTime64(6, 'UTC'),
    installments    Nullable(Int32),
    _op             LowCardinality(String),
    _lsn            UInt64,
    _is_deleted     UInt8,
    _source_ts_ms   Int64,
    _ingested_at    DateTime64(3, 'UTC'),
    INDEX idx_ingested _ingested_at TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(_lsn, _is_deleted)
ORDER BY transaction_id
COMMENT 'owner=data-platform@payyar; layer=stg; grain=transaction_id (FINAL); pii=none';

CREATE MATERIALIZED VIEW stg.mv_transactions TO stg.transactions AS
SELECT
    transaction_id,
    assumeNotNull(merchant_id)  AS merchant_id,
    assumeNotNull(channel)      AS channel,
    assumeNotNull(card_bin)     AS card_bin,
    assumeNotNull(amount)       AS amount,
    assumeNotNull(fee_amount)   AS fee_amount,
    assumeNotNull(status)       AS status,
    assumeNotNull(created_at)   AS created_at,
    assumeNotNull(updated_at)   AS updated_at,
    installments,
    _op,
    _lsn,
    _is_deleted,
    _source_ts_ms,
    _ingested_at
FROM raw.cdc_transactions;

CREATE TABLE stg.merchants
(
    merchant_id             Int64,
    name                    String,
    mcc                     LowCardinality(String),
    province                LowCardinality(String),
    risk_level              LowCardinality(String),
    owner_national_id_hash  String COMMENT 'pii_pseudonymised: keyed SHA-256',
    owner_mobile_hash       String COMMENT 'pii_pseudonymised: keyed SHA-256',
    owner_mobile_masked     String COMMENT 'pii_masked: 0912****567',
    updated_at              DateTime64(6, 'UTC'),
    _op                     LowCardinality(String),
    _lsn                    UInt64,
    _is_deleted             UInt8,
    _source_ts_ms           Int64,
    _ingested_at            DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(_lsn, _is_deleted)
ORDER BY merchant_id
COMMENT 'owner=data-platform@payyar; layer=stg; grain=merchant_id (FINAL); pii=pseudonymised';

CREATE MATERIALIZED VIEW stg.mv_merchants TO stg.merchants AS
SELECT
    merchant_id,
    assumeNotNull(name)                    AS name,
    assumeNotNull(mcc)                     AS mcc,
    assumeNotNull(province)                AS province,
    assumeNotNull(risk_level)              AS risk_level,
    assumeNotNull(owner_national_id_hash)  AS owner_national_id_hash,
    assumeNotNull(owner_mobile_hash)       AS owner_mobile_hash,
    assumeNotNull(owner_mobile_masked)     AS owner_mobile_masked,
    assumeNotNull(updated_at)              AS updated_at,
    _op,
    _lsn,
    _is_deleted,
    _source_ts_ms,
    _ingested_at
FROM raw.cdc_merchants;

-- Every version a merchant ever had (the history core throws away).
-- _event_at = commit time in PostgreSQL (source.ts_ms), independent of the app's updated_at.
CREATE TABLE stg.merchant_versions
(
    merchant_id             Int64,
    name                    String,
    mcc                     LowCardinality(String),
    province                LowCardinality(String),
    risk_level              LowCardinality(String),
    owner_national_id_hash  String COMMENT 'pii_pseudonymised: keyed SHA-256',
    owner_mobile_masked     String COMMENT 'pii_masked: 0912****567',
    _event_at               DateTime64(6, 'UTC'),
    _op                     LowCardinality(String),
    _lsn                    UInt64,
    _ingested_at            DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (merchant_id, _lsn)
COMMENT 'owner=data-platform@payyar; layer=stg; grain=merchant_id x _lsn; pii=pseudonymised';

CREATE MATERIALIZED VIEW stg.mv_merchant_versions TO stg.merchant_versions AS
SELECT
    merchant_id,
    assumeNotNull(name)                    AS name,
    assumeNotNull(mcc)                     AS mcc,
    assumeNotNull(province)                AS province,
    assumeNotNull(risk_level)              AS risk_level,
    assumeNotNull(owner_national_id_hash)  AS owner_national_id_hash,
    assumeNotNull(owner_mobile_masked)     AS owner_mobile_masked,
    toDateTime64(fromUnixTimestamp64Milli(_source_ts_ms, 'UTC'), 6, 'UTC') AS _event_at,
    _op,
    _lsn,
    _ingested_at
FROM raw.cdc_merchants
WHERE _is_deleted = 0;
