-- =====================================================================================
-- mart: the report layer read by Metabase.
--   * dimensions are fed in real time by plain MVs on stg (projection only);
--   * the fact and the aggregates are built by refreshable materialized views (RMV) that
--     run inside ClickHouse every minute - no external scheduler. They live in the
--     `pipeline` database (not visible to Metabase) and write APPEND into mart tables.
--   Chain per minute:  rmv_fct_transactions -> rmv_agg_txn_daily_channel
--                                           -> rmv_agg_merchant_daily_success
--   Monitoring: SELECT * FROM system.view_refreshes WHERE database = 'pipeline'
-- =====================================================================================

CREATE DATABASE IF NOT EXISTS pipeline;

-- ------------------------------------------------------------------ dimensions (real time)
CREATE TABLE mart.dim_merchant
(
    merchant_id             Int64,
    name                    String,
    mcc                     LowCardinality(String),
    province                LowCardinality(String),
    risk_level              LowCardinality(String),
    owner_national_id_hash  String COMMENT 'pii_pseudonymised: keyed SHA-256 of national id',
    owner_mobile_masked     String COMMENT 'pii_masked: 0912****567',
    updated_at              DateTime64(6, 'UTC'),
    _lsn                    UInt64,
    _is_deleted             UInt8
)
ENGINE = ReplacingMergeTree(_lsn, _is_deleted)
ORDER BY merchant_id
COMMENT 'owner=data-platform@payyar; steward=merchant-operations@payyar; grain=merchant_id (read with FINAL); scd=1; sensitive=owner_national_id_hash,owner_mobile_masked';

CREATE MATERIALIZED VIEW mart.mv_dim_merchant TO mart.dim_merchant AS
SELECT merchant_id, name, mcc, province, risk_level, owner_national_id_hash, owner_mobile_masked,
       updated_at, _lsn, _is_deleted
FROM stg.merchants;

-- SCD2 history. The first version we saw (the initial snapshot, op = r) is assumed valid since
-- the beginning of time, because core kept no earlier history (documented assumption).
CREATE TABLE mart.dim_merchant_history
(
    merchant_id             Int64,
    name                    String,
    mcc                     LowCardinality(String),
    province                LowCardinality(String),
    risk_level              LowCardinality(String),
    owner_national_id_hash  String COMMENT 'pii_pseudonymised: keyed SHA-256 of national id',
    owner_mobile_masked     String COMMENT 'pii_masked: 0912****567',
    valid_from              DateTime64(6, 'UTC') COMMENT 'commit time of this version in core',
    _lsn                    UInt64,
    _ingested_at            DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (merchant_id, _lsn)
COMMENT 'owner=data-platform@payyar; steward=risk-management@payyar; grain=merchant_id x version (_lsn), read with FINAL; scd=2; sensitive=owner_national_id_hash,owner_mobile_masked';

CREATE MATERIALIZED VIEW mart.mv_dim_merchant_history TO mart.dim_merchant_history AS
SELECT merchant_id, name, mcc, province, risk_level, owner_national_id_hash, owner_mobile_masked,
       if(_op = 'r', toDateTime64('1970-01-01 00:00:00', 6, 'UTC'), _event_at) AS valid_from,
       _lsn, _ingested_at
FROM stg.merchant_versions;

-- Convenience view with validity intervals [valid_from, valid_to).
CREATE VIEW mart.dim_merchant_scd2 AS
SELECT
    merchant_id,
    row_number() OVER w AS version_no,
    name, mcc, province, risk_level, owner_national_id_hash, owner_mobile_masked,
    valid_from,
    leadInFrame(valid_from, 1, toDateTime64('2299-12-31 00:00:00', 6, 'UTC')) OVER w AS valid_to,
    _lsn
FROM mart.dim_merchant_history FINAL
WINDOW w AS (PARTITION BY merchant_id ORDER BY _lsn ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
COMMENT 'owner=data-platform@payyar; steward=risk-management@payyar; grain=merchant_id x version_no; scd=2; sensitive=owner_national_id_hash,owner_mobile_masked';

-- ------------------------------------------------------------------ fact
CREATE TABLE mart.fct_transactions
(
    transaction_id     Int64,
    created_date       Date COMMENT 'calendar day in Asia/Tehran',
    created_month      Date,
    created_at         DateTime64(6, 'UTC'),
    updated_at         DateTime64(6, 'UTC'),
    merchant_id        Int64,
    channel            LowCardinality(String),
    card_bin           String COMMENT 'internal: issuer BIN (first 6 digits), not a PAN',
    amount             Int64 COMMENT 'Rial',
    fee_amount         Int64 COMMENT 'Rial',
    status             LowCardinality(String),
    installments       Nullable(Int32),
    mcc_at_txn         LowCardinality(String) COMMENT 'merchant MCC valid at created_at',
    province_at_txn    LowCardinality(String) COMMENT 'merchant province valid at created_at',
    risk_level_at_txn  LowCardinality(String) COMMENT 'merchant risk level valid at created_at',
    _is_deleted        UInt8,
    _loaded_at         DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(_loaded_at, _is_deleted)
PARTITION BY toYYYYMM(created_date)
ORDER BY (created_date, transaction_id)
COMMENT 'owner=data-platform@payyar; steward=finance-reporting@payyar; grain=transaction_id, current state (read with FINAL); sensitive=card_bin';

-- Incremental: a transaction is recomputed when
--   1. any of its CDC events reached stg since the last refresh (insert/status change/delete), or
--   2. its merchant got a version valid at/before created_at that arrived after the fact was
--      built (late-arriving dimension).
-- The latest version per transaction (argMax by LSN) includes delete markers; merchant
-- attributes are fixed point-in-time with an ASOF JOIN on valid_from <= created_at.
-- The 5-minute overlap absorbs rows that became visible late; re-appending is idempotent.
CREATE MATERIALIZED VIEW pipeline.rmv_fct_transactions
REFRESH EVERY 1 MINUTE
SETTINGS refresh_retries = 3
APPEND TO mart.fct_transactions
AS
WITH
watermark AS (
    SELECT max(_loaded_at) - INTERVAL 5 MINUTE AS wm FROM mart.fct_transactions
),
changed_merchants AS (
    SELECT merchant_id, min(valid_from) AS changed_since
    FROM mart.dim_merchant_history
    WHERE _ingested_at > (SELECT wm FROM watermark)
    GROUP BY merchant_id
),
affected AS (
    SELECT transaction_id
    FROM stg.transactions
    WHERE _ingested_at > (SELECT wm FROM watermark)
    UNION DISTINCT
    SELECT t.transaction_id
    FROM stg.transactions AS t
    INNER JOIN changed_merchants AS m ON t.merchant_id = m.merchant_id
    WHERE t.created_at >= m.changed_since
),
latest AS (
    SELECT
        transaction_id,
        argMax((merchant_id, channel, card_bin, amount, fee_amount, status,
                created_at, updated_at, installments, _is_deleted), _lsn) AS s
    FROM stg.transactions
    WHERE transaction_id IN (SELECT transaction_id FROM affected)
    GROUP BY transaction_id
),
txn AS (
    SELECT
        transaction_id,
        s.1 AS merchant_id, s.2 AS channel, s.3 AS card_bin, s.4 AS amount, s.5 AS fee_amount,
        s.6 AS status, s.7 AS created_at, s.8 AS updated_at, s.9 AS installments, s.10 AS is_deleted
    FROM latest
),
merchant_versions AS (
    SELECT merchant_id, valid_from, mcc, province, risk_level, toUInt8(1) AS matched
    FROM mart.dim_merchant_history FINAL
)
SELECT
    t.transaction_id                                        AS transaction_id,
    toDate(t.created_at, 'Asia/Tehran')                     AS created_date,
    toStartOfMonth(toDate(t.created_at, 'Asia/Tehran'))     AS created_month,
    t.created_at                                            AS created_at,
    t.updated_at                                            AS updated_at,
    t.merchant_id                                           AS merchant_id,
    t.channel                                               AS channel,
    t.card_bin                                              AS card_bin,
    t.amount                                                AS amount,
    t.fee_amount                                            AS fee_amount,
    t.status                                                AS status,
    t.installments                                          AS installments,
    if(m.matched = 1, m.mcc, 'UNKNOWN')                     AS mcc_at_txn,
    if(m.matched = 1, m.province, 'UNKNOWN')                AS province_at_txn,
    if(m.matched = 1, m.risk_level, 'UNKNOWN')              AS risk_level_at_txn,
    t.is_deleted                                            AS _is_deleted,
    now64(3, 'UTC')                                         AS _loaded_at
FROM txn AS t
ASOF LEFT JOIN merchant_versions AS m
    ON t.merchant_id = m.merchant_id AND t.created_at >= m.valid_from
SETTINGS max_bytes_before_external_group_by = 800000000, max_bytes_before_external_sort = 800000000;

-- ------------------------------------------------------------------ aggregates
-- Incremental per day: every refresh recomputes only the days that got new fact rows since
-- its previous refresh and appends them with a newer _version. Keys that existed for such a
-- day but no longer do (e.g. a late merchant change moved rows to another province) get an
-- _is_deleted = 1 row, so FINAL always equals a full recomputation.

CREATE TABLE mart.agg_txn_daily_channel
(
    created_date  Date,
    channel       LowCardinality(String),
    status        LowCardinality(String),
    txn_count     UInt64,
    amount_sum    Int64 COMMENT 'Rial',
    fee_sum       Int64 COMMENT 'Rial',
    _is_deleted   UInt8,
    _version      DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(_version, _is_deleted)
PARTITION BY toYYYYMM(created_date)
ORDER BY (created_date, channel, status)
COMMENT 'owner=data-platform@payyar; steward=finance-reporting@payyar; grain=created_date x channel x status (read with FINAL); sensitive=none; cards=1,2';

CREATE MATERIALIZED VIEW pipeline.rmv_agg_txn_daily_channel
REFRESH EVERY 1 MINUTE
DEPENDS ON pipeline.rmv_fct_transactions
SETTINGS refresh_retries = 3
APPEND TO mart.agg_txn_daily_channel
AS
WITH
last_run AS (
    SELECT max(_version) - INTERVAL 5 MINUTE AS since FROM mart.agg_txn_daily_channel
),
affected_days AS (
    SELECT DISTINCT created_date FROM mart.fct_transactions
    WHERE _loaded_at > (SELECT since FROM last_run)
),
fresh AS (
    SELECT created_date, channel, status,
           count()         AS txn_count,
           sum(amount)     AS amount_sum,
           sum(fee_amount) AS fee_sum
    FROM mart.fct_transactions FINAL
    WHERE created_date IN (SELECT created_date FROM affected_days)
    GROUP BY created_date, channel, status
),
existing AS (
    SELECT created_date, channel, status
    FROM mart.agg_txn_daily_channel FINAL
    WHERE created_date IN (SELECT created_date FROM affected_days)
)
SELECT created_date, channel, status, txn_count, amount_sum, fee_sum,
       toUInt8(0) AS _is_deleted, now64(3, 'UTC') AS _version
FROM fresh
UNION ALL
SELECT e.created_date, e.channel, e.status, toUInt64(0), toInt64(0), toInt64(0),
       toUInt8(1) AS _is_deleted, now64(3, 'UTC') AS _version
FROM existing AS e
LEFT ANTI JOIN fresh AS f
    ON e.created_date = f.created_date AND e.channel = f.channel AND e.status = f.status;

CREATE TABLE mart.agg_merchant_daily_success
(
    created_date       Date,
    created_month      Date,
    merchant_id        Int64,
    mcc_at_txn         LowCardinality(String),
    province_at_txn    LowCardinality(String),
    risk_level_at_txn  LowCardinality(String),
    success_count      UInt64,
    success_amount     Int64 COMMENT 'Rial',
    success_fee        Int64 COMMENT 'Rial',
    _is_deleted        UInt8,
    _version           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(_version, _is_deleted)
PARTITION BY toYYYYMM(created_date)
ORDER BY (created_date, merchant_id, mcc_at_txn, province_at_txn, risk_level_at_txn)
COMMENT 'owner=data-platform@payyar; steward=finance-reporting@payyar; grain=created_date x merchant_id x mcc/province/risk at transaction time, SUCCESS only (read with FINAL); sensitive=none; cards=3,4,5';

CREATE MATERIALIZED VIEW pipeline.rmv_agg_merchant_daily_success
REFRESH EVERY 1 MINUTE
DEPENDS ON pipeline.rmv_fct_transactions
SETTINGS refresh_retries = 3
APPEND TO mart.agg_merchant_daily_success
AS
WITH
last_run AS (
    SELECT max(_version) - INTERVAL 5 MINUTE AS since FROM mart.agg_merchant_daily_success
),
affected_days AS (
    SELECT DISTINCT created_date FROM mart.fct_transactions
    WHERE _loaded_at > (SELECT since FROM last_run)
),
fresh AS (
    SELECT created_date, merchant_id, mcc_at_txn, province_at_txn, risk_level_at_txn,
           count()         AS success_count,
           sum(amount)     AS success_amount,
           sum(fee_amount) AS success_fee
    FROM mart.fct_transactions FINAL
    WHERE status = 'SUCCESS'
      AND created_date IN (SELECT created_date FROM affected_days)
    GROUP BY created_date, merchant_id, mcc_at_txn, province_at_txn, risk_level_at_txn
),
existing AS (
    SELECT created_date, merchant_id, mcc_at_txn, province_at_txn, risk_level_at_txn
    FROM mart.agg_merchant_daily_success FINAL
    WHERE created_date IN (SELECT created_date FROM affected_days)
)
SELECT created_date, toStartOfMonth(created_date) AS created_month, merchant_id,
       mcc_at_txn, province_at_txn, risk_level_at_txn,
       success_count, success_amount, success_fee,
       toUInt8(0) AS _is_deleted, now64(3, 'UTC') AS _version
FROM fresh
UNION ALL
SELECT e.created_date, toStartOfMonth(e.created_date), e.merchant_id,
       e.mcc_at_txn, e.province_at_txn, e.risk_level_at_txn,
       toUInt64(0), toInt64(0), toInt64(0),
       toUInt8(1) AS _is_deleted, now64(3, 'UTC') AS _version
FROM existing AS e
LEFT ANTI JOIN fresh AS f
    ON  e.created_date = f.created_date AND e.merchant_id = f.merchant_id
    AND e.mcc_at_txn = f.mcc_at_txn AND e.province_at_txn = f.province_at_txn
    AND e.risk_level_at_txn = f.risk_level_at_txn;
