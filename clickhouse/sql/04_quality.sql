-- =====================================================================================
-- Data quality & governance (T5), entirely inside ClickHouse.
--   meta.v_data_quality  : every check as one row (check_name, severity, failed_rows, sample)
--   meta.dq_results      : history of runs, appended every 15 minutes by an RMV
--   on demand            : docker compose exec clickhouse bash /scripts/run_tests.sh
-- A check passes when failed_rows = 0. Placeholders __PG_*__ are substituted at init.
-- =====================================================================================

CREATE DATABASE IF NOT EXISTS meta;

-- Live, read-only window on core.transactions for the reconciliation checks. A PostgreSQL-engine
-- table with explicit columns does not connect at creation time (unlike the postgresql() table
-- function inside a view), so ClickHouse can initialise before PostgreSQL is up. It lives in
-- `secure` because its definition carries the bi_reader credentials.
CREATE TABLE secure.core_transactions
(
    transaction_id  Int64,
    status          String,
    amount          Int64,
    created_at      DateTime64(6, 'UTC')
)
ENGINE = PostgreSQL('__PG_HOST__:5432', '__PG_DB__', 'transactions', '__PG_READER_USER__', '__PG_READER_PASSWORD__', 'core');

CREATE TABLE meta.dq_results
(
    checked_at   DateTime64(3, 'UTC'),
    check_name   String,
    severity     LowCardinality(String),
    failed_rows  UInt64,
    sample       String
)
ENGINE = MergeTree
ORDER BY (checked_at, check_name)
TTL toDateTime(checked_at) + INTERVAL 30 DAY
COMMENT 'owner=data-platform@payyar; grain=check run x check; sensitive=none';

CREATE VIEW meta.v_data_quality AS

-- ---------------------------------------------------------------- keys (uniqueness on current state)
SELECT 'keys: stg.transactions.transaction_id unique' AS check_name, 'error' AS severity,
       toUInt64(count() - uniqExact(transaction_id)) AS failed_rows, '' AS sample
FROM stg.transactions FINAL
UNION ALL
SELECT 'keys: stg.merchants.merchant_id unique', 'error',
       toUInt64(count() - uniqExact(merchant_id)), ''
FROM stg.merchants FINAL
UNION ALL
SELECT 'keys: mart.dim_merchant.merchant_id unique', 'error',
       toUInt64(count() - uniqExact(merchant_id)), ''
FROM mart.dim_merchant FINAL
UNION ALL
SELECT 'keys: mart.fct_transactions.transaction_id unique', 'error',
       toUInt64(count() - uniqExact(transaction_id)), ''
FROM mart.fct_transactions FINAL
UNION ALL
SELECT 'keys: mart.agg_txn_daily_channel grain unique', 'error',
       toUInt64(count() - uniqExact(created_date, channel, status)), ''
FROM mart.agg_txn_daily_channel FINAL
UNION ALL
SELECT 'keys: mart.agg_merchant_daily_success grain unique', 'error',
       toUInt64(count() - uniqExact(created_date, merchant_id, mcc_at_txn, province_at_txn, risk_level_at_txn)), ''
FROM mart.agg_merchant_daily_success FINAL
UNION ALL
SELECT 'keys: fct merchant_id exists in dim_merchant', 'error',
       toUInt64(count()), toString(groupUniqArray(5)(merchant_id))
FROM mart.fct_transactions FINAL
WHERE merchant_id NOT IN (SELECT merchant_id FROM mart.dim_merchant FINAL)

-- ---------------------------------------------------------------- valid values
UNION ALL
SELECT 'values: raw _op in (r, c, u, d)', 'error',
       toUInt64(countIf(_op NOT IN ('r', 'c', 'u', 'd'))), toString(groupUniqArrayIf(5)(_op, _op NOT IN ('r', 'c', 'u', 'd')))
FROM raw.cdc_transactions
UNION ALL
SELECT 'values: fct channel in (POS, IPG, QR)', 'error',
       toUInt64(countIf(channel NOT IN ('POS', 'IPG', 'QR'))),
       toString(groupUniqArrayIf(5)(channel, channel NOT IN ('POS', 'IPG', 'QR')))
FROM mart.fct_transactions FINAL
UNION ALL
SELECT 'values: fct status in (PENDING, SUCCESS, FAILED, REVERSED)', 'error',
       toUInt64(countIf(status NOT IN ('PENDING', 'SUCCESS', 'FAILED', 'REVERSED'))),
       toString(groupUniqArrayIf(5)(status, status NOT IN ('PENDING', 'SUCCESS', 'FAILED', 'REVERSED')))
FROM mart.fct_transactions FINAL
UNION ALL
SELECT 'values: fct risk_level_at_txn in (LOW, MEDIUM, HIGH)', 'error',
       toUInt64(countIf(risk_level_at_txn NOT IN ('LOW', 'MEDIUM', 'HIGH'))),
       toString(groupUniqArrayIf(5)(risk_level_at_txn, risk_level_at_txn NOT IN ('LOW', 'MEDIUM', 'HIGH')))
FROM mart.fct_transactions FINAL
UNION ALL
SELECT 'values: dim_merchant risk_level in (LOW, MEDIUM, HIGH)', 'error',
       toUInt64(countIf(risk_level NOT IN ('LOW', 'MEDIUM', 'HIGH'))), ''
FROM mart.dim_merchant FINAL
UNION ALL
SELECT 'values: fct amount >= 0 and 0 <= fee_amount <= amount', 'error',
       toUInt64(countIf(amount < 0 OR fee_amount < 0 OR fee_amount > amount)),
       toString(groupArrayIf(5)(transaction_id, amount < 0 OR fee_amount < 0 OR fee_amount > amount))
FROM mart.fct_transactions FINAL

-- ---------------------------------------------------------------- PII / governance
UNION ALL
SELECT 'pii: no clear national id / mobile column in stg or mart', 'error',
       toUInt64(count()), toString(groupArray(5)(concat(database, '.', table, '.', name)))
FROM system.columns
WHERE database IN ('stg', 'mart')
  AND match(name, 'national_id|mobile')
  AND NOT match(name, '_(hash|masked)$')
UNION ALL
SELECT 'pii: hashes are SHA-256 hex and mobile is masked', 'error',
       toUInt64(countIf(NOT match(owner_national_id_hash, '^[0-9a-f]{64}$')
                        OR NOT match(owner_mobile_hash, '^[0-9a-f]{64}$')
                        OR match(owner_mobile_masked, '^09[0-9]{9}$'))), ''
FROM stg.merchants FINAL
UNION ALL
SELECT 'governance: every stg/mart table documents its owner', 'error',
       toUInt64(count()), toString(groupArray(5)(concat(database, '.', name)))
FROM system.tables
WHERE database IN ('stg', 'mart')
  AND engine != 'MaterializedView'
  AND NOT match(comment, 'owner=')

-- ---------------------------------------------------------------- point in time
-- Independent of the ASOF logic: interval join on [valid_from, valid_to). Merchants changed in
-- the last 5 minutes are skipped (the fact is corrected by the next refresh).
UNION ALL
SELECT 'pit: fct attributes = merchant version valid at created_at (last 3 days)', 'error',
       toUInt64(count()), toString(groupArray(5)(f.transaction_id))
FROM
(
    SELECT transaction_id, merchant_id, created_at, mcc_at_txn, province_at_txn, risk_level_at_txn
    FROM mart.fct_transactions FINAL
    WHERE created_date >= toDate(now(), 'Asia/Tehran') - 3
) AS f
INNER JOIN mart.dim_merchant_scd2 AS h ON f.merchant_id = h.merchant_id
WHERE f.created_at >= h.valid_from
  AND f.created_at < h.valid_to
  AND (f.risk_level_at_txn != h.risk_level OR f.province_at_txn != h.province OR f.mcc_at_txn != h.mcc)
  AND f.merchant_id NOT IN (SELECT merchant_id FROM mart.dim_merchant_history
                            WHERE _ingested_at > now() - INTERVAL 5 MINUTE)

-- ---------------------------------------------------------------- internal consistency
-- Aggregates must add up to the fact for every day the aggregate refresh has already processed.
UNION ALL
SELECT 'consistency: agg_txn_daily_channel = fct per day', 'error',
       toUInt64(count()), toString(groupArray(5)(d))
FROM
(
    SELECT f.created_date AS d
    FROM (SELECT created_date, count() AS fc, sum(amount) AS fa, sum(fee_amount) AS ff
          FROM mart.fct_transactions FINAL GROUP BY created_date) AS f
    LEFT JOIN (SELECT created_date, sum(txn_count) AS gc, sum(amount_sum) AS ga, sum(fee_sum) AS gf
               FROM mart.agg_txn_daily_channel FINAL GROUP BY created_date) AS g
        ON f.created_date = g.created_date
    WHERE (fc != gc OR fa != ga OR ff != gf)
      AND f.created_date NOT IN (SELECT created_date FROM mart.fct_transactions
                                 WHERE _loaded_at > (SELECT max(_version) FROM mart.agg_txn_daily_channel))
)
UNION ALL
SELECT 'consistency: agg_merchant_daily_success = fct SUCCESS per day', 'error',
       toUInt64(count()), toString(groupArray(5)(d))
FROM
(
    SELECT f.created_date AS d
    FROM (SELECT created_date, count() AS fc, sum(amount) AS fa, sum(fee_amount) AS ff
          FROM mart.fct_transactions FINAL WHERE status = 'SUCCESS' GROUP BY created_date) AS f
    LEFT JOIN (SELECT created_date, sum(success_count) AS gc, sum(success_amount) AS ga, sum(success_fee) AS gf
               FROM mart.agg_merchant_daily_success FINAL GROUP BY created_date) AS g
        ON f.created_date = g.created_date
    WHERE (fc != gc OR fa != ga OR ff != gf)
      AND f.created_date NOT IN (SELECT created_date FROM mart.fct_transactions
                                 WHERE _loaded_at > (SELECT max(_version) FROM mart.agg_merchant_daily_success))
)

-- ---------------------------------------------------------------- reconciliation with core
-- Live read of core through secure.core_transactions (PostgreSQL engine); only days closed for >= 15 minutes are compared, so
-- CDC lag, in-flight PENDING rows and short-lived test transactions cannot cause false alarms.
UNION ALL
SELECT 'reconcile: core vs mart, daily count+amount per status', 'error',
       toUInt64(count()), toString(groupArray(5)(tuple(created_date, status, core_cnt, mart_cnt)))
FROM
(
    SELECT created_date, status, c.cnt AS core_cnt, m.cnt AS mart_cnt, c.amt AS core_amt, m.amt AS mart_amt
    FROM (SELECT toDate(created_at, 'Asia/Tehran') AS created_date, status, count() AS cnt, sum(amount) AS amt
          FROM secure.core_transactions
          GROUP BY created_date, status) AS c
    FULL OUTER JOIN (SELECT created_date, status, sum(txn_count) AS cnt, sum(amount_sum) AS amt
                     FROM mart.agg_txn_daily_channel FINAL GROUP BY created_date, status) AS m
        USING (created_date, status)
    WHERE created_date < toDate(now() - INTERVAL 15 MINUTE, 'Asia/Tehran')
      AND (core_cnt != mart_cnt OR core_amt != mart_amt)
)
UNION ALL
SELECT 'reconcile: core vs mart, daily SUCCESS count+amount', 'error',
       toUInt64(count()), toString(groupArray(5)(tuple(created_date, core_cnt, mart_cnt)))
FROM
(
    SELECT created_date, c.cnt AS core_cnt, m.cnt AS mart_cnt, c.amt AS core_amt, m.amt AS mart_amt
    FROM (SELECT toDate(created_at, 'Asia/Tehran') AS created_date, count() AS cnt, sum(amount) AS amt
          FROM secure.core_transactions
          WHERE status = 'SUCCESS'
          GROUP BY created_date) AS c
    FULL OUTER JOIN (SELECT created_date, sum(success_count) AS cnt, sum(success_amount) AS amt
                     FROM mart.agg_merchant_daily_success FINAL GROUP BY created_date) AS m
        USING (created_date)
    WHERE created_date < toDate(now() - INTERVAL 15 MINUTE, 'Asia/Tehran')
      AND (core_cnt != mart_cnt OR core_amt != mart_amt)
)

-- ---------------------------------------------------------------- pipeline health (warnings)
UNION ALL
SELECT 'freshness: raw received CDC data in the last 5 minutes', 'warn',
       toUInt64(max(_ingested_at) < now() - INTERVAL 5 MINUTE), toString(max(_ingested_at))
FROM raw.cdc_transactions
UNION ALL
SELECT 'freshness: fact refreshed in the last 5 minutes', 'warn',
       toUInt64(max(_loaded_at) < now() - INTERVAL 5 MINUTE), toString(max(_loaded_at))
FROM mart.fct_transactions
UNION ALL
SELECT 'pipeline: refreshable views without errors', 'warn',
       toUInt64(countIf(exception != '')), toString(groupArrayIf(5)(view, exception != ''))
FROM system.view_refreshes
WHERE database = 'pipeline'
UNION ALL
SELECT 'pipeline: no messages in the CDC dead-letter table', 'warn',
       toUInt64(count()), toString(groupUniqArray(5)(source))
FROM raw.cdc_dead_letter;

CREATE MATERIALIZED VIEW pipeline.rmv_data_quality
REFRESH EVERY 15 MINUTE
SETTINGS refresh_retries = 1
APPEND TO meta.dq_results
EMPTY
AS
SELECT now64(3, 'UTC') AS checked_at, check_name, severity, failed_rows, sample
FROM meta.v_data_quality;
