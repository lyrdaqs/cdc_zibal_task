"""The five dashboard cards, defined once: the legacy SQL that runs on core (PostgreSQL)
and the optimised SQL that runs on the ClickHouse report layer (mart).

Shared definitions (assumptions documented in the README):
  * a day / month is a calendar day / month in Asia/Tehran;
  * "successful" = status SUCCESS (REVERSED and FAILED excluded);
  * success rate = SUCCESS / finished transactions (PENDING excluded from the denominator);
  * top merchants and fees use successful transactions only.
"""

TZ = "Asia/Tehran"

# ------------------------------------------------------------------ legacy: straight on core
PG_TODAY = f"(now() AT TIME ZONE '{TZ}')::date"


def pg_since(days_back: int) -> str:
    return f"(({PG_TODAY} - {days_back})::timestamp AT TIME ZONE '{TZ}')"


PG_LOCAL_DATE = f"(t.created_at AT TIME ZONE '{TZ}')::date"

LEGACY = {
    1: f"""SELECT {PG_LOCAL_DATE} AS day,
       count(*)      AS success_count,
       sum(t.amount) AS success_amount
FROM core.transactions t
WHERE t.status = 'SUCCESS'
  AND t.created_at >= {pg_since(89)}
GROUP BY 1
ORDER BY 1""",
    2: f"""SELECT {PG_LOCAL_DATE} AS day,
       t.channel,
       round(100.0 * count(*) FILTER (WHERE t.status = 'SUCCESS')
             / nullif(count(*) FILTER (WHERE t.status <> 'PENDING'), 0), 2) AS success_rate_pct
FROM core.transactions t
WHERE t.created_at >= {pg_since(29)}
GROUP BY 1, 2
ORDER BY 1, 2""",
    3: f"""SELECT t.merchant_id,
       m.name,
       sum(t.amount) AS total_amount,
       count(*)      AS success_count
FROM core.transactions t
JOIN core.merchants m ON m.merchant_id = t.merchant_id
WHERE t.status = 'SUCCESS'
  AND t.created_at >= (date_trunc('month', now() AT TIME ZONE '{TZ}') AT TIME ZONE '{TZ}')
GROUP BY 1, 2
ORDER BY total_amount DESC, t.merchant_id
LIMIT 10""",
    4: f"""SELECT m.mcc,
       m.province,
       sum(t.fee_amount) AS fee_amount
FROM core.transactions t
JOIN core.merchants m ON m.merchant_id = t.merchant_id
WHERE t.status = 'SUCCESS'
  AND t.created_at >= {pg_since(89)}
GROUP BY 1, 2
ORDER BY 1, 2""",
    5: f"""SELECT date_trunc('month', t.created_at AT TIME ZONE '{TZ}')::date AS month,
       m.risk_level,
       count(*)      AS success_count,
       sum(t.amount) AS success_amount
FROM core.transactions t
JOIN core.merchants m ON m.merchant_id = t.merchant_id   -- CURRENT risk level (the known flaw)
WHERE t.status = 'SUCCESS'
GROUP BY 1, 2
ORDER BY 1, 2""",
}

# ------------------------------------------------------------------ optimised: ClickHouse mart
# Aggregates are ReplacingMergeTree tables maintained incrementally by refreshable MVs, so they
# are read with FINAL (cheap: a few hundred thousand rows at most).
CH_TODAY = f"toDate(now(), '{TZ}')"

OPTIMISED = {
    1: f"""SELECT created_date AS day,
       sum(txn_count)  AS success_count,
       sum(amount_sum) AS success_amount
FROM mart.agg_txn_daily_channel FINAL
WHERE status = 'SUCCESS'
  AND created_date >= {CH_TODAY} - 89
GROUP BY day
ORDER BY day""",
    2: f"""SELECT created_date AS day,
       channel,
       round(100 * sumIf(txn_count, status = 'SUCCESS')
             / nullIf(sumIf(txn_count, status != 'PENDING'), 0), 2) AS success_rate_pct
FROM mart.agg_txn_daily_channel FINAL
WHERE created_date >= {CH_TODAY} - 29
GROUP BY day, channel
ORDER BY day, channel""",
    3: f"""SELECT a.merchant_id AS merchant_id,
       m.name AS name,
       sum(a.success_amount) AS total_amount,
       sum(a.success_count)  AS success_count
FROM mart.agg_merchant_daily_success AS a FINAL
LEFT JOIN (SELECT merchant_id, name FROM mart.dim_merchant FINAL) AS m ON m.merchant_id = a.merchant_id
WHERE a.created_date >= toStartOfMonth({CH_TODAY})
GROUP BY merchant_id, name
ORDER BY total_amount DESC, merchant_id
LIMIT 10""",
    4: f"""SELECT mcc_at_txn      AS mcc,
       province_at_txn AS province,
       sum(success_fee) AS fee_amount
FROM mart.agg_merchant_daily_success FINAL
WHERE created_date >= {CH_TODAY} - 89
GROUP BY mcc, province
ORDER BY mcc, province""",
    5: """SELECT created_month     AS month,
       risk_level_at_txn AS risk_level,
       sum(success_count)  AS success_count,
       sum(success_amount) AS success_amount
FROM mart.agg_merchant_daily_success FINAL
GROUP BY month, risk_level
ORDER BY month, risk_level""",
}

# Card 4 with CURRENT merchant attributes on the optimised model: used only by the comparison
# script to prove the data is identical to core and the remaining difference is the
# point-in-time semantics.
CARD4_CURRENT_ATTRIBUTES = f"""SELECT m.mcc AS mcc, m.province AS province, sum(f.fee_amount) AS fee_amount
FROM mart.fct_transactions AS f FINAL
JOIN (SELECT merchant_id, mcc, province FROM mart.dim_merchant FINAL) AS m ON m.merchant_id = f.merchant_id
WHERE f.status = 'SUCCESS' AND f.created_date >= {CH_TODAY} - 89
GROUP BY mcc, province
ORDER BY mcc, province"""

CARDS = {
    1: dict(name="1. Daily successful transactions (90 days)", display="line",
            viz={"graph.dimensions": ["day"], "graph.metrics": ["success_amount", "success_count"]}),
    2: dict(name="2. Daily success rate by channel (30 days)", display="line",
            viz={"graph.dimensions": ["day", "channel"], "graph.metrics": ["success_rate_pct"]}),
    3: dict(name="3. Top 10 merchants this month", display="table", viz={}),
    4: dict(name="4. Fee by MCC and province (90 days)", display="table", viz={}),
    5: dict(name="5. Monthly successful volume by risk level", display="bar",
            viz={"graph.dimensions": ["month", "risk_level"], "graph.metrics": ["success_amount"],
                 "stackable.stack_type": "stacked"}),
}
