#!/bin/bash
# Generates the initial data set entirely inside PostgreSQL (generate_series), which is
# much faster than streaming 5M rows from a client. Deterministic thanks to setseed().
set -eo pipefail

: "${SEED_MERCHANTS:=2000}"
: "${SEED_TRANSACTIONS:=5000000}"
: "${SEED_DAYS:=180}"

echo "Seeding ${SEED_MERCHANTS} merchants and ${SEED_TRANSACTIONS} transactions over ${SEED_DAYS} days..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v merchants="$SEED_MERCHANTS" -v txns="$SEED_TRANSACTIONS" -v days="$SEED_DAYS" <<-'EOSQL'
    \timing on
    SET synchronous_commit = off;
    SELECT setseed(0.42);

    -- Merchants: 10 MCCs, 31 provinces (Tehran over-weighted), risk 70/22/8 %.
    INSERT INTO core.merchants (merchant_id, name, mcc, province, risk_level,
                                owner_national_id, owner_mobile, updated_at)
    SELECT g,
           'Merchant ' || lpad(g::text, 4, '0'),
           (ARRAY['5411','5812','5732','5541','5912','5691','4121','7011','5999','8062'])
               [1 + floor(random() * 10)::int],
           (ARRAY['Tehran','Tehran','Tehran','Tehran','Tehran','Isfahan','Isfahan','Khorasan Razavi',
                  'Khorasan Razavi','Fars','Fars','East Azerbaijan','Khuzestan','Alborz','Mazandaran',
                  'Gilan','Kerman','West Azerbaijan','Qom','Yazd','Hormozgan','Golestan','Hamadan',
                  'Markazi','Kermanshah','Ardabil','Qazvin','Zanjan','Semnan','Lorestan','Bushehr',
                  'Kurdistan','Sistan and Baluchestan','Chaharmahal and Bakhtiari','Ilam',
                  'Kohgiluyeh and Boyer-Ahmad','North Khorasan','South Khorasan'])
               [1 + floor(random() * 38)::int],
           CASE WHEN x.r < 0.70 THEN 'LOW' WHEN x.r < 0.92 THEN 'MEDIUM' ELSE 'HIGH' END,
           lpad(floor(random() * 1e10)::bigint::text, 10, '0'),
           '09' || lpad(floor(random() * 1e9)::bigint::text, 9, '0'),
           now() - random() * make_interval(days => :days)
    FROM generate_series(1, :merchants) AS g,
         LATERAL (SELECT random() + 0 * g AS r) AS x;

    -- Transactions: ids grow with time (like a real sequence), merchant popularity is skewed,
    -- channel 60/30/10 %, status 80/10/10 %, log-normal amounts rounded to 1,000 Rial.
    INSERT INTO core.transactions (transaction_id, merchant_id, channel, card_bin, amount,
                                   fee_amount, status, created_at, updated_at)
    SELECT g,
           merchant_id,
           channel,
           card_bin,
           amount,
           CASE WHEN status = 'FAILED' THEN 0
                ELSE LEAST(GREATEST(round(amount * CASE channel WHEN 'POS' THEN 0.002
                                                                WHEN 'IPG' THEN 0.004
                                                                ELSE 0.003 END), 1000), 200000)::bigint
           END,
           status,
           created_at,
           -- never in the future: the latest seeded rows are only seconds old
           LEAST(now(), created_at + CASE WHEN status = 'REVERSED' THEN (60 + r * 3600) * interval '1 second'
                                          ELSE (1 + r * 8) * interval '1 second' END)
    FROM (
        SELECT g,
               1 + floor(power(random(), 2.2) * :merchants)::bigint AS merchant_id,
               (ARRAY['POS','POS','POS','POS','POS','POS','IPG','IPG','IPG','QR'])
                   [1 + floor(random() * 10)::int] AS channel,
               (ARRAY['603799','589210','627648','610433','603769','627353','589463','627412',
                      '502229','622106'])[1 + floor(random() * 10)::int] AS card_bin,
               LEAST(2000000000, GREATEST(10000,
                     round(exp(13.3 + 1.1 * random_normal()) / 1000) * 1000))::bigint AS amount,
               CASE WHEN x.s < 0.80 THEN 'SUCCESS' WHEN x.s < 0.90 THEN 'FAILED' ELSE 'REVERSED' END AS status,
               (now() - make_interval(days => :days))
                   + ((g - 1) + random()) * (make_interval(days => :days) / :txns) AS created_at,
               random() AS r
        FROM generate_series(1, :txns) AS g,
             LATERAL (SELECT random() + 0 * g AS s) AS x
    ) AS t;

    SELECT setval(pg_get_serial_sequence('core.transactions', 'transaction_id'),
                  (SELECT max(transaction_id) FROM core.transactions));

    -- OLTP-style indexes only (what an operational system would have); no reporting indexes.
    ALTER TABLE core.transactions
        ADD CONSTRAINT transactions_merchant_fk FOREIGN KEY (merchant_id)
        REFERENCES core.merchants (merchant_id);
    CREATE INDEX transactions_merchant_idx   ON core.transactions (merchant_id);
    CREATE INDEX transactions_created_at_idx ON core.transactions (created_at);
    CREATE INDEX transactions_status_idx     ON core.transactions (status) WHERE status = 'PENDING';

    VACUUM ANALYZE core.merchants;
    VACUUM ANALYZE core.transactions;

    SELECT status, count(*), round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
    FROM core.transactions GROUP BY status ORDER BY status;
EOSQL

echo "Seed complete."
