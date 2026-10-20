#!/bin/bash
# Applies the transformation layers that live in ClickHouse itself (mounted at /sql):
#   02_stg.sql      plain MVs raw -> stg (real time)
#   03_mart.sql     dims (MVs) + fact/aggregates (refreshable MVs, every minute)
#   04_quality.sql  data-quality view + refreshable MV writing meta.dq_results
# __PG_*__ placeholders (used by the reconciliation with core) are filled from the environment.
set -eo pipefail

CH=(clickhouse client --host 127.0.0.1 --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery)

for f in /sql/*.sql; do
  echo "applying $f"
  sed -e "s|__PG_HOST__|${PG_HOST:-postgres}|g" \
      -e "s|__PG_DB__|${PG_DB}|g" \
      -e "s|__PG_READER_USER__|${PG_READER_USER}|g" \
      -e "s|__PG_READER_PASSWORD__|${PG_READER_PASSWORD}|g" \
      "$f" | "${CH[@]}"
done

"${CH[@]}" --query "GRANT SELECT ON meta.dq_results TO ${CH_METABASE_USER}"
echo "transformation layers applied."
