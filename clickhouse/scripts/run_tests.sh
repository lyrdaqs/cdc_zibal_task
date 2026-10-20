#!/bin/bash
# Runs every data-quality check now and fails (exit 1) if any error-level check fails.
#   docker compose exec clickhouse bash /scripts/run_tests.sh
# Same checks as the automatic 15-minute run; the result is stored in meta.dq_results too.
set -eo pipefail
CH=(clickhouse client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD")

START=$("${CH[@]}" --query "SELECT toString(now64(3, 'UTC'))")
"${CH[@]}" --query "SYSTEM REFRESH VIEW pipeline.rmv_data_quality"

# wait until a run that started after us has written its results (max 10 minutes)
for _ in $(seq 1 120); do
  done_=$("${CH[@]}" --query "SELECT max(checked_at) >= toDateTime64('$START', 3, 'UTC') FROM meta.dq_results")
  [ "$done_" = "1" ] && break
  sleep 5
done
[ "$done_" = "1" ] || { echo "data-quality refresh did not finish:"; \
  "${CH[@]}" --query "SELECT status, exception FROM system.view_refreshes WHERE view = 'rmv_data_quality'"; exit 1; }

LAST="(SELECT max(checked_at) FROM meta.dq_results)"
"${CH[@]}" --format PrettyCompactMonoBlock --query "
  SELECT if(failed_rows = 0, 'PASS', if(severity = 'error', 'FAIL', 'WARN')) AS result,
         check_name, failed_rows, sample
  FROM meta.dq_results WHERE checked_at = $LAST
  ORDER BY result = 'PASS', check_name"

"${CH[@]}" --query "
  SELECT throwIf(countIf(severity = 'error' AND failed_rows > 0) > 0, 'data quality: error-level checks failed')
  FROM meta.dq_results WHERE checked_at = $LAST" > /dev/null
echo "all error-level data-quality checks passed"
