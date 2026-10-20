"""End-to-end CDC acceptance test (T1/T2/T3), run against the live stack:

    docker compose run --rm ops python e2e_cdc_test.py

Writes probe rows into core and follows them through raw -> stg -> mart:
  * insert -> update -> update keeps order (LSN increasing) and full history in raw;
  * hard delete removes the row from the current state;
  * a merchant change creates a new SCD2 version and only NEW transactions get the new risk;
  * installments (if the schema change was applied) flows through without pipeline changes;
  * PII is pseudonymised in raw.
Exit code 0 = all checks passed.
"""
import os
import sys
import time

import psycopg

from mb_client import ch_connect

APP_DSN = (f"host={os.getenv('PG_HOST', 'postgres')} dbname={os.environ['PG_DB']} "
           f"user={os.environ['PG_APP_USER']} password={os.environ['PG_APP_PASSWORD']}")
PROBE_BIN = "888888"
failures = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}", flush=True)
    if not ok:
        failures.append(name)


def wait_for(description: str, fn, timeout: int):
    deadline = time.time() + timeout
    while True:
        value = fn()
        if value or time.time() > deadline:
            return value
        print(f"  ...waiting for {description}", flush=True)
        time.sleep(5)


def main() -> None:
    ch = ch_connect()
    pg = psycopg.connect(APP_DSN, autocommit=True)
    has_installments = pg.execute(
        "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'core' "
        "AND table_name = 'transactions' AND column_name = 'installments'").fetchone()[0] == 1

    merchant_id, old_risk = pg.execute(
        "SELECT merchant_id, risk_level FROM core.merchants ORDER BY merchant_id LIMIT 1").fetchone()
    cols = "merchant_id, channel, card_bin, amount, fee_amount, status" + (", installments" if has_installments else "")
    vals = "%s, 'IPG', %s, 1230000, 4920, 'PENDING'" + (", 6" if has_installments else "")

    # 1) lifecycle probe: insert -> SUCCESS -> REVERSED
    a = pg.execute(f"INSERT INTO core.transactions ({cols}) VALUES ({vals}) RETURNING transaction_id",
                   [merchant_id, PROBE_BIN]).fetchone()[0]
    pg.execute("UPDATE core.transactions SET status='SUCCESS', updated_at=now() WHERE transaction_id=%s", [a])
    pg.execute("UPDATE core.transactions SET status='REVERSED', updated_at=now() WHERE transaction_id=%s", [a])
    # 2) hard-delete probe
    b = pg.execute(f"INSERT INTO core.transactions ({cols}) VALUES ({vals}) RETURNING transaction_id",
                   [merchant_id, PROBE_BIN]).fetchone()[0]
    pg.execute("DELETE FROM core.transactions WHERE transaction_id=%s", [b])
    # 3) merchant change, then a transaction after it
    new_risk = "HIGH" if old_risk != "HIGH" else "LOW"
    time.sleep(1)
    pg.execute("UPDATE core.merchants SET risk_level=%s, updated_at=now() WHERE merchant_id=%s",
               [new_risk, merchant_id])
    time.sleep(1)
    c = pg.execute(f"INSERT INTO core.transactions ({cols}) VALUES ({vals}) RETURNING transaction_id",
                   [merchant_id, PROBE_BIN]).fetchone()[0]
    pg.execute("UPDATE core.transactions SET status='SUCCESS', updated_at=now() WHERE transaction_id=%s", [c])
    print(f"probes: lifecycle={a} deleted={b} after-merchant-change={c} merchant={merchant_id} "
          f"risk {old_risk}->{new_risk}", flush=True)

    # --- raw layer (seconds)
    events = wait_for("raw events", lambda: (lambda r: r if len(r) >= 3 else None)(ch.query(
        "SELECT _op, status, _lsn FROM raw.cdc_transactions WHERE transaction_id = {id:Int64} ORDER BY _lsn",
        parameters={"id": a}).result_rows), 120) or []
    check("raw keeps full history of the lifecycle", [e[0] for e in events][-3:] == ["c", "u", "u"], str(events))
    check("raw keeps change order", [e[1] for e in events][-3:] == ["PENDING", "SUCCESS", "REVERSED"]
          and all(x[2] < y[2] for x, y in zip(events, events[1:])))
    deleted = ch.query("SELECT _op, _is_deleted FROM raw.cdc_transactions WHERE transaction_id = {id:Int64} "
                       "ORDER BY _lsn", parameters={"id": b}).result_rows
    check("raw captures the hard delete", bool(deleted) and deleted[-1] == ("d", 1), str(deleted))
    pii = ch.query("SELECT owner_national_id_hash, _payload FROM raw.cdc_merchants "
                   "WHERE merchant_id = {m:Int64} ORDER BY _lsn DESC LIMIT 1",
                   parameters={"m": merchant_id}).result_rows
    check("PII pseudonymised in raw", bool(pii) and len(pii[0][0]) == 64 and "<redacted>" in pii[0][1])

    # --- mart (next refresh of the refreshable MVs, up to ~2 minutes)
    def mart_ready():
        rows = ch.query("SELECT transaction_id, status, risk_level_at_txn, installments "
                        "FROM mart.fct_transactions FINAL WHERE transaction_id IN ({a:Int64}, {b:Int64}, {c:Int64})",
                        parameters={"a": a, "b": b, "c": c}).result_rows
        got = {r[0]: r for r in rows}
        return got if a in got and c in got and got[c][1] == "SUCCESS" else None

    got = wait_for("mart fact rows", mart_ready, 300) or {}
    check("update applied (current state = REVERSED)", got.get(a, (0, None))[1] == "REVERSED", str(got.get(a)))
    check("delete applied (row gone from current state)", b not in got)
    check("transaction after the change carries the new risk", got.get(c, (0, 0, None))[2] == new_risk,
          str(got.get(c)))
    if has_installments:
        check("installments flows end-to-end", got.get(a, (0, 0, 0, None))[3] == 6, str(got.get(a)))
    else:
        print("[SKIP] installments not added yet (docker compose exec simulator python schema_change.py)")
    versions = ch.query("SELECT risk_level FROM mart.dim_merchant_scd2 WHERE merchant_id = {m:Int64} "
                        "ORDER BY version_no", parameters={"m": merchant_id}).result_rows
    stg_rows = ch.query("SELECT status FROM stg.transactions FINAL WHERE transaction_id IN ({a:Int64}, {b:Int64})",
                        parameters={"a": a, "b": b}).result_rows
    check("stg current state (real-time MV): REVERSED kept, deleted row hidden", stg_rows == [("REVERSED",)],
          str(stg_rows))
    check("merchant history keeps the previous risk level",
          len(versions) >= 2 and versions[-1][0] == new_risk and versions[-2][0] == old_risk, str(versions))

    pg.execute("DELETE FROM core.transactions WHERE card_bin = %s", [PROBE_BIN])
    print("\nALL CHECKS PASSED" if not failures else f"\nFAILED: {failures}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
