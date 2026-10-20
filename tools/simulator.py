"""Change simulator for the Payyar OLTP database (core schema).

Runs forever and applies the changes described in the task:
  * ~20 new PENDING transactions per second, settled to SUCCESS/FAILED after a few seconds;
    part of the successful ones are REVERSED a few minutes later.
  * every minute one merchant changes risk_level and province.
  * test transactions (card_bin 999999) are inserted and hard-deleted shortly after.
  * once `installments` exists (see schema_change.py) new rows fill it.
State (what is pending / due for reversal) is kept in memory; on restart, stale PENDING
rows are settled so nothing stays pending forever.
"""
import heapq
import logging
import os
import random
import time

import psycopg

logging.basicConfig(level=logging.INFO, format="%(asctime)s simulator %(message)s")
log = logging.getLogger()

DSN = (
    f"host={os.getenv('PG_HOST', 'postgres')} dbname={os.environ['PG_DB']} "
    f"user={os.environ['PG_APP_USER']} password={os.environ['PG_APP_PASSWORD']}"
)
TPS = float(os.getenv("SIM_TPS", "20"))
SETTLE_DELAY = (2.0, 8.0)          # seconds PENDING -> SUCCESS/FAILED
SUCCESS_RATE = 0.89                # of settled transactions
REVERSAL_RATE = 0.11               # of successful transactions
REVERSAL_DELAY = (30.0, 240.0)     # seconds SUCCESS -> REVERSED
MERCHANT_CHANGE_EVERY = 60.0
TEST_TXN_EVERY = 30.0
TEST_TXN_TTL = (5.0, 20.0)
TEST_CARD_BIN = "999999"

CHANNELS = ["POS"] * 6 + ["IPG"] * 3 + ["QR"]
BINS = ["603799", "589210", "627648", "610433", "603769",
        "627353", "589463", "627412", "502229", "622106"]
FEE_RATE = {"POS": 0.002, "IPG": 0.004, "QR": 0.003}
RISK_LEVELS = ["LOW", "MEDIUM", "HIGH"]
INSTALLMENTS = [1] * 16 + [3, 3, 6, 12]


def fee_for(amount: int, channel: str) -> int:
    return int(min(max(round(amount * FEE_RATE[channel]), 1000), 200000))


def random_amount() -> int:
    raw = random.lognormvariate(13.3, 1.1)
    return int(min(2_000_000_000, max(10_000, round(raw / 1000) * 1000)))


class Simulator:
    def __init__(self, conn: psycopg.Connection):
        self.conn = conn
        self.pending: list[tuple[float, int]] = []    # (due, transaction_id)
        self.reversals: list[tuple[float, int]] = []
        self.test_rows: list[tuple[float, int]] = []
        now = time.monotonic()
        self.next_merchant_change = now + MERCHANT_CHANGE_EVERY
        self.next_test_txn = now + TEST_TXN_EVERY
        self.next_schema_check = 0.0
        self.has_installments = False
        self.stats = dict(inserted=0, success=0, failed=0, reversed=0,
                          merchant_changes=0, test_inserted=0, test_deleted=0)
        self.merchants = [r[0] for r in conn.execute("SELECT merchant_id FROM core.merchants")]
        # Same popularity skew as the seed: low ids are busier.
        n = len(self.merchants)
        self.weights = [((i + 1) / n) ** (1 / 2.2) - (i / n) ** (1 / 2.2) for i in range(n)]

    # --- helpers ---------------------------------------------------------------------
    def check_schema(self, now: float) -> None:
        if now < self.next_schema_check:
            return
        self.next_schema_check = now + 10
        found = self.conn.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = 'core' AND table_name = 'transactions' AND column_name = 'installments'"
        ).fetchone() is not None
        if found and not self.has_installments:
            log.info("installments column detected - new transactions will fill it")
        self.has_installments = found

    def insert_transactions(self, count: int, status: str, card_bin: str | None = None) -> list[int]:
        cols = ["merchant_id", "channel", "card_bin", "amount", "fee_amount", "status"]
        if self.has_installments:
            cols.append("installments")
        rows = []
        merchants = random.choices(self.merchants, weights=self.weights, k=count)
        for merchant_id in merchants:
            channel = random.choice(CHANNELS)
            amount = random_amount()
            row = [merchant_id, channel, card_bin or random.choice(BINS), amount,
                   fee_for(amount, channel), status]
            if self.has_installments:
                row.append(random.choice(INSTALLMENTS))
            rows.append(row)
        placeholders = ", ".join(["(" + ", ".join(["%s"] * len(cols)) + ")"] * len(rows))
        params = [v for row in rows for v in row]
        sql = (f"INSERT INTO core.transactions ({', '.join(cols)}) VALUES {placeholders} "
               "RETURNING transaction_id")
        return [r[0] for r in self.conn.execute(sql, params).fetchall()]

    @staticmethod
    def pop_due(heap: list[tuple[float, int]], now: float) -> list[int]:
        due = []
        while heap and heap[0][0] <= now:
            due.append(heapq.heappop(heap)[1])
        return due

    # --- the four kinds of change -------------------------------------------------------
    def new_transactions(self, now: float) -> None:
        count = max(1, round(random.gauss(TPS, TPS * 0.15)))
        for tid in self.insert_transactions(count, "PENDING"):
            heapq.heappush(self.pending, (now + random.uniform(*SETTLE_DELAY), tid))
        self.stats["inserted"] += count

    def settle(self, now: float) -> None:
        due = self.pop_due(self.pending, now)
        if not due:
            return
        success = [t for t in due if random.random() < SUCCESS_RATE]
        failed = [t for t in due if t not in set(success)]
        with self.conn.transaction():
            if success:
                self.conn.execute(
                    "UPDATE core.transactions SET status = 'SUCCESS', updated_at = now() "
                    "WHERE transaction_id = ANY(%s) AND status = 'PENDING'", [success])
            if failed:
                self.conn.execute(
                    "UPDATE core.transactions SET status = 'FAILED', fee_amount = 0, updated_at = now() "
                    "WHERE transaction_id = ANY(%s) AND status = 'PENDING'", [failed])
        for tid in success:
            if random.random() < REVERSAL_RATE:
                heapq.heappush(self.reversals, (now + random.uniform(*REVERSAL_DELAY), tid))
        self.stats["success"] += len(success)
        self.stats["failed"] += len(failed)

    def reverse(self, now: float) -> None:
        due = self.pop_due(self.reversals, now)
        if due:
            self.conn.execute(
                "UPDATE core.transactions SET status = 'REVERSED', updated_at = now() "
                "WHERE transaction_id = ANY(%s) AND status = 'SUCCESS'", [due])
            self.stats["reversed"] += len(due)

    def change_merchant(self, now: float) -> None:
        if now < self.next_merchant_change:
            return
        self.next_merchant_change = now + MERCHANT_CHANGE_EVERY
        merchant_id = random.choice(self.merchants)
        risk, province = self.conn.execute(
            "SELECT risk_level, province FROM core.merchants WHERE merchant_id = %s",
            [merchant_id]).fetchone()
        new_risk = random.choice([r for r in RISK_LEVELS if r != risk])
        new_province = self.conn.execute(
            "SELECT province FROM (SELECT DISTINCT province FROM core.merchants) p "
            "WHERE province <> %s ORDER BY random() LIMIT 1", [province]).fetchone()[0]
        self.conn.execute(
            "UPDATE core.merchants SET risk_level = %s, province = %s, updated_at = now() "
            "WHERE merchant_id = %s", [new_risk, new_province, merchant_id])
        self.stats["merchant_changes"] += 1
        log.info("merchant %s: risk %s -> %s, province %s -> %s",
                 merchant_id, risk, new_risk, province, new_province)

    def test_transactions(self, now: float) -> None:
        if now >= self.next_test_txn:
            self.next_test_txn = now + TEST_TXN_EVERY
            ids = self.insert_transactions(random.randint(1, 3), "SUCCESS", card_bin=TEST_CARD_BIN)
            for tid in ids:
                heapq.heappush(self.test_rows, (now + random.uniform(*TEST_TXN_TTL), tid))
            self.stats["test_inserted"] += len(ids)
        due = self.pop_due(self.test_rows, now)
        if due:
            self.conn.execute("DELETE FROM core.transactions WHERE transaction_id = ANY(%s)", [due])
            self.stats["test_deleted"] += len(due)

    def recover(self) -> None:
        """Settle PENDING rows left over by a previous run (state is in memory only)."""
        cur = self.conn.execute(
            "UPDATE core.transactions SET status = CASE WHEN random() < %s THEN 'SUCCESS' ELSE 'FAILED' END, "
            "updated_at = now() WHERE status = 'PENDING' AND created_at < now() - interval '30 seconds'",
            [SUCCESS_RATE])
        self.conn.execute(
            "DELETE FROM core.transactions WHERE card_bin = %s AND created_at < now() - interval '1 minute'",
            [TEST_CARD_BIN])
        if cur.rowcount:
            log.info("recovered %s stale PENDING transactions", cur.rowcount)

    def run(self) -> None:
        self.recover()
        last_report = time.monotonic()
        while True:
            started = time.monotonic()
            self.check_schema(started)
            self.new_transactions(started)
            self.settle(started)
            self.reverse(started)
            self.change_merchant(started)
            self.test_transactions(started)
            if started - last_report >= 30:
                log.info("stats %s | pending=%s reversal_queue=%s",
                         self.stats, len(self.pending), len(self.reversals))
                last_report = started
            time.sleep(max(0.0, 1.0 - (time.monotonic() - started)))


def main() -> None:
    while True:
        try:
            with psycopg.connect(DSN, autocommit=True) as conn:
                log.info("connected, simulating ~%s tx/s", TPS)
                Simulator(conn).run()
        except psycopg.OperationalError as exc:
            log.warning("database unavailable (%s), retrying in 5s", exc)
            time.sleep(5)


if __name__ == "__main__":
    main()
