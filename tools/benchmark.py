"""Benchmark the dashboard cards through Metabase and compare legacy vs optimised results.

    docker compose run --rm ops python benchmark.py            # live (simulator running)
    docker compose stop simulator                              # for an exact comparison:
    docker compose run --rm ops python benchmark.py --quiesce  # waits until CH caught up

Timing: Metabase's own `running_time` of POST /api/card/:id/query (cache bypassed),
1 warm-up + N measured runs, median reported. Results go to /results (docs/results).
"""
import argparse
import datetime as dt
import json
import os
import pathlib
import statistics
import time
from decimal import Decimal

from cards import CARD4_CURRENT_ATTRIBUTES, CARDS, LEGACY, OPTIMISED
from mb_client import CH_DB_NAME, CORE_DB_NAME, Metabase, ch_connect, pg_connect

RESULTS = pathlib.Path(os.getenv("RESULTS_DIR", "/results"))
RUNS = int(os.getenv("BENCHMARK_RUNS", "5"))


# ------------------------------------------------------------------ pipeline readiness
def pipeline_gap(cutoff_minutes: int = 3) -> tuple[int, int]:
    """Transactions created before (now - cutoff) in core vs in the ClickHouse fact table."""
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=cutoff_minutes)
    with pg_connect() as pg:
        core = pg.execute("SELECT count(*) FROM core.transactions WHERE created_at < %s", [cutoff]).fetchone()[0]
    try:
        mart = ch_connect().query(
            "SELECT count() FROM mart.fct_transactions FINAL WHERE created_at < {c:DateTime64(6, 'UTC')}",
            parameters={"c": cutoff}).result_rows[0][0]
    except Exception:  # fact table not built yet
        mart = 0
    return core, mart


def wait_for_catch_up(timeout_minutes: int = 60) -> None:
    deadline = time.time() + timeout_minutes * 60
    while time.time() < deadline:
        core, mart = pipeline_gap()
        print(f"pipeline catch-up: core={core:,} mart={mart:,}", flush=True)
        if core and core == mart:
            time.sleep(90)  # let the aggregates of the same micro-batch land
            return
        time.sleep(30)
    raise SystemExit("pipeline did not catch up in time")


def wait_quiescent() -> None:
    """For exact comparison: simulator stopped and the last change processed end-to-end.
    Quiet = PostgreSQL's own write counters for the database do not move for 20 seconds."""
    sql = ("SELECT tup_inserted + tup_updated + tup_deleted FROM pg_stat_database "
           "WHERE datname = current_database()")
    with pg_connect() as pg:
        before = pg.execute(sql).fetchone()[0]
        time.sleep(20)
        after = pg.execute(sql).fetchone()[0]
    if after != before:
        raise SystemExit("core is still changing - run `docker compose stop simulator` first")
    while True:
        core, mart = pipeline_gap(cutoff_minutes=0)
        print(f"quiesce: core={core:,} mart={mart:,}", flush=True)
        if core == mart:
            break
        time.sleep(20)
    time.sleep(120)  # two refresh cycles: aggregates and late merchant versions settled


# ------------------------------------------------------------------ timing
def time_card(mb: Metabase, card_id: int) -> dict:
    mb.run_card(card_id)  # warm-up (PostgreSQL buffer cache / ClickHouse mark cache)
    samples = [mb.run_card(card_id) for _ in range(RUNS)]
    times = [s["running_time_ms"] for s in samples]
    return {"median_ms": statistics.median(times), "min_ms": min(times), "max_ms": max(times),
            "rows": samples[-1]["row_count"]}


# ------------------------------------------------------------------ comparison
def normalise(rows) -> list[tuple]:
    out = []
    for row in rows:
        norm = []
        for v in row:
            if isinstance(v, (dt.date, dt.datetime)):
                v = v.isoformat()[:10]
            elif isinstance(v, Decimal):
                v = int(v) if v == v.to_integral_value() else round(float(v), 2)
            elif isinstance(v, float):
                v = round(v, 2)
            norm.append(v)
        out.append(tuple(norm))
    return out


def diff(a: list[tuple], b: list[tuple]) -> list[str]:
    def close(x, y):
        if isinstance(x, float) or isinstance(y, float):
            return abs(float(x) - float(y)) <= 0.011  # 2-decimal rounding of the success rate
        return x == y
    problems = []
    if len(a) != len(b):
        problems.append(f"row count {len(a)} vs {len(b)}")
    for i, (x, y) in enumerate(zip(a, b)):
        if len(x) != len(y) or not all(close(p, q) for p, q in zip(x, y)):
            problems.append(f"row {i}: core={x} optimised={y}")
    return problems


def compare() -> dict:
    report = {}
    pg, ch = pg_connect(), ch_connect()
    for n in CARDS:
        core_rows = normalise(pg.execute(LEGACY[n]).fetchall())
        opt_rows = normalise(ch.query(OPTIMISED[n]).result_rows)
        report[n] = {"identical": not diff(core_rows, opt_rows), "differences": diff(core_rows, opt_rows)[:20]}
    current = normalise(ch.query(CARD4_CURRENT_ATTRIBUTES).result_rows)
    report["4_current_attributes"] = {
        "identical": not diff(normalise(pg.execute(LEGACY[4]).fetchall()), current),
        "note": "card 4 recomputed on the fact table with CURRENT merchant attributes"}
    report["merchant_changes_captured"] = ch.query(
        "SELECT count() - uniqExact(merchant_id) FROM mart.dim_merchant_history").result_rows[0][0]
    return report


# ------------------------------------------------------------------ report
def write_report(timings: dict, comparison: dict, mode: str) -> None:
    RESULTS.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"# Card benchmark ({stamp}, {mode}, median of {RUNS} runs after 1 warm-up)", "",
             "| Card | core (PostgreSQL) ms | optimised (ClickHouse) ms | speed-up | rows core / opt | result |",
             "|---|---:|---:|---:|---:|---|"]
    for n, meta in CARDS.items():
        before, after = timings[n]["legacy"], timings[n]["optimised"]
        speedup = before["median_ms"] / max(after["median_ms"], 1)
        verdict = "identical" if comparison[n]["identical"] else "differs - see comparison"
        lines.append(f"| {meta['name']} | {before['median_ms']:.0f} | {after['median_ms']:.0f} | "
                     f"x{speedup:.0f} | {before['rows']} / {after['rows']} | {verdict} |")
    lines += ["", "## Comparison details", "", "```json", json.dumps(comparison, indent=2, default=str), "```"]
    (RESULTS / "benchmark.md").write_text("\n".join(lines), encoding="utf-8")
    (RESULTS / "benchmark.json").write_text(
        json.dumps({"mode": mode, "timings": timings, "comparison": comparison}, indent=2, default=str),
        encoding="utf-8")
    print("\n".join(lines[:9 + len(CARDS)]), flush=True)


def run(mb: Metabase, legacy: dict, optimised: dict, wait_for_pipeline: bool = False,
        quiesce: bool = False) -> None:
    if quiesce:
        wait_quiescent()
    elif wait_for_pipeline:
        wait_for_catch_up()
    timings = {}
    for n in CARDS:
        timings[n] = {"legacy": time_card(mb, legacy[n]), "optimised": time_card(mb, optimised[n])}
        print(f"card {n}: {timings[n]['legacy']['median_ms']} ms -> {timings[n]['optimised']['median_ms']} ms",
              flush=True)
    write_report(timings, compare(), "quiesced" if quiesce else "live")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--quiesce", action="store_true",
                        help="exact comparison: requires the simulator to be stopped")
    args = parser.parse_args()
    mb = Metabase()
    mb.wait_healthy()
    mb.login()
    dbs = mb.databases()
    cards = mb.cards()
    legacy = {n: cards[(f"{m['name']} [core]", dbs[CORE_DB_NAME])] for n, m in CARDS.items()}
    optimised = {n: cards[(f"{m['name']} [ClickHouse]", dbs[CH_DB_NAME])] for n, m in CARDS.items()}
    run(mb, legacy, optimised, wait_for_pipeline=not args.quiesce, quiesce=args.quiesce)


if __name__ == "__main__":
    main()
