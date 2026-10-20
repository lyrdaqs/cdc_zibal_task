"""One-shot Metabase provisioning (idempotent):
  1. admin user, report timezone;
  2. two connections: core (PostgreSQL, read-only role) and mart (ClickHouse, read-only role);
  3. the five cards twice - legacy on core, optimised on ClickHouse;
  4. dashboards "فروش روزانه" (legacy) and "فروش روزانه (بهینه - ClickHouse)";
  5. dashboard "کیفیت داده" over meta.dq_results (written by the data-quality RMV);
  6. once the pipeline has caught up: benchmark + result comparison (docs/results/).
"""
import os

import benchmark
from cards import CARDS, LEGACY, OPTIMISED, TZ
from mb_client import (CH_DB_NAME, CORE_DB_NAME, DQ_DASHBOARD, LEGACY_DASHBOARD, OPTIMISED_DASHBOARD,
                       Metabase)


def ensure_databases(mb: Metabase) -> tuple[int, int]:
    existing = mb.databases()
    if CORE_DB_NAME not in existing:
        mb.call("POST", "/database", json={
            "engine": "postgres", "name": CORE_DB_NAME, "is_full_sync": False,
            "details": {"host": os.getenv("PG_HOST", "postgres"), "port": 5432,
                        "dbname": os.environ["PG_DB"], "user": os.environ["PG_READER_USER"],
                        "password": os.environ["PG_READER_PASSWORD"], "ssl": False}})
    if CH_DB_NAME not in existing:
        mb.call("POST", "/database", json={
            "engine": "clickhouse", "name": CH_DB_NAME, "is_full_sync": True,
            "details": {"host": os.getenv("CH_HOST", "clickhouse"), "port": 8123, "dbname": "mart",
                        "user": os.environ["CH_METABASE_USER"],
                        "password": os.environ["CH_METABASE_PASSWORD"], "ssl": False}})
    existing = mb.databases()
    return existing[CORE_DB_NAME], existing[CH_DB_NAME]


def ensure_cards(mb: Metabase, database_id: int, sql: dict, suffix: str) -> dict[int, int]:
    existing = mb.cards()
    ids = {}
    for number, meta in CARDS.items():
        name = f"{meta['name']} [{suffix}]"
        if (name, database_id) in existing:
            ids[number] = existing[(name, database_id)]
            continue
        card = mb.call("POST", "/card", json={
            "name": name,
            "display": meta["display"],
            "visualization_settings": meta["viz"],
            "dataset_query": {"type": "native", "database": database_id,
                              "native": {"query": sql[number], "template-tags": {}}},
        })
        ids[number] = card["id"]
    return ids


def ensure_dashboard(mb: Metabase, name: str, description: str, card_ids: dict[int, int]) -> int:
    found = mb.call("GET", "/search", params={"q": name, "models": "dashboard"})
    rows = found["data"] if isinstance(found, dict) else found
    for row in rows:
        if row.get("name") == name and not row.get("archived"):
            return row["id"]
    dashboard = mb.call("POST", "/dashboard", json={"name": name, "description": description})
    layout = {1: (0, 0, 24, 6), 2: (0, 6, 24, 6), 3: (0, 12, 12, 7), 4: (12, 12, 12, 7), 5: (0, 19, 24, 7)}
    dashcards = [
        {"id": -n, "card_id": card_ids[n], "col": col, "row": row, "size_x": w, "size_y": h,
         "parameter_mappings": [], "visualization_settings": {}}
        for n, (col, row, w, h) in layout.items()
    ]
    mb.call("PUT", f"/dashboard/{dashboard['id']}", json={"dashcards": dashcards})
    return dashboard["id"]


DQ_CARDS = {
    "Data quality - latest run": ("table", {}, """SELECT if(failed_rows = 0, 'PASS', if(severity = 'error', 'FAIL', 'WARN')) AS result,
       check_name, failed_rows, sample, checked_at
FROM meta.dq_results
WHERE checked_at = (SELECT max(checked_at) FROM meta.dq_results)
ORDER BY result = 'PASS', check_name"""),
    "Data quality - failing checks over time": ("line", {"graph.dimensions": ["checked_at"],
                                                         "graph.metrics": ["failing_errors", "failing_warnings"]},
        """SELECT checked_at,
       countIf(severity = 'error' AND failed_rows > 0) AS failing_errors,
       countIf(severity = 'warn' AND failed_rows > 0)  AS failing_warnings
FROM meta.dq_results
GROUP BY checked_at
ORDER BY checked_at"""),
}


def ensure_dq_dashboard(mb: Metabase, database_id: int) -> int:
    existing = mb.cards()
    ids = []
    for name, (display, viz, sql) in DQ_CARDS.items():
        if (name, database_id) in existing:
            ids.append(existing[(name, database_id)])
            continue
        ids.append(mb.call("POST", "/card", json={
            "name": name, "display": display, "visualization_settings": viz,
            "dataset_query": {"type": "native", "database": database_id,
                              "native": {"query": sql, "template-tags": {}}}})["id"])
    found = mb.call("GET", "/search", params={"q": DQ_DASHBOARD, "models": "dashboard"})
    rows = found["data"] if isinstance(found, dict) else found
    for row in rows:
        if row.get("name") == DQ_DASHBOARD and not row.get("archived"):
            return row["id"]
    dashboard = mb.call("POST", "/dashboard", json={
        "name": DQ_DASHBOARD, "description": "نتایج تست‌های کیفیت داده که هر ۱۵ دقیقه داخل ClickHouse اجرا می‌شوند."})
    mb.call("PUT", f"/dashboard/{dashboard['id']}", json={"dashcards": [
        {"id": -1, "card_id": ids[0], "col": 0, "row": 0, "size_x": 24, "size_y": 10,
         "parameter_mappings": [], "visualization_settings": {}},
        {"id": -2, "card_id": ids[1], "col": 0, "row": 10, "size_x": 24, "size_y": 6,
         "parameter_mappings": [], "visualization_settings": {}}]})
    return dashboard["id"]


def main() -> None:
    mb = Metabase()
    mb.wait_healthy()
    mb.login()
    mb.call("PUT", "/setting/report-timezone", json={"value": TZ})
    core_id, ch_id = ensure_databases(mb)
    legacy = ensure_cards(mb, core_id, LEGACY, "core")
    optimised = ensure_cards(mb, ch_id, OPTIMISED, "ClickHouse")
    d1 = ensure_dashboard(mb, LEGACY_DASHBOARD,
                          "نسخه فعلی: کارت‌ها مستقیم روی پایگاه داده عملیاتی (core) اجرا می‌شوند.", legacy)
    d2 = ensure_dashboard(mb, OPTIMISED_DASHBOARD,
                          "نسخه بهینه: کارت‌ها روی لایه گزارش ClickHouse (mart) که با CDC به‌روز می‌شود.", optimised)
    d3 = ensure_dq_dashboard(mb, ch_id)
    print(f"dashboards ready: /dashboard/{d1} (legacy), /dashboard/{d2} (optimised), "
          f"/dashboard/{d3} (data quality)", flush=True)

    benchmark.run(mb, legacy, optimised, wait_for_pipeline=True)


if __name__ == "__main__":
    main()
