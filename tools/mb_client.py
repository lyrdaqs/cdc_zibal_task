"""Minimal Metabase REST client plus connections to both databases, shared by the tools."""
import os
import time

import clickhouse_connect
import psycopg
import requests

MB_URL = os.getenv("MB_URL", "http://metabase:3000")
CORE_DB_NAME = "Payyar core (PostgreSQL OLTP)"
CH_DB_NAME = "Payyar analytics (ClickHouse mart)"
LEGACY_DASHBOARD = "فروش روزانه"
OPTIMISED_DASHBOARD = "فروش روزانه (بهینه - ClickHouse)"
DQ_DASHBOARD = "کیفیت داده"


class Metabase:
    def __init__(self) -> None:
        self.http = requests.Session()

    def wait_healthy(self, timeout: int = 900) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if self.http.get(f"{MB_URL}/api/health", timeout=5).json().get("status") == "ok":
                    return
            except (requests.RequestException, ValueError):
                pass
            print("waiting for Metabase...", flush=True)
            time.sleep(10)
        raise SystemExit("Metabase did not become healthy")

    def call(self, method: str, path: str, **kwargs):
        resp = self.http.request(method, f"{MB_URL}/api{path}", timeout=600, **kwargs)
        if resp.status_code >= 400:
            raise RuntimeError(f"{method} {path} -> {resp.status_code}: {resp.text[:500]}")
        return resp.json() if resp.content else None

    def login(self) -> None:
        email, password = os.environ["MB_ADMIN_EMAIL"], os.environ["MB_ADMIN_PASSWORD"]
        props = self.call("GET", "/session/properties")
        if not props.get("has-user-setup"):
            self.call("POST", "/setup", json={
                "token": props["setup-token"],
                "user": {"email": email, "password": password, "first_name": "Payyar",
                         "last_name": "Admin", "site_name": "Payyar"},
                "prefs": {"site_name": "Payyar", "site_locale": "en", "allow_tracking": False},
            })
            print("Metabase admin user created", flush=True)
        session = self.call("POST", "/session", json={"username": email, "password": password})
        self.http.headers["X-Metabase-Session"] = session["id"]

    def databases(self) -> dict:
        data = self.call("GET", "/database")
        rows = data["data"] if isinstance(data, dict) else data
        return {d["name"]: d["id"] for d in rows}

    def cards(self) -> dict:
        return {(c["name"], c.get("database_id")): c["id"] for c in self.call("GET", "/card", params={"f": "all"})}

    def run_card(self, card_id: int) -> dict:
        started = time.perf_counter()
        result = self.call("POST", f"/card/{card_id}/query", json={"ignore_cache": True})
        wall_ms = (time.perf_counter() - started) * 1000
        if result.get("status") != "completed":
            raise RuntimeError(f"card {card_id} failed: {result.get('error')}")
        return {"running_time_ms": result.get("running_time"), "wall_ms": wall_ms,
                "row_count": result.get("row_count"), "rows": result["data"]["rows"]}


def pg_connect() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("PG_HOST", "postgres"), dbname=os.environ["PG_DB"],
        user=os.environ["PG_READER_USER"], password=os.environ["PG_READER_PASSWORD"], autocommit=True)


def ch_connect():
    return clickhouse_connect.get_client(
        host=os.getenv("CH_HOST", "clickhouse"), port=8123,
        username=os.environ["CH_ETL_USER"], password=os.environ["CH_ETL_PASSWORD"])
