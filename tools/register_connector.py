"""Registers (or updates) the Debezium PostgreSQL connector on Kafka Connect. Idempotent."""
import json
import os
import pathlib
import string
import time

import requests

CONNECT_URL = os.getenv("CONNECT_URL", "http://connect:8083")
TEMPLATE = pathlib.Path(__file__).with_name("connector.json")


def wait_for_connect() -> None:
    for _ in range(120):
        try:
            if requests.get(f"{CONNECT_URL}/connectors", timeout=5).ok:
                return
        except requests.RequestException:
            pass
        print("waiting for Kafka Connect...", flush=True)
        time.sleep(5)
    raise SystemExit("Kafka Connect did not become ready")


def main() -> None:
    spec = json.loads(string.Template(TEMPLATE.read_text()).substitute(os.environ))
    wait_for_connect()
    name, config = spec["name"], spec["config"]
    resp = requests.put(f"{CONNECT_URL}/connectors/{name}/config", json=config, timeout=30)
    print(f"PUT connector {name}: {resp.status_code} {resp.text[:300]}", flush=True)
    resp.raise_for_status()
    for _ in range(60):
        status = requests.get(f"{CONNECT_URL}/connectors/{name}/status", timeout=10).json()
        states = [status.get("connector", {}).get("state")] + [t.get("state") for t in status.get("tasks", [])]
        print("status:", states, flush=True)
        if states and all(s == "RUNNING" for s in states) and len(states) > 1:
            return
        if "FAILED" in states:
            raise SystemExit(json.dumps(status, indent=2))
        time.sleep(3)
    raise SystemExit("connector did not reach RUNNING")


if __name__ == "__main__":
    main()
