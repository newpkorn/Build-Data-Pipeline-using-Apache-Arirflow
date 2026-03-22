#!/usr/bin/env python3
import argparse
import base64
import json
import pathlib
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


def build_auth_header(username: str, password: str) -> str:
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def request_json(url: str, method: str, auth_header: str, payload=None):
    data = None
    headers = {"Authorization": auth_header}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body) if body else {}


def wait_for_grafana(base_url: str, auth_header: str, retries: int = 30, delay: int = 2):
    health_url = urllib.parse.urljoin(base_url.rstrip("/") + "/", "api/health")
    for attempt in range(1, retries + 1):
        try:
            data = request_json(health_url, "GET", auth_header)
            if data.get("database") == "ok":
                return
        except Exception:
            pass
        time.sleep(delay)
    raise RuntimeError("Grafana did not become ready in time.")


def dashboard_exists(base_url: str, uid: str, auth_header: str) -> bool:
    url = urllib.parse.urljoin(base_url.rstrip("/") + "/", f"api/dashboards/uid/{uid}")
    try:
        request_json(url, "GET", auth_header)
        return True
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise


def import_dashboard(base_url: str, dashboard_path: pathlib.Path, auth_header: str, if_missing: bool):
    data = json.loads(dashboard_path.read_text())
    if "dashboard" in data:
        data = data["dashboard"]

    uid = data.get("uid")
    title = data.get("title", dashboard_path.stem)
    if not uid:
        raise RuntimeError(f"Dashboard {dashboard_path} is missing a uid.")

    if if_missing and dashboard_exists(base_url, uid, auth_header):
        print(f"Skipping existing dashboard: {title} ({uid})")
        return

    data.pop("id", None)
    payload = {"dashboard": data, "folderId": 0, "overwrite": not if_missing}
    url = urllib.parse.urljoin(base_url.rstrip("/") + "/", "api/dashboards/db")
    response = request_json(url, "POST", auth_header, payload=payload)
    print(f"Imported dashboard: {title} ({uid}) -> {response.get('status', 'ok')}")


def main():
    parser = argparse.ArgumentParser(description="Seed Grafana dashboards into the Grafana DB.")
    parser.add_argument("--url", required=True, help="Grafana base URL, e.g. http://grafana:3000")
    parser.add_argument("--username", required=True, help="Grafana admin username")
    parser.add_argument("--password", required=True, help="Grafana admin password")
    parser.add_argument("--dashboard-dir", required=True, help="Directory containing dashboard JSON files")
    parser.add_argument("--if-missing", action="store_true", help="Only create dashboards that do not already exist")
    args = parser.parse_args()

    dashboard_dir = pathlib.Path(args.dashboard_dir)
    if not dashboard_dir.is_dir():
        raise RuntimeError(f"Dashboard directory not found: {dashboard_dir}")

    auth_header = build_auth_header(args.username, args.password)
    wait_for_grafana(args.url, auth_header)

    dashboard_files = sorted(dashboard_dir.glob("*.json"))
    if not dashboard_files:
        raise RuntimeError(f"No dashboard JSON files found in {dashboard_dir}")

    for dashboard_path in dashboard_files:
        import_dashboard(args.url, dashboard_path, auth_header, if_missing=args.if_missing)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
