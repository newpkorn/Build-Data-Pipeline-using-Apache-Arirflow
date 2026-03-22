#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <exported-dashboard.json> <repo-dashboard.json>" >&2
  echo "Example: $0 ~/Downloads/dashboard.json grafana/dashboards/curated/05_Thailand_Weather_Overview.json" >&2
  exit 1
fi

SOURCE_JSON="$1"
TARGET_JSON="$2"

if [ ! -f "$SOURCE_JSON" ]; then
  echo "Source file not found: $SOURCE_JSON" >&2
  exit 1
fi

TARGET_DIR="$(dirname "$TARGET_JSON")"
mkdir -p "$TARGET_DIR"

python3 - "$SOURCE_JSON" "$TARGET_JSON" <<'PY'
import json
import pathlib
import sys

source = pathlib.Path(sys.argv[1])
target = pathlib.Path(sys.argv[2])

with source.open() as f:
    data = json.load(f)

# Support both raw dashboard exports and API-style payloads that wrap the
# dashboard under {"dashboard": ..., "meta": ...}.
if isinstance(data, dict) and "dashboard" in data:
    data = data["dashboard"]

if not isinstance(data, dict):
    raise SystemExit("Exported file does not contain a dashboard JSON object.")

# These fields are environment-specific and should not be committed as-is.
data.pop("id", None)
data.pop("version", None)

with target.open("w") as f:
    json.dump(data, f, indent=2)
    f.write("\n")
PY

echo "Synced dashboard JSON to $TARGET_JSON"
