#!/usr/bin/env bash
# Generate InfluxDB admin token file.
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

INFLUXDB_TOKEN="${INFLUXDB_TOKEN:-apiv3_puda}"
python3 - "$INFLUXDB_TOKEN" <<'PY'
import json
import sys
from pathlib import Path

root = Path(".")
token = sys.argv[1]

(root / "admin-token.json").write_text(
    json.dumps({"token": token, "name": "admin", "description": "puda admin token"}) + "\n"
)
PY

chmod 644 admin-token.json
