#!/usr/bin/env bash
# Bootstrap InfluxDB database and migrate away superseded Grafana dashboard UIDs.
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo "Missing .env — copy .env.example to .env and fill it in." >&2
  exit 1
fi

# Load .env without shell-evaluating values (plugins line contains ';').
while IFS= read -r line || [[ -n "$line" ]]; do
  line="${line#"${line%%[![:space:]]*}"}"
  [[ -z "$line" || "$line" == \#* ]] && continue
  key="${line%%=*}"
  value="${line#*=}"
  export "$key=$value"
done < .env

required=(
  INFLUXDB_URL
  INFLUXDB_TOKEN
  INFLUXDB_PUDA_DATABASE
  INFLUXDB_HERMES_DATABASE
  GRAFANA_URL
  GRAFANA_USER
  GRAFANA_PASS
  GRAFANA_VIEWER_USER
  GRAFANA_VIEWER_PASSWORD
  GRAFANA_IMRE_MOF_USER
  GRAFANA_IMRE_MOF_PASSWORD
  GRAFANA_IMRE_MOF_DASHBOARD
)
for key in "${required[@]}"; do
  if [[ -z "${!key:-}" ]]; then
    echo "Missing ${key} in .env" >&2
    exit 1
  fi
done

SUPERSEDED_UIDS=(
  puda-machine-status
  puda-command-log
  puda-system-metrics
  bears-machines
  ifim-machines
  vipsa-machines
  hermes
  first
  opentrons
  joule_heater
  vipsa-v2
  puda-imre-mof
)

for db in "${INFLUXDB_PUDA_DATABASE}" "${INFLUXDB_HERMES_DATABASE}"; do
  echo "▶ Ensuring InfluxDB database '${db}' exists …"
  code=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "${INFLUXDB_URL}/api/v3/configure/database" \
    -H "Authorization: Bearer ${INFLUXDB_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"db\":\"${db}\"}")

  case "$code" in
    201) echo "  Created database '${db}'" ;;
    409) echo "  Database '${db}' already exists" ;;
    *)
      echo "  Failed to create database '${db}' (HTTP ${code})" >&2
      exit 1
      ;;
  esac
done

echo "▶ Waiting for Grafana …"
for _ in $(seq 1 30); do
  if curl -sf "${GRAFANA_URL}/api/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! curl -sf "${GRAFANA_URL}/api/health" >/dev/null 2>&1; then
  echo "  Grafana did not become ready in time" >&2
  exit 1
fi

existing_uids=$(curl -sf -u "${GRAFANA_USER}:${GRAFANA_PASS}" \
  "${GRAFANA_URL}/api/search?type=dash-db" | python3 -c \
  "import sys, json; print(' '.join(d['uid'] for d in json.load(sys.stdin)))")

needs_reset=false
for uid in "${SUPERSEDED_UIDS[@]}"; do
  if [[ " ${existing_uids} " == *" ${uid} "* ]]; then
    needs_reset=true
    break
  fi
done

if [[ "${needs_reset}" == true ]]; then
  echo "▶ Superseded dashboard UIDs found — resetting Grafana state …"
  docker compose stop grafana
  docker compose rm -f grafana
  volume=$(docker volume ls -q | grep _grafana_data | head -1)
  if [[ -n "${volume}" ]]; then
    docker volume rm "${volume}"
  fi
  docker compose up -d grafana

  for _ in $(seq 1 30); do
    if curl -sf "${GRAFANA_URL}/api/health" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  echo "  Grafana reprovisioned from dashboards/"
fi

echo "▶ Ensuring Grafana viewer '${GRAFANA_VIEWER_USER}' exists …"
if curl -sf -u "${GRAFANA_USER}:${GRAFANA_PASS}" \
  "${GRAFANA_URL}/api/users/lookup?loginOrEmail=${GRAFANA_VIEWER_USER}" >/dev/null; then
  echo "  Viewer '${GRAFANA_VIEWER_USER}' already exists"
else
  code=$(curl -s -o /tmp/gf-viewer.json -w "%{http_code}" \
    -u "${GRAFANA_USER}:${GRAFANA_PASS}" \
    -X POST "${GRAFANA_URL}/api/admin/users" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"PUDA\",\"login\":\"${GRAFANA_VIEWER_USER}\",\"password\":\"${GRAFANA_VIEWER_PASSWORD}\"}")
  case "$code" in
    200)
      user_id=$(python3 -c "import json; print(json.load(open('/tmp/gf-viewer.json'))['id'])")
      curl -sf -u "${GRAFANA_USER}:${GRAFANA_PASS}" \
        -X PATCH "${GRAFANA_URL}/api/org/users/${user_id}" \
        -H "Content-Type: application/json" \
        -d '{"role":"Viewer"}' >/dev/null
      echo "  Created viewer '${GRAFANA_VIEWER_USER}'"
      ;;
    *)
      echo "  Failed to create viewer '${GRAFANA_VIEWER_USER}' (HTTP ${code})" >&2
      cat /tmp/gf-viewer.json >&2 || true
      exit 1
      ;;
  esac
fi

echo "▶ Ensuring '${GRAFANA_IMRE_MOF_USER}' can only open '${GRAFANA_IMRE_MOF_DASHBOARD}' …"
export GRAFANA_URL GRAFANA_USER GRAFANA_PASS
export GRAFANA_VIEWER_USER GRAFANA_VIEWER_PASSWORD
export GRAFANA_IMRE_MOF_USER GRAFANA_IMRE_MOF_PASSWORD GRAFANA_IMRE_MOF_DASHBOARD
python3 - <<'PY'
import json
import os
import urllib.error
import urllib.parse
import urllib.request
import base64

base = os.environ["GRAFANA_URL"].rstrip("/")
admin_user = os.environ["GRAFANA_USER"]
admin_pass = os.environ["GRAFANA_PASS"]
viewer_login = os.environ["GRAFANA_VIEWER_USER"]
viewer_password = os.environ["GRAFANA_VIEWER_PASSWORD"]
login = os.environ["GRAFANA_IMRE_MOF_USER"]
password = os.environ["GRAFANA_IMRE_MOF_PASSWORD"]
only_uid = os.environ["GRAFANA_IMRE_MOF_DASHBOARD"]


def call(method, path, payload=None, user=None, pw=None):
    user = admin_user if user is None else user
    pw = admin_pass if pw is None else pw
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(base + path, data=data, method=method)
    token = base64.b64encode(f"{user}:{pw}".encode()).decode()
    req.add_header("Authorization", f"Basic {token}")
    if payload is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            body = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            body = raw
        return e.code, body


def lookup(name):
    code, body = call(
        "GET", "/api/users/lookup?loginOrEmail=" + urllib.parse.quote(name)
    )
    if code == 200:
        return body
    if code == 404:
        return None
    raise SystemExit(f"  Lookup for '{name}' failed (HTTP {code})")


viewer = lookup(viewer_login)
if not viewer:
    raise SystemExit(
        f"  Shared viewer '{viewer_login}' is missing; not changing dashboard permissions"
    )

user = lookup(login)
if user is None:
    code, body = call(
        "POST",
        "/api/admin/users",
        {"name": login, "login": login, "password": password},
    )
    if code != 200 or not isinstance(body, dict) or "id" not in body:
        raise SystemExit(f"  Failed to create viewer '{login}' (HTTP {code})")
    user_id = body["id"]
    print(f"  Created viewer '{login}'")
else:
    user_id = user["id"]
    code, _body = call(
        "PUT", f"/api/admin/users/{user_id}/password", {"password": password}
    )
    if code != 200:
        raise SystemExit(f"  Failed to set password for '{login}' (HTTP {code})")
    print(f"  Viewer '{login}' already exists; password updated")

code, _body = call("PATCH", f"/api/org/users/{user_id}", {"role": "Viewer"})
if code != 200:
    raise SystemExit(f"  Failed to set '{login}' role to Viewer (HTTP {code})")

code, dashes = call("GET", "/api/search?type=dash-db")
if code != 200 or not isinstance(dashes, list):
    raise SystemExit(f"  Failed to list dashboards (HTTP {code})")
uids = [d["uid"] for d in dashes]
if only_uid not in uids:
    raise SystemExit(f"  Dashboard '{only_uid}' was not found")

for dash in dashes:
    uid = dash["uid"]
    items = [{"role": "Editor", "permission": 2}]
    if uid == only_uid:
        items.append({"role": "Viewer", "permission": 1})
    else:
        items.append({"userId": viewer["id"], "permission": 1})
    code, _body = call(
        "POST",
        "/api/dashboards/uid/" + urllib.parse.quote(uid) + "/permissions",
        {"items": items},
    )
    if code != 200:
        raise SystemExit(f"  Failed to set permissions on '{uid}' (HTTP {code})")

code, visible = call("GET", "/api/search?type=dash-db", user=login, pw=password)
if code != 200 or not isinstance(visible, list):
    raise SystemExit(f"  Failed to verify '{login}' dashboard list (HTTP {code})")
visible_uids = sorted(d["uid"] for d in visible)
if visible_uids != [only_uid]:
    raise SystemExit(
        f"  '{login}' can see {visible_uids}, expected only '{only_uid}'"
    )

code, _body = call(
    "GET", "/api/dashboards/uid/" + urllib.parse.quote(only_uid), user=login, pw=password
)
if code != 200:
    raise SystemExit(f"  '{login}' cannot open '{only_uid}' (HTTP {code})")

other_uid = next(uid for uid in uids if uid != only_uid)
code, _body = call(
    "GET",
    "/api/dashboards/uid/" + urllib.parse.quote(other_uid),
    user=login,
    pw=password,
)
if code != 403:
    raise SystemExit(
        f"  '{login}' was not denied '{other_uid}' (HTTP {code})"
    )

code, viewer_visible = call(
    "GET", "/api/search?type=dash-db", user=viewer_login, pw=viewer_password
)
if code != 200 or not isinstance(viewer_visible, list):
    raise SystemExit(
        f"  Failed to verify '{viewer_login}' dashboard list (HTTP {code})"
    )
if sorted(d["uid"] for d in viewer_visible) != sorted(uids):
    raise SystemExit(f"  '{viewer_login}' lost access to one or more dashboards")

code, _body = call(
    "PUT",
    "/api/user/preferences",
    {"homeDashboardUID": only_uid},
    user=login,
    pw=password,
)
if code != 200:
    raise SystemExit(f"  Failed to set home dashboard for '{login}' (HTTP {code})")

print(f"  '{login}' can open only '{only_uid}'")
print(f"  '{viewer_login}' can still open all {len(uids)} dashboards")
PY
