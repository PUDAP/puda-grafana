#!/usr/bin/env bash
# Bootstrap InfluxDB database and migrate away superseded Grafana dashboard UIDs.
set -euo pipefail

cd "$(dirname "$0")/.."

INFLUXDB_URL="${INFLUXDB_URL:-http://localhost:8181}"
INFLUXDB_TOKEN="${INFLUXDB_TOKEN:-apiv3_puda}"
INFLUXDB_MACHINES_DATABASE="${INFLUXDB_MACHINES_DATABASE:-machines}"
INFLUXDB_HERMES_DATABASE="${INFLUXDB_HERMES_DATABASE:-hermes-logs}"

GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"
GRAFANA_USER="${GF_SECURITY_ADMIN_USER:-admin}"
GRAFANA_PASS="${GF_SECURITY_ADMIN_PASSWORD:-admin}"

SUPERSEDED_UIDS=(
  puda-machine-status
  puda-command-log
  puda-system-metrics
  bears-machines
  ifim-machines
  vipsa-machines
  hermes
)

for db in "${INFLUXDB_MACHINES_DATABASE}" "${INFLUXDB_HERMES_DATABASE}"; do
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
