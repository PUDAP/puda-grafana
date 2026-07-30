#!/usr/bin/env bash
set -euo pipefail

# DEMO ONLY: loopback services, no InfluxDB auth/TLS, known Grafana password.
compose=(docker compose -f compose.demo.yml)
"${compose[@]}" up -d

influx_port="${PUDA_DEMO_INFLUX_PORT:-8182}"
grafana_port="${PUDA_DEMO_GRAFANA_PORT:-3001}"

wait_http() {
  local name="$1" url="$2"
  for _ in $(seq 1 60); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "ERROR: $name did not become ready: $url" >&2
  "${compose[@]}" ps >&2
  return 1
}

wait_http InfluxDB "http://127.0.0.1:${influx_port}/health"

create_status=$(curl -sS -o /dev/null -w '%{http_code}' \
  -X POST "http://127.0.0.1:${influx_port}/api/v3/configure/database" \
  -H 'Content-Type: application/json' \
  --data '{"db":"machines"}')
case "$create_status" in
  200|201|204|409) ;;
  *) echo "InfluxDB demo database creation failed (HTTP ${create_status})" >&2; exit 1 ;;
esac

wait_http Grafana "http://127.0.0.1:${grafana_port}/api/health"
wait_http ALD-dashboard "http://127.0.0.1:${grafana_port}/api/dashboards/uid/puda-ald-opcua"

printf '%s\n' \
  "PUDA dashboard demo is ready." \
  "  Grafana: http://127.0.0.1:${grafana_port}/d/puda-ald-opcua" \
  "  Demo login: admin / puda-demo" \
  "  InfluxDB: http://127.0.0.1:${influx_port}" \
  "  Stop: docker compose -f compose.demo.yml down"
