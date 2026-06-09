#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

build_args=()
if [[ "${1:-}" == "--build" ]]; then
  build_args=(--build)
elif [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  echo "Usage: ./start.sh [--build]"
  echo ""
  echo "Starts the stack without rebuilding images by default."
  echo "Use --build after changing Dockerfile or Python dependencies."
  echo "After changing watcher.py, run: docker compose restart watcher"
  exit 0
elif [[ -n "${1:-}" ]]; then
  echo "Unknown option: $1" >&2
  echo "Usage: ./start.sh [--build]" >&2
  exit 2
fi

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

bash scripts/render-config.sh

echo "▶ Waiting for InfluxDB to be ready …"
docker compose up --force-recreate influxdb-init

if [[ "${#build_args[@]}" -gt 0 ]]; then
  echo "▶ Building and starting all services …"
else
  echo "▶ Starting all services without rebuilding images …"
fi
docker compose up -d "${build_args[@]}" --remove-orphans

bash scripts/init.sh

echo ""
echo "  Grafana  → http://localhost:3000   (${GF_SECURITY_ADMIN_USER:-admin} / ${GF_SECURITY_ADMIN_PASSWORD:-admin})"
echo "  InfluxDB → http://localhost:8181"
echo ""
echo "  Dashboards:"
echo "    Machine Status  → /d/machine-status"
echo "    Command Log     → /d/command-log"
echo "    Bears           → /d/bears"
echo "    IFIM            → /d/ifim"
echo "    VIPSA           → /d/vipsa"
echo ""
echo "Logs: docker compose logs -f watcher"
echo "Restart watcher after code edits: docker compose restart watcher"
