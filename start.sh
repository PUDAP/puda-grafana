#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

bash scripts/render-config.sh

echo "▶ Building and starting all services …"
docker compose up -d --build

bash scripts/init.sh

echo ""
echo "  Grafana  → http://localhost:3000   (${GF_SECURITY_ADMIN_USER:-admin} / ${GF_SECURITY_ADMIN_PASSWORD:-admin})"
echo "  InfluxDB → http://localhost:8181"
echo ""
echo "  Dashboards:"
echo "    Machine Status  → /d/machine-status"
echo "    Command Log     → /d/command-log"
echo ""
echo "Logs: docker compose logs -f watcher"
