#!/usr/bin/env bash
set -euo pipefail

echo "▶ Building and starting all services …"
docker compose up -d --build

echo ""
echo "  Grafana  → http://localhost:3000   (admin / admin)"
echo "  InfluxDB → http://localhost:8086   (admin / adminpassword)"
echo ""
echo "Logs: docker compose logs -f watcher"
