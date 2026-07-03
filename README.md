# PUDA Machine Monitoring — Grafana Dashboards

Real-time monitoring for PUDA lab machines (Biologic, FIRST, Opentrons).
Health and command data is streamed via NATS, ingested and stored in InfluxDB 3 by the separate
[`puda-logger`](../puda-logger) service, and visualised here in Grafana.

## Architecture

```
PUDA NATS subjects
        │
        ▼
   puda-logger         ← health + command events → InfluxDB (separate repo/service)
        │
        ▼
   InfluxDB 3 Core     ← time-series storage (database: machines)
        │
        ▼
   Grafana 13           ← dashboards at http://localhost:3000 (this repo)
```

## Dashboards

| Dashboard | URL | Description |
|---|---|---|
| Machine Status | `/d/machine-status` | Status timeline, CPU, memory, temperature |
| Command Log | `/d/command-log` | NATS commands sent and responses received |
| VIPSA | `/d/vipsa` | VIPSA machine status and command logs |
| IFIM | `/d/ifim` | IFIM PL system status and command logs |
| Bears | `/d/bears` | Bears FIRST, Biologic, and Opentrons status and command logs |

## Quick start

```bash
cp .env.example .env   # optional — point at your InfluxDB (puda-logger) instance
./start.sh
```

`start.sh` generates the local InfluxDB admin token file (used by Grafana to authenticate), starts Grafana, ensures the InfluxDB database exists, and resets Grafana state once if superseded dashboard UIDs (`puda-*`) are detected.

Dashboard JSON files are bind-mounted into Grafana from `./dashboards`, so changing a dashboard does not require rebuilding the Docker image. Grafana polls provisioned dashboards every 10 seconds; refresh the browser after editing a dashboard file.

| Service | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3000 | admin / admin (override via `.env`) |
| InfluxDB | http://localhost:8181 | token: `INFLUXDB_TOKEN` from `.env` (default `apiv3_puda`) |

## Services

### `grafana`

Grafana 13 with provisioned InfluxDB data source and file-based dashboards.
Anonymous viewer access is enabled.

InfluxDB itself is not part of this repo — it's ingested and hosted by the separate `puda-logger` service, which writes `machine_status` and `machine_commands` measurements from NATS traffic. This repo only reads from it via the provisioned Grafana data source.

## Configuration

Copy `.env.example` to `.env` and adjust as needed. All variables have defaults matching the PUDA lab setup.

| Variable | Default | Description |
|---|---|---|
| `INFLUXDB_URL` | `http://bearsnas:8181` | URL of the `puda-logger` InfluxDB instance |
| `INFLUXDB_TOKEN` | `apiv3_puda` | InfluxDB admin token (generates `admin-token.json`; passed to Grafana as an environment variable) |
| `INFLUXDB_MACHINES_DATABASE` | `machines` | InfluxDB database name for machine telemetry/command logs |
| `INFLUXDB_HERMES_DATABASE` | `hermes-logs` | InfluxDB database name for Hermes agent session logs |
| `GF_SECURITY_ADMIN_USER` | `admin` | Grafana admin username |
| `GF_SECURITY_ADMIN_PASSWORD` | `admin` | Grafana admin password |
| `GF_PANELS_DISABLE_SANITIZE_HTML` | `true` | Allows provisioned text panels to embed VIPSA camera/control iframes |

## Deploying to a new host

1. Clone the repo and install Docker + Docker Compose.
2. Copy `.env.example` → `.env` and point `INFLUXDB_URL` at the target `puda-logger` instance.
3. Run `./start.sh`.
4. Open Grafana at http://localhost:3000 and confirm dashboards load data.

For a completely clean slate (wipes Grafana state):

```bash
docker compose down -v
./start.sh
```

## Useful commands

```bash
# Start everything
./start.sh

# Stop containers
docker compose down

# Stop and wipe all data volumes
docker compose down -v
```
