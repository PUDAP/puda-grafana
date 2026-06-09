# PUDA Machine Monitoring — Grafana Dashboards

Real-time monitoring for PUDA lab machines (Biologic, FIRST, Opentrons).
Health and command data is streamed via NATS, stored in InfluxDB 3, and visualised in Grafana.

## Architecture

```
PUDA NATS subjects
        │
        ▼
   watcher.py          ← health + command events → InfluxDB
        │
        ▼
   InfluxDB 3 Core     ← time-series storage (database: machines)
        │
        ▼
   Grafana 13           ← dashboards at http://localhost:3000
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
cp .env.example .env   # optional — edit NATS servers / machines
./start.sh
```

`start.sh` generates the local InfluxDB admin token file, starts all services, creates the InfluxDB database if needed, and resets Grafana state once if superseded dashboard UIDs (`puda-*`) are detected.

Use `./start.sh` rather than `docker compose up` directly — the generated `admin-token.json` is required at startup.

Dashboard JSON files are bind-mounted into Grafana from `./dashboards`, so changing a dashboard does not require rebuilding the Docker image. Grafana polls provisioned dashboards every 10 seconds; refresh the browser after editing a dashboard file.

`watcher.py` is also bind-mounted into the watcher container, so changing it does not require rebuilding the image. Restart the running process after edits:

```bash
docker compose restart watcher
```

Use `./start.sh --build` only after changing image contents such as `Dockerfile`, `pyproject.toml`, or `uv.lock`.

| Service | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3000 | admin / admin (override via `.env`) |
| InfluxDB | http://localhost:8181 | token: `INFLUXDB_TOKEN` from `.env` (default `apiv3_puda`) |

## Services

### `watcher`

`watcher.py` connects directly to NATS with the Python NATS SDK and writes two measurements to InfluxDB:

| Measurement | Source | Key fields |
|---|---|---|
| `machine_status` | `tlm/health` | `status`, `cpu`, `mem`, `temp` |
| `machine_commands` | `cmd/*` | `cmd_name`, `step_number`, `params_json`, `response_code`, … |

Machines are discovered from NATS traffic and marked **offline** after 30 s without a health heartbeat.

It subscribes to `puda.*.tlm.>` and `puda.*.cmd.>`, so all PUDA machine IDs matching those subjects are captured.
Uses `network_mode: host` so the container can reach NATS on the host network (Linux only).

### `influxdb`

InfluxDB 3 Core on port **8181**. `start.sh` generates `admin-token.json` from `INFLUXDB_TOKEN` before starting containers.
The `machines` database is created automatically by `scripts/init.sh` on first run.

### `grafana`

Grafana 13 with provisioned InfluxDB data source and file-based dashboards.
Anonymous viewer access is enabled.

## Configuration

Copy `.env.example` to `.env` and adjust as needed. All variables have defaults matching the PUDA lab setup.

| Variable | Default | Description |
|---|---|---|
| `INFLUXDB_TOKEN` | `apiv3_puda` | InfluxDB admin token (generates `admin-token.json`; passed to Grafana as an environment variable) |
| `INFLUXDB_DATABASE` | `machines` | InfluxDB database name |
| `NATS_SERVERS` | `nats://localhost:4222,…` | Local NATS cluster endpoints |
| `GF_SECURITY_ADMIN_USER` | `admin` | Grafana admin username |
| `GF_SECURITY_ADMIN_PASSWORD` | `admin` | Grafana admin password |

Watcher-specific variables (`INFLUXDB_URL`, etc.) are set in `compose.yml` and can be extended there if needed.

## Deploying to a new host

1. Clone the repo and install Docker + Docker Compose.
2. Copy `.env.example` → `.env` and set `NATS_SERVERS` for the target environment.
3. Run `./start.sh`.
4. Open Grafana at http://localhost:3000 and confirm both dashboards load data.

For a completely clean slate (wipes all stored metrics and Grafana state):

```bash
docker compose down -v
./start.sh
```

## Useful commands

```bash
# Start everything
./start.sh

# Rebuild the watcher image, then start everything
./start.sh --build

# Restart watcher after editing watcher.py
docker compose restart watcher

# Tail watcher logs
docker compose logs -f watcher

# Stop containers
docker compose down

# Stop and wipe all data volumes
docker compose down -v
```
