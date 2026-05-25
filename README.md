# PUDA Machine Status — Grafana Dashboard

Real-time monitoring dashboard for PUDA lab machines (Biologic, FIRST, Opentrons).
Machine health data is streamed via NATS, stored in InfluxDB, and visualised in Grafana.

## Architecture

```
puda machine watch (NATS)
        │
        ▼
   watcher.py          ← streams health events, writes to InfluxDB
        │
        ▼
   InfluxDB 2.7        ← time-series storage (bucket: machines)
        │
        ▼
  Grafana 11.5         ← dashboard at http://localhost:3000
```

## Dashboard panels

| Panel | Description |
|---|---|
| State timeline | Per-machine status history (Online / Running / Succeeded / …) |
| Biologic / FIRST / Opentrons | Current status badge |
| CPU Utilization % | Live CPU % for biologic and first |
| Memory Usage % | Gauge showing current memory % |
| Temperature °C | Current temperature per machine |

## Quick start

```bash
./start.sh
```

This builds the watcher image and starts all three services in the background.

| Service | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3000 | admin / admin |
| InfluxDB | http://localhost:8086 | admin / adminpassword |

## Services

### `watcher` (Python)

`watcher.py` runs `puda machine watch` and pipes JSON health events into InfluxDB.

- Writes a `machine_status` measurement with tag `machine_id`
- Fields: `status` (string), `cpu` (float), `mem` (float), `temp` (float)
- Marks a machine **offline** after 30 s of silence

### `influxdb`

InfluxDB 2.7 with auto-initialised org `puda` and bucket `machines`.
Token: `puda-admin-token` (set via environment variable `INFLUXDB_TOKEN`).

### `grafana`

Grafana 11.5 with provisioned InfluxDB data source and dashboard.
Anonymous viewer access is enabled.

## Environment variables

All variables have sensible defaults for local development and can be overridden in `compose.yml` or at runtime.

| Variable | Default | Description |
|---|---|---|
| `INFLUXDB_URL` | `http://localhost:8086` | InfluxDB endpoint |
| `INFLUXDB_TOKEN` | `puda-admin-token` | InfluxDB API token |
| `INFLUXDB_ORG` | `puda` | InfluxDB organisation |
| `INFLUXDB_BUCKET` | `machines` | InfluxDB bucket |
| `NATS_SERVERS` | `nats://100.109.131.12:4222,…` | NATS cluster endpoints |
| `MACHINES` | `biologic,first,opentrons` | Comma-separated machine IDs to watch |

## Useful commands

```bash
# Start everything
./start.sh

# Tail watcher logs
docker compose logs -f watcher

# Stop and remove containers
docker compose down

# Stop and wipe all data volumes
docker compose down -v
```
