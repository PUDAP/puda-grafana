"""
PUDA Machine Status Watcher

Streams `puda machine watch` output and writes machine status to InfluxDB
so Grafana can display a real-time state timeline.

Status mapping:
  - biologic / first : "online" when health received, "offline" after 30s silence
  - plsystem         : uses connection_status field from health data (e.g. "sila_connected")
"""
import json
import logging
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── InfluxDB ──────────────────────────────────────────────────────────────────
INFLUXDB_URL    = os.getenv("INFLUXDB_URL",    "http://localhost:8086")
INFLUXDB_TOKEN  = os.getenv("INFLUXDB_TOKEN",  "puda-admin-token")
INFLUXDB_ORG    = os.getenv("INFLUXDB_ORG",    "puda")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "machines")

# ── PUDA / NATS ───────────────────────────────────────────────────────────────
NATS_SERVERS = os.getenv(
    "NATS_SERVERS",
    "nats://100.109.131.12:4222,nats://100.109.131.12:4223,nats://100.109.131.12:4224",
)
MACHINES = os.getenv("MACHINES", "biologic,first,opentrons").split(",")
OFFLINE_TIMEOUT_SECS = 30

# ── shared state (written by main thread, read by offline monitor) ────────────
last_seen: dict[str, float | None] = {m: None for m in MACHINES}
last_status: dict[str, str] = {}
state_lock = threading.Lock()


def derive_status(machine_id: str, data: dict) -> str:
    """Return a human-readable status string from a health message."""
    if machine_id == "opentrons":
        run_status = data.get("run_status", "").strip()
        if run_status:
            return run_status
    return "online"


def write_status(
    write_api,
    machine_id: str,
    status: str,
    ts: datetime,
    extra: dict | None = None,
) -> None:
    point = (
        Point("machine_status")
        .tag("machine_id", machine_id)
        .field("status", status)
        .time(ts, WritePrecision.NS)
    )
    if extra:
        for k, v in extra.items():
            if isinstance(v, (int, float)):
                point = point.field(k, float(v))
    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
    log.debug("wrote  machine_id=%-10s  status=%s", machine_id, status)


def offline_monitor(write_api) -> None:
    """Background thread: marks machines offline when they stop sending health."""
    while True:
        time.sleep(10)
        now = time.time()
        with state_lock:
            for machine_id in MACHINES:
                ls = last_seen.get(machine_id)
                if ls is None:
                    continue
                if (now - ls) > OFFLINE_TIMEOUT_SECS:
                    if last_status.get(machine_id) != "offline":
                        log.info("%s went offline (no heartbeat for >%ds)", machine_id, OFFLINE_TIMEOUT_SECS)
                        write_status(write_api, machine_id, "offline", datetime.now(timezone.utc))
                        last_status[machine_id] = "offline"
                    last_seen[machine_id] = None


def main() -> None:
    log.info("Connecting to InfluxDB at %s …", INFLUXDB_URL)
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    threading.Thread(target=offline_monitor, args=(write_api,), daemon=True).start()

    cmd = [
        "puda", "machine", "watch",
        "--targets", ",".join(MACHINES),
        "--nats-servers", NATS_SERVERS,
    ]
    log.info("Running: %s", " ".join(cmd))

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=sys.stderr, text=True)
    try:
        for raw in proc.stdout:
            line = raw.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError as exc:
                log.warning("JSON parse error: %s — %r", exc, line[:120])
                continue

            machine_id = msg.get("machine_id")
            topic = msg.get("topic")
            data = msg.get("data", {})
            ts_str = msg.get("timestamp", "")

            # Only health messages carry status; ignore pos/alert/etc. for timeline
            if not machine_id or topic != "health":
                continue

            try:
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                ts = datetime.now(timezone.utc)

            status = derive_status(machine_id, data)
            extra = {k: v for k, v in data.items() if k in ("cpu", "mem", "temp")}

            write_status(write_api, machine_id, status, ts, extra)

            with state_lock:
                last_seen[machine_id] = time.time()
                prev = last_status.get(machine_id)
                last_status[machine_id] = status
                if prev != status:
                    log.info("%s status changed: %s → %s", machine_id, prev or "(none)", status)

    except KeyboardInterrupt:
        log.info("Interrupted — shutting down")
    finally:
        proc.terminate()
        proc.wait()
        client.close()
        log.info("Done")


if __name__ == "__main__":
    main()
