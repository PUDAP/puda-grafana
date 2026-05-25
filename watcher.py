"""
PUDA Machine Status Watcher

Streams `puda machine watch` output and writes to InfluxDB:
  - machine_status   : health messages → real-time state timeline
  - machine_commands : cmd/response messages → command log dashboard

Topic routing:
  health                      → machine_status
  cmd.queue / immediate       → machine_commands (msg_type=command)
  cmd.response.*              → machine_commands (msg_type=response)

Status mapping:
  - biologic / first : "online" when health received, "offline" after 30s silence
  - opentrons        : uses run_status field from health data
"""
import json
import logging
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone

from influxdb_client_3 import InfluxDBClient3, Point, WritePrecision

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── InfluxDB ──────────────────────────────────────────────────────────────────
INFLUXDB_URL      = os.getenv("INFLUXDB_URL",      "http://localhost:8181")
INFLUXDB_TOKEN    = os.getenv("INFLUXDB_TOKEN",    "apiv3_puda")
INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE", "machines")

# ── PUDA / NATS ───────────────────────────────────────────────────────────────
NATS_SERVERS = os.getenv(
    "NATS_SERVERS",
    "nats://100.109.131.12:4222,nats://100.109.131.12:4223,nats://100.109.131.12:4224",
)
MACHINES = os.getenv("MACHINES", "biologic,first,opentrons").split(",")
OFFLINE_TIMEOUT_SECS = 30

# topic values as reported by `puda machine watch` (the part after category)
COMMAND_TOPICS   = {"queue", "immediate"}
RESPONSE_TOPICS  = {"response.queue", "response.immediate"}

# ── shared state (written by main thread, read by offline monitor) ────────────
last_seen: dict[str, float | None] = {m: None for m in MACHINES}
last_status: dict[str, str] = {}
state_lock = threading.Lock()


# ── helpers ───────────────────────────────────────────────────────────────────

def _parse_ts(ts_str: str) -> datetime:
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return datetime.now(timezone.utc)


# ── health ────────────────────────────────────────────────────────────────────

def derive_status(machine_id: str, data: dict) -> str:
    if machine_id == "opentrons":
        run_status = data.get("run_status", "").strip()
        if run_status:
            return run_status
    return "online"


def write_status(client, machine_id: str, status: str, ts: datetime, extra: dict | None = None) -> None:
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
    client.write(record=point)
    log.debug("health  machine_id=%-10s  status=%s", machine_id, status)


def offline_monitor(client) -> None:
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
                        write_status(client, machine_id, "offline", datetime.now(timezone.utc))
                        last_status[machine_id] = "offline"
                    last_seen[machine_id] = None


# ── command log ───────────────────────────────────────────────────────────────

def write_command(client, machine_id: str, topic: str, msg: dict) -> None:
    """Write a CommandRequest or CommandResponse to machine_commands.

    puda machine watch JSON envelope:
      { timestamp, subject, machine_id, category, topic,
        data: { header: {...}, command: {...}, response: {...} } }
    response key is absent on command messages.
    """
    body:     dict = msg.get("data") or {}
    header:   dict = body.get("header") or {}
    command:  dict = body.get("command") or {}
    response: dict | None = body.get("response")  # None for command messages

    username = header.get("username") or ""
    user_id  = header.get("user_id")  or ""
    run_id   = header.get("run_id")
    ts_str   = header.get("timestamp") or msg.get("timestamp") or ""
    ts       = _parse_ts(ts_str)

    is_command = topic in COMMAND_TOPICS  # "queue" or "immediate"

    point = (
        Point("machine_commands")
        .tag("machine_id", machine_id)
        .tag("msg_type", "command" if is_command else "response")
        .tag("username", username or "unknown")
        .field("topic", topic)
        .field("user_id", user_id)
        .time(ts, WritePrecision.NS)
    )
    if run_id is not None:
        point = point.tag("run_id", str(run_id))

    if is_command:
        cmd_name    = command.get("name", "")
        step_number = command.get("step_number", 0)
        version     = command.get("version") or ""
        params      = command.get("params")  or {}
        kwargs      = command.get("kwargs")  or {}

        point = (
            point
            .tag("status", "sent")
            .field("cmd_name", cmd_name)
            .field("step_number", int(step_number))
            .field("cmd_version", str(version))
            .field("params_json", json.dumps(params))
            .field("kwargs_json", json.dumps(kwargs))
            .field("response_code", "")
            .field("response_message", "")
            .field("data_json", "")
            .field("completed_at", "")
        )
    else:
        resp         = response or {}
        status       = resp.get("status") or ""
        code         = resp.get("code")   or ""
        message      = resp.get("message") or ""
        resp_data    = resp.get("data")   or {}
        completed_at = resp.get("completed_at") or ""
        # also capture the originating command name for easy cross-referencing
        cmd_name     = command.get("name", "")
        step_number  = command.get("step_number", 0)

        point = point.tag("status", status)
        point = point.field("cmd_name", cmd_name)
        point = point.field("step_number", int(step_number))
        point = point.field("cmd_version", str(command.get("version") or ""))
        point = point.field("params_json", json.dumps(command.get("params") or {}))
        point = point.field("kwargs_json", json.dumps(command.get("kwargs") or {}))
        point = point.field("response_code", str(code))
        point = point.field("response_message", str(message))
        point = point.field("data_json", json.dumps(resp_data))
        point = point.field("completed_at", str(completed_at))

    client.write(record=point)
    log.debug("cmd_log  machine=%-12s  topic=%-20s  type=%s", machine_id, topic, "cmd" if is_command else "resp")


# ── main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("Connecting to InfluxDB at %s …", INFLUXDB_URL)
    client = InfluxDBClient3(host=INFLUXDB_URL, token=INFLUXDB_TOKEN, database=INFLUXDB_DATABASE)

    threading.Thread(target=offline_monitor, args=(client,), daemon=True).start()

    cmd = [
        "puda", "machine", "watch",
        "--targets", ",".join(MACHINES),
        "--nats-servers", NATS_SERVERS,
        "--subjects", "tlm,cmd",
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
            category   = msg.get("category", "")
            topic      = msg.get("topic", "")
            data       = msg.get("data", {})
            ts_str     = msg.get("timestamp", "")

            if not machine_id:
                continue

            if category == "tlm" and topic == "health":
                ts     = _parse_ts(ts_str)
                status = derive_status(machine_id, data)
                extra  = {k: v for k, v in data.items() if k in ("cpu", "mem", "temp")}
                write_status(client, machine_id, status, ts, extra)

                with state_lock:
                    last_seen[machine_id] = time.time()
                    prev = last_status.get(machine_id)
                    last_status[machine_id] = status
                    if prev != status:
                        log.info("%s status changed: %s → %s", machine_id, prev or "(none)", status)

            elif category == "cmd" and topic in COMMAND_TOPICS | RESPONSE_TOPICS:
                try:
                    write_command(client, machine_id, topic, msg)
                    log.info("cmd_log  machine=%-12s  topic=%-20s  type=%s", machine_id, topic,
                             "cmd" if topic in COMMAND_TOPICS else "resp")
                except Exception as exc:
                    log.error("cmd_log write failed: %s", exc)

    except KeyboardInterrupt:
        log.info("Interrupted — shutting down")
    finally:
        proc.terminate()
        proc.wait()
        client.close()
        log.info("Done")


if __name__ == "__main__":
    main()
