"""
PUDA Machine Status Watcher

Streams PUDA NATS machine traffic and writes to InfluxDB:
  - machine_status   : health messages → real-time state timeline
  - machine_commands : cmd/response messages → command log dashboard

Topic routing:
  puda.<machine>.tlm.health             → machine_status
  puda.<machine>.cmd.queue / immediate  → machine_commands (msg_type=command)
  puda.<machine>.cmd.response.*         → machine_commands (msg_type=response)

Status mapping:
  - biologic / first : "online" when health received, "offline" after 30s silence
  - opentrons        : uses run_status field from health data
"""
import asyncio
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

from influxdb_client_3 import InfluxDBClient3, Point, WritePrecision
import nats
from nats.aio.msg import Msg

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ── InfluxDB ──────────────────────────────────────────────────────────────────
INFLUXDB_URL      = os.getenv("INFLUXDB_URL",      "http://localhost:8181")
INFLUXDB_TOKEN    = os.getenv("INFLUXDB_TOKEN",    "apiv3_puda")
INFLUXDB_DATABASE = os.getenv("INFLUXDB_DATABASE", "machines")

# ── PUDA / NATS ───────────────────────────────────────────────────────────────
NATS_SERVERS = os.getenv(
    "NATS_SERVERS",
    "nats://100.109.131.12:4222,nats://100.109.131.12:4223,nats://100.109.131.12:4224",
)
MACHINES = [machine.strip() for machine in os.getenv("MACHINES", "biologic,first,opentrons").split(",") if machine.strip()]
OFFLINE_TIMEOUT_SECS = 30

# topic values from puda.<machine_id>.cmd.<topic>
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


def _nats_servers() -> list[str]:
    return [server.strip() for server in NATS_SERVERS.split(",") if server.strip()]


def _parse_subject(subject: str) -> tuple[str, str, str] | None:
    """Return (machine_id, category, topic) for puda.<machine_id>.<category>.<topic>."""
    parts = subject.split(".")
    if len(parts) < 4 or parts[0] != "puda":
        return None
    return parts[1], parts[2], ".".join(parts[3:])


def _decode_payload(msg: Msg) -> dict | None:
    try:
        return json.loads(msg.data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        logger.warning("NATS payload parse error on %s: %s", msg.subject, exc)
        return None


def _message_timestamp(data: dict) -> str:
    header = data.get("header") or {}
    if isinstance(header, dict):
        header_ts = header.get("timestamp")
        if header_ts:
            return str(header_ts)
    data_ts = data.get("timestamp")
    return str(data_ts) if data_ts else datetime.now(timezone.utc).isoformat()


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
    logger.debug("health  machine_id=%-10s  status=%s", machine_id, status)


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
                        logger.info("%s went offline (no heartbeat for >%ds)", machine_id, OFFLINE_TIMEOUT_SECS)
                        write_status(client, machine_id, "offline", datetime.now(timezone.utc))
                        last_status[machine_id] = "offline"
                    last_seen[machine_id] = None


# ── command log ───────────────────────────────────────────────────────────────

def write_command(client, machine_id: str, topic: str, msg: dict) -> None:
    """Write a CommandRequest or CommandResponse to machine_commands.

    Internal JSON envelope:
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
    logger.debug("cmd_log  machine=%-12s  topic=%-20s  type=%s", machine_id, topic, "cmd" if is_command else "resp")


# ── NATS message handling ─────────────────────────────────────────────────────

def handle_nats_message(client, msg: Msg) -> None:
    parsed_subject = _parse_subject(msg.subject)
    if parsed_subject is None:
        logger.debug("Ignoring unexpected subject: %s", msg.subject)
        return

    machine_id, category, topic = parsed_subject
    if machine_id not in last_seen:
        logger.debug("Ignoring unconfigured machine %s on %s", machine_id, msg.subject)
        return

    data = _decode_payload(msg)
    if data is None:
        return

    envelope = {
        "timestamp": _message_timestamp(data),
        "subject": msg.subject,
        "machine_id": machine_id,
        "category": category,
        "topic": topic,
        "data": data,
    }

    if category == "tlm" and topic == "health":
        ts = _parse_ts(envelope["timestamp"])
        status = derive_status(machine_id, data)
        extra = {k: v for k, v in data.items() if k in ("cpu", "mem", "temp")}
        write_status(client, machine_id, status, ts, extra)

        with state_lock:
            last_seen[machine_id] = time.time()
            prev = last_status.get(machine_id)
            last_status[machine_id] = status
            if prev != status:
                logger.info("%s status changed: %s → %s", machine_id, prev or "(none)", status)

    elif category == "cmd" and topic in COMMAND_TOPICS | RESPONSE_TOPICS:
        try:
            write_command(client, machine_id, topic, envelope)
            logger.info(
                "cmd_log  machine=%-12s  topic=%-20s  type=%s",
                machine_id,
                topic,
                "cmd" if topic in COMMAND_TOPICS else "resp",
            )
        except Exception as exc:
            logger.error("cmd_log write failed: %s", exc)


# ── main ───────────────────────────────────────────────────────────────────────

async def main() -> None:
    logger.info("Connecting to InfluxDB at %s …", INFLUXDB_URL)
    client = InfluxDBClient3(host=INFLUXDB_URL, token=INFLUXDB_TOKEN, database=INFLUXDB_DATABASE)

    threading.Thread(target=offline_monitor, args=(client,), daemon=True).start()

    async def on_error(exc: Exception) -> None:
        logger.error("NATS error: %s", exc)

    async def on_disconnect() -> None:
        logger.warning("Disconnected from NATS")

    async def on_reconnect() -> None:
        logger.info("Reconnected to NATS")

    servers = _nats_servers()
    logger.info("Connecting to NATS at %s", ",".join(servers))
    nc = await nats.connect(
        servers=servers,
        error_cb=on_error,
        disconnected_cb=on_disconnect,
        reconnected_cb=on_reconnect,
    )
    try:
        subscriptions = []
        for machine_id in MACHINES:
            for category in ("tlm", "cmd"):
                subject = f"puda.{machine_id}.{category}.>"

                async def callback(msg: Msg) -> None:
                    handle_nats_message(client, msg)

                subscriptions.append(await nc.subscribe(subject, cb=callback))
                logger.info("Subscribed to %s", subject)

        await asyncio.Future()
    except asyncio.CancelledError:
        logger.info("Interrupted — shutting down")
    finally:
        await nc.drain()
        client.close()
        logger.info("Done")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
