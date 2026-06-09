"""
PUDA Machine Status Watcher

Streams PUDA NATS machine traffic and writes to InfluxDB:
  - machine_status   : health messages → real-time state timeline
  - machine_commands : cmd/response messages → command log dashboard

Topic routing:
  puda.*.tlm.health                     → machine_status
  puda.*.cmd.>                          → machine_commands

Status mapping:
  - machines         : "online" when health received, "offline" after 30s silence
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

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
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
    "nats://localhost:4222,nats://localhost:4223,nats://localhost:4224",
)
OFFLINE_TIMEOUT_SECS = 30
NATS_RECONNECT_WAIT_SECS = float(os.getenv("NATS_RECONNECT_WAIT_SECS", "2"))
# ── shared state (written by main thread, read by offline monitor) ────────────
last_seen: dict[str, float | None] = {}
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
        decoded = json.loads(msg.data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        logger.warning("NATS payload parse error on %s: %s", msg.subject, exc)
        return None
    if isinstance(decoded, dict):
        return decoded
    return {"payload": decoded}


def _message_timestamp(data: dict) -> str:
    header = data.get("header") or {}
    if isinstance(header, dict):
        header_ts = header.get("timestamp")
        if header_ts:
            return str(header_ts)
    data_ts = data.get("timestamp")
    return str(data_ts) if data_ts else datetime.now(timezone.utc).isoformat()


def _as_dict(value) -> dict:
    return value if isinstance(value, dict) else {}


def _json_dump(value) -> str:
    try:
        return json.dumps(value if value is not None else {})
    except TypeError:
        return json.dumps(str(value))


def _safe_int(value) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _is_response_message(topic: str, body: dict) -> bool:
    return topic.startswith("response") or "response" in body


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
        offline_machines = []
        with state_lock:
            for machine_id, ls in list(last_seen.items()):
                if ls is None:
                    continue
                if (now - ls) > OFFLINE_TIMEOUT_SECS:
                    if last_status.get(machine_id) != "offline":
                        offline_machines.append(machine_id)
                        last_status[machine_id] = "offline"
                    last_seen[machine_id] = None
        for machine_id in offline_machines:
            logger.info("%s went offline (no heartbeat for >%ds)", machine_id, OFFLINE_TIMEOUT_SECS)
            write_status(client, machine_id, "offline", datetime.now(timezone.utc))


def refresh_liveness(client, machine_id: str, status: str | None = None, ts: datetime | None = None) -> None:
    with state_lock:
        last_seen[machine_id] = time.time()
        prev = last_status.get(machine_id)
        next_status = status or ("online" if prev == "offline" else prev)
        if next_status:
            last_status[machine_id] = next_status

    if next_status and prev != next_status:
        if status is None:
            write_status(client, machine_id, next_status, ts or datetime.now(timezone.utc))
        logger.info("%s status changed: %s → %s", machine_id, prev or "(none)", next_status)


# ── command log ───────────────────────────────────────────────────────────────

def write_command(client, machine_id: str, topic: str, msg: dict) -> None:
    """Write a CommandRequest or CommandResponse to machine_commands.

    Internal JSON envelope:
      { timestamp, subject, machine_id, category, topic,
        data: { header: {...}, command: {...}, response: {...} } }
    response key is absent on command messages.
    """
    body: dict = _as_dict(msg.get("data"))
    header: dict = _as_dict(body.get("header"))
    command: dict = _as_dict(body.get("command"))
    response: dict | None = body.get("response") if isinstance(body.get("response"), dict) else None

    username = header.get("username") or ""
    user_id  = header.get("user_id")  or ""
    run_id   = header.get("run_id")
    ts_str   = header.get("timestamp") or msg.get("timestamp") or ""
    ts       = _parse_ts(ts_str)

    is_response = _is_response_message(topic, body)
    msg_type = "response" if is_response else "command"

    point = (
        Point("machine_commands")
        .tag("machine_id", machine_id)
        .tag("msg_type", msg_type)
        .tag("username", username or "unknown")
        .field("subject", str(msg.get("subject") or ""))
        .field("topic", topic)
        .field("user_id", user_id)
        .field("raw_json", _json_dump(body))
        .time(ts, WritePrecision.NS)
    )
    if run_id is not None:
        point = point.tag("run_id", str(run_id))

    if not is_response:
        cmd_name    = command.get("name", "")
        step_number = command.get("step_number", 0)
        version     = command.get("version") or ""
        params      = command.get("params")  or {}
        kwargs      = command.get("kwargs")  or {}

        point = (
            point
            .tag("status", "sent")
            .field("cmd_name", cmd_name)
            .field("step_number", _safe_int(step_number))
            .field("cmd_version", str(version))
            .field("params_json", _json_dump(params))
            .field("kwargs_json", _json_dump(kwargs))
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
        point = point.field("step_number", _safe_int(step_number))
        point = point.field("cmd_version", str(command.get("version") or ""))
        point = point.field("params_json", _json_dump(command.get("params") or {}))
        point = point.field("kwargs_json", _json_dump(command.get("kwargs") or {}))
        point = point.field("response_code", str(code))
        point = point.field("response_message", str(message))
        point = point.field("data_json", _json_dump(resp_data))
        point = point.field("completed_at", str(completed_at))

    client.write(record=point)
    logger.debug("cmd_log  machine=%-12s  topic=%-20s  type=%s", machine_id, topic, msg_type)


# ── NATS message handling ─────────────────────────────────────────────────────

def handle_nats_message(client, msg: Msg) -> None:
    parsed_subject = _parse_subject(msg.subject)
    if parsed_subject is None:
        logger.debug("Ignoring unexpected subject: %s", msg.subject)
        return

    machine_id, category, topic = parsed_subject

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

    if category == "tlm" and topic == "heartbeat":
        payload_ts = _parse_ts(envelope["timestamp"])
        received_ts = datetime.now(timezone.utc)
        with state_lock:
            prev_status = last_status.get(machine_id)
        heartbeat_status = prev_status if prev_status and prev_status != "offline" else "online"
        logger.info(
            "heartbeat_received  machine=%-12s  subject=%s  payload_ts=%s  received_ts=%s",
            machine_id,
            msg.subject,
            payload_ts.isoformat(),
            received_ts.isoformat(),
        )
        write_status(client, machine_id, heartbeat_status, received_ts)
        refresh_liveness(client, machine_id, status=heartbeat_status, ts=received_ts)

    elif category == "tlm" and topic == "health":
        ts = _parse_ts(envelope["timestamp"])
        status = derive_status(machine_id, data)
        extra = {k: v for k, v in data.items() if k in ("cpu", "mem", "temp")}
        write_status(client, machine_id, status, ts, extra)
        refresh_liveness(client, machine_id, status=status, ts=ts)

    elif category == "cmd":
        try:
            write_command(client, machine_id, topic, envelope)
            logger.info(
                "cmd_log  machine=%-12s  topic=%-20s  type=%s",
                machine_id,
                topic,
                "resp" if _is_response_message(topic, data) else "cmd",
            )
        except Exception as exc:
            logger.error("cmd_log write failed: %s", exc)


# ── main ───────────────────────────────────────────────────────────────────────

async def main() -> None:
    logger.info("Connecting to InfluxDB at %s …", INFLUXDB_URL)
    client = InfluxDBClient3(host=INFLUXDB_URL, token=INFLUXDB_TOKEN, database=INFLUXDB_DATABASE)

    threading.Thread(target=offline_monitor, args=(client,), daemon=True).start()

    async def on_error(exc: Exception) -> None:
        logger.error("NATS error: %r", exc)

    async def on_disconnect() -> None:
        logger.warning(
            "Disconnected from NATS; reconnecting every %.1fs",
            NATS_RECONNECT_WAIT_SECS,
        )

    async def on_reconnect() -> None:
        logger.info("Reconnected to NATS at %s", nc.connected_url.netloc)

    async def on_closed() -> None:
        logger.error("NATS connection closed; watcher process will exit for Docker restart")

    servers = _nats_servers()
    logger.info("Connecting to NATS at %s", ",".join(servers))
    nc = await nats.connect(
        servers=servers,
        error_cb=on_error,
        disconnected_cb=on_disconnect,
        reconnected_cb=on_reconnect,
        closed_cb=on_closed,
        max_reconnect_attempts=-1,
        reconnect_time_wait=NATS_RECONNECT_WAIT_SECS,
    )
    logger.info("Connected to NATS at %s", nc.connected_url.netloc)
    try:
        subscriptions = []
        for category in ("tlm", "cmd"):
            subject = f"puda.*.{category}.>"

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
