#!/usr/bin/env python3
"""Snapshot Hermes token aggregates once per Singapore calendar day.

Grafana /d/token-usage reads token_cache_* measurements. The heavy
hermes_api_request / hermes_tool_call scans run here, not on dashboard load.
"""
from __future__ import annotations

import json
import os
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Singapore")
INFLUXDB_URL = os.environ.get("INFLUXDB_URL", "http://bearsnas:8181").rstrip("/")
INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN", "")
INFLUXDB_DATABASE = os.environ.get("INFLUXDB_HERMES_DATABASE", "hermes-logs")
WINDOWS = ("7 days", "3 days", "1 day")


def log(msg: str) -> None:
    now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[{now}] {msg}", flush=True)


def http_text(req: urllib.request.Request, timeout: int = 120) -> str:
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        raise RuntimeError(f"HTTP {exc.code} {exc.reason}: {body[:1000]}") from exc


def query_sql(sql: str) -> list[dict]:
    params = urllib.parse.urlencode(
        {"db": INFLUXDB_DATABASE, "format": "json", "q": sql}
    )
    req = urllib.request.Request(
        f"{INFLUXDB_URL}/api/v3/query_sql?{params}",
        headers={"Authorization": f"Bearer {INFLUXDB_TOKEN}"},
    )
    body = http_text(req)
    if not body.strip():
        return []
    data = json.loads(body)
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(data["error"])
    if isinstance(data, list):
        return data
    raise RuntimeError(f"unexpected query response: {body[:300]}")


def query_with_window(sql_template: str) -> tuple[str, list[dict]]:
    last_err = None
    for window in WINDOWS:
        sql = sql_template.format(window=window)
        try:
            return window, query_sql(sql)
        except Exception as exc:
            last_err = exc
            err = str(exc)
            if "432" in err or "file limit" in err.lower() or "Parquet" in err:
                log(f"window {window} hit file limit, narrowing")
                continue
            raise
    raise RuntimeError(f"all windows failed: {last_err}")


def lp_float(value) -> str:
    number = float(value)
    if number != number or number in (float("inf"), float("-inf")):
        number = 0.0
    return repr(number)


def write_lp(lines: list[str]) -> None:
    url = (
        f"{INFLUXDB_URL}/api/v3/write_lp"
        f"?db={urllib.parse.quote(INFLUXDB_DATABASE)}"
        f"&precision=nanosecond&accept_partial=true"
    )
    for start in range(0, len(lines), 400):
        chunk = lines[start : start + 400]
        payload = ("\n".join(chunk) + "\n").encode()
        req = urllib.request.Request(
            url,
            data=payload,
            method="POST",
            headers={
                "Authorization": f"Bearer {INFLUXDB_TOKEN}",
                "Content-Type": "text/plain; charset=utf-8",
            },
        )
        http_text(req, timeout=60)


def escape_tag(value: str) -> str:
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace(" ", "\\ ")
        .replace(",", "\\,")
        .replace("=", "\\=")
    )


def escape_field_str(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def to_ns(value) -> int:
    if value is None:
        raise ValueError("missing time")
    if isinstance(value, (int, float)):
        n = int(value)
        if n > 1e15:
            return n
        return n * 1_000_000_000
    text = str(value).replace("Z", "+00:00")
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000_000)


def n(row: dict, key: str, default=0):
    val = row.get(key)
    return default if val is None else val


def has_recent_snapshot() -> bool:
    try:
        rows = query_sql(
            "SELECT COUNT(*) AS n FROM token_cache_meta "
            "WHERE time >= now() - INTERVAL '20 hours'"
        )
    except Exception as exc:
        log(f"no usable snapshot yet ({exc})")
        return False
    count = n(rows[0], "n") if rows else 0
    return count > 0


def snapshot() -> None:
    if not INFLUXDB_TOKEN:
        raise SystemExit("INFLUXDB_TOKEN is required")

    now = datetime.now(TZ)
    run_id = now.strftime("%Y%m%dT%H%M%S")
    snapshot_at = now.strftime("%Y-%m-%d %H:%M:%S %Z")
    snap_ns = int(now.timestamp() * 1_000_000_000)
    tag = f"run_id={escape_tag(run_id)}"
    log(f"starting snapshot run_id={run_id}")

    api = (
        "FROM hermes_api_request WHERE event = 'api_end' "
        "AND total_tokens IS NOT NULL "
        "AND time >= now() - INTERVAL '{window}'"
    )
    tools = (
        "FROM hermes_tool_call WHERE event = 'tool_end' "
        "AND time >= now() - INTERVAL '{window}'"
    )

    window, stats = query_with_window(
        "SELECT COUNT(*) AS n, SUM(total_tokens) AS sum_total, "
        "SUM(prompt_tokens) AS sum_prompt, SUM(completion_tokens) AS sum_completion, "
        "AVG(total_tokens) AS avg_total, AVG(message_count) AS avg_msgs, "
        "AVG(api_duration_ms) AS avg_ms " + api
    )
    _, by_agent_hour = query_with_window(
        "SELECT date_bin(INTERVAL '1 hour', time, TIMESTAMP '1970-01-01 00:00:00Z') AS time, "
        "agent_id, SUM(total_tokens) AS tokens " + api + " GROUP BY 1, agent_id ORDER BY 1"
    )
    _, by_hour = query_with_window(
        "SELECT date_bin(INTERVAL '1 hour', time, TIMESTAMP '1970-01-01 00:00:00Z') AS time, "
        "SUM(prompt_tokens) AS prompt, SUM(completion_tokens) AS completion, COUNT(*) AS calls "
        + api + " GROUP BY 1 ORDER BY 1"
    )
    _, by_agent = query_with_window(
        "SELECT agent_id, COUNT(*) AS calls, SUM(total_tokens) AS tokens, "
        "SUM(prompt_tokens) AS prompt, SUM(completion_tokens) AS completion, "
        "CAST(SUM(prompt_tokens) AS DOUBLE) * 100.0 / NULLIF(SUM(total_tokens), 0) AS prompt_pct, "
        "AVG(total_tokens) AS avg_tok, AVG(message_count) AS avg_msgs, "
        "AVG(api_duration_ms) AS avg_ms " + api + " GROUP BY agent_id"
    )
    _, by_model = query_with_window(
        "SELECT model, SUM(total_tokens) AS tokens " + api + " GROUP BY model"
    )
    _, by_finish = query_with_window(
        "SELECT finish_reason, COUNT(*) AS calls, SUM(total_tokens) AS tokens "
        + api + " GROUP BY finish_reason"
    )
    _, by_tool = query_with_window(
        "SELECT tool_name, COUNT(*) AS calls " + tools
        + " GROUP BY tool_name ORDER BY 2 DESC LIMIT 15"
    )

    row = stats[0] if stats else {}
    total = n(row, "sum_total")
    prompt = n(row, "sum_prompt")
    prompt_share = (float(prompt) * 100.0 / float(total)) if total else 0.0

    lines: list[str] = [
        f'token_cache_meta,{tag} snapshot_at="{escape_field_str(snapshot_at)}",window="{escape_field_str(window)}",calls={int(n(row, "n"))}i {snap_ns}',
        f"token_cache_stats,{tag},stat=tokens value={lp_float(total)} {snap_ns}",
        f"token_cache_stats,{tag},stat=calls value={lp_float(n(row, 'n'))} {snap_ns}",
        f"token_cache_stats,{tag},stat=avg_tokens value={lp_float(n(row, 'avg_total'))} {snap_ns}",
        f"token_cache_stats,{tag},stat=prompt_share value={lp_float(prompt_share)} {snap_ns}",
    ]

    for item in by_agent_hour:
        if not item.get("agent_id") or item.get("time") is None:
            continue
        lines.append(
            f"token_cache_hourly,{tag},agent_id={escape_tag(item['agent_id'])} "
            f"tokens={int(n(item, 'tokens'))}i {to_ns(item['time'])}"
        )
    for item in by_hour:
        if item.get("time") is None:
            continue
        lines.append(
            f"token_cache_hourly_split,{tag} "
            f"prompt={int(n(item, 'prompt'))}i,completion={int(n(item, 'completion'))}i,"
            f"calls={int(n(item, 'calls'))}i {to_ns(item['time'])}"
        )
    for item in by_agent:
        if not item.get("agent_id"):
            continue
        lines.append(
            f"token_cache_agent,{tag},agent_id={escape_tag(item['agent_id'])} "
            f"calls={int(n(item, 'calls'))}i,tokens={int(n(item, 'tokens'))}i,"
            f"prompt={int(n(item, 'prompt'))}i,completion={int(n(item, 'completion'))}i,"
            f"prompt_pct={lp_float(n(item, 'prompt_pct'))},"
            f"avg_tok={lp_float(n(item, 'avg_tok'))},"
            f"avg_msgs={lp_float(n(item, 'avg_msgs'))},"
            f"avg_ms={lp_float(n(item, 'avg_ms'))} {snap_ns}"
        )
    for item in by_model:
        if not item.get("model"):
            continue
        lines.append(
            f"token_cache_model,{tag},model={escape_tag(item['model'])} "
            f"tokens={int(n(item, 'tokens'))}i {snap_ns}"
        )
    for item in by_finish:
        reason = item.get("finish_reason") or "unknown"
        lines.append(
            f"token_cache_finish,{tag},finish_reason={escape_tag(reason)} "
            f"calls={int(n(item, 'calls'))}i,tokens={int(n(item, 'tokens'))}i {snap_ns}"
        )
    for item in by_tool:
        if not item.get("tool_name"):
            continue
        lines.append(
            f"token_cache_tools,{tag},tool_name={escape_tag(item['tool_name'])} "
            f"calls={int(n(item, 'calls'))}i {snap_ns}"
        )

    write_lp(lines)
    log(
        f"wrote {len(lines)} cache points run_id={run_id} window={window} "
        f"tokens={int(total)} calls={int(n(row, 'n'))}"
    )


def seconds_until_next_midnight() -> float:
    now = datetime.now(TZ)
    nxt = (now + timedelta(days=1)).replace(hour=0, minute=0, second=5, microsecond=0)
    delay = (nxt - now).total_seconds()
    return delay if delay > 30 else delay + 86400


def loop() -> None:
    if has_recent_snapshot():
        log("recent snapshot exists; skipping until midnight SGT")
    else:
        try:
            snapshot()
        except Exception:
            traceback.print_exc()
            log("initial snapshot failed; will retry at midnight SGT")
    while True:
        delay = seconds_until_next_midnight()
        log(f"sleeping {int(delay)}s until next midnight SGT")
        time.sleep(delay)
        try:
            snapshot()
        except Exception:
            traceback.print_exc()
            log("snapshot failed; retrying in 15 minutes")
            time.sleep(15 * 60)
            try:
                snapshot()
            except Exception:
                traceback.print_exc()


if __name__ == "__main__":
    if "--loop" in sys.argv:
        loop()
    else:
        snapshot()
