#!/usr/bin/env python3
"""Shared Influx helpers and daily lookback walker for usage caches."""
from __future__ import annotations

import json
import os
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Singapore")
INFLUXDB_URL = os.environ.get("INFLUXDB_URL", "http://bearsnas:8181").rstrip("/")
INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN", "")
CHUNK = timedelta(hours=6)
EMPTY_STREAK = 60
META_WINDOW = timedelta(days=180)
MAX_LOOKBACK_DAYS = 365 * 5


class CacheJob(Protocol):
    name: str
    database: str
    meta_table: str

    def snapshot_day(self, day: date) -> int: ...


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


def query_sql(database: str, sql: str) -> list[dict]:
    params = urllib.parse.urlencode(
        {"db": database, "format": "json", "q": sql}
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


def lp_float(value) -> str:
    number = float(value)
    if number != number or number in (float("inf"), float("-inf")):
        number = 0.0
    return repr(number)


def write_lp(database: str, lines: list[str]) -> None:
    url = (
        f"{INFLUXDB_URL}/api/v3/write_lp"
        f"?db={urllib.parse.quote(database)}"
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


def sql_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=TZ)
    return start, start + timedelta(days=1)


def day_chunks(day: date) -> list[tuple[datetime, datetime]]:
    start, end = day_bounds(day)
    chunks = []
    cursor = start
    while cursor < end:
        nxt = min(cursor + CHUNK, end)
        chunks.append((cursor, nxt))
        cursor = nxt
    return chunks


def parse_run_id(run_id: str) -> date | None:
    if len(run_id) != 8 or not run_id.isdigit():
        return None
    try:
        return date(int(run_id[:4]), int(run_id[4:6]), int(run_id[6:8]))
    except ValueError:
        return None


def iter_days(start: date, end: date):
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)


def merge_by(rows: list[dict], key: str, sum_fields: tuple[str, ...]) -> list[dict]:
    out: dict[str, dict] = {}
    for row in rows:
        name = row.get(key)
        if not name:
            continue
        acc = out.setdefault(name, {key: name, **{field: 0 for field in sum_fields}})
        for field in sum_fields:
            acc[field] = n(acc, field) + n(row, field)
    return list(out.values())


def query_range(
    database: str, sql_template: str, start: datetime, end: datetime
) -> list[dict]:
    sql = sql_template.format(start=sql_ts(start), end=sql_ts(end))
    return query_sql(database, sql)


def day_tag(day: date) -> tuple[str, str, int]:
    run_id = day.strftime("%Y%m%d")
    _, end = day_bounds(day)
    snap_ns = int(end.timestamp() * 1_000_000_000)
    tag = f"run_id={escape_tag(run_id)},grain=day"
    return run_id, tag, snap_ns


def cached_days(job: CacheJob) -> dict[date, int]:
    have: dict[date, int] = {}
    end = datetime.now(TZ)
    empty_windows = 0
    for _ in range(40):
        start = end - META_WINDOW
        try:
            rows = query_sql(
                job.database,
                f"SELECT run_id, calls FROM {job.meta_table} "
                f"WHERE grain = 'day' AND time >= TIMESTAMP '{sql_ts(start)}' "
                f"AND time < TIMESTAMP '{sql_ts(end)}'",
            )
        except Exception as exc:
            log(f"{job.name}: no usable daily cache window ({exc})")
            rows = []
        if not rows:
            empty_windows += 1
            if empty_windows >= 2:
                break
        else:
            empty_windows = 0
            for row in rows:
                day = parse_run_id(str(row.get("run_id") or ""))
                if day:
                    have[day] = int(n(row, "calls"))
        end = start
    return have


def cache_day(job: CacheJob, day: date, have: dict[date, int]) -> None:
    if day in have:
        return
    have[day] = job.snapshot_day(day)


def ensure_days(job: CacheJob) -> None:
    yesterday = datetime.now(TZ).date() - timedelta(days=1)
    have = cached_days(job)
    start = min(have) if have else yesterday
    failed = 0

    log(f"{job.name}: ensuring cache {start.isoformat()} .. {yesterday.isoformat()}")
    for day in iter_days(start, yesterday):
        try:
            cache_day(job, day, have)
        except Exception:
            failed += 1
            traceback.print_exc()
            log(f"{job.name}: failed to cache {day.isoformat()}; continuing")

    streak = 0
    probe = start
    while probe <= yesterday and have.get(probe, 1) == 0:
        streak += 1
        probe += timedelta(days=1)

    day = start - timedelta(days=1)
    oldest = yesterday - timedelta(days=MAX_LOOKBACK_DAYS)
    while streak < EMPTY_STREAK and day >= oldest:
        try:
            cache_day(job, day, have)
        except Exception:
            failed += 1
            traceback.print_exc()
            log(f"{job.name}: failed to cache {day.isoformat()}; stopping lookback")
            break
        if have.get(day, 0) == 0:
            streak += 1
        else:
            streak = 0
        day -= timedelta(days=1)

    if streak >= EMPTY_STREAK:
        log(f"{job.name}: reached start of logs ({EMPTY_STREAK} empty days before first data)")
    elif day < oldest:
        log(f"{job.name}: stopped lookback at {MAX_LOOKBACK_DAYS} days")

    if failed:
        raise RuntimeError(f"{job.name}: {failed} day(s) failed to cache")
    log(f"{job.name}: daily cache complete")


def seconds_until_next_midnight() -> float:
    now = datetime.now(TZ)
    nxt = (now + timedelta(days=1)).replace(hour=0, minute=0, second=5, microsecond=0)
    delay = (nxt - now).total_seconds()
    return delay if delay > 30 else delay + 86400


def run_jobs(jobs: list[CacheJob], label: str = "cache") -> None:
    errors = 0
    for job in jobs:
        try:
            ensure_days(job)
        except Exception:
            errors += 1
            traceback.print_exc()
            log(f"{job.name}: {label} incomplete")
    if errors:
        raise RuntimeError(f"{errors} cache job(s) failed")


def loop(jobs: list[CacheJob]) -> None:
    if not INFLUXDB_TOKEN:
        raise SystemExit("INFLUXDB_TOKEN is required")
    try:
        run_jobs(jobs, "initial backfill")
    except Exception:
        log("initial backfill incomplete; will retry at midnight SGT")
    while True:
        delay = seconds_until_next_midnight()
        log(f"sleeping {int(delay)}s until next midnight SGT")
        time.sleep(delay)
        try:
            run_jobs(jobs, "daily")
        except Exception:
            log("daily cache failed; retrying in 15 minutes")
            time.sleep(15 * 60)
            try:
                run_jobs(jobs, "retry")
            except Exception:
                traceback.print_exc()
