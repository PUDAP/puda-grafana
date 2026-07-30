#!/usr/bin/env python3
"""Validate live Grafana frames for every SQL target in a dashboard."""

from __future__ import annotations

import argparse
import base64
import json
import stat
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


def validate_live_frames(
    payload: dict[str, Any],
    *,
    ref_id: str,
    required_identity_fields: tuple[str, ...] = (),
    min_cardinality: dict[str, int] | None = None,
    require_non_empty: bool = True,
    require_numeric: bool = True,
) -> list[str]:
    errors: list[str] = []
    result = payload.get("results", {}).get(ref_id)
    if not isinstance(result, dict):
        return [f"result {ref_id} is missing"]
    if result.get("error"):
        errors.append(f"result {ref_id} returned an error")
    frames = result.get("frames")
    if not isinstance(frames, list) or not frames:
        if require_non_empty:
            return errors + [f"result {ref_id} has no frames"]
        return errors

    total_rows = 0
    all_values: dict[str, list[Any]] = {}
    numeric_seen = False
    for frame_index, frame in enumerate(frames):
        schema_fields = frame.get("schema", {}).get("fields")
        values = frame.get("data", {}).get("values")
        if not isinstance(schema_fields, list) or not isinstance(values, list):
            errors.append(f"result {ref_id} frame {frame_index} is structurally incomplete")
            continue
        if len(schema_fields) != len(values):
            errors.append(f"result {ref_id} frame {frame_index} column/value counts differ")
            continue
        lengths = {len(column) for column in values if isinstance(column, list)}
        if len(lengths) != 1 or any(not isinstance(column, list) for column in values):
            errors.append(f"result {ref_id} frame {frame_index} columns have invalid lengths")
            continue
        row_count = next(iter(lengths), 0)
        total_rows += row_count
        for field, column in zip(schema_fields, values, strict=True):
            name = field.get("name")
            if not isinstance(name, str) or not name:
                errors.append(f"result {ref_id} frame {frame_index} has an unnamed field")
                continue
            all_values.setdefault(name, []).extend(column)
            if field.get("type") == "number":
                numeric_seen = True

    if require_non_empty and total_rows == 0:
        errors.append(f"result {ref_id} must return a non-empty live frame")
    for field in required_identity_fields:
        if field not in all_values:
            errors.append(f"result {ref_id} is missing identity field {field}")
        elif require_non_empty and any(value in (None, "") for value in all_values[field]):
            errors.append(f"result {ref_id} identity field {field} contains empty values")
    for field, minimum in (min_cardinality or {}).items():
        values = {value for value in all_values.get(field, []) if value not in (None, "")}
        if len(values) < minimum:
            errors.append(
                f"result {ref_id} field {field} cardinality {len(values)} is below {minimum}"
            )
    if require_non_empty and require_numeric and not numeric_seen:
        errors.append(f"result {ref_id} has no numeric value field")
    return errors


def live_row_count(payload: dict[str, Any], ref_id: str) -> int:
    result = payload.get("results", {}).get(ref_id, {})
    frames = result.get("frames", []) if isinstance(result, dict) else []
    total = 0
    for frame in frames if isinstance(frames, list) else []:
        values = frame.get("data", {}).get("values", [])
        if isinstance(values, list) and values and isinstance(values[0], list):
            total += len(values[0])
    return total


def strict_password(path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        raise ValueError("Grafana password file must be a non-symlink regular file")
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        raise ValueError("Grafana password file must not grant group/other permissions")
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise ValueError("Grafana password file is empty")
    return value


def dashboard_targets(dashboard: dict[str, Any]) -> list[tuple[int, dict[str, Any]]]:
    targets: list[tuple[int, dict[str, Any]]] = []
    for panel in dashboard.get("panels", []):
        panel_id = panel.get("id")
        for target in panel.get("targets", []):
            if target.get("hide") is True or not target.get("rawSql"):
                continue
            targets.append((int(panel_id), target))
    return targets


def substitute_dashboard_variables(sql: str, machine_id: str) -> str:
    quoted_machine = "'" + machine_id.replace("'", "''") + "'"
    return (
        sql.replace("${machine_id:singlequote}", quoted_machine)
        .replace("${tube:singlequote}", "'1','2','3','4','5','6'")
        .replace("${category:singlequote}", "'energy','process','alarm','state','boat'")
        .replace(
            "${point_key:singlequote}",
            "'loader.energy.power_total','tube1.process.actual_pressure',"
            "'tube1.alarm.system','tube1.state.process_busy',"
            "'tube1.process.recipe_name'",
        )
    )


def query_target(
    *,
    grafana_url: str,
    authorization: str,
    target: dict[str, Any],
    machine_id: str,
    from_ms: int,
    to_ms: int,
) -> dict[str, Any]:
    query = dict(target)
    query["rawSql"] = substitute_dashboard_variables(str(query["rawSql"]), machine_id)
    query["format"] = "table"
    query.setdefault("intervalMs", 1000)
    query.setdefault("maxDataPoints", 20000)
    body = json.dumps(
        {
            "from": str(from_ms),
            "to": str(to_ms),
            "queries": [query],
        },
        separators=(",", ":"),
    ).encode("utf-8")
    request = urllib.request.Request(
        f"{grafana_url.rstrip('/')}/api/ds/query",
        data=body,
        method="POST",
        headers={
            "Authorization": authorization,
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--grafana-url", default="http://127.0.0.1:3000")
    parser.add_argument("--dashboard", type=Path, required=True)
    parser.add_argument("--username", default="admin")
    parser.add_argument("--password-file", type=Path, required=True)
    parser.add_argument("--machine-id", required=True)
    parser.add_argument("--lookback-seconds", type=int, default=3600)
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="demo mode: execute every target but allow panels not covered by the small mock",
    )
    parser.add_argument("--minimum-non-empty-targets", type=int, default=0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.lookback_seconds <= 0 or args.lookback_seconds > 86400:
        print("FAIL: lookback must be in 1..86400 seconds", file=sys.stderr)
        return 2
    try:
        password = strict_password(args.password_file)
        dashboard = json.loads(args.dashboard.read_text(encoding="utf-8"))
        targets = dashboard_targets(dashboard)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        print(f"FAIL: cannot load live gate inputs: {exc}", file=sys.stderr)
        return 1
    if not targets:
        print("FAIL: dashboard contains no visible SQL targets", file=sys.stderr)
        return 1

    token = base64.b64encode(f"{args.username}:{password}".encode()).decode("ascii")
    authorization = f"Basic {token}"
    to_ms = int(time.time() * 1000)
    from_ms = to_ms - args.lookback_seconds * 1000
    errors: list[str] = []
    non_empty_targets = 0
    for panel_id, target in targets:
        ref_id = str(target.get("refId", ""))
        try:
            payload = query_target(
                grafana_url=args.grafana_url,
                authorization=authorization,
                target=target,
                machine_id=args.machine_id,
                from_ms=from_ms,
                to_ms=to_ms,
            )
        except urllib.error.HTTPError as exc:
            detail = exc.read(1000).decode("utf-8", errors="replace").replace("\n", " ")
            errors.append(
                f"panel {panel_id} target {ref_id}: HTTP {exc.code}: {detail[:500]}"
            )
            continue
        except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
            errors.append(
                f"panel {panel_id} target {ref_id}: request failed ({type(exc).__name__})"
            )
            continue
        sql = str(target.get("rawSql", "")).lower()
        if live_row_count(payload, ref_id) > 0:
            non_empty_targets += 1
        select_clause = sql.split(" from ", 1)[0]
        identities = tuple(
            field
            for field in ("machine_id", "tube", "point_key")
            if field in select_clause
        )
        numeric_sql = any(
            marker in select_clause
            for marker in ("value_numeric", "count(", "avg(", "sum(", "min(", "max(")
        )
        frame_errors = validate_live_frames(
            payload,
            ref_id=ref_id,
            required_identity_fields=identities,
            require_non_empty=not args.allow_empty,
            require_numeric=numeric_sql and not args.allow_empty,
        )
        errors.extend(
            f"panel {panel_id} target {ref_id}: {error}" for error in frame_errors
        )

    if non_empty_targets < args.minimum_non_empty_targets:
        errors.append(
            f"only {non_empty_targets} targets returned rows; "
            f"minimum is {args.minimum_non_empty_targets}"
        )
    if errors:
        for error in errors:
            print(f"FAIL: {error}", file=sys.stderr)
        return 1
    mode = "query-valid" if args.allow_empty else "non-empty contract-valid"
    print(
        f"PASS: all {len(targets)} dashboard targets returned {mode} frames; "
        f"non_empty={non_empty_targets}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
