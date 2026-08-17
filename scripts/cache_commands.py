"""Daily PUDA command aggregates into cmd_cache_* (puda database)."""
from __future__ import annotations

import os
from datetime import date, datetime

from cache_lib import (
    TZ,
    day_chunks,
    day_tag,
    escape_field_str,
    escape_tag,
    log,
    lp_float,
    merge_by,
    n,
    query_range,
    to_ns,
    write_lp,
)

DATABASE = os.environ.get("INFLUXDB_PUDA_DATABASE", "puda")


class CommandJob:
    name = "commands"
    database = DATABASE
    meta_table = "cmd_cache_meta"

    def snapshot_day(self, day: date) -> int:
        run_id, tag, snap_ns = day_tag(day)
        log(f"{self.name}: caching SGT {day.isoformat()} run_id={run_id}")

        src = "FROM commands WHERE time >= TIMESTAMP '{start}' AND time < TIMESTAMP '{end}'"
        cmds = src + " AND msg_type = 'command'"

        stats_rows: list[dict] = []
        for chunk_start, chunk_end in day_chunks(day):
            stats_rows.extend(
                query_range(
                    self.database,
                    "SELECT "
                    "SUM(CASE WHEN msg_type = 'command' THEN 1 ELSE 0 END) AS commands, "
                    "SUM(CASE WHEN msg_type = 'response' THEN 1 ELSE 0 END) AS responses, "
                    "SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successes, "
                    "SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS errors " + src,
                    chunk_start,
                    chunk_end,
                )
            )

        commands = sum(n(row, "commands") for row in stats_rows)
        responses = sum(n(row, "responses") for row in stats_rows)
        successes = sum(n(row, "successes") for row in stats_rows)
        errors = sum(n(row, "errors") for row in stats_rows)
        calls = int(commands)

        by_machine_hour: list[dict] = []
        by_hour: list[dict] = []
        by_machine_rows: list[dict] = []
        by_cmd_rows: list[dict] = []
        by_user_rows: list[dict] = []
        by_status_rows: list[dict] = []
        by_topic_rows: list[dict] = []

        if commands or responses:
            for chunk_start, chunk_end in day_chunks(day):
                by_machine_hour.extend(
                    query_range(
                        self.database,
                        "SELECT date_bin(INTERVAL '1 hour', time, TIMESTAMP '1970-01-01 00:00:00Z') AS time, "
                        "machine_id, COUNT(*) AS commands " + cmds + " GROUP BY 1, machine_id ORDER BY 1",
                        chunk_start,
                        chunk_end,
                    )
                )
                by_hour.extend(
                    query_range(
                        self.database,
                        "SELECT date_bin(INTERVAL '1 hour', time, TIMESTAMP '1970-01-01 00:00:00Z') AS time, "
                        "SUM(CASE WHEN msg_type = 'command' THEN 1 ELSE 0 END) AS commands, "
                        "SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successes, "
                        "SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS errors "
                        + src
                        + " GROUP BY 1 ORDER BY 1",
                        chunk_start,
                        chunk_end,
                    )
                )
                by_machine_rows.extend(
                    query_range(
                        self.database,
                        "SELECT machine_id, "
                        "SUM(CASE WHEN msg_type = 'command' THEN 1 ELSE 0 END) AS commands, "
                        "SUM(CASE WHEN msg_type = 'response' THEN 1 ELSE 0 END) AS responses, "
                        "SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successes, "
                        "SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS errors "
                        + src
                        + " GROUP BY machine_id",
                        chunk_start,
                        chunk_end,
                    )
                )
                by_cmd_rows.extend(
                    query_range(
                        self.database,
                        "SELECT cmd_name, COUNT(*) AS commands " + cmds + " GROUP BY cmd_name",
                        chunk_start,
                        chunk_end,
                    )
                )
                by_user_rows.extend(
                    query_range(
                        self.database,
                        "SELECT username, COUNT(*) AS commands " + cmds + " GROUP BY username",
                        chunk_start,
                        chunk_end,
                    )
                )
                by_status_rows.extend(
                    query_range(
                        self.database,
                        "SELECT status, COUNT(*) AS calls " + src + " GROUP BY status",
                        chunk_start,
                        chunk_end,
                    )
                )
                by_topic_rows.extend(
                    query_range(
                        self.database,
                        "SELECT topic, COUNT(*) AS commands " + cmds + " GROUP BY topic",
                        chunk_start,
                        chunk_end,
                    )
                )

        by_machine = merge_by(
            by_machine_rows,
            "machine_id",
            ("commands", "responses", "successes", "errors"),
        )
        by_cmd = merge_by(by_cmd_rows, "cmd_name", ("commands",))
        by_user = merge_by(by_user_rows, "username", ("commands",))
        by_status = merge_by(by_status_rows, "status", ("calls",))
        by_topic = merge_by(by_topic_rows, "topic", ("commands",))

        lines: list[str] = []
        for item in by_machine_hour:
            if not item.get("machine_id") or item.get("time") is None:
                continue
            lines.append(
                f"cmd_cache_hourly,{tag},machine_id={escape_tag(item['machine_id'])} "
                f"commands={int(n(item, 'commands'))}i {to_ns(item['time'])}"
            )
        for item in by_hour:
            if item.get("time") is None:
                continue
            lines.append(
                f"cmd_cache_hourly_split,{tag} "
                f"commands={int(n(item, 'commands'))}i,"
                f"successes={int(n(item, 'successes'))}i,"
                f"errors={int(n(item, 'errors'))}i {to_ns(item['time'])}"
            )
        for item in by_machine:
            lines.append(
                f"cmd_cache_machine,{tag},machine_id={escape_tag(item['machine_id'])} "
                f"commands={int(n(item, 'commands'))}i,responses={int(n(item, 'responses'))}i,"
                f"successes={int(n(item, 'successes'))}i,errors={int(n(item, 'errors'))}i {snap_ns}"
            )
        for item in by_cmd:
            lines.append(
                f"cmd_cache_cmd,{tag},cmd_name={escape_tag(item['cmd_name'])} "
                f"commands={int(n(item, 'commands'))}i {snap_ns}"
            )
        for item in by_user:
            lines.append(
                f"cmd_cache_user,{tag},username={escape_tag(item['username'])} "
                f"commands={int(n(item, 'commands'))}i {snap_ns}"
            )
        for item in by_status:
            reason = item.get("status") or "unknown"
            lines.append(
                f"cmd_cache_status,{tag},status={escape_tag(reason)} "
                f"calls={int(n(item, 'calls'))}i {snap_ns}"
            )
        for item in by_topic:
            lines.append(
                f"cmd_cache_topic,{tag},topic={escape_tag(item['topic'])} "
                f"commands={int(n(item, 'commands'))}i {snap_ns}"
            )
        lines.append(
            f"cmd_cache_stats,{tag} commands={lp_float(commands)},"
            f"responses={lp_float(responses)},successes={lp_float(successes)},"
            f"errors={lp_float(errors)} {snap_ns}"
        )
        lines.append(
            f'cmd_cache_meta,{tag} snapshot_at="{escape_field_str(datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S %Z"))}",'
            f'window="1 day",calls={int(calls)}i {snap_ns}'
        )

        write_lp(self.database, lines)
        log(
            f"{self.name}: wrote {len(lines)} cache points run_id={run_id} "
            f"commands={int(commands)} responses={int(responses)} errors={int(errors)}"
        )
        return calls
