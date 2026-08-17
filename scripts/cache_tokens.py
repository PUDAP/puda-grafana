"""Daily Hermes token aggregates into token_cache_* (hermes-logs)."""
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

DATABASE = os.environ.get("INFLUXDB_HERMES_DATABASE", "hermes-logs")


class TokenJob:
    name = "tokens"
    database = DATABASE
    meta_table = "token_cache_meta"

    def snapshot_day(self, day: date) -> int:
        run_id, tag, snap_ns = day_tag(day)
        log(f"{self.name}: caching SGT {day.isoformat()} run_id={run_id}")

        api = (
            "FROM hermes_api_request WHERE event = 'api_end' "
            "AND total_tokens IS NOT NULL "
            "AND time >= TIMESTAMP '{start}' AND time < TIMESTAMP '{end}'"
        )
        tools = (
            "FROM hermes_tool_call WHERE event = 'tool_end' "
            "AND time >= TIMESTAMP '{start}' AND time < TIMESTAMP '{end}'"
        )

        stats_rows: list[dict] = []
        for chunk_start, chunk_end in day_chunks(day):
            stats_rows.extend(
                query_range(
                    self.database,
                    "SELECT COUNT(*) AS n, SUM(total_tokens) AS sum_total, "
                    "SUM(prompt_tokens) AS sum_prompt, SUM(completion_tokens) AS sum_completion, "
                    "SUM(message_count) AS sum_msgs, SUM(api_duration_ms) AS sum_ms " + api,
                    chunk_start,
                    chunk_end,
                )
            )

        calls = sum(n(row, "n") for row in stats_rows)
        total = sum(n(row, "sum_total") for row in stats_rows)
        prompt = sum(n(row, "sum_prompt") for row in stats_rows)
        completion = sum(n(row, "sum_completion") for row in stats_rows)

        by_agent_hour: list[dict] = []
        by_hour: list[dict] = []
        by_agent_rows: list[dict] = []
        by_model_rows: list[dict] = []
        by_finish_rows: list[dict] = []
        by_tool_rows: list[dict] = []

        if calls:
            for chunk_start, chunk_end in day_chunks(day):
                by_agent_hour.extend(
                    query_range(
                        self.database,
                        "SELECT date_bin(INTERVAL '1 hour', time, TIMESTAMP '1970-01-01 00:00:00Z') AS time, "
                        "agent_id, SUM(total_tokens) AS tokens " + api + " GROUP BY 1, agent_id ORDER BY 1",
                        chunk_start,
                        chunk_end,
                    )
                )
                by_hour.extend(
                    query_range(
                        self.database,
                        "SELECT date_bin(INTERVAL '1 hour', time, TIMESTAMP '1970-01-01 00:00:00Z') AS time, "
                        "SUM(prompt_tokens) AS prompt, SUM(completion_tokens) AS completion, COUNT(*) AS calls "
                        + api
                        + " GROUP BY 1 ORDER BY 1",
                        chunk_start,
                        chunk_end,
                    )
                )
                by_agent_rows.extend(
                    query_range(
                        self.database,
                        "SELECT agent_id, COUNT(*) AS calls, SUM(total_tokens) AS tokens, "
                        "SUM(prompt_tokens) AS prompt, SUM(completion_tokens) AS completion, "
                        "SUM(message_count) AS sum_msgs, SUM(api_duration_ms) AS sum_ms "
                        + api
                        + " GROUP BY agent_id",
                        chunk_start,
                        chunk_end,
                    )
                )
                by_model_rows.extend(
                    query_range(
                        self.database,
                        "SELECT model, SUM(total_tokens) AS tokens " + api + " GROUP BY model",
                        chunk_start,
                        chunk_end,
                    )
                )
                by_finish_rows.extend(
                    query_range(
                        self.database,
                        "SELECT finish_reason, COUNT(*) AS calls, SUM(total_tokens) AS tokens "
                        + api
                        + " GROUP BY finish_reason",
                        chunk_start,
                        chunk_end,
                    )
                )
                by_tool_rows.extend(
                    query_range(
                        self.database,
                        "SELECT tool_name, COUNT(*) AS calls " + tools + " GROUP BY tool_name",
                        chunk_start,
                        chunk_end,
                    )
                )

        by_agent = merge_by(
            by_agent_rows,
            "agent_id",
            ("calls", "tokens", "prompt", "completion", "sum_msgs", "sum_ms"),
        )
        by_model = merge_by(by_model_rows, "model", ("tokens",))
        for row in by_finish_rows:
            if not row.get("finish_reason"):
                row["finish_reason"] = "unknown"
        by_finish = merge_by(by_finish_rows, "finish_reason", ("calls", "tokens"))
        by_tool = merge_by(by_tool_rows, "tool_name", ("calls",))

        lines: list[str] = []
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
            lines.append(
                f"token_cache_agent,{tag},agent_id={escape_tag(item['agent_id'])} "
                f"calls={int(n(item, 'calls'))}i,tokens={int(n(item, 'tokens'))}i,"
                f"prompt={int(n(item, 'prompt'))}i,completion={int(n(item, 'completion'))}i,"
                f"sum_msgs={lp_float(n(item, 'sum_msgs'))},"
                f"sum_ms={lp_float(n(item, 'sum_ms'))} {snap_ns}"
            )
        for item in by_model:
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
            lines.append(
                f"token_cache_tools,{tag},tool_name={escape_tag(item['tool_name'])} "
                f"calls={int(n(item, 'calls'))}i {snap_ns}"
            )
        lines.append(
            f"token_cache_stats,{tag} tokens={lp_float(total)},calls={lp_float(calls)},"
            f"prompt={lp_float(prompt)},completion={lp_float(completion)} {snap_ns}"
        )
        lines.append(
            f'token_cache_meta,{tag} snapshot_at="{escape_field_str(datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S %Z"))}",'
            f'window="1 day",calls={int(calls)}i {snap_ns}'
        )

        write_lp(self.database, lines)
        log(
            f"{self.name}: wrote {len(lines)} cache points run_id={run_id} "
            f"tokens={int(total)} calls={int(calls)}"
        )
        return int(calls)
