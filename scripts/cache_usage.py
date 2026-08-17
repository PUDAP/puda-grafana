#!/usr/bin/env python3
"""Daily usage caches for Grafana /d/token-usage and /d/puda-usage.

Runs token (hermes-logs) and command (puda) snapshots in one process.
Each job scans raw logs in 6-hour chunks so a query stays under InfluxDB 3
Core's 432-file limit, then Grafana reads only the compact cache tables.
"""
from __future__ import annotations

import sys

from cache_commands import CommandJob
from cache_lib import INFLUXDB_TOKEN, ensure_days, loop
from cache_tokens import TokenJob


def jobs() -> list:
    return [TokenJob(), CommandJob()]


if __name__ == "__main__":
    if not INFLUXDB_TOKEN:
        raise SystemExit("INFLUXDB_TOKEN is required")
    selected = jobs()
    if "--loop" in sys.argv:
        loop(selected)
    else:
        for job in selected:
            ensure_days(job)
