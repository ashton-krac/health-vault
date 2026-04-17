"""
Parser for Health Auto Export JSON files.

Handles the native JSON schema produced by the iOS app:
{
  "data": {
    "metrics": [
      {
        "name": "heart_rate",
        "units": "count/min",
        "data": [
          {"date": "2026-04-16 14:30:00 -0400", "qty": 72.0, "source": "Apple Watch"}
        ]
      }
    ],
    "workouts": [
      {
        "name": "Running",
        "start": "...",
        "end": "...",
        "duration": 1800.0,
        "activeEnergy": {"qty": 350.0, "units": "kcal"},
        ...
      }
    ]
  }
}
"""

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from psycopg.types.json import Jsonb

logger = logging.getLogger("health_vault.parser")

# ──────────────────────────────────────────────────────────────────────
# Date parsing
# ──────────────────────────────────────────────────────────────────────

# Health Auto Export uses: "2026-04-16 14:30:00 -0400"
_DATE_FMT_PRIMARY = "%Y-%m-%d %H:%M:%S %z"
# Fallback ISO-8601
_DATE_FMT_ISO = "%Y-%m-%dT%H:%M:%S%z"


def _parse_timestamp(raw: str) -> datetime | None:
    """Parse a Health Auto Export timestamp into a timezone-aware datetime."""
    if not raw:
        return None
    for fmt in (_DATE_FMT_PRIMARY, _DATE_FMT_ISO):
        try:
            return datetime.strptime(raw.strip(), fmt)
        except ValueError:
            continue
    # Last resort: try Python's flexible parser
    try:
        return datetime.fromisoformat(raw.strip())
    except (ValueError, TypeError):
        logger.warning("Unparseable timestamp: %s", raw)
        return None


# ──────────────────────────────────────────────────────────────────────
# Metric name normalization
# ──────────────────────────────────────────────────────────────────────

def _normalize_metric_name(name: str) -> str:
    """Convert metric name to consistent snake_case."""
    # "Heart Rate" -> "heart_rate"
    # "active_energy_burned" stays as-is
    name = name.strip()
    # Replace spaces, hyphens, dots with underscores
    name = re.sub(r"[\s\-\.]+", "_", name)
    # CamelCase -> snake_case
    name = re.sub(r"([a-z])([A-Z])", r"\1_\2", name)
    return name.lower()


# ──────────────────────────────────────────────────────────────────────
# Core parsers
# ──────────────────────────────────────────────────────────────────────


def _parse_metric_block(metric: dict) -> list[dict]:
    """
    Parse a single metric block (e.g. heart_rate with its data array).

    Returns a list of row-dicts ready for DB insertion.
    """
    metric_name = _normalize_metric_name(metric.get("name", "unknown"))
    units = metric.get("units", metric.get("unit", ""))
    data_points = metric.get("data", [])

    rows = []
    for point in data_points:
        ts = _parse_timestamp(point.get("date", ""))
        if ts is None:
            logger.debug("Skipping data point with no valid timestamp in %s", metric_name)
            continue

        # Extract numeric value — could be "qty", "value", "avg", "min", "max"
        value = None
        for key in ("qty", "value", "avg", "Avg", "quantity"):
            if key in point:
                try:
                    value = float(point[key])
                except (ValueError, TypeError):
                    pass
                break

        source = point.get("source", point.get("sourceName", ""))

        rows.append({
            "metric_type": metric_name,
            "recorded_at": ts.isoformat(),
            "source_device": source or None,
            "value": value,
            "unit": units or None,
            "raw_payload": Jsonb(point),
        })

    return rows


def _parse_workout_block(workout: dict) -> list[dict]:
    """
    Parse a workout entry into one or more metric rows.

    Workouts are stored as metric_type = "workout_<type>".
    """
    workout_type = _normalize_metric_name(workout.get("name", "workout"))
    metric_name = f"workout_{workout_type}"

    ts = _parse_timestamp(
        workout.get("start", workout.get("date", ""))
    )
    if ts is None:
        return []

    # Duration in seconds as the primary "value"
    duration = None
    if "duration" in workout:
        try:
            duration = float(workout["duration"])
        except (ValueError, TypeError):
            pass

    source = workout.get("source", workout.get("sourceName", ""))

    return [{
        "metric_type": metric_name,
        "recorded_at": ts.isoformat(),
        "source_device": source or None,
        "value": duration,
        "unit": "seconds",
        "raw_payload": Jsonb(workout),
    }]


# ──────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────


def parse_export_file(filepath: Path) -> list[dict]:
    """
    Parse a Health Auto Export JSON file and return a flat list of
    row-dicts ready for insertion into health_metrics.

    Handles both "metrics" and "workouts" sections.
    """
    logger.info("Parsing: %s", filepath.name)

    with open(filepath, "r", encoding="utf-8") as f:
        try:
            raw = json.load(f)
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in %s: %s", filepath.name, e)
            return []

    # Navigate the Health Auto Export schema
    data = raw.get("data", raw)  # top-level might be "data" or flat

    rows: list[dict] = []

    # Parse metrics
    metrics = data.get("metrics", [])
    for metric in metrics:
        rows.extend(_parse_metric_block(metric))

    # Parse workouts
    workouts = data.get("workouts", [])
    for workout in workouts:
        rows.extend(_parse_workout_block(workout))

    # If neither key exists, try treating the entire payload as a flat
    # list of metric objects (some export configurations do this)
    if not metrics and not workouts and isinstance(data, list):
        for item in data:
            if "data" in item:
                rows.extend(_parse_metric_block(item))
            elif "start" in item or "duration" in item:
                rows.extend(_parse_workout_block(item))

    logger.info("Parsed %d data points from %s", len(rows), filepath.name)
    return rows
