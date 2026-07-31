#!/usr/bin/env python3
"""
Run parameter scripts and test whether their outputs line up with bus bunching.

The script produces two analysis tables:
  1. hourly_parameter_dataset.csv: citywide hourly bunching counts joined to
     time-only inputs such as weather, active alerts, and incident counts.
  2. local_parameter_dataset.csv: route/stop/hour rows from static GTFS joined
     to location-sensitive inputs when those inputs include coordinates.

Important limitation:
  If a parameter source only contains a current snapshot, not historical rows
  covering the event table dates, the script will report it but cannot make a
  defensible historical correlation claim from it.
"""

from __future__ import annotations

import argparse
import math
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


LOCAL_TZ = "America/Los_Angeles"
DEFAULT_DATABRICKS_PROJECT_ROOT = "/Workspace/Users/manu.agnihotri@dot.gov"
DEFAULT_PARAMETER_SCRIPTS = [
    "seattle_recent_weather.py",
    "seattle_fire_mvi.py",
    "wsdot_alerts_kc.py",
    "kcm_alerts.py",
    "seattle_special_events.py",
]


def is_databricks_runtime() -> bool:
    return bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))


def default_project_root() -> Path:
    configured = os.environ.get("IP3_PROJECT_ROOT") or os.environ.get("DATABRICKS_WORKSPACE_DIR")
    if configured:
        return Path(configured)
    if is_databricks_runtime():
        return Path(DEFAULT_DATABRICKS_PROJECT_ROOT)
    return Path(".")


def default_path(*parts: str) -> str:
    root = default_project_root()
    if str(root) in {"", "."}:
        return str(Path(*parts))
    return str(root.joinpath(*parts))


def parse_args_compat(parser: argparse.ArgumentParser) -> argparse.Namespace:
    args, unknown = parser.parse_known_args()
    if unknown and not is_databricks_runtime():
        parser.error(f"unrecognized arguments: {' '.join(unknown)}")
    return args


def cli_arg_was_provided(flag: str) -> bool:
    return any(arg == flag or arg.startswith(flag + "=") for arg in sys.argv[1:])


def apply_project_root_defaults(args: argparse.Namespace) -> argparse.Namespace:
    root = Path(args.project_root)
    if str(root) in {"", "."}:
        return args

    defaults = {
        "--events-csv": ("events_csv", root / "bus_bunching_events.csv"),
        "--parameter-scripts-dir": ("parameter_scripts_dir", root / "Parameter Scripts"),
        "--input-data-dir": ("input_data_dir", root / "KCM Input Data"),
        "--gtfs-dir": ("gtfs_dir", root / "google_transit"),
        "--output-dir": ("output_dir", root / "parameter_bunching_correlation_output"),
    }
    for flag, (attr, value) in defaults.items():
        if not cli_arg_was_provided(flag):
            setattr(args, attr, str(value))
    return args


def should_use_spark(engine: str) -> bool:
    if engine == "pandas":
        return False
    if engine == "spark":
        return True
    if not is_databricks_runtime():
        return False
    try:
        import pyspark  # noqa: F401
    except ImportError:
        return False
    return True


@dataclass(frozen=True)
class ParameterFile:
    name: str
    path: Path
    source_type: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run parameter download scripts and correlate their outputs with "
            "bus_bunching_events.csv."
        )
    )
    parser.add_argument(
        "--events-csv",
        default=default_path("bus_bunching_events.csv"),
        help="Bus bunching event table. Default: %(default)s",
    )
    parser.add_argument(
        "--parameter-scripts-dir",
        default=default_path("Parameter Scripts"),
        help="Folder containing the parameter downloader scripts. Default: %(default)s",
    )
    parser.add_argument(
        "--input-data-dir",
        default=default_path("KCM Input Data"),
        help="Folder where parameter scripts write CSVs. Default: %(default)s",
    )
    parser.add_argument(
        "--gtfs-dir",
        default=default_path("google_transit"),
        help="Static GTFS folder for local route/stop/hour baselines. Default: %(default)s",
    )
    parser.add_argument(
        "--output-dir",
        default=default_path("parameter_bunching_correlation_output"),
        help="Folder for analysis outputs. Default: %(default)s",
    )
    parser.add_argument(
        "--project-root",
        default=str(default_project_root()),
        help=(
            "Project root used for Databricks-friendly defaults. Can also be set "
            "with IP3_PROJECT_ROOT. Default: %(default)s"
        ),
    )
    parser.add_argument(
        "--skip-run-parameters",
        action="store_true",
        help="Do not execute parameter scripts; analyze existing CSV outputs only.",
    )
    parser.add_argument(
        "--engine",
        choices=["auto", "pandas", "spark"],
        default="auto",
        help="Analysis engine. auto uses Spark in Databricks when PySpark is available. Default: %(default)s",
    )
    parser.add_argument(
        "--event-unit",
        choices=["stop-event", "trip-pair-day", "trip-pair-episode"],
        default="trip-pair-episode",
        help=(
            "How to count bus bunching. stop-event preserves one row per stop; "
            "trip-pair-day counts the same two trips once per day; "
            "trip-pair-episode counts the same two trips once per continuous episode. "
            "Default: %(default)s"
        ),
    )
    parser.add_argument(
        "--episode-gap-minutes",
        type=float,
        default=30.0,
        help=(
            "Start a new trip-pair episode after this many minutes without another "
            "row for the same route/direction/trip pair. Default: %(default)s"
        ),
    )
    parser.add_argument(
        "--spatial-radius-meters",
        type=float,
        default=800.0,
        help="Radius for matching coordinate-bearing parameter rows to stops. Default: %(default)s",
    )
    parser.add_argument(
        "--incident-window-hours",
        type=float,
        default=2.0,
        help="Hours after an incident/alert start to count as exposed if no end time exists. Default: %(default)s",
    )
    parser.add_argument(
        "--event-window-hours",
        type=float,
        default=4.0,
        help="Hours around a special event to count as exposed if no end time exists. Default: %(default)s",
    )
    parser.add_argument(
        "--min-pairs",
        type=int,
        default=8,
        help="Minimum non-null observations required for a correlation row. Default: %(default)s",
    )
    return apply_project_root_defaults(parse_args_compat(parser))


def ensure_parameter_output_dirs(input_data_dir: Path) -> None:
    for subdir in [
        input_data_dir / "MVI",
        input_data_dir / "Weather",
        input_data_dir / "Special Events",
        input_data_dir / "WSDOT Alerts",
    ]:
        subdir.mkdir(parents=True, exist_ok=True)


def run_parameter_scripts(parameter_scripts_dir: Path, input_data_dir: Path) -> list[str]:
    ensure_parameter_output_dirs(input_data_dir)
    messages: list[str] = []
    run_cwd = parameter_scripts_dir.parent if parameter_scripts_dir.parent.exists() else Path.cwd()

    for script_name in DEFAULT_PARAMETER_SCRIPTS:
        script_path = parameter_scripts_dir / script_name
        if not script_path.exists():
            messages.append(f"SKIP {script_name}: file not found")
            continue

        messages.append(f"RUN {script_name}")
        completed = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=run_cwd,
            text=True,
            capture_output=True,
            check=False,
        )
        if completed.returncode == 0:
            messages.append(f"OK {script_name}")
        else:
            messages.append(f"FAIL {script_name}: exit code {completed.returncode}")
        if completed.stdout.strip():
            messages.append(indent_block("stdout", completed.stdout.strip()))
        if completed.stderr.strip():
            messages.append(indent_block("stderr", completed.stderr.strip()))

    return messages


def indent_block(label: str, text: str) -> str:
    lines = text.splitlines()
    preview = lines[:30]
    suffix = "" if len(lines) <= 30 else f"\n  ... {len(lines) - 30} more lines"
    return f"{label}:\n  " + "\n  ".join(preview) + suffix


def discover_parameter_files(parameter_scripts_dir: Path, input_data_dir: Path) -> list[ParameterFile]:
    mvi_history = input_data_dir / "MVI" / "seattle_fire_mvi_history.csv"
    mvi_today = input_data_dir / "MVI" / "seattle_fire_mvi_today.csv"
    mvi_path = mvi_history if mvi_history.exists() and mvi_history.stat().st_size > 0 else mvi_today

    candidates = [
        ParameterFile(
            "weather_history",
            input_data_dir / "Weather" / "seattle_weather_history.csv",
            "weather",
        ),
        ParameterFile(
            "weather_forecast",
            input_data_dir / "Weather" / "seattle_weather_forecast.csv",
            "weather",
        ),
        ParameterFile(
            "mvi",
            mvi_path,
            "incident",
        ),
        ParameterFile(
            "wsdot_alerts",
            input_data_dir / "WSDOT Alerts" / "wsdot_alerts_kc.csv",
            "alert",
        ),
        ParameterFile(
            "special_events",
            input_data_dir / "Special Events" / "seattle_special_events_scored.csv",
            "special_event",
        ),
        ParameterFile(
            "kcm_active_advisories",
            parameter_scripts_dir / "kcm_active_advisories.csv",
            "route_alert_snapshot",
        ),
    ]
    return [item for item in candidates if item.path.exists() and item.path.stat().st_size > 0]


def require_columns(df: pd.DataFrame, path: Path, columns: Iterable[str]) -> None:
    missing = [col for col in columns if col not in df.columns]
    if missing:
        raise ValueError(f"{path} is missing required columns: {', '.join(missing)}")


def parse_local_datetime(series: pd.Series) -> pd.Series:
    parsed = series.map(parse_one_local_datetime)
    return pd.to_datetime(parsed, errors="coerce")


def parse_one_local_datetime(value: object) -> pd.Timestamp:
    if pd.isna(value):
        return pd.NaT

    text = str(value).strip()
    if not text:
        return pd.NaT

    cleaned = (
        text.replace(" PDT", " -0700")
        .replace(" PST", " -0800")
        .replace(" UTC", " +0000")
    )
    has_timezone = bool(
        re.search(r"(z|[+-]\d{2}:?\d{2})$", cleaned, flags=re.IGNORECASE)
        or re.search(r"[+-]\d{2}:?\d{2}\)", cleaned)
    )

    if has_timezone:
        parsed_utc = pd.to_datetime(cleaned, errors="coerce", utc=True)
        if pd.isna(parsed_utc):
            return pd.NaT
        return parsed_utc.tz_convert(LOCAL_TZ)

    parsed = pd.to_datetime(cleaned, errors="coerce")
    if pd.isna(parsed):
        return pd.NaT
    if parsed.tzinfo is not None:
        return parsed.tz_convert(LOCAL_TZ)
    return parsed.tz_localize(LOCAL_TZ, nonexistent="NaT", ambiguous="NaT")


def parse_bus_event_time(events: pd.DataFrame) -> pd.Series:
    if {"event_date", "event_hour"}.issubset(events.columns):
        combined = events["event_date"].astype(str) + " " + events["event_hour"].astype(str)
        return parse_local_datetime(combined)

    if "first_seen" in events.columns:
        cleaned = (
            events["first_seen"]
            .astype(str)
            .str.replace(" PDT", " -0700", regex=False)
            .str.replace(" PST", " -0800", regex=False)
        )
        parsed = pd.to_datetime(cleaned, errors="coerce", utc=True)
        return parsed.dt.tz_convert(LOCAL_TZ)

    raise ValueError("Event CSV needs event_date/event_hour or first_seen timestamps.")


def parse_event_sort_time(events: pd.DataFrame) -> pd.Series:
    fallback = parse_bus_event_time(events)
    for col in ["first_predicted_arrival", "first_seen", "last_predicted_arrival", "last_seen"]:
        if col not in events.columns:
            continue
        parsed = parse_local_datetime(events[col])
        if parsed.notna().any():
            return parsed.fillna(fallback)
    return fallback


def load_events(events_csv: Path) -> pd.DataFrame:
    if not events_csv.exists():
        raise FileNotFoundError(f"Event CSV not found: {events_csv}")

    events = pd.read_csv(
        events_csv,
        dtype={
            "route_id": str,
            "route_label": str,
            "direction_id": str,
            "stop_id": str,
        },
    )
    require_columns(
        events,
        events_csv,
        ["route_id", "route_label", "direction_id", "stop_id", "stop_name"],
    )
    events["event_time"] = parse_bus_event_time(events)
    events["event_sort_time"] = parse_event_sort_time(events)
    events = events.dropna(subset=["event_time"]).copy()
    events["event_sort_time"] = events["event_sort_time"].fillna(events["event_time"])
    events["hour_start"] = events["event_time"].dt.floor("h")
    events["event_date_local"] = events["hour_start"].dt.date.astype(str)
    events["hour"] = events["hour_start"].dt.hour
    events["stop_lat_num"] = pd.to_numeric(events.get("stop_lat"), errors="coerce")
    events["stop_lon_num"] = pd.to_numeric(events.get("stop_lon"), errors="coerce")
    return events


def build_count_units(
    events: pd.DataFrame,
    event_unit: str,
    episode_gap_minutes: float,
    warnings: list[str],
) -> pd.DataFrame:
    if event_unit == "stop-event":
        units = events.copy()
        units["count_unit_id"] = "stop-event-" + units.index.astype(str)
        units["count_unit_type"] = "stop-event"
        units["raw_event_rows"] = 1
        units["distinct_stops_in_unit"] = 1
        units["unit_start_time"] = units["event_sort_time"]
        units["unit_end_time"] = units["event_sort_time"]
        return units

    for col in ["lead_trip_id", "trailing_trip_id"]:
        if col not in events.columns:
            warnings.append(
                f"{event_unit}: {col} is missing; falling back to stop-event counting."
            )
            return build_count_units(events, "stop-event", episode_gap_minutes, warnings)

    ordered = events.sort_values(
        ["event_date_local", "route_id", "direction_id", "event_sort_time", "stop_id"],
        kind="mergesort",
    ).copy()
    lead = ordered["lead_trip_id"].fillna("").astype(str).str.strip()
    trailing = ordered["trailing_trip_id"].fillna("").astype(str).str.strip()
    has_pair = (lead != "") & (trailing != "")
    missing_pairs = int((~has_pair).sum())
    if missing_pairs:
        warnings.append(
            f"{event_unit}: {missing_pairs} rows missing one trip id were kept as single-row units."
        )

    pair_min = np.minimum(lead, trailing)
    pair_max = np.maximum(lead, trailing)
    ordered["_trip_pair_key"] = np.where(
        has_pair,
        pair_min + "|" + pair_max,
        "single-row-" + ordered.index.astype(str),
    )

    base_cols = ["event_date_local", "route_id", "direction_id", "_trip_pair_key"]
    if event_unit == "trip-pair-day":
        group_cols = [*base_cols]
    else:
        ordered["_gap_minutes"] = (
            ordered.groupby(base_cols, dropna=False)["event_sort_time"]
            .diff()
            .dt.total_seconds()
            .div(60.0)
        )
        ordered["_new_episode"] = ordered["_gap_minutes"].isna() | (
            ordered["_gap_minutes"] > episode_gap_minutes
        )
        ordered["_episode_seq"] = (
            ordered["_new_episode"].astype(int).groupby(
                [ordered[col] for col in base_cols], dropna=False
            ).cumsum()
        )
        group_cols = [*base_cols, "_episode_seq"]

    first_rows = ordered.groupby(group_cols, as_index=False, dropna=False).first()
    aggregates = (
        ordered.groupby(group_cols, as_index=False, dropna=False)
        .agg(
            raw_event_rows=("route_id", "size"),
            distinct_stops_in_unit=("stop_id", "nunique"),
            unit_start_time=("event_sort_time", "min"),
            unit_end_time=("event_sort_time", "max"),
            min_actual_headway_minutes=(
                "min_actual_headway_minutes",
                "min",
            )
            if "min_actual_headway_minutes" in ordered.columns
            else ("route_id", "size"),
        )
    )
    units = first_rows.drop(
        columns=[
            col
            for col in [
                "raw_event_rows",
                "distinct_stops_in_unit",
                "unit_start_time",
                "unit_end_time",
                "min_actual_headway_minutes",
            ]
            if col in first_rows.columns
        ]
    ).merge(aggregates, on=group_cols, how="inner")
    units["count_unit_type"] = event_unit
    units["count_unit_id"] = (
        units["event_date_local"].astype(str)
        + "|"
        + units["route_id"].astype(str)
        + "|"
        + units["direction_id"].astype(str)
        + "|"
        + units["_trip_pair_key"].astype(str)
    )
    if "_episode_seq" in units.columns:
        units["count_unit_id"] = (
            units["count_unit_id"] + "|" + units["_episode_seq"].astype(str)
        )
    units = units.drop(
        columns=[
            col
            for col in ["_trip_pair_key", "_gap_minutes", "_new_episode", "_episode_seq"]
            if col in units.columns
        ]
    )
    return units


def build_hourly_base(events: pd.DataFrame) -> pd.DataFrame:
    start = events["hour_start"].min()
    end = events["hour_start"].max()
    if pd.isna(start) or pd.isna(end):
        raise ValueError("No usable event timestamps were found.")

    hours = pd.date_range(start=start, end=end, freq="h", tz=LOCAL_TZ)
    base = pd.DataFrame({"hour_start": hours})
    counts = (
        events.groupby("hour_start", as_index=False)
        .agg(
            bunching_events=("route_id", "size"),
            bunched_routes=("route_id", "nunique"),
            bunched_stops=("stop_id", "nunique"),
            median_min_headway_minutes=("min_actual_headway_minutes", "median")
            if "min_actual_headway_minutes" in events.columns
            else ("route_id", "size"),
        )
    )
    merged = base.merge(counts, on="hour_start", how="left")
    for col in ["bunching_events", "bunched_routes", "bunched_stops"]:
        merged[col] = merged[col].fillna(0).astype(int)
    return merged


def load_gtfs_panel(gtfs_dir: Path, events: pd.DataFrame, warnings: list[str]) -> pd.DataFrame:
    required_paths = {
        "trips": gtfs_dir / "trips.txt",
        "stop_times": gtfs_dir / "stop_times.txt",
        "stops": gtfs_dir / "stops.txt",
        "calendar": gtfs_dir / "calendar.txt",
        "calendar_dates": gtfs_dir / "calendar_dates.txt",
    }
    missing = [str(path) for path in required_paths.values() if not path.exists()]
    if missing:
        warnings.append(
            "Local GTFS route/stop/hour baseline skipped; missing files: "
            + ", ".join(missing)
        )
        return pd.DataFrame()

    event_dates = sorted(events["event_date_local"].dropna().unique())
    if not event_dates:
        warnings.append("Local GTFS panel skipped because event dates are empty.")
        return pd.DataFrame()

    trips = pd.read_csv(
        required_paths["trips"],
        dtype=str,
        usecols=["route_id", "service_id", "trip_id", "direction_id"],
    )
    stops = pd.read_csv(
        required_paths["stops"],
        dtype=str,
        usecols=["stop_id", "stop_name", "stop_lat", "stop_lon"],
    )
    stop_times = pd.read_csv(
        required_paths["stop_times"],
        dtype=str,
        usecols=["trip_id", "arrival_time", "stop_id"],
    )
    calendar = pd.read_csv(required_paths["calendar"], dtype=str)
    calendar_dates = pd.read_csv(required_paths["calendar_dates"], dtype=str)

    active_service_dates = active_services_for_dates(calendar, calendar_dates, event_dates)
    if active_service_dates.empty:
        warnings.append("Local GTFS panel skipped because no active services matched event dates.")
        return pd.DataFrame()

    stop_times["scheduled_seconds"] = stop_times["arrival_time"].map(gtfs_time_to_seconds)
    stop_times = stop_times.dropna(subset=["scheduled_seconds"])
    stop_times["hour"] = ((stop_times["scheduled_seconds"] // 3600) % 24).astype(int)

    trip_dates = trips.merge(active_service_dates, on="service_id", how="inner")
    scheduled = stop_times.merge(trip_dates, on="trip_id", how="inner")
    scheduled = scheduled.merge(stops, on="stop_id", how="left")

    panel = (
        scheduled.groupby(
            [
                "service_date",
                "hour",
                "route_id",
                "direction_id",
                "stop_id",
                "stop_name",
                "stop_lat",
                "stop_lon",
            ],
            as_index=False,
        )
        .agg(scheduled_stop_arrivals=("trip_id", "nunique"))
    )
    panel["hour_start"] = pd.to_datetime(
        panel["service_date"] + " " + panel["hour"].astype(str) + ":00",
        errors="coerce",
    ).dt.tz_localize(LOCAL_TZ, nonexistent="NaT", ambiguous="NaT")
    panel = panel.dropna(subset=["hour_start"])
    panel["stop_lat_num"] = pd.to_numeric(panel["stop_lat"], errors="coerce")
    panel["stop_lon_num"] = pd.to_numeric(panel["stop_lon"], errors="coerce")

    event_counts = (
        events.groupby(["hour_start", "route_id", "direction_id", "stop_id"], as_index=False)
        .agg(bunching_events=("route_id", "size"))
    )
    panel = panel.merge(
        event_counts,
        on=["hour_start", "route_id", "direction_id", "stop_id"],
        how="left",
    )
    panel["bunching_events"] = panel["bunching_events"].fillna(0).astype(int)
    panel["bunching_rate_per_100_scheduled"] = np.where(
        panel["scheduled_stop_arrivals"] > 0,
        panel["bunching_events"] / panel["scheduled_stop_arrivals"] * 100.0,
        np.nan,
    )
    return panel


def active_services_for_dates(
    calendar: pd.DataFrame, calendar_dates: pd.DataFrame, event_dates: list[str]
) -> pd.DataFrame:
    weekday_cols = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ]
    require_columns(calendar, Path("calendar.txt"), ["service_id", "start_date", "end_date", *weekday_cols])

    rows: list[dict[str, str]] = []
    for date_string in event_dates:
        date = pd.to_datetime(date_string).date()
        yyyymmdd = date.strftime("%Y%m%d")
        weekday_col = weekday_cols[date.weekday()]

        active = calendar[
            (calendar["start_date"] <= yyyymmdd)
            & (calendar["end_date"] >= yyyymmdd)
            & (calendar[weekday_col] == "1")
        ]["service_id"].astype(str)

        if not calendar_dates.empty and {"service_id", "date", "exception_type"}.issubset(
            calendar_dates.columns
        ):
            exceptions = calendar_dates[calendar_dates["date"] == yyyymmdd]
            removed = set(
                exceptions.loc[exceptions["exception_type"] == "2", "service_id"].astype(str)
            )
            added = set(
                exceptions.loc[exceptions["exception_type"] == "1", "service_id"].astype(str)
            )
            services = (set(active) - removed) | added
        else:
            services = set(active)

        rows.extend({"service_id": service_id, "service_date": date_string} for service_id in services)

    return pd.DataFrame(rows).drop_duplicates()


def gtfs_time_to_seconds(value: object) -> float:
    if pd.isna(value):
        return math.nan
    parts = str(value).split(":")
    if len(parts) != 3:
        return math.nan
    try:
        hours, minutes, seconds = (int(part) for part in parts)
    except ValueError:
        return math.nan
    return float(hours * 3600 + minutes * 60 + seconds)


def read_parameter_csv(parameter_file: ParameterFile, warnings: list[str]) -> pd.DataFrame:
    try:
        df = pd.read_csv(parameter_file.path)
    except Exception as exc:
        warnings.append(f"{parameter_file.name}: could not read {parameter_file.path}: {exc}")
        return pd.DataFrame()

    df.columns = [str(col).strip() for col in df.columns]
    if df.empty:
        warnings.append(f"{parameter_file.name}: file exists but has no rows.")
    return df


def add_weather_features(hourly: pd.DataFrame, df: pd.DataFrame, name: str, warnings: list[str]) -> pd.DataFrame:
    if "timestamp" not in df.columns:
        warnings.append(f"{name}: skipped weather join because timestamp is missing.")
        return hourly

    weather = df.copy()
    weather["hour_start"] = parse_local_datetime(weather["timestamp"]).dt.floor("h")
    weather = weather.dropna(subset=["hour_start"])
    if not overlaps_analysis_hours(
        weather["hour_start"],
        weather["hour_start"],
        hourly["hour_start"].min(),
        hourly["hour_start"].max(),
    ):
        warnings.append(
            f"{name}: weather timestamps do not overlap the bus bunching analysis window."
        )
        return hourly

    numeric_cols = numeric_parameter_columns(
        weather,
        exclude={"timestamp", "hour_start", "weather_code"},
    )
    if "weather_code" in weather.columns:
        weather[f"{name}_weather_code"] = pd.to_numeric(weather["weather_code"], errors="coerce")
        numeric_cols.append(f"{name}_weather_code")

    if not numeric_cols:
        warnings.append(f"{name}: no numeric weather columns found for correlation.")
        return hourly

    renamed = weather[["hour_start", *numeric_cols]].copy()
    renamed = renamed.groupby("hour_start", as_index=False).mean(numeric_only=True)
    renamed = renamed.rename(columns={col: f"{name}_{col}" for col in numeric_cols})
    return hourly.merge(renamed, on="hour_start", how="left")


def add_count_window_features(
    hourly: pd.DataFrame,
    df: pd.DataFrame,
    name: str,
    time_cols: Iterable[str],
    end_cols: Iterable[str],
    default_window_hours: float,
    warnings: list[str],
    open_status_cols: Iterable[str] = (),
    analysis_end: pd.Timestamp | None = None,
) -> pd.DataFrame:
    start_col = first_existing(df, time_cols)
    if start_col is None:
        warnings.append(f"{name}: no usable timestamp column; skipped time-window count feature.")
        return hourly

    temp = df.copy()
    temp["_start_time"] = parse_local_datetime(temp[start_col])
    temp = temp.dropna(subset=["_start_time"])
    if temp.empty:
        warnings.append(f"{name}: timestamp column exists but no dates parsed.")
        return hourly

    end_col = first_existing(temp, end_cols)
    if end_col is not None:
        temp["_end_time"] = parse_local_datetime(temp[end_col])
    else:
        temp["_end_time"] = pd.NaT

    open_status_col = first_existing(temp, open_status_cols)
    if open_status_col is not None and analysis_end is not None:
        is_open = temp[open_status_col].astype(str).str.contains("open|active", case=False, na=False)
        temp.loc[is_open & temp["_end_time"].isna(), "_end_time"] = analysis_end

    temp["_end_time"] = temp["_end_time"].fillna(
        temp["_start_time"] + pd.to_timedelta(default_window_hours, unit="h")
    )
    if not overlaps_analysis_hours(
        temp["_start_time"],
        temp["_end_time"],
        hourly["hour_start"].min(),
        hourly["hour_start"].max(),
    ):
        warnings.append(
            f"{name}: time windows do not overlap the bus bunching analysis window."
        )
        return hourly

    hourly[f"{name}_active_count"] = 0
    for _, row in temp.iterrows():
        mask = (hourly["hour_start"] >= row["_start_time"].floor("h")) & (
            hourly["hour_start"] <= row["_end_time"].ceil("h")
        )
        hourly.loc[mask, f"{name}_active_count"] += 1

    severity_col = first_existing(temp, ["Severity_Score", "severity", "Severity"])
    if severity_col is not None:
        hourly[f"{name}_severity_sum"] = 0.0
        temp["_severity"] = pd.to_numeric(temp[severity_col], errors="coerce").fillna(0.0)
        for _, row in temp.iterrows():
            mask = (hourly["hour_start"] >= row["_start_time"].floor("h")) & (
                hourly["hour_start"] <= row["_end_time"].ceil("h")
            )
            hourly.loc[mask, f"{name}_severity_sum"] += float(row["_severity"])

    return hourly


def add_route_snapshot_features(
    events: pd.DataFrame, df: pd.DataFrame, name: str, output_dir: Path, warnings: list[str]
) -> None:
    if "Affected_Routes" not in df.columns:
        warnings.append(f"{name}: snapshot file has no Affected_Routes column.")
        return

    route_tokens = set()
    for value in df["Affected_Routes"].dropna().astype(str):
        for token in re.split(r"[,;/]", value):
            cleaned = normalize_route_label(token)
            if cleaned:
                route_tokens.add(cleaned)

    if not route_tokens:
        warnings.append(f"{name}: no route labels could be parsed from Affected_Routes.")
        return

    route_summary = (
        events.assign(_route_norm=events["route_label"].map(normalize_route_label))
        .groupby(["route_label", "_route_norm"], as_index=False)
        .agg(bunching_events=("route_label", "size"))
    )
    route_summary[f"{name}_is_currently_alerted"] = route_summary["_route_norm"].isin(route_tokens)
    route_summary = route_summary.drop(columns=["_route_norm"])
    route_summary.to_csv(output_dir / f"{name}_route_snapshot_summary.csv", index=False)
    warnings.append(
        f"{name}: route snapshot summary written, but this is not a historical correlation "
        "unless the advisory file covers the same dates as the event CSV."
    )


def add_local_spatial_features(
    panel: pd.DataFrame,
    df: pd.DataFrame,
    name: str,
    time_cols: Iterable[str],
    end_cols: Iterable[str],
    default_window_hours: float,
    radius_meters: float,
    warnings: list[str],
    open_status_cols: Iterable[str] = (),
    analysis_end: pd.Timestamp | None = None,
) -> pd.DataFrame:
    if panel.empty:
        return panel

    lat_col = first_existing(df, ["lat", "latitude", "Latitude", "LAT", "Y"])
    lon_col = first_existing(df, ["lon", "lng", "longitude", "Longitude", "LON", "X"])
    start_col = first_existing(df, time_cols)
    if lat_col is None or lon_col is None:
        warnings.append(
            f"{name}: no latitude/longitude columns found; coordinate-radius matching skipped."
        )
        return panel
    if start_col is None:
        warnings.append(f"{name}: coordinates found but no timestamp column; local matching skipped.")
        return panel

    temp = df.copy()
    temp["_lat"] = pd.to_numeric(temp[lat_col], errors="coerce")
    temp["_lon"] = pd.to_numeric(temp[lon_col], errors="coerce")
    temp["_start_time"] = parse_local_datetime(temp[start_col])
    temp = temp.dropna(subset=["_lat", "_lon", "_start_time"])
    if temp.empty:
        warnings.append(f"{name}: coordinates/timestamps did not parse into usable rows.")
        return panel

    end_col = first_existing(temp, end_cols)
    if end_col is not None:
        temp["_end_time"] = parse_local_datetime(temp[end_col])
    else:
        temp["_end_time"] = pd.NaT

    open_status_col = first_existing(temp, open_status_cols)
    if open_status_col is not None and analysis_end is not None:
        is_open = temp[open_status_col].astype(str).str.contains("open|active", case=False, na=False)
        temp.loc[is_open & temp["_end_time"].isna(), "_end_time"] = analysis_end

    temp["_end_time"] = temp["_end_time"].fillna(
        temp["_start_time"] + pd.to_timedelta(default_window_hours, unit="h")
    )
    if not overlaps_analysis_hours(
        temp["_start_time"],
        temp["_end_time"],
        panel["hour_start"].min(),
        panel["hour_start"].max(),
    ):
        warnings.append(
            f"{name}: coordinate-bearing rows do not overlap the local analysis window."
        )
        return panel

    feature_count = f"{name}_nearby_active_count"
    panel[feature_count] = 0
    severity_col = first_existing(temp, ["Severity_Score", "severity", "Severity"])
    if severity_col is not None:
        panel[f"{name}_nearby_severity_sum"] = 0.0
        temp["_severity"] = pd.to_numeric(temp[severity_col], errors="coerce").fillna(0.0)

    panel_coords = panel.dropna(subset=["stop_lat_num", "stop_lon_num"])
    if panel_coords.empty:
        warnings.append(f"{name}: local panel has no stop coordinates.")
        return panel

    for _, row in temp.iterrows():
        time_mask = (panel_coords["hour_start"] >= row["_start_time"].floor("h")) & (
            panel_coords["hour_start"] <= row["_end_time"].ceil("h")
        )
        if not time_mask.any():
            continue

        candidate_index = panel_coords.index[time_mask]
        distances = haversine_meters(
            panel.loc[candidate_index, "stop_lat_num"].to_numpy(dtype=float),
            panel.loc[candidate_index, "stop_lon_num"].to_numpy(dtype=float),
            float(row["_lat"]),
            float(row["_lon"]),
        )
        matched_index = candidate_index[distances <= radius_meters]
        panel.loc[matched_index, feature_count] += 1
        if severity_col is not None:
            panel.loc[matched_index, f"{name}_nearby_severity_sum"] += float(row["_severity"])

    return panel


def add_local_text_features(
    panel: pd.DataFrame,
    df: pd.DataFrame,
    name: str,
    time_cols: Iterable[str],
    end_cols: Iterable[str],
    text_cols: Iterable[str],
    default_window_hours: float,
    warnings: list[str],
    open_status_cols: Iterable[str] = (),
    analysis_end: pd.Timestamp | None = None,
) -> pd.DataFrame:
    if panel.empty:
        return panel

    start_col = first_existing(df, time_cols)
    if start_col is None:
        warnings.append(f"{name}: no timestamp column; local text matching skipped.")
        return panel

    available_text_cols = [col for col in text_cols if col in df.columns]
    if not available_text_cols:
        warnings.append(f"{name}: no location text columns; local text matching skipped.")
        return panel

    temp = df.copy()
    temp["_start_time"] = parse_local_datetime(temp[start_col])
    temp = temp.dropna(subset=["_start_time"])
    if temp.empty:
        warnings.append(f"{name}: no parseable timestamps for local text matching.")
        return panel

    end_col = first_existing(temp, end_cols)
    if end_col is not None:
        temp["_end_time"] = parse_local_datetime(temp[end_col])
    else:
        temp["_end_time"] = pd.NaT

    open_status_col = first_existing(temp, open_status_cols)
    if open_status_col is not None and analysis_end is not None:
        is_open = temp[open_status_col].astype(str).str.contains("open|active", case=False, na=False)
        temp.loc[is_open & temp["_end_time"].isna(), "_end_time"] = analysis_end

    temp["_end_time"] = temp["_end_time"].fillna(
        temp["_start_time"] + pd.to_timedelta(default_window_hours, unit="h")
    )
    if not overlaps_analysis_hours(
        temp["_start_time"],
        temp["_end_time"],
        panel["hour_start"].min(),
        panel["hour_start"].max(),
    ):
        warnings.append(
            f"{name}: location-text rows do not overlap the local analysis window."
        )
        return panel

    temp["_location_tokens"] = temp[available_text_cols].apply(
        lambda row: sorted(
            {
                token
                for value in row
                for token in extract_location_tokens(value)
            }
        ),
        axis=1,
    )
    temp = temp[temp["_location_tokens"].map(bool)].copy()
    if temp.empty:
        warnings.append(f"{name}: location text parsed but no street-like tokens were found.")
        return panel

    feature_count = f"{name}_local_text_active_count"
    panel[feature_count] = 0
    stop_name_norm = panel["stop_name"].fillna("").map(normalize_place_text)

    severity_col = first_existing(temp, ["Severity_Score", "severity", "Severity"])
    if severity_col is not None:
        severity_feature = f"{name}_local_text_severity_sum"
        panel[severity_feature] = 0.0
        temp["_severity"] = pd.to_numeric(temp[severity_col], errors="coerce").fillna(0.0)
    else:
        severity_feature = ""

    for _, row in temp.iterrows():
        time_mask = (panel["hour_start"] >= row["_start_time"].floor("h")) & (
            panel["hour_start"] <= row["_end_time"].ceil("h")
        )
        if not time_mask.any():
            continue
        candidate_index = panel.index[time_mask]
        matched = pd.Series(False, index=candidate_index)
        candidate_stop_names = stop_name_norm.loc[candidate_index]
        for token in row["_location_tokens"]:
            matched = matched | candidate_stop_names.str.contains(
                re.escape(token), regex=True, na=False
            )
        matched_index = candidate_index[matched.to_numpy()]
        panel.loc[matched_index, feature_count] += 1
        if severity_feature:
            panel.loc[matched_index, severity_feature] += float(row["_severity"])

    warnings.append(
        f"{name}: local text matching uses street/name overlap with GTFS stop names; "
        "treat it as approximate unless the source is geocoded."
    )
    return panel


def extract_location_tokens(value: object) -> list[str]:
    if pd.isna(value):
        return []
    text = str(value)
    pieces = re.split(r"[,;/|]|\band\b|&|\n", text, flags=re.IGNORECASE)
    tokens: list[str] = []
    for piece in pieces:
        normalized = normalize_place_text(piece)
        if len(normalized) < 4:
            continue
        if is_street_like_token(normalized):
            tokens.append(normalized)
    return tokens


def normalize_place_text(value: object) -> str:
    text = str(value).lower()
    text = text.replace("@", " at ")
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9\s-]", " ", text)
    text = re.sub(r"\bavenue\b", "ave", text)
    text = re.sub(r"\bstreet\b", "st", text)
    text = re.sub(r"\broad\b", "rd", text)
    text = re.sub(r"\bdrive\b", "dr", text)
    text = re.sub(r"\bboulevard\b", "blvd", text)
    text = re.sub(r"\bhighway\b", "hwy", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_street_like_token(token: str) -> bool:
    suffixes = (
        " ave",
        " av",
        " st",
        " street",
        " rd",
        " road",
        " way",
        " blvd",
        " dr",
        " pkwy",
        " hwy",
        " highway",
        " pl",
        " ct",
        " ln",
        " ter",
    )
    if any(token.endswith(suffix) for suffix in suffixes):
        return True
    return bool(re.search(r"\b(i|sr|us)-?\s?\d+\b", token))


def overlaps_analysis_hours(
    starts: pd.Series,
    ends: pd.Series,
    analysis_start: pd.Timestamp,
    analysis_end: pd.Timestamp,
) -> bool:
    if pd.isna(analysis_start) or pd.isna(analysis_end):
        return False
    starts = pd.to_datetime(starts, errors="coerce")
    ends = pd.to_datetime(ends, errors="coerce")
    valid = starts.notna() & ends.notna()
    if not valid.any():
        return False
    return bool(((starts[valid] <= analysis_end) & (ends[valid] >= analysis_start)).any())


def haversine_meters(lat1: np.ndarray, lon1: np.ndarray, lat2: float, lon2: float) -> np.ndarray:
    radius = 6_371_000.0
    lat1_rad = np.radians(lat1)
    lon1_rad = np.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1_rad) * math.cos(lat2_rad) * np.sin(
        dlon / 2.0
    ) ** 2
    return radius * 2.0 * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a))


def numeric_parameter_columns(df: pd.DataFrame, exclude: set[str]) -> list[str]:
    cols: list[str] = []
    for col in df.columns:
        if col in exclude or col.startswith("_"):
            continue
        converted = pd.to_numeric(df[col], errors="coerce")
        if converted.notna().sum() > 0:
            df[col] = converted
            cols.append(col)
    return cols


def first_existing(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    lower_lookup = {str(col).lower(): col for col in df.columns}
    for col in candidates:
        found = lower_lookup.get(col.lower())
        if found is not None:
            return found
    return None


def normalize_route_label(value: object) -> str:
    text = str(value).strip()
    text = re.sub(r"^route\s+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text)
    return text.upper()


def correlation_table(
    df: pd.DataFrame,
    target_col: str,
    label: str,
    min_pairs: int,
    exclude_cols: set[str],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if target_col not in df.columns:
        return pd.DataFrame()

    target = pd.to_numeric(df[target_col], errors="coerce")
    for col in df.columns:
        if col in exclude_cols or col == target_col or col.startswith("_"):
            continue
        values = pd.to_numeric(df[col], errors="coerce")
        valid = target.notna() & values.notna()
        n = int(valid.sum())
        if n < min_pairs or values[valid].nunique() < 2 or target[valid].nunique() < 2:
            continue

        pearson = pearson_corr(target[valid], values[valid])
        spearman = pearson_corr(target[valid].rank(method="average"), values[valid].rank(method="average"))
        rows.append(
            {
                "analysis_table": label,
                "target": target_col,
                "parameter": col,
                "n": n,
                "pearson_corr": pearson,
                "spearman_corr": spearman,
                "mean_target_when_parameter_positive": float(target[valid & (values > 0)].mean())
                if (valid & (values > 0)).any()
                else np.nan,
                "mean_target_when_parameter_zero": float(target[valid & (values == 0)].mean())
                if (valid & (values == 0)).any()
                else np.nan,
            }
        )

    result = pd.DataFrame(rows)
    if not result.empty:
        result["abs_spearman_corr"] = result["spearman_corr"].abs()
        result = result.sort_values(["abs_spearman_corr", "n"], ascending=[False, False])
    return result


def pearson_corr(left: pd.Series, right: pd.Series) -> float:
    left_values = pd.to_numeric(left, errors="coerce").to_numpy(dtype=float)
    right_values = pd.to_numeric(right, errors="coerce").to_numpy(dtype=float)
    valid = np.isfinite(left_values) & np.isfinite(right_values)
    if valid.sum() < 2:
        return np.nan

    left_values = left_values[valid]
    right_values = right_values[valid]
    left_std = left_values.std(ddof=0)
    right_std = right_values.std(ddof=0)
    if left_std == 0 or right_std == 0:
        return np.nan
    return float(np.mean((left_values - left_values.mean()) * (right_values - right_values.mean())) / (left_std * right_std))


def lift_table(
    df: pd.DataFrame,
    target_col: str,
    label: str,
    min_pairs: int,
    exclude_cols: set[str],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if target_col not in df.columns:
        return pd.DataFrame()

    target = pd.to_numeric(df[target_col], errors="coerce")
    for col in df.columns:
        if col in exclude_cols or col == target_col or col.startswith("_"):
            continue
        values = pd.to_numeric(df[col], errors="coerce")
        valid = target.notna() & values.notna()
        if int(valid.sum()) < min_pairs or values[valid].nunique() < 2:
            continue

        positive = valid & (values > 0)
        zero = valid & (values == 0)
        if positive.sum() >= 2 and zero.sum() >= 2:
            rows.append(
                {
                    "analysis_table": label,
                    "target": target_col,
                    "parameter": col,
                    "comparison": "positive_vs_zero",
                    "n_high": int(positive.sum()),
                    "n_low": int(zero.sum()),
                    "mean_target_high": float(target[positive].mean()),
                    "mean_target_low": float(target[zero].mean()),
                    "difference": float(target[positive].mean() - target[zero].mean()),
                    "ratio": safe_ratio(target[positive].mean(), target[zero].mean()),
                }
            )
            continue

        q75 = values[valid].quantile(0.75)
        q25 = values[valid].quantile(0.25)
        high = valid & (values >= q75)
        low = valid & (values <= q25)
        if high.sum() >= 2 and low.sum() >= 2:
            rows.append(
                {
                    "analysis_table": label,
                    "target": target_col,
                    "parameter": col,
                    "comparison": "top_quartile_vs_bottom_quartile",
                    "n_high": int(high.sum()),
                    "n_low": int(low.sum()),
                    "mean_target_high": float(target[high].mean()),
                    "mean_target_low": float(target[low].mean()),
                    "difference": float(target[high].mean() - target[low].mean()),
                    "ratio": safe_ratio(target[high].mean(), target[low].mean()),
                }
            )

    result = pd.DataFrame(rows)
    if not result.empty:
        result["abs_difference"] = result["difference"].abs()
        result = result.sort_values(["abs_difference", "n_high"], ascending=[False, False])
    return result


def safe_ratio(numerator: float, denominator: float) -> float:
    if pd.isna(denominator) or denominator == 0:
        return np.nan
    return float(numerator / denominator)


def write_report(
    output_dir: Path,
    raw_events: pd.DataFrame,
    count_units: pd.DataFrame,
    event_unit: str,
    hourly: pd.DataFrame,
    local_panel: pd.DataFrame,
    parameter_files: list[ParameterFile],
    run_messages: list[str],
    warnings: list[str],
    correlations: pd.DataFrame,
    lifts: pd.DataFrame,
) -> None:
    lines: list[str] = []
    lines.append("Bus bunching parameter correlation report")
    lines.append("=" * 48)
    lines.append("")
    lines.append(f"Raw event rows loaded: {len(raw_events):,}")
    lines.append(f"Counting unit: {event_unit}")
    lines.append(f"Counted bunching units: {len(count_units):,}")
    if len(raw_events):
        collapse_ratio = len(raw_events) / max(len(count_units), 1)
        lines.append(f"Raw rows per counted unit: {collapse_ratio:.2f}")
    lines.append(
        f"Counted unit date range: {count_units['hour_start'].min()} to {count_units['hour_start'].max()}"
    )
    lines.append(f"Hourly analysis rows: {len(hourly):,}")
    lines.append(f"Local GTFS panel rows: {len(local_panel):,}" if not local_panel.empty else "Local GTFS panel rows: 0")
    lines.append("")
    lines.append("Parameter CSVs found:")
    if parameter_files:
        for item in parameter_files:
            lines.append(f"- {item.name}: {item.path}")
    else:
        lines.append("- none")
    lines.append("")

    if run_messages:
        lines.append("Parameter script run log:")
        lines.extend(run_messages)
        lines.append("")

    lines.append("Top correlations by absolute Spearman correlation:")
    if correlations.empty:
        lines.append("- none with enough usable observations")
    else:
        for _, row in correlations.head(12).iterrows():
            lines.append(
                "- {table} {target} vs {parameter}: spearman={spearman:.3f}, "
                "pearson={pearson:.3f}, n={n}".format(
                    table=row["analysis_table"],
                    target=row["target"],
                    parameter=row["parameter"],
                    spearman=row["spearman_corr"],
                    pearson=row["pearson_corr"],
                    n=int(row["n"]),
                )
            )
    lines.append("")

    lines.append("Largest mean target differences:")
    if lifts.empty:
        lines.append("- none with enough usable observations")
    else:
        for _, row in lifts.head(12).iterrows():
            lines.append(
                "- {table} {target} by {parameter} ({comparison}): "
                "high={high:.3f}, low={low:.3f}, diff={diff:.3f}, ratio={ratio}".format(
                    table=row["analysis_table"],
                    target=row["target"],
                    parameter=row["parameter"],
                    comparison=row["comparison"],
                    high=row["mean_target_high"],
                    low=row["mean_target_low"],
                    diff=row["difference"],
                    ratio="NA" if pd.isna(row["ratio"]) else f"{row['ratio']:.3f}",
                )
            )
    lines.append("")

    lines.append("Warnings and limitations:")
    if warnings:
        lines.extend(f"- {warning}" for warning in warnings)
    else:
        lines.append("- none")

    (output_dir / "parameter_correlation_report.txt").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def main() -> int:
    args = parse_args()
    if should_use_spark(args.engine):
        from spark_parameter_bunching_correlations import run_spark_analysis

        return run_spark_analysis(args)

    events_csv = Path(args.events_csv)
    parameter_scripts_dir = Path(args.parameter_scripts_dir)
    input_data_dir = Path(args.input_data_dir)
    gtfs_dir = Path(args.gtfs_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    warnings: list[str] = []
    run_messages: list[str] = []

    if not args.skip_run_parameters:
        run_messages = run_parameter_scripts(parameter_scripts_dir, input_data_dir)

    raw_events = load_events(events_csv)
    count_units = build_count_units(
        raw_events,
        args.event_unit,
        args.episode_gap_minutes,
        warnings,
    )
    count_units.to_csv(output_dir / "bus_bunching_count_units.csv", index=False)

    hourly = build_hourly_base(count_units)
    local_panel = load_gtfs_panel(gtfs_dir, count_units, warnings)

    parameter_files = discover_parameter_files(parameter_scripts_dir, input_data_dir)
    for parameter_file in parameter_files:
        df = read_parameter_csv(parameter_file, warnings)
        if df.empty:
            continue

        if parameter_file.source_type == "weather":
            hourly = add_weather_features(hourly, df, parameter_file.name, warnings)
        elif parameter_file.source_type == "incident":
            hourly = add_count_window_features(
                hourly,
                df,
                parameter_file.name,
                ["Date/Time", "Datetime", "timestamp", "StartTime"],
                ["EndTime"],
                args.incident_window_hours,
                warnings,
            )
            local_panel = add_local_spatial_features(
                local_panel,
                df,
                parameter_file.name,
                ["Date/Time", "Datetime", "timestamp", "StartTime"],
                ["EndTime"],
                args.incident_window_hours,
                args.spatial_radius_meters,
                warnings,
            )
            local_panel = add_local_text_features(
                local_panel,
                df,
                parameter_file.name,
                ["Date/Time", "Datetime", "timestamp", "StartTime"],
                ["EndTime"],
                ["Location", "Address", "Type"],
                args.incident_window_hours,
                warnings,
            )
        elif parameter_file.source_type == "alert":
            end_col = first_existing(df, ["EndTime", "End Time"])
            status_col = first_existing(df, ["EventStatus", "Status"])
            if end_col is not None and status_col is not None:
                missing_end = df[end_col].isna() | df[end_col].astype(str).str.strip().eq("")
                open_no_end_count = int(
                    (
                        df[status_col].astype(str).str.contains("open|active", case=False, na=False)
                        & missing_end
                    ).sum()
                )
                if open_no_end_count:
                    warnings.append(
                        f"{parameter_file.name}: treated {open_no_end_count} open alerts with no end time "
                        "as active through the analysis period."
                    )

            hourly = add_count_window_features(
                hourly,
                df,
                parameter_file.name,
                ["StartTime", "Start Time", "timestamp", "Date/Time"],
                ["EndTime", "End Time"],
                args.incident_window_hours,
                warnings,
                open_status_cols=["EventStatus", "Status"],
                analysis_end=hourly["hour_start"].max(),
            )
            local_panel = add_local_spatial_features(
                local_panel,
                df,
                parameter_file.name,
                ["StartTime", "Start Time", "timestamp", "Date/Time"],
                ["EndTime", "End Time"],
                args.incident_window_hours,
                args.spatial_radius_meters,
                warnings,
                open_status_cols=["EventStatus", "Status"],
                analysis_end=local_panel["hour_start"].max() if not local_panel.empty else None,
            )
            local_panel = add_local_text_features(
                local_panel,
                df,
                parameter_file.name,
                ["StartTime", "Start Time", "timestamp", "Date/Time"],
                ["EndTime", "End Time"],
                ["Location", "HeadlineDescription", "RoadName", "County", "Region"],
                args.incident_window_hours,
                warnings,
                open_status_cols=["EventStatus", "Status"],
                analysis_end=local_panel["hour_start"].max() if not local_panel.empty else None,
            )
        elif parameter_file.source_type == "special_event":
            hourly = add_count_window_features(
                hourly,
                df,
                parameter_file.name,
                ["Date_Time", "Date/Time", "Datetime", "timestamp", "StartTime"],
                ["EndTime", "End_Time"],
                args.event_window_hours,
                warnings,
            )
            local_panel = add_local_spatial_features(
                local_panel,
                df,
                parameter_file.name,
                ["Date_Time", "Date/Time", "Datetime", "timestamp", "StartTime"],
                ["EndTime", "End_Time"],
                args.event_window_hours,
                args.spatial_radius_meters,
                warnings,
            )
            local_panel = add_local_text_features(
                local_panel,
                df,
                parameter_file.name,
                ["Date_Time", "Date/Time", "Datetime", "timestamp", "StartTime"],
                ["EndTime", "End_Time"],
                ["Location", "Intersecting_Streets", "Event_Name"],
                args.event_window_hours,
                warnings,
            )
        elif parameter_file.source_type == "route_alert_snapshot":
            add_route_snapshot_features(count_units, df, parameter_file.name, output_dir, warnings)

    hourly.to_csv(output_dir / "hourly_parameter_dataset.csv", index=False)
    if not local_panel.empty:
        local_panel.to_csv(output_dir / "local_parameter_dataset.csv", index=False)

    hourly_exclude = {
        "hour_start",
        "median_min_headway_minutes",
        "bunched_routes",
        "bunched_stops",
    }
    local_exclude = {
        "service_date",
        "hour",
        "hour_start",
        "route_id",
        "direction_id",
        "stop_id",
        "stop_name",
        "stop_lat",
        "stop_lon",
        "stop_lat_num",
        "stop_lon_num",
        "scheduled_stop_arrivals",
        "bunching_events",
    }

    correlation_frames = [
        correlation_table(
            hourly,
            "bunching_events",
            "hourly",
            args.min_pairs,
            hourly_exclude,
        )
    ]
    lift_frames = [
        lift_table(hourly, "bunching_events", "hourly", args.min_pairs, hourly_exclude)
    ]
    if not local_panel.empty:
        correlation_frames.append(
            correlation_table(
                local_panel,
                "bunching_rate_per_100_scheduled",
                "local_route_stop_hour",
                args.min_pairs,
                local_exclude,
            )
        )
        lift_frames.append(
            lift_table(
                local_panel,
                "bunching_rate_per_100_scheduled",
                "local_route_stop_hour",
                args.min_pairs,
                local_exclude,
            )
        )

    correlations = pd.concat(
        [frame for frame in correlation_frames if not frame.empty], ignore_index=True
    ) if any(not frame.empty for frame in correlation_frames) else pd.DataFrame()
    lifts = pd.concat(
        [frame for frame in lift_frames if not frame.empty], ignore_index=True
    ) if any(not frame.empty for frame in lift_frames) else pd.DataFrame()

    if not correlations.empty:
        correlations.to_csv(output_dir / "correlation_summary.csv", index=False)
    else:
        (output_dir / "correlation_summary.csv").write_text("", encoding="utf-8")
    if not lifts.empty:
        lifts.to_csv(output_dir / "parameter_lift_summary.csv", index=False)
    else:
        (output_dir / "parameter_lift_summary.csv").write_text("", encoding="utf-8")

    write_report(
        output_dir,
        raw_events,
        count_units,
        args.event_unit,
        hourly,
        local_panel,
        parameter_files,
        run_messages,
        warnings,
        correlations,
        lifts,
    )

    print(f"Wrote analysis outputs to {output_dir}")
    print(f"Report: {output_dir / 'parameter_correlation_report.txt'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
