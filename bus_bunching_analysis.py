#!/usr/bin/env python3
"""
Analyze GTFS-RT SQLite captures for bus bunching.

Definition used here:
  Two adjacent buses are bunched when they are on the same route, same
  direction, and same stop snapshot, and their predicted arrivals at that stop
  are closer than a GTFS-scheduled-headway-based threshold. Route terminals
  are excluded by default because layovers and bus staging naturally cluster
  vehicles there.

Default sliding threshold:
  bunch_threshold = 25% of scheduled headway, with a 2 minute floor and
  10 minute cap. Examples: 10 minute scheduled headway -> 2.5 minute threshold;
  30 minute scheduled headway -> 7.5 minute threshold.
"""

from __future__ import annotations

import argparse
import glob
import math
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python 3.9+ has zoneinfo.
    ZoneInfo = None


@dataclass(frozen=True)
class GtfsData:
    timezone_name: str
    trip_rows_for_sql: list[tuple[str, str, str, str]]
    routes: pd.DataFrame
    stops: pd.DataFrame
    trip_stop_schedule: pd.DataFrame
    trip_terminal_stops: pd.DataFrame
    fallback_headways: pd.DataFrame


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Detect bus bunching from KCM GTFS-RT SQLite .db files."
    )
    parser.add_argument(
        "--db-glob",
        default="*.db",
        help="Glob for SQLite capture files. Default: %(default)s",
    )
    parser.add_argument(
        "--gtfs-dir",
        default="/Workspace/Shared/IP3/fta/google_transit",
        help="Static GTFS folder. Default: %(default)s",
    )
    parser.add_argument(
        "--output",
        default="bus_bunching_report.txt",
        help="Text report path. Default: %(default)s",
    )
    parser.add_argument(
        "--events-csv",
        default="bus_bunching_events.csv",
        help="Machine-readable event table path for graphics/modeling. Default: %(default)s",
    )
    parser.add_argument(
        "--threshold-ratio",
        type=float,
        default=0.25,
        help="Bunching threshold as a fraction of scheduled headway. Default: %(default)s",
    )
    parser.add_argument(
        "--min-bunch-minutes",
        type=float,
        default=2.0,
        help="Minimum bunching threshold in minutes. Default: %(default)s",
    )
    parser.add_argument(
        "--max-bunch-minutes",
        type=float,
        default=10.0,
        help="Maximum bunching threshold in minutes. Default: %(default)s",
    )
    parser.add_argument(
        "--fallback-headway-minutes",
        type=float,
        default=30.0,
        help="Headway used only when GTFS pair and route-stop fallback are missing. Default: %(default)s",
    )
    parser.add_argument(
        "--max-scheduled-headway-minutes",
        type=float,
        default=240.0,
        help="Ignore scheduled gaps larger than this many minutes. Default: %(default)s",
    )
    parser.add_argument(
        "--include-terminals",
        action="store_true",
        help="Include first/last GTFS stops for each trip. Default is to exclude terminals.",
    )
    return parser.parse_args([])


def require_columns(df: pd.DataFrame, file_name: str, columns: Iterable[str]) -> None:
    missing = [col for col in columns if col not in df.columns]
    if missing:
        raise ValueError(f"{file_name} is missing required columns: {', '.join(missing)}")


def read_gtfs_time(value: object) -> float:
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


def load_gtfs(gtfs_dir: Path, max_scheduled_headway_seconds: float) -> GtfsData:
    agency_path = gtfs_dir / "agency.txt"
    routes_path = gtfs_dir / "routes.txt"
    stops_path = gtfs_dir / "stops.txt"
    trips_path = gtfs_dir / "trips.txt"
    stop_times_path = gtfs_dir / "stop_times.txt"

    for path in [agency_path, routes_path, stops_path, trips_path, stop_times_path]:
        if not path.exists():
            raise FileNotFoundError(f"Required GTFS file not found: {path}")

    agency = pd.read_csv(agency_path, dtype=str)
    require_columns(agency, "agency.txt", ["agency_timezone"])
    timezone_name = agency["agency_timezone"].dropna().iloc[0]

    routes = pd.read_csv(
        routes_path,
        dtype=str,
        usecols=["route_id", "route_short_name", "route_long_name", "route_desc"],
    ).fillna("")
    stops = pd.read_csv(
        stops_path,
        dtype=str,
        usecols=["stop_id", "stop_name", "stop_lat", "stop_lon"],
    ).fillna("")
    trips = pd.read_csv(
        trips_path,
        dtype=str,
        usecols=["route_id", "service_id", "trip_id", "trip_headsign", "direction_id"],
    ).fillna("")
    stop_times = pd.read_csv(
        stop_times_path,
        dtype=str,
        usecols=["trip_id", "arrival_time", "stop_id", "stop_sequence"],
    )

    require_columns(routes, "routes.txt", ["route_id", "route_short_name"])
    require_columns(stops, "stops.txt", ["stop_id", "stop_name"])
    require_columns(trips, "trips.txt", ["route_id", "service_id", "trip_id", "direction_id"])
    require_columns(
        stop_times,
        "stop_times.txt",
        ["trip_id", "arrival_time", "stop_id", "stop_sequence"],
    )

    stop_times["scheduled_arrival_seconds"] = stop_times["arrival_time"].map(read_gtfs_time)
    stop_times = stop_times.dropna(subset=["scheduled_arrival_seconds"])
    stop_times["scheduled_arrival_seconds"] = stop_times["scheduled_arrival_seconds"].astype("int64")
    stop_times["stop_sequence_num"] = pd.to_numeric(stop_times["stop_sequence"], errors="coerce")

    trip_stop_schedule = stop_times[
        ["trip_id", "stop_id", "scheduled_arrival_seconds"]
    ].drop_duplicates(["trip_id", "stop_id"])

    ordered_stop_times = stop_times.dropna(subset=["stop_sequence_num"]).copy()
    ordered_stop_times = ordered_stop_times.sort_values(["trip_id", "stop_sequence_num"])
    first_stops = ordered_stop_times.groupby("trip_id", as_index=False).first()[
        ["trip_id", "stop_id"]
    ]
    last_stops = ordered_stop_times.groupby("trip_id", as_index=False).last()[
        ["trip_id", "stop_id"]
    ]
    trip_terminal_stops = (
        pd.concat([first_stops, last_stops], ignore_index=True)
        .drop_duplicates(["trip_id", "stop_id"])
        .assign(is_terminal_stop=True)
    )

    schedule = stop_times[["trip_id", "stop_id", "scheduled_arrival_seconds"]].merge(
        trips[["trip_id", "route_id", "direction_id", "service_id"]],
        on="trip_id",
        how="inner",
    )
    schedule = schedule.sort_values(
        ["service_id", "route_id", "direction_id", "stop_id", "scheduled_arrival_seconds"]
    )
    group_cols = ["service_id", "route_id", "direction_id", "stop_id"]
    schedule["scheduled_gap_seconds"] = schedule.groupby(group_cols)[
        "scheduled_arrival_seconds"
    ].diff()
    valid_gaps = schedule[
        (schedule["scheduled_gap_seconds"] > 0)
        & (schedule["scheduled_gap_seconds"] <= max_scheduled_headway_seconds)
    ].copy()
    fallback_headways = (
        valid_gaps.groupby(["route_id", "direction_id", "stop_id"], as_index=False)[
            "scheduled_gap_seconds"
        ]
        .median()
        .rename(columns={"scheduled_gap_seconds": "fallback_headway_seconds"})
    )

    trip_rows_for_sql = list(
        trips[["trip_id", "route_id", "direction_id", "trip_headsign"]]
        .drop_duplicates("trip_id")
        .itertuples(index=False, name=None)
    )

    return GtfsData(
        timezone_name=timezone_name,
        trip_rows_for_sql=trip_rows_for_sql,
        routes=routes,
        stops=stops,
        trip_stop_schedule=trip_stop_schedule,
        trip_terminal_stops=trip_terminal_stops,
        fallback_headways=fallback_headways,
    )


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def install_gtfs_trip_temp_table(
    conn: sqlite3.Connection, trip_rows: list[tuple[str, str, str, str]]
) -> None:
    conn.execute(
        """
        CREATE TEMP TABLE gtfs_trips (
            trip_id TEXT PRIMARY KEY,
            static_route_id TEXT,
            direction_id TEXT,
            trip_headsign TEXT
        )
        """
    )
    conn.executemany(
        """
        INSERT OR REPLACE INTO gtfs_trips
            (trip_id, static_route_id, direction_id, trip_headsign)
        VALUES (?, ?, ?, ?)
        """,
        trip_rows,
    )


def read_candidate_pairs(
    db_path: Path, gtfs: GtfsData, max_bunch_seconds: float
) -> tuple[pd.DataFrame, str]:
    conn = sqlite3.connect(db_path)
    try:
        if not table_exists(conn, "stop_time_updates"):
            return pd.DataFrame(), "skipped: no stop_time_updates table"
        install_gtfs_trip_temp_table(conn, gtfs.trip_rows_for_sql)
        query = """
            WITH paired AS (
                SELECT
                    s.trip_id AS trailing_trip_id,
                    s.route_id AS route_id,
                    s.stop_id AS stop_id,
                    s.stop_sequence AS stop_sequence,
                    s.arrival_time AS trailing_arrival_time,
                    s.feed_timestamp AS feed_timestamp,
                    s.logged_at AS logged_at,
                    COALESCE(g.direction_id, 'unknown') AS direction_id,
                    COALESCE(g.trip_headsign, '') AS trailing_headsign,
                    LAG(s.trip_id) OVER (
                        PARTITION BY s.logged_at, s.route_id, s.stop_id, COALESCE(g.direction_id, 'unknown')
                        ORDER BY s.arrival_time, s.trip_id
                    ) AS lead_trip_id,
                    LAG(s.arrival_time) OVER (
                        PARTITION BY s.logged_at, s.route_id, s.stop_id, COALESCE(g.direction_id, 'unknown')
                        ORDER BY s.arrival_time, s.trip_id
                    ) AS lead_arrival_time,
                    LAG(COALESCE(g.trip_headsign, '')) OVER (
                        PARTITION BY s.logged_at, s.route_id, s.stop_id, COALESCE(g.direction_id, 'unknown')
                        ORDER BY s.arrival_time, s.trip_id
                    ) AS lead_headsign
                FROM stop_time_updates s
                LEFT JOIN gtfs_trips g ON s.trip_id = g.trip_id
                WHERE
                    s.arrival_time IS NOT NULL
                    AND s.trip_id IS NOT NULL
                    AND s.route_id IS NOT NULL
                    AND s.stop_id IS NOT NULL
            )
            SELECT
                trailing_trip_id,
                lead_trip_id,
                route_id,
                direction_id,
                stop_id,
                stop_sequence,
                lead_arrival_time,
                trailing_arrival_time,
                trailing_arrival_time - lead_arrival_time AS actual_headway_seconds,
                feed_timestamp,
                logged_at,
                lead_headsign,
                trailing_headsign
            FROM paired
            WHERE
                lead_trip_id IS NOT NULL
                AND trailing_trip_id <> lead_trip_id
                AND trailing_arrival_time >= lead_arrival_time
                AND trailing_arrival_time - lead_arrival_time <= ?
        """
        df = pd.read_sql_query(query, conn, params=(int(max_bunch_seconds),))
        if df.empty:
            return df, "ok: no candidate pairs"
        df["source_db"] = db_path.name
        return df, f"ok: {len(df):,} candidate pairs"
    finally:
        conn.close()


def apply_gtfs_thresholds(
    candidates: pd.DataFrame,
    gtfs: GtfsData,
    threshold_ratio: float,
    min_bunch_seconds: float,
    max_bunch_seconds: float,
    fallback_headway_seconds: float,
    max_scheduled_headway_seconds: float,
    include_terminals: bool,
) -> pd.DataFrame:
    if candidates.empty:
        return candidates

    df = candidates
    if not include_terminals:
        terminal_stops = gtfs.trip_terminal_stops
        lead_terminals = terminal_stops.rename(
            columns={"trip_id": "lead_trip_id", "is_terminal_stop": "lead_is_terminal_stop"}
        )
        trailing_terminals = terminal_stops.rename(
            columns={
                "trip_id": "trailing_trip_id",
                "is_terminal_stop": "trailing_is_terminal_stop",
            }
        )
        df = df.merge(lead_terminals, on=["lead_trip_id", "stop_id"], how="left")
        df = df.merge(trailing_terminals, on=["trailing_trip_id", "stop_id"], how="left")
        terminal_mask = (
            df["lead_is_terminal_stop"].fillna(False)
            | df["trailing_is_terminal_stop"].fillna(False)
        )
        df = df[~terminal_mask].drop(
            columns=["lead_is_terminal_stop", "trailing_is_terminal_stop"]
        )
        if df.empty:
            return df

    schedule = gtfs.trip_stop_schedule
    lead_schedule = schedule.rename(
        columns={
            "trip_id": "lead_trip_id",
            "scheduled_arrival_seconds": "lead_scheduled_arrival_seconds",
        }
    )
    trailing_schedule = schedule.rename(
        columns={
            "trip_id": "trailing_trip_id",
            "scheduled_arrival_seconds": "trailing_scheduled_arrival_seconds",
        }
    )

    df = df.merge(lead_schedule, on=["lead_trip_id", "stop_id"], how="left")
    df = df.merge(trailing_schedule, on=["trailing_trip_id", "stop_id"], how="left")
    df["scheduled_headway_seconds"] = (
        df["trailing_scheduled_arrival_seconds"] - df["lead_scheduled_arrival_seconds"]
    ).abs()

    invalid_pair_headway = (
        df["scheduled_headway_seconds"].isna()
        | (df["scheduled_headway_seconds"] <= 0)
        | (df["scheduled_headway_seconds"] > max_scheduled_headway_seconds)
    )
    df.loc[invalid_pair_headway, "scheduled_headway_seconds"] = pd.NA
    df["headway_source"] = "trip_pair"
    df.loc[invalid_pair_headway, "headway_source"] = "route_stop_median"

    df = df.merge(gtfs.fallback_headways, on=["route_id", "direction_id", "stop_id"], how="left")
    df["scheduled_headway_seconds"] = df["scheduled_headway_seconds"].fillna(
        df["fallback_headway_seconds"]
    )
    missing_after_fallback = df["scheduled_headway_seconds"].isna()
    df.loc[missing_after_fallback, "headway_source"] = "default"
    df["scheduled_headway_seconds"] = df["scheduled_headway_seconds"].fillna(
        fallback_headway_seconds
    )

    df["bunch_threshold_seconds"] = (
        df["scheduled_headway_seconds"] * threshold_ratio
    ).clip(lower=min_bunch_seconds, upper=max_bunch_seconds)
    bunched = df[df["actual_headway_seconds"] <= df["bunch_threshold_seconds"]].copy()
    return bunched


def add_labels_and_times(events: pd.DataFrame, gtfs: GtfsData) -> pd.DataFrame:
    if events.empty:
        return events

    route_labels = gtfs.routes.copy()
    route_labels["route_label"] = route_labels.apply(
        lambda row: (
            row["route_short_name"]
            if row["route_short_name"]
            else row["route_id"]
        ),
        axis=1,
    )
    route_labels["route_name"] = route_labels.apply(
        lambda row: (
            f"{row['route_label']} - {row['route_desc']}"
            if row["route_desc"]
            else (
                f"{row['route_label']} - {row['route_long_name']}"
                if row["route_long_name"]
                else row["route_label"]
            )
        ),
        axis=1,
    )

    df = events.merge(
        route_labels[["route_id", "route_label", "route_name"]],
        on="route_id",
        how="left",
    )
    df = df.merge(gtfs.stops, on="stop_id", how="left")
    df["route_label"] = df["route_label"].fillna(df["route_id"])
    df["route_name"] = df["route_name"].fillna(df["route_id"])
    df["stop_name"] = df["stop_name"].fillna("")
    df["stop_lat"] = df["stop_lat"].fillna("")
    df["stop_lon"] = df["stop_lon"].fillna("")

    timezone_name = gtfs.timezone_name
    arrival_dt = pd.to_datetime(df["trailing_arrival_time"], unit="s", utc=True).dt.tz_convert(
        timezone_name
    )
    lead_arrival_dt = pd.to_datetime(df["lead_arrival_time"], unit="s", utc=True).dt.tz_convert(
        timezone_name
    )
    snapshot_dt = pd.to_datetime(df["feed_timestamp"], unit="s", utc=True).dt.tz_convert(
        timezone_name
    )
    df["event_date"] = arrival_dt.dt.strftime("%Y-%m-%d")
    df["event_hour"] = arrival_dt.dt.strftime("%H:00")
    df["lead_arrival_local"] = lead_arrival_dt.dt.strftime("%Y-%m-%d %H:%M:%S %Z")
    df["trailing_arrival_local"] = arrival_dt.dt.strftime("%Y-%m-%d %H:%M:%S %Z")
    df["snapshot_local"] = snapshot_dt.dt.strftime("%Y-%m-%d %H:%M:%S %Z")
    return df


def collapse_repeated_snapshots(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return events

    dedupe_cols = [
        "route_id",
        "direction_id",
        "stop_id",
        "lead_trip_id",
        "trailing_trip_id",
        "logged_at",
        "feed_timestamp",
        "lead_arrival_time",
        "trailing_arrival_time",
    ]
    events = events.drop_duplicates(dedupe_cols)

    group_cols = [
        "event_date",
        "route_id",
        "route_label",
        "route_name",
        "direction_id",
        "stop_id",
        "stop_name",
        "stop_lat",
        "stop_lon",
        "lead_trip_id",
        "trailing_trip_id",
    ]
    grouped = (
        events.groupby(group_cols, dropna=False)
        .agg(
            first_seen=("snapshot_local", "min"),
            last_seen=("snapshot_local", "max"),
            first_predicted_arrival=("trailing_arrival_local", "min"),
            last_predicted_arrival=("trailing_arrival_local", "max"),
            event_hour=("event_hour", "first"),
            min_actual_headway_seconds=("actual_headway_seconds", "min"),
            median_actual_headway_seconds=("actual_headway_seconds", "median"),
            scheduled_headway_seconds=("scheduled_headway_seconds", "median"),
            bunch_threshold_seconds=("bunch_threshold_seconds", "median"),
            snapshots_flagged=("logged_at", "nunique"),
            source_files=("source_db", lambda values: ", ".join(sorted(set(values)))),
            headway_source=("headway_source", lambda values: ", ".join(sorted(set(values)))),
            lead_headsign=("lead_headsign", "first"),
            trailing_headsign=("trailing_headsign", "first"),
        )
        .reset_index()
    )
    grouped = grouped.sort_values(
        ["event_date", "route_label", "direction_id", "first_predicted_arrival", "stop_id"]
    )
    return grouped


def minutes(seconds: float) -> float:
    return float(seconds) / 60.0


def format_minutes(seconds: float) -> str:
    return f"{minutes(seconds):.1f} min"


def write_section_table(handle, title: str, df: pd.DataFrame, columns: list[str]) -> None:
    handle.write(f"\n{title}\n")
    handle.write("-" * len(title) + "\n")
    if df.empty:
        handle.write("No rows.\n")
        return
    printable = df[columns].copy()
    handle.write(printable.to_string(index=False))
    handle.write("\n")


def format_route_list(df: pd.DataFrame, label_col: str, value_cols: list[str]) -> str:
    if df.empty:
        return "none"
    parts = []
    for _, row in df.iterrows():
        label = str(row[label_col])
        details = ", ".join(f"{col}={row[col]}" for col in value_cols)
        parts.append(f"{label} ({details})")
    return "; ".join(parts)


def write_per_route_breakdown(handle, events: pd.DataFrame) -> None:
    handle.write("\nPer-Route Where/When Bunching Happens Most\n")
    handle.write("------------------------------------------\n")
    if events.empty:
        handle.write("No rows.\n")
        return

    route_totals = (
        events.groupby(["route_label", "route_name"], as_index=False)
        .agg(events=("route_id", "size"), stops=("stop_id", "nunique"))
        .sort_values(["events", "route_label"], ascending=[False, True])
    )

    for _, route_row in route_totals.iterrows():
        route_events = events[events["route_label"] == route_row["route_label"]]
        handle.write(
            f"\nRoute {route_row['route_label']} - "
            f"{int(route_row['events'])} events across {int(route_row['stops'])} stops\n"
        )

        top_stops = (
            route_events.groupby(["stop_id", "stop_name"], as_index=False)
            .agg(events=("route_id", "size"))
            .sort_values(["events", "stop_id"], ascending=[False, True])
            .head(5)
        )
        top_stops["place"] = top_stops.apply(
            lambda row: f"{row['stop_name']} [{row['stop_id']}]", axis=1
        )
        handle.write(
            "  Where: "
            + format_route_list(top_stops, "place", ["events"])
            + "\n"
        )

        top_hours = (
            route_events.groupby(["event_hour"], as_index=False)
            .agg(events=("route_id", "size"))
            .sort_values(["events", "event_hour"], ascending=[False, True])
            .head(5)
        )
        handle.write(
            "  When:  "
            + format_route_list(top_hours, "event_hour", ["events"])
            + "\n"
        )

        top_directions = (
            route_events.groupby(["direction_id"], as_index=False)
            .agg(events=("route_id", "size"))
            .sort_values(["events", "direction_id"], ascending=[False, True])
        )
        handle.write(
            "  Direction split: "
            + format_route_list(top_directions, "direction_id", ["events"])
            + "\n"
        )


def write_events_csv(events: pd.DataFrame, output_path: Path) -> None:
    events_for_csv = events.copy()
    if not events_for_csv.empty:
        events_for_csv["min_actual_headway_minutes"] = (
            events_for_csv["min_actual_headway_seconds"] / 60.0
        )
        events_for_csv["scheduled_headway_minutes"] = (
            events_for_csv["scheduled_headway_seconds"] / 60.0
        )
        events_for_csv["bunch_threshold_minutes"] = (
            events_for_csv["bunch_threshold_seconds"] / 60.0
        )
    events_for_csv.to_csv(output_path, index=False)


def build_report(
    output_path: Path,
    db_statuses: list[tuple[str, str]],
    events: pd.DataFrame,
    gtfs: GtfsData,
    args: argparse.Namespace,
) -> None:
    generated_at = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    example_10 = max(args.min_bunch_minutes, min(args.max_bunch_minutes, 10 * args.threshold_ratio))
    example_30 = max(args.min_bunch_minutes, min(args.max_bunch_minutes, 30 * args.threshold_ratio))

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        handle.write("Bus Bunching Analysis Report\n")
        handle.write("============================\n")
        handle.write(f"Generated: {generated_at}\n")
        handle.write(f"GTFS timezone: {gtfs.timezone_name}\n")
        handle.write(f"GTFS folder: {Path(args.gtfs_dir).resolve()}\n")
        handle.write(f"DB glob: {args.db_glob}\n")
        handle.write("\nDefinition\n")
        handle.write("----------\n")
        handle.write(
            "A bunching event is an adjacent bus pair on the same route, same direction, "
            "and same stop snapshot where the predicted arrival gap is less than or "
            "equal to the dynamic threshold.\n"
        )
        if args.include_terminals:
            handle.write("Terminal stops: included because --include-terminals was used.\n")
        else:
            handle.write(
                "Terminal stops: excluded using each GTFS trip's first and last stop.\n"
            )
        handle.write(
            "Dynamic threshold = scheduled headway * "
            f"{args.threshold_ratio:.2f}, bounded to "
            f"{args.min_bunch_minutes:.1f}-{args.max_bunch_minutes:.1f} minutes.\n"
        )
        handle.write(
            f"Examples: 10 minute scheduled headway -> {example_10:.1f} minute threshold; "
            f"30 minute scheduled headway -> {example_30:.1f} minute threshold.\n"
        )
        handle.write(
            "Scheduled headway source priority: exact GTFS trip-pair stop times, then "
            "route/direction/stop median GTFS headway, then the fallback headway.\n"
        )

        handle.write("\nInput Files\n")
        handle.write("-----------\n")
        for db_name, status in db_statuses:
            handle.write(f"{db_name}: {status}\n")

        handle.write("\nSummary\n")
        handle.write("-------\n")
        handle.write(f"Unique bus bunching events: {len(events):,}\n")
        if events.empty:
            handle.write("No bunching events matched the configured threshold.\n")
            return

        handle.write(
            f"Date range by predicted arrivals: {events['event_date'].min()} to {events['event_date'].max()}\n"
        )
        handle.write(
            f"Routes affected: {events['route_label'].nunique():,}; "
            f"stops/intersections affected: {events['stop_id'].nunique():,}\n"
        )

        route_hotspots = (
            events.groupby(["route_label", "route_name", "direction_id"], as_index=False)
            .agg(events=("stop_id", "size"), stops=("stop_id", "nunique"))
            .sort_values(["events", "stops"], ascending=False)
            .head(20)
        )
        write_section_table(
            handle,
            "Top Routes and Directions",
            route_hotspots,
            ["route_label", "direction_id", "events", "stops", "route_name"],
        )

        stop_hotspots = (
            events.groupby(["stop_id", "stop_name", "stop_lat", "stop_lon"], as_index=False)
            .agg(events=("route_id", "size"), routes=("route_label", "nunique"))
            .sort_values(["events", "routes"], ascending=False)
            .head(25)
        )
        write_section_table(
            handle,
            "Top Stops and Intersections",
            stop_hotspots,
            ["stop_id", "events", "routes", "stop_name", "stop_lat", "stop_lon"],
        )

        route_stop_hotspots = (
            events.groupby(
                ["route_label", "direction_id", "stop_id", "stop_name"], as_index=False
            )
            .agg(events=("route_id", "size"))
            .sort_values("events", ascending=False)
            .head(30)
        )
        write_section_table(
            handle,
            "Top Route/Direction/Stop Hotspots",
            route_stop_hotspots,
            ["route_label", "direction_id", "stop_id", "events", "stop_name"],
        )

        time_hotspots = (
            events.groupby(["event_date", "event_hour"], as_index=False)
            .agg(events=("route_id", "size"), routes=("route_label", "nunique"))
            .sort_values("events", ascending=False)
            .head(30)
        )
        write_section_table(
            handle,
            "Top Dates and Hours",
            time_hotspots,
            ["event_date", "event_hour", "events", "routes"],
        )

        write_per_route_breakdown(handle, events)

        all_events = events.copy()
        all_events["min_actual_headway"] = all_events["min_actual_headway_seconds"].map(
            format_minutes
        )
        all_events["scheduled_headway"] = all_events["scheduled_headway_seconds"].map(
            format_minutes
        )
        all_events["threshold"] = all_events["bunch_threshold_seconds"].map(format_minutes)
        all_events = all_events[
            [
                "event_date",
                "event_hour",
                "route_label",
                "direction_id",
                "stop_id",
                "stop_name",
                "lead_trip_id",
                "trailing_trip_id",
                "lead_headsign",
                "trailing_headsign",
                "first_seen",
                "last_seen",
                "first_predicted_arrival",
                "last_predicted_arrival",
                "min_actual_headway",
                "scheduled_headway",
                "threshold",
                "snapshots_flagged",
                "headway_source",
                "source_files",
            ]
        ]
        write_section_table(
            handle,
            "All Identified Bus Bunching Events",
            all_events,
            list(all_events.columns),
        )


def main() -> int:
    args = parse_args()
    gtfs_dir = Path(args.gtfs_dir)
    output_path = Path(args.output)
    events_csv_path = Path(args.events_csv)

    if args.threshold_ratio <= 0:
        raise ValueError("--threshold-ratio must be positive")
    if args.min_bunch_minutes < 0:
        raise ValueError("--min-bunch-minutes cannot be negative")
    if args.max_bunch_minutes <= 0:
        raise ValueError("--max-bunch-minutes must be positive")
    if args.min_bunch_minutes > args.max_bunch_minutes:
        raise ValueError("--min-bunch-minutes cannot exceed --max-bunch-minutes")

    max_bunch_seconds = args.max_bunch_minutes * 60
    min_bunch_seconds = args.min_bunch_minutes * 60
    fallback_headway_seconds = args.fallback_headway_minutes * 60
    max_scheduled_headway_seconds = args.max_scheduled_headway_minutes * 60

    print("Loading static GTFS...")
    gtfs = load_gtfs(gtfs_dir, max_scheduled_headway_seconds)
    if ZoneInfo is not None:
        try:
            ZoneInfo(gtfs.timezone_name)
        except Exception as exc:
            raise RuntimeError(
                f"Python could not load GTFS timezone {gtfs.timezone_name!r}. "
                "Install tzdata or use a Python environment with zoneinfo data."
            ) from exc

    db_paths = [Path(path) for path in sorted(glob.glob(args.db_glob))]
    if not db_paths:
        raise FileNotFoundError(f"No database files matched: {args.db_glob}")

    all_candidate_frames: list[pd.DataFrame] = []
    db_statuses: list[tuple[str, str]] = []
    for db_path in db_paths:
        print(f"Scanning {db_path}...")
        try:
            candidates, status = read_candidate_pairs(db_path, gtfs, max_bunch_seconds)
        except Exception as exc:
            candidates = pd.DataFrame()
            status = f"error: {exc}"
        db_statuses.append((db_path.name, status))
        if not candidates.empty:
            all_candidate_frames.append(candidates)

    if all_candidate_frames:
        candidates = pd.concat(all_candidate_frames, ignore_index=True)
        print(f"Applying GTFS headway thresholds to {len(candidates):,} candidate pairs...")
        thresholded = apply_gtfs_thresholds(
            candidates,
            gtfs,
            args.threshold_ratio,
            min_bunch_seconds,
            max_bunch_seconds,
            fallback_headway_seconds,
            max_scheduled_headway_seconds,
            args.include_terminals,
        )
        print(f"Labeling and deduplicating {len(thresholded):,} bunched observations...")
        labeled = add_labels_and_times(thresholded, gtfs)
        events = collapse_repeated_snapshots(labeled)
    else:
        events = pd.DataFrame()

    print(f"Writing event CSV to {events_csv_path}...")
    write_events_csv(events, events_csv_path)
    print(f"Writing report to {output_path}...")
    build_report(output_path, db_statuses, events, gtfs, args)
    print(f"Done. Unique events: {len(events):,}")
    return 0


if __name__ == "__main__":
    main()