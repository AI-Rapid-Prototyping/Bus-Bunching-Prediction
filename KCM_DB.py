from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence, Tuple

VEHICLE_POSITIONS_URL = "https://s3.amazonaws.com/kcm-alerts-realtime-prod/vehiclepositions.pb"
TRIP_UPDATES_URL = "https://s3.amazonaws.com/kcm-alerts-realtime-prod/tripupdates.pb"

DEFAULT_CATALOG = "volpe_ip3_dev"
DEFAULT_SCHEMA = "gtfsrt"

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

VehicleRow = Tuple[
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[int],
    datetime,
]

TripRow = Tuple[
    Optional[str],
    Optional[str],
    Optional[int],
    Optional[str],
    Optional[int],
    Optional[int],
    Optional[int],
    Optional[int],
    Optional[int],
    datetime,
]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _import_requests():
    try:
        import requests
    except ImportError as exc:
        raise RuntimeError(
            "requests is required to fetch live GTFS-RT feeds. "
            "Add it to the Databricks job libraries."
        ) from exc
    return requests


def _import_gtfs_proto():
    try:
        from google.transit import gtfs_realtime_pb2
    except ImportError as exc:
        raise RuntimeError(
            "gtfs-realtime-bindings is required to parse GTFS-RT payloads. "
            "Add it to the Databricks job libraries."
        ) from exc
    return gtfs_realtime_pb2


def _import_spark():
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError("PySpark is required to write Delta tables in Databricks.") from exc
    return SparkSession.builder.getOrCreate()


def _import_delta_table():
    try:
        from delta.tables import DeltaTable
    except ImportError as exc:
        raise RuntimeError("Delta Lake is required to merge rows into target tables.") from exc
    return DeltaTable


def _load_feed_bytes(url: str, local_path: Optional[str], timeout_seconds: int) -> Tuple[bytes, str]:
    if local_path:
        path = Path(local_path).expanduser()
        return path.read_bytes(), str(path)

    requests = _import_requests()
    response = requests.get(url, headers=HTTP_HEADERS, timeout=timeout_seconds)
    response.raise_for_status()
    return response.content, url


def parse_vehicle_rows(feed_bytes: bytes, logged_at: datetime) -> list[VehicleRow]:
    gtfs = _import_gtfs_proto()
    feed = gtfs.FeedMessage()
    feed.ParseFromString(feed_bytes)

    rows: list[VehicleRow] = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue

        vehicle = entity.vehicle
        rows.append(
            (
                vehicle.vehicle.id or None,
                vehicle.trip.trip_id or None,
                vehicle.trip.route_id or None,
                vehicle.position.latitude if vehicle.position.HasField("latitude") else None,
                vehicle.position.longitude if vehicle.position.HasField("longitude") else None,
                vehicle.position.bearing if vehicle.position.HasField("bearing") else None,
                vehicle.position.speed if vehicle.position.HasField("speed") else None,
                vehicle.timestamp if vehicle.timestamp else None,
                logged_at,
            )
        )

    return rows


def parse_trip_rows(feed_bytes: bytes, logged_at: datetime) -> list[TripRow]:
    gtfs = _import_gtfs_proto()
    feed = gtfs.FeedMessage()
    feed.ParseFromString(feed_bytes)

    rows: list[TripRow] = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        trip_update = entity.trip_update
        trip_id = trip_update.trip.trip_id or None
        route_id = trip_update.trip.route_id or None
        feed_timestamp = trip_update.timestamp if trip_update.timestamp else None

        for stop_update in trip_update.stop_time_update:
            arrival_delay = (
                stop_update.arrival.delay
                if stop_update.HasField("arrival") and stop_update.arrival.HasField("delay")
                else None
            )
            arrival_time = (
                stop_update.arrival.time
                if stop_update.HasField("arrival") and stop_update.arrival.HasField("time")
                else None
            )
            departure_delay = (
                stop_update.departure.delay
                if stop_update.HasField("departure") and stop_update.departure.HasField("delay")
                else None
            )
            departure_time = (
                stop_update.departure.time
                if stop_update.HasField("departure") and stop_update.departure.HasField("time")
                else None
            )

            rows.append(
                (
                    trip_id,
                    route_id,
                    stop_update.stop_sequence if stop_update.HasField("stop_sequence") else None,
                    stop_update.stop_id or None,
                    arrival_delay,
                    arrival_time,
                    departure_delay,
                    departure_time,
                    feed_timestamp,
                    logged_at,
                )
            )

    return rows


def _vehicle_schema():
    try:
        from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType
    except ImportError as exc:
        raise RuntimeError("PySpark is required to build the vehicle_positions schema.") from exc

    return StructType(
        [
            StructField("vehicle_id", StringType(), True),
            StructField("trip_id", StringType(), True),
            StructField("route_id", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("bearing", DoubleType(), True),
            StructField("speed", DoubleType(), True),
            StructField("vehicle_timestamp", LongType(), True),
            StructField("logged_at", TimestampType(), True),
        ]
    )


def _trip_schema():
    try:
        from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType
    except ImportError as exc:
        raise RuntimeError("PySpark is required to build the stop_time_updates schema.") from exc

    return StructType(
        [
            StructField("trip_id", StringType(), True),
            StructField("route_id", StringType(), True),
            StructField("stop_sequence", LongType(), True),
            StructField("stop_id", StringType(), True),
            StructField("arrival_delay", LongType(), True),
            StructField("arrival_time", LongType(), True),
            StructField("departure_delay", LongType(), True),
            StructField("departure_time", LongType(), True),
            StructField("feed_timestamp", LongType(), True),
            StructField("logged_at", TimestampType(), True),
        ]
    )


def _qualified_name(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def _create_table_sql(catalog: str, schema: str, table: str, columns_ddl: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {_qualified_name(catalog, schema, table)} (
{columns_ddl}
)
USING DELTA
""".strip()


def ensure_target_tables(spark, catalog: str, schema: str) -> None:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(
        _create_table_sql(
            catalog,
            schema,
            "vehicle_positions",
            """
  vehicle_id STRING,
  trip_id STRING,
  route_id STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  bearing DOUBLE,
  speed DOUBLE,
  vehicle_timestamp BIGINT,
  logged_at TIMESTAMP
""".strip(),
        )
    )
    spark.sql(
        _create_table_sql(
            catalog,
            schema,
            "stop_time_updates",
            """
  trip_id STRING,
  route_id STRING,
  stop_sequence BIGINT,
  stop_id STRING,
  arrival_delay BIGINT,
  arrival_time BIGINT,
  departure_delay BIGINT,
  departure_time BIGINT,
  feed_timestamp BIGINT,
  logged_at TIMESTAMP
""".strip(),
        )
    )


def _merge_rows(spark, table_name: str, key_columns: Sequence[str], rows, schema) -> int:
    if not rows:
        print(f"{table_name}: no rows parsed.", flush=True)
        return 0

    field_names = [field.name for field in schema.fields]
    key_indexes = [field_names.index(col) for col in key_columns]
    unique_rows = []
    seen_keys = set()
    for row in rows:
        key = tuple(row[index] for index in key_indexes)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        unique_rows.append(row)

    if not unique_rows:
        print(f"{table_name}: no unique rows after deduplication.", flush=True)
        return 0

    source_df = spark.createDataFrame(unique_rows, schema=schema)
    DeltaTable = _import_delta_table()
    condition = " AND ".join([f"target.{col} = source.{col}" for col in key_columns])
    (
        DeltaTable.forName(spark, table_name)
        .alias("target")
        .merge(source_df.alias("source"), condition)
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"{table_name}: merged {len(unique_rows)} unique rows.", flush=True)
    return len(unique_rows)


def _ingest_feed(label: str, url: str, local_path: Optional[str], parser, logged_at: datetime, timeout_seconds: int):
    try:
        feed_bytes, source = _load_feed_bytes(url, local_path, timeout_seconds)
        rows = parser(feed_bytes, logged_at)
        print(f"{label}: parsed {len(rows)} rows from {source}", flush=True)
        return rows, source, None
    except Exception as exc:
        print(f"{label}: {exc}", flush=True)
        return [], local_path or url, exc


def _print_dry_run_summary(vehicle_rows, trip_rows, vehicle_source: str, trip_source: str) -> None:
    print("\n--- DRY RUN SUMMARY ---", flush=True)
    print(f"Vehicle feed source: {vehicle_source}", flush=True)
    print(f"Vehicle rows parsed: {len(vehicle_rows)}", flush=True)
    if vehicle_rows:
        print(f"Vehicle sample: {vehicle_rows[0]}", flush=True)
    print(f"Trip feed source: {trip_source}", flush=True)
    print(f"Trip rows parsed: {len(trip_rows)}", flush=True)
    if trip_rows:
        print(f"Trip sample: {trip_rows[0]}", flush=True)
    print("----------------------\n", flush=True)


def run_pipeline(
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
    vehicle_feed_file: Optional[str] = None,
    trip_feed_file: Optional[str] = None,
    timeout_seconds: int = 20,
    dry_run: bool = False,
) -> None:
    logged_at = _utc_now()

    vehicle_rows, vehicle_source, vehicle_error = _ingest_feed(
        "vehicle_positions",
        VEHICLE_POSITIONS_URL,
        vehicle_feed_file,
        parse_vehicle_rows,
        logged_at,
        timeout_seconds,
    )
    trip_rows, trip_source, trip_error = _ingest_feed(
        "stop_time_updates",
        TRIP_UPDATES_URL,
        trip_feed_file,
        parse_trip_rows,
        logged_at,
        timeout_seconds,
    )

    if dry_run:
        _print_dry_run_summary(vehicle_rows, trip_rows, vehicle_source, trip_source)
        if vehicle_error or trip_error:
            raise RuntimeError("Dry run completed with at least one feed failure.")
        return

    spark = _import_spark()
    ensure_target_tables(spark, catalog, schema)

    vehicle_table = _qualified_name(catalog, schema, "vehicle_positions")
    trip_table = _qualified_name(catalog, schema, "stop_time_updates")

    vehicle_written = _merge_rows(
        spark,
        vehicle_table,
        ["vehicle_id", "vehicle_timestamp"],
        vehicle_rows,
        _vehicle_schema(),
    )
    trip_written = _merge_rows(
        spark,
        trip_table,
        ["trip_id", "stop_id", "feed_timestamp"],
        trip_rows,
        _trip_schema(),
    )

    if vehicle_error and trip_error:
        raise RuntimeError("Both KCM feeds failed to ingest; no rows were written.")

    print(
        f"Completed one ingest cycle at {logged_at.isoformat()} "
        f"(vehicle rows: {vehicle_written}, trip rows: {trip_written}).",
        flush=True,
    )


def parse_args(argv: Optional[Sequence[str]] = None):
    parser = argparse.ArgumentParser(description="Ingest KCM GTFS-RT feeds into Databricks Delta tables.")
    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--vehicle-feed-file", default=None)
    parser.add_argument("--trip-feed-file", default=None)
    parser.add_argument("--timeout-seconds", type=int, default=20)
    parser.add_argument("--dry-run", action="store_true")
    # Databricks notebooks inject their own launcher args, including "-f".
    return parser.parse_known_args(argv)[0]


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    run_pipeline(
        catalog=args.catalog,
        schema=args.schema,
        vehicle_feed_file=args.vehicle_feed_file,
        trip_feed_file=args.trip_feed_file,
        timeout_seconds=args.timeout_seconds,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
