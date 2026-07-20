import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql import Window

def analyze_transit_bunching_system(headway_threshold_mins=2):
    """
    Analyzes GTFS-RT stop time updates for bus bunching. Reports collection time horizons,
    bus directions, and performs a deep route-by-route 'when and where' failure diagnostic.

    Performance: All heavy processing (terminal exclusion, headway calculation,
    bunching detection, deduplication) runs in Spark before any pandas collection.
    """
    threshold_seconds = headway_threshold_mins * 60

    # -------------------------------------------------------------------------
    # STEP 1: GTFS STATIC METADATA (Routes, Stops, Trip Headsigns)
    # -------------------------------------------------------------------------
    routes_dict = {}
    stops_dict = {}
    trips_headsign_dict = {}

    print("Loading GTFS structural metadata...")
    try:
        routes_df = pd.read_csv('/Workspace/Shared/IP3/fta/google_transit/routes.txt', dtype=str)
        routes_dict = dict(zip(routes_df['route_id'], routes_df['route_short_name']))
    except Exception as e:
        print(f"Note: Could not parse routes.txt ({e})")

    try:
        stops_df = pd.read_csv('/Workspace/Shared/IP3/fta/google_transit/stops.txt', dtype=str)
        stops_dict = dict(zip(stops_df['stop_id'], stops_df['stop_name']))
    except Exception as e:
        print(f"Note: Could not parse stops.txt ({e})")

    try:
        trips_df = pd.read_csv('/Workspace/Shared/IP3/fta/google_transit/trips.txt', dtype=str)
        trips_headsign_dict = dict(zip(trips_df['trip_id'], trips_df['trip_headsign']))
    except Exception as e:
        print(f"Note: Could not parse trips.txt ({e})")

    # -------------------------------------------------------------------------
    # STEP 2: LOAD GTFS-RT STOP TIME UPDATES FROM SPARK TABLE
    # -------------------------------------------------------------------------
    print("\n============================================================")
    print("         DATA PORTFOLIO CAPTURE TIME HORIZONS               ")
    print("============================================================")
    stop_time_updates = spark.table("volpe_ip3_dev.gtfsrt.stop_time_updates")

    # Fix: use F.min/F.max directly (avoids the duplicate-key bug in dict-based agg)
    time_horizons = stop_time_updates.agg(
        F.min("logged_at").alias("start_t"),
        F.max("logged_at").alias("end_t")
    )
    display(time_horizons)

    df = stop_time_updates.select(
        "trip_id", "route_id", "stop_sequence", "stop_id", "arrival_time", "logged_at"
    ).where("arrival_time IS NOT NULL")

    # -------------------------------------------------------------------------
    # STEP 3: TERMINAL EXCLUSION & HEADWAY CALCULATION — entirely in Spark
    # -------------------------------------------------------------------------
    # Compute per-trip terminal bounds via groupBy + broadcast join.
    # Faster than a window function because the result (28K rows) fits in a broadcast.
    trip_bounds = df.groupBy("trip_id").agg(
        F.min("stop_sequence").alias("trip_min_seq"),
        F.max("stop_sequence").alias("trip_max_seq")
    )
    df = df.join(F.broadcast(trip_bounds), on="trip_id", how="left")

    # Exclude first and last stops per trip
    df_intermediate = df.filter(
        (F.col("stop_sequence") != F.col("trip_min_seq")) &
        (F.col("stop_sequence") != F.col("trip_max_seq"))
    ).drop("trip_min_seq", "trip_max_seq")

    # Compute headways with Spark Window lag — replaces the pandas sort + groupby + shift
    headway_window = (
        Window.partitionBy("logged_at", "route_id", "stop_id")
              .orderBy("arrival_time")
    )
    df_intermediate = df_intermediate \
        .withColumn("prev_arrival_time", F.lag("arrival_time").over(headway_window)) \
        .withColumn("prev_trip_id",      F.lag("trip_id").over(headway_window)) \
        .withColumn("headway_seconds",   F.col("arrival_time") - F.col("prev_arrival_time"))

    # Filter to bunched records only — massively reduces rows before collection
    bunched_spark = df_intermediate.filter(
        F.col("headway_seconds").between(0, threshold_seconds)
    )

    # -------------------------------------------------------------------------
    # STEP 4: ENRICH WITH HEADSIGNS, PARSE TIMESTAMPS, DEDUPLICATE — in Spark
    # -------------------------------------------------------------------------
    # Parse date and hour in Spark
    bunched_spark = bunched_spark \
        .withColumn("datetime_parsed", F.col("logged_at").cast("timestamp")) \
        .withColumn("date",     F.to_date("datetime_parsed")) \
        .withColumn("hour_str", F.date_format("datetime_parsed", "HH:00"))

    # Headsign lookup via broadcast join on the small trips reference table
    if trips_headsign_dict:
        headsign_ref = spark.createDataFrame(
            list(trips_headsign_dict.items()), ["trip_id", "headsign"]
        )
        bunched_spark = bunched_spark \
            .join(F.broadcast(headsign_ref), on="trip_id", how="left") \
            .withColumn("headsign", F.coalesce(F.col("headsign"), F.lit("Unknown Destination")))
    else:
        bunched_spark = bunched_spark.withColumn("headsign", F.lit("Unknown Destination"))

    # Deduplicate: one row per (date, route, stop, bus-pair) — still in Spark
    unique_events_spark = bunched_spark.groupBy(
        "date", "route_id", "stop_id", "prev_trip_id", "trip_id"
    ).agg(
        F.first("logged_at",    ignorenulls=True).alias("logged_at"),
        F.min("headway_seconds").alias("headway_seconds"),
        F.first("hour_str",     ignorenulls=True).alias("hour_str"),
        F.first("headsign",     ignorenulls=True).alias("headsign")
    )

    # Collect only the small deduplicated result to the driver
    unique_events = unique_events_spark.toPandas()

    if unique_events.empty:
        print(f"\nNo bunching detected under threshold of {headway_threshold_mins} minutes.")
        return

    # -------------------------------------------------------------------------
    # STEP 5: COMPREHENSIVE INCIDENT & ROUTE-SPECIFIC REPORTING
    # -------------------------------------------------------------------------
    get_route_name = lambda r_id: routes_dict.get(str(r_id), f"ID-{r_id}")
    get_stop_name  = lambda s_id: stops_dict.get(str(s_id), f"ID-{s_id}")

    print("\n============================================================")
    print("             REAL-TIME BUS BUNCHING SAMPLE LOG              ")
    print("============================================================")
    sample_size = min(4, len(unique_events))
    sample_df = unique_events.sample(n=sample_size, random_state=101).sort_values('logged_at')
    for _, row in sample_df.iterrows():
        r_lbl = get_route_name(row['route_id'])
        s_lbl = get_stop_name(row['stop_id'])
        print(f"• Time: {row['logged_at']} | {r_lbl.ljust(8)} towards [{row['headsign']}]")
        print(f"  Location : Stop {row['stop_id']} ({s_lbl})")
        print(f"  Headway  : Drivers bunched down to {int(row['headway_seconds'])} seconds space.\n")

    print("============================================================")
    print("      ROUTE DIAGNOSTIC MATRIX: WHEN & WHERE HOTSPOTS       ")
    print("============================================================")

    sorted_routes = unique_events['route_id'].value_counts()

    for r_id, total_count in sorted_routes.items():
        r_name = get_route_name(r_id)
        r_events = unique_events[unique_events['route_id'] == r_id]

        print(f"\nRoute {r_name} (Total Bunching Events Flagged: {total_count})")
        print(f"  " + "-" * 45)

        print("  CRITICAL GEOGRAPHIC HOTSPOTS:")
        top_stops = r_events['stop_id'].value_counts().head(3)
        for s_id, count in top_stops.items():
            print(f"    » Stop {s_id.ljust(6)} ({get_stop_name(s_id)}) : {count} times")

        print("  PEAK TIMING & CORRIDOR DIRECTIONS:")
        time_dir_patterns = r_events.groupby(['headsign', 'hour_str']).size().reset_index(name='count')
        top_patterns = time_dir_patterns.sort_values(by='count', ascending=False).head(3)
        for _, pattern in top_patterns.iterrows():
            print(f"    » Moving towards [{pattern['headsign']}] at {pattern['hour_str']} : {pattern['count']} times")

    print("\n" + "="*60 + "\n")


# ==============================================================================
# SECTION 6: FILE WRITING AND ENTRYPOINT
# ==============================================================================
if __name__ == "__main__":
    import sys

    class Tee(object):
        """
        A helper class that intercepts all 'print' statements and sends them
        to BOTH the VS Code terminal and a text file simultaneously.
        """
        def __init__(self, *files):
            self.files = files

        def write(self, obj):
            for f in self.files:
                f.write(obj)
                try:
                    f.flush()
                except:
                    pass

        def flush(self):
            for f in self.files:
                try:
                    f.flush()
                except:
                    pass

    # Open a text file in UTF-8 mode to prevent Windows crashes with special characters
    report_file = open("bunching_report.txt", "w", encoding="utf-8")

    # Save standard terminal output
    original_stdout = sys.stdout

    # Redirect stdout to write to both screen and file
    sys.stdout = Tee(original_stdout, report_file)

    try:
        analyze_transit_bunching_system()
    finally:
        # Crucial step: Restore terminal output and close the file safely
        sys.stdout = original_stdout
        report_file.close()

    print("\n" + "="*60)
    print(" [SUCCESS] Your complete un-truncated analysis report has been saved!")
    print(" Location: bunching_report.txt (in your active folder)")
    print("="*60 + "\n")
