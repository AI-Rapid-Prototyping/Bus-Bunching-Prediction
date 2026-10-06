import os
import glob
import sqlite3
import pandas as pd
import numpy as np

def analyze_transit_bunching_system(headway_threshold_mins=2):
    """
    Analyzes daily KCM databases for bus bunching. Reports collection time horizons, 
    bus directions, and performs a deep route-by-route 'when and where' failure diagnostic.
    """
    threshold_seconds = headway_threshold_mins * 60

    # -------------------------------------------------------------------------
    # STEP 1: DEEP GTFS STATIC METADATA LOADING (Routes, Stops, and Trip Headsigns)
    # -------------------------------------------------------------------------
    routes_dict = {}
    stops_dict = {}
    trips_headsign_dict = {}
    
    print("Loading GTFS structural metadata...")
    if os.path.exists('google_transit/routes.txt'):
        try:
            routes_df = pd.read_csv('google_transit/routes.txt', dtype=str)
            routes_dict = dict(zip(routes_df['route_id'], routes_df['route_short_name']))
        except Exception as e:
            print(f"Note: Could not parse routes.txt ({e})")
            
    if os.path.exists('google_transit/stops.txt'):
        try:
            stops_df = pd.read_csv('google_transit/stops.txt', dtype=str)
            stops_dict = dict(zip(stops_df['stop_id'], stops_df['stop_name']))
        except Exception as e:
            print(f"Note: Could not parse stops.txt ({e})")

    if os.path.exists('google_transit/trips.txt'):
        try:
            trips_df = pd.read_csv('google_transit/trips.txt', dtype=str)
            trips_headsign_dict = dict(zip(trips_df['trip_id'], trips_df['trip_headsign']))
        except Exception as e:
            print(f"Note: Could not parse trips.txt ({e})")

    # -------------------------------------------------------------------------
    # STEP 2: PROFILE TIME HORIZONS PER DATABASE AND AGGREGATE ROWS
    # -------------------------------------------------------------------------
    db_files = glob.glob("KCM_data_*.db")
    if not db_files:
        print("Error: No database files matching pattern 'KCM_data_*.db' found.")
        return

    print("\n============================================================")
    print("         DATA PORTFOLIO CAPTURE TIME HORIZONS               ")
    print("============================================================")
    all_dfs = []
    for db_path in sorted(db_files):
        try:
            conn = sqlite3.connect(db_path)
            # Find exact timestamps present in this day's run
            time_query = "SELECT MIN(logged_at) as start_t, MAX(logged_at) as end_t FROM stop_time_updates;"
            time_df = pd.read_sql_query(time_query, conn)
            start_val = time_df['start_t'].iloc[0]
            end_val = time_df['end_t'].iloc[0]
            print(f"• File: {db_path.ljust(22)} | Ingestion Active From {start_val} To {end_val}")

            # Extract the raw updates
            query = """
            SELECT trip_id, route_id, stop_sequence, stop_id, arrival_time, logged_at
            FROM stop_time_updates
            WHERE arrival_time IS NOT NULL;
            """
            df_day = pd.read_sql_query(query, conn)
            conn.close()
            if not df_day.empty:
                all_dfs.append(df_day)
        except Exception as e:
            print(f"Warning: Skipping {db_path} due to issue: {e}")

    if not all_dfs:
        print("Error: No data could be compiled.")
        return

    df = pd.concat(all_dfs, ignore_index=True)

    # -------------------------------------------------------------------------
    # STEP 3: TERMINAL EXCLUSION & CLEAN HEADWAY MATCHING
    # -------------------------------------------------------------------------
    trip_bounds = df.groupby('trip_id')['stop_sequence'].agg(['min', 'max']).reset_index()
    df = df.merge(trip_bounds, on='trip_id')
    df_intermediate = df[(df['stop_sequence'] != df['min']) & (df['stop_sequence'] != df['max'])].copy()

    df_intermediate = df_intermediate.sort_values(by=['logged_at', 'route_id', 'stop_id', 'arrival_time'])
    df_intermediate['prev_arrival_time'] = df_intermediate.groupby(['logged_at', 'route_id', 'stop_id'])['arrival_time'].shift(1)
    df_intermediate['prev_trip_id'] = df_intermediate.groupby(['logged_at', 'route_id', 'stop_id'])['trip_id'].shift(1)
    df_intermediate['headway_seconds'] = df_intermediate['arrival_time'] - df_intermediate['prev_arrival_time']

    # Filter out valid headways within threshold
    bunched_raw = df_intermediate[(df_intermediate['headway_seconds'] >= 0) & 
                                  (df_intermediate['headway_seconds'] <= threshold_seconds)].copy()
    
    if bunched_raw.empty:
        print(f"\nNo bunching detected under threshold of {headway_threshold_mins} minutes.")
        return

    # Parse datetimes
    bunched_raw['datetime_parsed'] = pd.to_datetime(bunched_raw['logged_at'])
    bunched_raw['date'] = bunched_raw['datetime_parsed'].dt.date
    bunched_raw['hour_str'] = bunched_raw['datetime_parsed'].dt.strftime('%H:00')

    # -------------------------------------------------------------------------
    # STEP 4: RESOLVE BUS DIREX / DESTINATIONS AND DEDUPLICATE SNAPSHOTS
    # -------------------------------------------------------------------------
    bunched_raw['headsign'] = bunched_raw['trip_id'].map(trips_headsign_dict).fillna("Unknown Destination")
    
    unique_events = bunched_raw.groupby(['date', 'route_id', 'stop_id', 'prev_trip_id', 'trip_id']).agg({
        'logged_at': 'first',
        'headway_seconds': 'min',
        'hour_str': 'first',
        'headsign': 'first'
    }).reset_index()

    # -------------------------------------------------------------------------
    # STEP 5: COMPREHENSIVE INCIDENT & ROUTE-SPECIFIC REPORTING
    # -------------------------------------------------------------------------
    get_route_name = lambda r_id: routes_dict.get(str(r_id), f"ID-{r_id}")
    get_stop_name = lambda s_id: stops_dict.get(str(s_id), f"ID-{s_id}")

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
    
    # Sort routes based on bunching severity counts
    sorted_routes = unique_events['route_id'].value_counts()
    
    for r_id, total_count in sorted_routes.items():
        r_name = get_route_name(r_id)
        r_events = unique_events[unique_events['route_id'] == r_id]
        
        print(f"\nRoute {r_name} (Total Bunching Events Flagged: {total_count})")
        print(f"  " + "-" * 45)
        
        # Geolocation Diagnostic (Where)
        print("  CRITICAL GEOGRAPHIC HOTSPOTS:")
        top_stops = r_events['stop_id'].value_counts().head(3)
        for s_id, count in top_stops.items():
            print(f"    » Stop {s_id.ljust(6)} ({get_stop_name(s_id)}) : {count} times")
            
        # Chronological & Direction Diagnostic (When)
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