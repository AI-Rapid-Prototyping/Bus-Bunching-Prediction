import os
import sqlite3
import time
from datetime import datetime
import requests
from google.transit import gtfs_realtime_pb2

# ==============================================================================
# SECTION 1: STORAGE AND ENDPOINT CONFIGURATION
# ==============================================================================
# Ensure the required data folder is established locally
os.makedirs("data", exist_ok=True)
DB_PATH = "data/transit_data.db"

# Public Amazon S3 Production Endpoints for King County Metro (KCM) real-time feeds
VEHICLE_POSITIONS_URL = "https://s3.amazonaws.com/kcm-alerts-realtime-prod/vehiclepositions.pb"
TRIP_UPDATES_URL = "https://s3.amazonaws.com/kcm-alerts-realtime-prod/tripupdates.pb"

# Custom headers mimicking a browser to prevent AWS CloudFront/S3 from dropping requests
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


# ==============================================================================
# SECTION 2: RELATIONAL DATABASE SYSTEM SETUP
# ==============================================================================
def init_combined_db():
    """
    Creates tables and applies performance optimizations to SQLite for streaming data.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Schema 1: Physical bus coordinate data table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS vehicle_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vehicle_id TEXT,
            trip_id TEXT,
            route_id TEXT,
            latitude REAL,
            longitude REAL,
            bearing REAL,
            speed REAL,
            vehicle_timestamp INTEGER,
            logged_at TEXT,
            UNIQUE(vehicle_id, vehicle_timestamp)
        );
    ''')
    
    # Schema 2: Live scheduled bus stop arrival delay table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stop_time_updates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trip_id TEXT,
            route_id TEXT,
            stop_sequence INTEGER,
            stop_id TEXT,
            arrival_delay INTEGER,
            arrival_time INTEGER,
            departure_delay INTEGER,
            departure_time INTEGER,
            feed_timestamp INTEGER,
            logged_at TEXT,
            UNIQUE(trip_id, stop_id, feed_timestamp)
        );
    ''')
    
    # Force Write-Ahead Logging (WAL) mode to keep database engine highly responsive
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")
    
    conn.commit()
    conn.close()
    print(f"Database prepared and optimized at: {DB_PATH}", flush=True)


# ==============================================================================
# SECTION 3: REAL-TIME SAMPLE PREVIEW DIAGNOSTICS
# ==============================================================================
def print_database_sample():
    """
    Queries the database entries instantly and flushes a validation preview to standard output.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("\n--- LIVE DATABASE ENTRY SNAPSHOT ---", flush=True)
    
    # Vehicle coordinates query validation
    cursor.execute("SELECT vehicle_id, trip_id, latitude, longitude, logged_at FROM vehicle_positions ORDER BY id DESC LIMIT 2;")
    v_rows = cursor.fetchall()
    print("[Sample - vehicle_positions Table]", flush=True)
    if v_rows:
        for row in v_rows:
            print(f"  Vehicle: {row[0]} | Trip: {row[1]} | Lat/Lon: ({row[2]}, {row[3]}) | Logged: {row[4]}", flush=True)
    else:
        print("  Table vehicle_positions is currently empty.", flush=True)
        
    # Schedule delays query validation
    cursor.execute("SELECT trip_id, stop_id, arrival_delay, logged_at FROM stop_time_updates ORDER BY id DESC LIMIT 2;")
    t_rows = cursor.fetchall()
    print("[Sample - stop_time_updates Table]", flush=True)
    if t_rows:
        for row in t_rows:
            print(f"  Trip: {row[0]} | Stop ID: {row[1]} | Delay: {row[2]} seconds | Logged: {row[3]}", flush=True)
    else:
        print("  Table stop_time_updates is currently empty.", flush=True)
        
    print("------------------------------------\n", flush=True)
    conn.close()


# ==============================================================================
# SECTION 4: ROBUST INGESTION LOOP CORE ENGINE
# ==============================================================================
def run_pipeline():
    """
    Executes a high-responsiveness ingestion cycle wrapped completely in a 
    top-level interrupt block to guarantee manual cancellation capability.
    """
    print("Activating King County Metro Live Streaming Pipeline...", flush=True)
    
    # This outer try block catches KeyboardInterrupt anywhere, ensuring termination works
    try:
        while True:
            current_log_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            # ------------------------------------------------------------------
            # PART 4.1: Vehicle Positions Processing Segment
            # ------------------------------------------------------------------
            try:
                # Issue secure network call equipped with fake browser agent and distinct timeouts
                v_response = requests.get(VEHICLE_POSITIONS_URL, headers=HTTP_HEADERS, timeout=10)
                if v_response.status_code == 200:
                    v_feed = gtfs_realtime_pb2.FeedMessage()
                    v_feed.ParseFromString(v_response.content)
                    
                    v_records = []
                    for entity in v_feed.entity:
                        if entity.HasField('vehicle'):
                            v = entity.vehicle
                            v_records.append((
                                v.vehicle.id if v.vehicle.id else None,
                                v.trip.trip_id if v.trip.trip_id else None,
                                v.trip.route_id if v.trip.route_id else None,
                                v.position.latitude if v.position.HasField('latitude') else None,
                                v.position.longitude if v.position.HasField('longitude') else None,
                                v.position.bearing if v.position.HasField('bearing') else None,
                                v.position.speed if v.position.HasField('speed') else None,
                                v.timestamp if v.timestamp else None,
                                current_log_time
                            ))
                    
                    cursor.executemany('''
                        INSERT OR IGNORE INTO vehicle_positions 
                        (vehicle_id, trip_id, route_id, latitude, longitude, bearing, speed, vehicle_timestamp, logged_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', v_records)
                    print(f"[{current_log_time}] Processed positions feed. Stored unique changes: {conn.total_changes}", flush=True)
                else:
                    print(f"[{current_log_time}] Position feed error: Status code {v_response.status_code}", flush=True)
            except requests.exceptions.RequestException as net_err:
                print(f"[{current_log_time}] Position network timeout/drop: {net_err}", flush=True)
            except Exception as parse_err:
                print(f"[{current_log_time}] Position parse anomaly skipped: {parse_err}", flush=True)

            # ------------------------------------------------------------------
            # PART 4.2: Trip Updates Processing Segment
            # ------------------------------------------------------------------
            try:
                t_response = requests.get(TRIP_UPDATES_URL, headers=HTTP_HEADERS, timeout=10)
                if t_response.status_code == 200:
                    t_feed = gtfs_realtime_pb2.FeedMessage()
                    t_feed.ParseFromString(t_response.content)
                    
                    t_records = []
                    for entity in t_feed.entity:
                        if entity.HasField('trip_update'):
                            tu = entity.trip_update
                            t_id = tu.trip.trip_id if tu.trip.trip_id else None
                            r_id = tu.trip.route_id if tu.trip.route_id else None
                            f_time = tu.timestamp if tu.timestamp else None
                            
                            for stu in tu.stop_time_update:
                                arr_delay = stu.arrival.delay if stu.HasField('arrival') and stu.arrival.HasField('delay') else None
                                arr_time = stu.arrival.time if stu.HasField('arrival') and stu.arrival.HasField('time') else None
                                dep_delay = stu.departure.delay if stu.HasField('departure') and stu.departure.HasField('delay') else None
                                dep_time = stu.departure.time if stu.HasField('departure') and stu.departure.HasField('time') else None
                                
                                t_records.append((
                                    t_id, r_id,
                                    stu.stop_sequence if stu.HasField('stop_sequence') else None,
                                    stu.stop_id if stu.stop_id else None,
                                    arr_delay, arr_time, dep_delay, dep_time, f_time, current_log_time
                                ))
                
                # Check performance mutations delta across trip execution
                pre_save_changes = conn.total_changes
                cursor.executemany('''
                    INSERT OR IGNORE INTO stop_time_updates 
                    (trip_id, route_id, stop_sequence, stop_id, arrival_delay, arrival_time, departure_delay, departure_time, feed_timestamp, logged_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', t_records)
                print(f"[{current_log_time}] Processed updates feed. Stored unique rows: {conn.total_changes - pre_save_changes}", flush=True)
            except requests.exceptions.RequestException as net_err:
                print(f"[{current_log_time}] Schedule update feed connection issue: {net_err}", flush=True)
            except Exception as parse_err:
                print(f"[{current_log_time}] Schedule update parsing anomaly skipped: {parse_err}", flush=True)

            # Finalize open processing transactions
            conn.commit()
            conn.close()
            
            # Print database validation preview
            print_database_sample()
            
            # Sleep step between calls
            time.sleep(20)

    except KeyboardInterrupt:
        print("\nPipeline ingestion explicitly terminated via user stop command.", flush=True)
    finally:
        # Guarantee database connection is dropped safely even under harsh crashes
        try:
            conn.close()
        except:
            pass


# ==============================================================================
# SECTION 5: ENVIRONMENT ENTRYPOINT
# ==============================================================================
if __name__ == "__main__":
    init_combined_db()
    run_pipeline()