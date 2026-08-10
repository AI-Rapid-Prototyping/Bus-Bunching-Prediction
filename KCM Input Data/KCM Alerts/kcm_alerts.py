import requests
import pandas as pd
import os
import zipfile
import io
from datetime import datetime
import pytz

# Define the absolute Databricks path for storage
BASE_DIR = "/Workspace/Shared/IP3/fta/KCM Input Data/KCM Alerts"

# ==========================================
# FETCH ROUTE NAME MAPPING (STATIC GTFS)
# ==========================================
def get_kcm_route_mapping():
    """
    Downloads KCM's static GTFS to map internal route_ids (e.g., '100489') 
    to public names (e.g., 'Route 156'). Caches it locally for speed.
    """
    # Ensure the directory exists in Databricks Workspace
    os.makedirs(BASE_DIR, exist_ok=True)
    mapping_file = os.path.join(BASE_DIR, "kcm_route_mapping.csv")
    
    # 1. Load from cache if we already downloaded it
    if os.path.exists(mapping_file):
        df = pd.read_csv(mapping_file, dtype=str)
        return dict(zip(df['route_id'], df['formatted_name']))
        
    print("Downloading static GTFS to build route ID mapping (this only happens once)...")
    try:
        # Standard KCM GTFS Zip URL
        url = "https://metro.kingcounty.gov/GTFS/google_transit.zip"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Unzip in memory and extract routes.txt
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            with z.open('routes.txt') as f:
                routes_df = pd.read_csv(f, dtype=str)
                
                def format_route_name(row):
                    short_name = str(row['route_short_name']) if pd.notna(row['route_short_name']) else ''
                    long_name = str(row['route_long_name']) if pd.notna(row['route_long_name']) else ''
                    
                    # Use short name if available, otherwise fallback to long name
                    name = short_name if short_name else long_name
                    
                    # If the name is just a number (e.g., "156"), format it nicely.
                    # Otherwise keep it as is (e.g., "A Line" or "RapidRide C")
                    if name.isdigit():
                        return f"Route {name}"
                    return name
                    
                routes_df['formatted_name'] = routes_df.apply(format_route_name, axis=1)
                
                # Save just the mapped columns to our lightweight cache file
                mapping_df = routes_df[['route_id', 'formatted_name']]
                mapping_df.to_csv(mapping_file, index=False)
                
                print("Route mapping successfully built and cached!")
                return dict(zip(mapping_df['route_id'], mapping_df['formatted_name']))
                
    except Exception as e:
        print(f"Failed to fetch route mapping from static GTFS: {e}")
        return {}

# ==========================================
# FETCH KCM ALERTS VIA GTFS-REALTIME JSON
# ==========================================
def fetch_kcm_gtfs_rt_alerts():
    """
    Pulls live transit alerts directly from King County Metro's 
    GTFS-Realtime API infrastructure.
    """
    print("Fetching active KCM advisories from GTFS-RT JSON feed...")
    
    # Passing a User-Agent is good practice to ensure we aren't blocked by AWS
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json"
    }
    
    # The official King County Metro GTFS-RT Service Alerts endpoint
    urls_to_try = [
        "https://s3.amazonaws.com/kcm-alerts-realtime-prod/alerts_pb.json",
        "https://s3.amazonaws.com/kcm-alerts-realtime-prod/alerts.json"
    ]
    
    # Grab the translation dictionary
    route_map = get_kcm_route_mapping()
    
    for url in urls_to_try:
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            entities = data.get('entity', [])
            parsed_alerts = []
            
            # GTFS-RT Cause Mapping
            cause_mapping = {
                1: "UNKNOWN_CAUSE", 2: "OTHER_CAUSE", 3: "TECHNICAL_PROBLEM",
                4: "STRIKE", 5: "DEMONSTRATION", 6: "ACCIDENT", 7: "HOLIDAY",
                8: "WEATHER", 9: "MAINTENANCE", 10: "CONSTRUCTION",
                11: "POLICE_ACTIVITY", 12: "MEDICAL_EMERGENCY"
            }
            
            for entity in entities:
                alert = entity.get('alert', {})
                
                # Extract Affected Routes 
                informed_entities = alert.get('informed_entity', [])
                affected_routes = []
                for ie in informed_entities:
                    if 'route_id' in ie:
                        # Translate the ID to the public name!
                        r_id = ie['route_id']
                        r_name = route_map.get(r_id, r_id) 
                        affected_routes.append(r_name)
                
                # Deduplicate routes 
                affected_routes = list(set(affected_routes))
                
                # Extract Cause (JSON-PB often returns strings instead of ints for enums)
                raw_cause = alert.get('cause', 'UNKNOWN_CAUSE')
                if isinstance(raw_cause, str):
                    cause_str = raw_cause
                else:
                    cause_str = cause_mapping.get(raw_cause, "UNKNOWN_CAUSE")
                
                # Extract Text Description
                header_texts = alert.get('header_text', {}).get('translation', [])
                desc_texts = alert.get('description_text', {}).get('translation', [])
                
                header = header_texts[0].get('text', '') if header_texts else ""
                description = desc_texts[0].get('text', '') if desc_texts else ""
                
                # Combine into a summary
                summary = header if header else description
                
                # CRITICAL: Always stamp in Seattle local time regardless of server timezone
                seattle_tz = pytz.timezone('America/Los_Angeles')
                seattle_time = datetime.now(seattle_tz).strftime("%Y-%m-%d %H:%M:%S")
                
                parsed_alerts.append({
                    "Fetch_Time": seattle_time,
                    "End_Time": "N/A", # Add End_Time placeholder for active alerts
                    "Alert_ID": str(entity.get('id', 'N/A')),
                    "Status": "Active",
                    "Affected_Routes": ", ".join(affected_routes) if affected_routes else "Systemwide/Unknown",
                    "Cause": cause_str,
                    "Summary": summary[:300] + "..." if len(summary) > 300 else summary
                })
                
            df = pd.DataFrame(parsed_alerts)
            print(f"Successfully pulled {len(df)} active alerts via GTFS-RT JSON from {url}.")
            return df
            
        except Exception as e:
            print(f"Error fetching GTFS-RT JSON from {url}: {e}")
            
    # If all URLs fail, return an empty DataFrame
    print("Failed to connect to KCM API servers.")
    return pd.DataFrame()

# --- Execution ---
if __name__ == "__main__":
    
    # Pull GTFS-RT Alerts
    alerts_df = fetch_kcm_gtfs_rt_alerts()
    
    # --- SMART MERGE TO DATABRICKS WORKSPACE CSV ---
    os.makedirs(BASE_DIR, exist_ok=True)
    csv_filename = os.path.join(BASE_DIR, "kcm_advisory_history.csv")
    
    file_exists = os.path.isfile(csv_filename)
    
    if file_exists:
        try:
            # Read the existing CSV, forcing Alert_ID to be read as a string to prevent int/str matching bugs
            existing_df = pd.read_csv(csv_filename, dtype={'Alert_ID': str})
            
            # Ensure the Status and End_Time columns exist for backward compatibility with older files
            if 'Status' not in existing_df.columns:
                existing_df['Status'] = 'Resolved'
            if 'End_Time' not in existing_df.columns:
                existing_df['End_Time'] = 'N/A'
                
            # Ensure both dataframes have perfectly matching string IDs
            existing_df['Alert_ID'] = existing_df['Alert_ID'].astype(str)
            
            # PRESERVE ORIGINAL FETCH TIME:
            # If an active alert already exists in our historical CSV, retain its original 
            # Fetch_Time so we know when it first started, rather than updating it to right now.
            if not alerts_df.empty and 'Fetch_Time' in existing_df.columns:
                existing_fetch_times = dict(zip(existing_df['Alert_ID'], existing_df['Fetch_Time']))
                
                def preserve_original_start_time(row):
                    aid = row['Alert_ID']
                    if aid in existing_fetch_times:
                        return existing_fetch_times[aid] # Keep original start time
                    return row['Fetch_Time'] # New alert gets current timestamp
                    
                alerts_df['Fetch_Time'] = alerts_df.apply(preserve_original_start_time, axis=1)
            
            # Mark any historical alert that is NOT in the current fetch as 'Resolved'
            if not alerts_df.empty:
                alerts_df['Alert_ID'] = alerts_df['Alert_ID'].astype(str)
                current_active_ids = alerts_df['Alert_ID'].tolist()
            else:
                current_active_ids = []
                
            # Identify alerts that are missing from the current active fetch
            missing_alerts_mask = ~existing_df['Alert_ID'].isin(current_active_ids)
            
            # Identify alerts that are NEWLY resolved (they were Active on the last run, but missing now)
            newly_resolved_mask = missing_alerts_mask & (existing_df['Status'] != 'Resolved')
            
            # Stamp the current Seattle time as the End_Time for newly resolved alerts
            seattle_tz = pytz.timezone('America/Los_Angeles')
            seattle_time = datetime.now(seattle_tz).strftime("%Y-%m-%d %H:%M:%S")
            existing_df.loc[newly_resolved_mask, 'End_Time'] = seattle_time
            
            # Update the status to Resolved
            existing_df.loc[missing_alerts_mask, 'Status'] = 'Resolved'
            
            # Combine the old and new data
            combined_df = pd.concat([alerts_df, existing_df], ignore_index=True)
            
            # Drop duplicates based on the unique 'Alert_ID', 
            # keeping the first occurrence (which will be the freshly fetched 'Active' state if it still exists)
            combined_df = combined_df.drop_duplicates(subset=['Alert_ID'], keep='first')
            
            # Overwrite the file with the deduplicated historical data
            combined_df.to_csv(csv_filename, index=False)
            
            new_count = len(combined_df) - len(existing_df)
            print(f"\nSuccessfully processed updates! {csv_filename} now has {len(combined_df)} total recorded alerts.")
            if new_count > 0:
                print(f"Added {new_count} NEW alerts to the historical record.")
                
        except pd.errors.EmptyDataError:
            if not alerts_df.empty:
                alerts_df.to_csv(csv_filename, index=False)
                print(f"\nPopulated empty file {csv_filename} with {len(alerts_df)} alerts!")
    else:
        if not alerts_df.empty:
            alerts_df.to_csv(csv_filename, index=False)
            print(f"\nCreated Databricks Workspace file {csv_filename} with {len(alerts_df)} alerts!")

    # Print a terminal summary
    if not alerts_df.empty:
        print("\n--- CURRENTLY ACTIVE KCM SERVICE ADVISORIES ---")
        cols_to_print = [col for col in ['Fetch_Time', 'Affected_Routes', 'Status', 'Cause', 'Summary'] if col in alerts_df.columns]
        print(alerts_df[cols_to_print].head(10))
    else:
        print("\nNo active KCM advisories at this time.")