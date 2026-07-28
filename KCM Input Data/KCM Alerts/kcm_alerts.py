import requests
import pandas as pd
import os
import zipfile
import io

# ==========================================
# FETCH ROUTE NAME MAPPING (STATIC GTFS)
# ==========================================
def get_kcm_route_mapping():
    """
    Downloads KCM's static GTFS to map internal route_ids (e.g., '100489') 
    to public names (e.g., 'Route 156'). Caches it locally for speed.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    mapping_file = os.path.join(script_dir, "kcm_route_mapping.csv")
    
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
    # We try _pb.json first as it has proven to be the most reliable
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
                
                parsed_alerts.append({
                    "Alert_ID": entity.get('id', 'N/A'),
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
    
    if not alerts_df.empty:
        print("\n--- RECENT KCM SERVICE ADVISORIES ---")
        cols_to_print = [col for col in ['Affected_Routes', 'Cause', 'Summary'] if col in alerts_df.columns]
        print(alerts_df[cols_to_print].head(10))
        
        # --- SAVE TO CSV ---
        # Ensure the file saves in the same directory as the script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csv_filename = os.path.join(script_dir, "kcm_active_advisories.csv")
        
        # We overwrite this file because it represents the *current* state of the transit network
        alerts_df.to_csv(csv_filename, index=False)
        
        print(f"\nSuccessfully saved {len(alerts_df)} active KCM advisories to {csv_filename}!")
    else:
        print("\nNo KCM advisories found.")