import requests
import pandas as pd
import re
import os

def parse_wsdot_date(date_string):
    """
    The WSDOT API often returns dates in a legacy Microsoft JSON format: /Date(1629837239000-0700)/
    This helper function extracts the milliseconds and converts it to a standard Pandas Datetime.
    """
    if not isinstance(date_string, str):
        return date_string
        
    match = re.search(r'\/Date\((\d+)(?:[-\+]\d+)?\)\/', date_string)
    if match:
        timestamp_ms = int(match.group(1))
        dt = pd.to_datetime(timestamp_ms, unit='ms')
        return dt.tz_localize('UTC').tz_convert('America/Los_Angeles')
    
    return pd.to_datetime(date_string, errors='ignore')

def fetch_wsdot_data(access_code):
    """
    Queries the WSDOT API for active highway alerts,
    filtering the dataset specifically for King County.
    """
    print("Fetching active highway alerts from WSDOT API...")
    
    alerts_url = "https://wsdot.wa.gov/Traffic/api/HighwayAlerts/HighwayAlertsREST.svc/GetAlertsAsJson"
    
    params = {"AccessCode": access_code}
    
    alerts_df = pd.DataFrame()
    
    # ==========================================
    # 1. FETCH & FILTER HIGHWAY ALERTS
    # ==========================================
    try:
        response = requests.get(alerts_url, params=params)
        response.raise_for_status() 
        alerts_data = response.json()
        
        if alerts_data:
            df = pd.DataFrame(alerts_data)
            
            # Clean up Dates
            if 'StartTime' in df.columns:
                df['StartTime'] = df['StartTime'].apply(parse_wsdot_date)
            if 'EndTime' in df.columns:
                df['EndTime'] = df['EndTime'].apply(parse_wsdot_date)
                
            # Filter Alerts for King County (checking multiple columns due to WSDOT blank fields)
            is_target_area = pd.Series(False, index=df.index)
            
            if 'County' in df.columns:
                is_target_area = is_target_area | df['County'].str.contains('King', case=False, na=False)
            if 'Region' in df.columns:
                is_target_area = is_target_area | df['Region'].str.contains('NW|Northwest', case=False, na=False)
            if 'HeadlineDescription' in df.columns:
                is_target_area = is_target_area | df['HeadlineDescription'].str.contains('Seattle|Bellevue|Renton|Kirkland|Redmond', case=False, na=False)
                
            # Filter for Closures, Construction, and Collisions
            if 'EventCategory' in df.columns:
                is_relevant_event = df['EventCategory'].str.contains('Clos|Construct|Collis|Road Work|Maint|Incident', case=False, na=False)
                alerts_df = df[is_target_area & is_relevant_event].copy()
            else:
                alerts_df = df[is_target_area].copy()
                
            # Sort chronological (newest first)
        if 'StartTime' in alerts_df.columns:
            alerts_df = alerts_df.sort_values(by='StartTime', ascending=False).reset_index(drop=True)
            
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP Error fetching Alerts: {http_err} - Make sure your WSDOT Access Code is valid!")
    except Exception as e:
        print(f"Error fetching Alerts: {e}")

    return alerts_df

# --- Execution ---
if __name__ == "__main__":
    # IMPORTANT: You must register for a free WSDOT Access Code 
    # at https://wsdot.wa.gov/traffic/api/ and paste it below.
    YOUR_WSDOT_ACCESS_CODE = "17f5cebc-1eb0-4f87-a7db-cd7d69806856"
    
    alerts_df = fetch_wsdot_data(YOUR_WSDOT_ACCESS_CODE)
    
    # Print and Save Alerts
    print("\n--- ACTIVE KING COUNTY HIGHWAY ALERTS ---")
    if not alerts_df.empty:
        cols_to_print = [c for c in ['EventCategory', 'HeadlineDescription', 'StartTime'] if c in alerts_df.columns]
        print(alerts_df[cols_to_print].head(5))
        
        save_path = os.path.join("KCM Input Data", "WSDOT Alerts", "wsdot_alerts_kc.csv")
        alerts_df.to_csv(save_path, index=False)
        print(f"\nSaved {len(alerts_df)} alerts to {save_path}!")
    else:
        print("No active alerts found.")