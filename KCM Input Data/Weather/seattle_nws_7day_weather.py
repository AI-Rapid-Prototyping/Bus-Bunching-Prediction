import requests
import pandas as pd

def fetch_nws_7day_observations(station_id="KBFI"):
    """
    Queries the National Weather Service API for the past 7 days of weather 
    observations at a specific weather station.
    
    KBFI = Boeing Field (Seattle)
    KSEA = Sea-Tac International Airport
    """
    print(f"Fetching past 7 days of weather for station {station_id} from NWS...")
    
    url = f"https://api.weather.gov/stations/{station_id}/observations"
    
    # NWS strictly requires a User-Agent header with contact info to prevent spam.
    headers = {
        "User-Agent": "BusBunchingPredictionApp (tynan.mathieu@dot.gov)",
        "Accept": "application/geo+json"
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        
        # NWS returns a GeoJSON FeatureCollection. The weather data is inside 'features' -> 'properties'
        features = data.get('features', [])
        if not features:
            print("No observation data found.")
            return pd.DataFrame()
            
        parsed_records = []
        for feature in features:
            props = feature.get('properties', {})
            
            # Extract raw metric values (NWS defaults to Celsius, km/h, and meters)
            temp_c = props.get('temperature', {}).get('value')
            wind_kmh = props.get('windSpeed', {}).get('value')
            
            # Precipitation is tricky; it can be null if it didn't rain
            precip_data = props.get('precipitationLastHour', {})
            precip_m = precip_data.get('value') if precip_data else 0
            
            # Safely convert to Imperial (Fahrenheit, MPH, Inches)
            temp_f = (temp_c * 9/5) + 32 if temp_c is not None else None
            wind_mph = wind_kmh / 1.60934 if wind_kmh is not None else None
            precip_in = precip_m * 39.3701 if precip_m is not None else 0
            
            parsed_records.append({
                "timestamp": props.get('timestamp'),
                "temperature_f": round(temp_f, 2) if temp_f is not None else None,
                "precipitation_in": round(precip_in, 4) if precip_in is not None else 0,
                "wind_speed_mph": round(wind_mph, 2) if wind_mph is not None else None,
                "weather_description": props.get('textDescription', 'Unknown')
            })
            
        df = pd.DataFrame(parsed_records)
        
        # Convert timestamp to Datetime and align to Seattle local time
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['timestamp'] = df['timestamp'].dt.tz_convert('America/Los_Angeles')
        
        # NWS returns data from newest to oldest. We reverse it so it goes chronologically.
        df = df.sort_values(by='timestamp').reset_index(drop=True)
        
        return df
        
    else:
        print(f"Error {response.status_code}: {response.text}")
        return pd.DataFrame()

# --- Execution ---
if __name__ == "__main__":
    # Fetch 7-day history for Boeing Field (KBFI)
    nws_df = fetch_nws_7day_observations("KBFI")
    
    if not nws_df.empty:
        print("\n--- NWS 7-DAY HOURLY DATA (KBFI) ---")
        # Print the last 5 rows to see the most recent data
        print(nws_df.tail()) 
        
        # Save to CSV
        nws_df.to_csv("seattle_nws_7day_weather.csv", index=False)
        print(f"\nSuccessfully saved {len(nws_df)} hourly records to seattle_nws_7day_weather.csv!")