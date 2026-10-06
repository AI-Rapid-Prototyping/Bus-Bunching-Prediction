import requests
import pandas as pd
import datetime
import os

# Tomorrow.io Weather Code Dictionary
WEATHER_CODES = {
    1000: "Clear, Sunny",
    1100: "Mostly Clear",
    1101: "Partly Cloudy",
    1102: "Mostly Cloudy",
    1001: "Cloudy",
    2000: "Fog",
    2100: "Light Fog",
    4000: "Drizzle",
    4001: "Rain",
    4200: "Light Rain",
    4201: "Heavy Rain",
    5000: "Snow",
    5001: "Flurries",
    5100: "Light Snow",
    5101: "Heavy Snow",
    6000: "Freezing Drizzle",
    6001: "Freezing Rain",
    7000: "Ice Pellets"
}

def fetch_hourly_recent_weather(api_key, lat, lon):
    """
    Queries the Tomorrow.io API for the past 24 hours of hourly weather.
    """
    print(f"Fetching recent hourly weather for Seattle (Lat: {lat}, Lon: {lon})...")
    
    url = "https://api.tomorrow.io/v4/weather/history/recent"
    
    querystring = {
        "location": f"{lat},{lon}",
        "units": "imperial",
        "apikey": api_key
    }
    
    response = requests.get(url, params=querystring)
    
    if response.status_code == 200:
        data = response.json()
        hourly_data = data.get('timelines', {}).get('hourly', [])
        
        parsed_records = []
        for hour in hourly_data:
            values = hour.get('values', {})
            code = values.get('weatherCode')
            parsed_records.append({
                "timestamp": hour.get('time'),
                "temperature": values.get('temperature'),
                "precipitation_intensity": values.get('precipitationIntensity'),
                "wind_speed": values.get('windSpeed'),
                "weather_code": code,
                "weather_description": WEATHER_CODES.get(code, "Unknown")
            })
            
        df = pd.DataFrame(parsed_records)
        
        # Check if the DataFrame is empty before trying to convert timestamps
        if not df.empty and 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['timestamp'] = df['timestamp'].dt.tz_convert('America/Los_Angeles')
            
            # Sort descending so the newest data is at the top
            df = df.sort_values(by='timestamp', ascending=False).reset_index(drop=True)
            
        return df
    else:
        print(f"Error {response.status_code}: {response.text}")
        return pd.DataFrame()

def fetch_hourly_forecast(api_key, lat, lon):
    """
    Queries the Tomorrow.io API for the upcoming hourly weather forecast.
    """
    print(f"\nFetching hourly weather forecast for Seattle (Lat: {lat}, Lon: {lon})...")
    
    url = "https://api.tomorrow.io/v4/weather/forecast"
    
    querystring = {
        "location": f"{lat},{lon}",
        "units": "imperial",
        "apikey": api_key
    }
    
    response = requests.get(url, params=querystring)
    
    if response.status_code == 200:
        data = response.json()
        # The forecast endpoint uses the exact same JSON structure as the /recent endpoint!
        hourly_data = data.get('timelines', {}).get('hourly', [])
        
        parsed_records = []
        for hour in hourly_data:
            values = hour.get('values', {})
            code = values.get('weatherCode')
            parsed_records.append({
                "timestamp": hour.get('time'),
                "temperature": values.get('temperature'),
                "precipitation_intensity": values.get('precipitationIntensity'),
                "wind_speed": values.get('windSpeed'),
                "weather_code": code,
                "weather_description": WEATHER_CODES.get(code, "Unknown")
            })
            
        df = pd.DataFrame(parsed_records)
        
        if not df.empty and 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['timestamp'] = df['timestamp'].dt.tz_convert('America/Los_Angeles')
            
            # Sort ascending for forecasts so the immediate next hour is at the top
            df = df.sort_values(by='timestamp', ascending=True).reset_index(drop=True)
            
        return df
    else:
        print(f"Error {response.status_code}: {response.text}")
        return pd.DataFrame()

# --- Execution ---
if __name__ == "__main__":
    # Paste your API key here!
    YOUR_API_KEY = "AvJNmHVH92HCdH4bfrMsQmuzUhAmFjLr"
    
    # Coordinates for Downtown Seattle
    SEATTLE_LAT = "47.6062"
    SEATTLE_LON = "-122.3321"
    
    # 1. Fetch Hourly (Past 24h using the /recent endpoint)
    hourly_df = fetch_hourly_recent_weather(YOUR_API_KEY, SEATTLE_LAT, SEATTLE_LON)
    if not hourly_df.empty:
        print("\n--- HOURLY DATA (Past 24h) ---")
        print(hourly_df.head())
        
        # --- SMART MERGE TO CSV (NEWEST AT TOP) ---
        csv_filename = os.path.join("KCM Input Data", "Weather", "seattle_weather_history.csv")
        file_exists = os.path.isfile(csv_filename)
        
        if file_exists:
            # Read the existing CSV to find the most recent timestamp we already saved
            try:
                existing_df = pd.read_csv(csv_filename)
                if not existing_df.empty and 'timestamp' in existing_df.columns:
                    # Convert CSV strings back to tz-aware datetimes for safe comparison
                    existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'], utc=True).dt.tz_convert('America/Los_Angeles')
                    latest_saved_time = existing_df['timestamp'].max()
                    
                    # Filter our newly fetched data to ONLY keep rows newer than what we already have
                    new_data_df = hourly_df[hourly_df['timestamp'] > latest_saved_time]
                    
                    if not new_data_df.empty:
                        # Combine the old and new data
                        combined_df = pd.concat([new_data_df, existing_df], ignore_index=True)
                        
                        # Sort descending (newest at the top)
                        combined_df = combined_df.sort_values(by='timestamp', ascending=False).reset_index(drop=True)
                        
                        # Overwrite the file with the perfectly sorted combined data
                        combined_df.to_csv(csv_filename, index=False)
                        print(f"\nSuccessfully added {len(new_data_df)} NEW hours of data to {csv_filename}!")
                    else:
                        print(f"\nNo new data to add. {csv_filename} is already up to date.")
                else:
                    # If file exists but is missing data/columns, just overwrite
                    hourly_df.to_csv(csv_filename, index=False)
                    print(f"\nOverwrote {csv_filename} with {len(hourly_df)} hours of data!")
            except pd.errors.EmptyDataError:
                # If the file exists but is completely empty, we can just proceed as normal
                hourly_df.to_csv(csv_filename, index=False)
                print(f"\nPopulated empty file {csv_filename} with {len(hourly_df)} hours of data!")
        else:
            # If file doesn't exist, just save the perfectly sorted fetched data
            hourly_df.to_csv(csv_filename, index=False)
            print(f"\nCreated {csv_filename} with {len(hourly_df)} hours of data!")
            
    # 2. Fetch and Save Forecast (Overwrite Mode)
    forecast_df = fetch_hourly_forecast(YOUR_API_KEY, SEATTLE_LAT, SEATTLE_LON)
    if not forecast_df.empty:
        print("\n--- HOURLY FORECAST ---")
        print(forecast_df.head())
        
        forecast_filename = os.path.join("KCM Input Data", "Weather", "seattle_weather_forecast.csv")
        
        # We always overwrite the forecast so your model has the single most up-to-date prediction
        forecast_df.to_csv(forecast_filename, index=False)
        print(f"\nOverwrote {forecast_filename} with the latest {len(forecast_df)} hours of forecast data!")