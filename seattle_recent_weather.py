import requests
import pandas as pd
import json

def fetch_weather_history(api_key, lat, lon):
    """
    Queries the Tomorrow.io API for recent historical weather observations.
    """
    print(f"Fetching recent weather history for Seattle (Lat: {lat}, Lon: {lon})...")
    
    # The Tomorrow.io V4 Recent History endpoint
    url = "https://api.tomorrow.io/v4/weather/history/recent"
    
    # Query parameters
    querystring = {
        "location": f"{lat},{lon}",
        "units": "imperial",
        "apikey": api_key
    }
    
    # Make the GET request
    response = requests.get(url, params=querystring)
    
    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        
        # Extract the hourly data array from the JSON response
        hourly_data = data.get('timelines', {}).get('hourly', [])
        
        if not hourly_data:
            print("No hourly data found in the response.")
            return pd.DataFrame()

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
            
        # Parse the data into a list of dictionaries for Pandas
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
            
        # Convert to a Pandas DataFrame
        df = pd.DataFrame(parsed_records)
        
        # Convert timestamp strings to actual datetime objects
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Convert from UTC to Seattle local time (Pacific Time)
        df['timestamp'] = df['timestamp'].dt.tz_convert('America/Los_Angeles')
        
        return df
        
    else:
        print(f"Error {response.status_code}: {response.text}")
        return pd.DataFrame()

# --- Execution ---
if __name__ == "__main__":
    # TODO: Replace with your actual Tomorrow.io API key
    YOUR_API_KEY = "AvJNmHVH92HCdH4bfrMsQmuzUhAmFjLr"
    
    # Coordinates for Downtown Seattle / King County Metro core
    SEATTLE_LAT = "47.6062"
    SEATTLE_LON = "-122.3321"
    
    # Run the function
    weather_df = fetch_weather_history(YOUR_API_KEY, SEATTLE_LAT, SEATTLE_LON)
    
    # Display the results
    if not weather_df.empty:
        print("\nSuccessfully fetched weather data!")
        print("-" * 40)
        print(weather_df.head())
        print("-" * 40)
        
        # Optional: Save to CSV to use in your model later
        weather_df.to_csv("seattle_recent_weather.csv", index=False)
        print("Data saved to seattle_recent_weather.csv")