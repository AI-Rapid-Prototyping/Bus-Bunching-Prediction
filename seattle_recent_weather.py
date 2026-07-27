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
            
        # Parse the data into a list of dictionaries for Pandas
        parsed_records = []
        for hour in hourly_data:
            values = hour.get('values', {})
            parsed_records.append({
                "timestamp": hour.get('time'),
                "temperature": values.get('temperature'),
                "precipitation_intensity": values.get('precipitationIntensity'),
                "wind_speed": values.get('windSpeed'),
                "weather_code": values.get('weatherCode') # Numeric code for rain, snow, clear, etc.
            })
            
        # Convert to a Pandas DataFrame
        df = pd.DataFrame(parsed_records)
        
        # Convert timestamp strings to actual datetime objects
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
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
        # weather_df.to_csv("seattle_recent_weather.csv", index=False)
        # print("Data saved to seattle_recent_weather.csv")