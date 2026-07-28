import requests
import pandas as pd
import io
import os

def fetch_sfd_mvi_incidents():
    """
    Scrapes the Seattle Fire Realtime 911 dispatch log and filters for 
    Motor Vehicle Incidents (MVI) which act as proxies for severe traffic bottlenecks.
    """
    print("Fetching Seattle Fire 911 dispatch log...")
    
    url = "https://web.seattle.gov/sfd/realtime911/getRecsForDatePub.asp?action=Today&incDate=&rad1=des"
    
    # Passing a User-Agent so the city server doesn't block the request as a bot
    headers = {
        "User-Agent": "BusBunchingPredictionApp (tynan.mathieu@dot.gov)"
    }
    
    try:
        # Fetch the raw HTML of the page
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Wrap the HTML in StringIO to prevent Pandas from reading it as a file path
        tables = pd.read_html(io.StringIO(response.text))
        
        if not tables:
            print("No tables found on the page.")
            return pd.DataFrame()
            
        # The SFD page uses deeply nested HTML tables. The actual data table is separated 
        # from the headers, but it is always the longest table with exactly 6 columns.
        df = max(tables, key=len)
        
        if len(df.columns) >= 6:
            # Keep only the first 6 columns and manually assign the known headers
            df = df.iloc[:, :6]
            df.columns = ['Date/Time', 'Incident Number', 'Level', 'Units', 'Location', 'Type']
        else:
             print(f"Could not locate the incident table in the HTML. Found {len(df.columns)} columns.")
             return pd.DataFrame()

        # Just in case Pandas accidentally caught the textual headers in row 0, remove it
        if str(df.iloc[0]['Date/Time']).strip() == 'Date/Time':
            df = df[1:].reset_index(drop=True)
            
        # --- FILTERING ---
        # We want anything containing "MVI" (MVI - Motor Vehicle Incident, MVI Freeway, MVI Rescue, etc.)
        # na=False ensures it doesn't crash if it hits a blank row
        mvi_df = df[df['Type'].str.contains('MVI', case=False, na=False)].copy()
        
        # Clean up the Date/Time column into a true Pandas Datetime object
        # Note: The column is usually "Date/Time" on the SFD site.
        date_col = 'Date/Time' if 'Date/Time' in mvi_df.columns else 'Datetime'
        
        if date_col in mvi_df.columns:
            mvi_df[date_col] = pd.to_datetime(mvi_df[date_col], errors='coerce')
            
        print(f"Found {len(mvi_df)} Motor Vehicle Incidents today.")
        return mvi_df
        
    except Exception as e:
        print(f"Error fetching or parsing SFD data: {e}")
        return pd.DataFrame()

# --- Execution ---
if __name__ == "__main__":
    # Fetch today's crashes
    mvi_incidents_df = fetch_sfd_mvi_incidents()
    
    if not mvi_incidents_df.empty:
        print("\n--- RECENT SEATTLE MOTOR VEHICLE INCIDENTS ---")
        
        # Identify the correct date column name for printing
        date_col = 'Date/Time' if 'Date/Time' in mvi_incidents_df.columns else 'Datetime'
        
        # Safely select columns to print (avoids errors if a column name slightly changed)
        cols_to_print = [col for col in [date_col, 'Location', 'Type'] if col in mvi_incidents_df.columns]
        print(mvi_incidents_df[cols_to_print].head(10))
        
        # --- SMART MERGE TO CSV (NO DUPLICATES, NEWEST AT TOP) ---
        csv_filename = os.path.join("KCM Input Data", "MVI", "seattle_fire_mvi_today.csv")
        file_exists = os.path.isfile(csv_filename)
        
        if file_exists:
            try:
                # Read the existing CSV
                existing_df = pd.read_csv(csv_filename)
                
                # Combine the old and new data
                combined_df = pd.concat([mvi_incidents_df, existing_df], ignore_index=True)
                
                # Drop duplicates based on the unique 'Incident Number'
                # keeping the first occurrence (which will be from our freshly fetched mvi_incidents_df)
                combined_df = combined_df.drop_duplicates(subset=['Incident Number'], keep='first')
                
                # Ensure the Date column is a proper datetime object for accurate sorting
                if date_col in combined_df.columns:
                    combined_df[date_col] = pd.to_datetime(combined_df[date_col], errors='coerce')
                    # Sort descending (newest at the top)
                    combined_df = combined_df.sort_values(by=date_col, ascending=False).reset_index(drop=True)
                
                # Overwrite the file with the perfectly deduplicated and sorted data
                combined_df.to_csv(csv_filename, index=False)
                
                # Calculate how many NEW incidents were actually added
                new_count = len(combined_df) - len(existing_df)
                if new_count > 0:
                    print(f"\nSuccessfully added {new_count} NEW incidents to {csv_filename}!")
                else:
                    print(f"\nNo new incidents to add. {csv_filename} is already up to date.")
                    
            except pd.errors.EmptyDataError:
                mvi_incidents_df.to_csv(csv_filename, index=False)
                print(f"\nPopulated empty file {csv_filename} with {len(mvi_incidents_df)} incidents!")
        else:
            mvi_incidents_df.to_csv(csv_filename, index=False)
            print(f"\nCreated {csv_filename} with {len(mvi_incidents_df)} incidents!")