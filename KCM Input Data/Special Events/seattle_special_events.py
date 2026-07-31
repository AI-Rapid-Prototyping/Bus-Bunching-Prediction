import os
import time
import json
from datetime import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.common.by import By
import openai

# ==========================================
# 1. SCRAPE DYNAMIC CALENDAR VIA SELENIUM
# ==========================================
def scrape_eproval_calendar():
    """
    Uses Selenium to open a headless browser, waits for the Seattle 
    Special Events RPC calendar to render, and extracts the visible text.
    """
    current_month_name = datetime.now().strftime("%B")
    print(f"Initializing headless Edge browser to scrape Seattle Special Events in {current_month_name}...")
    
    # Configure Microsoft Edge webdriver
    edge_options = EdgeOptions()
    
    # Configure headless options for background execution
    edge_options.add_argument("--headless")
    edge_options.add_argument("--window-size=1920,1080")
    edge_options.add_argument("--no-sandbox")
    edge_options.add_argument("--disable-dev-shm-usage")
    edge_options.add_argument("--disable-gpu")
    edge_options.add_argument("--log-level=3")
    
    # Start the Edge browser (Selenium 4.6+ will auto-download the correct Microsoft driver)
    driver = webdriver.Edge(options=edge_options)
    
    try:
        url = "https://eproval.seattle.gov/pages/special-events-public-calendar"
        driver.get(url)
        
        print("Waiting 8 seconds for dynamic calendar Javascript to render...")
        time.sleep(8)  # Give the RPC calls time to fetch and render the events
        
        print("Attempting to load more events via scrolling...")
        # Scroll down multiple times to trigger lazy loading if present
        last_height = driver.execute_script("return document.body.scrollHeight")
        for _ in range(5): # Try scrolling 5 times
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2) # Wait for new content to load
            
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break # Reached the bottom or no more lazy loading
            last_height = new_height

        # We grab the raw text of the entire body. 
        # Since Eproval uses complex grids, relying on an LLM to parse the raw 
        # text is much more resilient than trying to pinpoint specific HTML classes.
        body_text = driver.find_element(By.TAG_NAME, "body").text
        
        print(f"Successfully extracted {len(body_text)} characters of raw calendar text.")
        return body_text
        
    except Exception as e:
        print(f"Error scraping calendar: {e}")
        return ""
    finally:
        driver.quit()

# ==========================================
# 2. LLM ANALYSIS (OPENAI AGENT)
# ==========================================
def analyze_events_with_openai(raw_text, api_key):
    """
    Feeds the raw calendar text to OpenAI to extract the events, 
    score their traffic severity, and identify intersecting arterials.
    """
    if not raw_text.strip():
        print("No text provided to the AI Agent.")
        return []

    print("\nAwakening OpenAI Agent to analyze traffic impacts...")
    
    current_month_name = datetime.now().strftime("%B")
    current_year = datetime.now().strftime("%Y")
    
    # Configure the custom OpenAI client
    client = openai.OpenAI(
        api_key=api_key,
        base_url="http://10.75.42.137:4000/"
    )
    
    system_instruction = f"""
    You are a dual-purpose AI for King County Metro: a strict data extractor and an expert traffic analyst.
    
    CRITICAL RULES:
    1. STRICT EXTRACTION: Extract the Event Name, Date_Time, and Location STRICTLY from the user's provided raw text. Extract the date/time exactly as it appears. Do not invent events or dates. ONLY extract events occurring in {current_month_name} {current_year}.
    2. PREDICTION: For EVERY event you extract, you MUST use your own geographic and traffic knowledge to generate a "Severity_Score" (integer 1-10) and "Intersecting_Streets" (list of 1-3 street names).
    
    Return ONLY a raw JSON array of objects. Do not wrap the JSON in markdown formatting (like ```json).
    Use these exact keys: "Event_Name", "Date_Time", "Location", "Severity_Score", "Intersecting_Streets".
    """
    
    user_prompt = f"""
    Raw Calendar Text:
    {raw_text[:30000]}
    """
    
    try:
        response = client.chat.completions.create(
            model="GPT-5",
            temperature=0.0,
            seed=42,
            messages=[
                {
                    "role": "system",
                    "content": system_instruction
                },
                {
                    "role": "user",
                    "content": user_prompt
                }
            ]
        )
        response_text = response.choices[0].message.content.strip()
        
        # Clean up any accidental markdown formatting from the LLM
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.startswith("```"):
            response_text = response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]
            
        parsed_json = json.loads(response_text.strip())
        
        # Handle both raw arrays and wrapped dictionaries safely
        if isinstance(parsed_json, list):
            parsed_events = parsed_json
        else:
            parsed_events = parsed_json.get("events", [])
            
        print(f"OpenAI successfully extracted and scored {len(parsed_events)} special events!")
        return parsed_events
        
    except json.JSONDecodeError:
        print("Error: OpenAI failed to return valid JSON.")
        print("Raw output:", response_text)
        return []
    except Exception as e:
        print(f"Error communicating with OpenAI API: {e}")
        return []

# --- Execution ---
if __name__ == "__main__":
    
    # Azure OpenAI API Key and endpoint specified by the organization
    YOUR_OPENAI_API_KEY = "sk-ZSRdwtJ7Ta-LTcKTf4h72w"
    
    if YOUR_OPENAI_API_KEY == "INSERT_YOUR_OPENAI_API_KEY_HERE":
        print("CRITICAL: You must insert your OpenAI API Key into the script to run the AI Agent.")
    else:
        # 1. Scrape the raw webpage text
        raw_calendar_text = scrape_eproval_calendar()
        
        # 2. Let OpenAI analyze and score the events
        analyzed_events = analyze_events_with_openai(raw_calendar_text, YOUR_OPENAI_API_KEY)
        
        if analyzed_events:
            # Convert to a Pandas DataFrame for easy viewing and saving
            events_df = pd.DataFrame(analyzed_events)
            
            # Sort chronologically by Date/Time (Earliest at the top)
            if 'Date_Time' in events_df.columns:
                # Clean up ranges safely and let Pandas coerce any weird formats into NaT without crashing
                clean_dates = events_df['Date_Time'].apply(lambda x: str(x).split('-')[0].split(' to ')[0].strip() if pd.notna(x) else "")
                events_df['Temp_Sort_Date'] = pd.to_datetime(clean_dates, errors='coerce')
                
                # Sort by date ascending, and fall back to Severity Score for events on the same day/unknown dates
                if 'Severity_Score' in events_df.columns:
                    events_df['Severity_Score'] = pd.to_numeric(events_df['Severity_Score'], errors='coerce')
                    events_df = events_df.sort_values(by=['Temp_Sort_Date', 'Severity_Score'], ascending=[True, False]).reset_index(drop=True)
                else:
                    events_df = events_df.sort_values(by='Temp_Sort_Date', ascending=True).reset_index(drop=True)
                    
                # Drop the temporary sorting column
                events_df = events_df.drop(columns=['Temp_Sort_Date'])
            
            print("\n--- AI PREDICTED TRAFFIC BOTTLENECKS (SPECIAL EVENTS) ---")
            print(events_df[['Event_Name', 'Date_Time', 'Severity_Score', 'Intersecting_Streets']].head(10))
            
            # 3. Save to the "Special Events" folder
            save_dir = os.path.join("KCM Input Data", "Special Events")
            os.makedirs(save_dir, exist_ok=True)
            
            csv_filename = os.path.join(save_dir, "seattle_special_events_scored.csv")
            
            # We overwrite this file so it always represents the current calendar outlook
            events_df.to_csv(csv_filename, index=False)
            
            print(f"\nSuccessfully saved AI-scored events to {csv_filename}!")
        else:
            print("\nNo events were found or processed.")