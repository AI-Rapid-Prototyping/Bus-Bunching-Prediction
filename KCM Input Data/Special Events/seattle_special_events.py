import os
import time
import json
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import google.generativeai as genai

# ==========================================
# 1. SCRAPE DYNAMIC CALENDAR VIA SELENIUM
# ==========================================
def scrape_eproval_calendar():
    """
    Uses Selenium to open a headless Chrome browser, waits for the Seattle 
    Special Events RPC calendar to render, and extracts the visible text.
    """
    print("Initializing headless browser to scrape Seattle Special Events...")
    
    # Configure Chrome to run invisibly (headless)
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # Start the browser
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        url = "https://eproval.seattle.gov/pages/special-events-public-calendar"
        driver.get(url)
        
        print("Waiting 8 seconds for dynamic calendar Javascript to render...")
        time.sleep(8)  # Give the RPC calls time to fetch and render the events
        
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
# 2. LLM ANALYSIS (GEMINI AGENT)
# ==========================================
def analyze_events_with_gemini(raw_text, api_key):
    """
    Feeds the raw calendar text to Gemini to extract the events, 
    score their traffic severity, and identify intersecting arterials.
    """
    if not raw_text.strip():
        print("No text provided to the AI Agent.")
        return []

    print("\nAwakening Gemini AI Agent to analyze traffic impacts...")
    
    # Configure the Gemini API
    genai.configure(api_key=api_key)
    
    # Using the fast flash model for structured data extraction
    model = genai.GenerativeModel('gemini-2.0-flash')
    
    prompt = f"""
    You are an expert traffic analyst AI for King County Metro. 
    Below is the raw, unstructured text scraped from the Seattle Special Events Public Calendar.
    
    Your task is to extract all confirmed or upcoming special events (e.g., parades, marathons, festivals, major games).
    For each event you find, determine:
    1. Event Name
    2. Date and Time (if available)
    3. Location (Neighborhood or specific streets/parks)
    4. Severity Score (1-10): Predict the traffic bottleneck severity. 10 is a major stadium game or parade closing downtown. 1 is a small neighborhood farmers market.
    5. Intersecting Arterial Streets: Based on your geographic knowledge of Seattle and the event location, list 1 to 3 major arterial streets or highways likely to experience bus bunching.
    
    Return ONLY a valid, raw JSON array of objects. Do not wrap the JSON in markdown formatting (like ```json). Just the raw array.
    Use these exact keys: "Event_Name", "Date_Time", "Location", "Severity_Score", "Intersecting_Streets".
    
    Raw Calendar Text:
    {raw_text[:30000]} 
    """
    
    try:
        response = model.generate_content(prompt)
        response_text = response.text.strip()
        
        # Clean up any accidental markdown formatting from the LLM
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.startswith("```"):
            response_text = response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]
            
        parsed_events = json.loads(response_text.strip())
        print(f"Gemini successfully extracted and scored {len(parsed_events)} special events!")
        return parsed_events
        
    except json.JSONDecodeError:
        print("Error: Gemini failed to return valid JSON.")
        print("Raw output:", response.text)
        return []
    except Exception as e:
        print(f"Error communicating with Gemini API: {e}")
        return []

# --- Execution ---
if __name__ == "__main__":
    
    # IMPORTANT: Paste your Google Gemini API Key here
    # Get a free key at: https://aistudio.google.com/app/apikey
    YOUR_GEMINI_API_KEY = "INSERT_YOUR_GEMINI_API_KEY_HERE"
    
    if YOUR_GEMINI_API_KEY == "INSERT_YOUR_GEMINI_API_KEY_HERE":
        print("CRITICAL: You must insert your Gemini API Key into the script to run the AI Agent.")
    else:
        # 1. Scrape the raw webpage text
        raw_calendar_text = scrape_eproval_calendar()
        
        # 2. Let Gemini analyze and score the events
        analyzed_events = analyze_events_with_gemini(raw_calendar_text, YOUR_GEMINI_API_KEY)
        
        if analyzed_events:
            # Convert to a Pandas DataFrame for easy viewing and saving
            events_df = pd.DataFrame(analyzed_events)
            
            # Sort by Severity Score (Highest first) so the worst bottlenecks are at the top
            if 'Severity_Score' in events_df.columns:
                events_df['Severity_Score'] = pd.to_numeric(events_df['Severity_Score'], errors='coerce')
                events_df = events_df.sort_values(by='Severity_Score', ascending=False).reset_index(drop=True)
            
            print("\n--- AI PREDICTED TRAFFIC BOTTLENECKS (SPECIAL EVENTS) ---")
            print(events_df[['Event_Name', 'Severity_Score', 'Intersecting_Streets']].head(10))
            
            # 3. Save to the "Special Events" folder you requested
            save_dir = os.path.join("KCM Input Data", "Special Events")
            os.makedirs(save_dir, exist_ok=True)
            
            csv_filename = os.path.join(save_dir, "seattle_special_events_scored.csv")
            
            # We overwrite this file so it always represents the current calendar outlook
            events_df.to_csv(csv_filename, index=False)
            
            print(f"\nSuccessfully saved AI-scored events to {csv_filename}!")
        else:
            print("\nNo events were found or processed.")