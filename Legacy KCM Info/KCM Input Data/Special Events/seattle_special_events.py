import os
import time
import json
from datetime import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import openai

# ==========================================
# 1. SCRAPE DYNAMIC CALENDAR VIA SELENIUM
# ==========================================
def scrape_eproval_calendar_today(max_retries=3):
    """
    Uses Selenium to open a browser, waits for the Seattle 
    Special Events React app to render, clicks the 'Day' view to isolate 
    today's events, and extracts the visible text. 
    Includes an automatic retry mechanism for stability.
    """
    current_date_str = datetime.now().strftime("%B %d, %Y")
    
    # Configure Microsoft Edge webdriver
    edge_options = EdgeOptions()
    
    # CRITICAL STEALTH UPGRADE: 
    # Use '--headless=new' which runs the modern headless engine. It acts identically 
    # to a physical browser and avoids the classic Cloudflare headless traps.
    edge_options.add_argument("--headless=new")
    edge_options.add_argument("--window-size=1920,1080")
    
    # Strip the "automated test software" banners and flags NATIVELY so we don't 
    # trigger Javascript tampering detectors (like the CDP hack did).
    edge_options.add_argument("--disable-blink-features=AutomationControlled")
    edge_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    edge_options.add_experimental_option("useAutomationExtension", False)
    
    edge_options.add_argument("--no-sandbox")
    edge_options.add_argument("--disable-dev-shm-usage")
    edge_options.add_argument("--disable-gpu")
    edge_options.add_argument("--log-level=3")
    
    # Loop to allow for retries in case the React app fails to render properly
    for attempt in range(1, max_retries + 1):
        print(f"\n--- Scraping Attempt {attempt} of {max_retries} for TODAY ({current_date_str}) ---")
        driver = webdriver.Edge(options=edge_options)
        
        try:
            url = "https://eproval.seattle.gov/pages/special-events-public-calendar"
            driver.get(url)
            
            print("Waiting dynamically for the React application to render the calendar grid...")
            
            # Wait up to 30 seconds for the structural calendar text to appear in the DOM.
            # We wait for "Legend" or "TODAY" to confirm the React API call succeeded and bypassed the firewall.
            WebDriverWait(driver, 30).until(
                lambda d: "Legend" in d.find_element(By.TAG_NAME, "body").text or "TODAY" in d.find_element(By.TAG_NAME, "body").text
            )
            
            print("Calendar grid rendered successfully! Locating the 'Day' view button...")
            time.sleep(2)  # Give the DOM a moment to settle animations
            
            # Bulletproof fallback: Find any button on the page that says "Day" or "D" safely
            js_click_code = """
            var btns = Array.from(document.querySelectorAll('button, .btn'));
            var dayBtn = btns.find(el => {
                if (!el.textContent) return false;
                var txt = el.textContent.trim().toLowerCase();
                return txt === 'day' || txt === 'd';
            });
            if (dayBtn) {
                dayBtn.click();
                return true;
            }
            return false;
            """
            clicked = driver.execute_script(js_click_code)
            
            if clicked:
                print("Successfully clicked the 'Day' view button via JS text search!")
            else:
                raise ValueError("The 'Day' button could not be found on the page.")
            
            # Give the day view time to fetch the specific daily events from the server
            print("Waiting 5 seconds for the daily events to load...")
            time.sleep(5)
            
            print("Extracting visible text for today's events...")
            body_text = driver.find_element(By.TAG_NAME, "body").text
            
            # Basic validation: If it's too short, it's likely a blocked page or empty shell
            if len(body_text) < 200:
                raise ValueError(f"Extracted text too short ({len(body_text)} chars). Suspected blocked page or failed load.")
                
            print(f"Successfully extracted {len(body_text)} characters of raw calendar text.")
            driver.quit() # Clean up before returning success
            return body_text
            
        except Exception as e:
            print(f"Attempt {attempt} failed: {e}")
            
            try:
                # Print the exact text the browser saw so we can diagnose Cloudflare vs Loading errors
                print("\n--- WHAT THE BROWSER SAW AT FAILURE ---")
                fail_text = driver.find_element(By.TAG_NAME, "body").text
                print(fail_text[:800])
                print("---------------------------------------\n")
            except:
                pass
            
            driver.quit() # Ensure the failed driver is destroyed so it doesn't leak memory
            
            if attempt < max_retries:
                print("Waiting 5 seconds before retrying...")
                time.sleep(5)
            else:
                print("\nCRITICAL: Maximum retries reached. Unable to scrape Eproval Calendar today.")
                return ""

# ==========================================
# 2. LLM ANALYSIS (OPENAI AGENT)
# ==========================================
def analyze_events_with_openai(raw_text, api_key):
    """
    Feeds the raw calendar text to OpenAI to extract today's events, 
    score their traffic severity, and identify intersecting arterials.
    """
    # If we get a blank page or an access denied shell, abort
    if not raw_text.strip() or len(raw_text) < 200:
        print("No valid text provided to the AI Agent.")
        return []

    print("\nAwakening OpenAI Agent to analyze traffic impacts...")
    
    current_date = datetime.now().strftime("%B %d, %Y")
    
    # Configure the custom OpenAI client
    client = openai.OpenAI(
        api_key=api_key,
        base_url="http://10.75.42.137:4000/"
    )
    
    system_instruction = f"""
    You are an expert data extractor and traffic analyst for King County Metro.
    
    CRITICAL RULES:
    1. STRICT EXTRACTION: Extract events STRICTLY from the provided calendar text. The text is scraped from a 'Day View' calendar grid. Only extract the events, ignore navigation buttons like 'MONTH WEEK DAY LIST'.
    2. DATE FORMATTING: The events are for TODAY. You MUST set the 'Date' field explicitly to '{current_date}'. 
    3. TIME FORMATTING: Extract the 'Start_Time' and 'End_Time' exactly as they appear (e.g., '10:00 AM', '4:30 PM'). If a time is not listed or it spans multiple days, output 'Unknown' or 'All Day'.
    4. PREDICTION: For EVERY event, use your geographic knowledge of Seattle to generate a 'Severity_Score' (integer 1-10) and 'Intersecting_Streets' (list of 1-3 major street names).
    
    Return ONLY a raw JSON array of objects. Do not wrap the JSON in markdown formatting (like ```json).
    Use these exact keys: "Event_Name", "Date", "Start_Time", "End_Time", "Location", "Severity_Score", "Intersecting_Streets".
    """
    
    user_prompt = f"""
    Raw Calendar Text for {current_date}:
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
            
        print(f"OpenAI successfully extracted and scored {len(parsed_events)} special events for today!")
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
        # 1. Scrape the raw webpage text (Day View) using vanilla Edge Selenium
        raw_calendar_text = scrape_eproval_calendar_today()
        
        # 2. Let OpenAI analyze and score the events
        analyzed_events = analyze_events_with_openai(raw_calendar_text, YOUR_OPENAI_API_KEY)
        
        if analyzed_events:
            # Convert to a Pandas DataFrame
            events_df = pd.DataFrame(analyzed_events)
            
            print("\n--- AI PREDICTED TRAFFIC BOTTLENECKS (SPECIAL EVENTS) ---")
            
            # Safely select columns to print
            cols_to_print = [c for c in ['Event_Name', 'Start_Time', 'End_Time', 'Severity_Score', 'Intersecting_Streets'] if c in events_df.columns]
            print(events_df[cols_to_print].head(10))
            
            # 3. Save and APPEND to the local "Special Events" folder
            script_dir = os.path.dirname(os.path.abspath(__file__))
            csv_filename = os.path.join(script_dir, "seattle_special_events_scored.csv")
            
            file_exists = os.path.isfile(csv_filename)
            
            if file_exists:
                try:
                    # Read the historical archive
                    existing_df = pd.read_csv(csv_filename)
                    
                    # Combine old data with today's new data
                    combined_df = pd.concat([existing_df, events_df], ignore_index=True)
                    
                    # Drop duplicates in case the script is run multiple times on the same day
                    # We deduplicate based on Event_Name and Date
                    combined_df = combined_df.drop_duplicates(subset=['Event_Name', 'Date'], keep='last')
                    
                    # Sort chronologically by date
                    combined_df['Temp_Sort_Date'] = pd.to_datetime(combined_df['Date'], errors='coerce')
                    combined_df = combined_df.sort_values(by='Temp_Sort_Date', ascending=False).reset_index(drop=True)
                    combined_df = combined_df.drop(columns=['Temp_Sort_Date'])
                    
                    combined_df.to_csv(csv_filename, index=False)
                    
                    # Calculate how many truly new events were appended
                    new_count = len(combined_df) - len(existing_df)
                    print(f"\nSuccessfully processed updates! {csv_filename} now has {len(combined_df)} total recorded events.")
                    if new_count > 0:
                        print(f"Added {new_count} NEW events to the historical record.")
                    else:
                        print("No new events added (already recorded today).")
                        
                except pd.errors.EmptyDataError:
                    # If the file exists but is empty, just save the new data
                    events_df.to_csv(csv_filename, index=False)
                    print(f"\nPopulated empty file {csv_filename} with {len(events_df)} events!")
            else:
                # If the file doesn't exist at all yet, create it
                events_df.to_csv(csv_filename, index=False)
                print(f"\nCreated local file {csv_filename} with {len(events_df)} events!")
                
        else:
            print("\nNo events were found or processed for today.")