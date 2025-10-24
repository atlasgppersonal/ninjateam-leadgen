import sys
import json
import asyncio
import time
import re
import os
import sqlite3
import psutil
import subprocess
try:
    import google.generativeai as genai
    import phonenumbers
    from playwright.async_api import async_playwright
    import httpx # New: Import httpx
    from category_normalizer import normalize_business_category # New: Import category normalizer
except ImportError as e:
    print(f"Missing dependency: {e}")
    sys.exit(1)

# Redirect stdout to a log file
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "contact_extractor_log.txt")
class Logger(object):
    def __init__(self, filename):
        self.terminal = sys.__stdout__  # original console
        self.log = open(filename, "w", buffering=1)

    def write(self, message):
        self.terminal.write(message)   # write to console
        self.log.write(message)        # also write to file

    def flush(self):
        self.terminal.flush()
        self.log.flush()

log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "contact_extractor_log.txt")
sys.stdout = Logger(log_file_path)
sys.stderr = sys.stdout
print(f"--- Log started at {time.ctime()} ---")
print(f"--- All output redirected to {log_file_path} ---")

# Cloud Function URLs
RECEIVE_LEAD_DATA_URL = "https://us-central1-fourth-outpost-470409-u3.cloudfunctions.net/receiveLeadData"
ADD_LEAD_TO_SMS_QUEUE_URL = "https://us-central1-fourth-outpost-470409-u3.cloudfunctions.net/addLeadToSMSQueue"

# --- ============================== ---
# --- --- HELPER FUNCTIONS --- ---
# --- ============================== ---

def normalize_phone_number(phone):
    if not phone: return None
    cleaned = re.sub(r'\D', '', phone)
    if len(cleaned) == 11 and cleaned.startswith('1'):
        cleaned = cleaned[1:]
    return cleaned if len(cleaned) == 10 else None

def remove_emojis(text: str) -> str:
    if not text: return ""
    emoji_pattern = re.compile(r'['
                               u'\U0001F600-\U0001F64F\U0001F300-\U0001F5FF'
                               u'\U0001F680-\U0001F6FF'
                               u'\u2600-\u26FF\u2700-\u27BF\u2580-\u259F\uFE0F\u3030'
                               ']+', flags=re.UNICODE)
    return emoji_pattern.sub(r'', text).strip()

def extract_and_normalize_phone_numbers(text: str) -> list:
    """
    Extracts and normalizes all valid 10-digit US phone numbers from a given text using phonenumbers.
    """
    if not text:
        return []
    
    found_numbers = set()
    # Use PhoneNumberMatcher to find all possible phone numbers in the text
    for match in phonenumbers.PhoneNumberMatcher(text, "US"): # Assuming US numbers primarily
        try:
            # Parse the matched number
            parsed_number = match.number
            
            # Check if it's a valid number and is a mobile or fixed-line number (not premium, etc.)
            if phonenumbers.is_valid_number(parsed_number) and \
               phonenumbers.is_possible_number(parsed_number):
                
                # Format to E.164 (e.g., +12223334444) and then extract just the 10 digits
                # This handles various input formats and normalizes them
                e164_format = phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)
                
                # Remove country code (+1) and any non-digit characters to get 10 digits
                cleaned_number = re.sub(r'\D', '', e164_format)
                if len(cleaned_number) == 11 and cleaned_number.startswith('1'):
                    cleaned_number = cleaned_number[1:] # Remove leading '1' for US numbers
                
                if len(cleaned_number) == 10: # Ensure it's a 10-digit number
                    found_numbers.add(cleaned_number)
        except Exception as e:
            # Log or handle parsing errors if necessary
            print(f"Error parsing potential phone number: {match.raw_string} - {e}")
    
    return list(found_numbers)


# --- ============================== ---
# --- --- CORE ARCHITECTURAL LOGIC --- ---
# --- ============================== ---

async def send_to_server(lead_data):
    """Sends the finalized lead data to the receiveLeadData cloud function using httpx."""
    print("    [Server Sync] Attempting to send lead data to receiveLeadData cloud function...")
    
    # Construct payload with all available relevant data
    payload = {
        "phone": lead_data.get("phone"),
        "name": lead_data.get("name"),
        "email": lead_data.get("email"),
        "source_url": lead_data.get("url"), # 'url' in lead_data corresponds to 'source_url'
        "image_hash": lead_data.get("image_hash"),
        "business_name": lead_data.get("business_name"),
        "category": lead_data.get("category"),
        "services_rendered": lead_data.get("services_rendered"),
        "raw_contact": lead_data # Keep raw_contact for completeness
    }
    print(f"    [Server Sync] Payload being sent to receiveLeadData: {json.dumps(payload, indent=2)}")

    try:
        async with httpx.AsyncClient() as client: # Use httpx.AsyncClient
            response = await client.post(RECEIVE_LEAD_DATA_URL, json=payload, timeout=30.0) # Use json=payload for automatic JSON encoding, set timeout

        if response.is_success: # httpx uses is_success for 2xx status codes
            try:
                response_json = response.json()
                print(f"    [Server Sync] SUCCESS: Lead data sent to receiveLeadData. Status: {response.status_code}, Response: {json.dumps(response_json, indent=2)}")
                contact_id = response_json.get("contactId")
                if not contact_id:
                    print(f"    [Server Sync] WARNING: receiveLeadData successful but 'contactId' missing from response. Response: {json.dumps(response_json, indent=2)}")
                    return None
                return contact_id # Return contactId for further use
            except json.JSONDecodeError:
                print(f"    [Server Sync] FAILED: receiveLeadData returned non-JSON response. Status: {response.status_code}, Body: {response.text}")
                return None
        else:
            print(f"    [Server Sync] FAILED: receiveLeadData returned an error. Status: {response.status_code}, Body: {response.text}")
            return None

    except httpx.RequestError as e:
        print(f"!!! [Server Sync] An HTTPX request error occurred during the network request to receiveLeadData: {e}")
        return None
    except Exception as e:
        print(f"!!! [Server Sync] An unexpected error occurred during the network request to receiveLeadData: {e}")
        return None

async def add_lead_to_sms_queue_cloud_function(lead_id, phone, campaign_id, lead_data):
    """Sends lead data to the addLeadToSMSQueue cloud function using httpx."""
    if lead_data.get('cant_text') == 0:
        print(f"    [SMS Queue] Lead {phone} has 'cant_text' set to 0. Skipping adding to SMS queue.")
        return False
    print("    [SMS Queue] Attempting to send lead data to addLeadToSMSQueue cloud function...")
    
    payload = {
        "data": {
            "leadId": lead_id,
            "phone": phone,
            "campaignId": campaign_id
        }
    }

    try:
        async with httpx.AsyncClient() as client: # Use httpx.AsyncClient
            response = await client.post(ADD_LEAD_TO_SMS_QUEUE_URL, json=payload, timeout=30.0) # Use json=payload for automatic JSON encoding, set timeout

        if response.is_success: # httpx uses is_success for 2xx status codes
            response_json = response.json()
            print(f"    [SMS Queue] SUCCESS: Lead data sent to addLeadToSMSQueue. Status: {response.status_code}, Response: {response_json}")
            return True
        else:
            print(f"    [SMS Queue] FAILED: addLeadToSMSQueue returned an error. Status: {response.status_code}, Body: {response.text}")
            return False

    except httpx.RequestError as e:
        print(f"!!! [SMS Queue] An HTTPX request error occurred during the network request to addLeadToSMSQueue: {e}")
        return False
    except Exception as e:
        print(f"!!! [SMS Queue] An unexpected error occurred during the network request to addLeadToSMSQueue: {e}")
        return False


async def finalize_and_queue_lead(lead_data, config, template_id, leads_processed_counter):
    """The single, unified function to finalize a lead using robust context managers."""
    phone = lead_data.get('phone')
    print(f"    [Finalizer] Finalizing lead for phone: {phone}")
    
    producer_cfg = config['producer_settings']
    master_db = producer_cfg['master_database_file']
    queue_db = producer_cfg['queue_database_file']
    city = producer_cfg['craigslist_sub_domain']
    timezone = config['city_profiles'][city]['tz']
    state = config['city_profiles'][city].get('state') # Get state from config

    # Add city and state to lead_data for raw_contact
    lead_data['city'] = city
    if state:
        lead_data['state'] = state
    
    # Determine if this is a re-finalization (e.g., from CAPTCHA resolution)
    is_refinalization = False
    with sqlite3.connect(master_db) as con:
        cur = con.cursor()
        cur.execute("SELECT status FROM contacts WHERE phone = ?", (phone,))
        existing_status = cur.fetchone()
        if existing_status and existing_status[0] in ['pending captcha', 'email_found']:
            is_refinalization = True

    # 1. Save to local master database (update status to 'processed')
    try:
        with sqlite3.connect(master_db) as con:
            cur = con.cursor()
            cur.execute("""
                INSERT OR REPLACE INTO contacts (phone, name, email, last_sent, source_url, image_hash, business_name, category, services_rendered, status, city, lead_data_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                phone, lead_data.get("name"), lead_data.get("email"), "QUEUED", lead_data.get("url"), 
                lead_data.get("image_hash"), lead_data.get("business_name"), lead_data.get("category"), json.dumps(lead_data.get("services_rendered", [])),
                "processed", city, json.dumps(lead_data)
            ))
        print(f"    -> Saved to master_contacts.db with status 'processed'.")
    except sqlite3.Error as e:
        print(f"    -> ERROR saving to master_contacts.db: {e}")
        return # Exit if DB save fails

    # 2. Send to remote server (receiveLeadData)
    contact_id = None
    print(f"    [Finalizer] Attempting to send lead {phone} to remote server...")
    contact_id = await send_to_server(lead_data)
    if contact_id:
        print(f"    [Finalizer] Successfully sent lead {phone} to remote server. Contact ID: {contact_id}")
    else:
        print(f"    [Finalizer] Failed to send lead {phone} to remote server.")

    # 3. Handle SMS Queue if template_id is 99
    if template_id == 99 and contact_id and phone:
        print(f"    [Finalizer] Lead for SMS campaign (template_id=99). Adding to SMS queue.")
        await add_lead_to_sms_queue_cloud_function(contact_id, phone, template_id, lead_data)
        await asyncio.sleep(5) # Add a 5-second delay after sending SMS to the server

    # 4. Queue for the consumer (email/manual) - ONLY if template_id is NOT 99
    if template_id != 99 and lead_data.get('email'):
        try:
            with sqlite3.connect(queue_db) as con:
                cur = con.cursor()
                cur.execute("""
                    INSERT INTO email_queue (template_id, city, timezone, lead_data_json)
                    VALUES (?, ?, ?, ?)
                """, (template_id, city, timezone, json.dumps(lead_data)))
            print(f"    -> Lead added to email_queue.db with Template ID {template_id}.")
        except sqlite3.Error as e:
            print(f"    -> ERROR adding lead to email_queue.db: {e}")
    elif template_id == 99:
        print(f"    -> Lead for phone: {phone} (Template ID 99) - NOT added to email_queue.db as per SMS-only rule.")
    
    print(f"    [Finalizer] Summary for {phone}: Status: 'processed'. Email found: {'Yes' if lead_data.get('email') else 'No'}.")


async def enrich_batch_with_llm(model, batch_data, prompt_template):
    print(f"    [LLM Batch] Sending batch of {len(batch_data)} posts to LLM...")
    try:
        llm_input = [{"post_id": post.get("post_id"), "post_body": post.get("body_text")} for post in batch_data]
        batch_data_json = json.dumps(llm_input, indent=2)
        prompt = prompt_template.format(batch_data_json=batch_data_json)
        
        response = await model.generate_content_async(prompt)
        
        # Add robust response validation
        if response is None:
            print(f"!!! [LLM Batch] CRITICAL ERROR: LLM response object is None.")
            return None
        
        raw_response_text = response.text
        if not raw_response_text:
            print(f"!!! [LLM Batch] CRITICAL ERROR: LLM response text is empty.")
            return None

        print(f"    [LLM Batch] Raw LLM Response: \n{raw_response_text[:1000]}...") # Print first 1000 chars
        
        cleaned_text = raw_response_text.strip().replace("```json", "").replace("```", "").strip()
        enriched_results = json.loads(cleaned_text)
        print(f"    [LLM Batch] Successfully received and parsed {len(enriched_results)} results from LLM.")
        return enriched_results
    except json.JSONDecodeError as e:
        print(f"!!! [LLM Batch] CRITICAL ERROR: Failed to decode JSON from LLM response. Error: {e}")
        print(f"    Raw Response Text: {raw_response_text[:500] if 'raw_response_text' in locals() else 'No text found'}...")
        return None
    except Exception as e:
        print(f"!!! [LLM Batch] CRITICAL ERROR during LLM call: {e}")
        return None


async def scrape_website_body(url, browser_instance):
    """
    Scrapes the inner text of the <body> element of a given URL, waiting for network idle.
    """
    print(f"    [Website Scraper] Scraping website body for {url}")
    page = None
    try:
        page = await browser_instance.new_page()
        await page.goto(url, wait_until="networkidle", timeout=60000) # Increased timeout for websites
        body_text = await page.locator("body").inner_text(timeout=30000) # Get inner text of body
        return remove_emojis(body_text)
    except Exception as e:
        print(f"!!! [Website Scraper] FAILED to scrape {url}. Reason: {e}")
        return None
    finally:
        if page:
            await page.close()

async def scrape_post_body(headless_browser, post_data):
    url = post_data.get("url")
    print(f"    [Scraper] Scraping body for {url}")
    page = None
    try:
        page = await headless_browser.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        body_text = await page.locator("#postingbody").inner_text(timeout=15000)
        post_data["body_text"] = remove_emojis(body_text)
        return post_data
    except Exception as e:
        print(f"!!! [Scraper] FAILED to scrape {url}. Reason: {e}")
        post_data["body_text"] = None
        return post_data
    finally:
        if page:
            await page.close()


async def harvester(url_reservoir, headless_browser, config, harvester_finished_event):
    print("    [Harvester] Starting a new run...")
    print(f"    [Harvester] Initializing Playwright for Harvester...")
    page = None
    try:
        producer_cfg = config['producer_settings']
        master_db = producer_cfg['master_database_file']
        with sqlite3.connect(master_db) as con:
            cur = con.cursor()
            cur.execute("SELECT image_hash FROM contacts WHERE image_hash IS NOT NULL")
            processed_hashes = {row[0] for row in cur.fetchall()}
        print(f"    [Harvester] Loaded {len(processed_hashes)} existing image hashes from DB for duplicate checking.")
        
        state = {"min_post_id": None, "category_map": {}, "image_hash_map": {}, "raw_posts": []}
        
        page = await headless_browser.new_page()
        page.on("response", lambda response: asyncio.create_task(handle_search_response(response, state)))
        # Use the dynamically set sub_domain from config
        sub_domain = config['producer_settings']['craigslist_sub_domain'] 
        base_url = producer_cfg['craigslist_base_url']
        search_path = producer_cfg['craigslist_services_path']
        search_url = f"https://{sub_domain}.{base_url}{search_path}"
        print(f"    [Harvester] >>> Navigating to primary search URL: {search_url}")
        await page.goto(search_url, wait_until="domcontentloaded")
        for i in range(15):
            print(f"    [Harvester] Scrolling... ({i+1}/15)")
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2)
        await page.close()
        print(f"    [Harvester] Network scan complete. Found a total of {len(state['raw_posts'])} raw posts from traffic.")
        print(f"    [Harvester] Now filtering and processing raw posts...")
        sorted_posts = sorted(state["raw_posts"], key=lambda x: x.get("post_id_num", 0), reverse=True)
        urls_added, filtered_count, skipped_count = 0, 0, 0
        for post_data in sorted_posts:
            image_hash = state['image_hash_map'].get(post_data['base_id'])
            post_id = post_data.get('post_id_num', 'N/A')
            if image_hash and image_hash in processed_hashes:
                print(f"    -> [FILTER] Skipping post {post_id} (Image hash '{image_hash}' already processed).")
                filtered_count += 1
                continue
            category_id = state['category_map'].get(post_data['base_id'])
            if not category_id:
                print(f"    -> [SKIP] Skipping post {post_id} (Missing category ID).")
                skipped_count += 1
                continue
            abbr, cat_name = producer_cfg['category_mapping'].get(str(category_id), (None, "General"))
            
            # New filter for unwanted categories (hws and cps)
            unwanted_categories = ["hws", "cps"]
            if abbr in unwanted_categories:
                print(f"    -> [FILTER] Skipping post {post_id} (Category '{abbr}' is an unwanted category).")
                filtered_count += 1
                continue
            
            url_slug = post_data.get('url_slug')
            if abbr and url_slug:
                full_url = f"https://{sub_domain}.{base_url}/{abbr}/d/{url_slug}/{post_id}.html"
                print(f"    -> [NEW] Adding URL to reservoir: {full_url}")
                await url_reservoir.put({"url": full_url, "post_id": str(post_id), "image_hash": image_hash, "original_category": cat_name})
                urls_added += 1
            else:
                print(f"    -> [SKIP] Skipping post {post_id} (Missing URL slug or category abbreviation). Slug: '{url_slug}', Abbr: '{abbr}'.")
                skipped_count += 1
        print(f"    [Harvester] Finished. Scanned {len(state['raw_posts'])} raw posts. Filtered {filtered_count} duplicates. Skipped {skipped_count} due to data issues. Added {urls_added} new URLs to the reservoir. Current reservoir size: {url_reservoir.qsize()}")
    finally:
        if page:
            await page.close()
        harvester_finished_event.set()


async def handle_search_response(response, state):
    try:
        url = response.url
        if "full?batch" in url and "0-360" not in url:
            if state["min_post_id"]: return
            data = await response.json()
            state["min_post_id"] = data['data']['decode']['minPostingId']
            for item in data['data']['items']:
                base_id = item[0]
                if len(item) > 2: state["category_map"][base_id] = item[2]
                if len(item) > 5 and item[5] and item[5] != '0': state["image_hash_map"][base_id] = item[5]
        elif "batch?batch" in url:
            if not state["category_map"]: return
            data = await response.json()
            for item in data.get('data', {}).get('batch', []):
                try:
                    base_id, url_slug = item[0], next((el[1] for el in item if isinstance(el, list) and len(el) == 2 and el[0] == 6), None)
                    post_id_num = state['min_post_id'] + base_id
                    state["raw_posts"].append({"base_id": base_id, "url_slug": url_slug, "post_id_num": post_id_num})
                except (IndexError, TypeError): continue
    except Exception as e:
        print(f"!!! [Harvester/Network] ERROR parsing response from {response.url}. Error: {e}")


async def processor(url_reservoir, captcha_queue, zero_effort_queue, config, headless_browser, harvester_finished_event, leads_processed_counter, template_id):
    print("[Processor] Starting...")
    llm_batch_size = int(config['producer_settings'].get('llm_batch_size', 5))
    enrich_prompt = config['producer_settings']['enrich_lead_batch']
    website_scoring_prompt = config['producer_settings']['website_scoring_prompt'] # New prompt for website scoring
    
    while True:
        try:
            # Check if target number of leads has been processed
            if leads_processed_counter[0] >= config['target_count']:
                print(f"[Processor] Target leads ({config['target_count']}) processed. Exiting processor loop.")
                break

            batch_to_process = []
            if harvester_finished_event.is_set() and url_reservoir.empty():
                print("[Processor] Harvester is finished and URL reservoir is empty. Processor is pausing.")
                await asyncio.sleep(60)
                continue
            
            # Try to get items from url_reservoir up to llm_batch_size
            while len(batch_to_process) < llm_batch_size:
                try:
                    print(f"[Processor] Attempting to get item from URL reservoir. Current size: {url_reservoir.qsize()}")
                    item = await asyncio.wait_for(url_reservoir.get(), timeout=5.0) # Increased timeout for visibility
                    batch_to_process.append(item)
                    url_reservoir.task_done()
                    print(f"[Processor] Got item from URL reservoir. Batch size: {len(batch_to_process)}")
                except asyncio.TimeoutError:
                    print("[Processor] No more items in URL reservoir for now. Breaking batch collection.")
                    break # No more items in reservoir for now
            
            # Check for leads with 'email_found' status from CAPTCHA resolution
            leads_from_captcha = []
            with sqlite3.connect(config['producer_settings']['master_database_file']) as con:
                cur = con.cursor()
                cur.execute("SELECT phone, name, email, source_url, image_hash, business_name, category, services_rendered, city, lead_data_json FROM contacts WHERE status = 'email_found' LIMIT ?", (llm_batch_size,))
                raw_leads = cur.fetchall()
                for row in raw_leads:
                    lead_data = {
                        "phone": row[0], "name": row[1], "email": row[2], "url": row[3],
                        "image_hash": row[4], "business_name": row[5], "category": row[6],
                        "services_rendered": json.loads(row[7]), "city": row[8],
                        "lead_data_json": json.loads(row[9]) # Original lead_data_json
                    }
                    leads_from_captcha.append(lead_data)
            
            if leads_from_captcha:
                print(f"[Processor] Found {len(leads_from_captcha)} leads with 'email_found' status from CAPTCHA resolution.")
                for lead in leads_from_captcha:
                    if leads_processed_counter[0] >= config['target_count']:
                        print(f"    -> [TRIAGE] Target leads ({config['target_count']}) already processed. Stopping processing of email_found leads.")
                        break # Stop processing email_found leads if target is met
                    await finalize_and_queue_lead(lead, config, template_id, leads_processed_counter)
                # No counter increment here, as these leads were already counted when initially sent to CAPTCHA.
                
                if leads_processed_counter[0] >= config['target_count']:
                    print(f"[Processor] Target leads ({config['target_count']}) processed after handling email_found leads. Exiting processor loop.")
                    break # Exit processor loop if target is met after handling email_found leads

                continue # Continue to next iteration to check for more leads

            # Original logic for scraping and LLM enrichment
            batch_to_process = []
            if harvester_finished_event.is_set() and url_reservoir.empty():
                print("[Processor] Harvester is finished and URL reservoir is empty. Processor is pausing.")
                await asyncio.sleep(60)
                continue
            
            # Try to get items from url_reservoir up to llm_batch_size
            while len(batch_to_process) < llm_batch_size:
                try:
                    print(f"[Processor] Attempting to get item from URL reservoir. Current size: {url_reservoir.qsize()}")
                    item = await asyncio.wait_for(url_reservoir.get(), timeout=5.0) # Increased timeout for visibility
                    batch_to_process.append(item)
                    url_reservoir.task_done()
                    print(f"[Processor] Got item from URL reservoir. Batch size: {len(batch_to_process)}")
                except asyncio.TimeoutError:
                    print("[Processor] No more items in URL reservoir for now. Breaking batch collection.")
                    break # No more items in reservoir for now
            
            if not batch_to_process:
                print("[Processor] No items collected for batch. Checking harvester status and reservoir again.")
                if harvester_finished_event.is_set() and url_reservoir.empty():
                    print("[Processor] Harvester is finished and URL reservoir is empty. Processor is exiting.")
                    break # Exit if harvester is done and no more URLs
                await asyncio.sleep(5) # Wait a bit before retrying if no batch was formed
                continue
            
            print(f"\n[Processor] Starting new batch of {len(batch_to_process)} posts for scraping.")
            scraping_tasks = [scrape_post_body(headless_browser, post) for post in batch_to_process]
            scraped_posts_results = await asyncio.gather(*scraping_tasks)
            
            successfully_scraped_posts = [post for post in scraped_posts_results if post.get("body_text")]
            if not successfully_scraped_posts:
                print("[Processor] All scrapes in batch failed or returned empty body. Starting new batch.")
                continue
            print(f"[Processor] Successfully scraped {len(successfully_scraped_posts)} posts.")

            # Phase 1: Programmatic Phone Number Extraction and Duplicate Check
            posts_for_llm_enrichment = []
            with sqlite3.connect(config['producer_settings']['master_database_file']) as con:
                cur = con.cursor()
                for post in successfully_scraped_posts:
                    post_id = post['post_id']
                    extracted_phones = extract_and_normalize_phone_numbers(post['body_text'])
                    
                    is_duplicate_programmatic = False
                    for phone in extracted_phones:
                        cur.execute("SELECT phone FROM contacts WHERE phone = ?", (phone,))
                        if cur.fetchone():
                            print(f"    -> [SKIP - Phase 1] Post {post_id} skipped. Phone {phone} already in master DB (programmatic check).")
                            is_duplicate_programmatic = True
                            break
                    
                    if not is_duplicate_programmatic:
                        posts_for_llm_enrichment.append(post)
            
            if not posts_for_llm_enrichment:
                print("[Processor] All posts in batch skipped by programmatic phone check. Starting new batch.")
                continue

            print(f"[Processor] Sending {len(posts_for_llm_enrichment)} posts to LLM for initial enrichment (Phase 2).")
            enriched_data_list = await enrich_batch_with_llm(config['model'], posts_for_llm_enrichment, enrich_prompt)
            
            if not enriched_data_list:
                print("[Processor] LLM enrichment failed for the batch. Skipping.")
                continue
            
            enriched_map = {item['post_id']: item for item in enriched_data_list}
            print(f"[Processor] Triage for batch of {len(posts_for_llm_enrichment)} results (Phase 2)...")
            
            posts_for_finalization = []
            with sqlite3.connect(config['producer_settings']['master_database_file']) as con:
                cur = con.cursor()
                for post in posts_for_llm_enrichment:
                    post_id = post['post_id']
                    if post_id not in enriched_map:
                        print(f"    -> [SKIP] No enrichment data returned from LLM for post {post_id}.")
                        continue
                    
                    lead = {**post, **enriched_map[post_id]}
                    
                    # Normalize phone number extracted by LLM
                    llm_extracted_phone = normalize_phone_number(lead.get('phone'))
                    lead['phone'] = llm_extracted_phone # Update lead with normalized phone
                    
                    # Phase 2: Duplicate check after LLM extraction
                    if llm_extracted_phone:
                        cur.execute("SELECT phone FROM contacts WHERE phone = ?", (llm_extracted_phone,))
                        if cur.fetchone():
                            print(f"    -> [SKIP - Phase 2] Post {post_id} skipped. Phone {llm_extracted_phone} already in master DB (LLM extracted check).")
                            continue # Skip this lead entirely
                    
                    if not lead['phone']:
                        print(f"    -> [SKIP] Post {post_id} skipped. No valid phone number found after normalization (LLM extracted).")
                        continue
                    
            # Phase 2.5: Category Normalization and Arbitrage Data Generation
            # This function now handles batch LLM calls, conditional Surfer Prospecting,
            # and saving/syncing arbitrage data.
            print(f"    [Processor] Calling normalize_business_category for batch of {len(posts_for_finalization)} leads.")
            processed_leads_with_normalized_categories = await normalize_business_category(
                leads_batch=posts_for_finalization,
                llm_model=config['model'],
                master_db_path=config['producer_settings']['master_database_file'],
                firebase_arbitrage_sync_url=config['producer_settings']['firebase_category_arbitrage_sync_url'],
                category_arbitrage_update_interval_days=config['producer_settings']['category_arbitrage_update_interval_days']
            )
            print(f"    [Processor] Category normalization and arbitrage data generation completed for {len(processed_leads_with_normalized_categories)} leads.")

            # Now, posts_for_finalization should be updated with the normalized categories
            # and any leads that were processed by the arbitrage logic.
            # The loop below will continue to process these leads for website scoring and finalization.
            posts_for_finalization = processed_leads_with_normalized_categories
            
            if not posts_for_finalization:
                print("[Processor] No posts remaining for finalization after all checks (including category normalization). Starting new batch.")
                continue

            print(f"[Processor] Finalizing {len(posts_for_finalization)} leads.")
            for lead in posts_for_finalization:
                # Check if target leads processed before finalizing
                if leads_processed_counter[0] >= config['target_count']:
                    print(f"    -> [TRIAGE] Skipping lead {lead['phone']}. Target leads ({config['target_count']}) already processed.")
                    continue

                # Phase 3: Website Scoring (remains here, but now uses potentially normalized category)
                website_url = lead.get('website_url')
                if website_url:
                    print(f"    [Processor] Website URL found for {lead.get('post_id')}: {website_url}. Attempting to scrape and score.")
                    website_body_text = await scrape_website_body(website_url, headless_browser)
                    if website_body_text:
                        try:
                            website_scoring_response = await config['model'].generate_content_async(
                                website_scoring_prompt.format(website_body_text=website_body_text)
                            )
                            cleaned_scoring_text = website_scoring_response.text.strip().replace("```json", "").replace("```", "").strip()
                            website_analysis = json.loads(cleaned_scoring_text)
                            lead['website_analysis'] = website_analysis
                            print(f"    [Processor] Successfully scored website for {lead.get('post_id')}. Score: {website_analysis.get('score')}")
                        except json.JSONDecodeError as e:
                            print(f"!!! [Processor] ERROR decoding website scoring JSON for {lead.get('post_id')}: {e}")
                            print(f"    Raw Scoring Response Text: {website_scoring_response.text[:500] if hasattr(website_scoring_response, 'text') else 'No text found'}...")
                        except Exception as e:
                            print(f"!!! [Processor] ERROR during website scoring LLM call for {lead.get('post_id')}: {e}")
                    else:
                        print(f"    [Processor] No body text scraped from website {website_url} for {lead.get('post_id')}.")
                
                # Original finalization logic
                if template_id == 99:
                    print(f"    -> [TRIAGE] Lead for phone {lead['phone']} (Template ID 99) - Bypassing email queues and directly finalizing for SMS.")
                    await finalize_and_queue_lead(lead, config, template_id, leads_processed_counter)
                    leads_processed_counter[0] += 1 # Increment counter after successful triage
                    print(f"    -> Leads processed counter incremented to {leads_processed_counter[0]} (SMS lead).")
                    if leads_processed_counter[0] >= config['target_count']:
                        print(f"    -> [TRIAGE] Target leads ({config['target_count']}) processed. Stopping further lead triage.")
                        break # Stop processing further leads in this batch
                    continue # Skip further triage for SMS-only leads
                elif lead.get('email') and '@' in lead['email'] and '@serv.craigslist.org' not in lead['email']:
                    print(f"    -> [TRIAGE] Lead for phone {lead['phone']} has direct email. Sending to Zero-Effort queue. Current size: {zero_effort_queue.qsize()}")
                    await zero_effort_queue.put(lead)
                    leads_processed_counter[0] += 1 # Increment counter after successful triage
                    print(f"    -> Leads processed counter incremented to {leads_processed_counter[0]} (Zero-Effort queue).")
                    if leads_processed_counter[0] >= config['target_count']:
                        print(f"    -> [TRIAGE] Target leads ({config['target_count']}) processed. Stopping further lead triage.")
                        break # Stop processing further leads in this batch
                else:
                    print(f"    -> [TRIAGE] Lead for phone {lead['phone']} has no direct email. Sending to CAPTCHA queue. Current size: {captcha_queue.qsize()}")
                    await captcha_queue.put(lead)
                    # Also save to master_contacts.db with 'pending captcha' status and template_id
                    try:
                        with sqlite3.connect(config['producer_settings']['master_database_file']) as con:
                            cur = con.cursor()
                            # Ensure lead_data_json includes the template_id for later use
                            lead['template_id'] = template_id 
                            cur.execute("""
                                INSERT OR REPLACE INTO contacts (phone, name, email, last_sent, source_url, image_hash, business_name, category, services_rendered, status, city, lead_data_json)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                lead.get("phone"), lead.get("name"), lead.get("email"), None, lead.get("url"), 
                                lead.get("image_hash"), lead.get("business_name"), lead.get("category"), json.dumps(lead.get("services_rendered", [])),
                                "pending captcha", lead.get("city"), json.dumps(lead)
                            ))
                        print(f"    -> Lead {lead['phone']} saved to master_contacts.db with status 'pending captcha'.")
                        leads_processed_counter[0] += 1 # Increment counter after successful triage
                        print(f"    -> Leads processed counter incremented to {leads_processed_counter[0]} (CAPTCHA queue).")
                        if leads_processed_counter[0] >= config['target_count']:
                            print(f"    -> [TRIAGE] Target leads ({config['target_count']}) processed. Stopping further lead triage.")
                            break # Stop processing further leads in this batch
                    except sqlite3.Error as e:
                        print(f"    -> ERROR saving lead to master_contacts.db as 'pending captcha': {e}")
        
        except asyncio.CancelledError:
            print("[Processor] Task cancelled.")
            break
        except Exception as e:
            print(f"!!! [Processor] An unhandled error occurred in the main loop: {e}")
            import traceback
            traceback.print_exc()
            await asyncio.sleep(10)


async def zero_effort_handler(zero_effort_queue, config, template_id, leads_processed_counter):
    print("[Zero Effort] Starting...")
    while True:
        try:
            lead = await zero_effort_queue.get()
            print(f"    [Zero-Effort] Finalizing lead for {lead.get('phone')}")
            await finalize_and_queue_lead(lead, config, template_id, leads_processed_counter)
            zero_effort_queue.task_done()
        except asyncio.CancelledError:
            print("[Zero-Effort Handler] Task cancelled.")
            break

# --- ADVANCED CAPTCHA WORKFLOW HELPERS ---
async def main_async(config):
    producer_cfg = config['producer_settings']
    url_reservoir = asyncio.Queue()
    captcha_queue = asyncio.Queue()
    zero_effort_queue = asyncio.Queue()
    harvester_finished_event = asyncio.Event()
    
    p_instance = None
    headless_browser = None
    headful_browser = None

    try:
        # Initialize Playwright once
        p_instance = await async_playwright().start()

        # Launch headless browser (Firefox) for background tasks
        headless_browser = await p_instance.firefox.launch(headless=True)
        print("--- [Main] Headless Firefox browser launched for background tasks.")

        # Main loop to check processor_queue
        while True:
            master_db_file = producer_cfg['master_database_file']
            print(f"--- [Main] Checking processor_queue in database: {master_db_file} ---")
            conn = None
            try:
                conn = sqlite3.connect(master_db_file)
                conn.row_factory = sqlite3.Row # Access columns by name
                cursor = conn.cursor()
                print("--- [Main] Successfully connected to the database. ---")
                
                # Check for pending requests in processor_queue
                cursor.execute("SELECT * FROM processor_queue WHERE status = 'pending' ORDER BY request_timestamp ASC LIMIT 1")
                request = cursor.fetchone()

                if request:
                    request_id = request['id']
                    city = request['city']
                    template_id = request['template_id']
                    num_leads_to_process = request['number_of_leads_to_process'] # Corrected column name based on lead_gen_main.py
                    print(f"\n--- [Main] FOUND pending request (ID: {request_id}, City: {city}, Template: {template_id}, Leads: {num_leads_to_process}) ---")
                    
                    # Update request status to 'processing'
                    cursor.execute("UPDATE processor_queue SET status = 'processing' WHERE id = ?", (request_id,))
                    conn.commit()
                    print(f"--- [Main] Request {request_id} status updated to 'processing'. ---")

                    # Update config for the current processing run
                    config['producer_settings']['craigslist_sub_domain'] = city
                    config['run_template_id'] = template_id
                    config['target_count'] = num_leads_to_process # Set target for this run

                    # Clear existing queues for a fresh run
                    while not url_reservoir.empty(): await url_reservoir.get()
                    while not captcha_queue.empty(): await captcha_queue.get()
                    while not zero_effort_queue.empty(): await zero_effort_queue.get()
                    harvester_finished_event.clear() # Reset event for new run

                    # Initialize leads processed counter for this request
                    leads_processed_counter = [0] # Using a list to make it mutable

                    # Start harvester and processor tasks for this request
                    harvester_task = asyncio.create_task(harvester(url_reservoir, headless_browser, config, harvester_finished_event))
                    processor_task = asyncio.create_task(processor(url_reservoir, captcha_queue, zero_effort_queue, config, headless_browser, harvester_finished_event, leads_processed_counter, template_id))
                    zero_effort_task = asyncio.create_task(zero_effort_handler(zero_effort_queue, config, template_id, leads_processed_counter))

                    # Loop until leads_processed_counter reaches target_count
                    while leads_processed_counter[0] < num_leads_to_process:
                        # Count leads that are either processed, email_found, or captcha_email_not_found for this request
                        # This is for logging/visibility, not the primary loop condition
                        conn_inner = None
                        try:
                            conn_inner = sqlite3.connect(master_db_file)
                            cursor_inner = conn_inner.cursor()
                            cursor_inner.execute("""
                                SELECT COUNT(*) FROM contacts 
                                WHERE city = ? 
                                AND status IN ('processed', 'email_found', 'captcha_email_not_found')
                            """, (city,))
                            current_processed_count_db = cursor_inner.fetchone()[0]
                        except sqlite3.Error as e:
                            print(f"!!! [Main] Database error while counting processed leads for logging: {e}")
                            current_processed_count_db = -1 # Indicate error
                        finally:
                            if conn_inner:
                                conn_inner.close()

                        print(f"--- [Main] Leads processed by processor: {leads_processed_counter[0]}/{num_leads_to_process} (DB count: {current_processed_count_db})")

                        # Check if harvester and processor are still running or finished
                        if harvester_task.done() and processor_task.done() and url_reservoir.empty() and captcha_queue.empty() and zero_effort_queue.empty():
                            print(f"--- [Main] All harvester/processor tasks completed or queues empty. Exiting loop.")
                            break # Exit inner loop if tasks are done and queues are empty
                        
                        await asyncio.sleep(5) # Wait a bit before checking again
                    
                    # If loop exited because leads_processed_counter >= num_leads_to_process
                    if leads_processed_counter[0] >= num_leads_to_process:
                        print(f"--- [Main] All {num_leads_to_process} leads for request {request_id} processed by processor.")
                    else:
                        print(f"--- [Main] Loop exited, but not all leads ({leads_processed_counter[0]}/{num_leads_to_process}) processed by processor. Check logs for issues.")

                    # Add a grace period to allow final async operations to complete
                    print(f"--- [Main] Processing for request {request_id} completed. Entering 60-second grace period for final operations...")
                    await asyncio.sleep(60) # Grace period

                    # After processing for the request is done
                    print(f"--- [Main] Request {request_id} processing finished. Updating status to 'completed'.")
                    cursor.execute("UPDATE processor_queue SET status = 'completed' WHERE id = ?", (request_id,))
                    conn.commit()

                    # Cancel tasks for the current request
                    harvester_task.cancel()
                    processor_task.cancel()
                    zero_effort_task.cancel()
                    await asyncio.gather(harvester_task, processor_task, zero_effort_task, return_exceptions=True) # Wait for cancellation

                else:
                    print("--- [Main] No pending requests in processor_queue. Sleeping for 10 seconds. ---")
                    await asyncio.sleep(10) # Wait before checking again

            except sqlite3.Error as e:
                print(f"!!! [Main] Database error: {e}")
                await asyncio.sleep(10)
            except Exception as e:
                print(f"!!! [Main] An unhandled error occurred in the main loop: {e}")
                import traceback
                traceback.print_exc()
                await asyncio.sleep(10)
            finally:
                if conn:
                    conn.close()

    except asyncio.CancelledError:
        print("[main_async] Task cancelled.")
    except Exception as e:
        print(f"!!! [Main] An unhandled error occurred in main_async setup: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Ensure all Playwright instances are stopped on exit
        if headless_browser:
            await headless_browser.close()
        if headful_browser: # Should be None if closed after batch, but for safety
            await headful_browser.close()
        if p_instance:
            await p_instance.stop()


def main():
    print("--- Contact Extractor (Background Processor) - Script Start ---") # Added initial log
    
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_filename = os.path.join(script_dir, 'config.json')
        print(f"--- [Main] Attempting to load config from: {config_filename} ---") # Added log for config loading
        with open(config_filename, 'r') as f:
            config = json.load(f)
        print("--- [Main] Config loaded successfully. ---") # Added log for successful config load
    except FileNotFoundError:
        print(f"CRITICAL ERROR: '{config_filename}' file not found. Exiting.") # Changed sys.exit to print and then exit
        sys.exit(1) # Exit with an error code
    except json.JSONDecodeError:
        print("CRITICAL ERROR: Could not parse config file. Ensure it's valid JSON. Exiting.") # Changed sys.exit to print and then exit
        sys.exit(1) # Exit with an error code

    producer_cfg = config['producer_settings']
    
    global_cfg = config['global_settings']
    genai.configure(api_key=global_cfg['google_api_key'])
    config['model'] = genai.GenerativeModel(global_cfg['llm_model'])
    
    for key, path in producer_cfg.items():
        if isinstance(path, str) and (key.endswith('_file') or key.endswith('_dir')):
            producer_cfg[key] = os.path.join(script_dir, path)

    try:
        asyncio.run(main_async(config))
    except KeyboardInterrupt:
        print("\n--- [Main] Script interrupted by user. ---")
    except Exception as e:
        print(f"!!! [Main] An unhandled exception occurred in main(): {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("--- Contact Extractor shutting down. ---") # More appropriate shutdown message

if __name__ == "__main__":
    main()
