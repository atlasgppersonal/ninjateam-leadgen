import sys
import json
import asyncio
import time
import re
import os
import sqlite3
import psutil
import subprocess
import google.generativeai as genai
from playwright.async_api import async_playwright

# --- ============================== ---
# --- --- HELPER FUNCTIONS --- ---
# --- ============================== ---

def ensure_consumer_is_running(config):
    consumer_script_name = "email-sender.py"
    is_running = False
    for process in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            # CORRECTED: Changed 'consumer.script_name' to 'consumer_script_name'
            if process.info['cmdline'] and consumer_script_name in " ".join(process.info['cmdline']):
                print(f"-> Consumer script '{consumer_script_name}' is already running (PID: {process.info['pid']}).")
                is_running = True
                break
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

    if not is_running:
        print(f"-> Consumer script '{consumer_script_name}' not found. Starting it now...")
        try:
            creation_flags = subprocess.CREATE_NEW_CONSOLE if sys.platform == "win32" else 0
            subprocess.Popen([sys.executable, consumer_script_name], creationflags=creation_flags)
            print("-> Consumer script launched successfully in a new process.")
        except FileNotFoundError:
            print(f"!!! CRITICAL ERROR: Could not find '{consumer_script_name}'. Make sure it is in the same directory.")
            sys.exit(1)
        except Exception as e:
            print(f"!!! CRITICAL ERROR: Failed to launch consumer script: {e}")
            sys.exit(1)

def normalize_phone_number(phone):
    if not phone: return None
    cleaned = re.sub(r'\D', '', phone)
    if len(cleaned) == 11 and cleaned.startswith('1'):
        cleaned = cleaned[1:]
    return cleaned if len(cleaned) == 10 else None

def init_master_db(master_db_file):
    with sqlite3.connect(master_db_file) as con:
        cur = con.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS contacts (
                phone TEXT PRIMARY KEY, name TEXT, email TEXT, last_sent TEXT,
                source_url TEXT, image_hash TEXT, business_name TEXT,
                category TEXT, services_rendered TEXT
            )
        """)

def remove_emojis(text: str) -> str:
    if not text: return ""
    emoji_pattern = re.compile(r'['
                               u'\U0001F600-\U0001F64F\U0001F300-\U0001F5FF'
                               u'\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF'
                               u'\u2600-\u26FF\u2700-\u27BF\u2580-\u259F\uFE0F\u3030'
                               ']+', flags=re.UNICODE)
    return emoji_pattern.sub(r'', text).strip()

# --- ============================== ---
# --- --- CORE ARCHITECTURAL LOGIC --- ---
# --- ============================== ---

async def send_to_server(lead_data, config):
    """Sends the finalized lead data to a remote server endpoint using async playwright."""
    print("    [Server Sync] Attempting to send lead data to the server...")
    producer_cfg = config['producer_settings']
    url = producer_cfg.get('cloud_function_url')

    if not url:
        print("    [Server Sync] WARNING: 'cloud_function_url' not found in config. Skipping.")
        return

    payload = {k: v for k, v in lead_data.items() if v is not None}

    try:
        async with async_playwright() as p:
            request_context = await p.request.new_context()
            response = await request_context.post(url, data=payload)
            await request_context.dispose()

        if response.ok:
            print(f"    [Server Sync] SUCCESS: Lead data sent to server. Status: {response.status}")
        else:
            print(f"    [Server Sync] FAILED: Server returned an error. Status: {response.status}, Body: {await response.text()}")

    except Exception as e:
        print(f"!!! [Server Sync] An unexpected error occurred during the network request: {e}")


async def finalize_and_queue_lead(lead_data, config, template_id):
    """The single, unified function to finalize a lead using robust context managers."""
    phone = lead_data.get('phone')
    print(f"    [Finalizer] Finalizing lead for phone: {phone}")
    
    producer_cfg = config['producer_settings']
    master_db = producer_cfg['master_database_file']
    queue_db = producer_cfg['queue_database_file']
    city = producer_cfg['craigslist_sub_domain']
    timezone = config['city_profiles'][city]['tz']
    
    saved_to_master = False
    queued_for_email = False

    # 1. Save to local master database
    try:
        with sqlite3.connect(master_db) as con:
            cur = con.cursor()
            cur.execute("""
                INSERT OR REPLACE INTO contacts (phone, name, email, last_sent, source_url, image_hash, business_name, category, services_rendered)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                phone, lead_data.get("name"), lead_data.get("email"), "QUEUED", lead_data.get("url"), 
                lead_data.get("image_hash"), lead_data.get("business_name"), lead_data.get("category"), json.dumps(lead_data.get("services_rendered", []))
            ))
        print(f"    -> Saved to master_contacts.db")
        saved_to_master = True
    except sqlite3.Error as e:
        print(f"    -> ERROR saving to master_contacts.db: {e}")

    # 2. Send to remote server
    if saved_to_master:
        await send_to_server(lead_data, config)

    # 3. Queue for the consumer
    if lead_data.get('email'):
        try:
            with sqlite3.connect(queue_db) as con:
                cur = con.cursor()
                cur.execute("""
                    INSERT INTO email_queue (template_id, city, timezone, lead_data_json)
                    VALUES (?, ?, ?, ?)
                """, (template_id, city, timezone, json.dumps(lead_data)))
            print(f"    -> Lead added to email_queue.db with Template ID {template_id}.")
            queued_for_email = True
        except sqlite3.Error as e:
            print(f"    -> ERROR adding lead to email_queue.db: {e}")
    
    print(f"    [Finalizer] Summary for {phone}: Saved: {'Yes' if saved_to_master else 'No'}. Email found: {'Yes' if lead_data.get('email') else 'No'}. Queued: {'Yes' if queued_for_email else 'No'}.")


async def enrich_batch_with_llm(model, batch_data, prompt_template):
    print(f"    [LLM Batch] Sending batch of {len(batch_data)} posts to LLM...")
    try:
        llm_input = [{"post_id": post.get("post_id"), "post_body": post.get("body_text")} for post in batch_data]
        batch_data_json = json.dumps(llm_input, indent=2)
        prompt = prompt_template.format(batch_data_json=batch_data_json)
        response = await model.generate_content_async(prompt)
        cleaned_text = response.text.strip().replace("```json", "").replace("```", "").strip()
        enriched_results = json.loads(cleaned_text)
        print(f"    [LLM Batch] Successfully received and parsed {len(enriched_results)} results from LLM.")
        return enriched_results
    except json.JSONDecodeError as e:
        print(f"!!! [LLM Batch] CRITICAL ERROR: Failed to decode JSON from LLM response. Error: {e}")
        print(f"    Raw Response Text: {response.text[:500] if hasattr(response, 'text') else 'No text found'}...")
        return None
    except Exception as e:
        print(f"!!! [LLM Batch] CRITICAL ERROR during LLM call: {e}")
        return None


async def scrape_post_body(p_instance, post_data):
    url = post_data.get("url")
    print(f"    [Scraper] Scraping body for {url}")
    try:
        browser = await p_instance.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        body_text = await page.locator("#postingbody").inner_text(timeout=15000)
        post_data["body_text"] = remove_emojis(body_text)
        await browser.close()
        return post_data
    except Exception as e:
        print(f"!!! [Scraper] FAILED to scrape {url}. Reason: {e}")
        post_data["body_text"] = None
        return post_data


async def harvester(url_reservoir, playwright_instance, config, harvester_finished_event):
    print("    [Harvester] Starting a new run...")
    try:
        producer_cfg = config['producer_settings']
        master_db = producer_cfg['master_database_file']
        with sqlite3.connect(master_db) as con:
            cur = con.cursor()
            cur.execute("SELECT image_hash FROM contacts WHERE image_hash IS NOT NULL")
            processed_hashes = {row[0] for row in cur.fetchall()}
        print(f"    [Harvester] Loaded {len(processed_hashes)} existing image hashes from DB for duplicate checking.")
        state = {"min_post_id": None, "category_map": {}, "image_hash_map": {}, "raw_posts": []}
        browser = await playwright_instance.chromium.launch(headless=True)
        page = await browser.new_page()
        page.on("response", lambda response: asyncio.create_task(handle_search_response(response, state)))
        sub_domain = producer_cfg['craigslist_sub_domain']
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
        await browser.close()
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
            url_slug = post_data.get('url_slug')
            if abbr and url_slug:
                full_url = f"https://{sub_domain}.{base_url}/{abbr}/d/{url_slug}/{post_id}.html"
                print(f"    -> [NEW] Adding URL to reservoir: {full_url}")
                await url_reservoir.put({"url": full_url, "post_id": str(post_id), "image_hash": image_hash, "original_category": cat_name})
                urls_added += 1
            else:
                print(f"    -> [SKIP] Skipping post {post_id} (Missing URL slug or category abbreviation). Slug: '{url_slug}', Abbr: '{abbr}'.")
                skipped_count += 1
        print(f"    [Harvester] Finished. Scanned {len(state['raw_posts'])} raw posts. Filtered {filtered_count} duplicates. Skipped {skipped_count} due to data issues. Added {urls_added} new URLs to the reservoir.")
    finally:
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


async def processor(url_reservoir, captcha_queue, zero_effort_queue, config, p_instance, harvester_finished_event):
    print("[Processor] Starting...")
    llm_batch_size = int(config['producer_settings'].get('llm_batch_size', 5))
    enrich_prompt = config['producer_settings']['enrich_lead_batch']
    while True:
        try:
            batch_to_process = []
            if harvester_finished_event.is_set() and url_reservoir.empty():
                print("[Processor] Harvester is finished and queue is empty. Processor is pausing.")
                await asyncio.sleep(60)
                continue
            while len(batch_to_process) < llm_batch_size:
                try:
                    item = await asyncio.wait_for(url_reservoir.get(), timeout=1.0)
                    batch_to_process.append(item)
                    url_reservoir.task_done()
                except asyncio.TimeoutError:
                    break
            if not batch_to_process:
                continue
            print(f"\n[Processor] Starting new batch of {len(batch_to_process)} posts.")
            scraping_tasks = [scrape_post_body(p_instance, post) for post in batch_to_process]
            scraped_posts_results = await asyncio.gather(*scraping_tasks)
            successfully_scraped_posts = [post for post in scraped_posts_results if post.get("body_text")]
            if not successfully_scraped_posts:
                print("[Processor] All scrapes in batch failed. Starting new batch.")
                continue
            enriched_data_list = await enrich_batch_with_llm(config['model'], successfully_scraped_posts, enrich_prompt)
            if not enriched_data_list:
                print("[Processor] LLM enrichment failed for the batch. Skipping.")
                continue
            enriched_map = {item['post_id']: item for item in enriched_data_list}
            print(f"[Processor] Triage for batch of {len(successfully_scraped_posts)} results...")
            for post in successfully_scraped_posts:
                post_id = post['post_id']
                if post_id not in enriched_map:
                    print(f"    -> [SKIP] No enrichment data returned from LLM for post {post_id}.")
                    continue
                lead = {**post, **enriched_map[post_id]}
                lead['phone'] = normalize_phone_number(lead.get('phone'))
                if not lead['phone']:
                    print(f"    -> [SKIP] Post {post_id} skipped. No valid phone number found after normalization.")
                    continue
                with sqlite3.connect(config['producer_settings']['master_database_file']) as con:
                    cur = con.cursor()
                    cur.execute("SELECT phone FROM contacts WHERE phone = ?", (lead['phone'],))
                    if cur.fetchone():
                        print(f"    -> [SKIP] Post {post_id} skipped. Phone {lead['phone']} already in master DB.")
                        continue
                if lead.get('email') and '@' in lead['email'] and '@serv.craigslist.org' not in lead['email']:
                    print(f"    -> [TRIAGE] Lead for phone {lead['phone']} has direct email. Sending to Zero-Effort queue.")
                    await zero_effort_queue.put(lead)
                else:
                    print(f"    -> [TRIAGE] Lead for phone {lead['phone']} has no direct email. Sending to CAPTCHA queue.")
                    await captcha_queue.put(lead)
        except asyncio.CancelledError:
            print("[Processor] Task cancelled.")
            break
        except Exception as e:
            print(f"!!! [Processor] An unhandled error occurred in the main loop: {e}")
            import traceback
            traceback.print_exc()
            await asyncio.sleep(10)


async def zero_effort_handler(zero_effort_queue, config, template_id):
    while True:
        try:
            lead = await zero_effort_queue.get()
            print(f"    [Zero-Effort] Finalizing lead for {lead.get('phone')}")
            await finalize_and_queue_lead(lead, config, template_id)
            zero_effort_queue.task_done()
        except asyncio.CancelledError:
            print("[Zero-Effort Handler] Task cancelled.")
            break

# --- ADVANCED CAPTCHA WORKFLOW HELPERS ---
async def watch_and_click_email_button(page):
    print(f"    [Watcher] Task started for page: {page.url}")
    email_button_selector = 'button.reply-option-header:has(span.reply-option-label:text-is("email"))'
    try:
        print(f"    [Watcher] Waiting to find and click email button on {page.url}...")
        await page.locator(email_button_selector).click(timeout=300000)
        print(f"    [Watcher] SUCCESS: Clicked the email button on {page.url}.")
    except asyncio.CancelledError:
        pass
    except Exception as e:
        error_message = str(e)
        if "Timeout" in error_message:
            print(f"    [Watcher] TIMEOUT on {page.url}. Timed out waiting for selector '{email_button_selector}' to become actionable.")
        else:
            print(f"    [Watcher] FAILED on {page.url}. Could not find or click email button. Error: {e}")


async def main_async(config):
    template_id = config['run_template_id']
    producer_cfg = config['producer_settings']
    url_reservoir = asyncio.Queue()
    captcha_queue = asyncio.Queue()
    zero_effort_queue = asyncio.Queue()
    harvester_finished_event = asyncio.Event()
    
    p_instance = await async_playwright().start()
    
    harvester_finished_event.clear()
    asyncio.create_task(harvester(url_reservoir, p_instance, config, harvester_finished_event))
    processor_task = asyncio.create_task(processor(url_reservoir, captcha_queue, zero_effort_queue, config, p_instance, harvester_finished_event))
    zero_effort_task = asyncio.create_task(zero_effort_handler(zero_effort_queue, config, template_id))

    while True:
        try:
            batch_for_captcha, opened_pages, watcher_tasks = [], [], []
            print("\n--- [Foreground] Waiting for a batch of leads for CAPTCHA solving...")
            for _ in range(producer_cfg.get('batch_size', 5)):
                try:
                    lead = await asyncio.wait_for(captcha_queue.get(), timeout=10.0)
                    batch_for_captcha.append(lead)
                except asyncio.TimeoutError:
                    if processor_task.done(): break
            
            if not batch_for_captcha:
                if processor_task.done() and harvester_finished_event.is_set() and captcha_queue.empty():
                    print("--- [Foreground] All tasks appear to be finished. Exiting CAPTCHA loop.")
                    break
                continue

            print(f"--- [Foreground] Batch of {len(batch_for_captcha)} ready for CAPTCHA ---")
            browser = await p_instance.chromium.launch(headless=False)
            context = await browser.new_context()
            captured_emails = {}
            
            async def process_email_response(response, post_id, page, all_pages):
                try:
                    data = await response.json()
                    if "email" in data:
                        captured_emails[post_id] = data["email"]
                        print(f"    -> SUCCESS: Captured email for post {post_id}")
                        current_index = all_pages.index(page)
                        if current_index < len(all_pages) - 1:
                            next_page = all_pages[current_index + 1]
                            print(f"    -> Advancing to next tab: {next_page.url}")
                            await next_page.bring_to_front()
                        else:
                            print("    -> Last tab processed!")
                except Exception as e:
                    print(f"    -> ERROR processing XHR response: {e}")

            def create_listener(post_id, page, all_pages):
                async def handle_response(response):
                    if "/__SERVICE_ID__/contactinfo" in response.url:
                        await process_email_response(response, post_id, page, all_pages)
                return handle_response
            
            for lead in batch_for_captcha:
                page = await context.new_page()
                opened_pages.append(page)
            
            for i, lead in enumerate(batch_for_captcha):
                page = opened_pages[i]
                page.on("response", create_listener(lead['post_id'], page, opened_pages))
                print(f"    -> Opening tab for {lead['url']}")
                await page.goto(lead['url'])
                try:
                    await page.locator('button.reply-button').click(timeout=10000)
                    task = asyncio.create_task(watch_and_click_email_button(page))
                    watcher_tasks.append(task)
                except Exception as e:
                    print(f"    -> WARNING: Could not auto-click reply button for {lead['post_id']}. Please click manually. {e}")
            
            if opened_pages:
                await opened_pages[0].bring_to_front()

            input("\n--- All tabs are open. Please solve CAPTCHAs. The script will auto-click 'email' and advance tabs for you. Press Enter here when the entire batch is done. ---")
            
            for task in watcher_tasks:
                task.cancel()
            await browser.close()
            
            for lead in batch_for_captcha:
                if lead['post_id'] in captured_emails:
                    lead['email'] = captured_emails[lead['post_id']]
                else:
                    print(f"    -> WARNING: No email was captured for post {lead['post_id']}.")
                await finalize_and_queue_lead(lead, config, template_id)
            
            if config['target_count'] != float('inf'):
                config['target_count'] -= len(batch_for_captcha)
                if config['target_count'] <= 0:
                    print("Target lead count reached.")
                    break
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"!!! [Foreground] An error occurred in the CAPTCHA loop: {e}")
            import traceback
            traceback.print_exc()

    processor_task.cancel()
    zero_effort_task.cancel()
    await p_instance.stop()
    print("\nProducer script finished.")


def main():
    print("--- Contact Extractor (Producer) ---")
    
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_filename = os.path.join(script_dir, 'config.json')
        with open(config_filename, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        sys.exit(f"CRITICAL ERROR: '{config_filename}' file not found.")
    except json.JSONDecodeError:
        sys.exit("CRITICAL ERROR: Could not parse config file.")

    ensure_consumer_is_running(config)
    
    producer_cfg = config['producer_settings']
    queue_db_file = os.path.join(script_dir, producer_cfg['queue_database_file'])
    run_template_id = None
    
    try:
        with sqlite3.connect(queue_db_file) as con:
            cur = con.cursor()
            cur.execute("SELECT template_id, template_name, description FROM templates WHERE is_archived = 0 ORDER BY template_id")
            active_templates = cur.fetchall()
            
            print("\n--- Available Email Templates ---")
            if not active_templates: print("No active templates found.")
            else:
                for t_id, t_name, t_desc in active_templates: print(f"{t_id}: {t_name} - {t_desc}")
            print("---------------------------------")
            print("(R) Register a New Template")
            print("---------------------------------")
            
            choice = input("Choose a template to use for this run (e.g., 1, 2, R): ").lower()

            if choice == 'r':
                print("\n--- New Template Registration ---")
                templates_dir = os.path.join(script_dir, producer_cfg.get('templates_dir', 'templates'))
                template_path = input(f"Enter path to the new template file (relative to '{script_dir}'): ")
                if not os.path.exists(template_path): sys.exit(f"ERROR: File not found at '{template_path}'")
                
                with open(template_path, 'r', encoding='utf-8') as f: template_data = json.load(f)
                
                name = input("Enter a short name for this template: ")
                desc = input("Enter a one-line description: ")
                subject = template_data.get('subject')
                body = template_data.get('body_html')

                cur.execute("INSERT INTO templates (template_name, description, base_subject, base_body_html) VALUES (?, ?, ?, ?)", (name, desc, subject, body))
                run_template_id = cur.lastrowid
                print(f"SUCCESS: Template '{name}' registered with ID: {run_template_id}")
            else:
                run_template_id = int(choice)
    except sqlite3.Error as e:
        sys.exit(f"DATABASE ERROR during template selection: {e}")

    if run_template_id is None: sys.exit("No valid template selected. Exiting.")
    config['run_template_id'] = run_template_id
    
    target_input = input("How many new leads would you like to process in this run? (Press Enter for all): ")
    try:
        config['target_count'] = int(target_input) if target_input else float('inf')
    except ValueError:
        config['target_count'] = float('inf')
    
    global_cfg = config['global_settings']
    genai.configure(api_key=global_cfg['google_api_key'])
    config['model'] = genai.GenerativeModel(global_cfg['llm_model'])
    
    for key, path in producer_cfg.items():
        if isinstance(path, str) and (key.endswith('_file') or key.endswith('_dir')):
            producer_cfg[key] = os.path.join(script_dir, path)

    try:
        asyncio.run(main_async(config))
    except KeyboardInterrupt:
        print("\nScript interrupted by user.")
    finally:
        input("Press Enter to exit.")

if __name__ == "__main__":
    main()