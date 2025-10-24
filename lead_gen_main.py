import asyncio
import json
import os
import sys
import sqlite3
import subprocess
import psutil
import time
from datetime import datetime
from playwright.async_api import async_playwright
import httpx # Added for making HTTP requests
import random # Add this import at the top of the file

# --- Helper Functions ---
def get_script_dir():
    return os.path.dirname(os.path.abspath(__file__))

def load_config():
    config_path = os.path.join(get_script_dir(), 'config.json')
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        sys.exit(f"CRITICAL ERROR: '{config_path}' not found.")
    except json.JSONDecodeError:
        sys.exit(f"CRITICAL ERROR: Could not parse '{config_path}'. Ensure it's valid JSON.")

def get_active_templates(queue_db_file):
    with sqlite3.connect(queue_db_file) as con:
        cur = con.cursor()
        cur.execute("SELECT template_id, template_name, description FROM templates WHERE is_archived = 0 ORDER BY template_id")
        return cur.fetchall()

def get_city_profiles(config):
    return config.get('city_profiles', {})

def is_processor_running(script_name="contact-extractor.py"):
    for process in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if process.info['cmdline'] and script_name in " ".join(process.info['cmdline']):
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return False

def add_processor_request(queue_db_file, template_id, city, num_leads):
    with sqlite3.connect(queue_db_file) as con:
        cur = con.cursor()
        cur.execute(
            "INSERT INTO processor_queue (template_id, city, number_of_leads_to_process, status, request_timestamp) VALUES (?, ?, ?, ?, ?)",
            (template_id, city, num_leads, 'pending', datetime.now().isoformat())
        )
        con.commit()
    print(f"Added request to processor_queue: Template ID {template_id}, City {city}, Leads {num_leads}, Status: pending")


# --- Watcher Function (unchanged from your logic) ---
async def watch_and_click_email_button(page):
    print(f"    [Watcher] Task started for page: {page.url}")
    email_button_selector = 'button.reply-option-header:has(span.reply-option-label:has-text("email"))'
    email_info_selector = '.reply-info.js-only' # Selector for the div that appears after click

    try:
        print(f"    [Watcher] Waiting for email button to become visible/enabled on {page.url}...")
        await page.wait_for_selector(email_button_selector, state="visible", timeout=60000)
        print(f"    [Watcher] Email button is visible and enabled. Attempting human-like click on {page.url}.")
        
        # Human-like click simulation
        reply_btn = page.locator(email_button_selector)
        await reply_btn.hover()
        await asyncio.sleep(random.uniform(0.1, 0.3)) # Small random delay before click
        box = await reply_btn.bounding_box()
        if box:
            await page.mouse.move(box["x"] + box["width"]/2, box["y"] + box["height"]/2)        
        
        print(f"    [Watcher] Email button clicked. Now waiting for email info div to appear on {page.url}.")
        await asyncio.sleep(random.uniform(0.5, 1.5)) # Random delay after click
        await reply_btn.click(timeout=10000) # Fallback to regular click if bounding box not found
        await asyncio.sleep(random.uniform(0.5, 1.5)) # Random delay after click
    except asyncio.CancelledError:
        print(f"    [Watcher] Task cancelled for {page.url}.")
    except Exception as e:
        error_message = str(e)
        if "Timeout" in error_message:
            print(f"    [Watcher] TIMEOUT on {page.url}. Timed out waiting for selector '{email_button_selector}' or verification failed.")
        else:
            print(f"    [Watcher] FAILED on {page.url}. Could not find or click email button, or verification failed. Error: {e}")


# --- Fixed process_captchas (full implementation) ---
async def process_captchas(config):
    producer_cfg = config['producer_settings']
    master_db_file = os.path.join(get_script_dir(), producer_cfg['master_database_file'])
    batch_size = int(producer_cfg.get('batch_size', 5))
    timeout_secs = int(producer_cfg.get('captcha_timeout_secs', 300))

    p_instance = None
    context = None

    try:
        p_instance = await async_playwright().start()

        # Use persistent context for more "real" browsing
        user_data_dir = os.path.join(get_script_dir(), "playwright_user_data")
        context = await p_instance.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--start-maximized"
            ]
        )
        print(f"--- [Foreground] Playwright persistent context launched using user_data_dir: {user_data_dir}")

        while True:
            # Load batch of leads
            with sqlite3.connect(master_db_file) as con:
                cur = con.cursor()
                cur.execute(
                    "SELECT phone, name, email, source_url, image_hash, business_name, category, services_rendered, city, lead_data_json "
                    "FROM contacts WHERE status = 'pending captcha' LIMIT ?",
                    (batch_size,)
                )
                raw_leads = cur.fetchall()

            if not raw_leads:
                print("\n--- No leads pending CAPTCHA. Worker exiting. ---")
                break  # no more work

            leads_to_process = []
            for row in raw_leads:
                # unpack safely
                phone = row[0]
                src_url = row[3]
                services_rendered = row[7]
                lead_json_raw = row[9]
                try:
                    lead_data_json = json.loads(lead_json_raw) if lead_json_raw else {}
                except Exception:
                    lead_data_json = {}
                # normalize url & post id
                post_id = lead_data_json.get('post_id') or lead_data_json.get('id') or f"post_{phone}"
                url = lead_data_json.get('url') or src_url
                leads_to_process.append({
                    "phone": phone,
                    "post_id": post_id,
                    "url": url,
                    "lead_data_json": lead_data_json
                })

            print(f"\n--- [Foreground] Processing batch of {len(leads_to_process)} leads for CAPTCHA ---")
            for L in leads_to_process:
                print(f"    -> phone={L['phone']} post_id={L['post_id']} url={L['url']}")

            # Prepare structures
            opened_pages = []
            watcher_tasks = []
            status_by_post = {l['post_id']: 'pending' for l in leads_to_process}
            post_to_phone = {l['post_id']: l['phone'] for l in leads_to_process}

            # Define email response processor which saves immediately and advances tabs
            async def process_email_response(response, post_id, page, all_pages):
                print(f"    -> IN MAIN RESPONSE")
                try:
                    # Only interested in contactinfo responses (looser match)
                    try:
                        resp_body = await response.text()
                    except Exception:
                        resp_body = ""
                    if "email" not in resp_body and "mailto:" not in resp_body:
                        print(f"    -> Email and Mailto not found in response body for {post_id}. Ignoring.")
                        return
                    print(f"    -> Pre-check passed, trying JSON parse for {post_id}")
                    data = await response.json()
                    
                    if not isinstance(data, dict):
                        print(f"    -> NOT A DICT for {post_id}. Ignoring.")
                        return
                    print(f"    -> JSON parsed OK for {post_id}: keys={list(data.keys())}")
                    if "email" in data:    
                        email = data["email"]
                        print(f"    -> SUCCESS: Captured email for post {post_id}: {email}")

                        # Save immediately with status=email_found
                        phone = post_to_phone.get(post_id)
                        if phone:
                            try:
                                with sqlite3.connect(master_db_file) as con:
                                    cur = con.cursor()
                                    cur.execute("SELECT lead_data_json FROM contacts WHERE phone = ?", (phone,))
                                    row = cur.fetchone()
                                    if row:
                                        try:
                                            lead_obj = json.loads(row[0])
                                        except Exception:
                                            lead_obj = {}
                                        lead_obj['email'] = email
                                        cur.execute("UPDATE contacts SET status=?, email=?, lead_data_json=? WHERE phone=?",
                                                    ('email_found', email, json.dumps(lead_obj), phone))
                                        con.commit()
                                        print(f"    -> DB updated for phone {phone} => status=email_found")
                            except Exception as e:
                                print(f"    -> DB save error for {phone}: {e}")

                        status_by_post[post_id] = 'email_found'

                        # Advance to next tab if present
                        try:
                            idx = all_pages.index(page)
                            if idx < len(all_pages) - 1:
                                next_page = all_pages[idx + 1]
                                print(f"    -> Advancing to next tab (index {idx+1}): {next_page.url}")
                                await next_page.bring_to_front()
                            else:
                                # last tab processed -> close batch tabs
                                print("    -> Last tab processed for this batch. Closing batch tabs.")
                                for p in list(all_pages):
                                    try:
                                        await p.close()
                                    except Exception:
                                        pass
                        except Exception:
                            pass
                except Exception as e:
                    print(f"    -> Error processing response for {post_id}: {e}")

            # Open pages (attach listeners before navigation)
            for lead in leads_to_process:
                page = await context.new_page()
                # save attributes for lookup
                page._phone = lead['phone']
                page._post_id = lead['post_id']
                # attach general logging listeners
                def log_request(request):
                    try:
                        print(f"    [Browser Log] Request: {request.method} {request.url}")
                    except Exception:
                        pass
                def log_response(response):
                    try:
                        print(f"    [Browser Log] Response: {response.status} {response.url}")
                    except Exception:
                        pass
                def log_console_message(msg):
                    try:
                        print(f"    [Browser Log] Console {msg.type.upper()}: {msg.text}")
                    except Exception:
                        pass

                page.on("request", log_request)
                page.on(
                            "response",
                            lambda response, pid=post_id, pg=page: asyncio.get_event_loop().create_task(
                                process_email_response(response, pid, pg, opened_pages)
                            )
                        )
                page.on("console", log_console_message)

                opened_pages.append(page)

            # Attach listeners, navigate, click reply, and start watcher tasks
            for i, lead in enumerate(leads_to_process):
                page = opened_pages[i]
                post_id = lead['post_id']
                print(f"    -> Opening tab for {lead['url']}")
                try:
                    await page.goto(lead['url'], timeout=60000)
                except Exception as e:
                    print(f"    -> goto failed for {lead['url']}: {e}")

                # small random stagger
                await asyncio.sleep(random.uniform(0.2, 0.7))

                try:
                    # attempt to click the reply button
                    reply_locator = page.locator('button.reply-button')
                    if await reply_locator.count() > 0:
                        # human-like click
                        try:
                            await reply_locator.hover()
                            await asyncio.sleep(random.uniform(0.05, 0.25))
                            box = await reply_locator.bounding_box()
                            if box:
                                await page.mouse.move(box["x"] + box["width"]/2, box["y"] + box["height"]/2)
                                await page.mouse.down()
                                await asyncio.sleep(random.uniform(0.04, 0.12))
                                await page.mouse.up()
                            else:
                                await reply_locator.click(timeout=10000)
                        except Exception:
                            try:
                                await reply_locator.click(timeout=10000)
                            except Exception as e:
                                print(f"    -> reply click fallback failed: {e}")

                        # start a watcher task (not awaited here)
                        task = asyncio.create_task(watch_and_click_email_button(page))
                        watcher_tasks.append(task)
                    else:
                        print(f"    -> reply-button not found for {lead['post_id']} on {lead['url']}")
                except Exception as e:
                    print(f"    -> WARNING: Could not auto-click reply button for {lead['post_id']}. Please click manually. {e}")

            # Bring first page to front to prime captchas
            if opened_pages:
                try:
                    await opened_pages[0].bring_to_front()
                except Exception:
                    pass

            # Wait loop: until all processed or timeout reached
            start_time = time.time()
            while True:
                pending_count = sum(1 for s in status_by_post.values() if s == 'pending')
                print(f"    [Batch Watch] pending: {pending_count} (time elapsed: {int(time.time()-start_time)}s)")
                if pending_count == 0:
                    print("    [Batch Watch] All posts processed for this batch.")
                    break
                if time.time() - start_time > timeout_secs:
                    print("    [Batch Watch] Batch timeout reached.")
                    break
                await asyncio.sleep(1)

            # Cancel any watcher tasks still running (they may be waiting for UI elements)
            for t in watcher_tasks:
                if not t.done():
                    try:
                        t.cancel()
                    except Exception:
                        pass

            # Close any pages still open for this batch
            for page in opened_pages:
                try:
                    if not page.is_closed():
                        await page.close()
                except Exception:
                    pass

            # Mark unresolved leads as captcha_email_not_found
            with sqlite3.connect(master_db_file) as con:
                cur = con.cursor()
                for post_id, status in status_by_post.items():
                    if status == 'pending':
                        phone = post_to_phone.get(post_id)
                        if phone:
                            try:
                                cur.execute("UPDATE contacts SET status=? WHERE phone=?", ('captcha_email_not_found', phone))
                                print(f"    -> Marked phone {phone} as captcha_email_not_found")
                            except Exception as e:
                                print(f"    -> Error updating phone {phone} after timeout: {e}")
                con.commit()

            # done with batch -> continue loop to pick up next batch (if any)
            print("    [Batch] completed; checking for next batch...")

    except asyncio.CancelledError:
        print("[process_captchas] Task cancelled.")
    except Exception as e:
        print(f"!!! [process_captchas] An unhandled error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean shutdown of context and playwright
        if context:
            try:
                await context.close()
            except Exception:
                pass
        if p_instance:
            try:
                await p_instance.stop()
            except Exception:
                pass


# --- Microsoft Graph Device Flow (unchanged from your file) ---
async def authenticate_email_token_device_flow(config):
    print("\n--- Microsoft Graph API Device Code Authentication ---")
    global_cfg = config['global_settings']
    client_id = global_cfg['email_client_id']
    tenant_id = global_cfg['email_tenant_id']
    token_file = os.path.join(get_script_dir(), global_cfg['token_file'])
    
    scope = "https://graph.microsoft.com/Mail.Send https://graph.microsoft.com/User.Read offline_access"
    device_code_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/devicecode"
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    try:
        # Step 1: Request device code
        print("Requesting device code...")
        async with httpx.AsyncClient() as client:
            device_response = await client.post(device_code_url, data={
                "client_id": client_id,
                "scope": scope
            })
            device_response.raise_for_status()
            device_data = device_response.json()

        user_code = device_data['user_code']
        verification_uri = device_data['verification_uri']
        device_code = device_data['device_code']
        interval = device_data['interval']
        expires_in = device_data['expires_in']

        print(f"\n1. Open this URL in your browser: \n{verification_uri}")
        print(f"\n2. Enter the following code when prompted: \n{user_code}")
        print(f"\n(This code will expire in {expires_in} seconds)")

        # Step 2: Poll for token
        print("\nWaiting for you to complete authentication in the browser...")
        start_time = time.time()
        while time.time() - start_time < expires_in:
            await asyncio.sleep(interval)
            try:
                async with httpx.AsyncClient() as client:
                    token_response = await client.post(token_url, data={
                        "client_id": client_id,
                        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                        "device_code": device_code
                    })
                    token_response.raise_for_status()
                    token_data = token_response.json()

                if "access_token" in token_data:
                    refresh_token = token_data['refresh_token']
                    with open(token_file, 'w') as f:
                        f.write(refresh_token)
                    print(f"\nAuthentication successful! Refresh token saved to {token_file}")
                    return True
                elif token_data.get("error") == "authorization_pending":
                    sys.stdout.write(".")
                    sys.stdout.flush()
                    continue
                else:
                    print(f"\nError during token polling: {token_data.get('error_description', token_data.get('error'))}")
                    return False
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400 and e.response.json().get("error") == "authorization_pending":
                    sys.stdout.write(".")
                    sys.stdout.flush()
                    continue
                else:
                    print(f"\nHTTP error during token polling: {e}")
                    return False
            except Exception as e:
                print(f"\nAn unexpected error occurred during token polling: {e}")
                return False
        
        print("\nAuthentication timed out. Please try again.")
        return False

    except httpx.HTTPStatusError as e:
        print(f"HTTP error requesting device code: {e.response.status_code} - {e.response.text}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return False


# --- Main CLI (unchanged) ---
def main():
    config = load_config()
    script_dir = get_script_dir()
    queue_db_file = os.path.join(script_dir, config['producer_settings']['queue_database_file'])
    master_db_file = os.path.join(script_dir, config['producer_settings']['master_database_file'])

    # Ensure initial DB setup is done (contacts table, etc.)
    # This part is now handled by add_db_columns.py
    
    print("--- Lead Generation Main Script ---")

    while True:
        print("\n--- Main Menu ---")
        print("1. Process new leads (add to processor queue)")
        print("2. Process CAPTCHAs (manual intervention)")
        print("3. Authenticate Email Token (Device Flow)")
        print("4. Exit")
        choice = input("Enter your choice: ")

        if choice == '1':
            template_id = None
            active_templates = get_active_templates(queue_db_file)
            print("\n--- Available Email Templates ---")
            if not active_templates: print("No active templates found.")
            else:
                for t_id, t_name, t_desc in active_templates: print(f"{t_id}: {t_name} - {t_desc}")
            print("---------------------------------")
            print("(R) Register a New Template")
            print("(99) SMS Messages")
            print("---------------------------------")
            
            template_choice = input("Choose a template to use for this run (e.g., 1, 2, R, 99): ").lower()

            if template_choice == 'r':
                print("\n--- New Template Registration ---")
                templates_dir = os.path.join(script_dir, config['producer_settings'].get('templates_dir', 'templates'))
                template_path = input(f"Enter path to the new template file (relative to '{script_dir}'): ")
                if not os.path.exists(template_path): sys.exit(f"ERROR: File not found at '{template_path}'")
                
                with open(template_path, 'r', encoding='utf-8') as f: template_data = json.load(f)
                
                name = input("Enter a short name for this template: ")
                desc = input("Enter a one-line description: ")
                subject = template_data.get('subject')
                body = template_data.get('body_html')

                with sqlite3.connect(queue_db_file) as con:
                    cur = con.cursor()
                    cur.execute("INSERT INTO templates (template_name, description, base_subject, base_body_html) VALUES (?, ?, ?, ?)", (name, desc, subject, body))
                    template_id = cur.lastrowid
                    con.commit()
                print(f"SUCCESS: Template '{name}' registered with ID: {template_id}")
                message_type = 'email' # Default for new templates
            else:
                try:
                    template_id = int(template_choice)
                    message_type = 'email' # Default for existing email templates
                except Exception:
                    print("Invalid template choice. Returning to main menu.")
                    continue

            if template_id is None:
                print("No valid template selected. Returning to main menu.")
                continue

            city_profiles = get_city_profiles(config)
            print("\n--- Available Cities ---")
            if not city_profiles: print("No cities found in config.")
            else:
                for c_name, c_data in city_profiles.items(): print(f"- {c_name} (Timezone: {c_data['tz']})")
            city_choice = input("Enter the city to process leads for: ").lower()
            if city_choice not in city_profiles:
                print("Invalid city choice. Returning to main menu.")
                continue

            num_leads_input = input("How many new leads would you like to process in this run? (Enter 0 to skip harvesting): ")
            num_leads = int(num_leads_input) if num_leads_input.isdigit() else 0

            if num_leads > 0:
                add_processor_request(master_db_file, template_id, city_choice, num_leads)
                print("Processor request added to queue. Please start contact-extractor.py manually if it's not already running.")
            else:
                print("No new leads requested for processing.")

        elif choice == '2':
            print("Starting CAPTCHA processing...")
            asyncio.run(process_captchas(config))
            print("CAPTCHA processing finished.")
        elif choice == '3':
            print("Starting Email Token Authentication (Device Flow)...")
            # note: authenticate_email_token_device_flow is async; run it
            try:
                asyncio.run(authenticate_email_token_device_flow(config))
            except KeyboardInterrupt:
                print("Authentication interrupted by user.")
            print("Email Token Authentication (Device Flow) finished.")
        elif choice == '4':
            print("Exiting Lead Generation Main Script.")
            break
        elif choice == '99':
            print("Erocessor request added to queue for SMS")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    p_instance_global = None # Declare a global variable for p_instance
    try:
        main()
    except Exception as e:
        print(f"!!! [Main] An unhandled exception occurred in main execution: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if p_instance_global:
            print("--- [Main] Stopping Playwright instance globally. ---")
            pass # Placeholder — contexts are closed within async functions above
