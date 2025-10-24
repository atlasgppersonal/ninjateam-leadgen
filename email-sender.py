import sys
import json
import asyncio
import time
import re
import os
import sqlite3
import random
import base64
from datetime import datetime, timedelta, time as a_time
import pytz
import google.generativeai as genai
from playwright.async_api import async_playwright

# --- ============================== ---
# --- --- MAIN SENDER CLASS --- ---
# --- ============================== ---

class EmailSender:
    def __init__(self, config):
        self.config = config
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.queue_db_file = os.path.join(self.script_dir, config['producer_settings']['queue_database_file'])
        
        # In-memory state
        self.template_cache = {}
        self.variation_cache = {}
        self.sequential_counters = {}
        self.major_pause_counter = 0
        self.next_major_pause_at = self._get_next_major_pause_trigger()

        # Configure Google AI
        global_cfg = config['global_settings']
        genai.configure(api_key=global_cfg['google_api_key'])
        self.model = genai.GenerativeModel(global_cfg['llm_model'])
        
        self.access_token = None
        self.from_address = None

    def _get_next_major_pause_trigger(self):
        """Calculates a new random trigger for the next major pause."""
        settings = self.config['consumer_settings']['timing_settings']['pause_settings']['major_pause']
        return random.randint(settings['emails_before_pause']['min'], settings['emails_before_pause']['max'])

    async def get_auth_token(self):
        """Authenticates with Microsoft Graph API and gets an access token."""
        print(" -> Authenticating with Microsoft Graph...")
        token_file = os.path.join(self.script_dir, self.config['global_settings']['token_file'])
        auth_config = {}
        if os.path.exists(token_file):
            with open(token_file, 'r') as f:
                auth_config['refresh_token'] = f.read()
        else:
            print("!!! ERROR: No refresh_token.txt found. Please run the producer script once to authenticate and generate the token.")
            return False

        email_settings = self.config['global_settings']
        scope = "https://graph.microsoft.com/Mail.Send https://graph.microsoft.com/User.Read offline_access"
        token_url = f"https://login.microsoftonline.com/{email_settings['email_tenant_id']}/oauth2/v2.0/token"
        
        async with async_playwright() as p:
            request_context = await p.request.new_context()
            form_data = {
                "client_id": email_settings['email_client_id'], 
                "scope": scope, 
                "refresh_token": auth_config['refresh_token'], 
                "grant_type": "refresh_token"
            }
            token_response = await request_context.post(token_url, form=form_data)
            
            if token_response.ok:
                token_data = await token_response.json()
                with open(token_file, 'w') as f:
                    f.write(token_data["refresh_token"])
                self.access_token = token_data["access_token"]
                
                me_response = await request_context.get("https://graph.microsoft.com/v1.0/me", headers={"Authorization": f"Bearer {self.access_token}"})
                self.from_address = (await me_response.json())['mail']
                
                await request_context.dispose()
                print(f" -> Authentication successful. Will send emails from: {self.from_address}")
                return True
            else:
                await request_context.dispose()
                print("!!! AUTH ERROR: Failed to refresh access token. It may have expired.")
                if os.path.exists(token_file):
                    os.remove(token_file)
                print("    Please re-run the producer script to re-authenticate.")
                return False

    def is_script_on_global_break(self):
        """Checks if the SCRIPT ITSELF should be paused for a lunch break."""
        print(f"[{datetime.now():%H:%M:%S}] [Global Break Check]")
        work_hours = self.config['consumer_settings']['timing_settings']['working_hours']
        if not work_hours.get('enabled', True):
            print(" -> Working hours disabled, so global break is also disabled.")
            return False

        my_tz_str = self.config['global_settings'].get('my_timezone', 'UTC')
        try:
            my_tz = pytz.timezone(my_tz_str)
        except pytz.UnknownTimeZoneError:
            print(f" -> WARNING: Your local timezone '{my_tz_str}' is invalid. Skipping global break check.")
            return False

        now_local = datetime.now(my_tz)
        print(f" -> Current script time ({my_tz_str}): {now_local.strftime('%H:%M:%S')}")

        lunch_start_time = datetime.strptime(work_hours['lunch_break'], "%H:%M").time()
        duration_str = work_hours.get('lunch_break_duration', "1:00")
        duration_parts = [int(p) for p in duration_str.split(':')]
        lunch_duration = timedelta(hours=duration_parts[0], minutes=duration_parts[1])
        
        lunch_start_dt = my_tz.localize(datetime.combine(now_local.date(), lunch_start_time))
        lunch_end_dt = lunch_start_dt + lunch_duration
        
        print(f" -> Checking if current time is within lunch window: {lunch_start_dt.strftime('%H:%M')} - {lunch_end_dt.strftime('%H:%M')}")

        is_on_break = lunch_start_dt <= now_local < lunch_end_dt
        print(f" -> On Break? {'YES' if is_on_break else 'NO'}.")
        return is_on_break
        
    def get_currently_active_timezones(self):
        """Returns a list of target timezone strings that are currently within working hours."""
        print(f"[{datetime.now():%H:%M:%S}] [Active Timezone Check]")
        active_zones = []
        utc_now = datetime.now(pytz.utc)
        print(f" -> Current UTC time is: {utc_now.strftime('%H:%M:%S')}")

        consumer_cfg = self.config['consumer_settings']
        timing_settings = consumer_cfg.get('timing_settings', {})
        work_hours = timing_settings.get('working_hours', {})
        
        target_timezones = timing_settings.get('target_timezones', [])
        
        if not work_hours.get('enabled', True):
            print(" -> Working hours check is disabled. All target timezones are considered active.")
            return target_timezones
        
        if not target_timezones:
            print(" -> WARNING: 'target_timezones' list is empty in config. No zones can be active.")
            return []

        start_time = datetime.strptime(work_hours.get('start_time_military', '08:00'), "%H:%M").time()
        end_time = datetime.strptime(work_hours.get('end_time_military', '18:30'), "%H:%M").time()
        
        print(f" -> Checking against Work Hours: {start_time.strftime('%H:%M')} to {end_time.strftime('%H:%M')}")
        
        for tz_str in target_timezones:
            try:
                local_tz = pytz.timezone(tz_str)
                local_dt = utc_now.astimezone(local_tz)
                local_time = local_dt.time()
                local_day = local_dt.strftime('%A')
                
                is_in_hours = start_time < local_time < end_time
                is_workday = local_day in work_hours.get('work_days', [])
                
                log_msg = f"   - Checking {tz_str:<18} | Local Time: {local_time.strftime('%H:%M:%S')} | Workday? {'Yes' if is_workday else 'No'} | In Hours? {'Yes' if is_in_hours else 'No'}"

                if is_workday and is_in_hours:
                    active_zones.append(tz_str)
                    log_msg += " -> ACTIVE"
                else:
                    log_msg += " -> INACTIVE"
                print(log_msg)

            except pytz.UnknownTimeZoneError:
                print(f"   - WARNING: Unknown timezone '{tz_str}' in config.json.")
        
        print(f" -> Final list of active timezones: {active_zones}")
        return active_zones

    async def _send_email_api_call(self, to_address, subject, body_html):
        """The actual API call to Microsoft Graph to send an email with embedded images."""
        headers = {"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"}
        
        # --- NEW: Attachment Logic ---
        attachments = []
        attachment_map = self.config['consumer_settings'].get('email_attachments', {})
        for content_id, file_path_str in attachment_map.items():
            try:
                full_path = os.path.join(self.script_dir, file_path_str)
                with open(full_path, "rb") as image_file:
                    encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
                
                attachments.append({
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": os.path.basename(full_path),
                    "contentBytes": encoded_string,
                    "contentId": content_id,
                    "isInline": True
                })
                print(f"    -> Prepared attachment for CID: {content_id}")
            except FileNotFoundError:
                print(f"    -> WARNING: Attachment file not found at '{full_path}'. It will be missing from the email.")
            except Exception as e:
                print(f"    -> ERROR: Failed to process attachment '{full_path}'. Error: {e}")

        email_payload = {
            "message": {
                "subject": subject,
                "body": {"contentType": "HTML", "content": body_html},
                "toRecipients": [{"emailAddress": {"address": to_address}}],
                "attachments": attachments
            },
            "saveToSentItems": "true"
        }
        
        async with async_playwright() as p:
            request_context = await p.request.new_context()
            send_mail_url = "https://graph.microsoft.com/v1.0/me/sendMail"
            response = await request_context.post(send_mail_url, headers=headers, data=json.dumps(email_payload))
            await request_context.dispose()
        
        return response.ok

    # --- Variation Manager Task ---
    async def variation_manager(self):
        print("[Variation Manager] Starting...")
        settings = self.config['consumer_settings']['variation_management']
        
        while True:
            try:
                print(f"[{datetime.now():%H:%M:%S}] [Variation Manager] Running check...")
                with sqlite3.connect(self.queue_db_file) as con:
                    cur = con.cursor()
                    cur.execute("SELECT template_id, COUNT(*) FROM email_queue WHERE status = 'QUEUED' GROUP BY template_id")
                    demand = dict(cur.fetchall())
                
                print(f" -> DEMAND CHECK: Found the following demands: {demand or 'None'}")

                if not demand:
                    print(" -> No demand in queue. Sleeping.")
                else:
                    for template_id, queue_size in demand.items():
                        print(f" -> PROCESSING DEMAND for Template ID {template_id} with reported queue size of {queue_size}")
                        
                        with sqlite3.connect(self.queue_db_file) as con:
                            cur = con.cursor()
                            
                            matching_rule = max(
                                (r for r in settings['rules'] if queue_size >= int(r['queue_size_threshold'])),
                                key=lambda r: int(r['queue_size_threshold']),
                                default=None
                            )
                            target_count = int(matching_rule['target_variation_count']) if matching_rule else 0
                            
                            print(f"    -> RULE MATCHING: Best rule for queue size {queue_size} is threshold {matching_rule.get('queue_size_threshold', 'N/A') if matching_rule else 'N/A'}. Final target count is {target_count}")

                            cur.execute("SELECT COUNT(*) FROM variation_storage WHERE base_template_id = ?", (template_id,))
                            supply = cur.fetchone()[0]
                            print(f"    -> SUPPLY CHECK: Found {supply} existing variations in the database.")

                            if supply >= target_count:
                                print(f" -> Template ID {template_id}: Supply ({supply}) meets target ({target_count}). OK.")
                                continue

                            needed = target_count - supply
                            print(f"    -> ACTION: Supply ({supply}) is less than target ({target_count}). Need to generate {needed} more variations.")
                            
                            cur.execute("SELECT base_subject, base_body_html FROM templates WHERE template_id = ?", (template_id,))
                            base_template = cur.fetchone()
                            if not base_template:
                                print(f"    -> ERROR: Could not find base template for ID {template_id}.")
                                continue

                            prompts = self.config['consumer_settings']['prompts']
                            base_subject, base_body_html = base_template

                            print("    -> Attempt 1: Generating variations with standard prompt.")
                            prompt = prompts['generate_variations'].format(num_variations=needed, base_subject=base_subject, base_body=base_body_html)
                            response = await self.model.generate_content_async(prompt)
                            
                            new_variations = None
                            if response.parts:
                                cleaned_text = response.text.strip().replace("```json", "").replace("```", "").strip()
                                new_variations = json.loads(cleaned_text)
                            else:
                                finish_reason = response.candidates[0].finish_reason.name
                                print(f"    -> Attempt 1 FAILED. Finish Reason: {finish_reason}")
                                
                                if finish_reason == 'SAFETY':
                                    print("    -> Attempt 2: SAFETY triggered. Trying corrective generation prompt.")
                                    corrective_prompt = prompts['fix_and_generate_variations'].format(num_variations=needed, base_subject=base_subject, base_body=base_body_html)
                                    corrective_response = await self.model.generate_content_async(corrective_prompt)
                                    
                                    if corrective_response.parts:
                                        print("    -> Corrective attempt SUCCEEDED.")
                                        cleaned_text = corrective_response.text.strip().replace("```json", "").replace("```", "").strip()
                                        new_variations = json.loads(cleaned_text)
                                    else:
                                        corrective_finish_reason = corrective_response.candidates[0].finish_reason.name
                                        print(f"    -> Corrective attempt FAILED. Finish Reason: {corrective_finish_reason}. Manual template revision is required for Template ID {template_id}.")
                            
                            if new_variations:
                                for var in new_variations:
                                    cur.execute("INSERT INTO variation_storage (base_template_id, subject_html, body_html) VALUES (?, ?, ?)", (template_id, var['subject'], var['body_html']))
                                self.variation_cache.pop(template_id, None)
                                print(f"    -> SUCCESS: Added {len(new_variations)} new variations for Template ID {template_id}.")

            except Exception as e:
                print(f"!!! [Variation Manager] An unexpected error occurred: {e}")
            finally:
                await asyncio.sleep(settings['check_interval_seconds'])

    # --- Email Sending Task ---
    async def email_sending_loop(self):
        print("[Email Sender] Starting main loop...")
        
        while True:
            try:
                if self.is_script_on_global_break():
                    await asyncio.sleep(300)
                    continue

                active_zones = self.get_currently_active_timezones()
                if not active_zones:
                    print(f"[{datetime.now():%H:%M:%S}] [Email Sender] No active timezones. Pausing...")
                    await asyncio.sleep(300)
                    continue
                
                job = None
                with sqlite3.connect(self.queue_db_file) as con:
                    cur = con.cursor()
                    placeholders = ','.join('?' for _ in active_zones)
                    query = f"SELECT * FROM email_queue WHERE status = 'QUEUED' AND timezone IN ({placeholders}) ORDER BY created_at ASC LIMIT 1"
                    cur.execute(query, active_zones)
                    job = cur.fetchone()

                if not job:
                    print(f"[{datetime.now():%H:%M:%S}] [Email Sender] No pending emails in active timezones. Waiting...")
                    await asyncio.sleep(60)
                    continue
                
                queue_id, template_id, lead_data_json, city, _, _, _, _ = job
                lead_data = json.loads(lead_data_json)
                
                # Pre-process lead_data to handle None values
                processed_lead_data = {}
                for key, value in lead_data.items():
                    processed_lead_data[key] = value if value is not None else ""
                lead_data = processed_lead_data

                to_email = lead_data.get('email')
                
                if not to_email:
                    print(f" -> Job {queue_id}: Skipping, lead has no email address.")
                    with sqlite3.connect(self.queue_db_file) as con:
                        cur = con.cursor()
                        cur.execute("UPDATE email_queue SET status = 'ERROR_NO_EMAIL' WHERE queue_id = ?", (queue_id,))
                    continue

                print(f"\n-> Processing Job {queue_id} for {to_email} (Template: {template_id}, City: {city})")

                if template_id not in self.variation_cache:
                    with sqlite3.connect(self.queue_db_file) as con:
                        cur = con.cursor()
                        cur.execute("SELECT subject_html, body_html FROM variation_storage WHERE base_template_id = ?", (template_id,))
                        self.variation_cache[template_id] = cur.fetchall()
                
                variations = self.variation_cache.get(template_id)
                if not variations:
                    print(f"    -> WARNING: No variations for template {template_id}. Waiting for manager.")
                    await asyncio.sleep(10)
                    continue

                counter = self.sequential_counters.get(template_id, 0)
                selected_variation = variations[counter % len(variations)]
                self.sequential_counters[template_id] = counter + 1
                
                variation_subject, variation_body = selected_variation
                html_templates = self.config['consumer_settings']['html_templates']
                signature_block = html_templates['signature_html_template']
                master_template = html_templates['html_template']

                assembled_html = master_template.replace("{body_content}", variation_body)
                assembled_html = assembled_html.replace("{signature_block}", signature_block)

                # --- NEW, ROBUST REPLACEMENT LOGIC ---
                first_name = str(lead_data.get('name') or 'Friend').split(' ')[0]
                bitly_link = f"https://ninjateam.ai/lp/craigslist?h={lead_data.get('hash')}"
                
                final_subject = variation_subject.replace("{{first_name}}", first_name)
                
                replacement_context = {**lead_data} 
                replacement_context['first_name'] = first_name
                replacement_context['bitly_link'] = bitly_link
                replacement_context['subject'] = final_subject
                
                final_body = assembled_html
                
                for key, value in replacement_context.items():
                    if isinstance(value, str):
                        placeholder = f"{{{key}}}"
                        final_body = final_body.replace(placeholder, value)
                
                success = await self._send_email_api_call(to_email, final_subject, final_body)
                
                with sqlite3.connect(self.queue_db_file) as con:
                    cur = con.cursor()
                    if success:
                        print(f"    -> SUCCESS: Email sent to {to_email}.")
                        cur.execute("UPDATE email_queue SET status = 'SENT', sent_at = CURRENT_TIMESTAMP WHERE queue_id = ?", (queue_id,))
                    else:
                        print(f"    -> FAILED: API call to send email to {to_email} failed.")
                        cur.execute("UPDATE email_queue SET status = 'ERROR_API_FAILURE' WHERE queue_id = ?", (queue_id,))

                self.major_pause_counter += 1
                if self.major_pause_counter >= self.next_major_pause_at:
                    pause_settings = self.config['consumer_settings']['timing_settings']['pause_settings']['major_pause']
                    duration = random.randint(pause_settings['pause_duration_minutes']['min'] * 60, pause_settings['pause_duration_minutes']['max'] * 60)
                    print(f"    -> Taking a major pause for {duration // 60} minutes...")
                    await asyncio.sleep(duration)
                    self.major_pause_counter = 0
                    self.next_major_pause_at = self._get_next_major_pause_trigger()
                else:
                    interval_settings = self.config['consumer_settings']['timing_settings']['interval_between_emails_seconds']
                    duration = random.randint(interval_settings['min'], interval_settings['max'])
                    await asyncio.sleep(duration)
                
            except Exception as e:
                print(f"!!! [Email Sender] An unexpected error occurred: {e}")
                import traceback
                traceback.print_exc()
                # Update the status of the current job to an error state
                if 'queue_id' in locals(): # Ensure queue_id is defined
                    with sqlite3.connect(self.queue_db_file) as con:
                        cur = con.cursor()
                        cur.execute("UPDATE email_queue SET status = 'ERROR_UNEXPECTED', sent_at = CURRENT_TIMESTAMP WHERE queue_id = ?", (queue_id,))
                await asyncio.sleep(30) # Still pause to prevent rapid error looping

# --- ============================== ---
# --- --- MAIN EXECUTION --- ---
# --- ============================== ---

async def main():
    print("--- Email Sender Service (Consumer) ---")
    
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_filename = os.path.join(script_dir, 'config.json')
        with open(config_filename, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        sys.exit("CRITICAL ERROR: 'config.json' file not found.")
    except json.JSONDecodeError:
        sys.exit("CRITICAL ERROR: Could not parse config.json.")

    sender = EmailSender(config)
    
    if not await sender.get_auth_token():
        sys.exit("Could not authenticate. Exiting.")
        
    # await asyncio.gather(
    #     sender.variation_manager(),
    #     sender.email_sending_loop()
    # )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n-> Consumer process shut down by user.")
